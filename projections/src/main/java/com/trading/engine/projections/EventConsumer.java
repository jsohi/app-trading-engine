package com.trading.engine.projections;

import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Object2LongHashMap;

/**
 * Consumes the cluster's SBE-encoded event stream and dispatches each event to the set of {@link
 * Projection}s registered for its {@code eventType} (SBE {@code templateId}). The read-side
 * counterpart of the cluster's event publisher: one side emits events, this side routes them into
 * the projections.
 *
 * <p><b>seqNo semantics (Wave 3 scaffolding):</b> the {@code seqNo} delivered to each projection is
 * the consumer's internal <b>ingress counter</b> (messages dispatched since {@link #start}), not
 * the authoritative event sequence number from the SBE payload. The counter and the per-projection
 * last-processed sequence tracked inside this consumer are in the same units (messages), so lag
 * math in {@link ProjectionRegistry} is consistent. TODO APP-8 (Wave 4): replace with payload
 * sequence number once the cluster → projections event format is finalized.
 *
 * <p><b>Wiring (this PR is a scaffold; the live wiring lands in APP-8, Wave 4):</b>
 *
 * <ol>
 *   <li>Construct the consumer and {@link #registerProjection register} every projection BEFORE
 *       calling {@link #start}. Registration after start is rejected — the dispatch table is built
 *       once and then frozen, which gives the thread-start happens-before edge safe publication of
 *       the table to the poll thread without needing {@code volatile}.
 *   <li>Call {@link #start(Aeron, String, int)} to create the Aeron {@link Subscription}.
 *   <li>Drive {@link #poll(int)} from the caller's duty cycle. The consumer does NOT own a thread
 *       or an {@link org.agrona.concurrent.IdleStrategy IdleStrategy} — that's the caller's
 *       responsibility.
 *   <li>Call {@link #close} to shut down cleanly. {@link #close} is terminal — construct a new
 *       instance if a fresh consumer is needed.
 * </ol>
 *
 * <p><b>Where projections read from:</b> the issue spec says the cluster publishes events on a
 * dedicated Aeron channel / streamId, distinct from client egress, specifically so slow projections
 * cannot back-pressure the FIX client response path. This consumer is channel- agnostic — the
 * caller picks the stream. Wave 4 decides whether that stream is the live cluster egress or an
 * Aeron Archive replay (the latter gives more buffering headroom and is safer from a back-pressure
 * standpoint). In a CQRS system, coupling cluster write-side latency to read-side consumer speed
 * defeats the point of CQRS; the APP-8 implementer must ensure the chosen channel preserves the
 * decoupling.
 *
 * <p><b>Threading:</b> single-threaded by contract. One thread calls {@link #poll} and consequently
 * dispatches to projections. No synchronisation, no {@code volatile}. The dispatch table is
 * populated before {@link #start} and never mutated afterwards, so the handover to the poll thread
 * is via {@code Thread.start()} happens-before (or {@link Subscription} construction, whichever the
 * caller uses to hand off). Projections invoked from this consumer MUST NOT block or allocate.
 *
 * <p><b>Zero allocation in {@link #onFragment}:</b> pre-allocated {@link MessageHeaderDecoder}
 * flyweight, primitive-keyed {@link Int2ObjectHashMap} lookup (no boxing), pre-populated {@link
 * Object2LongHashMap} update (in-place on existing keys, zero allocation), stack-only array
 * iteration. No lambdas, no streams, no String concat outside throw branches. Fragments shorter
 * than the SBE header length, or fragments whose templateId has no registered projection, are
 * silently dropped (a counter is bumped for diagnostics but no allocation happens).
 *
 * <p><b>Scaffolding deferrals (APP-8, Wave 4 or later):</b>
 *
 * <ul>
 *   <li>Plain {@link FragmentHandler} rather than {@code ControlledFragmentHandler} — exceptions
 *       propagate out of the poll loop and crash the caller, which is a safe default for
 *       scaffolding. Revisit once error-state tracking lands.
 *   <li>No {@code FragmentAssembler} wrapper — SBE events in this engine are small (&lt;1 KB
 *       realistically, hard-capped at 64 KB per {@code EventEntry.MAX_PAYLOAD_LENGTH}), well under
 *       Aeron's default MTU. Add the wrapper if we ever emit larger events.
 *   <li>Catch-up via {@code EventJournal} is not wired here. {@code EventJournal} lives in the
 *       cluster module and projections cannot depend on it without inverting module direction.
 *       APP-8 will wire catch-up as a request/response over Aeron, not a direct call.
 * </ul>
 */
public final class EventConsumer implements FragmentHandler {

  /** Empty array used as a "no projections for this eventType" sentinel. */
  private static final Projection[] EMPTY = new Projection[0];

  /** Sentinel returned by {@link Object2LongHashMap} for missing keys. */
  private static final long MISSING_SEQUENCE = -1L;

  /** Pre-allocated SBE header flyweight; wrapped over each incoming fragment on dispatch. */
  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();

  /**
   * eventType (SBE templateId) → projections registered for it. Populated before {@link #start};
   * never mutated after start. Safe to read from the poll thread without {@code volatile} because
   * {@link #start} is the publication point and the caller's thread-start or subscription hand-off
   * supplies the happens-before edge.
   */
  private final Int2ObjectHashMap<Projection[]> dispatchTable = new Int2ObjectHashMap<>();

  /**
   * Authoritative per-projection last-processed sequence, updated on every dispatch. This consumer
   * owns the tracking rather than trusting {@link Projection#lastProcessedSequence()}, so {@link
   * ProjectionRegistry} gets consistent lag math even if a projection forgets to update its own
   * internal tracking. Pre-populated in {@link #start} with a zero entry for every distinct
   * projection so that on-dispatch updates never rehash or grow the table — {@code put} only
   * overwrites existing keys on the hot path.
   */
  private final Object2LongHashMap<Projection> lastSeqByProjection =
      new Object2LongHashMap<>(MISSING_SEQUENCE);

  /** The open subscription, non-null between {@link #start} and {@link #close}. */
  private Subscription subscription;

  /** Ingress message counter; incremented once per dispatched fragment. See class Javadoc. */
  private long ingressSequence;

  /** Count of fragments silently dropped because no projection is registered for their type. */
  private long unknownTemplateDropCount;

  /** Count of fragments silently dropped because they were shorter than the SBE header length. */
  private long truncatedFragmentDropCount;

  /** Lifecycle guard. {@code true} once {@link #start} has been called. */
  private boolean started;

  /** Set by {@link #close}; once closed, the consumer cannot be started again. */
  private boolean closed;

  // ---------------------------------------------------------------------------
  // Registration (startup-only)
  // ---------------------------------------------------------------------------

  /**
   * Register a projection to receive events of the given types. Must be called before {@link
   * #start}. Multiple projections can register for the same {@code eventType}; the same projection
   * can register for multiple types in a single call.
   *
   * <p>Registration uses a copy-on-write append into the per-eventType array, so dispatch remains a
   * simple array iteration with no set-contains check on the hot path. Validation is two-pass: all
   * eventTypes are validated before any mutation, so a rejected call leaves the dispatch table
   * unchanged.
   *
   * @param projection non-null projection
   * @param eventTypes one or more SBE {@code templateId}s the projection wants to receive — must
   *     not contain duplicates within the call and must not include a type the projection is
   *     already registered for
   * @throws NullPointerException if {@code projection} is null
   * @throws IllegalArgumentException if {@code eventTypes} is empty, contains a duplicate within
   *     the call, or contains a type the projection is already registered for
   * @throws IllegalStateException if called after {@link #start} or after {@link #close}
   */
  public void registerProjection(final Projection projection, final int... eventTypes) {
    if (projection == null) {
      throw new NullPointerException("projection must not be null");
    }
    if (eventTypes == null || eventTypes.length == 0) {
      throw new IllegalArgumentException("eventTypes must be non-empty");
    }
    if (closed) {
      throw new IllegalStateException("EventConsumer is closed");
    }
    if (started) {
      throw new IllegalStateException(
          "EventConsumer.registerProjection must be called before start()");
    }
    // Two-pass validation: reject duplicates-within-call and duplicates-against-existing BEFORE
    // mutating the dispatch table, so a rejected call leaves state unchanged (atomicity).
    for (int i = 0; i < eventTypes.length; i++) {
      final int eventType = eventTypes[i];
      final Projection[] existing = dispatchTable.getOrDefault(eventType, EMPTY);
      for (final Projection p : existing) {
        if (p == projection) {
          throw new IllegalArgumentException(
              "projection already registered for eventType " + eventType);
        }
      }
      for (int j = 0; j < i; j++) {
        if (eventTypes[j] == eventType) {
          throw new IllegalArgumentException("duplicate eventType in call: " + eventType);
        }
      }
    }
    for (final int eventType : eventTypes) {
      final Projection[] existing = dispatchTable.getOrDefault(eventType, EMPTY);
      final Projection[] updated = new Projection[existing.length + 1];
      System.arraycopy(existing, 0, updated, 0, existing.length);
      updated[existing.length] = projection;
      dispatchTable.put(eventType, updated);
    }
  }

  // ---------------------------------------------------------------------------
  // Lifecycle
  // ---------------------------------------------------------------------------

  /**
   * Create the Aeron {@link Subscription} on {@code channel} / {@code streamId} and seed the
   * per-projection tracking map with one zero entry per distinct registered projection. After this
   * call the dispatch table is frozen and {@link #poll} may be driven.
   *
   * @throws NullPointerException if {@code aeron} or {@code channel} is null
   * @throws IllegalStateException if already started or already closed
   */
  public void start(final Aeron aeron, final String channel, final int streamId) {
    if (aeron == null) {
      throw new NullPointerException("aeron must not be null");
    }
    if (channel == null) {
      throw new NullPointerException("channel must not be null");
    }
    if (closed) {
      throw new IllegalStateException("EventConsumer is closed");
    }
    if (started) {
      throw new IllegalStateException("EventConsumer already started");
    }
    seedLastSeqMap();
    this.subscription = aeron.addSubscription(channel, streamId);
    this.started = true;
  }

  /**
   * Poll the subscription, dispatching up to {@code fragmentLimit} fragments to registered
   * projections. Returns the number of fragments actually consumed (0 if the subscription had
   * nothing available).
   *
   * @throws IllegalStateException if called before {@link #start}
   */
  public int poll(final int fragmentLimit) {
    if (closed) {
      throw new IllegalStateException("EventConsumer is closed");
    }
    if (!started) {
      throw new IllegalStateException("EventConsumer.poll called before start()");
    }
    return subscription.poll(this, fragmentLimit);
  }

  /**
   * Close the subscription and mark the consumer terminal. Idempotent — safe to call multiple times
   * or without {@link #start}. After {@link #close} the consumer cannot be restarted, registered
   * to, polled, or {@link #reset} — every state-mutating method throws or returns the "missing"
   * fallback. Construct a new instance if a fresh consumer is needed. Drops both the dispatch table
   * and the per-projection tracking so stray post-close reads return consistent zeros.
   */
  public void close() {
    if (subscription != null) {
      subscription.close();
      subscription = null;
    }
    started = false;
    closed = true;
    ingressSequence = 0L;
    unknownTemplateDropCount = 0L;
    truncatedFragmentDropCount = 0L;
    // close() is terminal — drop both maps outright. Subsequent
    // lastProcessedSequence(projection) reads return 0L via the MISSING_SEQUENCE fallback, and
    // any caller that tries to register / poll / reset gets a clear IllegalStateException.
    dispatchTable.clear();
    lastSeqByProjection.clear();
  }

  // ---------------------------------------------------------------------------
  // FragmentHandler — hot path
  // ---------------------------------------------------------------------------

  /**
   * Dispatch one fragment. Package-private for direct test access (tests feed pre-encoded buffers
   * without spinning up a real Aeron driver). Zero allocation.
   *
   * <p>Fragments are silently dropped and counted in {@link #truncatedFragmentDropCount()} when:
   *
   * <ul>
   *   <li>{@code length < MessageHeaderDecoder.ENCODED_LENGTH} — header itself can't be decoded
   *   <li>{@code length < ENCODED_LENGTH + headerDecoder.blockLength()} — the declared SBE
   *       fixed-length block extends past the end of the delivered fragment, which means a
   *       projection's flyweight decoder would read garbage past the buffer end and silently build
   *       a corrupted read model. Catching this here is a load-bearing safety net.
   * </ul>
   *
   * <p>Unknown templateIds (no registered projection) are silently dropped and counted in {@link
   * #unknownTemplateDropCount()}. Drops never bump the ingress counter.
   */
  @Override
  public void onFragment(
      final DirectBuffer buffer, final int offset, final int length, final Header header) {
    if (length < MessageHeaderDecoder.ENCODED_LENGTH) {
      truncatedFragmentDropCount++;
      return;
    }
    headerDecoder.wrap(buffer, offset);
    // SBE blockLength is a uint16 (range 0..65535) widened to int — the addition with
    // ENCODED_LENGTH (8) cannot overflow and the result cannot be negative.
    final int blockLength = headerDecoder.blockLength();
    if (length < MessageHeaderDecoder.ENCODED_LENGTH + blockLength) {
      truncatedFragmentDropCount++;
      return;
    }
    final int eventType = headerDecoder.templateId();
    final Projection[] handlers = dispatchTable.get(eventType);
    if (handlers == null) {
      unknownTemplateDropCount++;
      return;
    }
    ingressSequence++;
    final int payloadOffset = offset + MessageHeaderDecoder.ENCODED_LENGTH;
    final int payloadLength = length - MessageHeaderDecoder.ENCODED_LENGTH;
    for (final Projection handler : handlers) {
      handler.onEvent(ingressSequence, eventType, buffer, payloadOffset, payloadLength);
      // Update consumer-owned tracking (pre-seeded in start() — put overwrites existing entry,
      // no allocation or rehash on the hot path).
      lastSeqByProjection.put(handler, ingressSequence);
    }
  }

  // ---------------------------------------------------------------------------
  // Diagnostics
  // ---------------------------------------------------------------------------

  /**
   * The number of fragments this consumer has dispatched since {@link #start} (or since the last
   * {@link #reset}). In the same units as {@link #lastProcessedSequence(Projection)}, so {@link
   * ProjectionRegistry#getLag()} math is meaningful. See class Javadoc — this is a consumer-side
   * ingress counter, not an authoritative event sequence number. Will be replaced by the payload
   * seqNo in APP-8.
   */
  public long lastProcessedSequence() {
    return ingressSequence;
  }

  /**
   * Consumer-authoritative last-processed sequence for the given projection, or {@code 0} if the
   * projection has not been dispatched to yet (or was never registered with this consumer).
   *
   * <p>Used by {@link ProjectionRegistry#getLag()} rather than {@link
   * Projection#lastProcessedSequence()} — the consumer's view is the ground truth for lag math,
   * since a buggy projection that forgets to update its own tracking would otherwise report stale
   * lag forever.
   */
  public long lastProcessedSequence(final Projection projection) {
    final long value = lastSeqByProjection.getValue(projection);
    return value == MISSING_SEQUENCE ? 0L : value;
  }

  public long unknownTemplateDropCount() {
    return unknownTemplateDropCount;
  }

  public long truncatedFragmentDropCount() {
    return truncatedFragmentDropCount;
  }

  public boolean isStarted() {
    return started;
  }

  public boolean isClosed() {
    return closed;
  }

  /**
   * Package-private test hook — flip the {@code started} flag without constructing a real Aeron
   * {@link Subscription}. Used only by {@code EventConsumerTest} to exercise the post-start
   * registration guard without spinning up an embedded media driver. Also seeds the per-projection
   * tracking map so tests can exercise dispatch after the fake start.
   */
  void markStartedForTest() {
    seedLastSeqMap();
    this.started = true;
  }

  /**
   * Reset the ingress counter, drop counters, and per-projection tracking to zero, then call {@link
   * Projection#reset()} on every distinct registered projection. Used before a full Aeron Archive
   * replay to rebuild all projections. Legal before {@link #start} (a no-op for the counters and
   * projection set, since nothing is registered yet). Rejected after {@link #close} — once
   * terminal, the consumer cannot resurrect projection state.
   *
   * <p>Must be called on the same thread as {@link #poll} (typically the poll thread, between
   * polls) so the reset writes are visible before the next dispatch. Dedup across projections
   * registered for multiple eventTypes is implicit: {@link #lastSeqByProjection}'s key set IS the
   * set of distinct registered projections (populated by {@link #seedLastSeqMap}), so iterating it
   * gives one reset per projection at no extra bookkeeping cost.
   *
   * @throws IllegalStateException if called after {@link #close}
   */
  public void reset() {
    if (closed) {
      throw new IllegalStateException("EventConsumer is closed");
    }
    ingressSequence = 0L;
    unknownTemplateDropCount = 0L;
    truncatedFragmentDropCount = 0L;
    // Re-seed first so reset() is valid before start() (where lastSeqByProjection is still
    // empty). seedLastSeqMap is idempotent — only puts missing keys.
    seedLastSeqMap();
    for (final Projection p : lastSeqByProjection.keySet()) {
      lastSeqByProjection.put(p, 0L);
      p.reset();
    }
  }

  // ---------------------------------------------------------------------------
  // Internals
  // ---------------------------------------------------------------------------

  /**
   * Pre-populate {@link #lastSeqByProjection} with one zero entry per distinct registered
   * projection, so subsequent updates on the hot path only overwrite existing keys (no allocation,
   * no rehash). Called from {@link #start} and {@link #markStartedForTest}.
   */
  private void seedLastSeqMap() {
    for (final Projection[] handlers : dispatchTable.values()) {
      for (final Projection handler : handlers) {
        if (lastSeqByProjection.getValue(handler) == MISSING_SEQUENCE) {
          lastSeqByProjection.put(handler, 0L);
        }
      }
    }
  }
}
