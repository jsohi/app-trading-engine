package com.trading.engine.projections;

import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import org.agrona.DirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Object2IntHashMap;

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
 * <p><b>Threading:</b> single-threaded for mutation — one thread calls {@link #poll} and dispatches
 * to projections. The ingress counter ({@code ingressSequence}) and per-projection tracking ({@link
 * #lastSeqByIndex}) use {@link VarHandle} release/acquire semantics for cross-thread diagnostic
 * reads (health checks, monitoring). This matches Aeron's {@code putLongOrdered} / {@code
 * getLongVolatile} pattern: the poll thread writes with {@code setRelease}, the monitoring thread
 * reads with {@code getAcquire}. Lifecycle flags ({@code started}, {@code closed}) and drop
 * counters are {@code volatile} for cross-thread visibility. The dispatch table and projection
 * index are frozen after {@link #start} and safely published via the volatile store of {@link
 * #lastSeqByIndex}.
 *
 * <p><b>Zero allocation in {@link #onFragment}:</b> pre-allocated {@link MessageHeaderDecoder}
 * flyweight, primitive-keyed {@link Int2ObjectHashMap} lookups (no boxing), pre-computed {@code
 * int[]} index arrays for direct {@link VarHandle#setRelease} into {@link #lastSeqByIndex} (no hash
 * probes on the per-projection hot path), stack-only array iteration. No lambdas, no streams, no
 * String concat outside throw branches. Fragments shorter than the SBE header length, or fragments
 * whose templateId has no registered projection, are silently dropped (a counter is bumped for
 * diagnostics but no allocation happens).
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

  // ---------------------------------------------------------------------------
  // VarHandles — static, shared across all instances
  // ---------------------------------------------------------------------------

  /** VarHandle for per-element release/acquire access on {@link #lastSeqByIndex}. */
  private static final VarHandle SEQ_ARRAY = MethodHandles.arrayElementVarHandle(long[].class);

  /** VarHandle for release/acquire access on {@link #ingressSequence}. */
  private static final VarHandle INGRESS_SEQ;

  static {
    try {
      INGRESS_SEQ =
          MethodHandles.lookup().findVarHandle(EventConsumer.class, "ingressSequence", long.class);
    } catch (final ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  // ---------------------------------------------------------------------------
  // Constants
  // ---------------------------------------------------------------------------

  /** Empty array used as a "no projections for this eventType" sentinel. */
  private static final Projection[] EMPTY = new Projection[0];

  // ---------------------------------------------------------------------------
  // Fields
  // ---------------------------------------------------------------------------

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
   * Projection → index into {@link #lastSeqByIndex}. Populated in {@link #seedLastSeqMap()}; never
   * mutated after. Safe for cross-thread reads via the volatile publication of {@link
   * #lastSeqByIndex}. Missing-value sentinel is {@code -1} (no valid index is negative).
   */
  private final Object2IntHashMap<Projection> projectionIndex = new Object2IntHashMap<>(-1);

  /**
   * Pre-computed projection ordinal indices for each eventType, parallel to the {@link
   * Projection}[] arrays in {@link #dispatchTable}. For eventType {@code t}, {@code
   * dispatchIndices.get(t)[i]} is the {@link #lastSeqByIndex} index for {@code
   * dispatchTable.get(t)[i]}. Eliminates the {@link #projectionIndex} hash probe from the dispatch
   * hot path. Populated in {@link #seedLastSeqMap()}; never mutated after.
   */
  private final Int2ObjectHashMap<int[]> dispatchIndices = new Int2ObjectHashMap<>();

  /**
   * Per-projection last-processed sequence, indexed by projection ordinal from {@link
   * #projectionIndex}. Single writer (poll thread) via {@code VarHandle.setRelease()}; monitoring
   * threads read via {@code VarHandle.getAcquire()}. {@code null} before {@link #seedLastSeqMap()};
   * once assigned, never nulled or replaced. The field is {@code volatile} so the reference
   * publication serves as the happens-before fence for {@link #projectionIndex} and {@link
   * #dispatchIndices} — any thread that reads {@code lastSeqByIndex != null} is guaranteed to see
   * the fully populated projection index and dispatch indices.
   */
  private volatile long[] lastSeqByIndex;

  /** The open subscription, non-null between {@link #start} and {@link #close}. */
  private Subscription subscription;

  /**
   * Ingress message counter; incremented once per dispatched fragment. Written as a plain {@code
   * long} on the poll thread (single-writer), then release-published via {@link #INGRESS_SEQ} after
   * the dispatch loop. Cross-thread reads use {@code INGRESS_SEQ.getAcquire()}.
   */
  private long ingressSequence;

  /**
   * Count of fragments silently dropped because no projection is registered for their type. {@code
   * volatile} for cross-thread diagnostic reads; written only on the drop path.
   */
  private volatile long unknownTemplateDropCount;

  /**
   * Count of fragments silently dropped because they were shorter than the SBE header length.
   * {@code volatile} for cross-thread diagnostic reads; written only on the drop path.
   */
  private volatile long truncatedFragmentDropCount;

  /**
   * Lifecycle guard. {@code true} once {@link #start} has been called. {@code volatile} for
   * cross-thread visibility from monitoring threads calling {@link #isStarted()}.
   */
  private volatile boolean started;

  /**
   * Set by {@link #close}; once closed, the consumer cannot be started again. {@code volatile} for
   * cross-thread visibility from monitoring threads calling {@link #isClosed()}.
   */
  private volatile boolean closed;

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
   * per-projection tracking array with zero entries. After this call the dispatch table is frozen
   * and {@link #poll} may be driven.
   *
   * @param aeron the Aeron client; must not be null
   * @param channel the Aeron channel; must not be null
   * @param streamId the Aeron stream ID
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
   * @param fragmentLimit maximum number of fragments to process
   * @return the number of fragments consumed
   * @throws IllegalStateException if called before {@link #start} or after {@link #close}
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
   * to, polled, or {@link #reset} — every state-mutating method throws.
   *
   * <p>Zeroes the ingress counter first (via {@code setRelease}) to minimise the transient lag
   * spike visible to concurrent monitoring threads. Does NOT null {@link #lastSeqByIndex}, clear
   * {@link #projectionIndex}, or clear {@link #dispatchTable} — these are frozen after seed and
   * must remain readable by concurrent monitoring threads.
   *
   * <p>Volatile writes of {@code started = false} and {@code closed = true} are performed last so
   * monitoring threads that see {@code closed == true} also see the zeroed counters (happens-before
   * from the volatile store).
   */
  public void close() {
    if (subscription != null) {
      subscription.close();
      subscription = null;
    }
    // Zero ingress FIRST — minimises transient lag spike for concurrent monitoring readers.
    // Reader sees head=0 with old per-projection values → lag negative → clamped to 0.
    INGRESS_SEQ.setRelease(this, 0L);
    ingressSequence = 0L; // defensive: VarHandle.setRelease above already wrote this field
    unknownTemplateDropCount = 0L;
    truncatedFragmentDropCount = 0L;
    // Zero per-projection tracking (do NOT null lastSeqByIndex or clear projectionIndex —
    // frozen state must remain readable by concurrent monitoring threads).
    final long[] seqArr = lastSeqByIndex;
    if (seqArr != null) {
      for (int i = 0; i < seqArr.length; i++) {
        SEQ_ARRAY.setRelease(seqArr, i, 0L);
      }
    }
    // Volatile writes LAST — monitoring threads that see closed=true also see zeroed counters.
    started = false;
    closed = true;
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
   *
   * <p>Per-projection tracking writes use {@code VarHandle.setRelease()} on the {@link
   * #lastSeqByIndex} array, with pre-computed indices from {@link #dispatchIndices} to avoid hash
   * probes on the hot path. The ingress counter is release-published after the dispatch loop via
   * {@link #INGRESS_SEQ}.
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
    final long[] seqArr = lastSeqByIndex; // hoist volatile read before loop
    final int[] indices = dispatchIndices.get(eventType); // pre-computed ordinals (null pre-seed)
    for (int i = 0; i < handlers.length; i++) {
      handlers[i].onEvent(ingressSequence, eventType, buffer, payloadOffset, payloadLength);
      if (seqArr != null && indices != null) {
        SEQ_ARRAY.setRelease(seqArr, indices[i], ingressSequence);
      }
    }
    INGRESS_SEQ.setRelease(this, ingressSequence);
  }

  // ---------------------------------------------------------------------------
  // Diagnostics
  // ---------------------------------------------------------------------------

  /**
   * The number of fragments this consumer has dispatched since {@link #start} (or since the last
   * {@link #reset}). In the same units as {@link #lastProcessedSequence(Projection)}, so {@link
   * ProjectionRegistry#getLagSnapshot()} math is meaningful. See class Javadoc — this is a
   * consumer-side ingress counter, not an authoritative event sequence number. Will be replaced by
   * the payload seqNo in APP-8.
   *
   * <p>Cross-thread safe — reads via {@code VarHandle.getAcquire()} for monitoring threads.
   *
   * @return the ingress sequence counter
   */
  public long lastProcessedSequence() {
    return (long) INGRESS_SEQ.getAcquire(this);
  }

  /**
   * Cross-thread-safe last-processed sequence for the given projection. Reads from the
   * volatile-backed {@link #lastSeqByIndex} array via {@code VarHandle.getAcquire()}.
   *
   * <p>Used by {@link ProjectionRegistry#getLagSnapshot()} rather than {@link
   * Projection#lastProcessedSequence()} — the consumer's view is the ground truth for lag math,
   * since a buggy projection that forgets to update its own tracking would otherwise report stale
   * lag forever.
   *
   * @param projection the projection to query; must not be null
   * @return the last-processed sequence, or {@code 0L} if the projection is unknown or the consumer
   *     has not been started/seeded
   */
  public long lastProcessedSequence(final Projection projection) {
    final long[] seqArr = lastSeqByIndex;
    if (seqArr == null) {
      return 0L;
    }
    final int idx = projectionIndex.getValue(projection);
    if (idx < 0) {
      return 0L;
    }
    return (long) SEQ_ARRAY.getAcquire(seqArr, idx);
  }

  /**
   * Count of fragments dropped because no projection was registered for their templateId. {@code
   * volatile} for cross-thread diagnostic reads.
   *
   * @return the drop count
   */
  public long unknownTemplateDropCount() {
    return unknownTemplateDropCount;
  }

  /**
   * Count of fragments dropped because they were shorter than the SBE header length. {@code
   * volatile} for cross-thread diagnostic reads.
   *
   * @return the drop count
   */
  public long truncatedFragmentDropCount() {
    return truncatedFragmentDropCount;
  }

  /**
   * Whether the consumer has been started. {@code volatile} for cross-thread visibility.
   *
   * @return {@code true} if {@link #start} has been called
   */
  public boolean isStarted() {
    return started;
  }

  /**
   * Whether the consumer has been closed. {@code volatile} for cross-thread visibility.
   *
   * @return {@code true} if {@link #close} has been called
   */
  public boolean isClosed() {
    return closed;
  }

  /**
   * Package-private test hook — flip the {@code started} flag without constructing a real Aeron
   * {@link Subscription}. Used only by {@code EventConsumerTest} to exercise the post-start
   * registration guard without spinning up an embedded media driver. Also seeds the per-projection
   * tracking array and pre-computes dispatch indices so tests can exercise dispatch after the fake
   * start.
   */
  void markStartedForTest() {
    seedLastSeqMap();
    this.started = true;
  }

  /**
   * Resets the ingress counter, drop counters, and per-projection tracking to zero, then calls
   * {@link Projection#reset()} on every distinct registered projection. Used before a full Aeron
   * Archive replay to rebuild all projections. Legal before {@link #start} (a no-op for the
   * counters and projection set, since nothing is registered yet). Rejected after {@link #close} —
   * once terminal, the consumer cannot resurrect projection state.
   *
   * <p>Zeroes the ingress counter first (via {@code setRelease}) to minimise the transient lag
   * spike visible to concurrent monitoring threads: a reader that sees {@code head=0} with some
   * projections still at their old values computes negative lag, which is clamped to zero — no
   * false alarm.
   *
   * <p>Must be called on the same thread as {@link #poll} (typically the poll thread, between
   * polls) so the reset writes are visible before the next dispatch. Dedup across projections
   * registered for multiple eventTypes is implicit: {@link #projectionIndex}'s key set IS the set
   * of distinct registered projections (populated by {@link #seedLastSeqMap}), so iterating it
   * gives one reset per projection at no extra bookkeeping cost.
   *
   * @throws IllegalStateException if called after {@link #close}
   */
  public void reset() {
    if (closed) {
      throw new IllegalStateException("EventConsumer is closed");
    }
    // Zero ingress FIRST — minimises transient lag spike for concurrent monitoring readers.
    INGRESS_SEQ.setRelease(this, 0L);
    ingressSequence = 0L; // defensive: VarHandle.setRelease above already wrote this field
    unknownTemplateDropCount = 0L;
    truncatedFragmentDropCount = 0L;
    seedLastSeqMap();
    final long[] seqArr = lastSeqByIndex;
    if (seqArr != null) {
      for (int i = 0; i < seqArr.length; i++) {
        SEQ_ARRAY.setRelease(seqArr, i, 0L);
      }
    }
    for (final Projection p : projectionIndex.keySet()) {
      p.reset();
    }
  }

  // ---------------------------------------------------------------------------
  // Internals
  // ---------------------------------------------------------------------------

  /**
   * Seed the per-projection tracking infrastructure. Populates {@link #projectionIndex} with one
   * entry per distinct registered projection, allocates the {@link #lastSeqByIndex} array (once),
   * and builds the pre-computed {@link #dispatchIndices} parallel to the dispatch table.
   *
   * <p>Idempotent and allocation-free on subsequent calls (e.g., {@link #reset()} after {@link
   * #start()}): the projection index already contains all projections, the per-projection array is
   * reused, and the dispatch indices are skipped (already populated).
   *
   * <p>The volatile store of {@link #lastSeqByIndex} is the publication fence for cross-thread
   * reads: any monitoring thread that sees {@code lastSeqByIndex != null} is guaranteed (via JMM
   * happens-before) to see the fully populated {@link #projectionIndex} and {@link
   * #dispatchIndices}.
   *
   * <p>Uses Agrona's flyweight {@link Int2ObjectHashMap} EntryIterator — methods are called on the
   * iterator itself (not on the {@code Map.Entry} returned by {@code next()}). This is the standard
   * Agrona idiom for zero-allocation iteration.
   */
  private void seedLastSeqMap() {
    int idx = projectionIndex.size();
    for (final Projection[] handlers : dispatchTable.values()) {
      for (final Projection handler : handlers) {
        if (!projectionIndex.containsKey(handler)) {
          projectionIndex.put(handler, idx++);
        }
      }
    }
    // Allocate per-projection array ONCE. On subsequent calls (reset() after start()),
    // projectionIndex already contains all projections and the array is reused.
    if (lastSeqByIndex == null) {
      lastSeqByIndex = new long[idx]; // volatile store — publication fence
    }
    // Build pre-computed index arrays parallel to dispatchTable ONCE. On subsequent calls
    // (reset() after start()), dispatchIndices is already populated with identical values —
    // skip the rebuild to avoid allocating new int[] arrays needlessly.
    if (dispatchIndices.isEmpty()) {
      final var it = dispatchTable.entrySet().iterator();
      while (it.hasNext()) {
        it.next();
        final int eventType = it.getIntKey();
        final Projection[] handlers = it.getValue();
        final int[] indices = new int[handlers.length];
        for (int j = 0; j < handlers.length; j++) {
          indices[j] = projectionIndex.getValue(handlers[j]);
        }
        dispatchIndices.put(eventType, indices);
      }
    }
  }
}
