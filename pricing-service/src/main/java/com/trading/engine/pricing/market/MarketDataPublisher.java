package com.trading.engine.pricing.market;

import static io.aeron.Publication.ADMIN_ACTION;
import static io.aeron.Publication.BACK_PRESSURED;
import static io.aeron.Publication.CLOSED;
import static io.aeron.Publication.MAX_POSITION_EXCEEDED;
import static io.aeron.Publication.NOT_CONNECTED;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.trading.engine.messages.sbe.MarketDataHeartbeatEncoder;
import com.trading.engine.messages.sbe.MarketDataTickEncoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.telemetry.MarketDataTickPublished;
import com.trading.engine.messages.telemetry.MarketDataTickRejected;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import java.util.Objects;
import java.util.function.LongFunction;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.LongObjConsumer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Phase 3 market-data broadcast publisher. Owns the {@link MarketDataTickEncoder} and {@link
 * MarketDataHeartbeatEncoder}, conflation map keyed by packed-symbol {@code long}, and the
 * five-case Aeron offer return-code handling.
 *
 * <p><b>Threading model.</b> Single-writer. All public entry points ({@code onTick}, {@code
 * doWork}, {@code snapshotForSymbol}) MUST be called from the pricing-service agent thread. A
 * runtime guard records the first-touch thread on {@code onStart} and an assertion inside {@code
 * onTick} fires if a future refactor accidentally introduces cross-thread invocation; the {@code
 * MarketDataPublisherSingleWriterJCStress} test asserts the guard activates. No fences, no
 * synchronisation — the agent thread is the sole writer and reader.
 *
 * <p><b>Allocation.</b> Zero after construction and first-tick-per-symbol slot creation. The
 * per-symbol slot is created exactly once at first sight via a {@code final}-field {@code
 * LongFunction<MarketDataTickSlot>} factory (NOT a per-call lambda — that would allocate a SAM on
 * every {@code computeIfAbsent} call in steady state). Subsequent ticks mutate the slot in place.
 * The drain consumer is also a {@code final} field, bound to a method reference once at
 * construction. Encoders + scratch buffer are pre-allocated.
 *
 * <p><b>Design rationale.</b>
 *
 * <ul>
 *   <li><b>5 ms drain cadence + per-symbol conflation.</b> CME MDP 3.0 / EBS Direct publish-side
 *       pattern: a 1 000 Hz mid-rate source compresses ~5× on the wire while always representing
 *       the latest top-of-book. See {@link
 *       com.trading.engine.messages.MarketDataConstants#MARKET_DATA_PUBLISH_CADENCE_MICROS} Javadoc
 *       for the three-point tuning rationale.
 *   <li><b>Heartbeat ±10 % jitter, per-process seed.</b> LMAX exchange-core anti-thundering-herd
 *       discipline: {@code seed = epochNanoClock.nanoTime() ^ ProcessHandle.current().pid()}
 *       reproducible within a JVM but divergent across processes. A constant seed (e.g. {@code 0L})
 *       shared across publishers would align heartbeats globally and defeat the purpose.
 *   <li><b>Five-case offer return-code handling.</b> Per the Aeron contract:
 *       <ul>
 *         <li>{@code >= 0}: published, counter increments, reset rate-limit slot.
 *         <li>{@link io.aeron.Publication#BACK_PRESSURED}: retry ONCE on the same drain; on second
 *             failure drop and increment {@link RejectReason#BACK_PRESSURED}. The tick remains in
 *             the slot — next drain re-publishes the latest conflated top-of-book.
 *         <li>{@link io.aeron.Publication#NOT_CONNECTED}: drop + rate-limited INFO; expected during
 *             cluster startup.
 *         <li>{@link io.aeron.Publication#ADMIN_ACTION}: drop + rate-limited INFO; transient (Aeron
 *             archive rolling).
 *         <li>{@link io.aeron.Publication#MAX_POSITION_EXCEEDED}: rate-limited WARN with
 *             publication-position context; pathological backlog signal.
 *         <li>{@link io.aeron.Publication#CLOSED}: fatal; agent shutdown.
 *       </ul>
 *   <li><b>Sanity rejects.</b> {@code bid >= ask} → {@link RejectReason#CROSSED}; {@code bid <= 0
 *       || ask <= 0} → {@link RejectReason#NON_POSITIVE}; symbol not in {@code configuredSymbols} →
 *       {@link RejectReason#UNCONFIGURED}. Drop + counter + rate-limited log before the Aeron
 *       offer.
 *   <li><b>Snapshot API.</b> {@link #snapshotForSymbol(long)} re-emits the cached slot with {@code
 *       symbolSeq = 0} (the snapshot sentinel — browsers reset their per-symbol gap tracker on
 *       seq=0). The publisher's per-session rate-limit token bucket lives on {@code
 *       WebSocketSession} (Phase 3 Commit 5); the publisher trusts that the websocket- server has
 *       admitted the request before it arrives on stream 205.
 * </ul>
 *
 * <p><b>Dependencies.</b> {@link BroadcastPublisher} seam over Aeron's {@code ExclusivePublication}
 * on stream 204 (publish) — the seam exists because Aeron's {@code ExclusivePublication} is {@code
 * final} and cannot be subclassed for unit-test isolation; the launcher binds method references
 * against the real publication at startup. {@link Subscription} on stream 205 (snapshot requests);
 * injected {@link EpochNanoClock} (FIX tag-52 {@code SendingTime} wall-clock); {@link NanoClock}
 * for monotonic drain-cadence checks; {@link MarketDataPublisherConfig} for cadence / heartbeat
 * overrides.
 *
 * @see com.trading.engine.messages.MarketDataConstants
 * @see MarketDataTickSlot
 * @see RejectReason
 */
public final class MarketDataPublisher implements Agent {

  private static final Log LOG = LogFactory.getLog(MarketDataPublisher.class);

  /** SBE message-header length in bytes. Same constant the codec emits inline. */
  private static final int HDR_LEN = MessageHeaderEncoder.ENCODED_LENGTH;

  /** Aeron back-pressure retry budget per drain — single retry per the Phase 3 plan. */
  private static final int BACK_PRESSURE_RETRY_BUDGET = 1;

  /** Pre-warm size of the conflation map; 16 buckets fits 4-major FX cohort with headroom. */
  private static final int INITIAL_SLOT_CAPACITY = 16;

  /** Scratch buffer size: header + max body (tick=72 / heartbeat=any reasonable group). */
  private static final int SCRATCH_BUFFER_BYTES = 4_096;

  /** Rate-limit interval — at most one log entry per reason per second on the hot path. */
  private static final long RATE_LIMIT_NANOS = 1_000_000_000L;

  private final BroadcastPublisher publication;
  private final Subscription snapshotRequestSubscription;
  private final EpochNanoClock epochNanoClock;
  private final NanoClock nanoClock;
  private final long cadenceNanos;
  private final long heartbeatBaseNanos;

  /** Conflation map keyed by 8-byte symbol packed into a long. */
  private final Long2ObjectHashMap<MarketDataTickSlot> slots =
      new Long2ObjectHashMap<>(INITIAL_SLOT_CAPACITY, 0.55f);

  /**
   * Slot factory bound once at construction. {@link Long2ObjectHashMap#computeIfAbsent} accepts a
   * {@code LongFunction}; passing a method reference to a {@code final} field avoids the per-call
   * lambda allocation that would otherwise happen in the steady-state branch.
   */
  private final LongFunction<MarketDataTickSlot> slotFactory = this::newSlot;

  /**
   * Drain consumer bound once at construction. Same allocation rationale as {@link #slotFactory}.
   */
  private final LongObjConsumer<MarketDataTickSlot> drainConsumer = this::publishOneSlot;

  /**
   * Heartbeat-group consumer bound once at construction. {@link Long2ObjectHashMap#forEachLong}
   * inside {@code publishHeartbeat} would otherwise need a per-emit capturing lambda (binding the
   * local {@code groupEncoder}); pre-binding to a method reference + storing the active group
   * encoder in {@link #activeGroupEncoder} keeps the heartbeat path zero-alloc as well.
   */
  private final LongObjConsumer<MarketDataTickSlot> heartbeatConsumer =
      this::encodeHeartbeatGroupEntry;

  /** Snapshot-request handler bound once at construction — single SAM allocation. */
  private final FragmentHandler snapshotRequestHandler = this::onSnapshotRequest;

  private final MarketDataTickEncoder tickEncoder = new MarketDataTickEncoder();
  private final MarketDataHeartbeatEncoder heartbeatEncoder = new MarketDataHeartbeatEncoder();
  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  private final UnsafeBuffer scratch = new UnsafeBuffer(new byte[SCRATCH_BUFFER_BYTES]);

  /**
   * Active heartbeat group-encoder slot. Stashed by {@link #publishHeartbeat} before invoking
   * {@link Long2ObjectHashMap#forEachLong} so the pre-bound {@link #heartbeatConsumer} method
   * reference can read it without capturing a local — keeping the heartbeat path zero-alloc. The
   * single-writer agent thread is the sole reader/writer of this field; no synchronisation.
   */
  private MarketDataHeartbeatEncoder.LastPublishedSeqEncoder activeGroupEncoder;

  /**
   * Rate-limit log timestamps, indexed by {@link RejectReason#ordinal()}. Primitive {@code long[]}
   * avoids the {@code Map<String, Long>} boxing + put-allocation.
   */
  private final long[] lastLogNs = new long[RejectReason.COUNT];

  /** Per-reason drop counters. */
  private final long[] droppedByReason = new long[RejectReason.COUNT];

  private long onTickCount;
  private long ticksPublished;
  private long heartbeatsPublished;

  /**
   * Cumulative on-demand snapshot publications successfully completed. Tracked SEPARATELY from
   * {@link #ticksPublished} so the Prometheus recording rule {@code marketdata_conflation_ratio =
   * onTick.count / ticks.published.count} is not biased downward by snapshot emissions (which are
   * not produced by an {@code onTick} call and therefore have no matching numerator). Exposed via
   * {@link #snapshotsPublished()}.
   */
  private long snapshotsPublished;

  /** Last drain wall-time in monotonic ns; used to gate the 5 ms cadence. */
  private long lastDrainNanos;

  /** Last heartbeat emit time in monotonic ns. */
  private long lastHeartbeatNanos;

  /**
   * Next heartbeat interval target in ns (base ± 10% jitter). Recomputed every emit so each
   * publisher's heartbeat trajectory desynchronises.
   */
  private long nextHeartbeatIntervalNanos;

  /**
   * XorShift64 seed for heartbeat jitter. Seeded {@code epochNanoClock.nanoTime() ^
   * ProcessHandle.current().pid()} at construction; mutated on every {@link #xorShift64Next()}.
   */
  private long jitterSeed;

  /**
   * Tracks how many symbols were touched between drain cycles. If zero on a drain tick AND a
   * heartbeat interval has elapsed, the heartbeat path fires instead of a no-op.
   */
  private int touchedSinceLastDrain;

  /**
   * Records the agent thread on first {@link #onStart}; an assertion inside {@link #onTick} fires
   * if a different thread calls into the publisher. The {@code
   * MarketDataPublisherSingleWriterJCStress} test asserts this guard activates.
   */
  private volatile Thread agentThread;

  /**
   * Stash used to propagate the packed symbol into {@link #drop(RejectReason)} when a rejection
   * occurs inside {@link #publishOneSlot(long, MarketDataTickSlot)}. Set before the Aeron offer
   * block and read by the JFR emit helper. The field is not {@code final} because it is reset to
   * {@code 0L} after each emit; the single-writer guarantee means no synchronisation is needed. Not
   * used in {@code onTick} input-validation rejects — those call {@link
   * #dropWithSymbol(RejectReason, long)} directly so the packedSymbol argument is in scope.
   */
  private long currentPublishPackedSymbol;

  /**
   * Constructs the publisher.
   *
   * @param publication outbound publication seam on Aeron stream 204; in production this is bound
   *     to {@code aeronExclusivePublication::offer} / {@code ::position} / {@code
   *     ::termBufferLength} method references at the launcher.
   * @param snapshotRequestSubscription inbound subscription on Aeron stream 205. May be {@code
   *     null} on unit-test paths that do not exercise the snapshot route.
   * @param epochNanoClock wall-clock source for FIX tag-52 {@code SendingTime}.
   * @param nanoClock monotonic-clock source for drain-cadence gating.
   * @param config runtime overrides for cadence + heartbeat base.
   */
  public MarketDataPublisher(
      final BroadcastPublisher publication,
      final Subscription snapshotRequestSubscription,
      final EpochNanoClock epochNanoClock,
      final NanoClock nanoClock,
      final MarketDataPublisherConfig config) {
    this.publication = Objects.requireNonNull(publication, "publication");
    this.snapshotRequestSubscription = snapshotRequestSubscription;
    this.epochNanoClock = Objects.requireNonNull(epochNanoClock, "epochNanoClock");
    this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
    this.cadenceNanos = config.cadenceMicros() * 1_000L;
    this.heartbeatBaseNanos = config.heartbeatBaseMs() * 1_000_000L;
  }

  /**
   * Called once at agent start. Records the current thread as the writer-thread invariant and seeds
   * the jitter PRNG. Cold path; allocation acceptable.
   */
  @Override
  public void onStart() {
    this.agentThread = Thread.currentThread();
    final long pid = ProcessHandle.current().pid();
    this.jitterSeed = epochNanoClock.nanoTime() ^ pid;
    if (this.jitterSeed == 0L) {
      // XorShift64 requires non-zero seed; fall back to a stable distinct value.
      this.jitterSeed = pid | 0x1L;
    }
    final long now = nanoClock.nanoTime();
    this.lastDrainNanos = now;
    this.lastHeartbeatNanos = now;
    this.nextHeartbeatIntervalNanos = computeNextHeartbeatInterval();
    LOG.info()
        .append("MarketDataPublisher started: cadenceNs=")
        .append(cadenceNanos)
        .append(" heartbeatBaseNs=")
        .append(heartbeatBaseNanos)
        .append(" pid=")
        .append(pid)
        .commit();
  }

  /** Agent role identifier — used by AgentRunner threads + error handlers for diagnostics. */
  @Override
  public String roleName() {
    return "market-data-publisher";
  }

  /** Called once at agent close. No external resources owned (caller closes the Aeron handles). */
  @Override
  public void onClose() {
    LOG.info()
        .append("MarketDataPublisher stopped: onTickCount=")
        .append(onTickCount)
        .append(" ticksPublished=")
        .append(ticksPublished)
        .append(" heartbeatsPublished=")
        .append(heartbeatsPublished)
        .append(" snapshotsPublished=")
        .append(snapshotsPublished)
        .commit();
  }

  /**
   * Receives a tick from an adapter running on the same agent thread. Updates (or creates) the
   * conflation slot; the actual wire publish happens on the next {@link #doWork} drain.
   *
   * @param packedSymbol the 8-byte symbol packed into a {@code long} (little-endian).
   * @param bid fixed-point 10^-8 bid price.
   * @param ask fixed-point 10^-8 ask price.
   * @param bidSize fixed-point 10^-8 bid quantity.
   * @param askSize fixed-point 10^-8 ask quantity.
   * @param ingressNanos epoch-nanos when the adapter sampled this rate (FIX tag-60).
   */
  public void onTick(
      final long packedSymbol,
      final long bid,
      final long ask,
      final long bidSize,
      final long askSize,
      final long ingressNanos) {
    assertAgentThread();
    onTickCount++;

    // Sanity rejects — drop before any allocation / publish.
    if (bid <= 0L || ask <= 0L) {
      dropWithSymbol(RejectReason.NON_POSITIVE, packedSymbol);
      return;
    }
    if (bid >= ask) {
      dropWithSymbol(RejectReason.CROSSED, packedSymbol);
      return;
    }

    final MarketDataTickSlot slot = slots.computeIfAbsent(packedSymbol, slotFactory);
    slot.set(bid, ask, bidSize, askSize, ingressNanos);
    touchedSinceLastDrain++;
  }

  /**
   * Agent duty-cycle entry. Drains conflated slots if the 5 ms cadence has elapsed; emits a
   * heartbeat if the symbol-touch counter is zero and the heartbeat interval has elapsed; polls the
   * snapshot-request subscription regardless.
   *
   * @return positive work units when a drain or heartbeat happened, {@code 0} otherwise — drives
   *     the {@code BackoffIdleStrategy} so the agent parks when idle.
   */
  @Override
  public int doWork() {
    int workUnits = 0;
    final long now = nanoClock.nanoTime();

    if (snapshotRequestSubscription != null) {
      workUnits += snapshotRequestSubscription.poll(snapshotRequestHandler, 8);
    }

    if (now - lastDrainNanos >= cadenceNanos) {
      if (touchedSinceLastDrain > 0) {
        workUnits += drainSlots();
        touchedSinceLastDrain = 0;
        lastDrainNanos = now;
        // A drain counts as activity; defer the heartbeat clock so a healthy publisher
        // is never seen as quiet by the ws-server liveness tracker.
        lastHeartbeatNanos = now;
      } else if (now - lastHeartbeatNanos >= nextHeartbeatIntervalNanos) {
        publishHeartbeat(now);
        lastHeartbeatNanos = now;
        lastDrainNanos = now;
        this.nextHeartbeatIntervalNanos = computeNextHeartbeatInterval();
        workUnits++;
      } else {
        lastDrainNanos = now;
      }
    }
    return workUnits;
  }

  /**
   * Emits a snapshot for one symbol on demand. Looks up the cached slot and re-publishes its
   * top-of-book with {@code symbolSeq = 0} (snapshot sentinel). The browser worker resets its
   * per-symbol gap-tracker {@code lastSeq} on {@code symbolSeq === 0} so the very next live tick at
   * {@code symbolSeq === 1} does not falsely register a gap.
   *
   * @param packedSymbol the 8-byte symbol packed into a {@code long}.
   * @return {@code true} if the symbol had a cached slot and a snapshot was emitted; {@code false}
   *     if the symbol is unknown (the websocket-server handles unknown-symbol upstream and never
   *     forwards in production, but the publisher returns false defensively).
   */
  public boolean snapshotForSymbol(final long packedSymbol) {
    assertAgentThread();
    final MarketDataTickSlot slot = slots.get(packedSymbol);
    if (slot == null) {
      dropWithSymbol(RejectReason.UNCONFIGURED, packedSymbol);
      return false;
    }
    // Stash + restore the live symbolSeq. publishOneSlot pre-increments slot.symbolSeq before
    // encoding; setting the stash to -1 means the wire value is -1 + 1 = 0 (snapshot sentinel).
    // The restore puts the live seq back so the next normal drain continues from savedSeq + 1.
    // try/finally guarantees the restore even if publishOneSlot throws (CLOSED →
    // IllegalStateException);
    // without it, the slot would be left at symbolSeq = -1L and the next live drain would emit a
    // false snapshot sentinel (wire seq = 0) instead of resuming from savedSeq + 1.
    final long savedSeq = slot.symbolSeq;
    slot.symbolSeq = -1L;
    try {
      publishOneSlotInternal(packedSymbol, slot, true);
    } finally {
      slot.symbolSeq = savedSeq;
    }
    return true;
  }

  /**
   * Total {@code onTick} invocations since start, including rejected ticks. Used by Prometheus
   * recording rule {@code marketdata_conflation_ratio = onTick.count / ticks.published.count}.
   *
   * @return monotonic counter.
   */
  public long onTickCount() {
    return onTickCount;
  }

  /**
   * @return cumulative tick publications successfully completed.
   */
  public long ticksPublished() {
    return ticksPublished;
  }

  /**
   * @return cumulative heartbeat emissions.
   */
  public long heartbeatsPublished() {
    return heartbeatsPublished;
  }

  /**
   * Cumulative successful on-demand snapshot publications (i.e. {@link #snapshotForSymbol(long)}
   * calls that succeeded on the Aeron offer). Tracked separately from {@link #ticksPublished()} so
   * the Prometheus rule {@code marketdata_conflation_ratio = onTick.count / ticks.published.count}
   * is not biased downward by snapshot emissions, which are not triggered by an {@code onTick}
   * call.
   *
   * @return monotonic counter.
   */
  public long snapshotsPublished() {
    return snapshotsPublished;
  }

  /**
   * @param reason the categorical drop reason.
   * @return cumulative drops for that reason.
   */
  public long droppedCount(final RejectReason reason) {
    return droppedByReason[reason.ordinal()];
  }

  // ─── internals ────────────────────────────────────────────────────────────

  private MarketDataTickSlot newSlot(final long packedSymbolUnused) {
    return new MarketDataTickSlot();
  }

  private int drainSlots() {
    // Hold the publish counter as `long` to avoid the silent int-truncation that would wrap at
    // ~2.1 B publishes (24 days at 1 k/s). Per-drain delta is always small (≤ #symbols), so the
    // narrowing cast on return is safe — Math.min caps at Integer.MAX_VALUE for forensic safety.
    final long countBefore = ticksPublished;
    slots.forEachLong(drainConsumer);
    final long delta = ticksPublished - countBefore;
    return (int) Math.min(delta, Integer.MAX_VALUE);
  }

  /**
   * Drain-path entry: publishes one slot. {@link Long2ObjectHashMap#forEachLong} invokes this
   * consumer once per registered entry; the consumer SAM is the {@code final}-field {@link
   * #drainConsumer}.
   *
   * <p>Delegates to {@link #publishOneSlotInternal(long, MarketDataTickSlot, boolean)} with the
   * snapshot flag set to {@code false} so the publish increments {@link #ticksPublished} (used in
   * the {@code marketdata_conflation_ratio} Prometheus rule). The snapshot path goes through the
   * same internal method with the flag set to {@code true} and increments {@link
   * #snapshotsPublished} instead — keeping the {@code onTick}/{@code ticks.published} ratio
   * undistorted by on-demand snapshot emissions.
   *
   * @param packedSymbol the map key.
   * @param slot the per-symbol conflation slot.
   */
  private void publishOneSlot(final long packedSymbol, final MarketDataTickSlot slot) {
    publishOneSlotInternal(packedSymbol, slot, false);
  }

  /**
   * Encodes and publishes a single slot, accounting the success on the correct counter: {@link
   * #ticksPublished} for drain-path publishes, {@link #snapshotsPublished} for on-demand snapshot
   * publishes. All Aeron return-code branches (back-pressure retry, NOT_CONNECTED, ADMIN_ACTION,
   * MAX_POSITION_EXCEEDED, CLOSED) behave identically regardless of the flag.
   *
   * @param packedSymbol the map key.
   * @param slot the per-symbol conflation slot.
   * @param isSnapshot {@code true} when invoked from {@link #snapshotForSymbol(long)}; {@code
   *     false} when invoked from the drain path.
   */
  private void publishOneSlotInternal(
      final long packedSymbol, final MarketDataTickSlot slot, final boolean isSnapshot) {
    slot.symbolSeq++;
    final long serverNanos = epochNanoClock.nanoTime();
    final long ingressNanos = slot.ingressNanos;
    final int encodedLen = encodeTick(packedSymbol, slot, serverNanos);

    // Stash packed symbol so dropWithSymbol() can forward it to the JFR reject event
    // if the Aeron offer fails. Reset after the offer block (success or reject).
    currentPublishPackedSymbol = packedSymbol;

    long result = publication.offer(scratch, 0, encodedLen);
    if (result == BACK_PRESSURED) {
      for (int attempt = 0; attempt < BACK_PRESSURE_RETRY_BUDGET; attempt++) {
        result = publication.offer(scratch, 0, encodedLen);
        if (result != BACK_PRESSURED) {
          break;
        }
      }
    }

    if (result >= 0L) {
      if (isSnapshot) {
        snapshotsPublished++;
      } else {
        ticksPublished++;
      }
      currentPublishPackedSymbol = 0L;
      // JFR publish event — zero-alloc when shouldCommit() is false (outside the 100 ms window
      // or JFR not recording). The shouldCommit() guard short-circuits before any field write.
      final var jfrPublish = new MarketDataTickPublished();
      if (jfrPublish.shouldCommit()) {
        jfrPublish.symbol = unpackSymbol(packedSymbol);
        jfrPublish.symbolSeq = slot.symbolSeq;
        jfrPublish.publishLatencyNanos = serverNanos - ingressNanos;
        jfrPublish.commit();
      }
      return;
    }

    if (result == BACK_PRESSURED) {
      dropWithSymbol(RejectReason.BACK_PRESSURED, packedSymbol);
      currentPublishPackedSymbol = 0L;
      return;
    }
    if (result == NOT_CONNECTED) {
      dropWithSymbol(RejectReason.NOT_CONNECTED, packedSymbol);
      currentPublishPackedSymbol = 0L;
      return;
    }
    if (result == ADMIN_ACTION) {
      dropWithSymbol(RejectReason.ADMIN_ACTION, packedSymbol);
      currentPublishPackedSymbol = 0L;
      return;
    }
    if (result == MAX_POSITION_EXCEEDED) {
      LOG.warn()
          .append("MarketDataPublisher MAX_POSITION_EXCEEDED: position=")
          .append(publication.position())
          .append(" termLength=")
          .append(publication.termBufferLength())
          .commit();
      dropWithSymbol(RejectReason.MAX_POSITION_EXCEEDED, packedSymbol);
      currentPublishPackedSymbol = 0L;
      return;
    }
    if (result == CLOSED) {
      currentPublishPackedSymbol = 0L;
      LOG.error()
          .append("MarketDataPublisher publication CLOSED — fatal, agent will terminate")
          .commit();
      throw new IllegalStateException("publication CLOSED");
    }
    // Unknown negative return — treat as transient drop for forensic safety.
    dropWithSymbol(RejectReason.ADMIN_ACTION, packedSymbol);
    currentPublishPackedSymbol = 0L;
  }

  private int encodeTick(
      final long packedSymbol, final MarketDataTickSlot slot, final long serverNanos) {
    tickEncoder.wrapAndApplyHeader(scratch, 0, headerEncoder);
    // Unpack packedSymbol back into the 8-byte symbol field little-endian.
    for (int i = 0; i < 8; i++) {
      tickEncoder.symbol(i, (byte) ((packedSymbol >>> (i * 8)) & 0xFFL));
    }
    tickEncoder.bidPrice(slot.bidPrice);
    tickEncoder.askPrice(slot.askPrice);
    tickEncoder.bidSize(slot.bidSize);
    tickEncoder.askSize(slot.askSize);
    tickEncoder.symbolSeq(slot.symbolSeq);
    tickEncoder.ingressNanos(slot.ingressNanos);
    tickEncoder.serverNanos(serverNanos);
    return HDR_LEN + tickEncoder.encodedLength();
  }

  private void publishHeartbeat(final long monotonicNow) {
    final long serverNanos = epochNanoClock.nanoTime();
    heartbeatEncoder.wrapAndApplyHeader(scratch, 0, headerEncoder);
    heartbeatEncoder.serverNanos(serverNanos);
    // Heartbeat carries per-symbol last-published seq for CME MDP 3.0 gap-attribution.
    // The group encoder is stashed into a field so the pre-bound `heartbeatConsumer`
    // (final-field method reference) can read it without a per-emit capturing lambda.
    this.activeGroupEncoder = heartbeatEncoder.lastPublishedSeqCount(slots.size());
    try {
      slots.forEachLong(heartbeatConsumer);
    } finally {
      this.activeGroupEncoder = null;
    }
    final int len = HDR_LEN + heartbeatEncoder.encodedLength();
    final long result = publication.offer(scratch, 0, len);
    if (result >= 0L) {
      heartbeatsPublished++;
    } else if (result == CLOSED) {
      throw new IllegalStateException("publication CLOSED");
    } else {
      // Heartbeat drops are best-effort; rate-limit log only.
      maybeLog(monotonicNow, mapOfferReturnToReason(result));
    }
  }

  /**
   * Group-encoder callback invoked by the pre-bound {@link #heartbeatConsumer}. Reads the stashed
   * {@link #activeGroupEncoder} field (set by {@link #publishHeartbeat} immediately before the
   * {@link Long2ObjectHashMap#forEachLong} call) — keeps the heartbeat path zero-alloc by avoiding
   * a capturing lambda.
   */
  private void encodeHeartbeatGroupEntry(final long packedSymbol, final MarketDataTickSlot slot) {
    final MarketDataHeartbeatEncoder.LastPublishedSeqEncoder enc = this.activeGroupEncoder;
    enc.next();
    for (int i = 0; i < 8; i++) {
      enc.symbol(i, (byte) ((packedSymbol >>> (i * 8)) & 0xFFL));
    }
    enc.seq(slot.symbolSeq);
  }

  private void onSnapshotRequest(
      final DirectBuffer buffer, final int offset, final int length, final Header header) {
    if (length < HDR_LEN) {
      return;
    }
    // Body offset 0 is the symbol char[8]; pack into a long.
    long packed = 0L;
    for (int i = 0; i < 8 && (HDR_LEN + i) < length; i++) {
      packed |= ((long) (buffer.getByte(offset + HDR_LEN + i) & 0xFF)) << (i * 8);
    }
    snapshotForSymbol(packed);
  }

  /**
   * Records a drop for the given reason, rate-limits the log entry, and emits a {@link
   * MarketDataTickRejected} JFR event with an empty symbol string. Used for Aeron-level rejects
   * inside {@code publishOneSlot} where {@link #dropWithSymbol(RejectReason, long)} is preferred
   * (this overload is retained only for the {@code heartbeat} path which has no symbol context).
   *
   * @param reason categorical drop reason.
   */
  private void drop(final RejectReason reason) {
    droppedByReason[reason.ordinal()]++;
    maybeLog(nanoClock.nanoTime(), reason);
    emitRejectJfrEvent(reason, "");
  }

  /**
   * Records a drop for the given reason, rate-limits the log entry, and emits a {@link
   * MarketDataTickRejected} JFR event with the unpacked symbol string.
   *
   * <p>Zero allocation on the hot path when JFR is not recording: the {@code shouldCommit()} guard
   * short-circuits before {@link #unpackSymbol(long)} is called. The symbol string is only
   * allocated when JFR is actively recording.
   *
   * @param reason categorical drop reason.
   * @param packedSymbol the 8-byte symbol packed into a {@code long}.
   */
  private void dropWithSymbol(final RejectReason reason, final long packedSymbol) {
    droppedByReason[reason.ordinal()]++;
    maybeLog(nanoClock.nanoTime(), reason);
    emitRejectJfrEvent(reason, packedSymbol);
  }

  /**
   * Emits a {@link MarketDataTickRejected} JFR event. When JFR is not recording, {@code
   * shouldCommit()} returns {@code false} before any field write — zero allocation on the hot path.
   * On the recording path, {@link #unpackSymbol(long)} allocates a short {@code String}; this is
   * acceptable because rejects are pathological and the recording path is the diagnostic case.
   *
   * @param reason categorical drop reason.
   * @param packedSymbol the 8-byte symbol packed into a {@code long} (little-endian).
   */
  private void emitRejectJfrEvent(final RejectReason reason, final long packedSymbol) {
    final var e = new MarketDataTickRejected();
    if (e.shouldCommit()) {
      e.reasonOrdinal = reason.ordinal();
      e.symbol = unpackSymbol(packedSymbol);
      e.commit();
    }
  }

  /**
   * Emits a {@link MarketDataTickRejected} JFR event with a pre-resolved symbol string. Used when
   * the symbol string is already available (e.g. from the heartbeat path) to avoid
   * double-unpacking.
   *
   * @param reason categorical drop reason.
   * @param symbolStr pre-resolved symbol string; empty string {@code ""} when unknown.
   */
  private void emitRejectJfrEvent(final RejectReason reason, final String symbolStr) {
    final var e = new MarketDataTickRejected();
    if (e.shouldCommit()) {
      e.reasonOrdinal = reason.ordinal();
      e.symbol = symbolStr;
      e.commit();
    }
  }

  /**
   * Unpacks a little-endian 8-byte packed symbol {@code long} into its ASCII string representation.
   * Trailing space padding ({@code 0x20}) and null bytes ({@code 0x00}) are stripped. Allocates a
   * new {@code String} on every call — callers MUST guard behind {@code shouldCommit()} or the
   * equivalent to prevent allocation on the zero-alloc hot path.
   *
   * @param packedSymbol the 8-byte symbol packed little-endian into a {@code long}.
   * @return the trimmed ASCII symbol string, e.g. {@code "EURUSD"}.
   */
  private static String unpackSymbol(final long packedSymbol) {
    final byte[] bytes = new byte[8];
    for (int i = 0; i < 8; i++) {
      bytes[i] = (byte) ((packedSymbol >>> (i * 8)) & 0xFFL);
    }
    int len = 8;
    while (len > 0 && (bytes[len - 1] == (byte) ' ' || bytes[len - 1] == 0)) {
      len--;
    }
    return new String(bytes, 0, len, java.nio.charset.StandardCharsets.US_ASCII);
  }

  private void maybeLog(final long monotonicNow, final RejectReason reason) {
    final int idx = reason.ordinal();
    if (monotonicNow - lastLogNs[idx] >= RATE_LIMIT_NANOS) {
      lastLogNs[idx] = monotonicNow;
      LOG.info()
          .append("MarketDataPublisher drop reason=")
          .append(reason.name())
          .append(" count=")
          .append(droppedByReason[idx])
          .commit();
    }
  }

  private RejectReason mapOfferReturnToReason(final long result) {
    if (result == BACK_PRESSURED) return RejectReason.BACK_PRESSURED;
    if (result == NOT_CONNECTED) return RejectReason.NOT_CONNECTED;
    if (result == ADMIN_ACTION) return RejectReason.ADMIN_ACTION;
    if (result == MAX_POSITION_EXCEEDED) return RejectReason.MAX_POSITION_EXCEEDED;
    return RejectReason.ADMIN_ACTION; // forensic-safe default
  }

  private long computeNextHeartbeatInterval() {
    // ±10 % jitter: pick a fraction in [0.9, 1.1) of heartbeatBaseNanos.
    final long r = xorShift64Next();
    // Map r to a value in [0, 2_000) representing 0.000 to 0.199 — then subtract 0.100 to centre.
    final long jitterUnits = (r >>> 1) % 2000L; // unsigned-safe range
    final long jitterPpm = (jitterUnits - 1000L) * 100L; // ±100_000 ppm = ±10 %
    return heartbeatBaseNanos + (heartbeatBaseNanos * jitterPpm) / 1_000_000L;
  }

  private long xorShift64Next() {
    long s = jitterSeed;
    s ^= s << 13;
    s ^= s >>> 7;
    s ^= s << 17;
    jitterSeed = s;
    return s;
  }

  /**
   * Runtime guard for the single-writer invariant. The first call (from {@link #onStart}) sets
   * {@link #agentThread}; subsequent calls assert the current thread matches. A future refactor
   * that accidentally introduces cross-thread invocation fails here loudly rather than producing
   * undetected data corruption.
   */
  private void assertAgentThread() {
    final Thread expected = agentThread;
    if (expected != null && Thread.currentThread() != expected) {
      throw new IllegalStateException(
          "MarketDataPublisher invoked from a non-agent thread: expected="
              + expected.getName()
              + " actual="
              + Thread.currentThread().getName());
    }
  }
}
