package com.trading.engine.cluster;

import static io.aeron.Publication.ADMIN_ACTION;
import static io.aeron.Publication.BACK_PRESSURED;

import com.trading.engine.cluster.handler.CommandHandler;
import com.trading.engine.cluster.handler.EventSink;
import com.trading.engine.cluster.handler.HaltTradingCommandHandler;
import com.trading.engine.cluster.handler.NewOrderSingleHandler;
import com.trading.engine.cluster.handler.PriceResponseHandler;
import com.trading.engine.cluster.handler.QuoteRequestHandler;
import com.trading.engine.cluster.handler.ResumeTradingCommandHandler;
import com.trading.engine.cluster.journal.EventJournal;
import com.trading.engine.cluster.metrics.RfqMetrics;
import com.trading.engine.cluster.refdata.AccountStore;
import com.trading.engine.cluster.refdata.CurrencyStore;
import com.trading.engine.cluster.refdata.ReferenceDataRegistry;
import com.trading.engine.cluster.refdata.RiskLimitStore;
import com.trading.engine.cluster.state.RfqStateMachine;
import com.trading.engine.cluster.state.TradingState;
import com.trading.engine.messages.sbe.AccountSnapshotDecoder;
import com.trading.engine.messages.sbe.ClOrdIdDedupSnapshotDecoder;
import com.trading.engine.messages.sbe.CurrencySnapshotDecoder;
import com.trading.engine.messages.sbe.EventSequencerSnapshotDecoder;
import com.trading.engine.messages.sbe.EventSequencerSnapshotEncoder;
import com.trading.engine.messages.sbe.IdGeneratorSnapshotDecoder;
import com.trading.engine.messages.sbe.IdGeneratorSnapshotEncoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.OrderBookSnapshotDecoder;
import com.trading.engine.messages.sbe.RfqStateSnapshotDecoder;
import com.trading.engine.messages.sbe.RiskLimitSnapshotDecoder;
import com.trading.engine.messages.sbe.SnapshotTakenDecoder;
import com.trading.engine.messages.sbe.SnapshotTakenEncoder;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import java.util.Objects;
import java.util.zip.CRC32C;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * The keystone cluster service for Wave 4. Dispatches inbound commands to handler implementations
 * and manages snapshot encode/restore for the deterministic state machine.
 *
 * <p><b>Dual handler pattern (APP-176):</b>
 *
 * <ul>
 *   <li><b>Reference-data commands</b> (templateIds 11-16) are dispatched to the legacy {@link
 *       ReferenceDataRegistry} interface. The registry pre-stamps sequence numbers and timestamps
 *       in its event buffer; the service commits the sequence, journals the event, and offers it to
 *       the client session.
 *   <li><b>Trading commands</b> (e.g., {@code NewOrderSingle}) are dispatched via a {@link
 *       CommandHandler} map keyed by SBE template ID. Each handler decodes, validates, encodes
 *       domain events, and emits them through the {@link EventSink} pipeline (sequence stamping,
 *       journaling, session offer). State mutation is handled by the handler via {@link
 *       TradingState}.
 * </ul>
 *
 * <p>{@link #onTakeSnapshot(ExclusivePublication)} emits an eight-body-fragment envelope: {@code
 * SnapshotTaken} (header, templateId 200) + {@code EventSequencerSnapshot} (206) + {@code
 * IdGeneratorSnapshot} (205) + {@code AccountSnapshot} (201) + {@code CurrencySnapshot} (208) +
 * {@code RiskLimitSnapshot} (209) + {@code OrderBookSnapshot} (202) + {@code RfqStateSnapshot}
 * (203) + {@code ClOrdIdDedupSnapshot} (210). The header carries a CRC32C checksum covering the
 * concatenated body bytes in publish order; {@link #onStart} verifies the checksum before handing
 * control back to the cluster framework.
 *
 * <p><b>Determinism.</b> Every mutation is driven by either (a) a cluster-supplied message and
 * timestamp or (b) a cluster-supplied snapshot fragment. No wall-clock, no randomness, no heap
 * allocation on the hot path.
 *
 * <p><b>Threading.</b> Not thread-safe — single-threaded cluster duty cycle only.
 *
 * @see CommandHandler
 * @see EventSink
 * @see TradingState
 * @see ReferenceDataRegistry
 */
public final class TradingClusteredService implements ClusteredService {

  static final int NOT_HANDLED = -1;

  /**
   * Maximum number of times {@link #offerFragment} will retry a back-pressured offer before giving
   * up (throwing on the snapshot path). Bounded tightly so even Aeron's default {@code
   * BackoffIdleStrategy} — whose max park is ~1 ms — keeps the total retry wall-time (≈ 128 ms
   * worst case) well under a 500 ms cluster heartbeat budget, so a single slow snapshot can never
   * trigger spurious leader elections.
   */
  private static final int MAX_BACKPRESSURE_RETRY = 128;

  /**
   * Snapshot envelope version understood by this service. Bumped whenever the envelope layout
   * (fragment set, header fields, checksum algorithm) changes in a non-forward-compatible way.
   */
  private static final long SUPPORTED_SNAPSHOT_VERSION = 1L;

  /**
   * Number of body fragments in a well-formed snapshot envelope. Bumped from 6 to 7 by APP-232 to
   * include {@code RfqStateSnapshot} (template 203). Bumped from 7 to 8 by APP-206 R7 to include
   * {@code ClOrdIdDedupSnapshot} (template 210) — required so the 24h ClOrdID-uniqueness contract
   * survives snapshot+restore.
   */
  private static final int SNAPSHOT_STORE_COUNT = 8;

  /**
   * Maximum consecutive empty polls tolerated during snapshot reassembly in {@link #onStart} before
   * we give up and throw. Protects cluster startup from hanging indefinitely on a corrupted image
   * that never signals end-of-stream. Tuned generously (1M polls with the cluster idle strategy is
   * several seconds of wall time at worst) so we never trip on a normal snapshot.
   */
  private static final int MAX_SNAPSHOT_EMPTY_POLLS = 1_000_000;

  /**
   * Hard cap multiplier applied to the publication's {@code maxMessageLength}. The actual cap is
   * {@code SNAPSHOT_HARD_CAP_MULTIPLIER * maxMessageLength}, so it scales automatically when the
   * snapshot channel's term-length is increased (e.g., doubling from 128 MB to 256 MB). The Aeron
   * size guard ({@code maxMessageLength}) fires first under normal conditions; this cap catches
   * pathological state growth before {@code checkLimit()} attempts allocation.
   */
  static final int SNAPSHOT_HARD_CAP_MULTIPLIER = 2;

  // ===== Collaborators =====
  private final TradingState tradingState;
  private final EventSink eventSink;
  private final EventJournal eventJournal;
  private final AccountStore accountStore;
  private final CurrencyStore currencyStore;
  private final RiskLimitStore riskLimitStore;
  private final ReferenceDataRegistry referenceDataRegistry;
  private final RfqStateMachine rfqStateMachine;
  private final RfqMetrics rfqMetrics;

  /**
   * Direct reference to the registered {@link NewOrderSingleHandler} so the snapshot path can
   * delegate ClOrdID-dedup encode/restore (APP-206 R7) to it. The handler is also in {@link
   * #commandHandlers} for command dispatch; the explicit field avoids a lookup-and-cast on the
   * snapshot path and makes the dependency obvious to the static analysis.
   */
  private final NewOrderSingleHandler newOrderSingleHandler;

  private final QuoteRequestHandler quoteRequestHandler;
  private final PriceResponseHandler priceResponseHandler;
  private final Int2ObjectHashMap<CommandHandler> commandHandlers;

  // ===== Pre-allocated SBE flyweights (zero allocation on the hot path) =====
  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  // Dedicated decoder used exclusively by {@link #appendToJournal} so the inbound command's
  // header-wrap state on {@link #headerDecoder} is never clobbered mid-flow. The ref-data
  // dispatch path journals events locally (not through EventSink), so it needs its own decoder.
  private final MessageHeaderDecoder journalHeaderDecoder = new MessageHeaderDecoder();
  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();

  // Snapshot encoders / decoders.
  private final SnapshotTakenEncoder snapshotTakenEncoder = new SnapshotTakenEncoder();
  private final SnapshotTakenDecoder snapshotTakenDecoder = new SnapshotTakenDecoder();
  private final EventSequencerSnapshotEncoder eventSeqSnapEncoder =
      new EventSequencerSnapshotEncoder();
  private final EventSequencerSnapshotDecoder eventSeqSnapDecoder =
      new EventSequencerSnapshotDecoder();
  private final IdGeneratorSnapshotEncoder idGenSnapEncoder = new IdGeneratorSnapshotEncoder();
  private final IdGeneratorSnapshotDecoder idGenSnapDecoder = new IdGeneratorSnapshotDecoder();

  // ===== Pre-allocated scratch buffers =====
  // Ref-data event buffer used by the legacy ReferenceDataRegistry dispatch path.
  private final UnsafeBuffer refDataEventBuffer = new UnsafeBuffer(new byte[8 * 1024]);

  // ===== Snapshot staging buffers =====
  private final MutableDirectBuffer snapshotHeaderBuf = new ExpandableArrayBuffer(64);
  private final MutableDirectBuffer eventSeqSnapBuf = new ExpandableArrayBuffer(64);
  private final MutableDirectBuffer idGenSnapBuf = new ExpandableArrayBuffer(256);
  private final MutableDirectBuffer accountSnapBuf = new ExpandableArrayBuffer(64 * 1024);
  private final MutableDirectBuffer currencySnapBuf = new ExpandableArrayBuffer(8 * 1024);
  private final MutableDirectBuffer riskLimitSnapBuf = new ExpandableArrayBuffer(64 * 1024);
  private final MutableDirectBuffer orderBookSnapBuf = new ExpandableArrayBuffer(8 * 1024 * 1024);

  /** Snapshot 203 (RfqStateSnapshot) staging buffer. Sized for capacity 8192 × ~320 bytes/slot. */
  private final MutableDirectBuffer rfqStateSnapBuf = new ExpandableArrayBuffer(4 * 1024 * 1024);

  /**
   * Snapshot 210 (ClOrdIdDedupSnapshot) staging buffer (APP-206 R7). Sized for up to
   * CLORDID_DEDUP_MAX_SIZE (100K) entries × 16 bytes per entry + group / header overhead ≈ 1.6 MB;
   * 2 MB is a safe round-up that also covers any future header-field additions.
   */
  private final MutableDirectBuffer clOrdIdDedupSnapBuf =
      new ExpandableArrayBuffer(2 * 1024 * 1024);

  // Lengths populated by encodeSnapshotFragments().
  private int snapshotHeaderLen;
  private int eventSeqSnapLen;
  private int idGenSnapLen;
  private int accountSnapLen;
  private int currencySnapLen;
  private int riskLimitSnapLen;
  private int orderBookSnapLen;
  private int rfqStateSnapLen;
  private int clOrdIdDedupSnapLen;

  // Used by onStart() for snapshot image reassembly and by onTakeSnapshot() for atomic assembly
  // before publication. Dual use is safe: Aeron Cluster guarantees onStart() completes before
  // onTakeSnapshot() is invoked (leader lifecycle ordering).
  //
  // Read-side note: onStart() uses a plain FragmentHandler (not FragmentAssembler). When
  // onTakeSnapshot() publishes a message larger than maxPayloadLength, Aeron splits it into
  // term-level frames. The FragmentHandler receives each frame individually and the appender
  // lambda concatenates them. The result is byte-identical to what was passed to offer(). This
  // is correct by construction.
  private final MutableDirectBuffer snapshotReassemblyBuf =
      new ExpandableArrayBuffer(16 * 1024 * 1024);
  private int snapshotReassemblyOffset;

  private final CRC32C crc = new CRC32C();

  // Snapshot-walk bookkeeping — mutated only inside loadSnapshot / applySnapshotFragment.
  private boolean eventSeqFragmentSeen;
  private boolean idGenFragmentSeen;
  private boolean orderBookFragmentSeen;
  private boolean accountFragmentSeen;
  private boolean currencyFragmentSeen;
  private boolean riskLimitFragmentSeen;
  private boolean rfqStateFragmentSeen;
  private boolean clOrdIdDedupFragmentSeen;
  private boolean orderIdGenRestored;
  private boolean execIdGenRestored;
  private boolean quoteIdGenRestored;

  private Cluster cluster;

  /**
   * Creates a TradingClusteredService wired with the given state, event pipeline, and reference
   * data infrastructure.
   *
   * @param tradingState the event-sourced order lifecycle state (must not be null)
   * @param eventSink the centralized event emission pipeline (must not be null)
   * @param eventJournal the bounded event journal for projection catch-up (must not be null)
   * @param accountStore the account reference data store (must not be null)
   * @param currencyStore the currency reference data store (must not be null)
   * @param riskLimitStore the risk limit store (must not be null)
   * @param referenceDataRegistry the ref-data command dispatcher + snapshot registry (must not be
   *     null)
   */
  public TradingClusteredService(
      final TradingState tradingState,
      final EventSink eventSink,
      final EventJournal eventJournal,
      final AccountStore accountStore,
      final CurrencyStore currencyStore,
      final RiskLimitStore riskLimitStore,
      final ReferenceDataRegistry referenceDataRegistry,
      final RfqStateMachine rfqStateMachine,
      final RfqMetrics rfqMetrics) {
    this.tradingState = notNull(tradingState, "tradingState");
    this.eventSink = notNull(eventSink, "eventSink");
    this.eventJournal = notNull(eventJournal, "eventJournal");
    this.accountStore = notNull(accountStore, "accountStore");
    this.currencyStore = notNull(currencyStore, "currencyStore");
    this.riskLimitStore = notNull(riskLimitStore, "riskLimitStore");
    this.referenceDataRegistry = notNull(referenceDataRegistry, "referenceDataRegistry");
    this.rfqStateMachine = notNull(rfqStateMachine, "rfqStateMachine");
    this.rfqMetrics = notNull(rfqMetrics, "rfqMetrics");
    // Consistency check: the registry must be backed by the same concrete store instances we
    // hold a direct reference to. Otherwise NewOrderSingle validation would read from one
    // object graph while ref-data commands mutate another, or a snapshot restore could put the
    // registry and the direct references out of sync. Fail fast at construction time.
    requireSameStore(
        referenceDataRegistry, AccountStore.SNAPSHOT_TEMPLATE_ID, accountStore, "accountStore");
    requireSameStore(
        referenceDataRegistry, CurrencyStore.SNAPSHOT_TEMPLATE_ID, currencyStore, "currencyStore");
    requireSameStore(
        referenceDataRegistry,
        RiskLimitStore.SNAPSHOT_TEMPLATE_ID,
        riskLimitStore,
        "riskLimitStore");

    // Register trading command handlers. Each handler is keyed by its SBE template ID for
    // O(1) dispatch in onSessionMessage.
    this.commandHandlers = new Int2ObjectHashMap<>();
    this.newOrderSingleHandler =
        new NewOrderSingleHandler(tradingState, accountStore, currencyStore, riskLimitStore);
    this.newOrderSingleHandler.wireRfqStateMachine(rfqStateMachine, rfqMetrics);
    commandHandlers.put(newOrderSingleHandler.commandTemplateId(), newOrderSingleHandler);
    this.quoteRequestHandler =
        new QuoteRequestHandler(rfqStateMachine, accountStore, currencyStore, rfqMetrics);
    // APP-151 phase 5 — route per-session QuoteRequest activity into the cluster's per-session
    // metrics maps. The recorder lives on the NewOrderSingleHandler (which owns the close-time
    // GFLog summary path) so all four counters live in one place.
    this.quoteRequestHandler.setSessionMetricsRecorder(this.newOrderSingleHandler);
    commandHandlers.put(quoteRequestHandler.commandTemplateId(), quoteRequestHandler);
    this.priceResponseHandler =
        new PriceResponseHandler(rfqStateMachine, tradingState.quoteIdGen(), rfqMetrics);
    commandHandlers.put(priceResponseHandler.commandTemplateId(), priceResponseHandler);

    // APP-152 slice 2: admin halt/resume commands toggle the trading-halt circuit breaker that
    // gates NewOrderSingle in NewOrderSingleHandler Check 0. Two separate handlers because each
    // CommandHandler registers under a single SBE template ID.
    final var haltHandler = new HaltTradingCommandHandler(tradingState);
    commandHandlers.put(haltHandler.commandTemplateId(), haltHandler);
    final var resumeHandler = new ResumeTradingCommandHandler(tradingState);
    commandHandlers.put(resumeHandler.commandTemplateId(), resumeHandler);
  }

  private static void requireSameStore(
      final ReferenceDataRegistry registry,
      final int snapshotTemplateId,
      final Object expected,
      final String name) {
    final var registered = registry.storeForSnapshotTemplateId(snapshotTemplateId);
    if (registered != expected) {
      throw new IllegalArgumentException(
          name
              + " must be the same instance registered in the ReferenceDataRegistry (templateId "
              + snapshotTemplateId
              + ")");
    }
  }

  private static <T> T notNull(final T value, final String name) {
    if (value == null) {
      throw new NullPointerException(name + " must not be null");
    }
    return value;
  }

  // ===========================================================================
  // ClusteredService lifecycle
  // ===========================================================================

  @Override
  public void onStart(final Cluster cluster, final Image snapshotImage) {
    this.cluster = cluster;
    eventSink.setCluster(cluster);
    rfqStateMachine.setCluster(cluster);
    quoteRequestHandler.setCluster(cluster);
    priceResponseHandler.setCluster(cluster);
    // APP-151 phase 4 — defer the first idle-session-scan timer to the first session message.
    // Aeron Cluster forbids scheduling timers from onStart (it throws
    // ClusterException: "sending messages or scheduling timers is not allowed from onStart").
    // The lazy-schedule flag below is flipped on first onSessionMessage; subsequent scans
    // self-sustain via onTimerEvent rescheduling.
    idleScanScheduled = false;
    if (snapshotImage == null) {
      return;
    }
    // Reassemble the image into a single contiguous buffer, then walk it once through
    // loadSnapshot(). Aeron Cluster delivers snapshot fragments in the order they were offered,
    // so the concatenated bytes match the publish order.
    snapshotReassemblyOffset = 0;
    // Lambda assigned to a `var` cannot infer its functional-interface target — keep the
    // explicit FragmentHandler type here so the lambda signature resolves.
    final FragmentHandler appender =
        (final DirectBuffer buffer, final int offset, final int length, final Header header) -> {
          snapshotReassemblyBuf.putBytes(snapshotReassemblyOffset, buffer, offset, length);
          snapshotReassemblyOffset += length;
        };
    // Drain until Aeron signals end-of-stream. An empty poll is a transient "no fragments
    // queued yet" signal — idle the cluster's IdleStrategy and try again rather than exiting
    // early (which would leave a half-restored snapshot). Bound the empty-poll count so a
    // corrupted image that never signals end-of-stream fails startup loudly rather than
    // hanging the cluster duty cycle forever.
    int emptyPolls = 0;
    while (!snapshotImage.isEndOfStream()) {
      if (snapshotImage.poll(appender, Integer.MAX_VALUE) == 0) {
        if (++emptyPolls > MAX_SNAPSHOT_EMPTY_POLLS) {
          throw new IllegalStateException(
              "snapshot reassembly stalled after "
                  + MAX_SNAPSHOT_EMPTY_POLLS
                  + " consecutive empty polls — reassembled "
                  + snapshotReassemblyOffset
                  + " bytes so far; image may be corrupted or snapshot channel misconfigured");
        }
        cluster.idleStrategy().idle();
      } else {
        emptyPolls = 0;
      }
    }
    if (snapshotReassemblyOffset > 0) {
      loadSnapshot(snapshotReassemblyBuf, 0, snapshotReassemblyOffset);
      // Recovery sweep §9.4: re-arm timers / emit terminal events for in-flight RFQ slots.
      // The sweep MUST run unconditionally — silently skipping it would leave restored slots
      // with stale timer correlation IDs and no Aeron timer scheduled, breaking RFQ TTL
      // semantics post-restart. If the cluster context provides no errorHandler we fall back to
      // a no-op handler; recovery sweep failures (rare, e.g. timer pool exhausted at startup)
      // still produce SBE events but cannot escalate to operator alerts.
      final long currentTs = cluster.time();
      final var ctx = cluster.context();
      final var ctxErrorHandler = ctx != null ? ctx.errorHandler() : null;
      // Conditional with a lambda branch: var would erase to Object — keep ErrorHandler
      // so the lambda has a functional-interface target.
      final ErrorHandler errorHandler =
          ctxErrorHandler != null ? ctxErrorHandler : (final Throwable t) -> {};
      rfqStateMachine.onSnapshotRestored(currentTs, eventSink, errorHandler);
    }
  }

  @Override
  public void onSessionOpen(final ClientSession session, final long timestamp) {
    Objects.requireNonNull(session, "session");
    // APP-151 phase 1 — pre-allocate the per-session order tracker so the order-admit hot path
    // is guaranteed zero-allocation. APP-151 phase 4 — also seed the idle-activity clock so a
    // freshly-opened session is not immediately considered idle.
    newOrderSingleHandler.onSessionOpen(session.id(), timestamp);
    // Future: authenticate, wire session → trader / desk mapping.
  }

  @Override
  public void onSessionClose(
      final ClientSession session, final long timestamp, final CloseReason closeReason) {
    // Plan §7.6a — fast-fail in-flight RFQ slots from this session and release rate-limit bucket.
    // Aeron Cluster's contract guarantees a non-null session here, but defend explicitly to
    // surface any framework regression as a clear NPE rather than a silent swallow.
    Objects.requireNonNull(session, "session");
    rfqStateMachine.onSessionClose(session.id(), timestamp);
    // APP-151 phase 1 — cancel every outstanding order placed by this session and emit one
    // OrderCanceledEvent per cancellation. RFQ teardown emits its events first; OrderCanceledEvents
    // follow on the same EventSink sequencer in the same duty-cycle invocation, giving consumers
    // a deterministic stream order (RFQ events strictly precede the per-session OrderCanceled
    // events). The two handlers share no mutable state, so the relative ordering is a stylistic
    // choice — kept RFQ-first to match the pre-APP-151 sequence and avoid churning downstream
    // projections that may rely on the observed order.
    newOrderSingleHandler.onSessionClose(session.id(), timestamp, eventSink);
  }

  @Override
  public void onSessionMessage(
      final ClientSession session,
      final long timestamp,
      final DirectBuffer buffer,
      final int offset,
      final int length,
      final Header header) {
    // APP-151 phase 4 — every CLIENT-session command resets the idle-activity clock for its
    // source session. Cheap (one Long2LongHashMap.put on existing key); runs BEFORE dispatch so
    // even commands that fail validation still count as "session is alive". Timer events and
    // internal ingress (with null session) are NOT session-attributable and skip the refresh —
    // they have no business resetting any user session's idle clock.
    if (session != null) {
      newOrderSingleHandler.recordSessionActivity(session.id(), timestamp);
    }
    // APP-151 phase 4 — Aeron Cluster forbids scheduleTimer from onStart, so we lazy-bootstrap
    // the idle-scan chain on the first session message. The flag is flipped INSIDE
    // scheduleIdleScan only on a successful schedule, so a partial-wire path (cluster == null,
    // typically tests) does NOT permanently mark the bootstrap done.
    if (!idleScanScheduled) {
      scheduleIdleScan(timestamp);
    }
    headerDecoder.wrap(buffer, offset);
    final int templateId = headerDecoder.templateId();

    // 1. Ref-data dispatch (templateIds 11-16) — legacy ReferenceDataRegistry path.
    // Ref-data handlers pre-stamp seqNo+timestamp, so pass next sequence BEFORE dispatch.
    final long refDataSeqNo = eventSink.sequencer().currentSequence() + 1L;
    final int refDataEventLen =
        referenceDataRegistry.dispatchCommand(
            headerDecoder, buffer, offset, length, refDataEventBuffer, 0, refDataSeqNo, timestamp);
    if (refDataEventLen > 0) {
      eventSink.sequencer().nextSequence(); // commit
      appendToJournal(refDataSeqNo, refDataEventBuffer, 0, refDataEventLen);
      eventSink.offerToSession(session, refDataEventBuffer, 0, refDataEventLen);
      return;
    }
    // refDataEventLen == NOT_HANDLED → fall through to trading command dispatch.

    // 2. Trading command dispatch via CommandHandler map.
    final var handler = commandHandlers.get(templateId);
    if (handler != null) {
      handler.onCommand(
          session,
          timestamp,
          buffer,
          offset,
          length,
          headerDecoder.blockLength(),
          headerDecoder.version(),
          eventSink);
      return;
    }

    // 3. Unknown templateId — silently drop in Phase 1. A future PR will add structured logging
    // via GFLog.
  }

  @Override
  public void onTimerEvent(final long correlationId, final long timestamp) {
    if (correlationId == IDLE_SCAN_TIMER_CORRELATION_ID) {
      // APP-151 phase 4 — periodic idle-session scan. Cancel any session's outstanding orders
      // where last activity is older than IDLE_SESSION_TIMEOUT_NANOS, then reschedule for the
      // next interval.
      newOrderSingleHandler.onIdleScan(
          timestamp, NewOrderSingleHandler.IDLE_SESSION_TIMEOUT_NANOS, eventSink);
      scheduleIdleScan(timestamp);
      return;
    }
    rfqStateMachine.onTimerExpiry(correlationId, timestamp, eventSink);
  }

  /**
   * Correlation id for the APP-151 phase 4 idle-session-scan timer. Chosen as {@link
   * Long#MAX_VALUE} — provably disjoint from any correlation id the {@link
   * com.trading.engine.cluster.state.RfqStateMachine} can produce under any reachable {@code
   * (generation, poolIndex)} combination.
   *
   * <p><b>Why not {@link Long#MIN_VALUE}.</b> The original choice — {@code Long.MIN_VALUE} —
   * appeared disjoint from RFQ's positive TTL ids but COLLIDED with RFQ's request-timeout
   * namespace: {@code RfqStateMachine.REQUEST_TIMEOUT_NAMESPACE_BIT == 0x8000_0000_0000_0000L ==
   * Long.MIN_VALUE}, and {@code REQUEST_TIMEOUT_NAMESPACE_BIT | ttlCorrelationFor(slot)} reduces to
   * exactly {@code Long.MIN_VALUE} when the slot's TTL correlation is zero (slot 0, generation 0).
   * That silently routed the very first slot's request-timeout fire into the idle-scan handler.
   *
   * <p><b>Why not {@link Long#MAX_VALUE} either.</b> The intermediate fix tried {@code
   * Long.MAX_VALUE} (provably disjoint from every RFQ correlation id), but Aeron's internal {@code
   * WheelTimerService} stores correlation ids in an Agrona {@link
   * org.agrona.collections.Long2LongHashMap} whose {@code missingValue} sentinel is {@code
   * Long.MAX_VALUE}; calling {@code scheduleTimer(Long.MAX_VALUE, …)} throws {@code "cannot accept
   * missingValue"}.
   *
   * <p><b>Why {@code -1L} is safe.</b> The value is:
   *
   * <ul>
   *   <li>Above the entire RFQ request-timeout range. Request-timeout ids equal {@code
   *       0x8000_0000_0000_0000L | ttlCorrelation}; with the lower 63 bits bounded by the max TTL
   *       ({@code 0x3FFF_FFFF_FFFF_FFFFL}) the range tops out at {@code 0xBFFF_FFFF_FFFF_FFFFL} (≈
   *       {@code -4.6e18}). {@code -1L} = {@code 0xFFFF_FFFF_FFFF_FFFFL} is strictly greater, so
   *       disjoint.
   *   <li>Distinct from {@link Long#MIN_VALUE} (original colliding value) and {@link
   *       Long#MAX_VALUE} (Aeron sentinel collider).
   *   <li>Not in RFQ's TTL range (TTL ids are non-negative).
   * </ul>
   *
   * <p>Verified by {@code TradingClusteredServiceCorrelationIdTest}.
   */
  static final long IDLE_SCAN_TIMER_CORRELATION_ID = -1L;

  /**
   * Interval between idle-session scans (30 seconds). At 5-minute idle threshold, this gives
   * scan-resolution of ~10× the threshold, plenty fine-grained for a circuit-breaker timeout. The
   * scan itself is bounded by the number of open sessions (typically ≤ 4096) so total duty-cycle
   * cost per scan is sub-millisecond even at the cap.
   */
  static final long IDLE_SCAN_INTERVAL_NANOS = 30L * 1_000_000_000L;

  /**
   * Lazy-bootstrap flag for the idle-scan timer chain. Aeron Cluster forbids {@code scheduleTimer}
   * from {@link #onStart}; the first call is made from {@link #onSessionMessage} once the cluster
   * is fully started. Subsequent scans reschedule themselves from {@link #onTimerEvent}, so this
   * flag flips true on the first session message and stays true.
   */
  private boolean idleScanScheduled = false;

  /**
   * Schedule the next idle-session scan. Called once lazily from {@link #onSessionMessage} (the
   * first message after {@link #onStart}, because Aeron forbids {@code scheduleTimer} from onStart)
   * and again from {@link #onTimerEvent} after every scan fires.
   *
   * <p>Aeron's {@code scheduleTimer} returns {@code false} only during cluster shutdown — after
   * which the cluster duty cycle stops and no further session messages will arrive to retry the
   * schedule. Return value is intentionally ignored on that basis; logging it would never reach an
   * operator since the cluster is exiting anyway.
   *
   * @param baseTimestamp the cluster timestamp from which the next deadline is offset
   */
  private void scheduleIdleScan(final long baseTimestamp) {
    if (cluster == null) {
      return; // No cluster wired — typically a unit-test path; harmless.
    }
    cluster.scheduleTimer(IDLE_SCAN_TIMER_CORRELATION_ID, baseTimestamp + IDLE_SCAN_INTERVAL_NANOS);
    // Only mark scheduled AFTER cluster.scheduleTimer was actually invoked. If we set this in the
    // caller, a no-cluster path would silently mark bootstrap done forever — a real bug if the
    // cluster gets wired later (test-fixture order-of-operations) or if Aeron ever returns false
    // for a transient reason that we want to retry on the next onSessionMessage.
    idleScanScheduled = true;
  }

  @Override
  public void onTakeSnapshot(final ExclusivePublication snapshotPublication) {
    final int maxMsg =
        snapshotPublication != null ? snapshotPublication.maxMessageLength() : Integer.MAX_VALUE;
    final int assembledLen = assembleSnapshot(maxMsg);
    offerFragment(snapshotPublication, snapshotReassemblyBuf, assembledLen);
  }

  /**
   * Encode, validate, and assemble the full snapshot into {@link #snapshotReassemblyBuf}. Returns
   * the total assembled length. Package-private so tests can exercise the assembly + guard chain
   * without requiring a real (final) {@link ExclusivePublication}.
   *
   * <p>Guard chain (in order):
   *
   * <ol>
   *   <li>Hard cap — fail fast before allocation if {@code totalLen > SNAPSHOT_HARD_CAP_MULTIPLIER
   *       * maxMessageLength}
   *   <li>Pre-size — {@code checkLimit(totalLen)} to avoid incremental doubling
   *   <li>Assemble — {@code putBytes} all 7 fragments into contiguous buffer
   *   <li>Integrity — assert {@code pos == totalLen}
   *   <li>80% warning — surfaced via cluster error handler
   *   <li>Size guard — fail if {@code totalLen > maxMessageLength}
   * </ol>
   *
   * @param maxMessageLength the publication's max message length, or {@code Integer.MAX_VALUE} if
   *     no publication is available (test / null-publication path)
   * @return the total number of bytes assembled into {@code snapshotReassemblyBuf}
   * @throws IllegalStateException if any guard fails
   */
  int assembleSnapshot(final int maxMessageLength) {
    encodeSnapshotFragments(cluster == null ? 0L : cluster.time());

    // Pre-compute total assembled length across all fragments (header + 8 body). Use long
    // arithmetic so a pathological state-growth bug cannot wrap the sum negative and silently
    // bypass the hard cap.
    final long totalLenLong =
        (long) snapshotHeaderLen
            + eventSeqSnapLen
            + idGenSnapLen
            + accountSnapLen
            + currencySnapLen
            + riskLimitSnapLen
            + orderBookSnapLen
            + rfqStateSnapLen
            + clOrdIdDedupSnapLen;

    // Hard cap: fail fast before attempting allocation to protect against OOM from unbounded
    // state growth (e.g., order pool leak that never releases slots). Scales with maxMessageLength
    // so increasing the snapshot channel's term-length automatically raises the cap.
    final long hardCap = (long) SNAPSHOT_HARD_CAP_MULTIPLIER * maxMessageLength;
    if (totalLenLong > hardCap) {
      throw new IllegalStateException(
          "CRITICAL: snapshot assembly size ("
              + totalLenLong
              + " bytes) exceeds hard limit ("
              + hardCap
              + " bytes) — investigate state growth (order count: "
              + tradingState.orderBook().size()
              + ")");
    }
    // Guard against int overflow before narrowing — unreachable under normal conditions (order
    // pool capped at 65,534 entries ≈ 7 MB) but prevents silent corruption if the hard cap is
    // configured above Integer.MAX_VALUE (e.g., maxMessageLength == Integer.MAX_VALUE in tests).
    if (totalLenLong > Integer.MAX_VALUE) {
      throw new IllegalStateException(
          "CRITICAL: snapshot assembly size ("
              + totalLenLong
              + " bytes) exceeds Integer.MAX_VALUE — cannot be represented as an int offset");
    }
    final int totalLen = (int) totalLenLong;

    // Ensure the reassembly buffer is large enough in a single allocation (avoids multiple
    // doublings on the duty-cycle thread).
    snapshotReassemblyBuf.checkLimit(totalLen);

    // Assemble all 8 fragments into snapshotReassemblyBuf as one contiguous block.
    int pos = 0;
    snapshotReassemblyBuf.putBytes(pos, snapshotHeaderBuf, 0, snapshotHeaderLen);
    pos += snapshotHeaderLen;
    snapshotReassemblyBuf.putBytes(pos, eventSeqSnapBuf, 0, eventSeqSnapLen);
    pos += eventSeqSnapLen;
    snapshotReassemblyBuf.putBytes(pos, idGenSnapBuf, 0, idGenSnapLen);
    pos += idGenSnapLen;
    snapshotReassemblyBuf.putBytes(pos, accountSnapBuf, 0, accountSnapLen);
    pos += accountSnapLen;
    snapshotReassemblyBuf.putBytes(pos, currencySnapBuf, 0, currencySnapLen);
    pos += currencySnapLen;
    snapshotReassemblyBuf.putBytes(pos, riskLimitSnapBuf, 0, riskLimitSnapLen);
    pos += riskLimitSnapLen;
    snapshotReassemblyBuf.putBytes(pos, orderBookSnapBuf, 0, orderBookSnapLen);
    pos += orderBookSnapLen;
    snapshotReassemblyBuf.putBytes(pos, rfqStateSnapBuf, 0, rfqStateSnapLen);
    pos += rfqStateSnapLen;
    snapshotReassemblyBuf.putBytes(pos, clOrdIdDedupSnapBuf, 0, clOrdIdDedupSnapLen);
    pos += clOrdIdDedupSnapLen;

    // Post-assembly integrity: verify cursor matches pre-computed total.
    if (pos != totalLen) {
      throw new IllegalStateException(
          "snapshot assembly integrity failure: expected "
              + totalLen
              + " bytes but assembled "
              + pos
              + " — fragment length field corruption");
    }

    // Size guard: validate against Aeron's max message length for this publication.
    if (maxMessageLength < Integer.MAX_VALUE) {
      // Early warning at 80% — surfaced via Aeron error handler since the cluster module has
      // no logging dependency. Operators can proactively increase term-length before this
      // becomes a hard failure.
      final int warnThreshold = maxMessageLength - (maxMessageLength / 5);
      if (pos > warnThreshold && pos <= maxMessageLength) {
        final var ctx = cluster != null ? cluster.context() : null;
        final var handler = ctx != null ? ctx.errorHandler() : null;
        if (handler != null) {
          handler.onError(
              new IllegalStateException(
                  "WARNING: snapshot size ("
                      + pos
                      + " bytes) is at "
                      + (pos * 100L / maxMessageLength)
                      + "% of maxMessageLength ("
                      + maxMessageLength
                      + " bytes) — consider increasing snapshot channel term-length"
                      + " in ClusterNodeLauncher before this becomes a hard failure"));
        }
      }
      if (pos > maxMessageLength) {
        throw new IllegalStateException(
            "CRITICAL: snapshot assembly ("
                + pos
                + " bytes) exceeds maxMessageLength ("
                + maxMessageLength
                + " bytes). Cluster cannot persist snapshots — risk of data loss on restart."
                + " Increase term-length in ClusterNodeLauncher.snapshotChannel()"
                + " (current yields "
                + maxMessageLength
                + " max; try doubling term-length)"
                + " and restart all nodes. Order count: "
                + tradingState.orderBook().size());
      }
    }

    return pos;
  }

  @Override
  public void onRoleChange(final Cluster.Role newRole) {
    // Phase 1: no role-specific behaviour. Future: warm/cold projection rebuild on LEADER promote.
  }

  @Override
  public void onTerminate(final Cluster cluster) {
    // Phase 1: nothing to release; buffers are on-heap and GC-managed.
  }

  // ===========================================================================
  // Journal helper (ref-data dispatch path — trading commands use EventSink)
  // ===========================================================================

  private void appendToJournal(
      final long seqNo, final DirectBuffer src, final int srcOffset, final int srcLength) {
    // Read the event's templateId from the SBE header at src[srcOffset..] so projections can
    // dispatch by type without re-wrapping the header themselves. Uses the dedicated
    // journalHeaderDecoder so the inbound command's headerDecoder wrap state is preserved.
    journalHeaderDecoder.wrap(src, srcOffset);
    final int eventType = journalHeaderDecoder.templateId();
    eventJournal.append(seqNo, eventType, src, srcOffset, srcLength);
  }

  private void offerFragment(
      final ExclusivePublication pub, final MutableDirectBuffer buf, final int length) {
    if (pub == null) {
      return; // Test path uses the pre-encoded fragments directly from the scratch buffers.
    }
    // Unlike EventSink.offerToSession, a failed snapshot offer is non-recoverable: a truncated
    // snapshot
    // leaves the cluster unable to recover on restart. Throw on any non-retryable return code
    // (NOT_CONNECTED / CLOSED / MAX_POSITION_EXCEEDED) and on retry exhaustion so the cluster
    // framework surfaces the failure rather than silently shipping a corrupted snapshot.
    for (int attempt = 0; attempt < MAX_BACKPRESSURE_RETRY; attempt++) {
      final long result = pub.offer(buf, 0, length);
      if (result >= 0L) {
        return;
      }
      if (result == BACK_PRESSURED || result == ADMIN_ACTION) {
        if (cluster != null) {
          cluster.idleStrategy().idle();
        }
        continue;
      }
      throw new IllegalStateException("snapshot fragment offer failed with result " + result);
    }
    throw new IllegalStateException(
        "snapshot fragment offer retry exhausted after " + MAX_BACKPRESSURE_RETRY + " attempts");
  }

  // ===========================================================================
  // Snapshot encode (package-private for tests)
  // ===========================================================================

  /**
   * Encode every snapshot fragment into the per-store staging buffers and populate the {@code
   * *SnapLen} fields. After this returns, the staging buffers hold (in publish order):
   *
   * <pre>
   *   [snapshotHeaderBuf][eventSeqSnapBuf][idGenSnapBuf][accountSnapBuf][currencySnapBuf]
   *   [riskLimitSnapBuf][orderBookSnapBuf]
   * </pre>
   *
   * <p>The header's {@code checksum} field is a CRC32C over the six body fragments concatenated in
   * publish order (the header itself is not covered, which matches the exchange-core idiom —
   * checksum validates what follows).
   */
  void encodeSnapshotFragments(final long snapshotTimestamp) {
    // 1. EventSequencer — SBE message is one long field (next-sequence-to-assign).
    eventSeqSnapEncoder.wrapAndApplyHeader(eventSeqSnapBuf, 0, headerEncoder);
    eventSeqSnapEncoder.nextSequence(eventSink.sequencer().currentSequence() + 1L);
    eventSeqSnapLen = MessageHeaderEncoder.ENCODED_LENGTH + eventSeqSnapEncoder.encodedLength();

    // 2. IdGeneratorSnapshot — three entries (ORD, EXE, QTE).
    idGenSnapEncoder.wrapAndApplyHeader(idGenSnapBuf, 0, headerEncoder);
    final var idGenGroup = idGenSnapEncoder.noGeneratorsCount(3);
    idGenGroup.next();
    idGenGroup.prefix(tradingState.orderIdGen().prefix());
    idGenGroup.counter(tradingState.orderIdGen().currentCounter());
    idGenGroup.next();
    idGenGroup.prefix(tradingState.execIdGen().prefix());
    idGenGroup.counter(tradingState.execIdGen().currentCounter());
    idGenGroup.next();
    idGenGroup.prefix(tradingState.quoteIdGen().prefix());
    idGenGroup.counter(tradingState.quoteIdGen().currentCounter());
    idGenSnapLen = MessageHeaderEncoder.ENCODED_LENGTH + idGenSnapEncoder.encodedLength();

    // 3-5. Ref-data stores — each returns the total bytes including header.
    accountSnapLen = accountStore.snapshotTo(accountSnapBuf, 0);
    currencySnapLen = currencyStore.snapshotTo(currencySnapBuf, 0);
    riskLimitSnapLen = riskLimitStore.snapshotTo(riskLimitSnapBuf, 0);

    // 6. OrderBookSnapshot.
    orderBookSnapLen = tradingState.snapshotOrderBookTo(orderBookSnapBuf, 0);

    // 7. RfqStateSnapshot (template 203) — APP-232.
    rfqStateSnapLen = rfqStateMachine.encodeInto(rfqStateSnapBuf, 0, headerEncoder);

    // 8. ClOrdIdDedupSnapshot (template 210) — APP-206 R7. Persists the NewOrderSingleHandler
    //    dedup registry + lastEvictionTimestampNanos throttle. Without this, after a cluster
    //    restart the registry rebuilds empty and any ClOrdID first seen before the snapshot
    //    but still inside the 24h dedup window would be admitted again.
    clOrdIdDedupSnapLen = newOrderSingleHandler.snapshotDedupTo(clOrdIdDedupSnapBuf, 0);

    // CRC32C over the eight body fragments in publish order.
    crc.reset();
    crc.update(eventSeqSnapBuf.byteArray(), 0, eventSeqSnapLen);
    crc.update(idGenSnapBuf.byteArray(), 0, idGenSnapLen);
    crc.update(accountSnapBuf.byteArray(), 0, accountSnapLen);
    crc.update(currencySnapBuf.byteArray(), 0, currencySnapLen);
    crc.update(riskLimitSnapBuf.byteArray(), 0, riskLimitSnapLen);
    crc.update(orderBookSnapBuf.byteArray(), 0, orderBookSnapLen);
    crc.update(rfqStateSnapBuf.byteArray(), 0, rfqStateSnapLen);
    crc.update(clOrdIdDedupSnapBuf.byteArray(), 0, clOrdIdDedupSnapLen);
    final int checksum = (int) crc.getValue();

    final long totalBody =
        (long) eventSeqSnapLen
            + idGenSnapLen
            + accountSnapLen
            + currencySnapLen
            + riskLimitSnapLen
            + orderBookSnapLen
            + rfqStateSnapLen
            + clOrdIdDedupSnapLen;

    // Finally, encode the SnapshotTaken header.
    snapshotTakenEncoder.wrapAndApplyHeader(snapshotHeaderBuf, 0, headerEncoder);
    snapshotTakenEncoder.lastSequenceNumber(eventSink.sequencer().currentSequence());
    snapshotTakenEncoder.snapshotTimestamp(snapshotTimestamp);
    snapshotTakenEncoder.snapshotVersion(SUPPORTED_SNAPSHOT_VERSION);
    snapshotTakenEncoder.storeCount((short) SNAPSHOT_STORE_COUNT);
    snapshotTakenEncoder.totalByteLength(totalBody);
    // checksum is stored as an unsigned 32-bit in the SBE schema; widen via masked long.
    snapshotTakenEncoder.checksum(Integer.toUnsignedLong(checksum));
    snapshotHeaderLen = MessageHeaderEncoder.ENCODED_LENGTH + snapshotTakenEncoder.encodedLength();
  }

  // Diagnostic accessors for tests — the lengths are populated by encodeSnapshotFragments().
  int snapshotHeaderLength() {
    return snapshotHeaderLen;
  }

  int eventSeqSnapLength() {
    return eventSeqSnapLen;
  }

  int idGenSnapLength() {
    return idGenSnapLen;
  }

  int accountSnapLength() {
    return accountSnapLen;
  }

  int currencySnapLength() {
    return currencySnapLen;
  }

  int riskLimitSnapLength() {
    return riskLimitSnapLen;
  }

  int orderBookSnapLength() {
    return orderBookSnapLen;
  }

  int rfqStateSnapLength() {
    return rfqStateSnapLen;
  }

  MutableDirectBuffer rfqStateSnapBuffer() {
    return rfqStateSnapBuf;
  }

  MutableDirectBuffer snapshotHeaderBuffer() {
    return snapshotHeaderBuf;
  }

  MutableDirectBuffer eventSeqSnapBuffer() {
    return eventSeqSnapBuf;
  }

  MutableDirectBuffer idGenSnapBuffer() {
    return idGenSnapBuf;
  }

  MutableDirectBuffer accountSnapBuffer() {
    return accountSnapBuf;
  }

  MutableDirectBuffer currencySnapBuffer() {
    return currencySnapBuf;
  }

  MutableDirectBuffer riskLimitSnapBuffer() {
    return riskLimitSnapBuf;
  }

  MutableDirectBuffer orderBookSnapBuffer() {
    return orderBookSnapBuf;
  }

  int clOrdIdDedupSnapLength() {
    return clOrdIdDedupSnapLen;
  }

  MutableDirectBuffer clOrdIdDedupSnapBuffer() {
    return clOrdIdDedupSnapBuf;
  }

  MutableDirectBuffer snapshotReassemblyBuffer() {
    return snapshotReassemblyBuf;
  }

  // ===========================================================================
  // Snapshot restore (package-private for tests)
  // ===========================================================================

  /**
   * Walk a single contiguous buffer containing all seven snapshot fragments in publish order and
   * apply them to the live state. Verifies the CRC32C over the six body fragments against the
   * checksum embedded in the {@code SnapshotTaken} header.
   *
   * <p>Test helper. Production uses {@link #onStart(Cluster, Image)} which reassembles the image
   * and then calls this method.
   *
   * @throws IllegalStateException if the first fragment is not {@code SnapshotTaken}, or the CRC
   *     does not match, or an unknown fragment templateId is encountered
   */
  void loadSnapshot(final DirectBuffer src, final int offset, final int length) {
    // 1. First fragment MUST be SnapshotTaken — captures expected body length + checksum.
    headerDecoder.wrap(src, offset);
    if (headerDecoder.templateId() != SnapshotTakenDecoder.TEMPLATE_ID) {
      throw new IllegalStateException(
          "snapshot must begin with SnapshotTaken (200), got templateId "
              + headerDecoder.templateId());
    }
    snapshotTakenDecoder.wrap(
        src,
        offset + MessageHeaderDecoder.ENCODED_LENGTH,
        headerDecoder.blockLength(),
        headerDecoder.version());
    final long snapshotVersion = snapshotTakenDecoder.snapshotVersion();
    if (snapshotVersion != SUPPORTED_SNAPSHOT_VERSION) {
      throw new IllegalStateException(
          "unsupported snapshotVersion "
              + snapshotVersion
              + ", only "
              + SUPPORTED_SNAPSHOT_VERSION
              + " is supported");
    }
    final int expectedStoreCount = snapshotTakenDecoder.storeCount();
    final long expectedBodyLength = snapshotTakenDecoder.totalByteLength();
    final long expectedChecksum = snapshotTakenDecoder.checksum();
    // Use the wire block length from the header (not the compile-time constant) so the walk
    // survives a future forward-compatible schema extension of SnapshotTaken.
    final int headerFragmentLen = MessageHeaderDecoder.ENCODED_LENGTH + headerDecoder.blockLength();
    final int bodyStart = offset + headerFragmentLen;
    // Bounds check in long arithmetic — a corrupted / malicious header could carry a
    // totalByteLength that overflows int, and silently casting would misindex the walk.
    final long availableBodyLength = (long) length - headerFragmentLen;
    if (expectedBodyLength < 0L || expectedBodyLength > availableBodyLength) {
      throw new IllegalStateException(
          "snapshot body length "
              + expectedBodyLength
              + " out of range for available buffer length "
              + availableBodyLength);
    }
    if (expectedBodyLength > Integer.MAX_VALUE - bodyStart) {
      throw new IllegalStateException(
          "snapshot body length "
              + expectedBodyLength
              + " overflows int cursor at bodyStart "
              + bodyStart);
    }
    final int bodyEnd = bodyStart + (int) expectedBodyLength;

    // 2. Reset destination ref-data state so smaller snapshots don't leave orphans behind.
    referenceDataRegistry.resetAll();
    tradingState.clearOrderBook();
    rfqStateMachine.clear();
    // Track whether each of the seven required fragments has been seen so we can reject
    // CRC-valid but semantically incomplete snapshots (missing or duplicated fragments).
    eventSeqFragmentSeen = false;
    idGenFragmentSeen = false;
    orderBookFragmentSeen = false;
    accountFragmentSeen = false;
    currencyFragmentSeen = false;
    riskLimitFragmentSeen = false;
    rfqStateFragmentSeen = false;
    clOrdIdDedupFragmentSeen = false;
    orderIdGenRestored = false;
    execIdGenRestored = false;
    quoteIdGenRestored = false;

    // 3. Walk body fragments in publish order, dispatching each by templateId and computing CRC
    //    as we go.
    crc.reset();
    int cursor = bodyStart;
    int fragmentCount = 0;
    while (cursor < bodyEnd) {
      headerDecoder.wrap(src, cursor);
      final int templateId = headerDecoder.templateId();
      final int consumed = applySnapshotFragment(templateId, src, cursor);
      if (consumed <= 0) {
        throw new IllegalStateException(
            "unknown or malformed snapshot fragment: templateId=" + templateId);
      }
      // Update CRC over the raw bytes of this fragment. When the DirectBuffer is backed by a
      // byte[] (the production path uses ExpandableArrayBuffer, tests use UnsafeBuffer over a
      // byte[]), use the array fast path with wrapAdjustment() added to cursor so a non-zero
      // wrap offset is handled correctly. Falls back to per-byte for native-memory buffers.
      final byte[] backing = src.byteArray();
      if (backing != null) {
        crc.update(backing, src.wrapAdjustment() + cursor, consumed);
      } else {
        for (int i = 0; i < consumed; i++) {
          crc.update(src.getByte(cursor + i) & 0xFF);
        }
      }
      cursor += consumed;
      fragmentCount++;
    }
    if (cursor != bodyEnd) {
      throw new IllegalStateException(
          "snapshot body walk ended at " + cursor + " but expected " + bodyEnd);
    }
    if (fragmentCount != expectedStoreCount) {
      throw new IllegalStateException(
          "snapshot storeCount mismatch: header said "
              + expectedStoreCount
              + " but walked "
              + fragmentCount);
    }
    if (!eventSeqFragmentSeen
        || !idGenFragmentSeen
        || !orderBookFragmentSeen
        || !accountFragmentSeen
        || !currencyFragmentSeen
        || !riskLimitFragmentSeen
        || !rfqStateFragmentSeen) {
      throw new IllegalStateException(
          "snapshot missing required fragments"
              + " (eventSeq="
              + eventSeqFragmentSeen
              + ", idGen="
              + idGenFragmentSeen
              + ", account="
              + accountFragmentSeen
              + ", currency="
              + currencyFragmentSeen
              + ", riskLimit="
              + riskLimitFragmentSeen
              + ", orderBook="
              + orderBookFragmentSeen
              + ", rfqState="
              + rfqStateFragmentSeen
              + ")");
    }
    if (!orderIdGenRestored || !execIdGenRestored || !quoteIdGenRestored) {
      throw new IllegalStateException(
          "snapshot IdGenerator fragment missing ORD, EXE or QTE counter (orderIdGenRestored="
              + orderIdGenRestored
              + ", execIdGenRestored="
              + execIdGenRestored
              + ", quoteIdGenRestored="
              + quoteIdGenRestored
              + ")");
    }
    final long actualChecksum = Integer.toUnsignedLong((int) crc.getValue());
    if (actualChecksum != expectedChecksum) {
      throw new IllegalStateException(
          "snapshot CRC32C mismatch: expected "
              + Long.toHexString(expectedChecksum)
              + ", got "
              + Long.toHexString(actualChecksum));
    }
  }

  private int applySnapshotFragment(
      final int templateId, final DirectBuffer src, final int offset) {
    if (templateId == EventSequencerSnapshotDecoder.TEMPLATE_ID) {
      if (eventSeqFragmentSeen) {
        throw new IllegalStateException("duplicate EventSequencerSnapshot fragment in snapshot");
      }
      eventSeqFragmentSeen = true;
      final int wireBlockLength = headerDecoder.blockLength();
      eventSeqSnapDecoder.wrap(
          src,
          offset + MessageHeaderDecoder.ENCODED_LENGTH,
          wireBlockLength,
          headerDecoder.version());
      // Route through the SBE decoder (not eventSequencer.loadFrom, which reads raw bytes)
      // so future block-length padding does not silently corrupt the restored counter.
      eventSink.sequencer().setNextSequence(eventSeqSnapDecoder.nextSequence());
      return MessageHeaderDecoder.ENCODED_LENGTH + wireBlockLength;
    }
    if (templateId == IdGeneratorSnapshotDecoder.TEMPLATE_ID) {
      if (idGenFragmentSeen) {
        throw new IllegalStateException("duplicate IdGeneratorSnapshot fragment in snapshot");
      }
      idGenFragmentSeen = true;
      idGenSnapDecoder.wrap(
          src,
          offset + MessageHeaderDecoder.ENCODED_LENGTH,
          headerDecoder.blockLength(),
          headerDecoder.version());
      final var group = idGenSnapDecoder.noGenerators();
      while (group.hasNext()) {
        group.next();
        final long counter = group.counter();
        if (prefixMatches(group, tradingState.orderIdGen().prefix())) {
          tradingState.orderIdGen().setCounter(counter);
          orderIdGenRestored = true;
        } else if (prefixMatches(group, tradingState.execIdGen().prefix())) {
          tradingState.execIdGen().setCounter(counter);
          execIdGenRestored = true;
        } else if (prefixMatches(group, tradingState.quoteIdGen().prefix())) {
          tradingState.quoteIdGen().setCounter(counter);
          quoteIdGenRestored = true;
        } else {
          // Snapshot carries an IdGenerator prefix we don't recognize — refuse to silently drop
          // it, since lost counter state would break determinism on the next command dispatch.
          // Use a fixed message (no allocation of prefix bytes or String decode on this path).
          throw new IllegalStateException(UNREGISTERED_ID_PREFIX_MESSAGE);
        }
      }
      return MessageHeaderDecoder.ENCODED_LENGTH + idGenSnapDecoder.encodedLength();
    }
    if (templateId == OrderBookSnapshotDecoder.TEMPLATE_ID) {
      if (orderBookFragmentSeen) {
        throw new IllegalStateException("duplicate OrderBookSnapshot fragment in snapshot");
      }
      orderBookFragmentSeen = true;
      return tradingState.restoreOrderBookFrom(src, offset);
    }
    if (templateId == ClOrdIdDedupSnapshotDecoder.TEMPLATE_ID) {
      if (clOrdIdDedupFragmentSeen) {
        throw new IllegalStateException("duplicate ClOrdIdDedupSnapshot fragment in snapshot");
      }
      clOrdIdDedupFragmentSeen = true;
      // APP-206 R7: restore the NewOrderSingleHandler dedup registry so the 24h ClOrdID-uniqueness
      // contract survives snapshot+restore.
      final int consumed =
          newOrderSingleHandler.restoreDedupFrom(
              src,
              offset + MessageHeaderDecoder.ENCODED_LENGTH,
              headerDecoder.blockLength(),
              headerDecoder.version());
      return MessageHeaderDecoder.ENCODED_LENGTH + consumed;
    }
    if (templateId == RfqStateSnapshotDecoder.TEMPLATE_ID) {
      if (rfqStateFragmentSeen) {
        throw new IllegalStateException("duplicate RfqStateSnapshot fragment in snapshot");
      }
      rfqStateFragmentSeen = true;
      return rfqStateMachine.restoreFrom(
          src,
          offset + MessageHeaderDecoder.ENCODED_LENGTH,
          headerDecoder.blockLength(),
          headerDecoder.version());
    }
    // Ref-data snapshots (Account 201, Currency 208, RiskLimit 209) route via the registry.
    if (templateId == AccountSnapshotDecoder.TEMPLATE_ID) {
      if (accountFragmentSeen) {
        throw new IllegalStateException("duplicate AccountSnapshot fragment in snapshot");
      }
      accountFragmentSeen = true;
    } else if (templateId == CurrencySnapshotDecoder.TEMPLATE_ID) {
      if (currencyFragmentSeen) {
        throw new IllegalStateException("duplicate CurrencySnapshot fragment in snapshot");
      }
      currencyFragmentSeen = true;
    } else if (templateId == RiskLimitSnapshotDecoder.TEMPLATE_ID) {
      if (riskLimitFragmentSeen) {
        throw new IllegalStateException("duplicate RiskLimitSnapshot fragment in snapshot");
      }
      riskLimitFragmentSeen = true;
    }
    final int consumed = referenceDataRegistry.restoreFragment(headerDecoder, src, offset);
    if (consumed != ReferenceDataRegistry.NOT_HANDLED) {
      return consumed;
    }
    return NOT_HANDLED;
  }

  private static final String UNREGISTERED_ID_PREFIX_MESSAGE =
      "IdGeneratorSnapshot contains an unregistered prefix (expected ORD, EXE, or QTE)";

  /**
   * Compare the full 8-byte prefix carried by an {@code IdGeneratorSnapshot} record against a known
   * generator's prefix (e.g. {@code "ORD"}). Trailing zero-padding on the wire side counts as "end
   * of prefix". Returns {@code true} on an exact match.
   */
  private static boolean prefixMatches(
      final IdGeneratorSnapshotDecoder.NoGeneratorsDecoder group, final String prefix) {
    final int prefixLen = prefix.length();
    if (prefixLen > IdGenerator.MAX_PREFIX_LENGTH) {
      return false;
    }
    for (int i = 0; i < prefixLen; i++) {
      if (group.prefix(i) != (byte) prefix.charAt(i)) {
        return false;
      }
    }
    for (int i = prefixLen; i < IdGenerator.MAX_PREFIX_LENGTH; i++) {
      if (group.prefix(i) != 0) {
        return false;
      }
    }
    return true;
  }
}
