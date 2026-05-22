package com.trading.engine.cluster.handler;

import com.trading.engine.cluster.OrderState;
import com.trading.engine.cluster.metrics.RfqMetrics;
import com.trading.engine.cluster.refdata.AccountState;
import com.trading.engine.cluster.refdata.AccountStore;
import com.trading.engine.cluster.refdata.CurrencyStore;
import com.trading.engine.cluster.refdata.RiskLimitStore;
import com.trading.engine.cluster.state.RfqSlot;
import com.trading.engine.cluster.state.RfqStateMachine;
import com.trading.engine.cluster.state.TradingState;
import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.NewOrderSingleDecoder;
import com.trading.engine.messages.sbe.OrdTypeEnum;
import com.trading.engine.messages.sbe.OrderCreatedEventEncoder;
import com.trading.engine.messages.sbe.OrderRejectedEventEncoder;
import com.trading.engine.messages.sbe.ProductTypeEnum;
import com.trading.engine.messages.sbe.RejectReasonEnum;
import com.trading.engine.messages.sbe.SettlTypeEnum;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.messages.sbe.TenorEnum;
import com.trading.engine.messages.sbe.TimeInForceEnum;
import io.aeron.cluster.service.ClientSession;
import java.util.Objects;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Handles NewOrderSingle (NOS) commands: decodes the inbound FIX-style command, validates it
 * against reference data and risk limits, and either emits an {@code OrderCreatedEvent} (happy
 * path) or an {@code OrderRejectedEvent} (validation failure). Extracted from the monolithic {@code
 * TradingClusteredService} to keep command dispatch handlers as focused, testable units.
 *
 * <p><b>Event-sourced two-phase flow:</b>
 *
 * <ol>
 *   <li><b>Phase A (before emit):</b> generate deterministic order/exec IDs via {@link
 *       TradingState#generateOrderId()} and {@link TradingState#generateExecId()}, then encode the
 *       {@code OrderCreatedEvent} into the pre-allocated egress buffer.
 *   <li><b>Phase B (after emit):</b> apply state via {@link TradingState#applyOrderCreated} —
 *       acquires a pool slot and populates {@link OrderState} from the event data. If pool
 *       acquisition fails at this stage (should not happen due to the pre-validation guard), an
 *       {@link IllegalStateException} is thrown to trigger Aeron Cluster failover.
 * </ol>
 *
 * <p><b>Dedup (APP-206):</b> Before any validation, the handler hashes {@code (sessionId, clOrdId)}
 * into a {@link Long2LongHashMap} keyed by FNV-1a 64-bit hash → first-seen cluster timestamp. A
 * second submission within the {@code CLORDID_DEDUP_WINDOW_NS} (24 h) window is rejected with
 * {@link RejectReasonEnum#DuplicateClOrdId} — even if the first attempt was itself rejected by
 * downstream validation, matching the LMAX / CME "ClOrdID consumed on first sight" semantics.
 *
 * <p><b>Validation (12 pre-trade checks plus §9.2a quote-acceptance peek):</b>
 *
 * <ol>
 *   <li>Symbol must not be empty (UnknownSymbol)
 *   <li>OrderQty must be positive (InvalidQuantity)
 *   <li>Limit orders must have positive price (InvalidPrice)
 *   <li>AccountCode must not be empty (AccountNotFound)
 *   <li>Account must exist in {@link AccountStore} (AccountNotFound)
 *   <li>Account status must be Active (AccountSuspended)
 *   <li>Account must have CAN_TRADE permission (AccountNoTradePermission)
 *   <li>Currency must be 3 uppercase ASCII letters (InvalidCurrencyCode)
 *   <li>Currency must exist in {@link CurrencyStore} (UnknownCurrency)
 *   <li>(NEW per APP-232 §9.2a) NOS-with-quoteId peek phase: when {@code ordType=PreviouslyQuoted}
 *       and {@code quoteId} is non-empty, the order is matched against an active QUOTED RFQ slot
 *       via {@link RfqStateMachine#peekByQuoteId}; rejects on unknown / expired quote, side
 *       mismatch, or price/qty bps tolerance breach
 *   <li>OrderQty must not exceed account maxOrderSize risk limit (OrderExceedsMaxSize)
 *   <li>Order book must not be full (BookFull) — checked before generating IDs to avoid wasting
 *       deterministic counter space
 * </ol>
 *
 * <p>If checks 1–10 pass, the §9.2a slot reference is cached in {@link #pendingQuoteAcceptSlot}; if
 * checks 11–12 then reject, the slot is left intact in QUOTED state for client retry. The slot
 * transitions atomically to ACCEPTED + release as step 13 of {@link #admitNewOrder} after {@link
 * TradingState#applyOrderCreated} succeeds.
 *
 * <p><b>Threading:</b> single-threaded cluster duty cycle. No synchronization required.
 *
 * <p><b>Allocation:</b> zero allocation after construction. All SBE flyweight decoders, encoders,
 * and scratch byte arrays are pre-allocated as instance fields.
 *
 * @see CommandHandler
 * @see EventSink
 * @see TradingState
 */
public final class NewOrderSingleHandler implements CommandHandler {

  // -- Pre-allocated SBE flyweights (zero-allocation hot path) --
  private final NewOrderSingleDecoder nosDecoder = new NewOrderSingleDecoder();
  private final OrderCreatedEventEncoder orderCreatedEncoder = new OrderCreatedEventEncoder();
  private final OrderRejectedEventEncoder orderRejectedEncoder = new OrderRejectedEventEncoder();
  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();

  /** Egress buffer for encoding domain events. Sized to accommodate the largest event. */
  private final UnsafeBuffer egressBuffer = new UnsafeBuffer(new byte[8 * 1024]);

  // -- Scratch buffers for char-array fields (SBE fixed-length character arrays) --

  /** ClOrdID scratch — FIX tag 11, 20-byte fixed-length ASCII. */
  private final byte[] clOrdIdScratch = new byte[20];

  /** Symbol scratch — FIX tag 55, 8-byte fixed-length ASCII. */
  private final byte[] symbolScratch = new byte[8];

  /** AccountCode scratch — FIX tag 1, 16-byte fixed-length ASCII. */
  private final byte[] accountCodeScratch = new byte[16];

  /** QuoteId scratch — FIX tag 117, 20-byte fixed-length ASCII. */
  private final byte[] quoteIdScratch = new byte[20];

  /** SettlDate scratch — FIX tag 64, 8-byte fixed-length ASCII. */
  private final byte[] settlDateScratch = new byte[8];

  // -- Currency bytes stashed for the reject path --
  // The reject encoder needs currency bytes that were extracted from the NOS decoder.
  // We stash them on instance fields so emitOrderRejected can access them even though
  // the reject may be emitted from validateNewOrder (which does not pass currency bytes).
  private byte currencyByte0;
  private byte currencyByte1;
  private byte currencyByte2;

  // -- Injected dependencies --
  private final TradingState tradingState;
  private final AccountStore accountStore;
  private final CurrencyStore currencyStore;
  private final RiskLimitStore riskLimitStore;

  // ===========================================================================
  // ClOrdID dedup (APP-206)
  //
  // Per FIX 4.4: ClOrdID (tag 11) must be unique per session for the trading
  // day. A duplicate ClOrdID — even after the original order was rejected —
  // is treated as a protocol error and rejected with
  // {@link RejectReasonEnum#DuplicateClOrdId} (5).
  //
  // Storage: {@link Long2LongHashMap} keyed by a 64-bit hash of
  // {@code (session.id, effective-clOrdId-bytes)} → first-seen cluster
  // timestamp (epoch nanos). The 24-hour window matches the FIX trading-day
  // boundary; entries outside the window are evicted lazily on the next
  // dedup-key insert that crosses the size watermark.
  //
  // Hash-collision risk: 64-bit hash space + 100K active entries gives
  // P(collision) ≈ 2.7e-10 — well below the noise floor of every other
  // failure mode in the pipeline. The trade-off buys hot-path zero-alloc:
  // a per-session {@code ObjectHashSet<byte[]>} would require byte-array
  // boxing on every put + AsciiSequenceView allocation per query.
  //
  // Snapshot-restore caveat: this registry is NOT yet in the cluster
  // snapshot. After a snapshot restore, the dedup state rebuilds only from
  // log entries replayed since the snapshot point — a window of up to one
  // snapshot interval may admit a duplicate that the pre-snapshot path
  // would have rejected. Tracked under APP-171 (atomic snapshot publish);
  // accepted for this slice because the snapshot subsystem is not yet
  // production-deployed.
  // ===========================================================================

  /** Dedup window matching the FIX trading-day boundary. */
  static final long CLORDID_DEDUP_WINDOW_NS = 24L * 3600L * 1_000_000_000L;

  /**
   * Size watermark above which the registry attempts lazy eviction on each new insert. At 100K
   * entries the registry retains ~10MB of off-heap memory and eviction walks remain bounded by the
   * watermark, not by total throughput.
   */
  static final int CLORDID_DEDUP_MAX_SIZE = 100_000;

  /** Sentinel returned by the Long2LongHashMap when a key is absent. */
  static final long CLORDID_DEDUP_MISSING = Long.MIN_VALUE;

  /**
   * Minimum interval between lazy eviction scans (60 s). Without this throttle, a registry that is
   * at the watermark AND receiving sustained traffic of NEW (not refreshed) keys would trigger a
   * full O(N) eviction walk on every NOS — a "death spiral" where tail latency degrades as
   * throughput climbs. The 60 s gate guarantees the O(N) scan runs at most once per minute, so the
   * amortised hot-path cost stays bounded regardless of insert rate.
   *
   * <p>Gemini-flagged HIGH on PR #81 (R2 round); the prior version gated eviction only on size,
   * which produced the death-spiral risk above.
   */
  static final long CLORDID_EVICTION_INTERVAL_NS = 60L * 1_000_000_000L;

  /**
   * {@code (sessionId, clOrdIdHash)} → first-seen cluster timestamp (epoch nanos). Pre-sized to the
   * watermark to avoid rehash thrash during steady- state operation; growth past the watermark
   * triggers lazy eviction.
   *
   * <p>Package-private for direct-size assertions in {@link NewOrderSingleHandlerClOrdIdDedupTest}.
   */
  final Long2LongHashMap clOrdIdRegistry =
      new Long2LongHashMap(CLORDID_DEDUP_MAX_SIZE * 2, 0.65f, CLORDID_DEDUP_MISSING);

  /**
   * Cluster timestamp at which {@link #evictExpiredClOrdIds} last ran. Initialised to {@code 0L}
   * (NOT {@link Long#MIN_VALUE}) so the first eviction is not blocked by the interval guard:
   * cluster timestamps are positive epoch nanos (≈ 1.7e18 in 2026), so {@code (clusterTimestamp -
   * 0L)} cleanly exceeds the 60 s interval. {@link Long#MIN_VALUE} would underflow the {@code
   * clusterTimestamp - lastEvictionTimestampNanos} subtraction because {@code 1.7e18 - (-9.2e18)}
   * overflows {@code long}.
   */
  private long lastEvictionTimestampNanos = 0L;

  /**
   * Optional injection from {@link com.trading.engine.cluster.TradingClusteredService} for plan
   * §9.2a quote-acceptance integration. When set, NOS commands carrying {@code
   * ordType=PreviouslyQuoted} and a non-empty quoteId are matched against an active QUOTED RFQ slot
   * via {@link RfqStateMachine#peekByQuoteId}, validated for side / price / qty match, and
   * atomically committed via {@link RfqStateMachine#commitAccept} after all NOS validations pass.
   * Null in tests that exercise the legacy single-leg flow.
   */
  private RfqStateMachine rfqStateMachine;

  /**
   * Cached metrics from the RfqStateMachine for §9.2a reject-path counter increments. Null when
   * {@link #rfqStateMachine} is null.
   */
  private RfqMetrics rfqMetrics;

  /**
   * Scratch field holding the QUOTED slot returned by {@link RfqStateMachine#peekByQuoteId} during
   * the peek phase. Cleared after commit (or on any reject path). Single-threaded duty cycle
   * invariant means this never races.
   */
  private RfqSlot pendingQuoteAcceptSlot;

  /**
   * Creates a NewOrderSingleHandler wired to the given cluster state and reference data stores.
   *
   * @param tradingState the event-sourced order lifecycle state (must not be null)
   * @param accountStore the account reference data store (must not be null)
   * @param currencyStore the currency reference data store (must not be null)
   * @param riskLimitStore the risk limit store (must not be null)
   */
  public NewOrderSingleHandler(
      final TradingState tradingState,
      final AccountStore accountStore,
      final CurrencyStore currencyStore,
      final RiskLimitStore riskLimitStore) {
    this.tradingState = Objects.requireNonNull(tradingState, "tradingState");
    this.accountStore = Objects.requireNonNull(accountStore, "accountStore");
    this.currencyStore = Objects.requireNonNull(currencyStore, "currencyStore");
    this.riskLimitStore = Objects.requireNonNull(riskLimitStore, "riskLimitStore");
  }

  /**
   * Optional: wires the RFQ state machine for plan §9.2a quote-acceptance integration. Called by
   * {@link com.trading.engine.cluster.TradingClusteredService} during construction.
   *
   * @param rfqStateMachine the cluster-side RFQ state machine
   * @param rfqMetrics observability counters for the RFQ path
   */
  public void wireRfqStateMachine(
      final RfqStateMachine rfqStateMachine, final RfqMetrics rfqMetrics) {
    this.rfqStateMachine = rfqStateMachine;
    this.rfqMetrics = rfqMetrics;
  }

  /** {@inheritDoc} */
  @Override
  public int commandTemplateId() {
    return NewOrderSingleDecoder.TEMPLATE_ID;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Decodes the NOS command, runs 12 pre-trade checks (plus the §9.2a quote-acceptance peek),
   * and either emits an {@code OrderCreatedEvent} (happy path) or an {@code OrderRejectedEvent}
   * (validation failure).
   */
  @Override
  public void onCommand(
      final ClientSession session,
      final long clusterTimestamp,
      final DirectBuffer buffer,
      final int offset,
      final int length,
      final int blockLength,
      final int version,
      final EventSink eventSink) {

    // 0. Reset the per-call quote-accept scratch in case the previous onCommand left it set
    //    after a validation reject (the slot was never committed but the field could still hold
    //    a stale reference).
    pendingQuoteAcceptSlot = null;

    // 1. Wrap the decoder at the body portion of the SBE message.
    nosDecoder.wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH, blockLength, version);

    // 2. Extract all fields from the decoded NOS command.
    final long orderQty = nosDecoder.orderQty();
    final var ordType = nosDecoder.ordType();
    final long price = nosDecoder.price();
    final var side = nosDecoder.side();
    final var timeInForce = nosDecoder.timeInForce();

    nosDecoder.getClOrdId(clOrdIdScratch, 0);
    // Trim trailing zeros BEFORE the dedup hash so equivalent ASCII strings ("ABC\0\0..." vs the
    // same "ABC\0..." from a different SBE encoder padding) produce the same dedup key.
    final int clOrdIdLen = trimTrailingZeros(clOrdIdScratch, NewOrderSingleDecoder.clOrdIdLength());
    nosDecoder.getSymbol(symbolScratch, 0);
    final int symbolLen = trimTrailingZeros(symbolScratch, OrderState.SYMBOL_LENGTH);
    nosDecoder.getAccountCode(accountCodeScratch, 0);
    final int accountCodeLen =
        trimTrailingZeros(accountCodeScratch, AccountStore.MAX_ACCOUNT_CODE_LENGTH);

    final byte ccy0 = nosDecoder.currency(0);
    final byte ccy1 = nosDecoder.currency(1);
    final byte ccy2 = nosDecoder.currency(2);
    // Stash on fields so emitOrderRejected can write them into the rejected event —
    // avoids leaking the previous message's currency bytes from the shared egressBuffer.
    currencyByte0 = ccy0;
    currencyByte1 = ccy1;
    currencyByte2 = ccy2;

    // 2a. (APP-206) ClOrdID dedup — per FIX 4.4, ClOrdID must be unique per
    // session within the 24h trading-day window. Reject duplicates before any
    // other validation work runs (cheapest reject path + matches LMAX / CME
    // semantics where a ClOrdID is consumed on first sight regardless of
    // first-attempt outcome).
    final long sessionId = session != null ? session.id() : 0L;
    final long dedupKey = computeClOrdIdDedupKey(sessionId, clOrdIdScratch, 0, clOrdIdLen);
    final long previousSeenNanos = clOrdIdRegistry.get(dedupKey);
    // Short-circuit on CLORDID_DEDUP_MISSING (= Long.MIN_VALUE) BEFORE the subtraction —
    // (clusterTimestamp - Long.MIN_VALUE) overflows, so the missing-key check must precede the
    // window comparison to avoid a false-positive reject on the first submission.
    if (previousSeenNanos != CLORDID_DEDUP_MISSING
        && (clusterTimestamp - previousSeenNanos) < CLORDID_DEDUP_WINDOW_NS) {
      emitOrderRejected(
          eventSink,
          session,
          clusterTimestamp,
          side,
          RejectReasonEnum.DuplicateClOrdId,
          "duplicate ClOrdID within 24h window");
      return;
    }
    // Register (or refresh) the ClOrdID under the cluster timestamp. Lazy eviction runs only when
    // ALL of:
    //   1. the new insert is a NEW key (not a refresh) — refreshes don't grow the registry, so
    //      the steady-state hot path stays O(1) even at the watermark.
    //   2. the registry has crossed CLORDID_DEDUP_MAX_SIZE — under the watermark, we have memory
    //      budget; don't pay the walk cost.
    //   3. it's been at least CLORDID_EVICTION_INTERVAL_NS since the last walk — guards against
    //      the "death spiral" of full-O(N) scans on every NOS under sustained at-watermark load.
    if (previousSeenNanos == CLORDID_DEDUP_MISSING
        && clOrdIdRegistry.size() >= CLORDID_DEDUP_MAX_SIZE
        && (clusterTimestamp - lastEvictionTimestampNanos) >= CLORDID_EVICTION_INTERVAL_NS) {
      evictExpiredClOrdIds(clusterTimestamp);
      lastEvictionTimestampNanos = clusterTimestamp;
    }
    clOrdIdRegistry.put(dedupKey, clusterTimestamp);

    // 3. Validate — returns AccountState on success, null on rejection (already emitted).
    final var account =
        validateNewOrder(
            eventSink,
            session,
            clusterTimestamp,
            side,
            ordType,
            orderQty,
            price,
            symbolLen,
            accountCodeLen,
            ccy0,
            ccy1,
            ccy2);
    if (account == null) {
      // Defensive: if the peek phase (§9.2a step 10) cached a slot but a later check rejected
      // the order, drain the field here so the stale reference cannot leak into the next call.
      // The slot itself remains in QUOTED state (peek is read-only), so the client may retry.
      pendingQuoteAcceptSlot = null;
      return;
    }

    // 4. Happy path — admit the order.
    admitNewOrder(
        eventSink,
        session,
        clusterTimestamp,
        account,
        side,
        ordType,
        timeInForce,
        orderQty,
        price,
        ccy0,
        ccy1,
        ccy2);
  }

  // ===========================================================================
  // Validation — 12 pre-trade checks (plus §9.2a NOS-with-quoteId peek as check 10)
  // ===========================================================================

  /**
   * Runs every pre-trade validation for a decoded NewOrderSingle. On the first failure, emits the
   * corresponding {@code OrderRejectedEvent} via {@link EventSink} and returns {@code null}. On
   * success returns the resolved {@link AccountState} — the caller reuses it for the happy path to
   * avoid a second lookup.
   *
   * @param eventSink the sink for emitting rejection events
   * @param session the client session that sent the command
   * @param timestamp the cluster-assigned timestamp in epoch nanos
   * @param side the order side (FIX tag 54)
   * @param ordType the order type (FIX tag 40)
   * @param orderQty the order quantity (FIX tag 38) in fixed-point 10^-8
   * @param price the order price (FIX tag 44) in fixed-point 10^-8
   * @param symbolLen the trimmed symbol length (0 = empty)
   * @param accountCodeLen the trimmed account code length (0 = empty)
   * @param ccy0 currency byte 0 (FIX tag 15)
   * @param ccy1 currency byte 1
   * @param ccy2 currency byte 2
   * @return the resolved {@link AccountState} on success, or {@code null} if rejected
   */
  private AccountState validateNewOrder(
      final EventSink eventSink,
      final ClientSession session,
      final long timestamp,
      final SideEnum side,
      final OrdTypeEnum ordType,
      final long orderQty,
      final long price,
      final int symbolLen,
      final int accountCodeLen,
      final byte ccy0,
      final byte ccy1,
      final byte ccy2) {

    // 1. Symbol must not be empty.
    if (symbolLen == 0) {
      emitOrderRejected(
          eventSink, session, timestamp, side, RejectReasonEnum.UnknownSymbol, "symbol is empty");
      return null;
    }

    // 2. OrderQty must be positive.
    if (orderQty <= 0L) {
      emitOrderRejected(
          eventSink,
          session,
          timestamp,
          side,
          RejectReasonEnum.InvalidQuantity,
          "orderQty must be > 0");
      return null;
    }

    // 3. Limit orders must have positive price.
    if (ordType == OrdTypeEnum.Limit && price <= 0L) {
      emitOrderRejected(
          eventSink,
          session,
          timestamp,
          side,
          RejectReasonEnum.InvalidPrice,
          "limit price must be > 0");
      return null;
    }

    // 4. AccountCode must not be empty.
    if (accountCodeLen == 0) {
      emitOrderRejected(
          eventSink,
          session,
          timestamp,
          side,
          RejectReasonEnum.AccountNotFound,
          "accountCode is empty");
      return null;
    }

    // 5. Account must exist in AccountStore.
    final var account = accountStore.getByCodeBytes(accountCodeScratch, 0, accountCodeLen);
    if (account == null) {
      emitOrderRejected(
          eventSink,
          session,
          timestamp,
          side,
          RejectReasonEnum.AccountNotFound,
          "account not in AccountStore");
      return null;
    }

    // 6. Account status must be Active.
    if (account.status() != AccountStatusEnum.Active) {
      emitOrderRejected(
          eventSink,
          session,
          timestamp,
          side,
          RejectReasonEnum.AccountSuspended,
          "account not active");
      return null;
    }

    // 7. Account must have CAN_TRADE permission.
    if (!account.canTrade()) {
      emitOrderRejected(
          eventSink,
          session,
          timestamp,
          side,
          RejectReasonEnum.AccountNoTradePermission,
          "account lacks CAN_TRADE");
      return null;
    }

    // 8. Currency must be 3 uppercase ASCII letters.
    final int ccyPacked = CurrencyStore.packCodeOrInvalid(ccy0, ccy1, ccy2);
    if (ccyPacked == CurrencyStore.INVALID_PACKED_CODE) {
      emitOrderRejected(
          eventSink,
          session,
          timestamp,
          side,
          RejectReasonEnum.InvalidCurrencyCode,
          "currency is not 3 uppercase ASCII letters");
      return null;
    }

    // 9. Currency must exist in CurrencyStore.
    if (!currencyStore.contains(ccyPacked)) {
      emitOrderRejected(
          eventSink,
          session,
          timestamp,
          side,
          RejectReasonEnum.UnknownCurrency,
          "currency not in CurrencyStore");
      return null;
    }

    // 10. (NEW per APP-232 §9.2a) NOS-with-quoteId peek phase — read-only RFQ slot lookup.
    //     Slot is cached in pendingQuoteAcceptSlot; commit happens at the end of admitNewOrder
    //     so a later validation reject (#11/#12) leaves the QUOTED slot intact for client retry.
    pendingQuoteAcceptSlot = null;
    if (rfqStateMachine != null && ordType == OrdTypeEnum.PreviouslyQuoted) {
      nosDecoder.getQuoteId(quoteIdScratch, 0);
      // Defence-in-depth presence check: scan all 20 bytes via trimTrailingZeros (matches the
      // accountCodeLen / symbolLen pattern earlier in this method). A first-byte-nonzero check
      // would let a hostile input with `quoteId="\0..."` bypass §9.2a entirely.
      final int quoteIdLen = trimTrailingZeros(quoteIdScratch, RfqSlot.QUOTE_ID_LENGTH);
      if (quoteIdLen == 0) {
        // FIX protocol: ordType=PreviouslyQuoted REQUIRES a non-empty quoteId. An empty
        // quoteId on this path is a protocol violation — reject rather than silently
        // falling through to the normal-order path (which would let a client execute at
        // their own NOS price without an actual quote on file).
        emitOrderRejected(
            eventSink, session, timestamp, side, RejectReasonEnum.QuoteNotFound, "missing quoteId");
        if (rfqMetrics != null) {
          rfqMetrics.rejectUnknownQuote++;
        }
        return null;
      }
      {
        final var slot = rfqStateMachine.peekByQuoteId(quoteIdScratch, 0, RfqSlot.QUOTE_ID_LENGTH);
        if (slot == null) {
          emitOrderRejected(
              eventSink, session, timestamp, side, RejectReasonEnum.QuoteNotFound, "unknown quote");
          if (rfqMetrics != null) {
            rfqMetrics.rejectUnknownQuote++;
          }
          return null;
        }
        // Side mismatch is hard reject (no tolerance).
        if (slot.side != (byte) side.value()) {
          emitOrderRejected(
              eventSink, session, timestamp, side, RejectReasonEnum.QuoteNotFound, "side mismatch");
          if (rfqMetrics != null) {
            rfqMetrics.rejectQuoteSideMismatch++;
          }
          return null;
        }
        // Price tolerance (bps).
        final long quotedPx = side == SideEnum.Buy ? slot.offerPx : slot.bidPx;
        final long quotedSize = side == SideEnum.Buy ? slot.offerSize : slot.bidSize;
        // One-sided quote: missing price for the requested side. A trader hitting the
        // missing side (e.g., Buy against a bid-only quote) MUST be rejected — accepting
        // would let the trader execute at their own NOS price with no firm offer.
        if (quotedPx <= 0L) {
          emitOrderRejected(
              eventSink,
              session,
              timestamp,
              side,
              RejectReasonEnum.QuoteNotFound,
              "quote side missing");
          if (rfqMetrics != null) {
            rfqMetrics.rejectUnknownQuote++;
          }
          return null;
        }
        // Overflow guards:
        //   1) `price - quotedPx` can equal Long.MIN_VALUE on hostile input; Math.abs of
        //      that is still Long.MIN_VALUE (negative). Treat negative pxDelta as
        //      saturation → hard reject.
        //   2) `pxDelta * 10_000L` can overflow long when pxDelta is large; guard by
        //      saturating to Long.MAX_VALUE.
        final long pxDelta = Math.abs(price - quotedPx);
        final long pxDeltaBps =
            (pxDelta < 0L || pxDelta > Long.MAX_VALUE / 10_000L)
                ? Long.MAX_VALUE
                : pxDelta * 10_000L / quotedPx;
        if (pxDeltaBps > rfqStateMachine.acceptPriceToleranceBps()) {
          emitOrderRejected(
              eventSink,
              session,
              timestamp,
              side,
              RejectReasonEnum.QuoteNotFound,
              "price mismatch");
          if (rfqMetrics != null) {
            rfqMetrics.rejectQuotePriceMismatch++;
          }
          return null;
        }
        // Qty tolerance (bps). One-sided quote with missing size is also rejected via the
        // same path the price check uses — a quote with bidPx>0 but bidSize=0 is malformed.
        if (quotedSize <= 0L) {
          emitOrderRejected(
              eventSink,
              session,
              timestamp,
              side,
              RejectReasonEnum.QuoteNotFound,
              "quote size missing");
          if (rfqMetrics != null) {
            rfqMetrics.rejectUnknownQuote++;
          }
          return null;
        }
        // Overflow guards: see price-bps above. quotedSize > 0 guaranteed by the
        // missing-size reject above.
        final long qtyDelta = Math.abs(orderQty - quotedSize);
        final long qtyDeltaBps =
            (qtyDelta < 0L || qtyDelta > Long.MAX_VALUE / 10_000L)
                ? Long.MAX_VALUE
                : qtyDelta * 10_000L / quotedSize;
        if (qtyDeltaBps > rfqStateMachine.acceptQtyToleranceBps()) {
          emitOrderRejected(
              eventSink, session, timestamp, side, RejectReasonEnum.QuoteNotFound, "qty mismatch");
          if (rfqMetrics != null) {
            rfqMetrics.rejectQuoteQtyMismatch++;
          }
          return null;
        }
        pendingQuoteAcceptSlot = slot;
      }
    }

    // 11. OrderQty must not exceed account maxOrderSize risk limit.
    final var riskLimit = riskLimitStore.get(account.accountId());
    if (riskLimit != null && riskLimit.maxOrderSize() > 0L && orderQty > riskLimit.maxOrderSize()) {
      emitOrderRejected(
          eventSink,
          session,
          timestamp,
          side,
          RejectReasonEnum.OrderExceedsMaxSize,
          "orderQty exceeds account maxOrderSize");
      return null;
    }

    // 11b. (APP-62 first slice) OrderNotional (price × qty / PRICE_SCALE) must not exceed account
    //      maxOrderNotional risk limit. Skipped for Market orders (price=0) — notional cannot be
    //      computed at order entry and is bounded by maxOrderSize × prevailing price. Skipped
    //      when the account has no notional limit configured (maxOrderNotional == 0 == unlimited).
    //
    //      Overflow guard: orderQty × price can overflow long for large values
    //      (qty=1e8 * price=1e8 / 1e8 = 1e8 result — fine — but qty=1e10 * price=1e10 = 1e20
    //      overflows). Use Math.multiplyHigh + low-bits to detect overflow without floating-
    //      point; on overflow, treat the order as exceeding the limit and reject.
    if (riskLimit != null
        && riskLimit.maxOrderNotional() > 0L
        && ordType == OrdTypeEnum.Limit
        && price > 0L) {
      final long notional = computeNotionalSaturating(orderQty, price);
      if (notional > riskLimit.maxOrderNotional()) {
        emitOrderRejected(
            eventSink,
            session,
            timestamp,
            side,
            RejectReasonEnum.OrderExceedsMaxSize,
            "orderNotional exceeds account maxOrderNotional");
        return null;
      }
    }

    // 12. Order book must not be full — checked BEFORE generating IDs to avoid wasting
    //     deterministic counter space on an order that cannot be admitted.
    if (tradingState.isOrderBookFull()) {
      emitOrderRejected(
          eventSink,
          session,
          timestamp,
          side,
          RejectReasonEnum.BookFull,
          "order book pool exhausted");
      return null;
    }

    return account;
  }

  // ===========================================================================
  // Happy path — two-phase event-sourced admission
  // ===========================================================================

  /**
   * Admits a validated NewOrderSingle into the order book. Generates deterministic IDs, encodes and
   * emits the {@code OrderCreatedEvent}, then applies state from the event.
   *
   * <p><b>Phase A:</b> generate order/exec IDs, encode the event, emit via {@link EventSink}.
   *
   * <p><b>Phase B:</b> apply state via {@link TradingState#applyOrderCreated}.
   *
   * @param eventSink the event emission pipeline
   * @param session the client session for egress reply
   * @param timestamp the cluster-assigned timestamp in epoch nanos
   * @param account the validated account
   * @param side the order side (FIX tag 54)
   * @param ordType the order type (FIX tag 40)
   * @param timeInForce the time-in-force (FIX tag 59)
   * @param orderQty the order quantity (FIX tag 38) in fixed-point 10^-8
   * @param price the order price (FIX tag 44) in fixed-point 10^-8
   * @param ccy0 currency byte 0 (FIX tag 15)
   * @param ccy1 currency byte 1
   * @param ccy2 currency byte 2
   */
  private void admitNewOrder(
      final EventSink eventSink,
      final ClientSession session,
      final long timestamp,
      final AccountState account,
      final SideEnum side,
      final OrdTypeEnum ordType,
      final TimeInForceEnum timeInForce,
      final long orderQty,
      final long price,
      final byte ccy0,
      final byte ccy1,
      final byte ccy2) {

    // --- Phase A: generate IDs, encode event, emit ---

    final long orderKey = tradingState.generateOrderId();
    tradingState.generateExecId(); // exec ID bytes available via tradingState.execIdScratch()

    // Encode OrderCreatedEvent with all 19 fields.
    orderCreatedEncoder.wrapAndApplyHeader(egressBuffer, 0, headerEncoder);
    // sequenceNumber + timestamp written here as zero by design: EventSink overwrites both
    // fields with the authoritative cluster sequence + nanosecond timestamp during egress
    // publication (the cluster duty-cycle thread is the single point of monotonic ordering).
    // Writing them here keeps the SBE block layout dense + avoids re-wrap allocation.
    orderCreatedEncoder.sequenceNumber(0L);
    orderCreatedEncoder.timestamp(0L);
    orderCreatedEncoder.putOrderId(tradingState.orderIdScratch(), 0);
    orderCreatedEncoder.putExecId(tradingState.execIdScratch(), 0);
    orderCreatedEncoder.putClOrdId(clOrdIdScratch, 0);
    orderCreatedEncoder.putSymbol(symbolScratch, 0);
    orderCreatedEncoder.side(side);
    orderCreatedEncoder.ordType(ordType);
    orderCreatedEncoder.timeInForce(timeInForce);
    orderCreatedEncoder.price(price);
    orderCreatedEncoder.orderQty(orderQty);

    // QuoteId — copy from the NOS decoder (may be zero-padded if not present).
    nosDecoder.getQuoteId(quoteIdScratch, 0);
    orderCreatedEncoder.putQuoteId(quoteIdScratch, 0);

    orderCreatedEncoder.putAccountCode(accountCodeScratch, 0);
    orderCreatedEncoder.productType(safeProductType());

    // SettlDate — FIX tag 64, 8-byte fixed-length ASCII.
    nosDecoder.getSettlDate(settlDateScratch, 0);
    orderCreatedEncoder.putSettlDate(settlDateScratch, 0);
    orderCreatedEncoder.settlType(safeSettlType());

    // Currency — FIX tag 15, 3-byte fixed-length ASCII.
    orderCreatedEncoder.putCurrency(ccy0, ccy1, ccy2);

    // SettlCurrency — FIX tag 120, 3-byte fixed-length ASCII.
    final byte sc0 = nosDecoder.settlCurrency(0);
    final byte sc1 = nosDecoder.settlCurrency(1);
    final byte sc2 = nosDecoder.settlCurrency(2);
    orderCreatedEncoder.putSettlCurrency(sc0, sc1, sc2);

    orderCreatedEncoder.tenor(safeTenor());

    // Emit via EventSink — stamps seqNo + timestamp, appends to journal, offers to session.
    final int eventLen = MessageHeaderEncoder.ENCODED_LENGTH + orderCreatedEncoder.encodedLength();
    eventSink.emit(timestamp, egressBuffer, 0, eventLen);

    // --- Phase B: apply state derived from the emitted event ---

    final var state =
        tradingState.applyOrderCreated(
            orderKey,
            timestamp,
            tradingState.orderIdScratch(),
            0,
            side,
            ordType,
            timeInForce,
            price,
            orderQty,
            account.accountId(),
            clOrdIdScratch,
            0,
            symbolScratch,
            0);

    if (state == null) {
      // The pre-validation guard (isOrderBookFull) should prevent this. If we reach here, the
      // event has already been journaled but state cannot be applied — this is an internal
      // consistency failure. Throwing triggers Aeron Cluster failover, which is the correct
      // recovery action for a deterministic state machine.
      throw new IllegalStateException(
          "Order pool exhausted after event emitted — state machine inconsistency");
    }

    // 13. (NEW per APP-232 §9.2a) Quote-acceptance commit phase. Runs as the LAST step after
    //     OrderCreatedEvent has been journaled successfully. Atomic transition QUOTED→ACCEPTED +
    //     release; never observable in snapshot due to the single-threaded duty-cycle invariant.
    if (pendingQuoteAcceptSlot != null && rfqStateMachine != null) {
      rfqStateMachine.commitAccept(pendingQuoteAcceptSlot, timestamp, eventSink);
      pendingQuoteAcceptSlot = null;
    }
  }

  // ===========================================================================
  // Rejection encoding + emission
  // ===========================================================================

  /**
   * Encodes and emits an {@code OrderRejectedEvent} via {@link EventSink}. Uses the pre-stashed
   * {@link #clOrdIdScratch}, {@link #symbolScratch}, {@link #accountCodeScratch}, and currency
   * bytes from the current NOS decode pass.
   *
   * @param eventSink the event emission pipeline
   * @param session the client session for egress reply
   * @param timestamp the cluster-assigned timestamp in epoch nanos
   * @param side the order side (may be NULL_VAL if decode failed before side extraction)
   * @param reason the rejection reason enum
   * @param text human-readable rejection text (max 64 ASCII chars)
   */
  private void emitOrderRejected(
      final EventSink eventSink,
      final ClientSession session,
      final long timestamp,
      final SideEnum side,
      final RejectReasonEnum reason,
      final String text) {

    orderRejectedEncoder.wrapAndApplyHeader(egressBuffer, 0, headerEncoder);
    // Authoritative sequenceNumber + timestamp stamped by EventSink at egress (see comment in
    // OrderCreatedEvent encode block above for full rationale).
    orderRejectedEncoder.sequenceNumber(0L);
    orderRejectedEncoder.timestamp(0L);
    orderRejectedEncoder.putClOrdId(clOrdIdScratch, 0);
    // Symbol may be zero-padded from the decoder; pass the scratch verbatim.
    orderRejectedEncoder.putSymbol(symbolScratch, 0);
    orderRejectedEncoder.side(side);
    orderRejectedEncoder.rejectReason(reason);
    // Account code may be empty — still ship the 16-byte scratch (zero-padded tail is valid SBE).
    orderRejectedEncoder.putAccountCode(accountCodeScratch, 0);
    // ProductType — use the decoded value if available. The decoder has been wrapped for all
    // reject paths (wrap happens before any validation), so the raw byte is always readable.
    // Use safeProductType() to handle unrecognized wire values gracefully.
    orderRejectedEncoder.productType(safeProductType());
    // Currency bytes — stashed from the current NOS decode pass.
    orderRejectedEncoder.putCurrency(currencyByte0, currencyByte1, currencyByte2);
    orderRejectedEncoder.text(text);

    final int rejEventLen =
        MessageHeaderEncoder.ENCODED_LENGTH + orderRejectedEncoder.encodedLength();
    eventSink.emit(timestamp, egressBuffer, 0, rejEventLen);

    tradingState.applyOrderRejected(); // no-op — structural completeness
  }

  // ===========================================================================
  // Utility
  // ===========================================================================

  /**
   * Returns the effective length of a fixed-length SBE character array by trimming trailing zero
   * bytes. SBE pads fixed-length character fields with {@code 0x00}; this helper finds the last
   * non-zero byte to determine the logical string length.
   *
   * @param data the byte array to inspect
   * @param length the declared fixed length of the SBE field
   * @return the number of significant (non-zero) bytes from the start
   */
  private static int trimTrailingZeros(final byte[] data, final int length) {
    int end = length;
    while (end > 0 && data[end - 1] == 0) {
      end--;
    }
    return end;
  }

  /**
   * Fixed-point scale factor used by all prices, quantities, and notional values in the trading
   * engine: 10^8 (matches the SBE schema's {@code priceScale="100000000"} attribute). Kept as a
   * local constant here so the notional calculation stays self-contained — the cluster module has
   * no shared "prices.PRICE_SCALE" symbol today, and importing one cross-module would couple this
   * handler to a constants type it otherwise doesn't need.
   */
  static final long PRICE_SCALE = 100_000_000L;

  /**
   * Computes {@code (orderQty * price) / PRICE_SCALE} for the maxOrderNotional check, saturating to
   * {@link Long#MAX_VALUE} on intermediate overflow. Both inputs are fixed-point 10^-8 longs; the
   * naive multiply can exceed long for qty × price > ~9.2e18, which happens at qty=1e10 *
   * price=1e10 (10 billion units at $100 — well within hostile-input range for a fuzz test,
   * plausible for an accidental decimal-place mistake in production).
   *
   * <p>Algorithm: use {@link Math#multiplyHigh(long, long)} to detect the high 64 bits of the
   * 128-bit product. If high != 0 (or the product would be negative when both inputs are positive),
   * the multiplication overflowed signed-long range — saturate to {@code Long.MAX_VALUE} so the
   * downstream {@code > maxOrderNotional} check rejects the order. Otherwise return the divided
   * value. Pure primitive arithmetic, zero allocation.
   *
   * @param orderQty fixed-point 10^-8 quantity (positive)
   * @param price fixed-point 10^-8 price (positive)
   * @return the notional in fixed-point 10^-8, or {@link Long#MAX_VALUE} on overflow
   */
  static long computeNotionalSaturating(final long orderQty, final long price) {
    // multiplyHigh returns the high 64 bits of the 128-bit signed product. If those bits are
    // anything but 0 for two positive inputs, the low 64 bits don't represent the true value.
    final long high = Math.multiplyHigh(orderQty, price);
    if (high != 0L) {
      return Long.MAX_VALUE;
    }
    final long product = orderQty * price;
    // Defensive: if signed-long arithmetic wrapped past Long.MAX_VALUE (product < 0 when both
    // inputs are positive), saturate. multiplyHigh = 0 with product < 0 is impossible for
    // positive inputs but guard anyway in case a caller passes a negative price/qty.
    if (product < 0L) {
      return Long.MAX_VALUE;
    }
    return product / PRICE_SCALE;
  }

  /**
   * FNV-1a 64-bit hash over {@code (sessionId-as-8-bytes, clOrdId-bytes)} producing the dedup-map
   * key. Deterministic — replays produce identical keys, which is required for Aeron Cluster log
   * replay. Pure primitive arithmetic — zero allocation.
   *
   * <p>Collision probability — birthday approximation against the 64-bit hash space:
   *
   * <ul>
   *   <li>Globally across all sessions at the {@link #CLORDID_DEDUP_MAX_SIZE} watermark (100K
   *       entries): ≈ 2.7e-10.
   *   <li>Per-session (even spread across N sessions): ≈ 2.7e-10 / N². A single session would need
   *       ~5 billion unique ClOrdIDs in 24h to expect one collision.
   * </ul>
   *
   * <p>The trade-off vs a per-session {@code ObjectHashSet<byte[]>}-keyed structure: a true set
   * would box the byte[] on every put and allocate an {@code AsciiSequenceView} on every query,
   * both of which violate the cluster hot-path zero-allocation rule. The collision rate is well
   * below the noise floor of every other failure mode in the pipeline.
   *
   * @param sessionId the {@link ClientSession#id} of the originating session (or 0 in tests)
   * @param clOrdIdBytes the ClOrdID byte buffer ({@link #clOrdIdScratch})
   * @param offset starting offset into {@code clOrdIdBytes}
   * @param length the effective length (post-trim) of the ClOrdID
   * @return a 64-bit hash usable as a {@link Long2LongHashMap} key
   */
  static long computeClOrdIdDedupKey(
      final long sessionId, final byte[] clOrdIdBytes, final int offset, final int length) {
    // FNV-1a constants: offset basis 0xcbf29ce484222325L; prime 0x100000001b3L.
    long hash = 0xcbf29ce484222325L;
    // Mix in session ID bytes (big-endian) first so identical ClOrdIDs across sessions get
    // distinct keys.
    for (int i = 7; i >= 0; i--) {
      hash = (hash ^ ((sessionId >>> (i * 8)) & 0xFFL)) * 0x100000001b3L;
    }
    for (int i = 0; i < length; i++) {
      hash = (hash ^ (clOrdIdBytes[offset + i] & 0xFFL)) * 0x100000001b3L;
    }
    return hash;
  }

  /**
   * Walks {@link #clOrdIdRegistry} and removes entries whose first-seen timestamp falls outside the
   * {@link #CLORDID_DEDUP_WINDOW_NS} dedup window relative to {@code nowNs}. Invoked only when the
   * registry crosses {@link #CLORDID_DEDUP_MAX_SIZE} on a NEW insert (never on a refresh), so
   * steady-state hot-path cost stays O(1); eviction cost is amortized across the inserts that push
   * the registry past the watermark.
   *
   * <p>Iteration uses {@link Long2LongHashMap.KeySet}'s primitive iterator and reads each value via
   * {@code get(key)} — both primitive, both zero-boxing. Avoids {@code entrySet()} which wraps each
   * key/value pair in a {@code Map.Entry<Long, Long>} (boxes both sides).
   *
   * <p>This eviction path is off the steady-state hot path by design; the watermark guard ensures
   * it runs only when the registry has accumulated 100K+ entries, which is a rare event even on a
   * busy trading day (24h × 100K/24h = ~1.16 puts/sec sustained throughput).
   *
   * @param nowNs the current cluster timestamp in epoch nanos
   */
  private void evictExpiredClOrdIds(final long nowNs) {
    // Explicit Long2LongHashMap.KeyIterator type (rather than `final var`) so a future
    // maintainer cannot mistake this for a `java.util.Iterator<Long>` that would box on
    // .next(). `nextValue()` returns primitive long.
    final Long2LongHashMap.KeyIterator keyIter = clOrdIdRegistry.keySet().iterator();
    while (keyIter.hasNext()) {
      final long key = keyIter.nextValue();
      final long firstSeenNanos = clOrdIdRegistry.get(key);
      if ((nowNs - firstSeenNanos) >= CLORDID_DEDUP_WINDOW_NS) {
        keyIter.remove();
      }
    }
  }

  /**
   * Reads the productType field from the NOS decoder using the raw byte accessor and maps it to the
   * corresponding {@link ProductTypeEnum}. Returns {@link ProductTypeEnum#NULL_VAL} for any
   * unrecognized wire value (including 0, which SBE zero-fills on an unset field). This avoids the
   * {@link IllegalArgumentException} that {@code nosDecoder.productType()} throws for unknown
   * values.
   *
   * @return the resolved product type enum, or {@code NULL_VAL} if the wire value is unrecognized
   */
  private ProductTypeEnum safeProductType() {
    final short raw = nosDecoder.productTypeRaw();
    return switch (raw) {
      case 1 -> ProductTypeEnum.Spot;
      case 2 -> ProductTypeEnum.Forward;
      case 3 -> ProductTypeEnum.Swap;
      default -> ProductTypeEnum.NULL_VAL;
    };
  }

  /**
   * Reads the settlType field from the NOS decoder using the raw byte accessor and maps it to the
   * corresponding {@link SettlTypeEnum}. Returns {@link SettlTypeEnum#NULL_VAL} for any
   * unrecognized wire value. Zero-allocation switch — no exception-as-control-flow.
   *
   * @return the resolved settle type enum, or {@code NULL_VAL} if the wire value is unrecognized
   */
  private SettlTypeEnum safeSettlType() {
    final short raw = nosDecoder.settlTypeRaw();
    return switch (raw) {
      case 0 -> SettlTypeEnum.Regular;
      case 1 -> SettlTypeEnum.Cash;
      case 2 -> SettlTypeEnum.NextDay;
      case 3 -> SettlTypeEnum.TPlus2;
      case 4 -> SettlTypeEnum.TPlus3;
      case 5 -> SettlTypeEnum.TPlus4;
      case 6 -> SettlTypeEnum.Future;
      case 7 -> SettlTypeEnum.WhenAndIfIssued;
      case 8 -> SettlTypeEnum.SellersOption;
      case 9 -> SettlTypeEnum.TPlus5;
      case 10 -> SettlTypeEnum.BrokenDate;
      case 11 -> SettlTypeEnum.FXSpotNextDay;
      default -> SettlTypeEnum.NULL_VAL;
    };
  }

  /**
   * Reads the tenor field from the NOS decoder using the raw byte accessor and maps it to the
   * corresponding {@link TenorEnum}. Returns {@link TenorEnum#NULL_VAL} for any unrecognized wire
   * value. Zero-allocation switch — no exception-as-control-flow.
   *
   * @return the resolved tenor enum, or {@code NULL_VAL} if the wire value is unrecognized
   */
  private TenorEnum safeTenor() {
    final short raw = nosDecoder.tenorRaw();
    return switch (raw) {
      case 1 -> TenorEnum.ON;
      case 2 -> TenorEnum.TN;
      case 3 -> TenorEnum.SN;
      case 4 -> TenorEnum.W1;
      case 5 -> TenorEnum.W2;
      case 6 -> TenorEnum.M1;
      case 7 -> TenorEnum.M2;
      case 8 -> TenorEnum.M3;
      case 9 -> TenorEnum.M6;
      case 10 -> TenorEnum.M9;
      case 11 -> TenorEnum.Y1;
      case 12 -> TenorEnum.Y2;
      case 13 -> TenorEnum.IMM;
      case 14 -> TenorEnum.BRK;
      default -> TenorEnum.NULL_VAL;
    };
  }
}
