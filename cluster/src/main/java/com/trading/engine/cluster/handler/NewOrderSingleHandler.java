package com.trading.engine.cluster.handler;

import com.trading.engine.cluster.OrderState;
import com.trading.engine.cluster.refdata.AccountState;
import com.trading.engine.cluster.refdata.AccountStore;
import com.trading.engine.cluster.refdata.CurrencyStore;
import com.trading.engine.cluster.refdata.RiskLimitStore;
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
 * <p><b>Validation (11 checks):</b>
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
 *   <li>OrderQty must not exceed account maxOrderSize risk limit (OrderExceedsMaxSize)
 *   <li>Order book must not be full (BookFull) — checked before generating IDs to avoid wasting
 *       deterministic counter space
 * </ol>
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

  /**
   * Optional injection from {@link com.trading.engine.cluster.TradingClusteredService} for plan
   * §9.2a quote-acceptance integration. When set, NOS commands carrying {@code
   * ordType=PreviouslyQuoted} and a non-empty quoteId are matched against an active QUOTED RFQ
   * slot via {@link com.trading.engine.cluster.state.RfqStateMachine#peekByQuoteId}, validated for
   * side / price / qty match, and atomically committed via
   * {@link com.trading.engine.cluster.state.RfqStateMachine#commitAccept} after all NOS
   * validations pass. Null in tests that exercise the legacy single-leg flow.
   */
  private com.trading.engine.cluster.state.RfqStateMachine rfqStateMachine;

  /**
   * Cached metrics from the RfqStateMachine for §9.2a reject-path counter increments. Null when
   * {@link #rfqStateMachine} is null.
   */
  private com.trading.engine.cluster.metrics.RfqMetrics rfqMetrics;

  /**
   * Scratch field holding the QUOTED slot returned by {@link
   * com.trading.engine.cluster.state.RfqStateMachine#peekByQuoteId} during the peek phase. Cleared
   * after commit (or on any reject path). Single-threaded duty cycle invariant means this never
   * races.
   */
  private com.trading.engine.cluster.state.RfqSlot pendingQuoteAcceptSlot;

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
      final com.trading.engine.cluster.state.RfqStateMachine rfqStateMachine,
      final com.trading.engine.cluster.metrics.RfqMetrics rfqMetrics) {
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
   * <p>Decodes the NOS command, runs 11 validation checks, and either emits an {@code
   * OrderCreatedEvent} (happy path) or an {@code OrderRejectedEvent} (validation failure).
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
  // Validation — 11 pre-trade checks
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
      if (quoteIdScratch[0] != 0) {
        final var slot = rfqStateMachine.peekByQuoteId(quoteIdScratch, 0, quoteIdScratch.length);
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
        if (quotedPx > 0L) {
          final long pxDeltaBps = Math.abs(price - quotedPx) * 10_000L / quotedPx;
          if (pxDeltaBps > rfqStateMachine.acceptPriceToleranceBps()) {
            emitOrderRejected(
                eventSink, session, timestamp, side, RejectReasonEnum.QuoteNotFound, "price mismatch");
            if (rfqMetrics != null) {
              rfqMetrics.rejectQuotePriceMismatch++;
            }
            return null;
          }
        }
        // Qty tolerance (bps).
        if (quotedSize > 0L) {
          final long qtyDeltaBps = Math.abs(orderQty - quotedSize) * 10_000L / quotedSize;
          if (qtyDeltaBps > rfqStateMachine.acceptQtyToleranceBps()) {
            emitOrderRejected(
                eventSink, session, timestamp, side, RejectReasonEnum.QuoteNotFound, "qty mismatch");
            if (rfqMetrics != null) {
              rfqMetrics.rejectQuoteQtyMismatch++;
            }
            return null;
          }
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
    orderCreatedEncoder.sequenceNumber(0L); // placeholder — EventSink stamps authoritative value
    orderCreatedEncoder.timestamp(0L); // placeholder — EventSink stamps authoritative value
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
    eventSink.emit(session, timestamp, egressBuffer, 0, eventLen);

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
    orderRejectedEncoder.sequenceNumber(0L); // placeholder — EventSink stamps
    orderRejectedEncoder.timestamp(0L); // placeholder — EventSink stamps
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
    eventSink.emit(session, timestamp, egressBuffer, 0, rejEventLen);

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
