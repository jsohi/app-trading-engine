package com.trading.engine.cluster.handler;

import com.trading.engine.cluster.metrics.RfqMetrics;
import com.trading.engine.cluster.refdata.AccountState;
import com.trading.engine.cluster.refdata.AccountStore;
import com.trading.engine.cluster.refdata.CurrencyStore;
import com.trading.engine.cluster.state.RfqSlot;
import com.trading.engine.cluster.state.RfqStateMachine;
import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.ProductTypeEnum;
import com.trading.engine.messages.sbe.QuoteRejectReasonEnum;
import com.trading.engine.messages.sbe.QuoteRejectedEventEncoder;
import com.trading.engine.messages.sbe.QuoteRequestDecoder;
import com.trading.engine.messages.sbe.QuoteRequestedEventEncoder;
import com.trading.engine.messages.sbe.SettlTypeEnum;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.messages.sbe.TenorEnum;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import java.util.Objects;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Handler for {@code QuoteRequest} (template 1). Validates against {@link AccountStore} / {@link
 * CurrencyStore} / {@link RfqStateMachine} state, schedules a request-timeout timer FIRST (so a
 * journal-then-rollback hazard is impossible), then encodes and emits {@code QuoteRequestedEvent}
 * (104) via {@link EventSink}, finally committing the slot to the lookup maps.
 *
 * <p>Reject ladder per plan §9.1 — every reachable state is explicitly handled with a typed reject
 * reason and a pre-allocated {@code text} constant from {@link RfqRejectMessages}. See the table in
 * plan §3.1 for the exhaustive list.
 *
 * <p><b>Threading:</b> single-threaded cluster duty cycle.
 *
 * <p><b>Allocation:</b> zero allocation after construction. All decoders, encoders, scratch byte
 * arrays, and egress buffers are pre-allocated instance fields.
 *
 * @see RfqStateMachine
 * @see PriceResponseHandler
 * @see EventSink
 */
public final class QuoteRequestHandler implements CommandHandler {

  private static final int HDR_LEN = MessageHeaderEncoder.ENCODED_LENGTH;
  private static final int EGRESS_BUFFER_SIZE = 8192;

  // ---- Pre-allocated SBE flyweights ----
  private final QuoteRequestDecoder qrDecoder = new QuoteRequestDecoder();
  private final QuoteRequestedEventEncoder requestedEncoder = new QuoteRequestedEventEncoder();
  private final QuoteRejectedEventEncoder rejectedEncoder = new QuoteRejectedEventEncoder();
  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  private final UnsafeBuffer egressBuffer = new UnsafeBuffer(new byte[EGRESS_BUFFER_SIZE]);

  // ---- Scratch byte arrays for char-array fields ----
  private final byte[] quoteReqIdScratch = new byte[RfqSlot.QUOTE_REQ_ID_LENGTH];

  /**
   * Pre-allocated UnsafeBuffer view over {@link #quoteReqIdScratch} for {@code DirectBuffer}-typed
   * lookups (e.g., {@link RfqStateMachine#recentlyTerminalReason}). Re-wrapped on every onCommand.
   */
  private final UnsafeBuffer quoteReqIdScratchBuffer = new UnsafeBuffer(quoteReqIdScratch);

  private final byte[] symbolScratch = new byte[RfqSlot.SYMBOL_LENGTH];
  private final byte[] accountCodeScratch = new byte[RfqSlot.ACCOUNT_CODE_LENGTH];
  private final byte[] settlDateScratch = new byte[RfqSlot.SETTL_DATE_LENGTH];
  private final byte[] currencyScratch = new byte[RfqSlot.CURRENCY_LENGTH];
  private final byte[] settlCurrencyScratch = new byte[RfqSlot.SETTL_CURRENCY_LENGTH];

  // ---- Injected dependencies ----
  private final RfqStateMachine rfqStateMachine;
  private final AccountStore accountStore;
  private final CurrencyStore currencyStore;
  private final RfqMetrics metrics;
  private Cluster cluster;

  /**
   * Constructs a {@link QuoteRequestHandler}.
   *
   * @param rfqStateMachine the cluster-side RFQ state machine
   * @param accountStore reference-data accessor for account validation
   * @param currencyStore reference-data accessor for currency validation
   * @param metrics observability counters
   */
  public QuoteRequestHandler(
      final RfqStateMachine rfqStateMachine,
      final AccountStore accountStore,
      final CurrencyStore currencyStore,
      final RfqMetrics metrics) {
    this.rfqStateMachine = Objects.requireNonNull(rfqStateMachine, "rfqStateMachine");
    this.accountStore = Objects.requireNonNull(accountStore, "accountStore");
    this.currencyStore = Objects.requireNonNull(currencyStore, "currencyStore");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
  }

  /**
   * Sets the cluster reference. Called once from {@code TradingClusteredService.onStart()}.
   *
   * @param cluster the Aeron cluster instance
   */
  public void setCluster(final Cluster cluster) {
    this.cluster = cluster;
  }

  @Override
  public int commandTemplateId() {
    return QuoteRequestDecoder.TEMPLATE_ID;
  }

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

    // 1. Length precondition
    if (length < HDR_LEN + QuoteRequestDecoder.BLOCK_LENGTH) {
      emitMalformed(session, clusterTimestamp, eventSink);
      return;
    }

    // 2. Decode wrap
    qrDecoder.wrap(buffer, offset + HDR_LEN, blockLength, version);
    qrDecoder.getQuoteReqId(quoteReqIdScratch, 0);
    qrDecoder.getSymbol(symbolScratch, 0);
    qrDecoder.getAccountCode(accountCodeScratch, 0);
    final SideEnum sideEnum = qrDecoder.side();
    final long orderQty = qrDecoder.orderQty();
    final ProductTypeEnum productType = qrDecoder.productType();
    qrDecoder.getSettlDate(settlDateScratch, 0);
    final SettlTypeEnum settlType = qrDecoder.settlType();
    qrDecoder.getCurrency(currencyScratch, 0);
    qrDecoder.getSettlCurrency(settlCurrencyScratch, 0);
    final TenorEnum tenor = qrDecoder.tenor();

    // 3. Symbol non-empty
    if (symbolScratch[0] == 0) {
      emitRejectByQuoteReqId(
          session,
          clusterTimestamp,
          eventSink,
          QuoteRejectReasonEnum.UnknownSymbol,
          RfqRejectMessages.SYMBOL_EMPTY,
          productType);
      metrics.rejectSymbolEmpty++;
      metrics.emitRejected++;
      return;
    }

    // 4. Account validation
    final AccountState account =
        accountStore.getByCodeBytes(accountCodeScratch, 0, accountCodeLen());
    if (account == null || account.status() != AccountStatusEnum.Active) {
      emitRejectByQuoteReqId(
          session,
          clusterTimestamp,
          eventSink,
          QuoteRejectReasonEnum.Other,
          RfqRejectMessages.ACCOUNT_INACTIVE,
          productType);
      metrics.rejectAccountInactive++;
      metrics.emitRejected++;
      return;
    }

    // 5. CAN_RFQ entitlement
    if ((account.capabilities() & AccountState.Capabilities.CAN_RFQ) == 0L) {
      emitRejectByQuoteReqId(
          session,
          clusterTimestamp,
          eventSink,
          QuoteRejectReasonEnum.Other,
          RfqRejectMessages.RFQ_NOT_PERMITTED,
          productType);
      metrics.rejectRfqNotPermitted++;
      metrics.emitRejected++;
      return;
    }

    // 6. Currency validation
    if (!currencyStore.contains(packCurrency(currencyScratch))
        || !currencyStore.contains(packCurrency(settlCurrencyScratch))) {
      emitRejectByQuoteReqId(
          session,
          clusterTimestamp,
          eventSink,
          QuoteRejectReasonEnum.Other,
          RfqRejectMessages.CURRENCY_UNKNOWN,
          productType);
      metrics.rejectCurrencyUnknown++;
      metrics.emitRejected++;
      return;
    }

    // 7. Per-session rate limit
    final long sessionId = session != null ? session.id() : 0L;
    if (!rfqStateMachine.rateLimitTryConsume(sessionId, clusterTimestamp)) {
      emitRejectByQuoteReqId(
          session,
          clusterTimestamp,
          eventSink,
          QuoteRejectReasonEnum.TooLateToEnter,
          RfqRejectMessages.RATE_LIMIT,
          productType);
      metrics.rejectRateLimit++;
      metrics.emitRejected++;
      return;
    }

    // 8. Duplicate / recently-terminal detection
    final RfqSlot existing =
        rfqStateMachine.lookupByQuoteReqId(quoteReqIdScratch, 0, RfqSlot.QUOTE_REQ_ID_LENGTH);
    if (existing != null) {
      // Duplicate quoteReqId in REQUESTED or QUOTED — emit 106 "duplicate".
      emitRejectByQuoteReqId(
          session,
          clusterTimestamp,
          eventSink,
          QuoteRejectReasonEnum.Other,
          RfqRejectMessages.DUPLICATE,
          productType);
      metrics.rejectDuplicate++;
      metrics.emitRejected++;
      return;
    }
    final byte terminalReason =
        rfqStateMachine.recentlyTerminalReason(
            quoteReqIdScratchBuffer, 0, RfqSlot.QUOTE_REQ_ID_LENGTH);
    if (terminalReason != 0) {
      // Already-terminal — silently drop.
      metrics.dropAfterTerminal++;
      return;
    }

    // 9. Pool acquisition
    final RfqSlot slot = rfqStateMachine.acquire();
    if (slot == null) {
      emitRejectByQuoteReqId(
          session,
          clusterTimestamp,
          eventSink,
          QuoteRejectReasonEnum.TooLateToEnter,
          RfqRejectMessages.POOL_EXHAUSTED,
          productType);
      metrics.rejectPoolExhausted++;
      metrics.emitRejected++;
      return;
    }

    // 10. Populate slot identity fields (byte arrays mutated here)
    System.arraycopy(quoteReqIdScratch, 0, slot.quoteReqIdBytes, 0, RfqSlot.QUOTE_REQ_ID_LENGTH);
    System.arraycopy(symbolScratch, 0, slot.symbolBytes, 0, RfqSlot.SYMBOL_LENGTH);
    System.arraycopy(accountCodeScratch, 0, slot.accountCodeBytes, 0, RfqSlot.ACCOUNT_CODE_LENGTH);
    System.arraycopy(settlDateScratch, 0, slot.settlDateBytes, 0, RfqSlot.SETTL_DATE_LENGTH);
    System.arraycopy(currencyScratch, 0, slot.currencyBytes, 0, RfqSlot.CURRENCY_LENGTH);
    System.arraycopy(
        settlCurrencyScratch, 0, slot.settlCurrencyBytes, 0, RfqSlot.SETTL_CURRENCY_LENGTH);
    slot.side = (byte) sideEnum.value();
    slot.productType = (byte) productType.value();
    slot.settlType = settlType == SettlTypeEnum.NULL_VAL ? 0 : (byte) settlType.value();
    slot.tenor = (byte) tenor.value();
    slot.orderQty = orderQty;
    slot.accountId = account.accountId();
    slot.sessionId = sessionId;
    slot.transactTime = clusterTimestamp;

    // Decode legs — bounds-check first to prevent ArrayIndexOutOfBoundsException (DoS vector).
    // RfqSlot.MAX_LEGS = 2 (Spot/Forward swap legs); a hostile or buggy client sending more
    // legs would crash the cluster duty cycle without this guard.
    final QuoteRequestDecoder.NoLegsDecoder legGrp = qrDecoder.noLegs();
    final int legCount = legGrp.count();
    if (legCount > RfqSlot.MAX_LEGS) {
      // Release the just-acquired slot before rejecting; slot is not yet in any lookup map.
      rfqStateMachine.release(slot);
      emitRejectByQuoteReqId(
          session,
          clusterTimestamp,
          eventSink,
          QuoteRejectReasonEnum.Other,
          RfqRejectMessages.MALFORMED,
          productType);
      metrics.rejectMalformed++;
      metrics.emitRejected++;
      return;
    }
    slot.noLegs = legCount;
    int legIdx = 0;
    while (legGrp.hasNext()) {
      legGrp.next();
      slot.legSide[legIdx] = (byte) legGrp.legSide().value();
      legGrp.getLegSettlDate(slot.legSettlDate[legIdx], 0);
      final SettlTypeEnum legSt = legGrp.legSettlType();
      slot.legSettlType[legIdx] = legSt == SettlTypeEnum.NULL_VAL ? 0 : (byte) legSt.value();
      legGrp.getLegCurrency(slot.legCurrency[legIdx], 0);
      slot.legTenor[legIdx] = (byte) legGrp.legTenor().value();
      slot.legOrderQty[legIdx] = legGrp.legOrderQty();
      legIdx++;
    }

    // Sync the byQuoteReqId key to the populated bytes.
    slot.syncQuoteReqIdKey();

    // 11. Compute correlation IDs
    slot.requestTimeoutCorrelationId = rfqStateMachine.requestTimeoutCorrelationFor(slot);
    slot.timerCorrelationId = rfqStateMachine.ttlCorrelationFor(slot);

    // 12. Schedule request-timeout timer FIRST (no journal-then-rollback hazard)
    if (cluster == null) {
      // Test path — skip timer; commit slot anyway.
    } else {
      final boolean ok =
          cluster.scheduleTimer(
              slot.requestTimeoutCorrelationId,
              clusterTimestamp + rfqStateMachine.requestTimeoutNanos());
      if (!ok) {
        // Rollback: release slot before any 104 emission.
        rfqStateMachine.release(slot);
        emitRejectByQuoteReqId(
            session,
            clusterTimestamp,
            eventSink,
            QuoteRejectReasonEnum.TooLateToEnter,
            RfqRejectMessages.TIMER_POOL_EXHAUSTED,
            productType);
        metrics.rejectTimerExhausted++;
        metrics.emitRejected++;
        return;
      }
    }

    // 13. Encode 104 into pre-allocated buffer
    requestedEncoder.wrapAndApplyHeader(egressBuffer, 0, headerEncoder);
    requestedEncoder.sequenceNumber(0L);
    requestedEncoder.timestamp(0L);
    requestedEncoder.putQuoteReqId(slot.quoteReqIdBytes, 0);
    requestedEncoder.putSymbol(slot.symbolBytes, 0);
    requestedEncoder.side(sideEnum);
    requestedEncoder.orderQty(orderQty);
    requestedEncoder.putAccountCode(slot.accountCodeBytes, 0);
    requestedEncoder.productType(productType);
    requestedEncoder.putSettlDate(slot.settlDateBytes, 0);
    requestedEncoder.settlType(settlType);
    requestedEncoder.putCurrency(slot.currencyBytes, 0);
    requestedEncoder.putSettlCurrency(slot.settlCurrencyBytes, 0);
    requestedEncoder.tenor(tenor);

    final QuoteRequestedEventEncoder.NoLegsEncoder outLegGrp =
        requestedEncoder.noLegsCount(slot.noLegs);
    for (int j = 0; j < slot.noLegs; j++) {
      outLegGrp.next();
      outLegGrp.legSide(SideEnum.get(slot.legSide[j]));
      outLegGrp.putLegSettlDate(slot.legSettlDate[j], 0);
      outLegGrp.legSettlType(
          slot.legSettlType[j] == 0
              ? SettlTypeEnum.NULL_VAL
              : SettlTypeEnum.get(slot.legSettlType[j]));
      outLegGrp.putLegCurrency(slot.legCurrency[j], 0);
      outLegGrp.legTenor(TenorEnum.get(slot.legTenor[j]));
      outLegGrp.legOrderQty(slot.legOrderQty[j]);
    }

    final int len = HDR_LEN + requestedEncoder.encodedLength();
    if (len > egressBuffer.capacity()) {
      throw new IllegalStateException("104 encode overflow: " + len);
    }

    // 14. Emit 104 (any throw aborts cluster — acceptable per monotonicity contract)
    eventSink.emit(session, clusterTimestamp, egressBuffer, 0, len);

    // 15. Commit slot to lookup maps (LAST step — emit succeeded)
    rfqStateMachine.registerRequested(slot);
    metrics.emitRequested++;
  }

  /**
   * Emits a 106 reject with quoteReqId+side+symbol+accountCode populated from the scratch arrays.
   */
  private void emitRejectByQuoteReqId(
      final ClientSession session,
      final long clusterTimestamp,
      final EventSink eventSink,
      final QuoteRejectReasonEnum reason,
      final byte[] text,
      final ProductTypeEnum productType) {
    rejectedEncoder.wrapAndApplyHeader(egressBuffer, 0, headerEncoder);
    rejectedEncoder.sequenceNumber(0L);
    rejectedEncoder.timestamp(0L);
    rejectedEncoder.putQuoteReqId(quoteReqIdScratch, 0);
    rejectedEncoder.putSymbol(symbolScratch, 0);
    rejectedEncoder.side(qrDecoder.side());
    rejectedEncoder.putAccountCode(accountCodeScratch, 0);
    rejectedEncoder.quoteRejectReason(reason);
    rejectedEncoder.productType(productType);
    rejectedEncoder.putText(text, 0);
    final int len = HDR_LEN + rejectedEncoder.encodedLength();
    if (len > egressBuffer.capacity()) {
      throw new IllegalStateException("106 encode overflow: " + len);
    }
    eventSink.emit(session, clusterTimestamp, egressBuffer, 0, len);
  }

  /**
   * Emits 106 for a malformed inbound message — no decoder fields are valid.
   *
   * <p>The egress buffer is reused across emits, so any field NOT explicitly written by an SBE
   * setter retains stale bytes from the previous message at that body offset. Explicitly zero every
   * char-array / enum field so a malformed-path 106 cannot ship the previous emit's QuoteReqID /
   * Symbol / AccountCode / Side / SettlDate / Currency / SettlCurrency content to the wire.
   */
  private void emitMalformed(
      final ClientSession session, final long clusterTimestamp, final EventSink eventSink) {
    rejectedEncoder.wrapAndApplyHeader(egressBuffer, 0, headerEncoder);
    rejectedEncoder.sequenceNumber(0L);
    rejectedEncoder.timestamp(0L);
    // Zero every field that would otherwise carry over from the prior emit at the same offset.
    rejectedEncoder.putQuoteReqId(ZERO_QUOTE_REQ_ID, 0);
    rejectedEncoder.putSymbol(ZERO_SYMBOL, 0);
    rejectedEncoder.side(SideEnum.NULL_VAL);
    rejectedEncoder.putAccountCode(ZERO_ACCOUNT_CODE, 0);
    rejectedEncoder.quoteRejectReason(QuoteRejectReasonEnum.Other);
    rejectedEncoder.productType(ProductTypeEnum.NULL_VAL);
    rejectedEncoder.putText(RfqRejectMessages.MALFORMED, 0);
    final int len = HDR_LEN + rejectedEncoder.encodedLength();
    eventSink.emit(session, clusterTimestamp, egressBuffer, 0, len);
    metrics.rejectMalformed++;
    metrics.emitRejected++;
  }

  /** Zero-filled scratch arrays for emitMalformed (no per-call allocation). */
  private static final byte[] ZERO_QUOTE_REQ_ID = new byte[RfqSlot.QUOTE_REQ_ID_LENGTH];

  private static final byte[] ZERO_SYMBOL = new byte[RfqSlot.SYMBOL_LENGTH];
  private static final byte[] ZERO_ACCOUNT_CODE = new byte[RfqSlot.ACCOUNT_CODE_LENGTH];

  /** Returns the actual length of the populated accountCode (trims trailing NULs). */
  private int accountCodeLen() {
    int len = RfqSlot.ACCOUNT_CODE_LENGTH;
    while (len > 0 && accountCodeScratch[len - 1] == 0) {
      len--;
    }
    return len;
  }

  /** Packs a 3-byte currency into an int for {@link CurrencyStore#contains}. */
  private static int packCurrency(final byte[] ccy) {
    return ((ccy[0] & 0xFF) << 16) | ((ccy[1] & 0xFF) << 8) | (ccy[2] & 0xFF);
  }
}
