package com.trading.engine.cluster.handler;

import com.trading.engine.cluster.IdGenerator;
import com.trading.engine.cluster.metrics.RfqMetrics;
import com.trading.engine.cluster.state.RfqSlot;
import com.trading.engine.cluster.state.RfqSlotState;
import com.trading.engine.cluster.state.RfqStateMachine;
import com.trading.engine.messages.sbe.BooleanType;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.PriceResponseDecoder;
import com.trading.engine.messages.sbe.ProductTypeEnum;
import com.trading.engine.messages.sbe.QuoteCreatedEventEncoder;
import com.trading.engine.messages.sbe.QuoteRejectReasonEnum;
import com.trading.engine.messages.sbe.QuoteRejectedEventEncoder;
import com.trading.engine.messages.sbe.SettlTypeEnum;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.messages.sbe.TenorEnum;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import java.util.Objects;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Handler for {@code PriceResponse} (template 51). Looks up the in-flight RFQ slot by quoteReqId
 * and, depending on slot state and the {@code accepted} field, either emits {@code QuoteCreatedEvent}
 * (105) with a freshly-minted quoteId and an armed TTL timer, or emits {@code QuoteRejectedEvent}
 * (106) with reason {@code InvalidPrice} and releases the slot.
 *
 * <p>State table per plan §9.2:
 *
 * <pre>
 * not found            any        → silent drop (rfq.drop.unknownReqId)
 * REQUESTED            true       → 105 + TTL timer (or 106 if timer pool exhausted)
 * REQUESTED            false      → 106 reason=InvalidPrice text="pricing rejected"
 * QUOTED               any        → silent drop (idempotent — 105 already emitted)
 * EXPIRED / REJECTED   any        → silent drop (terminal)
 * malformed            -          → silent drop with WARN
 * wrong templateId     -          → throw IllegalStateException (caught by ErrorHandler)
 * </pre>
 *
 * <p><b>Threading:</b> single-threaded cluster duty cycle.
 *
 * <p><b>Allocation:</b> zero allocation after construction.
 *
 * @see QuoteRequestHandler
 * @see RfqStateMachine
 * @see EventSink
 */
public final class PriceResponseHandler implements CommandHandler {

  private static final int HDR_LEN = MessageHeaderEncoder.ENCODED_LENGTH;
  private static final int EGRESS_BUFFER_SIZE = 8192;

  // ---- Pre-allocated SBE flyweights ----
  private final PriceResponseDecoder prDecoder = new PriceResponseDecoder();
  private final QuoteCreatedEventEncoder createdEncoder = new QuoteCreatedEventEncoder();
  private final QuoteRejectedEventEncoder rejectedEncoder = new QuoteRejectedEventEncoder();
  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  private final UnsafeBuffer egressBuffer = new UnsafeBuffer(new byte[EGRESS_BUFFER_SIZE]);

  // ---- Injected dependencies ----
  private final RfqStateMachine rfqStateMachine;
  private final IdGenerator quoteIdGen;
  private final RfqMetrics metrics;
  private Cluster cluster;

  /**
   * Constructs a {@link PriceResponseHandler}.
   *
   * @param rfqStateMachine the RFQ state machine
   * @param quoteIdGen deterministic QTE-prefixed ID generator
   * @param metrics observability counters
   */
  public PriceResponseHandler(
      final RfqStateMachine rfqStateMachine,
      final IdGenerator quoteIdGen,
      final RfqMetrics metrics) {
    this.rfqStateMachine = Objects.requireNonNull(rfqStateMachine, "rfqStateMachine");
    this.quoteIdGen = Objects.requireNonNull(quoteIdGen, "quoteIdGen");
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
    return PriceResponseDecoder.TEMPLATE_ID;
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
    if (length < HDR_LEN + PriceResponseDecoder.BLOCK_LENGTH) {
      metrics.dropMalformedPriceResponse++;
      return;
    }

    // 2. Decode wrap
    prDecoder.wrap(buffer, offset + HDR_LEN, blockLength, version);
    final byte[] reqIdScratch = new byte[RfqSlot.QUOTE_REQ_ID_LENGTH];
    prDecoder.getQuoteReqId(reqIdScratch, 0);

    // 3. Slot lookup
    final RfqSlot slot =
        rfqStateMachine.lookupByQuoteReqId(reqIdScratch, 0, RfqSlot.QUOTE_REQ_ID_LENGTH);
    if (slot == null) {
      metrics.dropUnknownReqId++;
      return;
    }
    if (slot.state == RfqSlotState.QUOTED) {
      metrics.dropAlreadyQuoted++;
      return;
    }
    if (slot.state != RfqSlotState.REQUESTED) {
      metrics.dropTerminal++;
      return;
    }

    final BooleanType accepted = prDecoder.accepted();

    // 4. Pricing rejected → emit 106 + release
    if (accepted != BooleanType.True) {
      emitPricingRejected(slot, session, clusterTimestamp, eventSink);
      rfqStateMachine.release(slot);
      metrics.rejectPricingDeclined++;
      metrics.emitRejected++;
      return;
    }

    // 5. Pricing accepted — read prices + sizes
    final long bidPx = prDecoder.bidPx();
    final long offerPx = prDecoder.offerPx();
    final long bidSize = prDecoder.bidSize();
    final long offerSize = prDecoder.offerSize();
    final long swapPoints = prDecoder.swapPoints();

    // 6. Compute validUntil and TTL correlation; schedule TTL timer FIRST.
    slot.validUntil = clusterTimestamp + rfqStateMachine.ttlForProduct(slot.productType);
    if (cluster != null) {
      final boolean ok = cluster.scheduleTimer(slot.timerCorrelationId, slot.validUntil);
      if (!ok) {
        // Rollback: do NOT advance QTE counter, do NOT emit 105. Emit 106 instead.
        emitTimerExhausted(slot, session, clusterTimestamp, eventSink);
        rfqStateMachine.release(slot);
        metrics.rejectTimerExhausted++;
        metrics.emitRejected++;
        return;
      }
    }

    // 7. Mint quoteId (advances QTE counter)
    quoteIdGen.nextInto(slot.quoteIdBuffer, 0);

    // 8. Populate slot pricing fields
    slot.bidPx = bidPx;
    slot.offerPx = offerPx;
    slot.bidSize = bidSize;
    slot.offerSize = offerSize;
    slot.swapPoints = swapPoints;

    // 9. Encode 105
    createdEncoder.wrapAndApplyHeader(egressBuffer, 0, headerEncoder);
    createdEncoder.sequenceNumber(0L);
    createdEncoder.timestamp(0L);
    createdEncoder.putQuoteId(slot.quoteIdBytes, 0);
    createdEncoder.putQuoteReqId(slot.quoteReqIdBytes, 0);
    createdEncoder.putSymbol(slot.symbolBytes, 0);
    createdEncoder.side(SideEnum.get(slot.side));
    createdEncoder.putAccountCode(slot.accountCodeBytes, 0);
    createdEncoder.bidPx(bidPx);
    createdEncoder.offerPx(offerPx);
    createdEncoder.bidSize(bidSize);
    createdEncoder.offerSize(offerSize);
    createdEncoder.validUntil(slot.validUntil);
    createdEncoder.productType(ProductTypeEnum.get(slot.productType));
    createdEncoder.putSettlDate(slot.settlDateBytes, 0);
    createdEncoder.settlType(slot.settlType == 0
        ? SettlTypeEnum.NULL_VAL
        : SettlTypeEnum.get(slot.settlType));
    createdEncoder.putCurrency(slot.currencyBytes, 0);
    createdEncoder.putSettlCurrency(slot.settlCurrencyBytes, 0);
    createdEncoder.tenor(TenorEnum.get(slot.tenor));
    createdEncoder.swapPoints(swapPoints);

    // Legs (PriceResponse may carry leg-level prices we propagate to the event)
    final QuoteCreatedEventEncoder.NoLegsEncoder outLegGrp = createdEncoder.noLegsCount(slot.noLegs);
    for (int j = 0; j < slot.noLegs; j++) {
      outLegGrp.next();
      outLegGrp.legSide(SideEnum.get(slot.legSide[j]));
      outLegGrp.putLegSettlDate(slot.legSettlDate[j], 0);
      outLegGrp.legSettlType(slot.legSettlType[j] == 0
          ? SettlTypeEnum.NULL_VAL
          : SettlTypeEnum.get(slot.legSettlType[j]));
      outLegGrp.putLegCurrency(slot.legCurrency[j], 0);
      outLegGrp.legBidPx(slot.legBidPx[j]);
      outLegGrp.legOfferPx(slot.legOfferPx[j]);
      outLegGrp.legBidSize(slot.legBidSize[j]);
      outLegGrp.legOfferSize(slot.legOfferSize[j]);
    }

    final int len = HDR_LEN + createdEncoder.encodedLength();
    if (len > egressBuffer.capacity()) {
      throw new IllegalStateException("105 encode overflow: " + len);
    }

    // 10. Emit 105 (after this returns, slot is durably QUOTED)
    eventSink.emit(session, clusterTimestamp, egressBuffer, 0, len);

    // 11. Update slot state and lookup maps
    slot.state = RfqSlotState.QUOTED;
    slot.syncQuoteIdKey();
    rfqStateMachine.registerQuoted(slot);
    metrics.emitCreated++;
  }

  /** Emits 106 reason=InvalidPrice text="pricing rejected" with full slot identity. */
  private void emitPricingRejected(
      final RfqSlot slot,
      final ClientSession session,
      final long clusterTimestamp,
      final EventSink eventSink) {
    rejectedEncoder.wrapAndApplyHeader(egressBuffer, 0, headerEncoder);
    rejectedEncoder.sequenceNumber(0L);
    rejectedEncoder.timestamp(0L);
    rejectedEncoder.putQuoteReqId(slot.quoteReqIdBytes, 0);
    rejectedEncoder.putSymbol(slot.symbolBytes, 0);
    rejectedEncoder.side(SideEnum.get(slot.side));
    rejectedEncoder.putAccountCode(slot.accountCodeBytes, 0);
    rejectedEncoder.quoteRejectReason(QuoteRejectReasonEnum.InvalidPrice);
    rejectedEncoder.productType(ProductTypeEnum.get(slot.productType));
    rejectedEncoder.putText(RfqRejectMessages.PRICING_REJECTED, 0);
    final int len = HDR_LEN + rejectedEncoder.encodedLength();
    eventSink.emit(session, clusterTimestamp, egressBuffer, 0, len);
  }

  /** Emits 106 reason=TooLateToEnter text="timer pool exhausted" — rollback path. */
  private void emitTimerExhausted(
      final RfqSlot slot,
      final ClientSession session,
      final long clusterTimestamp,
      final EventSink eventSink) {
    rejectedEncoder.wrapAndApplyHeader(egressBuffer, 0, headerEncoder);
    rejectedEncoder.sequenceNumber(0L);
    rejectedEncoder.timestamp(0L);
    rejectedEncoder.putQuoteReqId(slot.quoteReqIdBytes, 0);
    rejectedEncoder.putSymbol(slot.symbolBytes, 0);
    rejectedEncoder.side(SideEnum.get(slot.side));
    rejectedEncoder.putAccountCode(slot.accountCodeBytes, 0);
    rejectedEncoder.quoteRejectReason(QuoteRejectReasonEnum.TooLateToEnter);
    rejectedEncoder.productType(ProductTypeEnum.get(slot.productType));
    rejectedEncoder.putText(RfqRejectMessages.TIMER_POOL_EXHAUSTED, 0);
    final int len = HDR_LEN + rejectedEncoder.encodedLength();
    eventSink.emit(session, clusterTimestamp, egressBuffer, 0, len);
  }
}
