package com.trading.engine.projections.quote;

import com.trading.engine.messages.sbe.ProductTypeEnum;
import com.trading.engine.messages.sbe.QuoteRejectReasonEnum;
import com.trading.engine.messages.sbe.SettlTypeEnum;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.messages.sbe.TenorEnum;
import com.trading.engine.projections.ProjectionUtil;

/**
 * Immutable snapshot of a quote's state at a point in time. Returned by {@link QuoteProjection}
 * query methods to provide a thread-safe, detached view of quote state.
 *
 * <p><b>Threading:</b> immutable — safe to share across threads without synchronization.
 *
 * <p><b>Allocation:</b> one instance per query result. Created by copying fields from the internal
 * mutable {@link QuoteView} under the projection's read lock.
 *
 * @param quoteId FIX tag 117: quote identifier (empty for Requested-only quotes that have not yet
 *     received a QuoteCreatedEvent)
 * @param quoteReqId FIX tag 131: quote request identifier
 * @param symbol FIX tag 55: instrument symbol
 * @param accountCode FIX tag 1: account code
 * @param side FIX tag 54: order side
 * @param productType product type classification (Spot, Forward, Swap)
 * @param status projection-local lifecycle state (Requested, Active, Rejected, Expired, Used)
 * @param rejectReason FIX tag 658: quote reject reason ({@code NULL_VAL} if not rejected)
 * @param bidPx FIX tag 132: bid price, fixed-point 10^-8
 * @param offerPx FIX tag 133: offer price, fixed-point 10^-8
 * @param bidSize FIX tag 134: bid size, fixed-point 10^-8
 * @param offerSize FIX tag 135: offer size, fixed-point 10^-8
 * @param orderQty FIX tag 38: client-requested quantity, fixed-point 10^-8 (from
 *     QuoteRequestedEvent; 0 if no prior request)
 * @param swapPoints FX swap points from QuoteCreatedEvent (optional; {@code Long.MIN_VALUE} = not
 *     applicable for non-swap quotes — SBE int64 null sentinel)
 * @param validUntil FIX tag 62: quote expiry timestamp, epoch nanos (NOT fixed-point)
 * @param text FIX tag 58: free-text reject reason from QuoteRejectedEvent (empty if not rejected)
 * @param settlDate FIX tag 64: settlement date YYYYMMDD (empty for rejected-only quotes)
 * @param settlType FIX tag 63: settlement type ({@code NULL_VAL} for rejected-only quotes)
 * @param currency FIX tag 15: dealt currency ISO 4217 (empty for rejected-only quotes)
 * @param settlCurrency FIX tag 120: settlement currency ISO 4217 (empty for rejected-only quotes)
 * @param tenor tenor classification ({@code NULL_VAL} for rejected-only quotes)
 * @param responseLatencyNanos derived latency: QuoteCreated.timestamp - QuoteRequested.timestamp
 *     ({@code -1L} = not measured, e.g. QuoteCreated arrived without prior QuoteRequested)
 * @param sequenceNumber event sequence number of the most recently applied event
 * @param createdAt cluster timestamp (epoch nanos) of the first event (RFQ receipt time)
 * @param lastUpdatedAt cluster timestamp (epoch nanos) of the most recent event
 */
public record QuoteSnapshot(
    String quoteId,
    String quoteReqId,
    String symbol,
    String accountCode,
    SideEnum side,
    ProductTypeEnum productType,
    QuoteStatus status,
    QuoteRejectReasonEnum rejectReason,
    long bidPx,
    long offerPx,
    long bidSize,
    long offerSize,
    long orderQty,
    long swapPoints,
    long validUntil,
    String text,
    String settlDate,
    SettlTypeEnum settlType,
    String currency,
    String settlCurrency,
    TenorEnum tenor,
    long responseLatencyNanos,
    long sequenceNumber,
    long createdAt,
    long lastUpdatedAt) {

  /**
   * Creates an immutable snapshot by copying all fields from a mutable {@link QuoteView}. String
   * fields are decoded from SBE byte arrays using US-ASCII.
   *
   * <p>Must be called under the projection's read lock (or write lock during snapshot creation
   * inside event dispatch).
   *
   * @param v the mutable quote view to copy from
   * @return a new immutable snapshot
   */
  static QuoteSnapshot from(final QuoteView v) {
    return new QuoteSnapshot(
        ProjectionUtil.asciiString(v.quoteId(), v.quoteIdLen()),
        ProjectionUtil.asciiString(v.quoteReqId(), v.quoteReqIdLen()),
        ProjectionUtil.asciiString(v.symbol(), v.symbolLen()),
        ProjectionUtil.asciiString(v.accountCode(), v.accountCodeLen()),
        v.side(),
        v.productType(),
        v.status(),
        v.rejectReason(),
        v.bidPx(),
        v.offerPx(),
        v.bidSize(),
        v.offerSize(),
        v.orderQty(),
        v.swapPoints(),
        v.validUntil(),
        ProjectionUtil.asciiString(v.text(), v.textLen()),
        ProjectionUtil.asciiString(v.settlDate(), v.settlDateLen()),
        v.settlType(),
        ProjectionUtil.asciiString(v.currency(), v.currencyLen()),
        ProjectionUtil.asciiString(v.settlCurrency(), v.settlCurrencyLen()),
        v.tenor(),
        v.responseLatencyNanos(),
        v.sequenceNumber(),
        v.createdAt(),
        v.lastUpdatedAt());
  }
}
