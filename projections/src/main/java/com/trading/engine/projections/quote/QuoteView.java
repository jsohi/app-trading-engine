package com.trading.engine.projections.quote;

import com.trading.engine.messages.sbe.ProductTypeEnum;
import com.trading.engine.messages.sbe.QuoteRejectReasonEnum;
import com.trading.engine.messages.sbe.SettlTypeEnum;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.messages.sbe.TenorEnum;

/**
 * Mutable internal read-model for a single quote. Tracks the full RFQ lifecycle from initial
 * request through pricing to terminal disposition (Expired, Used, Rejected).
 *
 * <p><b>Ownership:</b> instances are owned exclusively by {@link QuoteProjection}. All setters are
 * package-private — only {@code QuoteProjection} mutates this object. Byte-array getters are also
 * package-private to prevent leaking mutable internal state.
 *
 * <p><b>Identity semantics:</b> do NOT override {@code equals()} or {@code hashCode()} on this
 * class. {@link org.agrona.collections.ObjectHashSet} in {@code QuoteProjection} relies on identity
 * equality for the symbol and account indexes.
 *
 * <p><b>Threading:</b> accessed single-threaded from the event-dispatch thread under the
 * projection's write lock. Query threads never see this object directly — they receive an immutable
 * {@link QuoteSnapshot} copy.
 *
 * <p><b>Allocation:</b> one instance per quote, allocated on the first event for that quoteReqId.
 * Fields are mutated in-place on subsequent events. No per-event allocation.
 *
 * @see QuoteSnapshot
 * @see QuoteProjection
 */
final class QuoteView {

  // --- Fixed-length byte array fields matching SBE schema types ---

  /** FIX tag 117: quote identifier. SBE QuoteID char[20]. Set on QuoteCreatedEvent (105). */
  private final byte[] quoteId = new byte[20];

  private int quoteIdLen;

  /** FIX tag 131: quote request identifier. SBE QuoteReqID char[20]. Set on all quote events. */
  private final byte[] quoteReqId = new byte[20];

  private int quoteReqIdLen;

  /** FIX tag 55: instrument symbol. SBE Symbol char[8]. */
  private final byte[] symbol = new byte[8];

  private int symbolLen;

  /** FIX tag 1: account code. SBE Account char[16]. */
  private final byte[] accountCode = new byte[16];

  private int accountCodeLen;

  /** FIX tag 58: free-text reason. SBE Text char[64]. Set on QuoteRejectedEvent (106) only. */
  private final byte[] text = new byte[64];

  private int textLen;

  /** FIX tag 64: settlement date YYYYMMDD. SBE SettlDate char[8]. Set on events 104, 105. */
  private final byte[] settlDate = new byte[8];

  private int settlDateLen;

  /** FIX tag 15: dealt currency ISO 4217. SBE Currency char[3]. Set on events 104, 105. */
  private final byte[] currency = new byte[3];

  private int currencyLen;

  /** FIX tag 120: settlement currency. SBE Currency char[3]. Set on events 104, 105. */
  private final byte[] settlCurrency = new byte[3];

  private int settlCurrencyLen;

  // --- Enum fields ---

  /** FIX tag 54: order side. */
  private SideEnum side = SideEnum.NULL_VAL;

  /** Product type classification (Spot, Forward, Swap). */
  private ProductTypeEnum productType = ProductTypeEnum.NULL_VAL;

  /** Projection-local lifecycle state. Not a wire-level enum. */
  private QuoteStatus status = QuoteStatus.Requested;

  /** FIX tag 658: reject reason. Populated only on QuoteRejectedEvent (106). */
  private QuoteRejectReasonEnum rejectReason = QuoteRejectReasonEnum.NULL_VAL;

  /** FIX tag 63: settlement type (optional). Set on events 104, 105. */
  private SettlTypeEnum settlType = SettlTypeEnum.NULL_VAL;

  /** Tenor classification (SN, ON, 1W, 1M, etc.). Set on events 104, 105. */
  private TenorEnum tenor = TenorEnum.NULL_VAL;

  // --- Numeric fields (fixed-point 10^-8, PRICE_SCALE) ---

  /** FIX tag 132: bid price, fixed-point 10^-8. Set on QuoteCreatedEvent (105). */
  private long bidPx;

  /** FIX tag 133: offer price, fixed-point 10^-8. Set on QuoteCreatedEvent (105). */
  private long offerPx;

  /** FIX tag 134: bid size, fixed-point 10^-8. Set on QuoteCreatedEvent (105). */
  private long bidSize;

  /** FIX tag 135: offer size, fixed-point 10^-8. Set on QuoteCreatedEvent (105). */
  private long offerSize;

  /** FIX tag 38: client-requested quantity, fixed-point 10^-8. Set on QuoteRequestedEvent (104). */
  private long orderQty;

  /**
   * FX swap points (optional). Set on QuoteCreatedEvent (105). {@code Long.MIN_VALUE} (SBE int64
   * null sentinel) means not applicable (non-swap quote or not yet priced).
   */
  private long swapPoints = Long.MIN_VALUE;

  // --- Timestamps (epoch nanos, NOT fixed-point) ---

  /**
   * FIX tag 62: quote expiry timestamp, epoch nanos. Set on QuoteCreatedEvent (105). Not
   * fixed-point — this is a nanosecond timestamp.
   */
  private long validUntil;

  /** Event sequence number from the most recently applied event. */
  private long sequenceNumber;

  /**
   * Cluster timestamp (epoch nanos) of the first event for this quote (QuoteRequestedEvent or
   * QuoteCreatedEvent). Set once, never overwritten.
   */
  private long createdAt;

  /** Cluster timestamp (epoch nanos) of the most recently applied event. */
  private long lastUpdatedAt;

  /**
   * Derived latency: {@code QuoteCreatedEvent.timestamp - QuoteRequestedEvent.timestamp}. {@code
   * -1L} if QuoteCreated arrived without a prior QuoteRequested (sentinel — see DD-9).
   */
  private long responseLatencyNanos = -1L;

  // --- Public getters (primitives and enums) ---

  public SideEnum side() {
    return side;
  }

  public ProductTypeEnum productType() {
    return productType;
  }

  public QuoteStatus status() {
    return status;
  }

  public QuoteRejectReasonEnum rejectReason() {
    return rejectReason;
  }

  public SettlTypeEnum settlType() {
    return settlType;
  }

  public TenorEnum tenor() {
    return tenor;
  }

  public long bidPx() {
    return bidPx;
  }

  public long offerPx() {
    return offerPx;
  }

  public long bidSize() {
    return bidSize;
  }

  public long offerSize() {
    return offerSize;
  }

  public long orderQty() {
    return orderQty;
  }

  public long swapPoints() {
    return swapPoints;
  }

  public long validUntil() {
    return validUntil;
  }

  public long sequenceNumber() {
    return sequenceNumber;
  }

  public long createdAt() {
    return createdAt;
  }

  public long lastUpdatedAt() {
    return lastUpdatedAt;
  }

  public long responseLatencyNanos() {
    return responseLatencyNanos;
  }

  /**
   * Returns {@code true} if the quote is in {@link QuoteStatus#Active} state.
   *
   * @return {@code true} if status is Active
   */
  public boolean isActive() {
    return status == QuoteStatus.Active;
  }

  /**
   * Returns {@code true} if the quote is in-flight (waiting for pricing or client action).
   *
   * @return {@code true} if status is {@link QuoteStatus#Requested} or {@link QuoteStatus#Active}
   */
  public boolean isInFlight() {
    return status == QuoteStatus.Requested || status == QuoteStatus.Active;
  }

  /**
   * Returns {@code true} if the quote is in a terminal state (no further transitions possible).
   *
   * @return {@code true} if status is {@link QuoteStatus#Rejected}, {@link QuoteStatus#Expired}, or
   *     {@link QuoteStatus#Used}
   */
  public boolean isTerminal() {
    return status == QuoteStatus.Rejected
        || status == QuoteStatus.Expired
        || status == QuoteStatus.Used;
  }

  // --- Package-private byte array getters (prevent leaking mutable state) ---

  byte[] quoteId() {
    return quoteId;
  }

  int quoteIdLen() {
    return quoteIdLen;
  }

  byte[] quoteReqId() {
    return quoteReqId;
  }

  int quoteReqIdLen() {
    return quoteReqIdLen;
  }

  byte[] symbol() {
    return symbol;
  }

  int symbolLen() {
    return symbolLen;
  }

  byte[] accountCode() {
    return accountCode;
  }

  int accountCodeLen() {
    return accountCodeLen;
  }

  byte[] text() {
    return text;
  }

  int textLen() {
    return textLen;
  }

  byte[] settlDate() {
    return settlDate;
  }

  int settlDateLen() {
    return settlDateLen;
  }

  byte[] currency() {
    return currency;
  }

  int currencyLen() {
    return currencyLen;
  }

  byte[] settlCurrency() {
    return settlCurrency;
  }

  int settlCurrencyLen() {
    return settlCurrencyLen;
  }

  // --- Package-private setters (only QuoteProjection mutates) ---

  void setQuoteId(final byte[] src, final int offset, final int length) {
    System.arraycopy(src, offset, quoteId, 0, length);
    // Zero trailing bytes in case a shorter quoteId replaces a longer one (defensive)
    for (int i = length; i < quoteId.length; i++) {
      quoteId[i] = 0;
    }
    quoteIdLen = length;
  }

  void setQuoteReqId(final byte[] src, final int offset, final int length) {
    System.arraycopy(src, offset, quoteReqId, 0, length);
    quoteReqIdLen = length;
  }

  void setSymbol(final byte[] src, final int offset, final int length) {
    System.arraycopy(src, offset, symbol, 0, length);
    symbolLen = length;
  }

  void setAccountCode(final byte[] src, final int offset, final int length) {
    System.arraycopy(src, offset, accountCode, 0, length);
    accountCodeLen = length;
  }

  void setText(final byte[] src, final int offset, final int length) {
    System.arraycopy(src, offset, text, 0, length);
    textLen = length;
  }

  void setSettlDate(final byte[] src, final int offset, final int length) {
    System.arraycopy(src, offset, settlDate, 0, length);
    settlDateLen = length;
  }

  void setCurrency(final byte[] src, final int offset, final int length) {
    System.arraycopy(src, offset, currency, 0, length);
    currencyLen = length;
  }

  void setSettlCurrency(final byte[] src, final int offset, final int length) {
    System.arraycopy(src, offset, settlCurrency, 0, length);
    settlCurrencyLen = length;
  }

  void setSide(final SideEnum side) {
    this.side = side;
  }

  void setProductType(final ProductTypeEnum productType) {
    this.productType = productType;
  }

  void setStatus(final QuoteStatus status) {
    this.status = status;
  }

  void setRejectReason(final QuoteRejectReasonEnum rejectReason) {
    this.rejectReason = rejectReason;
  }

  void setSettlType(final SettlTypeEnum settlType) {
    this.settlType = settlType;
  }

  void setTenor(final TenorEnum tenor) {
    this.tenor = tenor;
  }

  void setBidPx(final long bidPx) {
    this.bidPx = bidPx;
  }

  void setOfferPx(final long offerPx) {
    this.offerPx = offerPx;
  }

  void setBidSize(final long bidSize) {
    this.bidSize = bidSize;
  }

  void setOfferSize(final long offerSize) {
    this.offerSize = offerSize;
  }

  void setOrderQty(final long orderQty) {
    this.orderQty = orderQty;
  }

  void setSwapPoints(final long swapPoints) {
    this.swapPoints = swapPoints;
  }

  void setValidUntil(final long validUntil) {
    this.validUntil = validUntil;
  }

  void setSequenceNumber(final long sequenceNumber) {
    this.sequenceNumber = sequenceNumber;
  }

  void setCreatedAt(final long createdAt) {
    this.createdAt = createdAt;
  }

  void setLastUpdatedAt(final long lastUpdatedAt) {
    this.lastUpdatedAt = lastUpdatedAt;
  }

  void setResponseLatencyNanos(final long responseLatencyNanos) {
    this.responseLatencyNanos = responseLatencyNanos;
  }
}
