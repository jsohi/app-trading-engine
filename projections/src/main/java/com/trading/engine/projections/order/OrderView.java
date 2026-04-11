package com.trading.engine.projections.order;

import com.trading.engine.messages.sbe.ExecTypeEnum;
import com.trading.engine.messages.sbe.OrdStatusEnum;
import com.trading.engine.messages.sbe.OrdTypeEnum;
import com.trading.engine.messages.sbe.ProductTypeEnum;
import com.trading.engine.messages.sbe.RejectReasonEnum;
import com.trading.engine.messages.sbe.SettlTypeEnum;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.messages.sbe.TenorEnum;

/**
 * Mutable internal read-model for a single order. Tracks the full order lifecycle from creation
 * through fills to terminal state (Filled, Canceled, Rejected).
 *
 * <p><b>Ownership:</b> instances are owned exclusively by {@link OrderProjection}. All setters are
 * package-private — only {@code OrderProjection} mutates this object. Byte-array getters are also
 * package-private to prevent leaking mutable internal state.
 *
 * <p><b>Identity semantics:</b> do NOT override {@code equals()} or {@code hashCode()} on this
 * class. {@link org.agrona.collections.ObjectHashSet} in {@code OrderProjection} relies on identity
 * equality for the account and symbol indexes.
 *
 * <p><b>Threading:</b> accessed single-threaded from the event-dispatch thread under the
 * projection's write lock. Query threads never see this object directly — they receive an immutable
 * {@link OrderSnapshot} copy.
 *
 * <p><b>Allocation:</b> one instance per order, allocated on {@code OrderCreatedEvent}. Fields are
 * mutated in-place on subsequent events (fills, cancel). No per-event allocation.
 */
final class OrderView {

  // --- Fixed-length byte array fields matching SBE schema types ---

  /** FIX tag 37: exchange order identifier. SBE OrderID char[20]. */
  private final byte[] orderId = new byte[20];

  private int orderIdLen;

  /** FIX tag 11: client order identifier. SBE ClOrdID char[20]. */
  private final byte[] clOrdId = new byte[20];

  private int clOrdIdLen;

  /** FIX tag 55: instrument symbol. SBE Symbol char[8]. */
  private final byte[] symbol = new byte[8];

  private int symbolLen;

  /** FIX tag 1: account code. SBE Account char[16]. */
  private final byte[] accountCode = new byte[16];

  private int accountCodeLen;

  /** FIX tag 17: last execution identifier from most recent fill. SBE ExecID char[20]. */
  private final byte[] lastExecId = new byte[20];

  private int lastExecIdLen;

  /** FIX tag 64: settlement date YYYYMMDD. SBE SettlDate char[8]. */
  private final byte[] settlDate = new byte[8];

  private int settlDateLen;

  /** FIX tag 15: order currency. SBE Currency char[3]. */
  private final byte[] currency = new byte[3];

  private int currencyLen;

  /** FIX tag 120: settlement currency (differs from currency for NDFs). SBE Currency char[3]. */
  private final byte[] settlCurrency = new byte[3];

  private int settlCurrencyLen;

  // --- Enum fields ---

  /** FIX tag 54: order side. */
  private SideEnum side = SideEnum.NULL_VAL;

  /** FIX tag 40: order type (Market, Limit, PreviouslyQuoted). */
  private OrdTypeEnum ordType = OrdTypeEnum.NULL_VAL;

  /** FIX tag 39: current order status. */
  private OrdStatusEnum ordStatus = OrdStatusEnum.NULL_VAL;

  /** Product type classification (Spot, Forward, Swap). */
  private ProductTypeEnum productType = ProductTypeEnum.NULL_VAL;

  /** FIX tag 150: execution type of the most recent execution report. */
  private ExecTypeEnum execType = ExecTypeEnum.NULL_VAL;

  /** Rejection reason — populated only when ordStatus is Rejected. */
  private RejectReasonEnum rejectReason = RejectReasonEnum.NULL_VAL;

  /** FIX tag 63: settlement type (optional). */
  private SettlTypeEnum settlType = SettlTypeEnum.NULL_VAL;

  /** Tenor classification (Spot, 1W, 1M, etc.). */
  private TenorEnum tenor = TenorEnum.NULL_VAL;

  // --- Numeric fields (all fixed-point 10^-8, PRICE_SCALE) ---

  /** FIX tag 44: order price, fixed-point 10^-8. */
  private long price;

  /** FIX tag 38: order quantity, fixed-point 10^-8. */
  private long orderQty;

  /** FIX tag 151: remaining (unfilled) quantity, fixed-point 10^-8. */
  private long leavesQty;

  /** FIX tag 14: cumulative filled quantity, fixed-point 10^-8. */
  private long cumQty;

  /** Weighted average fill price, fixed-point 10^-8. Computed via mulDiv. */
  private long avgPx;

  /**
   * Cumulative fill notional in unscaled units: sum of mulDiv(lastPx, lastQty, PRICE_SCALE) per
   * fill. Used to derive avgPx = mulDiv(cumNotional, PRICE_SCALE, cumQty).
   */
  private long cumNotional;

  // --- Timestamps ---

  /** Event sequence number from the most recently applied event. */
  private long sequenceNumber;

  /** Cluster timestamp (epoch nanos) of the OrderCreatedEvent. */
  private long createdAt;

  /** Cluster timestamp (epoch nanos) of the most recently applied event. */
  private long lastUpdatedAt;

  // --- Public getters (primitives and enums) ---

  public SideEnum side() {
    return side;
  }

  public OrdTypeEnum ordType() {
    return ordType;
  }

  public OrdStatusEnum ordStatus() {
    return ordStatus;
  }

  public ProductTypeEnum productType() {
    return productType;
  }

  public ExecTypeEnum execType() {
    return execType;
  }

  public RejectReasonEnum rejectReason() {
    return rejectReason;
  }

  public SettlTypeEnum settlType() {
    return settlType;
  }

  public TenorEnum tenor() {
    return tenor;
  }

  public long price() {
    return price;
  }

  public long orderQty() {
    return orderQty;
  }

  public long leavesQty() {
    return leavesQty;
  }

  public long cumQty() {
    return cumQty;
  }

  public long avgPx() {
    return avgPx;
  }

  public long cumNotional() {
    return cumNotional;
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

  /**
   * Returns {@code true} if the order is in an active (non-terminal) state.
   *
   * @return {@code true} if ordStatus is {@link OrdStatusEnum#New} or {@link
   *     OrdStatusEnum#PartiallyFilled}
   */
  public boolean isActive() {
    return ordStatus == OrdStatusEnum.New || ordStatus == OrdStatusEnum.PartiallyFilled;
  }

  // --- Package-private byte array getters (prevent leaking mutable state) ---

  byte[] orderId() {
    return orderId;
  }

  int orderIdLen() {
    return orderIdLen;
  }

  byte[] clOrdId() {
    return clOrdId;
  }

  int clOrdIdLen() {
    return clOrdIdLen;
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

  byte[] lastExecId() {
    return lastExecId;
  }

  int lastExecIdLen() {
    return lastExecIdLen;
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

  // --- Package-private setters (only OrderProjection mutates) ---

  void setOrderId(final byte[] src, final int offset, final int length) {
    System.arraycopy(src, offset, orderId, 0, length);
    orderIdLen = length;
  }

  void setClOrdId(final byte[] src, final int offset, final int length) {
    System.arraycopy(src, offset, clOrdId, 0, length);
    clOrdIdLen = length;
  }

  void setSymbol(final byte[] src, final int offset, final int length) {
    System.arraycopy(src, offset, symbol, 0, length);
    symbolLen = length;
  }

  void setAccountCode(final byte[] src, final int offset, final int length) {
    System.arraycopy(src, offset, accountCode, 0, length);
    accountCodeLen = length;
  }

  void setLastExecId(final byte[] src, final int offset, final int length) {
    System.arraycopy(src, offset, lastExecId, 0, length);
    lastExecIdLen = length;
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

  void setOrdType(final OrdTypeEnum ordType) {
    this.ordType = ordType;
  }

  void setOrdStatus(final OrdStatusEnum ordStatus) {
    this.ordStatus = ordStatus;
  }

  void setProductType(final ProductTypeEnum productType) {
    this.productType = productType;
  }

  void setExecType(final ExecTypeEnum execType) {
    this.execType = execType;
  }

  void setRejectReason(final RejectReasonEnum rejectReason) {
    this.rejectReason = rejectReason;
  }

  void setSettlType(final SettlTypeEnum settlType) {
    this.settlType = settlType;
  }

  void setTenor(final TenorEnum tenor) {
    this.tenor = tenor;
  }

  void setPrice(final long price) {
    this.price = price;
  }

  void setOrderQty(final long orderQty) {
    this.orderQty = orderQty;
  }

  void setLeavesQty(final long leavesQty) {
    this.leavesQty = leavesQty;
  }

  void setCumQty(final long cumQty) {
    this.cumQty = cumQty;
  }

  void setAvgPx(final long avgPx) {
    this.avgPx = avgPx;
  }

  void setCumNotional(final long cumNotional) {
    this.cumNotional = cumNotional;
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
}
