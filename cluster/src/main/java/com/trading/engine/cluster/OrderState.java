package com.trading.engine.cluster;

import com.trading.engine.messages.sbe.OrdStatusEnum;
import com.trading.engine.messages.sbe.OrdTypeEnum;
import com.trading.engine.messages.sbe.ProductTypeEnum;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.messages.sbe.TimeInForceEnum;

/**
 * Mutable in-memory representation of an active order held by the cluster {@link OrderBook}.
 *
 * <p>Instances are owned by a pre-allocated pool inside {@link OrderBook} — the cluster duty cycle
 * {@linkplain OrderBook#acquire(long) acquires} a cleared instance on {@code NewOrderSingle},
 * {@linkplain #reset() resets} its fields, populates them from the decoded command, and returns it
 * to the pool via {@link OrderBook#release(long)} at the end of the order's lifecycle (cancel,
 * fill, expire). This matches the zero-allocation flyweight pattern used by exchange-core, LMAX,
 * and Aeron/Artio internals.
 *
 * <p>All numeric fields are fixed-point with scale {@code 10^-8} (matching the SBE wire format).
 * Enum types are reused from the generated SBE codecs to avoid translation cost when an order is
 * admitted into the book directly from a decoded {@code NewOrderSingle}.
 *
 * <p><b>Threading.</b> Not thread-safe — cluster duty-cycle thread only. Each instance is owned by
 * exactly one pool slot and must only be mutated through the owning {@link OrderBook}.
 */
public final class OrderState {

  /** Fixed wire length of the {@code OrderID} field (SBE {@code char[20]}). */
  public static final int ORDER_ID_LENGTH = 20;

  /** Fixed wire length of the {@code ClOrdID} field (SBE {@code char[20]}). */
  public static final int CL_ORD_ID_LENGTH = 20;

  /** Fixed wire length of the {@code Symbol} field (SBE {@code char[8]}). */
  public static final int SYMBOL_LENGTH = 8;

  /**
   * Pre-assigned pool slot index — set once at {@link OrderBook} construction and never mutated.
   * Lets {@link OrderBook#release(long)} return this instance to its free-list slot in O(1) without
   * a reverse-lookup scan.
   */
  private final int poolSlot;

  // Identity.
  private final byte[] orderId = new byte[ORDER_ID_LENGTH];
  private final byte[] clOrdId = new byte[CL_ORD_ID_LENGTH];
  private final byte[] symbol = new byte[SYMBOL_LENGTH];
  private long orderKey;
  private long accountId;

  // Order fields.
  private SideEnum side = SideEnum.NULL_VAL;
  private OrdTypeEnum ordType = OrdTypeEnum.NULL_VAL;
  private TimeInForceEnum timeInForce = TimeInForceEnum.NULL_VAL;
  // APP-151 phase 3 — productType retained on state so OrderCanceledEvent can populate the real
  // value instead of NULL_VAL. NOT serialized in OrderBookSnapshot (template 202) in this slice —
  // orders that cross a cluster snapshot revert to NULL_VAL after restore. Acceptable degradation
  // matching the session-tracker's in-memory-only design (cancel-on-disconnect is the primary
  // consumer; restored orders no longer have a live session and would not auto-cancel anyway).
  private ProductTypeEnum productType = ProductTypeEnum.NULL_VAL;
  private long price;
  private long orderQty;
  private long leavesQty;
  private long cumQty;
  private OrdStatusEnum ordStatus = OrdStatusEnum.NULL_VAL;
  private long transactTime;

  /** Construct an empty state bound to a pool slot. Called by {@link OrderBook} at startup. */
  OrderState(final int poolSlot) {
    this.poolSlot = poolSlot;
  }

  /** Construct an unpooled state (used by tests that don't exercise the pool). */
  public OrderState() {
    this(-1);
  }

  /** Pool slot index assigned at construction, or {@code -1} for an unpooled instance. */
  public int poolSlot() {
    return poolSlot;
  }

  /**
   * Clear every mutable field to its neutral value. Called by {@link OrderBook#acquire(long)}
   * before a pooled instance is handed back to the caller so stale data from a previous lifecycle
   * cannot leak out.
   */
  public void reset() {
    java.util.Arrays.fill(orderId, (byte) 0);
    java.util.Arrays.fill(clOrdId, (byte) 0);
    java.util.Arrays.fill(symbol, (byte) 0);
    orderKey = 0L;
    accountId = 0L;
    side = SideEnum.NULL_VAL;
    ordType = OrdTypeEnum.NULL_VAL;
    timeInForce = TimeInForceEnum.NULL_VAL;
    productType = ProductTypeEnum.NULL_VAL;
    price = 0L;
    orderQty = 0L;
    leavesQty = 0L;
    cumQty = 0L;
    ordStatus = OrdStatusEnum.NULL_VAL;
    transactTime = 0L;
  }

  // ---------------------------------------------------------------------------
  // Mutators
  // ---------------------------------------------------------------------------

  public void setOrderKey(final long value) {
    this.orderKey = value;
  }

  /** Copy the 20-byte fixed-length order id from {@code src[srcOffset..srcOffset+20]}. */
  public void setOrderIdBytes(final byte[] src, final int srcOffset) {
    System.arraycopy(src, srcOffset, orderId, 0, ORDER_ID_LENGTH);
  }

  /** Copy the 20-byte fixed-length client order id from {@code src[srcOffset..srcOffset+20]}. */
  public void setClOrdIdBytes(final byte[] src, final int srcOffset) {
    System.arraycopy(src, srcOffset, clOrdId, 0, CL_ORD_ID_LENGTH);
  }

  /** Copy the 8-byte fixed-length symbol from {@code src[srcOffset..srcOffset+8]}. */
  public void setSymbolBytes(final byte[] src, final int srcOffset) {
    System.arraycopy(src, srcOffset, symbol, 0, SYMBOL_LENGTH);
  }

  public void setAccountId(final long value) {
    this.accountId = value;
  }

  public void setSide(final SideEnum value) {
    this.side = value;
  }

  public void setOrdType(final OrdTypeEnum value) {
    this.ordType = value;
  }

  public void setTimeInForce(final TimeInForceEnum value) {
    this.timeInForce = value;
  }

  /**
   * Sets the product type (FIX custom tag 10013). Captured at admit time from the NOS decoder so a
   * later {@code OrderCanceledEvent} can carry the real productType instead of {@code NULL_VAL}.
   * APP-151 phase 3.
   *
   * @param value the product type from the NOS command — must not be null
   */
  public void setProductType(final ProductTypeEnum value) {
    this.productType = value;
  }

  public void setPrice(final long value) {
    this.price = value;
  }

  public void setOrderQty(final long value) {
    this.orderQty = value;
  }

  public void setLeavesQty(final long value) {
    this.leavesQty = value;
  }

  public void setCumQty(final long value) {
    this.cumQty = value;
  }

  public void setOrdStatus(final OrdStatusEnum value) {
    this.ordStatus = value;
  }

  public void setTransactTime(final long value) {
    this.transactTime = value;
  }

  // ---------------------------------------------------------------------------
  // Accessors
  // ---------------------------------------------------------------------------

  public long orderKey() {
    return orderKey;
  }

  /**
   * Copy the 20-byte fixed-length order id into {@code dst[dstOffset..dstOffset+20]}. Zero
   * allocation — for SBE encoders that accept a byte-array source.
   */
  public void copyOrderIdTo(final byte[] dst, final int dstOffset) {
    System.arraycopy(orderId, 0, dst, dstOffset, ORDER_ID_LENGTH);
  }

  public byte orderIdByte(final int index) {
    return orderId[index];
  }

  public void copyClOrdIdTo(final byte[] dst, final int dstOffset) {
    System.arraycopy(clOrdId, 0, dst, dstOffset, CL_ORD_ID_LENGTH);
  }

  public byte clOrdIdByte(final int index) {
    return clOrdId[index];
  }

  public void copySymbolTo(final byte[] dst, final int dstOffset) {
    System.arraycopy(symbol, 0, dst, dstOffset, SYMBOL_LENGTH);
  }

  public byte symbolByte(final int index) {
    return symbol[index];
  }

  public long accountId() {
    return accountId;
  }

  public SideEnum side() {
    return side;
  }

  public OrdTypeEnum ordType() {
    return ordType;
  }

  public TimeInForceEnum timeInForce() {
    return timeInForce;
  }

  /**
   * Returns the product type for this order (FIX custom tag 10013). Read by {@code
   * NewOrderSingleHandler.emitOrderCanceledEvent} so the cancel event carries the same product type
   * the order was admitted with (APP-151 phase 3).
   *
   * @return the product type, or {@link ProductTypeEnum#NULL_VAL} if the order crossed a cluster
   *     snapshot/restart (the field is in-memory only, not snapshot-persisted in this slice)
   */
  public ProductTypeEnum productType() {
    return productType;
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

  public OrdStatusEnum ordStatus() {
    return ordStatus;
  }

  public long transactTime() {
    return transactTime;
  }
}
