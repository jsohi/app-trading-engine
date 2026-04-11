package com.trading.engine.projections.position;

/**
 * Mutable internal read-model for a net position at a specific (symbol, account, settlDate). Tracks
 * gross buy/sell volumes and cumulative notional for per-side VWAP calculation.
 *
 * <p><b>Ownership:</b> instances are owned exclusively by {@link PositionProjection}. All setters
 * are package-private — only {@code PositionProjection} mutates this object.
 *
 * <p><b>Threading:</b> accessed single-threaded from the event-dispatch thread under the
 * projection's write lock. Query threads receive immutable {@link PositionSnapshot} copies.
 *
 * <p><b>Allocation:</b> one instance per unique (symbol, account, settlDate) combination. Fields
 * are mutated in-place on each fill. Flat positions (netQty=0) remain in the map — standard OMS
 * behavior; bounded by instrument × account × settlDate count.
 */
final class PositionView {

  /** Packed symbol long (8 ASCII bytes via {@link com.trading.engine.projections.SymbolPacker}). */
  private long symbolPacked;

  /** FIX tag 1: account code. SBE Account char[16]. */
  private final byte[] accountCode = new byte[16];

  private int accountCodeLen;

  /** FIX tag 64: settlement date YYYYMMDD. SBE SettlDate char[8]. */
  private final byte[] settlDate = new byte[8];

  private int settlDateLen;

  /** FIX tag 15: currency. SBE Currency char[3]. */
  private final byte[] currency = new byte[3];

  private int currencyLen;

  /** FIX tag 120: settlement currency (NDF distinction). SBE Currency char[3]. */
  private final byte[] settlCurrency = new byte[3];

  private int settlCurrencyLen;

  /** Net position quantity, fixed-point 10^-8. Positive = long, negative = short. */
  private long netQty;

  /** Gross buy quantity, fixed-point 10^-8. */
  private long buyQty;

  /** Gross sell quantity, fixed-point 10^-8. */
  private long sellQty;

  /**
   * Cumulative buy-side notional in unscaled units: sum of mulDiv(lastPx, lastQty, PRICE_SCALE) for
   * all buy fills. Used to derive avgBuyPx.
   */
  private long buyCumNotional;

  /**
   * Cumulative sell-side notional in unscaled units: sum of mulDiv(lastPx, lastQty, PRICE_SCALE)
   * for all sell fills. Used to derive avgSellPx.
   */
  private long sellCumNotional;

  /** Cluster timestamp (epoch nanos) of the most recent fill. */
  private long lastUpdatedAt;

  /** Event sequence number of the most recent fill. */
  private long lastSequenceNumber;

  // --- Public getters ---

  public long symbolPacked() {
    return symbolPacked;
  }

  public long netQty() {
    return netQty;
  }

  public long buyQty() {
    return buyQty;
  }

  public long sellQty() {
    return sellQty;
  }

  public long buyCumNotional() {
    return buyCumNotional;
  }

  public long sellCumNotional() {
    return sellCumNotional;
  }

  public long lastUpdatedAt() {
    return lastUpdatedAt;
  }

  public long lastSequenceNumber() {
    return lastSequenceNumber;
  }

  // --- Package-private byte array getters ---

  byte[] accountCode() {
    return accountCode;
  }

  int accountCodeLen() {
    return accountCodeLen;
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

  // --- Package-private setters ---

  void setSymbolPacked(final long symbolPacked) {
    this.symbolPacked = symbolPacked;
  }

  void setAccountCode(final byte[] src, final int offset, final int length) {
    System.arraycopy(src, offset, accountCode, 0, length);
    accountCodeLen = length;
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

  void setNetQty(final long netQty) {
    this.netQty = netQty;
  }

  void setBuyQty(final long buyQty) {
    this.buyQty = buyQty;
  }

  void setSellQty(final long sellQty) {
    this.sellQty = sellQty;
  }

  void setBuyCumNotional(final long buyCumNotional) {
    this.buyCumNotional = buyCumNotional;
  }

  void setSellCumNotional(final long sellCumNotional) {
    this.sellCumNotional = sellCumNotional;
  }

  void setLastUpdatedAt(final long lastUpdatedAt) {
    this.lastUpdatedAt = lastUpdatedAt;
  }

  void setLastSequenceNumber(final long lastSequenceNumber) {
    this.lastSequenceNumber = lastSequenceNumber;
  }
}
