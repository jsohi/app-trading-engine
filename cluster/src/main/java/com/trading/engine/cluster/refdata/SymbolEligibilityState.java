package com.trading.engine.cluster.refdata;

/**
 * Mutable in-memory representation of one per-symbol eligibility record held by {@link
 * SymbolEligibilityStore}. APP-62 §G — drives the restricted-symbol / short-sale-restricted
 * decision (SEC 15c3-5(c)(1)(ii)) at order admission time; APP-62 §I piggybacks the per-symbol
 * fat-finger override here because both knobs are symbol-keyed reference data and update on the
 * same operational cadence (start-of-day load + ad-hoc Reg SHO threshold updates).
 *
 * <p><b>Symbol key.</b> The cluster keys eligibility records by {@code symbolHash} (the 8-byte SBE
 * Symbol field packed into a little-endian {@code long}). The 8 raw bytes are also retained on the
 * state so the snapshot round-trip and downstream projection consumers can recover the original
 * symbol without inverting the pack. The packing must be byte-identical to {@code
 * com.trading.engine.cluster.handler.NewOrderSingleHandler#packSymbolKey(byte[], int)} so the
 * (account, symbol) maps inside the handler and the eligibility map here are keyed consistently.
 *
 * <p><b>Threading.</b> Not thread-safe — single-threaded cluster duty cycle only.
 *
 * <p><b>Allocation.</b> Zero-allocation after construction (mutated via setters; {@code
 * symbolBytes} is a fixed-length 8-byte array populated in place from the SBE decoder, so
 * populate-from-decoder is allocation-free).
 */
public final class SymbolEligibilityState {

  /** SBE {@code Symbol} field length (char[8]). */
  public static final int SYMBOL_LENGTH = 8;

  /**
   * Packed little-endian {@code long} form of {@link #symbolBytes}. Primary lookup key into {@link
   * SymbolEligibilityStore}; matches the packing produced by {@code
   * NewOrderSingleHandler#packSymbolKey} so the eligibility check and the position maps share a
   * single symbol-to-key encoding.
   */
  private long symbolHash;

  /**
   * Raw 8-byte FIX tag 55 (Symbol) bytes — fixed-length, NUL-padded. Retained alongside {@link
   * #symbolHash} for snapshot round-trip and projection consumption (the projection needs the
   * printable symbol, not just the hash).
   */
  private final byte[] symbolBytes = new byte[SYMBOL_LENGTH];

  /**
   * APP-62 §G — {@code true} when trading is allowed for this symbol; {@code false} when the symbol
   * is halted / restricted and every order (Buy or Sell) must be rejected with {@code
   * RegulatoryRestriction}. SBE wire type is uint8 (0/1); widened to {@code boolean} on read.
   */
  private boolean tradingAllowed;

  /**
   * APP-62 §G — {@code true} when short sales are allowed; {@code false} for Reg SHO threshold
   * securities, halt-and-locate violations, and hard-to-borrow names. Phase-1 §G treats every Sell
   * as potentially-short for the restricted set (conservative; refined to long/short discrimination
   * under APP-180). SBE wire type is uint8.
   */
  private boolean shortSaleAllowed;

  /**
   * APP-62 §I — per-symbol fat-finger tolerance override in basis points. {@code 0} = no override
   * (the per-account knob from {@code RiskLimitState#priceDeviationBps} applies). SBE wire type is
   * uint32; widened to {@code long} on read.
   */
  private long priceDeviationBpsOverride;

  /**
   * Cluster timestamp (epoch nanoseconds) when this record was last loaded. Sourced from the
   * cluster {@code onSessionMessage} timestamp; never from wall clock so the value is deterministic
   * under Aeron log replay.
   */
  private long asOfTimestamp;

  /** Sets the packed-{@code long} symbol key. */
  public void setSymbolHash(long value) {
    this.symbolHash = value;
  }

  /**
   * Copies up to {@link #SYMBOL_LENGTH} bytes from {@code src[srcOffset..]} into the backing {@link
   * #symbolBytes} buffer. Any tail bytes beyond {@code length} are zero-filled so the field has
   * well-defined contents after each load.
   *
   * @param src source byte buffer (FIX tag 55 symbol bytes)
   * @param srcOffset start offset within {@code src}
   * @param length number of source bytes to consider; values &gt; 8 are silently truncated to the
   *     8-byte field width
   */
  public void setSymbolBytes(final byte[] src, int srcOffset, int length) {
    int copyLen = Math.min(length, SYMBOL_LENGTH);
    System.arraycopy(src, srcOffset, symbolBytes, 0, copyLen);
    for (int i = copyLen; i < SYMBOL_LENGTH; i++) {
      symbolBytes[i] = 0;
    }
  }

  /** APP-62 §G — sets the per-symbol trading-allowed flag. */
  public void setTradingAllowed(boolean value) {
    this.tradingAllowed = value;
  }

  /** APP-62 §G — sets the per-symbol short-sale-allowed flag. */
  public void setShortSaleAllowed(boolean value) {
    this.shortSaleAllowed = value;
  }

  /**
   * APP-62 §I — sets the per-symbol fat-finger tolerance override in basis points; {@code 0} means
   * "no override".
   */
  public void setPriceDeviationBpsOverride(long value) {
    this.priceDeviationBpsOverride = value;
  }

  /** Sets the cluster timestamp (epoch nanos) at which this record was last loaded. */
  public void setAsOfTimestamp(long value) {
    this.asOfTimestamp = value;
  }

  public long symbolHash() {
    return symbolHash;
  }

  /**
   * Returns the backing 8-byte symbol buffer. Do not mutate outside {@link #setSymbolBytes}; the
   * store relies on its content matching the packed {@link #symbolHash}.
   */
  public byte[] symbolBytes() {
    return symbolBytes;
  }

  public boolean tradingAllowed() {
    return tradingAllowed;
  }

  public boolean shortSaleAllowed() {
    return shortSaleAllowed;
  }

  public long priceDeviationBpsOverride() {
    return priceDeviationBpsOverride;
  }

  public long asOfTimestamp() {
    return asOfTimestamp;
  }

  /**
   * Packs an 8-byte SBE Symbol field (NUL-padded char[8]) into a little-endian {@code long} for use
   * as the primary key in {@link SymbolEligibilityStore} and as the cross-store join key against
   * the cluster's working-position maps. Zero-allocation; byte-identical to {@code
   * com.trading.engine.cluster.handler.NewOrderSingleHandler#packSymbolKey(byte[], int)} so the NOS
   * admission path and this store agree on the symbol-to-key encoding without a cross-package
   * dependency. The packing helper lives on the state (rather than the store) so reference-data
   * ingress handlers can compute the key before constructing the state instance.
   *
   * @param src the symbol bytes (must be at least 8 bytes long)
   * @param offset the start offset of the 8-byte field
   * @return the packed symbol as a little-endian long
   */
  public static long packSymbolKey(final byte[] src, final int offset) {
    return (src[offset] & 0xFFL)
        | ((src[offset + 1] & 0xFFL) << 8)
        | ((src[offset + 2] & 0xFFL) << 16)
        | ((src[offset + 3] & 0xFFL) << 24)
        | ((src[offset + 4] & 0xFFL) << 32)
        | ((src[offset + 5] & 0xFFL) << 40)
        | ((src[offset + 6] & 0xFFL) << 48)
        | ((src[offset + 7] & 0xFFL) << 56);
  }
}
