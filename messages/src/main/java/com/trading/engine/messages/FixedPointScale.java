package com.trading.engine.messages;

/**
 * Fixed-point pricing constants shared across all modules in the trading engine.
 *
 * <p>The cluster stores prices and quantities as {@code long} with implicit scale {@code 10^-8}
 * ({@link #SCALE_DIGITS} digits after the decimal point). This is the engine's universal
 * convention, pinned in {@code CLAUDE.md}. It accommodates spot/forward FX, crypto, and equities
 * without per-instrument tick metadata. The trade-off is reduced overflow headroom: int64 caps the
 * representable real-unit value at {@code Long.MAX_VALUE / 10^8 ≈ 92 × 10^9}.
 *
 * <p><b>Thread-safety.</b> All fields are compile-time constants — safe for unrestricted concurrent
 * access.
 *
 * <p><b>Allocation.</b> Zero allocation — static utility methods only, no instances.
 *
 * @see com.trading.engine.messages.clock.TradingClocks
 */
public final class FixedPointScale {

  /** Number of decimal digits carried in the int64 fixed-point form. */
  public static final int SCALE_DIGITS = 8;

  /**
   * Pre-computed powers of 10, indexed 0–18.
   *
   * <p>Covers every shift the converter can produce on a {@code long}.
   */
  private static final long[] POW10 = {
    1L,
    10L,
    100L,
    1_000L,
    10_000L,
    100_000L,
    1_000_000L,
    10_000_000L,
    100_000_000L,
    1_000_000_000L,
    10_000_000_000L,
    100_000_000_000L,
    1_000_000_000_000L,
    10_000_000_000_000L,
    100_000_000_000_000L,
    1_000_000_000_000_000L,
    10_000_000_000_000_000L,
    100_000_000_000_000_000L,
    1_000_000_000_000_000_000L,
  };

  /**
   * Implicit scale factor: {@code 10^8 = 100_000_000L}.
   *
   * <p>Multiply a real-unit value by this constant to obtain its int64 fixed-point form: {@code
   * 1.085 × PRICE_SCALE = 108_500_000L} (EUR/USD spot).
   */
  public static final long PRICE_SCALE = POW10[SCALE_DIGITS];

  /**
   * Sentinel value indicating that no price is available.
   *
   * <p>Used by the pricing service's {@code MarketDataAdapter} to signal that a symbol has no
   * current mid-rate (unknown symbol, no market data, or adapter not yet initialised). Chosen as
   * {@link Long#MIN_VALUE} to guarantee it can never be confused with a valid fixed-point price
   * (which is always non-negative or a small negative for short-selling rebates).
   */
  public static final long PRICE_NOT_AVAILABLE = Long.MIN_VALUE;

  /**
   * Converts a human-readable (value, scale) pair to the engine's int64 fixed-point form.
   *
   * <p>Example: {@code toFixedPoint(105, 2)} returns {@code 105_000_000L} (= 1.05 × 10^8).
   *
   * <p>Supports scale in range {@code [0, SCALE_DIGITS]}. Scale values greater than {@code
   * SCALE_DIGITS} are rejected — the engine's fixed-point precision is 10^-8; finer-grained input
   * would require lossy truncation.
   *
   * <p><b>Overflow:</b> Uses {@link Math#multiplyExact(long, long)} — throws {@link
   * ArithmeticException} if the result overflows {@code long}.
   *
   * @param value the unscaled value (e.g., 105 for price "1.05")
   * @param scale number of decimal digits in {@code value} (e.g., 2 for "1.05")
   * @return int64 fixed-point representation
   * @throws IllegalArgumentException if {@code scale} is outside {@code [0, SCALE_DIGITS]}
   * @throws ArithmeticException if the result overflows {@code long}
   */
  public static long toFixedPoint(final long value, final int scale) {
    if (scale < 0 || scale > SCALE_DIGITS) {
      throw new IllegalArgumentException(
          "scale " + scale + " out of supported range [0, " + SCALE_DIGITS + "]");
    }
    return Math.multiplyExact(value, POW10[SCALE_DIGITS - scale]);
  }

  private FixedPointScale() {}
}
