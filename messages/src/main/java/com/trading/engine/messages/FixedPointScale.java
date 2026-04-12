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
 * <p><b>Allocation.</b> Zero allocation — no methods, no instances.
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

  private FixedPointScale() {}
}
