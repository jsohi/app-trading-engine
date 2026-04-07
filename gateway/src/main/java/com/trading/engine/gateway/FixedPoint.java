package com.trading.engine.gateway;

import uk.co.real_logic.artio.fields.DecimalFloat;
import uk.co.real_logic.artio.fields.ReadOnlyDecimalFloat;

/**
 * Conversion between Artio's {@link DecimalFloat} (FIX wire decimal) and the trading engine's int64
 * fixed-point representation. Zero allocation on every code path.
 *
 * <p>Artio represents a FIX decimal as {@code value × 10^-scale} where {@code value} is the signed
 * mantissa and {@code scale} is the non-negative number of digits after the decimal point. For
 * example, the FIX wire literal {@code 150.25} decodes to {@code value=15025, scale=2}.
 *
 * <p><b>Scale convention.</b> The cluster stores prices and quantities as {@code long} with
 * implicit scale {@code 10^-8} ({@link #FIXED_POINT_SCALE} digits after the decimal point). This is
 * the trading engine's chosen convention, pinned in {@code CLAUDE.md} and used by every downstream
 * consumer (SBE int64 fields, {@code OrderState}, {@code OrderBookTest}). It is <em>not</em> a
 * universal FIX industry default — different venues use different scales (cents/sub-penny for US
 * equities, fractional pip {@code 10^-5} for classic FX, per-instrument tick integers for CME/Eurex
 * futures, {@code 10^-8} for crypto-native venues). {@code 10^-8} was chosen here because it is the
 * only common scale that accommodates spot/forward FX, crypto, and equities without per-instrument
 * tick metadata. The trade-off is reduced overflow headroom: int64 caps the representable real-unit
 * value at {@code Long.MAX_VALUE / 10^8 ≈ 92e9}.
 *
 * <p><b>The zero-allocation discipline, by contrast, IS industry standard</b> for Artio-based
 * engines. Allocation on the FIX wire path is treated as a regression; this helper enforces it via
 * {@code static final} tables and mutate-in-place setters. See {@code
 * feedback_artio_industry_standard.md}.
 *
 * <p>Conversion is exact in both directions for any FIX value whose decimal precision is {@code <=
 * 8} digits. Inputs with finer precision throw {@link IllegalStateException} rather than silently
 * truncate. Inputs that overflow {@link Long#MAX_VALUE} throw {@link ArithmeticException} via
 * {@link Math#multiplyExact}.
 */
public final class FixedPoint {

  /** Decimal places carried in the int64 fixed-point form. */
  public static final int FIXED_POINT_SCALE = 8;

  /** Implicit scale factor of the cluster's int64 representation: {@code 10^FIXED_POINT_SCALE}. */
  public static final long PRICE_SCALE = pow10(FIXED_POINT_SCALE);

  private static long pow10(int n) {
    long r = 1L;
    for (int i = 0; i < n; i++) {
      r *= 10L;
    }
    return r;
  }

  // 10^0 .. 10^18 — covers every shift the converter can produce on a long.
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

  private FixedPoint() {}

  /**
   * Convert an Artio {@link ReadOnlyDecimalFloat} ({@code value × 10^-scale}) to a 10^-8 scaled
   * long.
   *
   * <p>Throws {@link IllegalStateException} on lossy conversion (FIX precision finer than 10^-8)
   * and {@link ArithmeticException} on multiplication overflow. Zero allocation.
   */
  public static long toInt64(ReadOnlyDecimalFloat fix) {
    final int scale = fix.scale();
    final long value = fix.value();
    final int shift = FIXED_POINT_SCALE - scale;
    // Single explicit range check covers both shift directions. Avoids Math.abs because
    // Math.abs(Integer.MIN_VALUE) returns a negative value (would be a footgun even though
    // shift can't realistically reach that here — Artio's parser bounds scale to a positive
    // int well below INT_MAX).
    if (shift <= -POW10.length || shift >= POW10.length) {
      throw new IllegalStateException("FIX decimal scale out of supported range: scale=" + scale);
    }
    if (shift >= 0) {
      return Math.multiplyExact(value, POW10[shift]);
    }
    final long divisor = POW10[-shift];
    if (value % divisor != 0L) {
      throw new IllegalStateException(
          "Lossy FIX→fixed-point conversion: value=" + value + " scale=" + scale);
    }
    return value / divisor;
  }

  /**
   * Set the supplied {@link DecimalFloat} to the FIX wire representation of a 10^-8 scaled int64
   * value. Mutates {@code dst} in place; the caller owns the instance. Zero allocation.
   *
   * <p>The result is always written as {@code (value=int64Value, scale=8)} — Artio's {@link
   * DecimalFloat#set(long, int)} is a plain field assignment and does not normalise trailing zeros.
   * Round-trip via {@link #toInt64} works regardless: {@code toInt64} handles both the
   * un-normalised form ({@code value=15_000_000_000, scale=8}) and any normalised form downstream
   * consumers may produce.
   */
  public static void toDecimalFloat(long int64Value, DecimalFloat dst) {
    dst.set(int64Value, FIXED_POINT_SCALE);
  }
}
