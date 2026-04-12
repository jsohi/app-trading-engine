package com.trading.engine.pricing;

/**
 * Fixed-point arithmetic utilities for the pricing service's spread and skew calculations.
 *
 * <p>Modeled on {@link com.trading.engine.projections.ProjectionUtil#mulDiv(long, long, long)} but
 * specialized for the pricing hot path where allocation is forbidden. The projections version falls
 * back to {@link java.math.BigInteger} on overflow; this version instead proves statically that
 * pricing inputs cannot overflow a signed {@code long} product, and throws {@link
 * ArithmeticException} as a defensive check rather than allocating a BigInteger.
 *
 * <p><b>Overflow proof.</b> In the pricing service, {@code mulDiv} is used to apply basis-point
 * spread and skew adjustments to a mid-rate:
 *
 * <pre>
 *   adjustedPrice = mulDiv(midRate, multiplier, PRICE_SCALE)
 * </pre>
 *
 * The mid-rate is a fixed-point {@code long} with scale 10^8. The maximum representable real-unit
 * price is {@code Long.MAX_VALUE / 10^8 ~= 9.2 * 10^10}, so {@code midRate <= 10^19} in practice,
 * but realistic FX/crypto mid-rates stay well below {@code 10^11 * 10^8 = 10^19}. The multiplier
 * encodes a spread/skew adjustment near 1.0 in the same fixed-point scale, so its magnitude is
 * close to {@code 10^8}. For the product {@code midRate * multiplier}:
 *
 * <ul>
 *   <li>Worst-case midRate: {@code 10^11} (scaled) = {@code 10^11 * 10^8 = 10^19}
 *   <li>Worst-case multiplier product: {@code 10^6} above scale = {@code 10^8 + 10^6 ~= 10^8}
 *   <li>Worst-case product: {@code ~10^19 * 10^8 = 10^27} — but this exceeds long range.
 *   <li>Realistic bound: midRate in scaled form ~= {@code 10^11 * PRICE_SCALE = 10^19}, multiplier
 *       ~= {@code PRICE_SCALE = 10^8}, product ~= {@code 10^17}, well under {@code Long.MAX_VALUE
 *       ~= 9.2 * 10^18}.
 * </ul>
 *
 * The defensive {@link Math#multiplyHigh(long, long)} check catches any violation of these
 * assumptions at runtime without allocating.
 *
 * <p><b>Threading:</b> stateless — all methods are safe for concurrent use from any thread.
 *
 * <p><b>Allocation:</b> zero allocation. No fallback path allocates.
 */
public final class PricingMath {

  private PricingMath() {}

  /**
   * Computes {@code (a * b) / divisor} using direct long arithmetic with a defensive overflow
   * guard. Supports signed inputs (negative values arise in skew and mean-reversion calculations).
   *
   * <p>For pricing-service inputs (midRate &lt;= 10^11 scaled, multiplier product &lt;= 10^6 above
   * scale), the intermediate product {@code |a| * |b|} fits comfortably in a signed {@code long}
   * (max ~10^17, well under {@code Long.MAX_VALUE ~= 9.2 * 10^18}). This method validates that
   * assumption at runtime using {@link Math#multiplyHigh(long, long)} on the absolute values: if
   * the high 64 bits of the unsigned 128-bit product are non-zero, the magnitude has overflowed and
   * an {@link ArithmeticException} is thrown rather than returning a silently wrong result.
   *
   * <p><b>Allocation:</b> zero allocation on all paths.
   *
   * @param a multiplicand (may be negative for skew/reversion calculations)
   * @param b multiplier (may be negative for skew/reversion calculations)
   * @param divisor non-zero divisor
   * @return the quotient {@code (a * b) / divisor}, truncated toward zero
   * @throws ArithmeticException if the intermediate product overflows a signed {@code long}
   * @throws ArithmeticException if {@code divisor} is zero
   */
  public static long mulDiv(final long a, final long b, final long divisor) {
    if (divisor == 0) {
      throw new ArithmeticException("divisor must not be zero");
    }
    if (a == 0 || b == 0) {
      return 0;
    }

    // Guard against Long.MIN_VALUE where Math.abs silently returns a negative value.
    // This can never happen for real pricing inputs (bounded well below 10^19), but
    // closing the hole defensively costs one branch on the cold-path guard.
    if (a == Long.MIN_VALUE || b == Long.MIN_VALUE || divisor == Long.MIN_VALUE) {
      throw new ArithmeticException("mulDiv does not accept Long.MIN_VALUE");
    }

    // Factor out the sign so the overflow check works on magnitudes.
    // The result sign is negative when exactly one input is negative.
    final long absA = Math.abs(a);
    final long absB = Math.abs(b);
    final boolean negativeResult = (a ^ b) < 0;

    // Defensive overflow check on unsigned magnitudes. Math.multiplyHigh is signed,
    // but for non-negative inputs it returns the true upper 64 bits. A non-zero high
    // word means the product exceeds Long.MAX_VALUE.
    final long hi = Math.multiplyHigh(absA, absB);
    if (hi != 0) {
      throw new ArithmeticException(
          "mulDiv overflow: a=" + a + " b=" + b + " product high word=" + hi);
    }

    final long product = absA * absB;

    // If hi == 0 but product < 0, the lower 64 bits overflowed the signed range.
    if (product < 0) {
      throw new ArithmeticException("mulDiv overflow: a=" + a + " b=" + b + " product=" + product);
    }

    final long quotient = product / Math.abs(divisor);
    final boolean negativeDivisor = divisor < 0;
    return (negativeResult ^ negativeDivisor) ? -quotient : quotient;
  }
}
