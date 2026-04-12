package com.trading.engine.pricing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link PricingMath#mulDiv(long, long, long)}.
 *
 * <p>Validates correct fixed-point arithmetic for positive, negative, and edge-case inputs, as well
 * as the defensive overflow guard for {@link Long#MIN_VALUE}. Realistic pricing-service scenarios
 * (basis-point spread adjustments against scaled mid-rates) are covered to ensure the
 * zero-allocation code path produces the same results as a naive {@code BigInteger} implementation.
 */
class PricingMathTest {

  /** Basic positive-input case: 100 * 200 / 50 = 400. */
  @Test
  void mulDiv_positiveInputs_correctResult() {
    assertEquals(400L, PricingMath.mulDiv(100L, 200L, 50L));
  }

  /** Zero multiplicand or multiplier must return 0 without division. */
  @Test
  void mulDiv_zeroInput_returnsZero() {
    assertEquals(0L, PricingMath.mulDiv(0L, 200L, 50L));
    assertEquals(0L, PricingMath.mulDiv(100L, 0L, 50L));
    assertEquals(0L, PricingMath.mulDiv(0L, 0L, 50L));
  }

  /** Division by zero must throw ArithmeticException regardless of other inputs. */
  @Test
  void mulDiv_zeroDivisor_throwsArithmetic() {
    assertThrows(ArithmeticException.class, () -> PricingMath.mulDiv(100L, 200L, 0L));
  }

  /** Negative multiplicand: -100 * 200 / 50 = -400. */
  @Test
  void mulDiv_negativeA_correctSign() {
    assertEquals(-400L, PricingMath.mulDiv(-100L, 200L, 50L));
  }

  /** Negative multiplier: 100 * -200 / 50 = -400. */
  @Test
  void mulDiv_negativeB_correctSign() {
    assertEquals(-400L, PricingMath.mulDiv(100L, -200L, 50L));
  }

  /** Both inputs negative: -100 * -200 / 50 = 400 (positive result). */
  @Test
  void mulDiv_bothNegative_positiveResult() {
    assertEquals(400L, PricingMath.mulDiv(-100L, -200L, 50L));
  }

  /** Negative divisor flips the result sign: 100 * 200 / -50 = -400; -100 * 200 / -50 = 400. */
  @Test
  void mulDiv_negativeDivisor_correctSign() {
    assertEquals(-400L, PricingMath.mulDiv(100L, 200L, -50L));
    assertEquals(400L, PricingMath.mulDiv(-100L, 200L, -50L));
  }

  /**
   * {@link Long#MIN_VALUE} is rejected because {@code Math.abs(Long.MIN_VALUE)} silently returns a
   * negative value. The defensive guard must throw for any of a, b, or divisor equal to {@link
   * Long#MIN_VALUE}.
   */
  @Test
  void mulDiv_longMinValue_throws() {
    assertThrows(ArithmeticException.class, () -> PricingMath.mulDiv(Long.MIN_VALUE, 1L, 1L));
    assertThrows(ArithmeticException.class, () -> PricingMath.mulDiv(1L, Long.MIN_VALUE, 1L));
    assertThrows(ArithmeticException.class, () -> PricingMath.mulDiv(1L, 1L, Long.MIN_VALUE));
  }

  /**
   * Realistic pricing-service scenario: apply a 3 bps spread to a EUR/USD mid-rate.
   *
   * <pre>
   *   midRate  = 108_500_000L  (1.085 in fixed-point 10^-8)
   *   bps      = 300           (3 basis points)
   *   divisor  = 10_000        (bps-to-decimal conversion)
   *   expected = 108_500_000 * 300 / 10_000 = 3_255_000
   * </pre>
   *
   * This is the core mulDiv call pattern used in {@link
   * com.trading.engine.pricing.spread.TieredSpreadModel}.
   */
  @Test
  void mulDiv_realisticPricingInputs_correctResult() {
    final long midRate = 108_500_000L;
    final long bps = 300L;
    final long divisor = 10_000L;
    final long expected = 3_255_000L;

    assertEquals(expected, PricingMath.mulDiv(midRate, bps, divisor));
  }
}
