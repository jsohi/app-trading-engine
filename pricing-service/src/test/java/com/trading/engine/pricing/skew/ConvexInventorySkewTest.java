package com.trading.engine.pricing.skew;

import static com.trading.engine.testsupport.buffer.SbeFieldUtil.zeroPad;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.FixedPointScale;
import com.trading.engine.pricing.PricingMath;
import com.trading.engine.testsupport.buffer.SbeFieldUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ConvexInventorySkew} — the quadratic (alpha=2) inventory skew model used in
 * FX market-making to penalise large positions non-linearly.
 *
 * <p>All prices and positions use fixed-point {@code 10^-8} representation via {@link
 * FixedPointScale#PRICE_SCALE}. The test parameters model a typical EUR/USD desk:
 *
 * <ul>
 *   <li>midRate = 108_500_000 (EUR/USD 1.085)
 *   <li>maxSkewBps = 5
 *   <li>positionThreshold = 1_000_000_000 (10 units notional in fixed-point, chosen to stay within
 *       {@link PricingMath#mulDiv} overflow bounds for quadratic computation)
 *   <li>alphaX100 = 200 (quadratic)
 * </ul>
 *
 * <p><b>Position threshold sizing rationale.</b> The {@link ConvexInventorySkew} computation
 * squares the clamped position via {@link PricingMath#mulDiv(long, long, long)}, which requires the
 * intermediate product {@code absPos * absPos} to fit in a signed {@code long}. The maximum safe
 * absolute position is {@code sqrt(Long.MAX_VALUE)} ~ 3.03 * 10^9. The threshold is set to 10^9 to
 * stay well within this limit while still exercising all code paths (zero, small, half, full
 * threshold).
 */
class ConvexInventorySkewTest {

  /** EUR/USD mid-rate of 1.085 in fixed-point 10^-8. */
  private static final long MID_RATE = 108_500_000L;

  /** Maximum skew in basis points at full inventory. */
  private static final int MAX_SKEW_BPS = 5;

  /**
   * Position threshold: 1_000_000_000 (10 units notional in fixed-point 10^-8). Chosen to stay
   * within {@code long} overflow bounds when squared (10^9 * 10^9 = 10^18, well under
   * Long.MAX_VALUE ~ 9.2 * 10^18). Positions at or beyond this magnitude receive full maxSkewBps.
   */
  private static final long POSITION_THRESHOLD = 1_000_000_000L;

  /** Convexity exponent * 100: 200 = alpha 2.0 (quadratic). */
  private static final int ALPHA_X_100 = 200;

  /** Symbol bytes for EUR/USD — 8-byte fixed-width SBE Symbol. */
  private static final byte[] EURUSD_BYTES = zeroPad("EURUSD", SbeFieldUtil.SYMBOL_LENGTH);

  private ConfigurablePositionSource positionSource;
  private ConvexInventorySkew skew;
  private UnsafeBuffer symbolBuffer;

  @BeforeEach
  void setUp() {
    positionSource = new ConfigurablePositionSource();
    skew = new ConvexInventorySkew(positionSource, MAX_SKEW_BPS, POSITION_THRESHOLD, ALPHA_X_100);
    symbolBuffer = new UnsafeBuffer(EURUSD_BYTES);
  }

  /**
   * Zero net position produces zero skew adjustment — the mid-rate is unperturbed when the dealer
   * is flat.
   */
  @Test
  void skewAdjustment_zeroPosition_returnsZero() {
    // positionSource defaults to 0 for unknown symbols — no setPosition call needed.

    final long adjustment = skew.skewAdjustment(symbolBuffer, 0, EURUSD_BYTES.length, MID_RATE);

    assertEquals(0L, adjustment);
  }

  /**
   * Positive net position (dealer is long) produces a <em>negative</em> skew adjustment. The
   * convention is: long position shifts the mid down to attract selling flow and reduce exposure.
   */
  @Test
  void skewAdjustment_longPosition_negativeAdjustment() {
    // Set a significant long position: half the threshold.
    final long longPosition = POSITION_THRESHOLD / 2;
    positionSource.setPosition(EURUSD_BYTES, longPosition);

    final long adjustment = skew.skewAdjustment(symbolBuffer, 0, EURUSD_BYTES.length, MID_RATE);

    assertTrue(adjustment < 0, "Long position must produce negative skew, got: " + adjustment);
  }

  /**
   * Negative net position (dealer is short) produces a <em>positive</em> skew adjustment. The
   * convention is: short position shifts the mid up to attract buying flow and reduce exposure.
   */
  @Test
  void skewAdjustment_shortPosition_positiveAdjustment() {
    // Set a significant short position: negative half the threshold.
    final long shortPosition = -(POSITION_THRESHOLD / 2);
    positionSource.setPosition(EURUSD_BYTES, shortPosition);

    final long adjustment = skew.skewAdjustment(symbolBuffer, 0, EURUSD_BYTES.length, MID_RATE);

    assertTrue(adjustment > 0, "Short position must produce positive skew, got: " + adjustment);
  }

  /**
   * A small position relative to the threshold produces a near-zero skew adjustment due to the
   * quadratic curve — at small inventory the marginal penalty is gentle.
   *
   * <p>With position = threshold/100, the quadratic ratio is (1/100)^2 = 1/10,000. The resulting
   * skew in bps = maxSkewBps * (1/10,000) = 5 / 10,000 = 0.0005 bps. At midRate 108_500_000 this
   * translates to approximately 108_500_000 * 0.0005 / 10,000 = ~5 in fixed-point, which is
   * negligible relative to the mid-rate.
   */
  @Test
  void skewAdjustment_smallPosition_nearZero() {
    // Position = threshold / 100 — very small relative to the threshold.
    final long smallPosition = POSITION_THRESHOLD / 100;
    positionSource.setPosition(EURUSD_BYTES, smallPosition);

    final long adjustment = skew.skewAdjustment(symbolBuffer, 0, EURUSD_BYTES.length, MID_RATE);

    // Quadratic: skewBps = 5 * (T/100)^2 / T^2 = 5 / 10,000 = 0.0005 bps
    // adjustment = midRate * 0.0005 / 10,000 => negligible after integer truncation.
    // With integer mulDiv: normalizedSq = mulDiv(10M, 10M, 1B) = 100, skewBps = mulDiv(100, 5,
    // 1B) = 0, so adjustment = 0.
    assertTrue(
        Math.abs(adjustment) <= 10,
        "Small position should produce near-zero skew, got: " + adjustment);
  }

  /**
   * When the position equals the threshold, the skew must equal maxSkewBps applied to the mid-rate.
   *
   * <p>Expected: adjustment = -midRate * maxSkewBps / 10,000 (negative because long position).
   * Numerically: -108_500_000 * 5 / 10,000 = -54,250.
   */
  @Test
  void skewAdjustment_atThreshold_equalsMaxSkew() {
    positionSource.setPosition(EURUSD_BYTES, POSITION_THRESHOLD);

    final long adjustment = skew.skewAdjustment(symbolBuffer, 0, EURUSD_BYTES.length, MID_RATE);

    // At threshold, quadratic ratio = T^2 / T^2 = 1, so skewBps = maxSkewBps = 5.
    // adjustment = midRate * (-5) / 10,000 = -108_500_000 * 5 / 10,000 = -54,250
    final long expected = -(MID_RATE * MAX_SKEW_BPS / 10_000L);
    assertEquals(expected, adjustment, "At threshold, skew must equal maxSkewBps applied to mid");
  }
}
