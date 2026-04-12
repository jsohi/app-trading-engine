package com.trading.engine.pricing.forward;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.trading.engine.pricing.PricingMath;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ConfigurableForwardPointSource} — the static forward point term structure
 * that supports exact tenor lookup, linear interpolation for broken dates, spot (zero-day) queries,
 * unknown symbols, extrapolation beyond the last tenor, and swap point calculation.
 *
 * <p>The test registers a EUR/USD term structure with tenors at 30, 90, and 180 days. Forward point
 * values are chosen to exercise interpolation arithmetic without overflow concerns:
 *
 * <ul>
 *   <li>30d: +12,500,000 (0.125 pips in fixed-point 10^-8)
 *   <li>90d: +37,500,000 (0.375 pips)
 *   <li>180d: +75,000,000 (0.75 pips)
 * </ul>
 *
 * These values model a positive-carry scenario where the base currency (EUR) has a lower interest
 * rate than the quote currency (USD), producing positive forward points.
 */
class ConfigurableForwardPointSourceTest {

  /** Symbol bytes for EUR/USD — 8-byte fixed-width SBE Symbol type. */
  private static final byte[] EURUSD_BYTES = "EURUSD\0\0".getBytes();

  /** Symbol bytes for an unregistered pair — used to test unknown symbol behaviour. */
  private static final byte[] GBPJPY_BYTES = "GBPJPY\0\0".getBytes();

  /** Standard tenors in calendar days: 1-month, 3-month, 6-month. */
  private static final int[] TENOR_DAYS = {30, 90, 180};

  /**
   * Forward points in fixed-point 10^-8, parallel to TENOR_DAYS. Chosen for clean interpolation
   * arithmetic.
   */
  private static final long[] FORWARD_POINTS = {12_500_000L, 37_500_000L, 75_000_000L};

  private ConfigurableForwardPointSource source;
  private UnsafeBuffer eurusdBuffer;
  private UnsafeBuffer gbpjpyBuffer;

  @BeforeEach
  void setUp() {
    source = new ConfigurableForwardPointSource(4);
    source.registerSymbol(EURUSD_BYTES, TENOR_DAYS, FORWARD_POINTS);
    eurusdBuffer = new UnsafeBuffer(EURUSD_BYTES);
    gbpjpyBuffer = new UnsafeBuffer(GBPJPY_BYTES);
  }

  /**
   * Querying at an exact configured tenor returns the stored forward point value directly — no
   * interpolation. Tests all three configured tenors.
   */
  @Test
  void forwardPoints_exactTenor_returnsConfiguredValue() {
    assertEquals(
        12_500_000L,
        source.forwardPoints(eurusdBuffer, 0, EURUSD_BYTES.length, 30),
        "30-day tenor must return exact configured value");

    assertEquals(
        37_500_000L,
        source.forwardPoints(eurusdBuffer, 0, EURUSD_BYTES.length, 90),
        "90-day tenor must return exact configured value");

    assertEquals(
        75_000_000L,
        source.forwardPoints(eurusdBuffer, 0, EURUSD_BYTES.length, 180),
        "180-day tenor must return exact configured value");
  }

  /**
   * Querying at 60 days (between the 30d and 90d configured tenors) applies linear interpolation.
   *
   * <p>Expected: pointsLo + (pointsHi - pointsLo) * (days - daysLo) / (daysHi - daysLo) =
   * 12,500,000 + (37,500,000 - 12,500,000) * (60 - 30) / (90 - 30) = 12,500,000 + 25,000,000 * 30 /
   * 60 = 12,500,000 + 12,500,000 = 25,000,000
   */
  @Test
  void forwardPoints_betweenTenors_interpolates() {
    final long result = source.forwardPoints(eurusdBuffer, 0, EURUSD_BYTES.length, 60);

    // Linear interpolation between 30d (12.5M) and 90d (37.5M) at 60d:
    // 12,500,000 + mulDiv(25,000,000, 30, 60) = 12,500,000 + 12,500,000 = 25,000,000
    final long expected = 12_500_000L + PricingMath.mulDiv(25_000_000L, 30, 60);
    assertEquals(expected, result, "60-day query must linearly interpolate between 30d and 90d");
  }

  /**
   * Spot settlement (0 days to settlement) returns 0 forward points — no forward adjustment on a
   * spot trade.
   */
  @Test
  void forwardPoints_spotZeroDays_returnsZero() {
    final long result = source.forwardPoints(eurusdBuffer, 0, EURUSD_BYTES.length, 0);

    assertEquals(0L, result, "Spot (0 days) must return zero forward points");
  }

  /**
   * Querying for an unregistered symbol returns 0 — no forward adjustment is applied. This is the
   * safe default for instruments without a configured term structure.
   */
  @Test
  void forwardPoints_unknownSymbol_returnsZero() {
    final long result = source.forwardPoints(gbpjpyBuffer, 0, GBPJPY_BYTES.length, 90);

    assertEquals(0L, result, "Unknown symbol must return zero forward points");
  }

  /**
   * Querying beyond the last configured tenor (180d) applies linear extrapolation from the last two
   * tenors (90d and 180d).
   *
   * <p>Expected at 365 days: pointsLo + (pointsHi - pointsLo) * (days - daysLo) / (daysHi - daysLo)
   * = 37,500,000 + (75,000,000 - 37,500,000) * (365 - 90) / (180 - 90) = 37,500,000 + 37,500,000 *
   * 275 / 90 = 37,500,000 + 114,583,333 = 152,083,333
   */
  @Test
  void forwardPoints_beyondLastTenor_extrapolates() {
    final long result = source.forwardPoints(eurusdBuffer, 0, EURUSD_BYTES.length, 365);

    // Extrapolation from 90d (37.5M) and 180d (75M) to 365d:
    final long expected =
        37_500_000L + PricingMath.mulDiv(75_000_000L - 37_500_000L, 365 - 90, 180 - 90);
    assertEquals(expected, result, "365-day query must extrapolate linearly from last two tenors");
  }

  /**
   * Swap points are defined as forwardPoints(far) - forwardPoints(near). With near=30d and
   * far=180d, the swap points equal the difference between the two exact tenor values.
   */
  @Test
  void swapPoints_nearAndFar_returnsDifference() {
    final long result = source.swapPoints(eurusdBuffer, 0, EURUSD_BYTES.length, 30, 180);

    // swapPoints(30, 180) = forwardPoints(180) - forwardPoints(30)
    //                     = 75,000,000 - 12,500,000 = 62,500,000
    final long expected = 75_000_000L - 12_500_000L;
    assertEquals(expected, result, "Swap points must equal far forward points minus near");
  }
}
