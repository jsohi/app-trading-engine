package com.trading.engine.fixbridge.translator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.gateway.FixedPoint;
import io.netty.buffer.Unpooled;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import org.junit.jupiter.api.Test;
import uk.co.real_logic.artio.fields.DecimalFloat;

/**
 * Verifies {@link DecimalStringEmitter} renders fixed-point int64 and Artio {@code DecimalFloat}
 * values to the canonical 8-frac-digit JSON wire form across all boundary corners, log-distributed
 * magnitudes, and {@link Random#nextLong()} samples; and rejects {@link Long#MIN_VALUE}.
 *
 * <p>Boundary corners (per plan Phase 3 spec): {@code 0, ±1, ±PRICE_SCALE, ±(PRICE_SCALE-1),
 * ±(PRICE_SCALE+1), ±(10*PRICE_SCALE), Long.MAX_VALUE, Long.MIN_VALUE+1}, plus the rejection of
 * {@code Long.MIN_VALUE}.
 *
 * <p>Property test (per plan §"Phase 3"): for every int64 sample, assert that {@code
 * emitDecimalFloat(toDecimalFloat(x))} produces the same byte output as {@code
 * emitInt64FixedPoint(x)}, exercising the {@code FixedPoint.toDecimalFloat} ↔ emitter round-trip.
 */
final class DecimalStringEmitterTest {

  private static final long PRICE_SCALE = 100_000_000L;
  private static final int FIXED_POINT_SCALE = 8;

  private final DecimalStringEmitter emitter = new DecimalStringEmitter();

  // ---------------------------------------------------------------------------
  // Helpers.
  // ---------------------------------------------------------------------------

  private String emit(final long int64Value) {
    final var dst = Unpooled.buffer(64);
    final int written = emitter.emitInt64FixedPoint(int64Value, dst);
    final var out = new byte[written];
    dst.readBytes(out);
    return new String(out, StandardCharsets.US_ASCII);
  }

  private String emit(final DecimalFloat df) {
    final var dst = Unpooled.buffer(64);
    final int written = emitter.emitDecimalFloat(df, dst);
    final var out = new byte[written];
    dst.readBytes(out);
    return new String(out, StandardCharsets.US_ASCII);
  }

  /** Reference implementation using BigDecimal — used only to compute expected byte output. */
  private static String referenceFormat(final long int64Value) {
    return new BigDecimal(int64Value)
        .movePointLeft(FIXED_POINT_SCALE)
        .setScale(FIXED_POINT_SCALE, RoundingMode.UNNECESSARY)
        .toPlainString();
  }

  // ---------------------------------------------------------------------------
  // Boundary corners.
  // ---------------------------------------------------------------------------

  @Test
  void emitInt64_zero_rendersAsCanonicalZero() {
    assertEquals("0.00000000", emit(0L));
  }

  @Test
  void emitInt64_positiveOne_rendersAsTinyFraction() {
    assertEquals("0.00000001", emit(1L));
  }

  @Test
  void emitInt64_negativeOne_rendersAsNegativeTinyFraction() {
    assertEquals("-0.00000001", emit(-1L));
  }

  @Test
  void emitInt64_priceScale_rendersAsExactlyOne() {
    assertEquals("1.00000000", emit(PRICE_SCALE));
  }

  @Test
  void emitInt64_negativePriceScale_rendersAsNegativeOne() {
    assertEquals("-1.00000000", emit(-PRICE_SCALE));
  }

  @Test
  void emitInt64_priceScaleMinusOne_rendersAsLargeFraction() {
    assertEquals("0.99999999", emit(PRICE_SCALE - 1));
  }

  @Test
  void emitInt64_negativePriceScaleMinusOne_rendersAsNegativeLargeFraction() {
    assertEquals("-0.99999999", emit(-(PRICE_SCALE - 1)));
  }

  @Test
  void emitInt64_priceScalePlusOne_rendersAsOnePlusTinyFraction() {
    assertEquals("1.00000001", emit(PRICE_SCALE + 1));
  }

  @Test
  void emitInt64_negativePriceScalePlusOne_rendersAsNegativeOnePlusTinyFraction() {
    assertEquals("-1.00000001", emit(-(PRICE_SCALE + 1)));
  }

  @Test
  void emitInt64_tenTimesPriceScale_rendersAsExactlyTen() {
    assertEquals("10.00000000", emit(10L * PRICE_SCALE));
  }

  @Test
  void emitInt64_negativeTenTimesPriceScale_rendersAsNegativeTen() {
    assertEquals("-10.00000000", emit(-10L * PRICE_SCALE));
  }

  @Test
  void emitInt64_longMaxValue_rendersFullPrecision() {
    // Long.MAX_VALUE = 9_223_372_036_854_775_807 → 92233720368.54775807
    assertEquals("92233720368.54775807", emit(Long.MAX_VALUE));
  }

  @Test
  void emitInt64_longMinValuePlusOne_rendersFullNegativePrecision() {
    assertEquals("-92233720368.54775807", emit(Long.MIN_VALUE + 1L));
  }

  @Test
  void emitInt64_longMinValue_throwsIllegalArgument() {
    final var dst = Unpooled.buffer(64);
    assertThrows(
        IllegalArgumentException.class, () -> emitter.emitInt64FixedPoint(Long.MIN_VALUE, dst));
  }

  // ---------------------------------------------------------------------------
  // Log-distributed magnitudes (plan §"Phase 3" line 179).
  // ---------------------------------------------------------------------------

  @Test
  void emitInt64_logDistributedMagnitudes_matchReferenceFormat() {
    final var rng = new Random(0xCAFEBABEL);
    for (int k = 1; k <= 18; k++) {
      final long magnitude = pow10(k);
      // ±10^k * randomFraction in [0,1)
      final long bound = magnitude;
      // Two samples per k: positive and negative.
      for (int s = 0; s < 2; s++) {
        long sample = ((rng.nextLong() & Long.MAX_VALUE) % bound);
        if (s == 1) {
          sample = -sample;
        }
        if (sample == Long.MIN_VALUE) {
          continue; // forbidden
        }
        assertEquals(referenceFormat(sample), emit(sample), "k=" + k + " sample=" + sample);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // 1000 random samples (sign-uniform).
  // ---------------------------------------------------------------------------

  @Test
  void emitInt64_thousandRandomSamples_matchReferenceFormat() {
    final var rng = new Random(0x42L);
    for (int i = 0; i < 1000; i++) {
      long sample = rng.nextLong();
      if (sample == Long.MIN_VALUE) {
        sample++;
      }
      assertEquals(referenceFormat(sample), emit(sample), "i=" + i + " sample=" + sample);
    }
  }

  // ---------------------------------------------------------------------------
  // emitDecimalFloat — round-trip via FixedPoint.toDecimalFloat must match emitInt64FixedPoint.
  //
  // Artio's DecimalFloat caps |value| at 10^18 - 1 (VALUE_MAX_VAL). FixedPoint.toDecimalFloat
  // therefore cannot accept the full int64 range, so the round-trip property is exercised
  // within ±ARTIO_DECIMAL_MAX. Realistic prices/qtys (< 10^10 in real units, scale 10^-8)
  // sit comfortably inside this bound.
  // ---------------------------------------------------------------------------

  /** Mirrors {@code ReadOnlyDecimalFloat.VALUE_MAX_VAL} — the maximum |value| Artio accepts. */
  private static final long ARTIO_DECIMAL_MAX = 999_999_999_999_999_999L;

  @Test
  void emitDecimalFloat_roundTripFromFixedPoint_matchesInt64Output() {
    final long[] samples = {
      0L,
      1L,
      -1L,
      PRICE_SCALE,
      -PRICE_SCALE,
      PRICE_SCALE - 1,
      -(PRICE_SCALE - 1),
      PRICE_SCALE + 1,
      -(PRICE_SCALE + 1),
      10L * PRICE_SCALE,
      -10L * PRICE_SCALE,
      ARTIO_DECIMAL_MAX,
      -ARTIO_DECIMAL_MAX,
    };
    final var df = new DecimalFloat();
    for (final long s : samples) {
      FixedPoint.toDecimalFloat(s, df);
      assertEquals(emit(s), emit(df), "sample=" + s);
    }
  }

  @Test
  void emitDecimalFloat_thousandRandomSamplesWithinArtioRange_roundTripExact() {
    final var rng = new Random(0xDEADBEEFL);
    final var df = new DecimalFloat();
    for (int i = 0; i < 1000; i++) {
      // Constrain to ±ARTIO_DECIMAL_MAX inclusive — matches the production constraint that
      // FixedPoint.toDecimalFloat is only ever called on values within Artio's DecimalFloat
      // range (locked §9: emitDecimalFloat is invoked only on DFs produced by toDecimalFloat).
      long sample = rng.nextLong() % (ARTIO_DECIMAL_MAX + 1L);
      FixedPoint.toDecimalFloat(sample, df);
      assertEquals(emit(sample), emit(df), "i=" + i + " sample=" + sample);
    }
  }

  // ---------------------------------------------------------------------------
  // emitDecimalFloat — value/scale combinations beyond the simple 10^-8 path.
  // ---------------------------------------------------------------------------

  @Test
  void emitDecimalFloat_scaleZero_rendersWithEightZeroFracDigits() {
    // 5 with scale=0 → exactly 5 → "5.00000000"
    final var df = new DecimalFloat(5L, 0);
    assertEquals("5.00000000", emit(df));
  }

  @Test
  void emitDecimalFloat_scaleTwo_rendersAlignedToEightDigits() {
    // 15025 * 10^-2 = 150.25 → "150.25000000"
    final var df = new DecimalFloat(15025L, 2);
    assertEquals("150.25000000", emit(df));
  }

  @Test
  void emitDecimalFloat_scaleEight_rendersIdenticallyToInt64() {
    // -150_000_000 fixed-point = -1.50000000
    final var df = new DecimalFloat(-150_000_000L, 8);
    assertEquals("-1.50000000", emit(df));
  }

  @Test
  @SuppressWarnings("deprecation")
  void emitDecimalFloat_scaleTen_truncatesFinerPrecision() {
    // Construct a DecimalFloat with scale > FIXED_POINT_SCALE using the raw setters, since
    // both DecimalFloat.set() and the constructor normalise the scale down. 1502500 * 10^-10 =
    // 0.0001502500 → fixed-point would be 15025 (last two zeros dropped). Output: "0.00015025".
    final var df = new DecimalFloat();
    df.value(1_502_500L);
    df.scale(10);
    assertEquals("0.00015025", emit(df));
  }

  @Test
  @SuppressWarnings("deprecation")
  void emitDecimalFloat_negativeScale_throwsIllegalArgument() {
    // Bypass DecimalFloat.set() normalisation (which silently flips negative scales) using the
    // raw value/scale setters — exercises the emitter's defensive path against a malformed DF
    // that nonetheless slipped through some upstream code path.
    final var df = new DecimalFloat();
    df.value(1L);
    df.scale(-1);
    final var dst = Unpooled.buffer(64);
    assertThrows(IllegalArgumentException.class, () -> emitter.emitDecimalFloat(df, dst));
  }

  @Test
  @SuppressWarnings("deprecation")
  void emitDecimalFloat_longMinValueValue_throwsIllegalArgument() {
    // DecimalFloat.set(MIN, _) throws via Artio's NaN check; the raw value() setter bypasses
    // it. The emitter still rejects MIN to avoid the asymmetric -Long.MIN_VALUE overflow case.
    final var df = new DecimalFloat();
    df.value(Long.MIN_VALUE);
    df.scale(FIXED_POINT_SCALE);
    final var dst = Unpooled.buffer(64);
    assertThrows(IllegalArgumentException.class, () -> emitter.emitDecimalFloat(df, dst));
  }

  @Test
  @SuppressWarnings("deprecation")
  void emitDecimalFloat_scaleAboveMaximum_throwsWithDescriptiveMessage() {
    // Scale 27 would overflow pow10(scale - FIXED_POINT_SCALE) = pow10(19), which is outside the
    // helper's [0, 18] range. The emitter rejects up front with a descriptive message rather than
    // letting the caller see a cryptic "pow10 exponent out of range" diagnostic. Production
    // callers cannot trigger this path because FixedPoint.toDecimalFloat constrains scale, but
    // the deprecated raw value/scale setters can.
    final var df = new DecimalFloat();
    df.value(1L);
    df.scale(27);
    final var dst = Unpooled.buffer(64);
    final var ex =
        assertThrows(IllegalArgumentException.class, () -> emitter.emitDecimalFloat(df, dst));
    assertTrue(
        ex.getMessage().contains("scale exceeds maximum representable"),
        "expected descriptive scale-bound message, got: " + ex.getMessage());
    assertTrue(
        ex.getMessage().contains("scale=27"),
        "expected message to include offending scale, got: " + ex.getMessage());
  }

  @Test
  @SuppressWarnings("deprecation")
  void emitDecimalFloat_scaleAtMaximumBoundary_succeeds() {
    // Boundary check: scale=26 must succeed (pow10(scale - 8) = pow10(18) is the helper's max).
    // The arithmetic produces a heavily truncated value (only the most-significant digit survives
    // the divide-down), but the operation itself must not throw — the production path tolerates
    // truncation when the raw scale exceeds FIXED_POINT_SCALE.
    final var df = new DecimalFloat();
    df.value(123_456_789_000_000_000L);
    df.scale(26);
    final var dst = Unpooled.buffer(64);
    // Should not throw.
    final int written = emitter.emitDecimalFloat(df, dst);
    assertTrue(written > 0, "emitDecimalFloat should write at least one byte at boundary scale");
  }

  // ---------------------------------------------------------------------------
  // Sanity: scratch is large enough for any signed long render.
  // ---------------------------------------------------------------------------

  @Test
  void scratchCapacity_accommodatesSignedLongMaxValue() {
    final int needed = 1 /* sign */ + DecimalStringEmitter.maxLongDigits() + 1 /* dot */ + 8;
    assertEquals(true, DecimalStringEmitter.scratchCapacity() >= needed);
  }

  // ---------------------------------------------------------------------------
  // Helpers.
  // ---------------------------------------------------------------------------

  private static long pow10(final int exp) {
    long r = 1L;
    for (int i = 0; i < exp; i++) {
      r *= 10L;
    }
    return r;
  }
}
