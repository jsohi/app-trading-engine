package com.trading.engine.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import uk.co.real_logic.artio.fields.DecimalFloat;

class FixedPointTest {

  @Test
  void canonicalCase_oneFifty_twentyFive() {
    // 150.25 → 15_025_000_000
    DecimalFloat fix = new DecimalFloat(15_025L, 2);
    assertEquals(15_025_000_000L, FixedPoint.toInt64(fix));
  }

  @Test
  void smallestNonZero() {
    // 0.00000001 → 1
    DecimalFloat fix = new DecimalFloat(1L, 8);
    assertEquals(1L, FixedPoint.toInt64(fix));
  }

  @Test
  void integerWithoutFraction() {
    // 100 → 10_000_000_000
    DecimalFloat fix = new DecimalFloat(100L, 0);
    assertEquals(10_000_000_000L, FixedPoint.toInt64(fix));
  }

  @Test
  void maxRepresentable() {
    // 99_999_999.99999999 (16 nines) → 9_999_999_999_999_999
    DecimalFloat fix = new DecimalFloat(9_999_999_999_999_999L, 8);
    assertEquals(9_999_999_999_999_999L, FixedPoint.toInt64(fix));
  }

  @ParameterizedTest
  @CsvSource({
    "1, 0, 100000000",
    "150, 0, 15000000000",
    "15025, 2, 15025000000",
    "1, 8, 1",
    "0, 0, 0",
    "99999999, 0, 9999999900000000",
  })
  void parameterizedExactCases(long value, int scale, long expected) {
    DecimalFloat fix = new DecimalFloat(value, scale);
    assertEquals(expected, FixedPoint.toInt64(fix));
  }

  @Test
  void rejectsLossyConversion() {
    // FIX precision finer than 10^-8 (9 decimal places) → must throw, never silently truncate.
    DecimalFloat fix = new DecimalFloat(123_456_789L, 9);
    assertThrows(IllegalStateException.class, () -> FixedPoint.toInt64(fix));
  }

  @Test
  void rejectsLossyNegativeConversion() {
    // Java's negative-modulo preserves the sign of the dividend, so -123_456_789 % 10 == -9
    // → non-zero → throws. Pins that the lossy check works for both signs.
    DecimalFloat fix = new DecimalFloat(-123_456_789L, 9);
    assertThrows(IllegalStateException.class, () -> FixedPoint.toInt64(fix));
  }

  @Test
  void exactDivideShiftNegativeNonLossy() {
    // value=10, scale=9 → shift=-1, divisor=10, value % divisor == 0, returns 1.
    DecimalFloat fix = new DecimalFloat(10L, 9);
    assertEquals(1L, FixedPoint.toInt64(fix));
  }

  @Test
  void zeroValueAtAnyScale() {
    // 0 % anything == 0, so any scale (including >8) round-trips to 0.
    DecimalFloat fix = new DecimalFloat(0L, 15);
    assertEquals(0L, FixedPoint.toInt64(fix));
  }

  @Test
  void scaleOutOfSupportedRangeThrows() {
    // scale=27 → shift=-19, |shift| >= POW10.length → IllegalStateException, NOT AIOOBE.
    DecimalFloat fix = new DecimalFloat(1L, 27);
    assertThrows(IllegalStateException.class, () -> FixedPoint.toInt64(fix));
  }

  @Test
  void rejectsOverflow() {
    // Long.MAX_VALUE * 10^8 overflows long → Math.multiplyExact throws.
    DecimalFloat fix = new DecimalFloat(Long.MAX_VALUE / 10L, 0);
    assertThrows(ArithmeticException.class, () -> FixedPoint.toInt64(fix));
  }

  @Test
  void rejectsNegativeOverflow() {
    // Long.MIN_VALUE * 10^8 also overflows → Math.multiplyExact throws.
    DecimalFloat fix = new DecimalFloat(Long.MIN_VALUE / 10L, 0);
    assertThrows(ArithmeticException.class, () -> FixedPoint.toInt64(fix));
  }

  @Test
  void preservesNegativeExactValues() {
    // -150.25 → -15_025_000_000. Pins sign preservation through Math.multiplyExact.
    DecimalFloat fix = new DecimalFloat(-15_025L, 2);
    assertEquals(-15_025_000_000L, FixedPoint.toInt64(fix));
  }

  @Test
  void roundTripPreservesValue() {
    long[] sweep = {
      Long.MIN_VALUE / 10L,
      -9_999_999_999_999_999L,
      -15_025_000_000L,
      -1L,
      0L,
      1L,
      99_999_999L,
      100_000_000L,
      150_25_000_000L,
      9_999_999_999_999_999L,
      Long.MAX_VALUE / 10L,
    };
    DecimalFloat scratch = new DecimalFloat();
    for (long v : sweep) {
      FixedPoint.toDecimalFloat(v, scratch);
      assertEquals(v, FixedPoint.toInt64(scratch), "round-trip failed for " + v);
    }
  }

  @Test
  void toDecimalFloatMutatesInPlace() {
    DecimalFloat scratch = new DecimalFloat();
    FixedPoint.toDecimalFloat(15_025_000_000L, scratch);
    assertEquals(15_025_000_000L, FixedPoint.toInt64(scratch));
    // Reuse: same instance, different value.
    FixedPoint.toDecimalFloat(1L, scratch);
    assertEquals(1L, FixedPoint.toInt64(scratch));
  }
}
