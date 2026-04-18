package com.trading.engine.messages;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link FixedPointScale#toFixedPoint(long, int)} — the public conversion utility that
 * transforms human-readable (value, scale) pairs into the engine's int64 fixed-point form.
 */
final class FixedPointScaleTest {

  @Test
  void toFixedPoint_wholeNumber_scalesToFullPrecision() {
    // 1 with scale=0 → 1 * 10^8 = 100_000_000
    assertEquals(100_000_000L, FixedPointScale.toFixedPoint(1, 0));
  }

  @Test
  void toFixedPoint_decimalValue_scalesToFixedPoint() {
    // 105 with scale=2 represents 1.05 → 105 * 10^6 = 105_000_000
    assertEquals(105_000_000L, FixedPointScale.toFixedPoint(105, 2));
  }

  @Test
  void toFixedPoint_maxScale_noMultiplication() {
    // scale=8 → shift=0, multiplied by 10^0 = 1 (no-op)
    assertEquals(105_000_000L, FixedPointScale.toFixedPoint(105_000_000L, 8));
  }

  @Test
  void toFixedPoint_zero_returnsZero() {
    assertEquals(0L, FixedPointScale.toFixedPoint(0, 0));
    assertEquals(0L, FixedPointScale.toFixedPoint(0, 8));
  }

  @Test
  void toFixedPoint_scaleExceedsMax_throwsIllegalArgument() {
    final var ex =
        assertThrows(IllegalArgumentException.class, () -> FixedPointScale.toFixedPoint(1, 9));
    assertEquals("scale 9 out of supported range [0, 8]", ex.getMessage());
  }

  @Test
  void toFixedPoint_negativeScale_throwsIllegalArgument() {
    final var ex =
        assertThrows(IllegalArgumentException.class, () -> FixedPointScale.toFixedPoint(1, -1));
    assertEquals("scale -1 out of supported range [0, 8]", ex.getMessage());
  }

  @Test
  void toFixedPoint_overflow_throwsArithmeticException() {
    // Long.MAX_VALUE * 10^8 overflows
    assertThrows(ArithmeticException.class, () -> FixedPointScale.toFixedPoint(Long.MAX_VALUE, 0));
  }

  @Test
  void toFixedPoint_typicalFxPrice_correctResult() {
    // 10850 with scale=4 represents 1.0850 → 10850 * 10^4 = 108_500_000
    assertEquals(108_500_000L, FixedPointScale.toFixedPoint(10850, 4));
  }
}
