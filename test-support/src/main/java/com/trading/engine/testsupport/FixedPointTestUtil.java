package com.trading.engine.testsupport;

import com.trading.engine.messages.FixedPointScale;

/**
 * Fixed-point arithmetic helpers for test readability.
 *
 * <p>Converts human-readable whole-number values to the engine's fixed-point representation (scale
 * factor {@link FixedPointScale#PRICE_SCALE}) used for prices and quantities.
 *
 * <p>Example: {@code price(100)} returns {@code 10_000_000_000L}.
 *
 * <p>Thread-safe — all methods are pure functions. No mutable state.
 *
 * <p>Allocates nothing — pure arithmetic on primitives.
 */
public final class FixedPointTestUtil {

  /**
   * The engine's fixed-point scale factor: 10^8.
   *
   * <p>Delegates to the canonical production constant in {@link FixedPointScale#PRICE_SCALE}
   * (messages module).
   */
  public static final long PRICE_SCALE = FixedPointScale.PRICE_SCALE;

  private FixedPointTestUtil() {}

  /**
   * Converts a whole-number price to fixed-point representation.
   *
   * @param whole the price as a whole number (e.g., 100 for "100.00")
   * @return the fixed-point value ({@code whole * PRICE_SCALE})
   */
  public static long price(final long whole) {
    return whole * PRICE_SCALE;
  }

  /**
   * Converts a whole-number quantity to fixed-point representation.
   *
   * @param whole the quantity as a whole number
   * @return the fixed-point value ({@code whole * PRICE_SCALE})
   */
  public static long qty(final long whole) {
    return whole * PRICE_SCALE;
  }
}
