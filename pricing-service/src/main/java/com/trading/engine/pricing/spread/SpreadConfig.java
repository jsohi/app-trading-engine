package com.trading.engine.pricing.spread;

/**
 * Per-symbol spread configuration parameters governing base spread width, pip precision, quote-size
 * limits, and quantity-dependent widening.
 *
 * <p>Instances are created at startup (from YAML or hard-coded defaults) and stored in an {@link
 * org.agrona.collections.Object2ObjectHashMap} keyed by {@link
 * com.trading.engine.pricing.ByteArrayKey}. A {@link #defaultConfig()} factory provides sensible
 * defaults for unknown symbols so that the spread model never fails a lookup.
 *
 * <h3>Immutability</h3>
 *
 * <p>All fields are {@code final}. Once constructed, a {@code SpreadConfig} is effectively
 * immutable and may be safely shared across threads (though in practice it is only accessed from
 * the single-threaded pricing-service duty cycle). Fields are accessed directly (record-style)
 * rather than via getters, following the flyweight convention used throughout the pricing service.
 *
 * <h3>Allocation behaviour</h3>
 *
 * <p>Zero allocation on lookup. Instances are pre-allocated at startup and retrieved from the
 * symbol config map via a reusable probe key.
 *
 * @see TieredSpreadModel
 */
public final class SpreadConfig {

  /**
   * Base spread in basis points (1 bps = 0.01%). Applied symmetrically around the mid-rate before
   * tier, quantity, and volatility multipliers are factored in.
   *
   * <p>Typical values: 2-5 bps for G10 spot, 5-15 bps for EM or forward tenors.
   */
  public final int baseSpreadBps;

  /**
   * Minimum price increment (pip) in fixed-point {@code 10^-8} representation. Bid prices are
   * rounded down and offer prices are rounded up to this precision.
   *
   * <p>Example: for EUR/USD with 0.00001 pip, {@code pipSize = 1_000L} (i.e., 0.00001 * 10^8).
   */
  public final long pipSize;

  /**
   * Minimum quote size in fixed-point {@code 10^-8} representation. Orders below this threshold may
   * be rejected or quoted at minimum spread.
   */
  public final long minQuoteSize;

  /**
   * Maximum quote size in fixed-point {@code 10^-8} representation. Orders above this threshold are
   * rejected outright — the pricing service will not quote.
   */
  public final long maxQuoteSize;

  /**
   * Quantity threshold in fixed-point {@code 10^-8} representation above which the quantity
   * multiplier begins to widen the spread. Orders at or below this size receive the base spread
   * (quantity multiplier = 100, i.e., 1.00x).
   */
  public final long quantityThreshold;

  /**
   * Maximum quantity multiplier expressed as an integer scaled by 100 (i.e., 200 = 2.00x). Caps the
   * spread widening for very large orders to prevent unreasonable quotes.
   */
  public final int quantityMaxMultiplier;

  /**
   * Constructs a spread configuration with the given parameters.
   *
   * @param baseSpreadBps base spread in basis points; must be {@code > 0}
   * @param pipSize minimum price increment in fixed-point {@code 10^-8}; must be {@code > 0}
   * @param minQuoteSize minimum quotable quantity in fixed-point {@code 10^-8}; must be {@code >=
   *     0}
   * @param maxQuoteSize maximum quotable quantity in fixed-point {@code 10^-8}; must be {@code >
   *     minQuoteSize}
   * @param quantityThreshold quantity above which spread widens in fixed-point {@code 10^-8}; must
   *     be {@code > 0}
   * @param quantityMaxMultiplier maximum quantity multiplier as integer x100; must be {@code >=
   *     100}
   */
  public SpreadConfig(
      final int baseSpreadBps,
      final long pipSize,
      final long minQuoteSize,
      final long maxQuoteSize,
      final long quantityThreshold,
      final int quantityMaxMultiplier) {
    if (baseSpreadBps <= 0) {
      throw new IllegalArgumentException("baseSpreadBps must be > 0, got " + baseSpreadBps);
    }
    if (pipSize <= 0) {
      throw new IllegalArgumentException("pipSize must be > 0, got " + pipSize);
    }
    if (minQuoteSize < 0) {
      throw new IllegalArgumentException("minQuoteSize must be >= 0, got " + minQuoteSize);
    }
    if (quantityThreshold <= 0) {
      throw new IllegalArgumentException("quantityThreshold must be > 0, got " + quantityThreshold);
    }
    if (quantityMaxMultiplier < 100) {
      throw new IllegalArgumentException(
          "quantityMaxMultiplier must be >= 100, got " + quantityMaxMultiplier);
    }
    if (maxQuoteSize <= minQuoteSize) {
      throw new IllegalArgumentException(
          "maxQuoteSize must be > minQuoteSize, got max=" + maxQuoteSize + " min=" + minQuoteSize);
    }
    this.baseSpreadBps = baseSpreadBps;
    this.pipSize = pipSize;
    this.minQuoteSize = minQuoteSize;
    this.maxQuoteSize = maxQuoteSize;
    this.quantityThreshold = quantityThreshold;
    this.quantityMaxMultiplier = quantityMaxMultiplier;
  }

  /**
   * Returns a default spread configuration suitable for unknown symbols.
   *
   * <p>Conservative defaults are chosen to produce wide (but not unreasonable) quotes for symbols
   * that lack explicit configuration:
   *
   * <ul>
   *   <li>Base spread: 10 bps
   *   <li>Pip size: 1_000 (0.00001 in real units — standard G10 FX pip)
   *   <li>Min quote size: 100_000 * 10^8 (100K units)
   *   <li>Max quote size: 10_000_000 * 10^8 (10M units)
   *   <li>Quantity threshold: 1_000_000 * 10^8 (1M units)
   *   <li>Quantity max multiplier: 300 (3.00x)
   * </ul>
   *
   * @return a new default SpreadConfig instance
   */
  public static SpreadConfig defaultConfig() {
    return new SpreadConfig(
        10, // baseSpreadBps
        1_000L, // pipSize (0.00001 in real units)
        100_000L * 100_000_000L, // minQuoteSize (100K units)
        10_000_000L * 100_000_000L, // maxQuoteSize (10M units)
        1_000_000L * 100_000_000L, // quantityThreshold (1M units)
        300); // quantityMaxMultiplier (3.00x)
  }
}
