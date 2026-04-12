package com.trading.engine.pricing.forward;

import com.trading.engine.pricing.ByteArrayKey;
import com.trading.engine.pricing.PricingMath;
import org.agrona.DirectBuffer;
import org.agrona.collections.Object2ObjectHashMap;

/**
 * Static forward point term structure source that loads per-symbol curves at construction and
 * performs linear interpolation for broken dates (tenors that do not match a configured standard
 * tenor).
 *
 * <p>Loads static forward point curves from YAML configuration. Linear interpolation for broken
 * dates. Not thread-safe — single-threaded Agent duty cycle only.
 *
 * <h3>Design</h3>
 *
 * <p>Each symbol's term structure is registered via {@link #registerSymbol(byte[], int[], long[])}
 * during cold-path setup (typically driven from YAML configuration at process startup). The term
 * structure consists of a sorted array of standard tenor days and their corresponding forward point
 * values in fixed-point {@code 10^-8}. At query time, the implementation resolves the forward
 * points for an arbitrary settlement day count by:
 *
 * <ol>
 *   <li><b>Exact match</b> — if the requested day count matches a configured tenor exactly, the
 *       stored value is returned directly.
 *   <li><b>Interpolation</b> — if the day count falls between two configured tenors, linear
 *       interpolation is applied: {@code pointsLo + mulDiv(pointsHi - pointsLo, days - daysLo,
 *       daysHi - daysLo)}.
 *   <li><b>Before first tenor</b> — if the day count is less than the shortest configured tenor,
 *       interpolation proceeds from the origin point (0 days, 0 points) to the first configured
 *       tenor.
 *   <li><b>Beyond last tenor</b> — if the day count exceeds the longest configured tenor, linear
 *       extrapolation from the last two configured tenors is applied.
 * </ol>
 *
 * <h3>Threading model</h3>
 *
 * <p><b>Not thread-safe.</b> Both {@link #registerSymbol} (cold-path setup) and {@link
 * #forwardPoints} / {@link #swapPoints} (hot-path query) must be called from the same thread — the
 * pricing-service agent's duty cycle. No synchronisation is provided.
 *
 * <h3>Allocation behaviour</h3>
 *
 * <p>{@link #registerSymbol} allocates on the cold path (owned {@link ByteArrayKey}, defensive
 * array copies, {@link TermStructure} record). After all symbols are registered, {@link
 * #forwardPoints} is <b>zero-allocation</b>: the symbol lookup uses a pre-allocated {@link
 * ByteArrayKey} probe, and all arithmetic is performed on primitives.
 *
 * @see ForwardPointSource
 * @see TermStructure
 * @see com.trading.engine.pricing.PricingMath#mulDiv(long, long, long)
 * @see com.trading.engine.messages.FixedPointScale#PRICE_SCALE
 */
public final class ConfigurableForwardPointSource implements ForwardPointSource {

  /**
   * Maximum symbol byte length (8-byte fixed-width SBE Symbol type). Used to size the pre-allocated
   * probe key's backing array.
   */
  private static final int MAX_SYMBOL_LENGTH = 8;

  /**
   * Load factor for the internal hash map. Agrona's {@link Object2ObjectHashMap} uses open
   * addressing, so a load factor of 0.55 provides good probe-chain length while keeping the table
   * compact for the small number of FX symbols (typically 20-40).
   */
  private static final float LOAD_FACTOR = 0.55f;

  /**
   * Per-symbol forward point term structures. Keyed by {@link ByteArrayKey} (owned copies for map
   * keys). Agrona's {@link Object2ObjectHashMap} is used instead of {@code java.util.HashMap} to
   * avoid autoboxing and to match the project's Agrona-only collections convention.
   */
  private final Object2ObjectHashMap<ByteArrayKey, TermStructure> curves;

  /**
   * Pre-allocated probe key for zero-allocation symbol lookups on the hot path. Mutated in-place
   * via {@link ByteArrayKey#wrapForProbe(DirectBuffer, int, int)} before each map lookup.
   */
  private final ByteArrayKey probeKey;

  /**
   * Constructs a new empty {@code ConfigurableForwardPointSource}. Register symbols via {@link
   * #registerSymbol(byte[], int[], long[])} before querying.
   *
   * <p><b>Allocation:</b> allocates the internal {@link Object2ObjectHashMap} and the probe key's
   * backing byte array. No further allocation occurs after symbol registration is complete.
   *
   * @param initialCapacity expected number of symbols to register; used to size the internal hash
   *     map and avoid rehashing. Must be {@code > 0}.
   */
  public ConfigurableForwardPointSource(final int initialCapacity) {
    this.curves = new Object2ObjectHashMap<>(initialCapacity, LOAD_FACTOR);
    this.probeKey = ByteArrayKey.emptyForLookup(MAX_SYMBOL_LENGTH);
  }

  /**
   * Registers a forward point term structure for the given symbol.
   *
   * <p>This is a <b>cold-path</b> method called during process startup (typically from YAML
   * configuration parsing). It allocates defensive copies of all input arrays and an owned {@link
   * ByteArrayKey} for the map key.
   *
   * <p>If a term structure for the symbol already exists, it is replaced silently. This allows
   * configuration reloads without restarting the process.
   *
   * @param symbol the symbol bytes (e.g., {@code "EURUSD\0\0".getBytes(US_ASCII)} for the 8-byte
   *     fixed-width SBE Symbol type)
   * @param tenorDays sorted ascending array of standard tenor day counts (e.g., {@code {30, 60, 90,
   *     180, 360}}). Must contain at least one element. The caller must ensure the array is sorted
   *     in strictly ascending order; no validation is performed.
   * @param forwardPointsFixed forward point values in fixed-point {@code 10^-8}, parallel to {@code
   *     tenorDays}. Must have the same length as {@code tenorDays}. Values may be positive or
   *     negative depending on the interest rate differential.
   * @throws IllegalArgumentException if {@code tenorDays} and {@code forwardPointsFixed} have
   *     different lengths, or if either array is empty
   * @throws NullPointerException if any argument is {@code null}
   */
  public void registerSymbol(
      final byte[] symbol, final int[] tenorDays, final long[] forwardPointsFixed) {

    if (tenorDays.length == 0) {
      throw new IllegalArgumentException("tenorDays must contain at least one element");
    }
    if (tenorDays.length != forwardPointsFixed.length) {
      throw new IllegalArgumentException(
          "tenorDays.length ("
              + tenorDays.length
              + ") != forwardPointsFixed.length ("
              + forwardPointsFixed.length
              + ")");
    }

    // Defensive copies — the caller may reuse or mutate the input arrays.
    final int[] tenorCopy = new int[tenorDays.length];
    System.arraycopy(tenorDays, 0, tenorCopy, 0, tenorDays.length);

    final long[] pointsCopy = new long[forwardPointsFixed.length];
    System.arraycopy(forwardPointsFixed, 0, pointsCopy, 0, forwardPointsFixed.length);

    final ByteArrayKey key = ByteArrayKey.owned(symbol, 0, symbol.length);
    curves.put(key, new TermStructure(tenorCopy, pointsCopy));
  }

  /**
   * {@inheritDoc}
   *
   * <p><b>Algorithm.</b> The symbol's {@link TermStructure} is located via a zero-allocation probe
   * key lookup. If no term structure is registered for the symbol, or if {@code daysToSettlement <=
   * 0}, returns {@code 0}. Otherwise:
   *
   * <ul>
   *   <li>A binary search locates the bracketing tenors.
   *   <li>On exact match, the stored value is returned directly.
   *   <li>Between two tenors, linear interpolation is applied using overflow-safe {@link
   *       PricingMath#mulDiv(long, long, long)}.
   *   <li>Before the first tenor, interpolation from the origin (0, 0) is applied.
   *   <li>Beyond the last tenor, extrapolation from the last two tenors is applied.
   * </ul>
   *
   * <p><b>Allocation:</b> zero-allocation. The probe key is mutated in-place; all arithmetic uses
   * primitive {@code long} and {@code int} values.
   */
  @Override
  public long forwardPoints(
      final DirectBuffer symbol, final int offset, final int length, final int daysToSettlement) {

    if (daysToSettlement <= 0) {
      return 0;
    }

    // Zero-allocation probe: copy symbol bytes into the pre-allocated probe key and look up.
    probeKey.wrapForProbe(symbol, offset, length);
    final TermStructure ts = curves.get(probeKey);
    if (ts == null) {
      return 0;
    }

    return interpolate(ts, daysToSettlement);
  }

  /**
   * Returns the number of symbols currently registered. Useful for diagnostics and test assertions.
   *
   * @return the number of registered symbol term structures
   */
  public int symbolCount() {
    return curves.size();
  }

  /**
   * Performs linear interpolation (or extrapolation) on the given term structure for the specified
   * day count.
   *
   * <p>The algorithm uses a simple linear scan rather than binary search because typical FX term
   * structures have only 5-8 standard tenors (ON, 1W, 1M, 2M, 3M, 6M, 9M, 1Y). At this size, a
   * linear scan with branch-free comparison is faster than binary search due to branch prediction
   * and cache locality.
   *
   * @param ts the term structure to interpolate
   * @param days the target day count (must be {@code > 0})
   * @return the interpolated forward points in fixed-point {@code 10^-8}
   */
  private static long interpolate(final TermStructure ts, final int days) {
    final int[] tenorDays = ts.tenorDays();
    final long[] points = ts.forwardPointsFixed();
    final int n = tenorDays.length;

    // Before the first configured tenor: interpolate from origin (0 days, 0 points).
    if (days < tenorDays[0]) {
      return PricingMath.mulDiv(points[0], days, tenorDays[0]);
    }

    // Linear scan for the bracketing interval. For the typical 5-8 tenor term structure,
    // this outperforms binary search due to branch prediction and sequential memory access.
    for (int i = 0; i < n; i++) {
      if (days == tenorDays[i]) {
        // Exact tenor match — return the configured value directly.
        return points[i];
      }
      if (days < tenorDays[i]) {
        // days falls between tenorDays[i-1] and tenorDays[i] — linear interpolation.
        // i > 0 is guaranteed because the days < tenorDays[0] case was handled above.
        final int daysLo = tenorDays[i - 1];
        final int daysHi = tenorDays[i];
        final long pointsLo = points[i - 1];
        final long pointsHi = points[i];
        return pointsLo + PricingMath.mulDiv(pointsHi - pointsLo, days - daysLo, daysHi - daysLo);
      }
    }

    // Beyond the last configured tenor: extrapolate linearly from the last two points.
    // For a single-tenor term structure, extrapolate from origin (0, 0) to that tenor.
    if (n == 1) {
      return PricingMath.mulDiv(points[0], days, tenorDays[0]);
    }

    final int daysLo = tenorDays[n - 2];
    final int daysHi = tenorDays[n - 1];
    final long pointsLo = points[n - 2];
    final long pointsHi = points[n - 1];
    return pointsLo + PricingMath.mulDiv(pointsHi - pointsLo, days - daysLo, daysHi - daysLo);
  }
}
