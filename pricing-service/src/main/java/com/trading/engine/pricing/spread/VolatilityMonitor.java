package com.trading.engine.pricing.spread;

import com.trading.engine.pricing.ByteArrayKey;
import com.trading.engine.pricing.PricingMath;
import org.agrona.DirectBuffer;
import org.agrona.collections.Object2ObjectHashMap;

/**
 * Rolling-window volatility tracker that computes a spread-widening multiplier based on recent
 * mid-rate movement per symbol.
 *
 * <p>For each registered symbol, the monitor maintains a fixed-size ring buffer of historical
 * mid-rate values. On each tick, the new mid-rate is recorded and the range (max - min) over the
 * window is compared against a configurable threshold expressed in basis points of the most recent
 * mid-rate. When the range exceeds the threshold, the returned multiplier increases linearly up to
 * a configurable maximum, causing {@link TieredSpreadModel} to widen spreads during volatile
 * conditions.
 *
 * <h3>Design rationale</h3>
 *
 * <p>This is a lightweight heuristic rather than a formal volatility model. True realized
 * volatility (standard deviation over returns) requires square-root and division operations that
 * are expensive on the hot path and difficult to implement in pure fixed-point without precision
 * loss. The range-based approach provides a monotone proxy for volatility that is trivial to
 * compute in integer arithmetic, is robust to the small window sizes (10-50 ticks) typical in a
 * streaming pricing service, and is sufficient for the spread-widening use case where the goal is
 * directional (wider vs. narrower) rather than precise.
 *
 * <h3>Threading model</h3>
 *
 * <p><b>Not thread-safe.</b> All methods are called exclusively from the pricing-service agent's
 * single-threaded duty cycle. The probe key is mutated in place on each lookup and must not be
 * accessed concurrently.
 *
 * <h3>Allocation behaviour</h3>
 *
 * <p><b>Zero allocation after construction.</b> All ring buffers and per-symbol state objects are
 * pre-allocated via {@link #registerSymbol(DirectBuffer, int, int)} at startup. The reusable probe
 * key avoids per-lookup allocation. Mid-rate recording and multiplier queries perform only array
 * reads/writes and integer arithmetic.
 *
 * @see TieredSpreadModel
 */
public final class VolatilityMonitor {

  /** Maximum byte length of an SBE Symbol field. */
  private static final int SYMBOL_LENGTH = 8;

  /** Default initial capacity for the symbol map. */
  private static final int INITIAL_CAPACITY = 64;

  /** Load factor for fewer collisions on the Agrona open-addressing map. */
  private static final float LOAD_FACTOR = 0.55f;

  /** Number of mid-rate observations retained in each symbol's ring buffer. */
  private final int windowSize;

  /**
   * Volatility threshold in basis points. When the range (max - min) over the window exceeds {@code
   * midRate * widenThresholdBps / 10_000}, the multiplier begins to increase above 100.
   */
  private final int widenThresholdBps;

  /**
   * Maximum multiplier returned by {@link #volatilityMultiplier}, scaled by 100. For example, 300 =
   * 3.00x.
   */
  private final int maxMultiplier;

  /** Per-symbol rolling-window state, keyed by owned ByteArrayKey. */
  private final Object2ObjectHashMap<ByteArrayKey, SymbolWindow> windows =
      new Object2ObjectHashMap<>(INITIAL_CAPACITY, LOAD_FACTOR);

  /** Reusable probe key for zero-allocation map lookups. Never inserted into the map. */
  private final ByteArrayKey probeKey = ByteArrayKey.emptyForLookup(SYMBOL_LENGTH);

  /**
   * Constructs a volatility monitor with the given window and threshold parameters.
   *
   * <p>Symbols must be registered via {@link #registerSymbol(DirectBuffer, int, int)} before
   * mid-rates can be recorded or multipliers queried.
   *
   * @param windowSize number of mid-rate observations per symbol ring buffer; must be {@code >= 2}
   *     (a single observation has no range)
   * @param widenThresholdBps the basis-point threshold above which the multiplier increases; must
   *     be {@code > 0}
   * @param maxMultiplier maximum multiplier scaled by 100 (e.g., 300 = 3.00x); must be {@code >=
   *     100}
   */
  public VolatilityMonitor(
      final int windowSize, final int widenThresholdBps, final int maxMultiplier) {
    if (windowSize < 2) {
      throw new IllegalArgumentException("windowSize must be >= 2, got " + windowSize);
    }
    if (widenThresholdBps <= 0) {
      throw new IllegalArgumentException("widenThresholdBps must be > 0, got " + widenThresholdBps);
    }
    if (maxMultiplier < 100) {
      throw new IllegalArgumentException("maxMultiplier must be >= 100, got " + maxMultiplier);
    }
    this.windowSize = windowSize;
    this.widenThresholdBps = widenThresholdBps;
    this.maxMultiplier = maxMultiplier;
  }

  /**
   * Pre-allocates the ring buffer and state for a symbol. Must be called at startup for every
   * symbol that will be priced. Subsequent calls with the same symbol are safe no-ops.
   *
   * <p><b>Allocation:</b> allocates a {@link ByteArrayKey} (owned copy), a {@link SymbolWindow},
   * and a {@code long[windowSize]} ring buffer. This is a cold-path operation performed only at
   * initialization.
   *
   * @param symbol buffer containing the symbol bytes
   * @param offset start offset within the buffer
   * @param length number of bytes (must be {@code <= SYMBOL_LENGTH})
   */
  public void registerSymbol(final DirectBuffer symbol, final int offset, final int length) {
    probeKey.set(symbol, offset, length);
    if (windows.get(probeKey) != null) {
      return; // Already registered — idempotent.
    }
    final ByteArrayKey ownedKey = ByteArrayKey.copyOf(symbol, offset, length);
    windows.put(ownedKey, new SymbolWindow(windowSize));
  }

  /**
   * Records a new mid-rate observation for the given symbol, advancing its ring buffer.
   *
   * <p>If the symbol has not been registered via {@link #registerSymbol}, this method is a silent
   * no-op — unknown symbols are not priced, so there is no window to update.
   *
   * <p><b>Allocation:</b> zero allocation.
   *
   * @param symbol buffer containing the symbol bytes
   * @param offset start offset within the buffer
   * @param length number of bytes (must be {@code <= SYMBOL_LENGTH})
   * @param midRate the observed mid-rate in fixed-point {@code 10^-8}
   */
  public void recordMidRate(
      final DirectBuffer symbol, final int offset, final int length, final long midRate) {
    probeKey.set(symbol, offset, length);
    final SymbolWindow window = windows.get(probeKey);
    if (window != null) {
      window.record(midRate);
    }
  }

  /**
   * Returns the volatility-based spread multiplier for the given symbol, scaled by 100.
   *
   * <p>Returns 100 (1.00x, no widening) when:
   *
   * <ul>
   *   <li>The symbol has not been registered
   *   <li>Fewer than 2 observations have been recorded (range is undefined)
   *   <li>The range over the window is at or below the threshold
   * </ul>
   *
   * <p>When the range exceeds the threshold, the multiplier increases linearly:
   *
   * <pre>
   *   multiplier = 100 + (range - threshold) * 100 / threshold
   * </pre>
   *
   * capped at {@link #maxMultiplier}.
   *
   * <p><b>Allocation:</b> zero allocation.
   *
   * @param symbol buffer containing the symbol bytes
   * @param offset start offset within the buffer
   * @param length number of bytes (must be {@code <= SYMBOL_LENGTH})
   * @return multiplier scaled by 100; at least 100, at most {@link #maxMultiplier}
   */
  public int volatilityMultiplier(final DirectBuffer symbol, final int offset, final int length) {
    probeKey.set(symbol, offset, length);
    final SymbolWindow window = windows.get(probeKey);
    if (window == null || window.count < 2) {
      return 100;
    }

    final long range = window.max - window.min;
    // Threshold in price units: midRate * widenThresholdBps / 10_000.
    // Use the most recent mid-rate as the reference for the threshold calculation.
    final long recentMid = window.buffer[window.lastIndex()];
    if (recentMid <= 0) {
      return 100;
    }

    // threshold = recentMid * widenThresholdBps / 10_000
    // Multiply first for precision; no overflow risk for realistic midRate (<= 10^11 scaled)
    // and bps (<= 10^4): max product ~10^15, well within long range.
    final long threshold = PricingMath.mulDiv(recentMid, widenThresholdBps, 10_000L);
    if (threshold <= 0 || range <= threshold) {
      return 100;
    }

    // Linear increase: 100 + (range - threshold) * 100 / threshold, capped at maxMultiplier.
    final long excess = range - threshold;
    final int widening = (int) Math.min(excess * 100L / threshold, maxMultiplier - 100L);
    return Math.min(100 + widening, maxMultiplier);
  }

  /**
   * Per-symbol rolling window state. Holds a pre-allocated ring buffer of mid-rate observations and
   * tracks the current min/max over the populated portion of the window.
   *
   * <p><b>Min/max tracking.</b> Rather than scanning the entire buffer on every query (O(N)), we
   * recompute min/max lazily when the evicted element was the current min or max. For typical
   * window sizes (10-50 entries) the scan cost is negligible on the rare eviction path, and the
   * common-case update is O(1).
   *
   * <p><b>Not thread-safe.</b> Accessed only from the pricing-service agent thread.
   */
  static final class SymbolWindow {

    /** Ring buffer of mid-rate observations in fixed-point {@code 10^-8}. */
    final long[] buffer;

    /** Write index into {@link #buffer}, wrapping at {@code buffer.length}. */
    int writeIndex;

    /** Number of valid observations in the buffer (capped at {@code buffer.length}). */
    int count;

    /** Current minimum mid-rate over the populated window entries. */
    long min;

    /** Current maximum mid-rate over the populated window entries. */
    long max;

    /**
     * Constructs a symbol window with a pre-allocated ring buffer.
     *
     * @param windowSize number of observations to retain; must be {@code >= 2}
     */
    SymbolWindow(final int windowSize) {
      this.buffer = new long[windowSize];
      this.writeIndex = 0;
      this.count = 0;
      this.min = Long.MAX_VALUE;
      this.max = Long.MIN_VALUE;
    }

    /**
     * Records a mid-rate observation, evicting the oldest entry if the buffer is full. Updates
     * min/max tracking accordingly.
     *
     * <p><b>Allocation:</b> zero allocation.
     *
     * @param midRate the observed mid-rate in fixed-point {@code 10^-8}
     */
    void record(final long midRate) {
      final boolean full = count == buffer.length;
      final long evicted = full ? buffer[writeIndex] : Long.MAX_VALUE;

      buffer[writeIndex] = midRate;
      writeIndex = (writeIndex + 1) % buffer.length;

      if (!full) {
        count++;
      }

      // Fast path: new value extends or maintains the current min/max.
      if (midRate <= min) {
        min = midRate;
      }
      if (midRate >= max) {
        max = midRate;
      }

      // Slow path: the evicted value was the min or max, so we must rescan.
      if (full && (evicted == min || evicted == max)) {
        recomputeMinMax();
      }
    }

    /**
     * Returns the index of the most recently written entry. Only valid when {@code count >= 1}.
     *
     * @return the index of the last written mid-rate
     */
    int lastIndex() {
      return (writeIndex == 0 ? buffer.length : writeIndex) - 1;
    }

    /**
     * Rescans the populated portion of the ring buffer to recompute min and max. Called only when
     * the evicted entry was the current min or max — a rare event for most price series.
     */
    private void recomputeMinMax() {
      long newMin = Long.MAX_VALUE;
      long newMax = Long.MIN_VALUE;
      final int entries = Math.min(count, buffer.length);
      for (int i = 0; i < entries; i++) {
        final long val = buffer[i];
        if (val < newMin) {
          newMin = val;
        }
        if (val > newMax) {
          newMax = val;
        }
      }
      min = newMin;
      max = newMax;
    }
  }
}
