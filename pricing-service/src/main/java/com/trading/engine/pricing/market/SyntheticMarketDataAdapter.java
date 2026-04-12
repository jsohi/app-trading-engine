package com.trading.engine.pricing.market;

import static com.trading.engine.pricing.PricingMath.mulDiv;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.trading.engine.pricing.XorShift128;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Market data adapter that generates realistic FX price series using geometric Brownian motion
 * (GBM) with mean reversion. Intended for dev and staging environments where deterministic but
 * non-trivial price behaviour is needed for spread calculation, inventory-skew testing, and UI
 * visualisation.
 *
 * <p><b>Not for production</b> — replace with a real market data adapter backed by a live feed
 * (e.g., Refinitiv Elektron, Bloomberg B-PIPE) or a replay adapter for historical tick data.
 *
 * <h3>Price dynamics</h3>
 *
 * <p>On each tick (governed by {@code updateIntervalNanos}), the adapter applies a simplified GBM
 * step to each symbol:
 *
 * <pre>
 *   delta     = currentRate * volatilityBps * U(-100..100) / (10_000 * 100)
 *   reversion = (baseRate - currentRate) * meanReversionStrength / 10_000
 *   newRate   = currentRate + delta + reversion
 *   clamp     to [baseRate / 2, baseRate * 2]
 * </pre>
 *
 * <p>The random component {@code U(-100..100)} is a uniform approximation of a normal variate,
 * sufficient for visual realism without the cost of a Box-Muller transform. The mean-reversion term
 * prevents the price from drifting too far from the configured base rate. The hard clamp is a
 * safety net against pathological PRNG sequences.
 *
 * <h3>Threading model</h3>
 *
 * <p><b>Not thread-safe.</b> Single-threaded agent duty cycle — all reads and writes to internal
 * state and the shared {@link MidRateCache} occur on the same thread.
 *
 * <h3>Allocation behaviour</h3>
 *
 * <p>Zero allocation after construction. Per-symbol state is stored in pre-allocated parallel
 * arrays indexed by a construction-time ordinal. The {@link XorShift128} PRNG is stack-only. {@link
 * MidRateCache#put} mutates existing entries in place for known symbols. The {@link
 * #symbolKeyBuffer} wraps the flat {@link #symbolKeys} byte array without copying, so passing
 * symbol bytes to the cache is zero-allocation.
 *
 * @see MarketDataAdapter
 * @see MidRateCache
 * @see XorShift128
 */
public final class SyntheticMarketDataAdapter implements MarketDataAdapter {

  private static final Log LOG = LogFactory.getLog(SyntheticMarketDataAdapter.class);

  /**
   * Divisor for the volatility calculation: {@code volatilityBps / 10_000} gives the decimal
   * fraction, and the uniform range {@code [-100, 100]} is divided by 100. Combined: {@code 10_000
   * * 100 = 1_000_000}.
   */
  private static final long VOLATILITY_DIVISOR = 10_000L * 100L;

  /**
   * Divisor for mean-reversion strength. A strength of 100 means 0.01 (1%), stored as an integer
   * scaled by 10,000.
   */
  private static final long REVERSION_DIVISOR = 10_000L;

  /**
   * Half the uniform range width. The PRNG output is mapped to {@code [-UNIFORM_HALF,
   * +UNIFORM_HALF]}.
   */
  private static final long UNIFORM_HALF = 100L;

  /** Full width of the uniform range: {@code 2 * UNIFORM_HALF + 1 = 201}. */
  private static final long UNIFORM_RANGE = 2L * UNIFORM_HALF + 1L;

  /** Shared cache that the pricing service reads from on the same thread. */
  private final MidRateCache cache;

  /** Monotonic clock for interval checking and timestamping updates. */
  private final NanoClock nanoClock;

  /** Deterministic PRNG — no heap allocation per call. */
  private final XorShift128 prng;

  /** Minimum nanoseconds between price updates. Controls the tick frequency. */
  private final long updateIntervalNanos;

  /** Volatility in basis points. Higher values produce wider per-tick price swings. */
  private final int volatilityBps;

  /** Mean-reversion strength scaled by 10,000 (e.g., 100 = 0.01 = 1%). */
  private final int meanReversionStrength;

  /** Number of symbols managed by this adapter. Fixed at construction. */
  private final int symbolCount;

  /**
   * Per-symbol base rate (the original rate from config). Used as the mean-reversion anchor.
   * Indexed by symbol ordinal assigned at construction.
   */
  private final long[] baseRates;

  /**
   * Per-symbol current rate (the latest computed rate). Updated on every tick. Indexed by symbol
   * ordinal assigned at construction.
   */
  private final long[] currentRates;

  /**
   * Per-symbol 8-byte key stored as a flat byte array. Symbol ordinal {@code i} occupies bytes
   * {@code [i * SYMBOL_LENGTH, (i + 1) * SYMBOL_LENGTH)}. Used to write updated rates back into the
   * cache without allocating a buffer.
   */
  private final byte[] symbolKeys;

  /**
   * Pre-allocated {@link UnsafeBuffer} wrapping {@link #symbolKeys}. Passed to {@link
   * MidRateCache#put} with the appropriate offset for each symbol ordinal, avoiding any per-tick
   * buffer allocation. The wrap is set once at construction and never changed.
   */
  private final UnsafeBuffer symbolKeyBuffer;

  /**
   * Monotonic nanosecond timestamp of the last update cycle. Used to determine whether {@code
   * updateIntervalNanos} has elapsed since the previous tick.
   */
  private long lastUpdateNanos;

  /**
   * Constructs a synthetic market data adapter with pre-allocated per-symbol state.
   *
   * <p>The caller must have already loaded base rates into the cache via {@link
   * MidRateCache#preload} before constructing this adapter. The caller must then register each
   * symbol's base rate via {@link #registerSymbol(int, byte[], long)} before the agent is started.
   *
   * @param cache the mid-rate cache shared with the pricing service
   * @param nanoClock monotonic clock for interval checking
   * @param prng deterministic PRNG for reproducible price sequences
   * @param updateIntervalNanos minimum nanoseconds between price update cycles; must be {@code > 0}
   * @param volatilityBps per-tick volatility in basis points (e.g., 10 = 0.10%)
   * @param meanReversionStrength reversion pull towards the base rate, scaled by 10,000 (e.g., 100
   *     = 0.01 = 1% per tick)
   * @param symbolCount the number of symbols this adapter will manage
   */
  public SyntheticMarketDataAdapter(
      final MidRateCache cache,
      final NanoClock nanoClock,
      final XorShift128 prng,
      final long updateIntervalNanos,
      final int volatilityBps,
      final int meanReversionStrength,
      final int symbolCount) {
    this.cache = cache;
    this.nanoClock = nanoClock;
    this.prng = prng;
    this.updateIntervalNanos = updateIntervalNanos;
    this.volatilityBps = volatilityBps;
    this.meanReversionStrength = meanReversionStrength;
    this.symbolCount = symbolCount;
    this.baseRates = new long[symbolCount];
    this.currentRates = new long[symbolCount];
    this.symbolKeys = new byte[symbolCount * MidRateCache.SYMBOL_LENGTH];
    this.symbolKeyBuffer = new UnsafeBuffer(symbolKeys);
    this.lastUpdateNanos = 0L;
  }

  /**
   * Registers a symbol's base rate and key bytes at the given ordinal index. Must be called once
   * per symbol before the agent is started, in ordinal order {@code [0, symbolCount)}.
   *
   * <p>This is a cold-path operation invoked only during setup — allocates nothing.
   *
   * @param ordinal zero-based index for this symbol in the parallel arrays
   * @param symbol the 8-byte symbol key (SBE Symbol type, right-padded)
   * @param baseRate the initial mid-rate in fixed-point {@code 10^-8}
   * @throws ArrayIndexOutOfBoundsException if {@code ordinal} is out of range
   */
  public void registerSymbol(final int ordinal, final byte[] symbol, final long baseRate) {
    if (symbol.length < MidRateCache.SYMBOL_LENGTH) {
      throw new IllegalArgumentException(
          "symbol byte array length "
              + symbol.length
              + " is less than required "
              + MidRateCache.SYMBOL_LENGTH);
    }
    baseRates[ordinal] = baseRate;
    currentRates[ordinal] = baseRate;
    System.arraycopy(
        symbol, 0, symbolKeys, ordinal * MidRateCache.SYMBOL_LENGTH, MidRateCache.SYMBOL_LENGTH);
  }

  /** Sets the initial {@link #lastUpdateNanos} timestamp and logs the adapter start. */
  @Override
  public void onStart() {
    lastUpdateNanos = nanoClock.nanoTime();
    LOG.info()
        .append("SyntheticMarketDataAdapter started: symbols=")
        .append(symbolCount)
        .append(" intervalNs=")
        .append(updateIntervalNanos)
        .append(" volBps=")
        .append(volatilityBps)
        .append(" meanRev=")
        .append(meanReversionStrength)
        .commit();
  }

  /** No-op — no external resources to release. */
  @Override
  public void onClose() {
    // Intentionally empty.
  }

  /**
   * Agent duty-cycle tick. Checks whether the update interval has elapsed; if so, applies a GBM
   * step with mean reversion to every symbol and writes the updated rates into the cache.
   *
   * <p>Zero allocation — all arithmetic uses pre-allocated arrays and fixed-point {@code long}
   * operations. The {@link #symbolKeyBuffer} wraps the flat {@link #symbolKeys} array, so passing
   * symbol bytes to the cache does not allocate.
   *
   * @return {@code 1} if prices were updated, {@code 0} if the interval has not yet elapsed
   */
  @Override
  public int doWork() {
    final long now = nanoClock.nanoTime();
    if (now - lastUpdateNanos < updateIntervalNanos) {
      return 0;
    }

    for (int i = 0; i < symbolCount; i++) {
      final long base = baseRates[i];
      long current = currentRates[i];

      // Geometric Brownian motion step: delta = currentRate * volatilityBps * U(-100..100)
      // divided by (10_000 * 100). Use nextPositiveLong() to avoid negative-modulo bias
      // (Java's % preserves sign of the dividend, producing a skewed [-300,+100] range
      // instead of the intended [-100,+100] when nextLong() returns negative values).
      final long uniformSample = (prng.nextPositiveLong() % UNIFORM_RANGE) - UNIFORM_HALF;
      final long delta = mulDiv(current, (long) volatilityBps * uniformSample, VOLATILITY_DIVISOR);

      // Mean-reversion pull towards the base rate.
      final long reversion = mulDiv(base - current, meanReversionStrength, REVERSION_DIVISOR);

      current = current + delta + reversion;

      // Hard clamp to [baseRate/2, baseRate*2] to prevent runaway drift.
      final long lowerBound = base >> 1; // base / 2 (unsigned-safe for positive rates)
      final long upperBound = base << 1; // base * 2 (overflow impossible for FX rates)
      if (current < lowerBound) {
        current = lowerBound;
      } else if (current > upperBound) {
        current = upperBound;
      }

      currentRates[i] = current;

      // Write updated rate into cache via the pre-allocated UnsafeBuffer wrapper.
      // symbolKeyBuffer wraps the flat symbolKeys byte[], so we pass the appropriate
      // offset for this symbol's 8-byte slice — zero allocation for existing entries.
      final int keyOffset = i * MidRateCache.SYMBOL_LENGTH;
      cache.put(symbolKeyBuffer, keyOffset, MidRateCache.SYMBOL_LENGTH, current, now);
    }

    lastUpdateNanos = now;
    return 1;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Delegates to the shared {@link MidRateCache}. Zero allocation.
   */
  @Override
  public long midRate(final DirectBuffer symbol, final int offset, final int length) {
    return cache.midRate(symbol, offset, length);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Delegates to the shared {@link MidRateCache}. Zero allocation.
   */
  @Override
  public long lastUpdateNanos(final DirectBuffer symbol, final int offset, final int length) {
    return cache.lastUpdateNanos(symbol, offset, length);
  }

  /**
   * Returns the agent role name for diagnostics and error reporting.
   *
   * @return {@code "synthetic-market-data"}
   */
  @Override
  public String roleName() {
    return "synthetic-market-data";
  }
}
