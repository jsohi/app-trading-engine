package com.trading.engine.pricing;

/**
 * Immutable configuration loaded from {@code pricing-config.yaml} at startup. All values are
 * pre-converted to nanoseconds and fixed-point at load time so the hot path never performs Map
 * lookups, unit conversions, or string parsing.
 *
 * <p>The YAML loader (in the launcher module) reads the human-friendly configuration file and
 * constructs this record via the all-fields constructor. Once constructed, the record is passed by
 * reference into the pricing-service agent and its collaborators. No field is ever mutated after
 * construction.
 *
 * <h3>Field conventions</h3>
 *
 * <ul>
 *   <li><b>Time fields</b> are stored as {@code long} nanoseconds, matching the engine's universal
 *       nanosecond clock convention (see {@link com.trading.engine.messages.clock.TradingClocks}).
 *   <li><b>Basis-point fields</b> are stored as {@code int} (1 bp = 0.01%).
 *   <li><b>Seed values</b> are stored as {@code long} for deterministic PRNG replay.
 *   <li><b>String fields</b> (adapter type, idle strategy) are stored as interned {@link String}s
 *       for identity comparison on the cold startup path.
 * </ul>
 *
 * <h3>Threading model</h3>
 *
 * <p>Effectively immutable after construction. All fields are {@code final} and of primitive or
 * immutable types. Safe for unrestricted concurrent access from any thread, though in practice only
 * read from the single-threaded pricing-service duty cycle and the cold startup path.
 *
 * <h3>Allocation behaviour</h3>
 *
 * <p>Zero allocation after construction. No methods allocate; all fields are primitives or
 * pre-existing {@link String} references.
 *
 * @see PricingService
 * @see PricingConstants
 */
public final class PricingServiceConfig {

  // ===========================================================================
  // Adapter selection
  // ===========================================================================

  /**
   * Market data adapter type: {@code "deterministic"} for integration-test fixtures, {@code
   * "synthetic"} for dev/staging Brownian-motion prices. Determines which {@link
   * com.trading.engine.pricing.market.MarketDataAdapter} implementation the launcher wires into the
   * pricing service.
   */
  private final String adapterType;

  // ===========================================================================
  // Staleness thresholds
  // ===========================================================================

  /**
   * Maximum age of a market data update (in nanoseconds, monotonic clock) before a price request is
   * rejected outright. If {@code nanoClock.nanoTime() - lastUpdateNanos(symbol) >
   * staleThresholdNanos}, the pricing service returns a declined PriceResponse with {@link
   * com.trading.engine.messages.sbe.QuoteRejectReasonEnum#Other} and text "Stale market data".
   *
   * <p>Converted from seconds in the YAML file to nanoseconds at load time.
   */
  private final long staleThresholdNanos;

  /**
   * Market data age threshold (in nanoseconds, monotonic clock) beyond which the spread is widened
   * via the {@link com.trading.engine.pricing.spread.VolatilityMonitor}. When the data age exceeds
   * this threshold but is still below {@link #staleThresholdNanos}, pricing continues but the
   * spread model applies a widening penalty to compensate for increased uncertainty.
   *
   * <p>Must be {@code <= staleThresholdNanos}. Converted from seconds in the YAML file.
   */
  private final long staleWidenThresholdNanos;

  // ===========================================================================
  // Synthetic adapter parameters
  // ===========================================================================

  /**
   * Interval (in nanoseconds) between synthetic price updates when using the {@link
   * com.trading.engine.pricing.market.SyntheticMarketDataAdapter}. Ignored when {@link
   * #adapterType} is {@code "deterministic"}.
   */
  private final long syntheticUpdateIntervalNanos;

  /**
   * Volatility parameter for the synthetic adapter, expressed in basis points (1 bp = 0.01%).
   * Controls the magnitude of random mid-rate perturbations per update tick.
   */
  private final int syntheticVolatilityBps;

  /**
   * Mean-reversion strength for the synthetic adapter, scaled by 100 (e.g., 10 = 0.10x pull toward
   * the reference mid). Higher values produce tighter oscillation around the reference price.
   */
  private final int syntheticMeanReversionStrength;

  /**
   * PRNG seed for the synthetic adapter. Setting a fixed seed produces deterministic price paths
   * across runs, enabling reproducible integration tests and performance benchmarks.
   */
  private final long syntheticSeed;

  // ===========================================================================
  // Quote TTL per product type
  // ===========================================================================

  /**
   * Time-to-live for Spot quotes (in nanoseconds). After this duration, the quote expires and can
   * no longer be executed. Spot TTLs are typically short (1-5 seconds) to limit market-maker
   * exposure to latency arbitrage.
   *
   * <p>Converted from seconds in the YAML file to nanoseconds at load time.
   */
  private final long spotTtlNanos;

  /**
   * Time-to-live for Forward quotes (in nanoseconds). Forward quotes typically have longer TTLs
   * than spot because the forward market is less liquid and clients need more time to evaluate
   * complex settlement structures.
   */
  private final long forwardTtlNanos;

  /**
   * Time-to-live for Swap quotes (in nanoseconds). Swap quotes carry the longest TTL because swap
   * pricing involves two legs and clients often need to confirm with back-office before executing.
   */
  private final long swapTtlNanos;

  // ===========================================================================
  // Quote pool sizing
  // ===========================================================================

  /**
   * Maximum number of concurrently active quotes tracked by the {@link
   * com.trading.engine.pricing.quote.QuoteManager}. Determines the pre-allocated pool size. When
   * the pool is exhausted, the oldest quote is evicted in FIFO order.
   */
  private final int maxActiveQuotes;

  // ===========================================================================
  // Expiry sweep
  // ===========================================================================

  /**
   * Interval (in nanoseconds, monotonic clock) between expired-quote sweep passes. The pricing
   * service calls {@link com.trading.engine.pricing.quote.QuoteManager#expireStale(long)} at most
   * once per this interval to avoid the O(n) scan cost on every duty-cycle tick.
   *
   * <p>Converted from milliseconds in the YAML file to nanoseconds at load time.
   */
  private final long sweepIntervalNanos;

  // ===========================================================================
  // Agent idle strategy
  // ===========================================================================

  /**
   * Idle strategy for the Aeron {@link org.agrona.concurrent.AgentRunner}: one of {@code
   * "backoff"}, {@code "yielding"}, or {@code "busy-spin"}. The launcher maps this string to the
   * corresponding Agrona {@link org.agrona.concurrent.IdleStrategy} implementation.
   *
   * <ul>
   *   <li>{@code "backoff"} — {@link org.agrona.concurrent.BackoffIdleStrategy}: progressive
   *       spin/yield/park; lowest CPU usage, highest tail latency.
   *   <li>{@code "yielding"} — {@link org.agrona.concurrent.YieldingIdleStrategy}: Thread.yield();
   *       moderate CPU, moderate latency.
   *   <li>{@code "busy-spin"} — {@link org.agrona.concurrent.BusySpinIdleStrategy}: no yield; burns
   *       a core for lowest latency.
   * </ul>
   */
  private final String idleStrategy;

  // ===========================================================================
  // Inventory skew model
  // ===========================================================================

  /**
   * Inventory skew model type: {@code "convex"} for production quadratic skew or {@code "linear"}
   * for testing/dev. Determines which {@link com.trading.engine.pricing.skew.InventorySkewModel}
   * implementation the launcher wires.
   *
   * <ul>
   *   <li>{@code "convex"} — {@link com.trading.engine.pricing.skew.ConvexInventorySkew}: quadratic
   *       penalty that increases aggressively near the position threshold. Production default.
   *   <li>{@code "linear"} — {@link com.trading.engine.pricing.skew.LinearInventorySkew}: constant
   *       marginal penalty per unit of inventory. Simpler, for testing.
   * </ul>
   */
  private final String skewModelType;

  /**
   * Constructs an immutable pricing service configuration with all fields pre-converted to hot-path
   * types.
   *
   * <p>This constructor is called by the YAML configuration loader at startup. All time values must
   * already be in nanoseconds; the constructor does not perform unit conversions.
   *
   * @param adapterType market data adapter type: {@code "deterministic"} or {@code "synthetic"}
   * @param staleThresholdNanos maximum market data age before outright rejection (nanos)
   * @param staleWidenThresholdNanos market data age threshold for spread widening (nanos); must be
   *     {@code <= staleThresholdNanos}
   * @param syntheticUpdateIntervalNanos synthetic adapter update interval (nanos)
   * @param syntheticVolatilityBps synthetic adapter volatility in basis points
   * @param syntheticMeanReversionStrength synthetic adapter mean-reversion strength (x100)
   * @param syntheticSeed PRNG seed for the synthetic adapter
   * @param spotTtlNanos time-to-live for Spot quotes (nanos)
   * @param forwardTtlNanos time-to-live for Forward quotes (nanos)
   * @param swapTtlNanos time-to-live for Swap quotes (nanos)
   * @param maxActiveQuotes maximum concurrently active quotes; must be {@code > 0}
   * @param sweepIntervalNanos interval between expired-quote sweep passes (nanos)
   * @param idleStrategy agent idle strategy: {@code "backoff"}, {@code "yielding"}, or {@code
   *     "busy-spin"}
   * @param skewModelType inventory skew model type: {@code "convex"} or {@code "linear"}
   * @throws IllegalArgumentException if {@code maxActiveQuotes <= 0}, or {@code
   *     staleWidenThresholdNanos > staleThresholdNanos}, or {@code adapterType} is null, or {@code
   *     idleStrategy} is null, or {@code skewModelType} is null
   */
  public PricingServiceConfig(
      final String adapterType,
      final long staleThresholdNanos,
      final long staleWidenThresholdNanos,
      final long syntheticUpdateIntervalNanos,
      final int syntheticVolatilityBps,
      final int syntheticMeanReversionStrength,
      final long syntheticSeed,
      final long spotTtlNanos,
      final long forwardTtlNanos,
      final long swapTtlNanos,
      final int maxActiveQuotes,
      final long sweepIntervalNanos,
      final String idleStrategy,
      final String skewModelType) {

    if (adapterType == null) {
      throw new IllegalArgumentException("adapterType must not be null");
    }
    if (idleStrategy == null) {
      throw new IllegalArgumentException("idleStrategy must not be null");
    }
    if (skewModelType == null) {
      throw new IllegalArgumentException("skewModelType must not be null");
    }
    if (maxActiveQuotes <= 0) {
      throw new IllegalArgumentException("maxActiveQuotes must be > 0, got: " + maxActiveQuotes);
    }
    if (staleWidenThresholdNanos > staleThresholdNanos) {
      throw new IllegalArgumentException(
          "staleWidenThresholdNanos ("
              + staleWidenThresholdNanos
              + ") must be <= staleThresholdNanos ("
              + staleThresholdNanos
              + ")");
    }
    if ("synthetic".equals(adapterType) && syntheticSeed == 0) {
      throw new IllegalArgumentException(
          "syntheticSeed must be non-zero when adapterType is 'synthetic'");
    }

    this.adapterType = adapterType;
    this.staleThresholdNanos = staleThresholdNanos;
    this.staleWidenThresholdNanos = staleWidenThresholdNanos;
    this.syntheticUpdateIntervalNanos = syntheticUpdateIntervalNanos;
    this.syntheticVolatilityBps = syntheticVolatilityBps;
    this.syntheticMeanReversionStrength = syntheticMeanReversionStrength;
    this.syntheticSeed = syntheticSeed;
    this.spotTtlNanos = spotTtlNanos;
    this.forwardTtlNanos = forwardTtlNanos;
    this.swapTtlNanos = swapTtlNanos;
    this.maxActiveQuotes = maxActiveQuotes;
    this.sweepIntervalNanos = sweepIntervalNanos;
    this.idleStrategy = idleStrategy;
    this.skewModelType = skewModelType;
  }

  /**
   * Convenience constructor for launcher/test use that provides sensible defaults for all tuning
   * parameters. Only the adapter type and skew model type need to be specified.
   *
   * <p><b>Defaults:</b>
   *
   * <ul>
   *   <li>Stale threshold: 5 seconds
   *   <li>Stale-widen threshold: 2 seconds
   *   <li>Synthetic update interval: 100ms
   *   <li>Synthetic volatility: 10 bps
   *   <li>Synthetic mean-reversion: 100 (1%)
   *   <li>Synthetic seed: 42 (deterministic)
   *   <li>Spot TTL: 3 seconds
   *   <li>Forward TTL: 10 seconds
   *   <li>Swap TTL: 15 seconds
   *   <li>Max active quotes: 10,000
   *   <li>Sweep interval: 100ms
   *   <li>Idle strategy: "backoff"
   * </ul>
   *
   * @param adapterType market data adapter type: {@code "deterministic"} or {@code "synthetic"}
   * @param skewModelType inventory skew model type: {@code "convex"} or {@code "linear"}
   */
  public PricingServiceConfig(final String adapterType, final String skewModelType) {
    this(
        adapterType,
        5_000_000_000L, // staleThresholdNanos (5s)
        2_000_000_000L, // staleWidenThresholdNanos (2s)
        100_000_000L, // syntheticUpdateIntervalNanos (100ms)
        10, // syntheticVolatilityBps
        100, // syntheticMeanReversionStrength
        42L, // syntheticSeed
        3_000_000_000L, // spotTtlNanos (3s)
        10_000_000_000L, // forwardTtlNanos (10s)
        15_000_000_000L, // swapTtlNanos (15s)
        10_000, // maxActiveQuotes
        100_000_000L, // sweepIntervalNanos (100ms)
        "backoff", // idleStrategy
        skewModelType);
  }

  // ===========================================================================
  // Accessors — zero allocation, direct field reads
  // ===========================================================================

  /**
   * Returns the market data adapter type.
   *
   * @return {@code "deterministic"} or {@code "synthetic"}
   */
  public String adapterType() {
    return adapterType;
  }

  /**
   * Returns the maximum market data age before outright rejection, in nanoseconds.
   *
   * @return stale threshold in nanoseconds
   */
  public long staleThresholdNanos() {
    return staleThresholdNanos;
  }

  /**
   * Returns the market data age threshold for spread widening, in nanoseconds.
   *
   * @return stale-widen threshold in nanoseconds; always {@code <= staleThresholdNanos()}
   */
  public long staleWidenThresholdNanos() {
    return staleWidenThresholdNanos;
  }

  /**
   * Returns the synthetic adapter update interval, in nanoseconds.
   *
   * @return update interval in nanoseconds
   */
  public long syntheticUpdateIntervalNanos() {
    return syntheticUpdateIntervalNanos;
  }

  /**
   * Returns the synthetic adapter volatility parameter, in basis points.
   *
   * @return volatility in basis points
   */
  public int syntheticVolatilityBps() {
    return syntheticVolatilityBps;
  }

  /**
   * Returns the synthetic adapter mean-reversion strength, scaled by 100.
   *
   * @return mean-reversion strength (x100)
   */
  public int syntheticMeanReversionStrength() {
    return syntheticMeanReversionStrength;
  }

  /**
   * Returns the PRNG seed for the synthetic adapter.
   *
   * @return seed value
   */
  public long syntheticSeed() {
    return syntheticSeed;
  }

  /**
   * Returns the time-to-live for Spot quotes, in nanoseconds.
   *
   * @return spot TTL in nanoseconds
   */
  public long spotTtlNanos() {
    return spotTtlNanos;
  }

  /**
   * Returns the time-to-live for Forward quotes, in nanoseconds.
   *
   * @return forward TTL in nanoseconds
   */
  public long forwardTtlNanos() {
    return forwardTtlNanos;
  }

  /**
   * Returns the time-to-live for Swap quotes, in nanoseconds.
   *
   * @return swap TTL in nanoseconds
   */
  public long swapTtlNanos() {
    return swapTtlNanos;
  }

  /**
   * Returns the maximum number of concurrently active quotes.
   *
   * @return max active quotes; always {@code > 0}
   */
  public int maxActiveQuotes() {
    return maxActiveQuotes;
  }

  /**
   * Returns the interval between expired-quote sweep passes, in nanoseconds.
   *
   * @return sweep interval in nanoseconds
   */
  public long sweepIntervalNanos() {
    return sweepIntervalNanos;
  }

  /**
   * Returns the agent idle strategy name.
   *
   * @return one of {@code "backoff"}, {@code "yielding"}, or {@code "busy-spin"}
   */
  public String idleStrategy() {
    return idleStrategy;
  }

  /**
   * Returns the inventory skew model type.
   *
   * @return {@code "convex"} or {@code "linear"}
   */
  public String skewModelType() {
    return skewModelType;
  }

  /**
   * Returns the PRNG seed for the synthetic adapter. Alias for {@link #syntheticSeed()} to provide
   * a more intuitive name when used from the launcher's adapter construction code.
   *
   * @return seed value
   */
  public long prngSeed() {
    return syntheticSeed;
  }
}
