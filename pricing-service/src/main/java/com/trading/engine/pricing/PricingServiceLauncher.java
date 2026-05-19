package com.trading.engine.pricing;

import static com.trading.engine.messages.MarketDataConstants.MARKET_DATA_CHANNEL;
import static com.trading.engine.messages.MarketDataConstants.MARKET_DATA_SNAPSHOT_REQUEST_STREAM_ID;
import static com.trading.engine.messages.MarketDataConstants.MARKET_DATA_STREAM_ID;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.trading.engine.messages.clock.TradingClocks;
import com.trading.engine.messages.util.ByteArrayKey;
import com.trading.engine.pricing.codec.PricingResponseEncoder;
import com.trading.engine.pricing.forward.ConfigurableForwardPointSource;
import com.trading.engine.pricing.market.BroadcastPublisher;
import com.trading.engine.pricing.market.DeterministicMarketDataAdapter;
import com.trading.engine.pricing.market.MarketDataAdapter;
import com.trading.engine.pricing.market.MarketDataPublisher;
import com.trading.engine.pricing.market.MarketDataPublisherConfig;
import com.trading.engine.pricing.market.MidRateCache;
import com.trading.engine.pricing.market.MidRateToTickBridge;
import com.trading.engine.pricing.market.SyntheticMarketDataAdapter;
import com.trading.engine.pricing.quote.PriceValidator;
import com.trading.engine.pricing.quote.QuoteManager;
import com.trading.engine.pricing.skew.ConfigurablePositionSource;
import com.trading.engine.pricing.skew.ConvexInventorySkew;
import com.trading.engine.pricing.skew.InventorySkewModel;
import com.trading.engine.pricing.skew.LinearInventorySkew;
import com.trading.engine.pricing.spread.ClientTierConfig;
import com.trading.engine.pricing.spread.SpreadConfig;
import com.trading.engine.pricing.spread.TieredSpreadModel;
import com.trading.engine.pricing.spread.VolatilityMonitor;
import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.collections.Object2ObjectHashMap;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.CompositeAgent;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Static factory that wires and starts the pricing service agent. Does NOT start a MediaDriver --
 * expects one running at {@code aeronDir}.
 *
 * <p><b>Construction order.</b> All hot-path objects are pre-allocated during {@link #launch}:
 * MidRateCache -> MarketDataAdapter -> ForwardPointSource -> SpreadConfig/TierConfig ->
 * VolatilityMonitor -> InventorySkewModel -> TieredSpreadModel -> QuoteManager -> PriceValidator ->
 * PricingResponseEncoder -> PricingMessageDispatcher -> PricingService -> AgentRunner -> start
 * thread.
 *
 * <p><b>Threading.</b> Creates one pricing duty-cycle thread named "pricing-service" (from the
 * agent's {@code roleName()}). The pricing service agent's {@code doWork()} polls the inbound
 * subscription, delegates to the market data adapter, and publishes responses on the outbound
 * exclusive publication.
 *
 * <p><b>Idle strategy.</b> Configurable via the {@code idleStrategy} parameter. {@link
 * org.agrona.concurrent.BackoffIdleStrategy} is reasonable for dev; production should use {@link
 * org.agrona.concurrent.YieldingIdleStrategy} (bounded ~10us, no park) or {@link
 * org.agrona.concurrent.BusySpinIdleStrategy} (dedicated core, lowest latency).
 *
 * <p><b>Allocation.</b> All hot-path objects pre-allocated during launch(). The MidRateCache is
 * pre-loaded with symbol base rates; the VolatilityMonitor and ForwardPointSource register symbols
 * at startup. No heap allocation occurs on the pricing duty-cycle hot path after launch completes.
 *
 * @see PricingComponents
 * @see PricingService
 */
public final class PricingServiceLauncher {

  private static final Log LOG = LogFactory.getLog(PricingServiceLauncher.class);

  // ---- Default configuration values (v1 hardcoded, will migrate to YAML) ----

  /** Default maximum concurrently active quotes in the QuoteManager pool. */
  private static final int DEFAULT_MAX_ACTIVE_QUOTES = 10_000;

  /** Default volatility monitor window size (number of mid-rate observations per symbol). */
  private static final int DEFAULT_VOL_WINDOW_SIZE = 20;

  /** Default volatility widening threshold in basis points. */
  private static final int DEFAULT_VOL_WIDEN_THRESHOLD_BPS = 50;

  /** Default maximum volatility multiplier (x100 scale, so 300 = 3.00x). */
  private static final int DEFAULT_VOL_MAX_MULTIPLIER = 300;

  /** Default convex skew maximum in basis points at full inventory. */
  private static final int DEFAULT_SKEW_MAX_BPS = 50;

  /**
   * Default skew position threshold: 10M units in fixed-point 10^-8. Positions beyond this
   * magnitude are clamped.
   */
  private static final long DEFAULT_SKEW_POSITION_THRESHOLD = 10_000_000L * 100_000_000L;

  /** Default convexity exponent (x100 scale): 200 = quadratic (alpha=2.0). */
  private static final int DEFAULT_ALPHA_X100 = 200;

  /** Default synthetic adapter update interval: 100ms in nanoseconds. */
  private static final long DEFAULT_SYNTHETIC_UPDATE_INTERVAL_NS =
      TimeUnit.MILLISECONDS.toNanos(100);

  /** Default synthetic adapter per-tick volatility in basis points. */
  private static final int DEFAULT_SYNTHETIC_VOLATILITY_BPS = 10;

  /** Default synthetic adapter mean-reversion strength (scaled by 10,000; 100 = 1%). */
  private static final int DEFAULT_SYNTHETIC_MEAN_REVERSION = 100;

  /** Default number of forward-point initial capacity. */
  private static final int DEFAULT_FORWARD_POINT_CAPACITY = 32;

  /**
   * Hardcoded FX symbol definitions for v1 compilation. Each entry contains: symbol bytes (8-byte
   * SBE Symbol), base mid-rate in fixed-point 10^-8. The real symbol configuration will be loaded
   * from YAML via PricingServiceConfig in a follow-up task.
   */
  private static final byte[][] SYMBOL_BYTES = {
    padSymbol("EURUSD"),
    padSymbol("GBPUSD"),
    padSymbol("USDJPY"),
    padSymbol("AUDUSD"),
    padSymbol("USDCAD"),
  };

  /**
   * Base mid-rates for the hardcoded symbols, in fixed-point 10^-8. EUR/USD ~1.0850, GBP/USD
   * ~1.2650, USD/JPY ~149.50, AUD/USD ~0.6520, USD/CAD ~1.3580.
   */
  private static final long[] BASE_MID_RATES = {
    108_500_000L, // EUR/USD = 1.08500000
    126_500_000L, // GBP/USD = 1.26500000
    14_950_000_000L, // USD/JPY = 149.50000000
    65_200_000L, // AUD/USD = 0.65200000
    135_800_000L, // USD/CAD = 1.35800000
  };

  /**
   * Default forward point tenor days for the hardcoded symbols. Standard FX tenors: 1W, 1M, 2M, 3M,
   * 6M, 1Y.
   */
  private static final int[] DEFAULT_TENOR_DAYS = {7, 30, 60, 90, 180, 360};

  /**
   * Default forward point values in fixed-point 10^-8. Placeholder values for v1 compilation --
   * real values will come from YAML config.
   */
  private static final long[] DEFAULT_FORWARD_POINTS = {
    -500L, -2_100L, -4_200L, -6_300L, -12_600L, -25_200L
  };

  private PricingServiceLauncher() {} // static factory only

  /**
   * Wires and starts the pricing service with all components connected.
   *
   * <p>The method performs the full 14-step construction sequence:
   *
   * <ol>
   *   <li>Connect to the shared Media Driver at {@code aeronDir}
   *   <li>Create inbound Subscription on {@link PricingConstants#IPC_CHANNEL} stream {@link
   *       PricingConstants#REQUEST_STREAM_ID}
   *   <li>Create outbound ExclusivePublication on {@link PricingConstants#IPC_CHANNEL} stream
   *       {@link PricingConstants#RESPONSE_STREAM_ID}
   *   <li>Construct MarketDataAdapter (deterministic or synthetic based on config)
   *   <li>Construct ConfigurableForwardPointSource with default term structures
   *   <li>Construct SpreadConfig map, ClientTierConfig, VolatilityMonitor
   *   <li>Construct InventorySkewModel (convex or linear based on config)
   *   <li>Construct TieredSpreadModel
   *   <li>Construct QuoteManager
   *   <li>Construct PriceValidator with slippage registrations
   *   <li>Construct PricingResponseEncoder
   *   <li>Construct PricingMessageDispatcher
   *   <li>Construct PricingService agent
   *   <li>Create and start AgentRunner on a dedicated thread
   * </ol>
   *
   * @param aeronDir Aeron CnC directory for the external Media Driver; must not be blank
   * @param config pricing service configuration (adapter type, symbol definitions, tuning
   *     parameters); must not be null
   * @param idleStrategy idle strategy for the pricing agent runner duty cycle; must not be null
   * @return a {@link PricingComponents} handle that owns the runner thread and Aeron client
   * @throws NullPointerException if {@code config} or {@code idleStrategy} is null
   * @throws IllegalArgumentException if {@code aeronDir} is blank
   */
  public static PricingComponents launch(
      final String aeronDir, final PricingServiceConfig config, final IdleStrategy idleStrategy) {

    // --- Validate inputs ---
    requireNonBlank(aeronDir, "aeronDir");
    if (config == null) {
      throw new NullPointerException("config must not be null");
    }
    if (idleStrategy == null) {
      throw new NullPointerException("idleStrategy must not be null");
    }

    LOG.info()
        .append("Launching pricing service: aeronDir=")
        .append(aeronDir)
        .append(" adapterType=")
        .append(config.adapterType())
        .commit();

    // --- Step 1: Connect to shared Media Driver ---
    final Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));

    try {
      return launchWithAeron(aeron, config, idleStrategy, true);
    } catch (final RuntimeException e) {
      CloseHelper.closeAll(aeron);
      throw e;
    }
  }

  /**
   * Internal wiring method that constructs all components given an established Aeron client. Split
   * from {@link #launch} to enable embedding in a larger process that provides a shared Aeron
   * client.
   *
   * @param aeron connected Aeron client
   * @param config pricing service configuration
   * @param idleStrategy idle strategy for the agent runner
   * @param ownsAeron whether the returned PricingComponents should close the Aeron client
   * @return a fully wired PricingComponents
   */
  private static PricingComponents launchWithAeron(
      final Aeron aeron,
      final PricingServiceConfig config,
      final IdleStrategy idleStrategy,
      final boolean ownsAeron) {

    // --- Step 2: Create inbound subscription (cluster -> pricing service) ---
    final Subscription subscription =
        aeron.addSubscription(PricingConstants.IPC_CHANNEL, PricingConstants.REQUEST_STREAM_ID);

    // --- Step 3: Create outbound publication (pricing service -> cluster) ---
    final ExclusivePublication publication =
        aeron.addExclusivePublication(
            PricingConstants.IPC_CHANNEL, PricingConstants.RESPONSE_STREAM_ID);

    // --- Step 4: Construct MarketDataAdapter based on config ---
    final MidRateCache midRateCache = new MidRateCache();
    final MarketDataAdapter marketDataAdapter = createMarketDataAdapter(config, midRateCache);

    // --- Step 5: Construct ConfigurableForwardPointSource ---
    final ConfigurableForwardPointSource forwardPointSource =
        new ConfigurableForwardPointSource(DEFAULT_FORWARD_POINT_CAPACITY);
    registerForwardPoints(forwardPointSource);

    // --- Step 6: Construct SpreadConfig map, ClientTierConfig, VolatilityMonitor ---
    final Object2ObjectHashMap<ByteArrayKey, SpreadConfig> spreadConfigs =
        new Object2ObjectHashMap<>(SYMBOL_BYTES.length, 0.55f);
    final SpreadConfig defaultSpreadConfig = SpreadConfig.defaultConfig();
    registerSpreadConfigs(spreadConfigs, defaultSpreadConfig);

    final ClientTierConfig tierConfig = ClientTierConfig.defaultConfig();

    final VolatilityMonitor volatilityMonitor =
        new VolatilityMonitor(
            DEFAULT_VOL_WINDOW_SIZE, DEFAULT_VOL_WIDEN_THRESHOLD_BPS, DEFAULT_VOL_MAX_MULTIPLIER);
    registerVolatilitySymbols(volatilityMonitor);

    // --- Step 7: Construct InventorySkewModel ---
    final ConfigurablePositionSource positionSource = new ConfigurablePositionSource();
    final InventorySkewModel skewModel = createSkewModel(config, positionSource);

    // --- Step 8: Construct TieredSpreadModel ---
    final TieredSpreadModel spreadModel =
        new TieredSpreadModel(
            spreadConfigs, defaultSpreadConfig, tierConfig, skewModel, volatilityMonitor);

    // --- Step 9: Construct QuoteManager ---
    final QuoteManager quoteManager = new QuoteManager(config.maxActiveQuotes());

    // --- Step 10: Construct PriceValidator ---
    final PriceValidator priceValidator = new PriceValidator(quoteManager, marketDataAdapter);
    registerSlippageConfigs(priceValidator);

    // --- Step 11: Construct PricingResponseEncoder ---
    final PricingResponseEncoder responseEncoder = new PricingResponseEncoder();

    // --- Step 12-13: Construct PricingService agent ---
    // PricingService implements PricingMessageHandler and Agent. It creates the dispatcher
    // internally, wiring itself as the message handler.
    final EpochNanoClock epochClock = TradingClocks.epochNanoClock();
    final NanoClock nanoClock = TradingClocks.nanoClock();
    final PricingService pricingService =
        new PricingService(
            subscription,
            publication,
            marketDataAdapter,
            forwardPointSource,
            spreadModel,
            volatilityMonitor,
            quoteManager,
            priceValidator,
            responseEncoder,
            epochClock,
            nanoClock,
            config.staleThresholdNanos(),
            config.staleWidenThresholdNanos(),
            config.spotTtlNanos(),
            config.forwardTtlNanos(),
            config.swapTtlNanos(),
            config.sweepIntervalNanos());

    // --- Step 14a: Open stream-204 publish + stream-205 snapshot-request channels ---
    // Phase 3 market-data broadcast feed. The publisher runs on the same agent thread as
    // PricingService via CompositeAgent, satisfying the single-writer invariant without any
    // cross-thread channel. Stream IDs and channel string come from MarketDataConstants
    // (cross-module shared with websocket-server).
    final ExclusivePublication mdPublication =
        aeron.addExclusivePublication(MARKET_DATA_CHANNEL, MARKET_DATA_STREAM_ID);
    final Subscription mdSnapshotRequestSubscription =
        aeron.addSubscription(MARKET_DATA_CHANNEL, MARKET_DATA_SNAPSHOT_REQUEST_STREAM_ID);
    // Bind the real ExclusivePublication to the BroadcastPublisher seam via an anonymous inner
    // class. BroadcastPublisher is a 3-method interface (offer / position / termBufferLength —
    // the forensic context needed for MAX_POSITION_EXCEEDED logging precludes a single-method
    // SAM); the anonymous class is the standard JLS idiom for binding a multi-method interface
    // and is the documented deviation from CLAUDE.md's canonical SAM-publisher pattern (see
    // BroadcastPublisher's class-level Javadoc + docs/publishers.md). The instance is
    // constructed ONCE at launcher startup (cold path) and lives for the agent's lifetime, so
    // the allocation profile is equivalent to a method-reference SAM. The seam exists because
    // ExclusivePublication is final and cannot be subclassed by unit tests.
    final BroadcastPublisher broadcastPublisher =
        new BroadcastPublisher() {
          @Override
          public long offer(final DirectBuffer buffer, final int offset, final int length) {
            return mdPublication.offer(buffer, offset, length);
          }

          @Override
          public long position() {
            return mdPublication.position();
          }

          @Override
          public int termBufferLength() {
            return mdPublication.termBufferLength();
          }
        };
    final MarketDataPublisher marketDataPublisher =
        new MarketDataPublisher(
            broadcastPublisher,
            mdSnapshotRequestSubscription,
            epochClock,
            nanoClock,
            MarketDataPublisherConfig.defaults());
    final MidRateToTickBridge midRateBridge =
        new MidRateToTickBridge(midRateCache, marketDataPublisher, epochClock, SYMBOL_BYTES);

    // --- Step 14b: Create and start AgentRunner over the composite agent ---
    final ErrorHandler errorHandler =
        throwable ->
            LOG.error()
                .append("Pricing service error: ")
                .append(throwable.getClass().getName())
                .append(" - ")
                .append(throwable.getMessage())
                .commit();

    // Composite agent — PricingService (RFQ pricing + adapter doWork) + MidRateToTickBridge
    // (reads mid-rates, derives bid/ask, pushes to publisher) + MarketDataPublisher (drains
    // conflated slots onto stream 204; polls snapshot requests on stream 205). All three on
    // the same agent thread → no cross-thread channel, no fences, single-writer invariant
    // held end-to-end.
    final CompositeAgent compositeAgent =
        new CompositeAgent(pricingService, midRateBridge, marketDataPublisher);
    final AgentRunner agentRunner =
        new AgentRunner(idleStrategy, errorHandler, null, compositeAgent);

    try {
      AgentRunner.startOnThread(agentRunner);
    } catch (final RuntimeException e) {
      CloseHelper.closeAll(agentRunner);
      throw e;
    }

    LOG.info()
        .append("Pricing service launched: adapter=")
        .append(config.adapterType())
        .append(" symbols=")
        .append(SYMBOL_BYTES.length)
        .commit();

    return new PricingComponents(agentRunner, aeron, ownsAeron);
  }

  // ===========================================================================
  // Market data adapter construction
  // ===========================================================================

  /**
   * Creates the appropriate {@link MarketDataAdapter} based on the configured adapter type. Loads
   * base rates into the shared {@link MidRateCache} for both adapter types.
   *
   * @param config pricing service configuration providing the adapter type
   * @param cache shared mid-rate cache to pre-load with base rates
   * @return the constructed market data adapter
   */
  private static MarketDataAdapter createMarketDataAdapter(
      final PricingServiceConfig config, final MidRateCache cache) {

    final long nowNanos = TradingClocks.nanoClock().nanoTime();

    // Preload all symbols into the cache regardless of adapter type
    for (int i = 0; i < SYMBOL_BYTES.length; i++) {
      cache.preload(SYMBOL_BYTES[i], BASE_MID_RATES[i], nowNanos);
    }

    return switch (config.adapterType()) {
      case "deterministic" -> new DeterministicMarketDataAdapter(cache, TradingClocks.nanoClock());
      case "synthetic" -> {
        final XorShift128 prng = new XorShift128(config.prngSeed());
        final SyntheticMarketDataAdapter adapter =
            new SyntheticMarketDataAdapter(
                cache,
                TradingClocks.nanoClock(),
                prng,
                config.syntheticUpdateIntervalNanos(),
                config.syntheticVolatilityBps(),
                config.syntheticMeanReversionStrength(),
                SYMBOL_BYTES.length);
        for (int i = 0; i < SYMBOL_BYTES.length; i++) {
          adapter.registerSymbol(i, SYMBOL_BYTES[i], BASE_MID_RATES[i]);
        }
        yield adapter;
      }
      default ->
          throw new IllegalArgumentException(
              "Unknown adapter type: "
                  + config.adapterType()
                  + " (expected 'deterministic' or 'synthetic')");
    };
  }

  // ===========================================================================
  // Inventory skew model construction
  // ===========================================================================

  /**
   * Creates the appropriate {@link InventorySkewModel} based on configuration.
   *
   * @param config pricing service configuration providing the skew model type
   * @param positionSource position source for inventory lookups
   * @return the constructed inventory skew model
   */
  private static InventorySkewModel createSkewModel(
      final PricingServiceConfig config, final ConfigurablePositionSource positionSource) {

    return switch (config.skewModelType()) {
      case "convex" ->
          new ConvexInventorySkew(
              positionSource,
              DEFAULT_SKEW_MAX_BPS,
              DEFAULT_SKEW_POSITION_THRESHOLD,
              DEFAULT_ALPHA_X100);
      case "linear" ->
          new LinearInventorySkew(
              positionSource, DEFAULT_SKEW_MAX_BPS, DEFAULT_SKEW_POSITION_THRESHOLD);
      default ->
          throw new IllegalArgumentException(
              "Unknown skew model type: "
                  + config.skewModelType()
                  + " (expected 'convex' or 'linear')");
    };
  }

  // ===========================================================================
  // Symbol registration helpers (cold-path, startup only)
  // ===========================================================================

  /**
   * Registers forward point term structures for all hardcoded symbols. In the follow-up YAML config
   * task, these will be loaded from {@link PricingServiceConfig} instead.
   *
   * @param source the forward point source to register symbols on
   */
  private static void registerForwardPoints(final ConfigurableForwardPointSource source) {
    for (final byte[] symbol : SYMBOL_BYTES) {
      source.registerSymbol(symbol, DEFAULT_TENOR_DAYS, DEFAULT_FORWARD_POINTS);
    }
  }

  /**
   * Registers spread configurations for all hardcoded symbols. Uses the default spread config for
   * all symbols in v1; per-symbol overrides will come from YAML config.
   *
   * @param configs the spread config map to populate
   * @param defaultConfig the default config to use for all symbols
   */
  private static void registerSpreadConfigs(
      final Object2ObjectHashMap<ByteArrayKey, SpreadConfig> configs,
      final SpreadConfig defaultConfig) {
    for (final byte[] symbol : SYMBOL_BYTES) {
      final ByteArrayKey key = ByteArrayKey.copyOf(symbol, 0, symbol.length);
      configs.put(key, defaultConfig);
    }
  }

  /**
   * Registers all hardcoded symbols with the volatility monitor so ring buffers are pre-allocated.
   *
   * @param monitor the volatility monitor to register symbols on
   */
  private static void registerVolatilitySymbols(final VolatilityMonitor monitor) {
    final UnsafeBuffer symbolBuf = new UnsafeBuffer(new byte[0]);
    for (final byte[] symbol : SYMBOL_BYTES) {
      symbolBuf.wrap(symbol);
      monitor.registerSymbol(symbolBuf, 0, symbol.length);
    }
  }

  /**
   * Registers default slippage tolerances for all hardcoded symbols on the price validator.
   * Per-symbol overrides will come from YAML config in a follow-up task.
   *
   * @param validator the price validator to register slippage configs on
   */
  private static void registerSlippageConfigs(final PriceValidator validator) {
    // Default: 5 bps tolerance, 10 bps last-look for all symbols
    for (final byte[] symbol : SYMBOL_BYTES) {
      validator.registerSlippage(symbol, 5, 10);
    }
  }

  // ===========================================================================
  // Utility
  // ===========================================================================

  /**
   * Pads a symbol string to the 8-byte SBE Symbol type with NUL-padding (0x00). Symbols shorter
   * than 8 characters are right-padded with NUL bytes — the canonical convention across every
   * other Symbol-bearing site in the engine ({@link
   * com.trading.engine.projections.SymbolPacker#pack(String)}, the cluster's {@code OrderState},
   * the WebSocket client's subscribe encoder, the YAML-driven {@code SymbolEntitlementMap}).
   *
   * <p><b>Why this matters.</b> The 8 bytes are packed little-endian into a {@code long} that is
   * used as a hash-map key in {@link com.trading.engine.pricing.market.MarketDataPublisher}'s
   * conflation slots AND in {@link
   * com.trading.engine.websocket.SubscriptionFilter#matches(int, byte[], int, int)}'s
   * binary-search lookup. A SPACE-padded {@code "EURUSD  "} packs to a different {@code long}
   * than a NUL-padded {@code "EURUSD\0\0"}, which silently breaks subscription matching at the
   * wire: every market-data tick is rejected because the client subscribed under a NUL-padded
   * key but the publisher emitted under a SPACE-padded key. Prior to this fix, the full-stack
   * Playwright suite specs 02 / 04 / 05 / 07 / 09 failed for exactly this reason.
   *
   * @param symbol the symbol name (e.g., "EURUSD"); must be {@code <= 8} characters
   * @return 8-byte NUL-padded array suitable for SBE Symbol fields
   */
  private static byte[] padSymbol(final String symbol) {
    // Allocation defaults to NUL (0x00); no explicit Arrays.fill needed. Allocation-light by
    // design — every other Symbol-field site in the engine relies on the same default-zero
    // semantic.
    final byte[] padded = new byte[8];
    final byte[] src = symbol.getBytes(StandardCharsets.US_ASCII);
    System.arraycopy(src, 0, padded, 0, Math.min(src.length, 8));
    return padded;
  }

  /**
   * Validates that a string argument is non-null and non-blank.
   *
   * @param value the value to check
   * @param name the parameter name for error messages
   * @throws NullPointerException if value is null
   * @throws IllegalArgumentException if value is blank
   */
  private static void requireNonBlank(final String value, final String name) {
    if (value == null) {
      throw new NullPointerException(name + " must not be null");
    }
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
  }
}
