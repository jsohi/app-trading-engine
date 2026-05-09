package com.trading.engine.cluster;

import com.trading.engine.cluster.handler.EventSink;
import com.trading.engine.cluster.journal.EventJournal;
import com.trading.engine.cluster.metrics.RfqMetrics;
import com.trading.engine.cluster.refdata.AccountStore;
import com.trading.engine.cluster.refdata.CurrencyStore;
import com.trading.engine.cluster.refdata.LoadAccountBatchHandler;
import com.trading.engine.cluster.refdata.LoadAccountHandler;
import com.trading.engine.cluster.refdata.LoadCurrencyBatchHandler;
import com.trading.engine.cluster.refdata.LoadCurrencyHandler;
import com.trading.engine.cluster.refdata.LoadRiskLimitBatchHandler;
import com.trading.engine.cluster.refdata.LoadRiskLimitHandler;
import com.trading.engine.cluster.refdata.ReferenceDataRegistry;
import com.trading.engine.cluster.refdata.RiskLimitStore;
import com.trading.engine.cluster.sequencer.EventSequencer;
import com.trading.engine.cluster.state.RfqStateMachine;
import com.trading.engine.cluster.state.TradingState;

/**
 * Canonical wiring for {@link TradingClusteredService}. Both the launcher (production bootstrap)
 * and {@code TradingClusteredServiceTest} go through this factory so the registry + store object
 * graph cannot drift between test and production — {@link TradingClusteredService}'s constructor
 * enforces a {@code requireSameStore} consistency check that would fire on any divergence.
 *
 * <p>The factory accepts externally-constructed ref-data stores so tests can seed them before
 * passing them in. The zero-arg overload constructs fresh empty stores for production bootstrap.
 */
public final class TradingClusteredServiceFactory {

  // ---- Default APP-232 RFQ configuration values (overrideable via LauncherConfig). ----

  /** Default RFQ slot pool capacity. Power-of-two; covers 30s TTL × ~273 RFQs/sec peak. */
  public static final int DEFAULT_RFQ_POOL_CAPACITY = 8192;

  /** Default RFQ TTL (30s) — fallback when productType is unknown. */
  public static final long DEFAULT_RFQ_TTL_NANOS = 30_000_000_000L;

  /** Default request-timeout (5s) — REQUESTED slot timeout if no PriceResponse arrives. */
  public static final long DEFAULT_RFQ_REQUEST_TIMEOUT_NANOS = 5_000_000_000L;

  /** Default per-session rate limit (100 RFQs/sec). */
  public static final long DEFAULT_RFQ_RATE_LIMIT_PER_SESSION = 100L;

  /** Default rate-limit window (1s). */
  public static final long DEFAULT_RFQ_RATE_LIMIT_WINDOW_NANOS = 1_000_000_000L;

  /** Default tolerance for NOS-with-quoteId acceptance (bps). 0 = exact match. */
  public static final int DEFAULT_RFQ_ACCEPT_PRICE_TOLERANCE_BPS = 0;

  /** Default tolerance for NOS-with-quoteId qty (bps). 0 = exact match. */
  public static final int DEFAULT_RFQ_ACCEPT_QTY_TOLERANCE_BPS = 0;

  private TradingClusteredServiceFactory() {}

  /** Convenience for production bootstrap: fresh empty ref-data stores. */
  public static TradingClusteredService create() {
    return create(new AccountStore(), new CurrencyStore(), new RiskLimitStore());
  }

  /**
   * Build a {@link TradingClusteredService} wired with the given ref-data stores. The factory
   * constructs the remaining dependencies ({@link IdGenerator}s, {@link OrderBook}, {@link
   * EventSequencer}, {@link EventJournal}, {@link ReferenceDataRegistry} with all three loaders)
   * internally.
   */
  public static TradingClusteredService create(
      final AccountStore accountStore,
      final CurrencyStore currencyStore,
      final RiskLimitStore riskLimitStore) {
    final var orderIdGen = new IdGenerator("ORD");
    final var execIdGen = new IdGenerator("EXE");
    final var quoteIdGen = new IdGenerator("QTE");
    // Default capacities (~65k each) for production bootstrap. The existing unit test in
    // TradingClusteredServiceTest uses the int-capacity constructors (128 / 64) to keep its
    // scratch buffers small — a deliberate divergence: the factory is for production, the test
    // is for assertion surface, and both go through the same store-wiring path so the
    // requireSameStore consistency check is unaffected.
    final var orderBook = new OrderBook();
    final var eventSequencer = new EventSequencer();
    final var eventJournal = new EventJournal();
    final var tradingState = new TradingState(orderBook, orderIdGen, execIdGen, quoteIdGen);
    final var eventSink = new EventSink(eventSequencer, eventJournal);

    final var registry = new ReferenceDataRegistry();
    registry.registerStore(accountStore);
    registry.registerStore(currencyStore);
    registry.registerStore(riskLimitStore);
    // Legacy single-record loaders (templateIds 11, 13, 15)
    registry.registerLoader(new LoadAccountHandler(accountStore, currencyStore));
    registry.registerLoader(new LoadCurrencyHandler(currencyStore));
    registry.registerLoader(new LoadRiskLimitHandler(riskLimitStore, accountStore));
    // Batch loaders (templateIds 12, 14, 16) — used by ReferenceDataOrchestrator via YAML files
    registry.registerBatchLoader(new LoadAccountBatchHandler(accountStore, currencyStore));
    registry.registerBatchLoader(new LoadCurrencyBatchHandler(currencyStore));
    registry.registerBatchLoader(new LoadRiskLimitBatchHandler(riskLimitStore, accountStore));

    final var rfqMetrics = new RfqMetrics();
    final var rfqStateMachine =
        new RfqStateMachine(
            DEFAULT_RFQ_POOL_CAPACITY,
            DEFAULT_RFQ_TTL_NANOS,
            DEFAULT_RFQ_TTL_NANOS,
            DEFAULT_RFQ_TTL_NANOS,
            DEFAULT_RFQ_TTL_NANOS,
            DEFAULT_RFQ_REQUEST_TIMEOUT_NANOS,
            DEFAULT_RFQ_RATE_LIMIT_PER_SESSION,
            DEFAULT_RFQ_RATE_LIMIT_WINDOW_NANOS,
            DEFAULT_RFQ_ACCEPT_PRICE_TOLERANCE_BPS,
            DEFAULT_RFQ_ACCEPT_QTY_TOLERANCE_BPS,
            accountStore,
            rfqMetrics);

    return new TradingClusteredService(
        tradingState,
        eventSink,
        eventJournal,
        accountStore,
        currencyStore,
        riskLimitStore,
        registry,
        rfqStateMachine,
        rfqMetrics);
  }
}
