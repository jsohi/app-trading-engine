package com.trading.engine.cluster;

import com.trading.engine.cluster.journal.EventJournal;
import com.trading.engine.cluster.refdata.AccountStore;
import com.trading.engine.cluster.refdata.CurrencyStore;
import com.trading.engine.cluster.refdata.LoadAccountHandler;
import com.trading.engine.cluster.refdata.LoadCurrencyHandler;
import com.trading.engine.cluster.refdata.LoadRiskLimitHandler;
import com.trading.engine.cluster.refdata.ReferenceDataRegistry;
import com.trading.engine.cluster.refdata.RiskLimitStore;
import com.trading.engine.cluster.sequencer.EventSequencer;

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
    final IdGenerator orderIdGen = new IdGenerator("ORD");
    final IdGenerator execIdGen = new IdGenerator("EXE");
    final OrderBook orderBook = new OrderBook(128);
    final EventSequencer eventSequencer = new EventSequencer();
    final EventJournal eventJournal = new EventJournal(64);

    final ReferenceDataRegistry registry = new ReferenceDataRegistry();
    registry.registerStore(accountStore);
    registry.registerStore(currencyStore);
    registry.registerStore(riskLimitStore);
    registry.registerLoader(new LoadAccountHandler(accountStore, currencyStore));
    registry.registerLoader(new LoadCurrencyHandler(currencyStore));
    registry.registerLoader(new LoadRiskLimitHandler(riskLimitStore, accountStore));

    return new TradingClusteredService(
        orderIdGen,
        execIdGen,
        orderBook,
        eventSequencer,
        eventJournal,
        accountStore,
        currencyStore,
        riskLimitStore,
        registry);
  }
}
