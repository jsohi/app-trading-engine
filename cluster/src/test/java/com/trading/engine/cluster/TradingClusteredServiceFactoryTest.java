package com.trading.engine.cluster;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.trading.engine.cluster.refdata.AccountStore;
import com.trading.engine.cluster.refdata.CurrencyStore;
import com.trading.engine.cluster.refdata.RiskLimitStore;
import org.junit.jupiter.api.Test;

/**
 * Smoke test for {@link TradingClusteredServiceFactory}. The real assertion of correctness lives
 * inside {@link TradingClusteredService}'s constructor: {@code requireSameStore} throws {@link
 * IllegalArgumentException} if the registry's store references drift from the direct ones. So "the
 * factory returns a non-null service" is equivalent to "the factory's registry wiring is internally
 * consistent".
 */
class TradingClusteredServiceFactoryTest {

  @Test
  void createWithFreshStoresSucceeds() {
    // assertDoesNotThrow is what we actually care about: the factory itself cannot return null
    // by construction, so the meaningful assertion is that requireSameStore (inside the
    // TradingClusteredService constructor) does not throw.
    assertDoesNotThrow(() -> TradingClusteredServiceFactory.create());
  }

  @Test
  void createWithExternalStoresSucceeds() {
    assertDoesNotThrow(
        () ->
            TradingClusteredServiceFactory.create(
                new AccountStore(), new CurrencyStore(), new RiskLimitStore()));
  }
}
