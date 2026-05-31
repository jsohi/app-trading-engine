package com.trading.engine.cluster.refdata;

import com.trading.engine.messages.sbe.AccountStatusEnum;

/**
 * Seeds {@link AccountStore}, {@link CurrencyStore}, and {@link RiskLimitStore} with the standard
 * reference-data dataset used across all cluster integration and unit tests. The dataset is an
 * exact replica of the inline {@code seedReferenceData()} method from {@code
 * TradingClusteredServiceTest}:
 *
 * <ul>
 *   <li><b>Account 1 (ACME)</b> — Active, CAN_TRADE
 *   <li><b>Account 2 (LOCKED)</b> — Suspended, CAN_TRADE
 *   <li><b>Account 3 (QUOTEONLY)</b> — Active, no capabilities (0)
 *   <li><b>Currency USD</b> — ISO 840, Fiat, Active, 2 decimals
 *   <li><b>Currency EUR</b> — ISO 978, Fiat, Active, 2 decimals
 *   <li><b>Risk limit (account 1)</b> — maxOrderSize 10 units, all other gates unlimited
 * </ul>
 *
 * <p>Extracting this into a shared test fixture eliminates copy-paste duplication across test
 * classes while preserving exact data fidelity with the canonical test dataset.
 *
 * <p><b>Threading.</b> Stateless utility class — safe for unrestricted concurrent use. The stores
 * passed to the seed methods must not be concurrently accessed by another thread during seeding
 * (they are single-threaded by design).
 *
 * <p><b>Allocation.</b> Allocates one state object per record plus transient {@code byte[]} arrays
 * for string-to-byte conversion. Acceptable for test setup; not intended for hot-path use.
 */
public final class ReferenceDataSeeder {

  private ReferenceDataSeeder() {}

  // ---------------------------------------------------------------------------
  // Full seed
  // ---------------------------------------------------------------------------

  /**
   * Seed all three stores with the standard test dataset: 3 accounts, 2 currencies, and 1 risk
   * limit.
   *
   * @param accounts the account store to populate (must be non-null)
   * @param currencies the currency store to populate (must be non-null)
   * @param limits the risk-limit store to populate (must be non-null)
   */
  public static void seed(
      final AccountStore accounts, final CurrencyStore currencies, final RiskLimitStore limits) {
    seedAccounts(accounts);
    seedCurrencies(currencies);
    seedRiskLimits(limits);
  }

  // ---------------------------------------------------------------------------
  // Per-store seeders
  // ---------------------------------------------------------------------------

  /**
   * Seed the account store with the standard 3-account test dataset.
   *
   * <ul>
   *   <li>Account 1 ({@code "ACME"}) — Active, CAN_TRADE
   *   <li>Account 2 ({@code "LOCKED"}) — Suspended, CAN_TRADE
   *   <li>Account 3 ({@code "QUOTEONLY"}) — Active, capabilities = 0 (no trading, no RFQ)
   * </ul>
   *
   * @param accounts the account store to populate (must be non-null)
   */
  public static void seedAccounts(final AccountStore accounts) {
    accounts.put(
        AccountFixtures.account(
            1L, "ACME", AccountStatusEnum.Active, AccountState.Capabilities.CAN_TRADE));
    accounts.put(
        AccountFixtures.account(
            2L, "LOCKED", AccountStatusEnum.Suspended, AccountState.Capabilities.CAN_TRADE));
    accounts.put(AccountFixtures.account(3L, "QUOTEONLY", AccountStatusEnum.Active, 0L));
  }

  /**
   * Seed the currency store with the standard 2-currency test dataset: USD (840) and EUR (978).
   *
   * @param currencies the currency store to populate (must be non-null)
   */
  public static void seedCurrencies(final CurrencyStore currencies) {
    currencies.put(
        CurrencyStore.packCode((byte) 'U', (byte) 'S', (byte) 'D'), CurrencyFixtures.usd());
    currencies.put(
        CurrencyStore.packCode((byte) 'E', (byte) 'U', (byte) 'R'), CurrencyFixtures.eur());
  }

  /**
   * Seed the risk-limit store with the standard 1-limit test dataset: a permissive limit for
   * account 1 (max order size 10 units, all other gates unlimited).
   *
   * @param limits the risk-limit store to populate (must be non-null)
   */
  public static void seedRiskLimits(final RiskLimitStore limits) {
    // APP-62 §E (Check 0a) — fail-closed boot rejects any order whose account has no loaded
    // RiskLimitRecord. The downstream tests for AccountSuspended (account 2 LOCKED) and
    // AccountNoTradePermission (account 3 QUOTEONLY) must reach those checks, so we seed
    // permissive risk limits for all three test accounts. Without these, Check 0a would
    // reject before the status / permission checks ever fire and those tests would fail
    // with RiskLimitsNotLoaded instead of the expected reason.
    limits.put(RiskLimitFixtures.permissive(1L));
    limits.put(RiskLimitFixtures.permissive(2L));
    limits.put(RiskLimitFixtures.permissive(3L));
  }
}
