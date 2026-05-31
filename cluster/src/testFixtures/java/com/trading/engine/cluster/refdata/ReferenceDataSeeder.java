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

  /**
   * Same as {@link #seed(AccountStore, CurrencyStore, RiskLimitStore)} plus the §G permissive
   * symbol-eligibility seed. Most tests should call this overload — Check 11g is fail-closed and
   * rejects every order whose symbol has no eligibility record loaded.
   *
   * @param accounts the account store to populate (must be non-null)
   * @param currencies the currency store to populate (must be non-null)
   * @param limits the risk-limit store to populate (must be non-null)
   * @param eligibilities the symbol-eligibility store to populate (must be non-null)
   */
  public static void seed(
      final AccountStore accounts,
      final CurrencyStore currencies,
      final RiskLimitStore limits,
      final SymbolEligibilityStore eligibilities) {
    seedAccounts(accounts);
    seedCurrencies(currencies);
    seedRiskLimits(limits);
    seedSymbolEligibility(eligibilities);
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

  /**
   * Seed the symbol-eligibility store with permissive records for the symbols every existing test
   * uses. APP-62 §G Check 11g is fail-closed: any order whose symbol has no record loaded is
   * rejected with {@code RegulatoryRestriction}. Tests that do NOT explicitly exercise §G must
   * therefore pre-seed every symbol they reference, otherwise they will fail at Check 11g before
   * reaching their actual assertions.
   *
   * <p>Seeded symbols (verified against existing test corpora — see {@code
   * NewOrderSingleHandler*Test}, {@code TradingClusteredServiceTest}, {@code Rfq*IT}):
   *
   * <ul>
   *   <li>{@code EURUSD} — primary spot test symbol
   *   <li>{@code GBPUSD} — secondary symbol used by alloc and dedup tests
   *   <li>{@code USDJPY} — used by snapshot round-trip tests
   *   <li>{@code AAA}, {@code BBB}, {@code SYM} — short synthetic symbols used by minimal tests
   * </ul>
   *
   * <p>All seeded records have {@code tradingAllowed=true} and {@code shortSaleAllowed=true} —
   * permissive defaults so existing assertions reach the gate they were written to exercise. Tests
   * that specifically validate §G semantics override these with restricted records.
   *
   * @param eligibilities the symbol-eligibility store to populate (must be non-null)
   */
  /**
   * Convenience factory: returns a {@link SymbolEligibilityStore} pre-seeded with the §G permissive
   * defaults for every symbol existing tests reference. Equivalent to {@code SymbolEligibilityStore
   * s = new SymbolEligibilityStore(); seedSymbolEligibility(s);}.
   *
   * <p>Use at every NOS-test construction site that previously instantiated {@code new
   * SymbolEligibilityStore()} — the empty store would cause Check 11g to fail-close on every order.
   * Tests that specifically validate §G fail-closed behaviour may continue to use the bare {@code
   * new SymbolEligibilityStore()} call to exercise that path.
   *
   * @return a freshly-constructed store containing the standard set of permissive symbol records
   */
  public static SymbolEligibilityStore permissiveSymbolEligibilityStore() {
    final var store = new SymbolEligibilityStore();
    seedSymbolEligibility(store);
    return store;
  }

  public static void seedSymbolEligibility(final SymbolEligibilityStore eligibilities) {
    putPermissive(eligibilities, "EURUSD");
    putPermissive(eligibilities, "GBPUSD");
    putPermissive(eligibilities, "USDJPY");
    putPermissive(eligibilities, "AAA");
    putPermissive(eligibilities, "BBB");
    putPermissive(eligibilities, "SYM");
    putPermissive(eligibilities, "TEST");
    putPermissive(eligibilities, "FOO");
  }

  /**
   * NUL-pads the given symbol string to the SBE {@code Symbol} char[8] wire shape, packs it via
   * {@link SymbolEligibilityState#packSymbolKey}, and puts a permissive record into the store.
   * NUL-padding (rather than space-padding) matches the cluster's actual decode path — the SBE
   * encoders zero-fill the trailing bytes of a fixed-length string field — so the seeded hash is
   * byte-identical to what {@code NewOrderSingleHandler.packSymbolKey} computes at Check 11g.
   */
  private static void putPermissive(final SymbolEligibilityStore store, final String symbol) {
    final var state = new SymbolEligibilityState();
    final byte[] symBytes = new byte[8];
    for (int i = 0; i < 8 && i < symbol.length(); i++) {
      symBytes[i] = (byte) symbol.charAt(i);
    }
    state.setSymbolBytes(symBytes, 0, symBytes.length);
    state.setSymbolHash(SymbolEligibilityState.packSymbolKey(symBytes, 0));
    state.setTradingAllowed(true);
    state.setShortSaleAllowed(true);
    state.setPriceDeviationBpsOverride(0L);
    state.setAsOfTimestamp(0L);
    store.put(state);
  }
}
