package com.trading.engine.cluster.refdata;

import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.AccountTypeEnum;
import com.trading.engine.messages.sbe.AcctIDSourceEnum;
import com.trading.engine.messages.sbe.ComplianceStatusEnum;

/**
 * Factory methods for {@link AccountState} test instances. Provides convenient overloads ranging
 * from fully specified accounts to sensible-default single-line constructors for the common case.
 *
 * <p>All methods replicate the setter sequence established by the production {@link AccountStore}
 * restore path and the existing inline helpers in {@code AccountStoreTest} and {@code
 * TradingClusteredServiceTest}, ensuring that test instances are indistinguishable from
 * cluster-restored state.
 *
 * <p><b>Threading.</b> Stateless utility class — safe for unrestricted concurrent use. Each call
 * allocates a fresh {@link AccountState} instance; callers own the returned reference.
 *
 * <p><b>Allocation.</b> One {@link AccountState} allocation per call plus transient {@code byte[]}
 * arrays for string-to-byte conversion. Acceptable for test setup; not intended for hot-path use.
 */
public final class AccountFixtures {

  /** Default capabilities mask: trade and RFQ enabled. */
  private static final long DEFAULT_CAPABILITIES =
      AccountState.Capabilities.CAN_TRADE | AccountState.Capabilities.CAN_RFQ;

  private AccountFixtures() {}

  // ---------------------------------------------------------------------------
  // Full factory
  // ---------------------------------------------------------------------------

  /**
   * Create a fully specified {@link AccountState}. Every field on the state is set explicitly; no
   * defaults are assumed beyond the fixed {@link AcctIDSourceEnum#Internal} id source, {@link
   * AccountTypeEnum#Client} account type, and {@link ComplianceStatusEnum#OK} compliance status
   * (matching the existing production loader pattern).
   *
   * @param id numeric account identifier (FIX tag 1 surrogate)
   * @param code short alpha account code, e.g. {@code "ACME"} (max 16 ASCII bytes)
   * @param name human-readable display name (max 64 ASCII bytes)
   * @param baseCcy ISO 4217 alpha-3 base currency, e.g. {@code "USD"} (exactly 3 ASCII bytes)
   * @param status account lifecycle status
   * @param capabilities bitfield of {@link AccountState.Capabilities} flags
   * @return a new {@link AccountState} ready for insertion into an {@link AccountStore}
   */
  public static AccountState account(
      final long id,
      final String code,
      final String name,
      final String baseCcy,
      final AccountStatusEnum status,
      final long capabilities) {
    final AccountState s = new AccountState();
    s.setAccountId(id);
    s.setParentAccountId(0L);
    final byte[] codeBytes = code.getBytes();
    s.setAccountCode(codeBytes, 0, codeBytes.length);
    s.setAcctIdSource(AcctIDSourceEnum.Internal);
    final byte[] nameBytes = name.getBytes();
    s.setAccountName(nameBytes, 0, nameBytes.length);
    s.setAccountType(AccountTypeEnum.Client);
    final byte[] ccy = baseCcy.getBytes();
    s.setBaseCurrency(ccy[0], ccy[1], ccy[2]);
    s.setStatus(status);
    s.setComplianceStatus(ComplianceStatusEnum.OK);
    s.setCapabilities(capabilities);
    s.setTransactTime(0L);
    return s;
  }

  // ---------------------------------------------------------------------------
  // Convenience overloads
  // ---------------------------------------------------------------------------

  /**
   * Create an {@link AccountStatusEnum#Active} account with default capabilities ({@link
   * AccountState.Capabilities#CAN_TRADE} | {@link AccountState.Capabilities#CAN_RFQ}).
   *
   * <p>Matches the inline {@code makeState} helper from {@code AccountStoreTest}.
   *
   * <p><b>Note:</b> This overload defaults to {@code CAN_TRADE | CAN_RFQ}, which does NOT match the
   * canonical ACME account from {@link ReferenceDataSeeder} (which has {@code CAN_TRADE} only). Use
   * the full factory or {@link ReferenceDataSeeder#seed} when replicating the standard test
   * dataset.
   *
   * @param id numeric account identifier
   * @param code short alpha account code (max 16 ASCII bytes)
   * @param name human-readable display name (max 64 ASCII bytes)
   * @param baseCcy ISO 4217 alpha-3 base currency (exactly 3 ASCII bytes)
   * @return a new active, fully capable {@link AccountState}
   */
  public static AccountState account(
      final long id, final String code, final String name, final String baseCcy) {
    return account(id, code, name, baseCcy, AccountStatusEnum.Active, DEFAULT_CAPABILITIES);
  }

  /**
   * Create an account with auto-generated name ({@code "Account " + code}) and USD base currency.
   *
   * <p>Matches the inline {@code makeAccount} helper from {@code TradingClusteredServiceTest}.
   *
   * @param id numeric account identifier
   * @param code short alpha account code (max 16 ASCII bytes)
   * @param status account lifecycle status
   * @param capabilities bitfield of {@link AccountState.Capabilities} flags
   * @return a new {@link AccountState} with auto-derived name and USD base currency
   */
  public static AccountState account(
      final long id, final String code, final AccountStatusEnum status, final long capabilities) {
    return account(id, code, "Account " + code, "USD", status, capabilities);
  }

  /**
   * Create a default test account: id=1, code={@code "ACC-001"}, name={@code "Test Account 1"},
   * base currency USD, {@link AccountStatusEnum#Active}, {@link
   * AccountState.Capabilities#CAN_TRADE} | {@link AccountState.Capabilities#CAN_RFQ}.
   *
   * @return a new default {@link AccountState}
   */
  public static AccountState defaultAccount() {
    return account(1L, "ACC-001", "Test Account 1", "USD");
  }
}
