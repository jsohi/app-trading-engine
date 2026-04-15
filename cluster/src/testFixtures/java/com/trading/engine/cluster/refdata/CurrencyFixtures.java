package com.trading.engine.cluster.refdata;

import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.CurrencyClassEnum;
import java.nio.charset.StandardCharsets;

/**
 * Factory methods for {@link CurrencyState} test instances. Provides overloads ranging from fully
 * specified currencies to one-liner helpers for the most common ISO 4217 codes (USD, EUR).
 *
 * <p>All methods replicate the setter sequence established by the production {@link CurrencyStore}
 * restore path and the existing inline {@code makeCurrency} helper in {@code
 * TradingClusteredServiceTest}, ensuring that test instances are indistinguishable from
 * cluster-restored state.
 *
 * <p><b>Threading.</b> Stateless utility class — safe for unrestricted concurrent use. Each call
 * allocates a fresh {@link CurrencyState} instance; callers own the returned reference.
 *
 * <p><b>Allocation.</b> One {@link CurrencyState} allocation per call plus transient {@code byte[]}
 * arrays for string-to-byte conversion. Acceptable for test setup; not intended for hot-path use.
 */
public final class CurrencyFixtures {

  private CurrencyFixtures() {}

  // ---------------------------------------------------------------------------
  // Full factory
  // ---------------------------------------------------------------------------

  /**
   * Create a fully specified {@link CurrencyState}. Every field on the state is set explicitly.
   *
   * @param code ISO 4217 alpha-3 code, e.g. {@code "USD"} (exactly 3 uppercase ASCII bytes)
   * @param isoNumeric ISO 4217 numeric code (e.g. 840 for USD, 978 for EUR)
   * @param name human-readable display name (max 64 ASCII bytes)
   * @param decimals minor unit decimal places (USD=2, JPY=0, KWD=3)
   * @param cls currency classification (Fiat, Metal, Crypto, Fund)
   * @param status lifecycle status
   * @return a new {@link CurrencyState} ready for insertion into a {@link CurrencyStore}
   */
  public static CurrencyState currency(
      final String code,
      final int isoNumeric,
      final String name,
      final int decimals,
      final CurrencyClassEnum cls,
      final AccountStatusEnum status) {
    final CurrencyState c = new CurrencyState();
    final byte[] codeBytes = code.getBytes(StandardCharsets.US_ASCII);
    c.setCcyCode(codeBytes, 0);
    c.setIsoNumeric(isoNumeric);
    final byte[] nameBytes = name.getBytes(StandardCharsets.US_ASCII);
    c.setName(nameBytes, 0, nameBytes.length);
    c.setDecimals(decimals);
    c.setCurrencyClass(cls);
    c.setStatus(status);
    c.setTransactTime(0L);
    return c;
  }

  // ---------------------------------------------------------------------------
  // Convenience overloads
  // ---------------------------------------------------------------------------

  /**
   * Create a {@link CurrencyClassEnum#Fiat}, {@link AccountStatusEnum#Active} currency with 2
   * decimal places and auto-generated name ({@code "Currency " + code}).
   *
   * <p>Matches the inline {@code makeCurrency} helper from {@code TradingClusteredServiceTest}.
   *
   * @param code ISO 4217 alpha-3 code (exactly 3 uppercase ASCII bytes)
   * @param isoNumeric ISO 4217 numeric code
   * @return a new active Fiat {@link CurrencyState}
   */
  public static CurrencyState currency(final String code, final int isoNumeric) {
    return currency(
        code, isoNumeric, "Currency " + code, 2, CurrencyClassEnum.Fiat, AccountStatusEnum.Active);
  }

  /**
   * Create a USD (United States Dollar) currency state: ISO alpha {@code "USD"}, numeric 840, 2
   * decimal places, Fiat, Active.
   *
   * @return a new USD {@link CurrencyState}
   */
  public static CurrencyState usd() {
    return currency("USD", 840);
  }

  /**
   * Create a EUR (Euro) currency state: ISO alpha {@code "EUR"}, numeric 978, 2 decimal places,
   * Fiat, Active.
   *
   * @return a new EUR {@link CurrencyState}
   */
  public static CurrencyState eur() {
    return currency("EUR", 978);
  }
}
