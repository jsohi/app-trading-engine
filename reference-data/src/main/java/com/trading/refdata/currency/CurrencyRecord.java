package com.trading.refdata.currency;

/**
 * Immutable currency record deserialized from YAML / CSV / RDBMS.
 *
 * <p>Field semantics mirror the SBE {@code LoadCurrencyBatch} message (templateId&nbsp;14):
 *
 * <ul>
 *   <li>{@code ccyCode} — ISO 4217 alphabetic code, 3 uppercase ASCII characters (tag&nbsp;15)
 *   <li>{@code isoNumeric} — ISO 4217 numeric code, 1–999 (tag&nbsp;10043)
 *   <li>{@code name} — human-readable currency name, max 64 characters (tag&nbsp;10026)
 *   <li>{@code decimals} — minor-unit precision, 0–18 (tag&nbsp;10044)
 *   <li>{@code currencyClass} — one of {@code Fiat}, {@code Metal}, {@code Crypto}, {@code Fund}
 *       (tag&nbsp;10045)
 *   <li>{@code status} — one of {@code Active}, {@code Suspended}, {@code Closed} (tag&nbsp;10027)
 * </ul>
 */
public record CurrencyRecord(
    String ccyCode,
    int isoNumeric,
    String name,
    int decimals,
    String currencyClass,
    String status) {

  /** Compact constructor — validates field constraints at construction time. */
  public CurrencyRecord {
    if (ccyCode == null || ccyCode.length() != 3) {
      throw new IllegalArgumentException(
          "ccyCode must be exactly 3 characters, got: '"
              + (ccyCode == null ? "null" : ccyCode)
              + "'");
    }
    for (int i = 0; i < 3; i++) {
      final char c = ccyCode.charAt(i);
      if (c < 'A' || c > 'Z') {
        throw new IllegalArgumentException(
            "ccyCode must be uppercase ASCII [A-Z], got: '" + ccyCode + "'");
      }
    }
    if (isoNumeric < 1 || isoNumeric > 999) {
      throw new IllegalArgumentException("isoNumeric must be in [1, 999], got " + isoNumeric);
    }
    if (name == null || name.isBlank()) {
      throw new IllegalArgumentException("name must not be blank");
    }
    if (decimals < 0 || decimals > 18) {
      throw new IllegalArgumentException("decimals must be in [0, 18], got " + decimals);
    }
    if (currencyClass == null || currencyClass.isBlank()) {
      throw new IllegalArgumentException("currencyClass must not be blank");
    }
    if (status == null || status.isBlank()) {
      throw new IllegalArgumentException("status must not be blank");
    }
  }
}
