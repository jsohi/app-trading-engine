package com.trading.refdata.eligibility;

import java.util.Objects;

/**
 * Immutable symbol-eligibility record deserialized from YAML / CSV / RDBMS.
 *
 * <p>Field semantics mirror the SBE {@code LoadSymbolEligibility} message (templateId&nbsp;19,
 * APP-62&nbsp;§G) and the batch form {@code LoadSymbolEligibilityBatch} (templateId&nbsp;20):
 *
 * <ul>
 *   <li>{@code symbol} — instrument symbol (FIX tag&nbsp;55). Must be non-blank, ASCII, and at most
 *       {@value #MAX_SYMBOL_LENGTH} characters (matches the SBE {@code Symbol} fixed-length type).
 *   <li>{@code tradingAllowed} — when {@code false}, every order on this symbol is rejected with
 *       {@code RegulatoryRestriction} (tag&nbsp;10070).
 *   <li>{@code shortSaleAllowed} — when {@code false}, sell orders (treated as potentially-short in
 *       Phase-1&nbsp;§G) are rejected with {@code RegulatoryRestriction} (Reg&nbsp;SHO threshold
 *       security, halt-and-locate violation, hard-to-borrow set). (tag&nbsp;10071)
 *   <li>{@code priceDeviationBpsOverride} — APP-62&nbsp;§I per-symbol fat-finger override in basis
 *       points. {@code 0} means no override — fall back to the per-account knob carried on {@code
 *       RiskLimit} (id=10103). Must be &ge; 0. (tag&nbsp;10124)
 *   <li>{@code asOfTimestamp} — POJO-level "as-of" marker. The YAML loader emits {@code 0L}
 *       (sentinel meaning "stamped at cluster ingest") per CLAUDE.md Rule 9 (no direct wall-clock
 *       outside the cluster). The encoder hardcodes the SBE batch envelope {@code transactTime} to
 *       {@code 0L} for the same reason, and the cluster restamps both {@code
 *       SymbolEligibilityState.asOfTimestamp} and the emitted event's wire {@code transactTime}
 *       with its deterministic timestamp on ingest.
 * </ul>
 *
 * <p>APP-62&nbsp;§G is fail-closed: any order whose symbol has no eligibility record loaded gets
 * rejected with {@code RegulatoryRestriction} before any other validation can fire. The ops
 * playbook for start-of-day onboarding therefore requires the full restricted-symbol cohort be
 * present in the eligibility YAML before the gateway accepts orders.
 *
 * @see com.trading.refdata.risklimit.RiskLimitRecord — peer record carrying the per-account
 *     fat-finger knob this record can override per symbol.
 */
public record SymbolEligibilityRecord(
    String symbol,
    boolean tradingAllowed,
    boolean shortSaleAllowed,
    long priceDeviationBpsOverride,
    long asOfTimestamp) {

  /**
   * Maximum length of the {@code symbol} string in characters. Matches the SBE {@code Symbol}
   * fixed-length type ({@code length="8"}). Longer values would silently truncate on encode, which
   * we instead reject at construction time.
   */
  public static final int MAX_SYMBOL_LENGTH = 8;

  /**
   * Upper bound for {@code priceDeviationBpsOverride}. Matches the SBE schema field type {@code
   * uint32} (max representable value {@code 4_294_967_294} — the SBE convention reserves {@code
   * 4_294_967_295} as the {@code nullValue}). Values above this would silently truncate to the low
   * 32 bits on encode, corrupting the per-symbol fat-finger override that gates order acceptance.
   * Rejected at the POJO boundary so a misconfigured YAML entry fails fast at load.
   */
  public static final long MAX_PRICE_DEVIATION_BPS_OVERRIDE = 4_294_967_294L;

  /** Compact constructor — validates schema constraints. */
  public SymbolEligibilityRecord {
    Objects.requireNonNull(symbol, "symbol");
    if (symbol.isBlank()) {
      throw new IllegalArgumentException("symbol must not be blank");
    }
    // SBE Symbol field is 8 ASCII bytes. Reject longer strings at the boundary rather than
    // silently truncating on encode (which would corrupt the symbol key used by the §G check).
    //
    // Gemini R3: validate ASCII codepoints EXPLICITLY rather than via getBytes(US_ASCII). The
    // getBytes path silently replaces non-ASCII codepoints with '?' (0x3F) before measuring the
    // byte length — so e.g. "EUR€USD" would have come through as 8 bytes (well under the limit)
    // and packed into the symbol hash with a fabricated '?' byte, drifting from the operator's
    // configured symbol. Range-check every char against [0x20, 0x7E] (printable ASCII) and reject
    // anything else. Symbol length is the char count, which equals the ASCII byte count when
    // every char is in the printable range.
    if (symbol.length() > MAX_SYMBOL_LENGTH) {
      throw new IllegalArgumentException(
          "symbol must be <= " + MAX_SYMBOL_LENGTH + " ASCII chars, got " + symbol.length());
    }
    for (int i = 0; i < symbol.length(); i++) {
      char c = symbol.charAt(i);
      if (c < 0x20 || c > 0x7E) {
        throw new IllegalArgumentException(
            "symbol must contain only printable ASCII (0x20-0x7E); offending codepoint at index "
                + i
                + ": 0x"
                + Integer.toHexString(c));
      }
    }
    if (priceDeviationBpsOverride < 0) {
      throw new IllegalArgumentException(
          "priceDeviationBpsOverride must be >= 0, got " + priceDeviationBpsOverride);
    }
    if (priceDeviationBpsOverride > MAX_PRICE_DEVIATION_BPS_OVERRIDE) {
      // Guard against silent high-bit truncation when encoded into the SBE uint32 field.
      throw new IllegalArgumentException(
          "priceDeviationBpsOverride must be <= uint32 max ("
              + MAX_PRICE_DEVIATION_BPS_OVERRIDE
              + "), got "
              + priceDeviationBpsOverride);
    }
  }
}
