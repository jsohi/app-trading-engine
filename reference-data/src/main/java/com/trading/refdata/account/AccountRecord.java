package com.trading.refdata.account;

import java.util.List;
import java.util.regex.Pattern;

/**
 * Immutable account record deserialized from YAML / CSV / RDBMS.
 *
 * <p>Field semantics mirror the SBE {@code LoadAccountBatch} message (templateId&nbsp;12):
 *
 * <ul>
 *   <li>{@code accountId} — unique account identifier, must be &gt; 0 (tag&nbsp;10024)
 *   <li>{@code parentAccountId} — parent account id, 0 if top-level (tag&nbsp;10025)
 *   <li>{@code accountCode} — human-readable account code, must not be blank (tag&nbsp;1)
 *   <li>{@code acctIdSource} — account ID source: Internal, BIC, SID, TFM, OMGEO (tag&nbsp;660)
 *   <li>{@code accountName} — display name, must not be blank (tag&nbsp;10026)
 *   <li>{@code accountType} — House, Client, or MarketMaker (tag&nbsp;581)
 *   <li>{@code baseCurrency} — ISO 4217 alphabetic code (tag&nbsp;15)
 *   <li>{@code status} — Active, Suspended, or Closed (tag&nbsp;10027)
 *   <li>{@code complianceStatus} — OK, PendingReview, Suspended, Blocked (tag&nbsp;10028)
 *   <li>{@code capabilities} — bitmask of account capabilities (tag&nbsp;10029)
 *   <li>{@code symbolPreferences} — Phase 3 Commit B: per-account default subscription symbols
 *       (each entry validated against {@code ^[A-Z]{6,8}$}); empty list = use cohort defaults
 *   <li>{@code panelLayout} — Phase 3 Commit B: per-account panel-mount preferences; empty list =
 *       fall back to UI defaults
 * </ul>
 *
 * <p><b>Phase 3 Commit B.</b> The {@code symbolPreferences} and {@code panelLayout} fields are
 * appended to the canonical constructor. Per the project's dev-phase convention every caller passes
 * them explicitly — use {@link java.util.List#of()} for the empty default. No legacy constructor
 * overload is provided; the schema rewrite is the source of truth.
 *
 * <p><b>Nested record.</b> {@link PanelSlot} describes a single panel mount-point: a {@code
 * panelId} (string identifier the React layout consults — e.g. {@code "order-entry"}) bound to a
 * named {@code slot} in the layout grid (e.g. {@code "right-top"}). Validated on construction.
 */
public record AccountRecord(
    long accountId,
    long parentAccountId,
    String accountCode,
    String acctIdSource,
    String accountName,
    String accountType,
    String baseCurrency,
    String status,
    String complianceStatus,
    long capabilities,
    List<String> symbolPreferences,
    List<PanelSlot> panelLayout) {

  /**
   * Compiled once at class load — used by the compact constructor's per-entry validation of {@link
   * #symbolPreferences}. Mirrors the reference-data convention {@code ^[A-Z]{6,8}$} (matches the
   * {@code SymbolPacking} 6–8 ASCII-upper-case constraint). The {@code Matcher} instances produced
   * by {@code matcher(s).matches()} are short-lived per-validate allocations; this is a one-shot
   * cost at YAML-load time, NOT a hot path.
   */
  private static final Pattern SYMBOL_PATTERN = Pattern.compile("^[A-Z]{6,8}$");

  /**
   * Nested record — a single panel-mount preference. The {@code panelId} string is the same
   * identifier the React layout module reads (e.g. {@code "order-entry"}); the {@code slot} names
   * the layout grid position (e.g. {@code "right-top"}, {@code "center-bottom"}). Both must be
   * non-blank.
   *
   * @param panelId the layout-side identifier; e.g. {@code "order-entry"}
   * @param slot the grid-slot name; e.g. {@code "right-top"}
   */
  public record PanelSlot(String panelId, String slot) {
    public PanelSlot {
      if (panelId == null || panelId.isBlank()) {
        throw new IllegalArgumentException("PanelSlot.panelId must not be blank");
      }
      if (slot == null || slot.isBlank()) {
        throw new IllegalArgumentException("PanelSlot.slot must not be blank");
      }
    }
  }

  /** Compact constructor — validates field constraints at construction time. */
  public AccountRecord {
    if (accountId <= 0) {
      throw new IllegalArgumentException("accountId must be > 0, got " + accountId);
    }
    if (parentAccountId < 0) {
      throw new IllegalArgumentException("parentAccountId must be >= 0, got " + parentAccountId);
    }
    if (accountCode == null || accountCode.isBlank()) {
      throw new IllegalArgumentException("accountCode must not be blank");
    }
    if (acctIdSource == null || acctIdSource.isBlank()) {
      throw new IllegalArgumentException("acctIdSource must not be blank");
    }
    if (accountName == null || accountName.isBlank()) {
      throw new IllegalArgumentException("accountName must not be blank");
    }
    if (accountType == null || accountType.isBlank()) {
      throw new IllegalArgumentException("accountType must not be blank");
    }
    if (baseCurrency == null || baseCurrency.isBlank()) {
      throw new IllegalArgumentException("baseCurrency must not be blank");
    }
    if (status == null || status.isBlank()) {
      throw new IllegalArgumentException("status must not be blank");
    }
    if (complianceStatus == null || complianceStatus.isBlank()) {
      throw new IllegalArgumentException("complianceStatus must not be blank");
    }
    if (capabilities < 0) {
      throw new IllegalArgumentException("capabilities must be >= 0, got " + capabilities);
    }
    // Phase 3 Commit B: validate the new lists. Null is rejected; empty list is the documented
    // legacy default; each symbol entry must match ^[A-Z]{6,8}$ (mirrors the wire constraint).
    if (symbolPreferences == null) {
      throw new IllegalArgumentException("symbolPreferences must not be null (use List.of())");
    }
    for (final var symbol : symbolPreferences) {
      if (symbol == null || symbol.isBlank()) {
        throw new IllegalArgumentException(
            "symbolPreferences entries must not be blank, got: " + symbol);
      }
      if (!SYMBOL_PATTERN.matcher(symbol).matches()) {
        throw new IllegalArgumentException(
            "symbolPreferences entry '" + symbol + "' does not match ^[A-Z]{6,8}$");
      }
    }
    if (panelLayout == null) {
      throw new IllegalArgumentException("panelLayout must not be null (use List.of())");
    }
    // Defensive copy via List.copyOf to make the stored lists immutable — protects against a
    // caller that retains a mutable reference and mutates after construction.
    symbolPreferences = List.copyOf(symbolPreferences);
    panelLayout = List.copyOf(panelLayout);
  }
}
