package com.trading.refdata.account;

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
 * </ul>
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
    long capabilities) {

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
  }
}
