package com.trading.refdata.risklimit;

/**
 * Immutable risk-limit record deserialized from YAML / CSV / RDBMS.
 *
 * <p>Field semantics mirror the SBE {@code LoadRiskLimitBatch} message (templateId&nbsp;16):
 *
 * <ul>
 *   <li>{@code accountId} — must be &gt; 0; FK to an existing account (tag&nbsp;10024)
 *   <li>{@code maxOrderSize} — maximum single-order size in fixed-point (&ge; 0, 0 = unlimited)
 *       (tag&nbsp;10030)
 *   <li>{@code maxOrderNotional} — maximum single-order notional in fixed-point (&ge; 0, 0 =
 *       unlimited) (tag&nbsp;10047)
 *   <li>{@code maxDailyVolume} — maximum daily cumulative volume (&ge; 0, 0 = unlimited)
 *       (tag&nbsp;10031)
 *   <li>{@code status} — one of {@code Active}, {@code Suspended}, {@code Closed} (tag&nbsp;10027)
 * </ul>
 *
 * <p>APP-62: {@code maxDailyLossBps} REMOVED — the field is added back by APP-180 when mark price +
 * filled position are produced by the matching engine. The new APP-62 risk-limit fields (position
 * L/S caps, fat-finger knobs, per-account idle timeout, 4-eyes identifiers) are not yet exposed on
 * this record; the YAML loader fills the SBE encoder with safe defaults for those fields, and
 * {@link RiskLimitCommandEncoder} populates default proposerId/approverId so the §H 4-eyes check
 * passes for ops-loaded fixtures. A dedicated ops-tool extension will surface the new fields
 * end-to-end.
 */
public record RiskLimitRecord(
    long accountId, long maxOrderSize, long maxOrderNotional, long maxDailyVolume, String status) {

  /** Compact constructor — validates SBE schema constraints. */
  public RiskLimitRecord {
    if (accountId <= 0) {
      throw new IllegalArgumentException("accountId must be > 0, got " + accountId);
    }
    if (maxOrderSize < 0) {
      throw new IllegalArgumentException("maxOrderSize must be >= 0, got " + maxOrderSize);
    }
    if (maxOrderNotional < 0) {
      throw new IllegalArgumentException("maxOrderNotional must be >= 0, got " + maxOrderNotional);
    }
    if (maxDailyVolume < 0) {
      throw new IllegalArgumentException("maxDailyVolume must be >= 0, got " + maxDailyVolume);
    }
    if (status == null || status.isBlank()) {
      throw new IllegalArgumentException("status must not be blank");
    }
  }
}
