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
 *   <li>{@code maxDailyLossBps} — maximum daily loss in basis points; SBE type {@code uint32} so
 *       value must fit [0, 4294967295] (tag&nbsp;10048)
 *   <li>{@code status} — one of {@code Active}, {@code Suspended}, {@code Closed} (tag&nbsp;10027)
 * </ul>
 */
public record RiskLimitRecord(
    long accountId,
    long maxOrderSize,
    long maxOrderNotional,
    long maxDailyVolume,
    long maxDailyLossBps,
    String status) {

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
    if (maxDailyLossBps < 0 || maxDailyLossBps > 0xFFFF_FFFFL) {
      throw new IllegalArgumentException(
          "maxDailyLossBps must fit uint32 [0, 4294967295], got " + maxDailyLossBps);
    }
    if (status == null || status.isBlank()) {
      throw new IllegalArgumentException("status must not be blank");
    }
  }
}
