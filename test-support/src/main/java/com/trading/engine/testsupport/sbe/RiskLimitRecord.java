package com.trading.engine.testsupport.sbe;

/**
 * Typed record for batch risk-limit encoding via {@link SbeTestEncoder#encodeLoadRiskLimitBatch}.
 *
 * <p>Thread-safe — immutable value type.
 *
 * @param accountId account this limit applies to
 * @param maxOrderSize maximum single-order size in fixed-point scale 10^8
 * @param maxOrderNotional maximum single-order notional in fixed-point scale 10^8
 * @param maxDailyVolume maximum daily volume in fixed-point scale 10^8
 * @param maxDailyLossBps maximum daily loss in basis points; SBE schema type is {@code uint32} so
 *     values must be in range {@code [0, 4_294_967_295]}. The SBE-generated encoder accepts {@code
 *     long} and truncates to 32 bits internally.
 */
public record RiskLimitRecord(
    long accountId,
    long maxOrderSize,
    long maxOrderNotional,
    long maxDailyVolume,
    long maxDailyLossBps) {

  /** Validates that {@code maxDailyLossBps} fits within the SBE {@code uint32} range. */
  public RiskLimitRecord {
    if (maxDailyLossBps < 0 || maxDailyLossBps > 0xFFFF_FFFFL) {
      throw new IllegalArgumentException(
          "maxDailyLossBps must fit in uint32 [0, 4294967295], was: " + maxDailyLossBps);
    }
  }
}
