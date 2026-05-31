package com.trading.engine.testsupport.sbe;

/**
 * Typed record for batch risk-limit encoding via {@link SbeTestEncoder#encodeLoadRiskLimitBatch}.
 *
 * <p>Thread-safe — immutable value type.
 *
 * <p>APP-62: {@code maxDailyLossBps} removed (re-introduced by APP-180 with mark price + filled
 * position). The new APP-62 fields (position L/S caps, fat-finger knobs, idle-timeout override,
 * 4-eyes identifiers) are populated to safe defaults by {@link SbeTestEncoder} when this minimal
 * record is encoded; tests needing non-default values use the direct encoder helpers.
 *
 * @param accountId account this limit applies to
 * @param maxOrderSize maximum single-order size in fixed-point scale 10^8
 * @param maxOrderNotional maximum single-order notional in fixed-point scale 10^8
 * @param maxDailyVolume maximum daily volume in fixed-point scale 10^8
 */
public record RiskLimitRecord(
    long accountId, long maxOrderSize, long maxOrderNotional, long maxDailyVolume) {}
