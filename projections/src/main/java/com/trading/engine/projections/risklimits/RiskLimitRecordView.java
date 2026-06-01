package com.trading.engine.projections.risklimits;

import com.trading.engine.messages.sbe.AccountStatusEnum;

/**
 * Immutable read-model record holding the projected APP-62 risk-limit fields for a single account.
 * Populated by {@link RiskLimitProjection} from {@code RiskLimitLoadedEvent} (SBE template 115) and
 * returned to callers via {@link RiskLimitProjection#getByAccountId(long)}.
 *
 * <p><b>Field set.</b> Every numeric and enum field carried by {@code RiskLimitLoadedEvent} —
 * {@code accountId}, the three notional / volume / order-size caps, the per-side position caps with
 * their enable flag, the fat-finger knobs, the per-account idle-session-timeout override, the
 * status, the audit fields ({@code proposerId}, {@code approverId}, {@code transactTime}), and the
 * snapshot's {@code sequenceNumber}. Fields are exposed as primitive accessors (no boxing) so the
 * caller can copy into a flyweight view without allocation.
 *
 * <p><b>Threading.</b> Immutable record — safe for unrestricted concurrent reads.
 *
 * <p><b>Allocation.</b> One record per projection upsert; consumers must NOT cache the reference
 * past the next upsert (the projection replaces the entry, but the prior instance remains valid —
 * it is simply orphaned for GC at the next write).
 *
 * @param accountId numeric account identifier (FIX-equivalent custom tag 10024)
 * @param maxOrderSize maximum single-order quantity in fixed-point 10⁻⁸; {@code 0} = unlimited
 * @param maxOrderNotional maximum single-order notional in fixed-point 10⁻⁸; {@code 0} = unlimited
 * @param maxDailyVolume maximum aggregate daily volume in fixed-point 10⁻⁸; {@code 0} = unlimited
 * @param maxOrdersPerSecond per-account command rate cap; {@code 0} = unlimited
 * @param maxLongPosition maximum simultaneous working long exposure (APP-62 §4)
 * @param maxShortPosition maximum simultaneous working short exposure (APP-62 §4)
 * @param positionLimitEnabled {@code true} if the §4 position-limit check is active
 * @param priceDeviationBps fat-finger band tolerance (APP-62 §5)
 * @param fatFingerEnabled {@code true} if the §5 fat-finger check is active
 * @param fatFingerFailClosed {@code true} if missing references fail-closed (industry default)
 * @param idleSessionTimeoutNanos per-account idle-session timeout (APP-62 §B); {@code 0} = use
 *     system default supplied to {@code onIdleScan}
 * @param status account status (see {@link AccountStatusEnum})
 * @param transactTime cluster timestamp (epoch nanos) the limit was loaded
 * @param sequenceNumber projection sequence number assigned by {@link RiskLimitProjection}
 */
public record RiskLimitRecordView(
    long accountId,
    long maxOrderSize,
    long maxOrderNotional,
    long maxDailyVolume,
    long maxOrdersPerSecond,
    long maxLongPosition,
    long maxShortPosition,
    boolean positionLimitEnabled,
    long priceDeviationBps,
    boolean fatFingerEnabled,
    boolean fatFingerFailClosed,
    long idleSessionTimeoutNanos,
    AccountStatusEnum status,
    long transactTime,
    long sequenceNumber) {}
