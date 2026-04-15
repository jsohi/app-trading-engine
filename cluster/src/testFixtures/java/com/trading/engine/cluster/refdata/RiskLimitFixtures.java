package com.trading.engine.cluster.refdata;

import com.trading.engine.messages.FixedPointScale;
import com.trading.engine.messages.sbe.AccountStatusEnum;

/**
 * Factory methods for {@link RiskLimitState} test instances. All limit values use the engine's
 * fixed-point scale ({@link FixedPointScale#PRICE_SCALE}, 10^8) — callers specify whole units and
 * the factory multiplies internally.
 *
 * <p>All methods replicate the setter sequence established by the production {@link RiskLimitStore}
 * restore path and the inline risk-limit setup from {@code TradingClusteredServiceTest}, ensuring
 * that test instances are indistinguishable from cluster-restored state.
 *
 * <p><b>Threading.</b> Stateless utility class — safe for unrestricted concurrent use. Each call
 * allocates a fresh {@link RiskLimitState} instance; callers own the returned reference.
 *
 * <p><b>Allocation.</b> One {@link RiskLimitState} allocation per call. Acceptable for test setup;
 * not intended for hot-path use.
 */
public final class RiskLimitFixtures {

  private RiskLimitFixtures() {}

  // ---------------------------------------------------------------------------
  // Full factory
  // ---------------------------------------------------------------------------

  /**
   * Create a fully specified {@link RiskLimitState} with {@link AccountStatusEnum#Active} status.
   * All limit values must be pre-scaled to the engine's fixed-point representation ({@link
   * FixedPointScale#PRICE_SCALE}). A value of {@code 0} means "unlimited" for that gate.
   *
   * @param accountId the owning account's numeric identifier
   * @param maxOrderSize maximum single-order size (fixed-point, e.g. {@code 10L *
   *     FixedPointScale.PRICE_SCALE} for 10 units)
   * @param maxOrderNotional maximum single-order notional value (fixed-point); {@code 0} =
   *     unlimited
   * @param maxDailyVolume maximum aggregate daily volume (fixed-point); {@code 0} = unlimited
   * @param maxDailyLossBps maximum daily loss in basis points (fixed-point); {@code 0} = unlimited
   * @return a new active {@link RiskLimitState} ready for insertion into a {@link RiskLimitStore}
   */
  public static RiskLimitState riskLimit(
      final long accountId,
      final long maxOrderSize,
      final long maxOrderNotional,
      final long maxDailyVolume,
      final long maxDailyLossBps) {
    final RiskLimitState limit = new RiskLimitState();
    limit.setAccountId(accountId);
    limit.setMaxOrderSize(maxOrderSize);
    limit.setMaxOrderNotional(maxOrderNotional);
    limit.setMaxDailyVolume(maxDailyVolume);
    limit.setMaxDailyLossBps(maxDailyLossBps);
    limit.setStatus(AccountStatusEnum.Active);
    limit.setTransactTime(0L);
    return limit;
  }

  // ---------------------------------------------------------------------------
  // Convenience overloads
  // ---------------------------------------------------------------------------

  /**
   * Create a permissive risk limit for the given account: max order size of 10 whole units ({@code
   * 10 * PRICE_SCALE}), all other gates set to {@code 0} (unlimited).
   *
   * <p>Matches the standard test dataset established in {@code
   * TradingClusteredServiceTest.seedReferenceData()}.
   *
   * @param accountId the owning account's numeric identifier
   * @return a new permissive {@link RiskLimitState}
   */
  public static RiskLimitState permissive(final long accountId) {
    return riskLimit(accountId, 10L * FixedPointScale.PRICE_SCALE, 0L, 0L, 0L);
  }
}
