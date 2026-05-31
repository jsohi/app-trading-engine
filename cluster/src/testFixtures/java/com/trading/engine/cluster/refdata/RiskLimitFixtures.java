package com.trading.engine.cluster.refdata;

import com.trading.engine.messages.FixedPointScale;
import com.trading.engine.messages.sbe.AccountStatusEnum;
import java.nio.charset.StandardCharsets;

/**
 * Factory methods for {@link RiskLimitState} test instances. All limit values use the engine's
 * fixed-point scale ({@link FixedPointScale#PRICE_SCALE}, 10^8) — callers specify whole units and
 * the factory multiplies internally.
 *
 * <p>All methods replicate the setter sequence established by the production {@link RiskLimitStore}
 * restore path and the inline risk-limit setup from {@code TradingClusteredServiceTest}, ensuring
 * that test instances are indistinguishable from cluster-restored state.
 *
 * <p>APP-62: factories populate the new fields with safe test defaults — position and fat-finger
 * checks both DISABLED (so existing tests that pre-date these fields see no behaviour change),
 * 4-eyes proposer/approver pre-populated with distinct sentinel byte patterns so a test loading via
 * the production handler path passes the §H validation.
 *
 * <p><b>Threading.</b> Stateless utility class — safe for unrestricted concurrent use. Each call
 * allocates a fresh {@link RiskLimitState} instance; callers own the returned reference.
 *
 * <p><b>Allocation.</b> One {@link RiskLimitState} allocation per call. Acceptable for test setup;
 * not intended for hot-path use.
 */
public final class RiskLimitFixtures {

  /** Sentinel proposerId byte pattern for tests: 'TEST-PROPOSER' padded with zeros. */
  private static final byte[] TEST_PROPOSER_ID = paddedBytes("TEST-PROPOSER");

  /** Sentinel approverId byte pattern for tests: 'TEST-APPROVER' padded with zeros. */
  private static final byte[] TEST_APPROVER_ID = paddedBytes("TEST-APPROVER");

  private RiskLimitFixtures() {}

  // ---------------------------------------------------------------------------
  // Full factory
  // ---------------------------------------------------------------------------

  /**
   * Create a fully specified {@link RiskLimitState} with {@link AccountStatusEnum#Active} status,
   * APP-62 §4 / §5 / §B knobs DISABLED (mirrors pre-APP-62 behaviour for tests that don't care
   * about the new gates), and pre-populated 4-eyes identifiers.
   *
   * @param accountId the owning account's numeric identifier
   * @param maxOrderSize maximum single-order size, fixed-point; {@code 0} = unlimited
   * @param maxOrderNotional maximum single-order notional value, fixed-point; {@code 0} = unlimited
   * @param maxDailyVolume maximum aggregate daily volume, fixed-point; {@code 0} = unlimited
   * @return a new active {@link RiskLimitState} ready for insertion into a {@link RiskLimitStore}
   */
  public static RiskLimitState riskLimit(
      long accountId, long maxOrderSize, long maxOrderNotional, long maxDailyVolume) {
    final var limit = new RiskLimitState();
    limit.setAccountId(accountId);
    limit.setMaxOrderSize(maxOrderSize);
    limit.setMaxOrderNotional(maxOrderNotional);
    limit.setMaxDailyVolume(maxDailyVolume);
    limit.setMaxOrdersPerSecond(0L);
    limit.setMaxLongPosition(0L);
    limit.setMaxShortPosition(0L);
    limit.setPositionLimitEnabled(false);
    limit.setPriceDeviationBps(0L);
    limit.setFatFingerEnabled(false);
    limit.setFatFingerFailClosed(true);
    limit.setIdleSessionTimeoutNanos(0L);
    limit.setProposerId(TEST_PROPOSER_ID, 0, TEST_PROPOSER_ID.length);
    limit.setApproverId(TEST_APPROVER_ID, 0, TEST_APPROVER_ID.length);
    limit.setStatus(AccountStatusEnum.Active);
    limit.setTransactTime(0L);
    return limit;
  }

  // ---------------------------------------------------------------------------
  // Convenience overloads
  // ---------------------------------------------------------------------------

  /**
   * Create a permissive risk limit for the given account: max order size of 10 whole units, all
   * other gates set to {@code 0} (unlimited).
   *
   * @param accountId the owning account's numeric identifier
   * @return a new permissive {@link RiskLimitState}
   */
  public static RiskLimitState permissive(long accountId) {
    return riskLimit(accountId, 10L * FixedPointScale.PRICE_SCALE, 0L, 0L);
  }

  private static byte[] paddedBytes(final String s) {
    final byte[] out = new byte[16];
    final byte[] src = s.getBytes(StandardCharsets.US_ASCII);
    System.arraycopy(src, 0, out, 0, Math.min(src.length, out.length));
    return out;
  }
}
