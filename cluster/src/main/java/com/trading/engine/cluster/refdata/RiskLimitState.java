package com.trading.engine.cluster.refdata;

import com.trading.engine.messages.sbe.AccountStatusEnum;

/**
 * Mutable in-memory representation of one per-account risk-limit record held by {@link
 * RiskLimitStore}. Industry-standard split from {@link AccountState} — pre-trade risk gates change
 * on a different cadence than account master data and are typically hierarchical (firm > desk >
 * trader > account); collapsing them into the account record paints you into a corner. This first
 * cut is flat per-account; hierarchical scoping is a follow-up where the store key becomes {@code
 * (scope, scopeId)}.
 *
 * <p>All limit values are {@code long} fixed-point × 10⁻⁸ (consistent with {@code PRICE_SCALE}). A
 * value of {@code 0} means "unlimited".
 */
public final class RiskLimitState {

  private long accountId;
  private long maxOrderSize;
  private long maxOrderNotional;
  private long maxDailyVolume;
  private long maxDailyLossBps;

  /**
   * Maximum NewOrderSingle admissions per 1-second wall-clock-aligned window per account (APP-62
   * slice 2). {@code 0} = unlimited (matches the rest of the limit fields' "0 means disabled"
   * convention). Stored as {@code long} (not {@code int}) for uniformity with the other limit
   * fields even though the practical range fits in {@code uint32} — SBE wire type is uint32 on the
   * schema side, widened on read.
   */
  private long maxOrdersPerSecond;

  private AccountStatusEnum status = AccountStatusEnum.Active;
  private long transactTime;

  public void setAccountId(final long value) {
    this.accountId = value;
  }

  public void setMaxOrderSize(final long value) {
    this.maxOrderSize = value;
  }

  public void setMaxOrderNotional(final long value) {
    this.maxOrderNotional = value;
  }

  public void setMaxDailyVolume(final long value) {
    this.maxDailyVolume = value;
  }

  public void setMaxDailyLossBps(final long value) {
    this.maxDailyLossBps = value;
  }

  public void setMaxOrdersPerSecond(final long value) {
    this.maxOrdersPerSecond = value;
  }

  public void setStatus(final AccountStatusEnum value) {
    this.status = value;
  }

  public void setTransactTime(final long value) {
    this.transactTime = value;
  }

  public long accountId() {
    return accountId;
  }

  public long maxOrderSize() {
    return maxOrderSize;
  }

  public long maxOrderNotional() {
    return maxOrderNotional;
  }

  public long maxDailyVolume() {
    return maxDailyVolume;
  }

  public long maxDailyLossBps() {
    return maxDailyLossBps;
  }

  public long maxOrdersPerSecond() {
    return maxOrdersPerSecond;
  }

  public AccountStatusEnum status() {
    return status;
  }

  public long transactTime() {
    return transactTime;
  }
}
