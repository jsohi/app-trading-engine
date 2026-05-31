package com.trading.engine.cluster.refdata;

import com.trading.engine.messages.sbe.AccountStatusEnum;

/**
 * Mutable in-memory representation of one per-account risk-limit record held by {@link
 * RiskLimitStore}. Industry-standard split from {@link AccountState} — pre-trade risk gates change
 * on a different cadence than account master data and are typically hierarchical (firm &gt; desk
 * &gt; trader &gt; account); collapsing them into the account record paints you into a corner.
 * This first cut is flat per-account; hierarchical scoping rolls into APP-58 AccountStore work.
 *
 * <p>All limit values are {@code long} fixed-point × 10⁻⁸ (consistent with {@code PRICE_SCALE}). A
 * value of {@code 0} on a numeric cap means "unlimited" unless the corresponding {@code *Enabled}
 * boolean is set, in which case the explicit enable flag gates the check (APP-62 introduces
 * {@code positionLimitEnabled} and {@code fatFingerEnabled} to eliminate the 0-as-disabled
 * ambiguity for the new caps).
 *
 * <p>APP-62 additions: {@code maxLongPosition}, {@code maxShortPosition}, {@code
 * positionLimitEnabled} (§4); {@code priceDeviationBps}, {@code fatFingerEnabled}, {@code
 * fatFingerFailClosed} (§5); {@code idleSessionTimeoutNanos} (§B); {@code proposerId}, {@code
 * approverId} (§H 4-eyes). {@code maxDailyLossBps} was REMOVED — it requires filled-position +
 * mark price which are produced by APP-180 (matching engine); it will be re-added by APP-180.
 *
 * <p>Threading: not thread-safe — single-threaded cluster duty cycle only.
 *
 * <p>Allocation: zero-allocation after construction (mutated via setters; {@code Account}
 * identifier fields use fixed-length byte arrays so populate-from-decoder is allocation-free).
 */
public final class RiskLimitState {

  /** Account-identifier byte arrays are bounded by the SBE {@code Account} char[16] type. */
  private static final int ACCOUNT_ID_BYTE_LEN = 16;

  private long accountId;
  private long maxOrderSize;
  private long maxOrderNotional;
  private long maxDailyVolume;

  /**
   * Maximum NewOrderSingle admissions per 1-second wall-clock-aligned window per account (APP-62
   * slice 2). {@code 0} = unlimited. SBE wire type is uint32; widened to long on read.
   */
  private long maxOrdersPerSecond;

  /** APP-62 §4 — max working long position per (account, symbol), fixed-point 10⁻⁸. */
  private long maxLongPosition;

  /** APP-62 §4 — max working short position per (account, symbol), fixed-point 10⁻⁸. */
  private long maxShortPosition;

  /** APP-62 §4 — 0 = position check skipped; 1 = enforce L/S caps. */
  private boolean positionLimitEnabled;

  /** APP-62 §5 — per-account fat-finger tolerance in basis points. Per-symbol §I override available. */
  private long priceDeviationBps;

  /** APP-62 §5 — 0 = fat-finger check skipped; 1 = enforce. */
  private boolean fatFingerEnabled;

  /** APP-62 §5 — 0 = skip on no-reference (open behaviour); 1 = reject limit orders until reference established (default). */
  private boolean fatFingerFailClosed;

  /** APP-62 §B — per-account idle-session-timeout override (nanoseconds); 0 = use system default. */
  private long idleSessionTimeoutNanos;

  /** APP-62 §H — operator submitting the load (mandatory, non-empty). Backing byte array is fixed-length. */
  private final byte[] proposerId = new byte[ACCOUNT_ID_BYTE_LEN];

  /** APP-62 §H — operator approving the load (mandatory, non-empty AND ≠ proposerId). */
  private final byte[] approverId = new byte[ACCOUNT_ID_BYTE_LEN];

  private AccountStatusEnum status = AccountStatusEnum.Active;
  private long transactTime;

  public void setAccountId(long value) {
    this.accountId = value;
  }

  public void setMaxOrderSize(long value) {
    this.maxOrderSize = value;
  }

  public void setMaxOrderNotional(long value) {
    this.maxOrderNotional = value;
  }

  public void setMaxDailyVolume(long value) {
    this.maxDailyVolume = value;
  }

  public void setMaxOrdersPerSecond(long value) {
    this.maxOrdersPerSecond = value;
  }

  public void setMaxLongPosition(long value) {
    this.maxLongPosition = value;
  }

  public void setMaxShortPosition(long value) {
    this.maxShortPosition = value;
  }

  public void setPositionLimitEnabled(boolean value) {
    this.positionLimitEnabled = value;
  }

  public void setPriceDeviationBps(long value) {
    this.priceDeviationBps = value;
  }

  public void setFatFingerEnabled(boolean value) {
    this.fatFingerEnabled = value;
  }

  public void setFatFingerFailClosed(boolean value) {
    this.fatFingerFailClosed = value;
  }

  public void setIdleSessionTimeoutNanos(long value) {
    this.idleSessionTimeoutNanos = value;
  }

  /**
   * Copies {@code length} bytes from {@code src[srcOffset..]} into the backing {@code proposerId}
   * byte array. Zero-fills any tail bytes so the field has well-defined contents.
   */
  public void setProposerId(byte[] src, int srcOffset, int length) {
    populateFixedField(proposerId, src, srcOffset, length);
  }

  /** See {@link #setProposerId(byte[], int, int)}. */
  public void setApproverId(byte[] src, int srcOffset, int length) {
    populateFixedField(approverId, src, srcOffset, length);
  }

  public void setStatus(AccountStatusEnum value) {
    this.status = value;
  }

  public void setTransactTime(long value) {
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

  public long maxOrdersPerSecond() {
    return maxOrdersPerSecond;
  }

  public long maxLongPosition() {
    return maxLongPosition;
  }

  public long maxShortPosition() {
    return maxShortPosition;
  }

  public boolean positionLimitEnabled() {
    return positionLimitEnabled;
  }

  public long priceDeviationBps() {
    return priceDeviationBps;
  }

  public boolean fatFingerEnabled() {
    return fatFingerEnabled;
  }

  public boolean fatFingerFailClosed() {
    return fatFingerFailClosed;
  }

  public long idleSessionTimeoutNanos() {
    return idleSessionTimeoutNanos;
  }

  /** Returns the backing 16-byte buffer for the proposerId. Do not mutate outside {@link #setProposerId}. */
  public byte[] proposerId() {
    return proposerId;
  }

  /** Returns the backing 16-byte buffer for the approverId. Do not mutate outside {@link #setApproverId}. */
  public byte[] approverId() {
    return approverId;
  }

  public AccountStatusEnum status() {
    return status;
  }

  public long transactTime() {
    return transactTime;
  }

  /** Returns true if the proposerId has any non-zero byte (i.e., non-empty). */
  public boolean proposerIdIsEmpty() {
    return isAllZero(proposerId);
  }

  /** Returns true if the approverId has any non-zero byte (i.e., non-empty). */
  public boolean approverIdIsEmpty() {
    return isAllZero(approverId);
  }

  /** Returns true when {@code proposerId} byte-equals {@code approverId} (APP-62 §H 4-eyes self-approval check). */
  public boolean proposerEqualsApprover() {
    for (int i = 0; i < ACCOUNT_ID_BYTE_LEN; i++) {
      if (proposerId[i] != approverId[i]) {
        return false;
      }
    }
    return true;
  }

  private static void populateFixedField(byte[] dst, byte[] src, int srcOffset, int length) {
    int copyLen = Math.min(length, dst.length);
    System.arraycopy(src, srcOffset, dst, 0, copyLen);
    for (int i = copyLen; i < dst.length; i++) {
      dst[i] = 0;
    }
  }

  private static boolean isAllZero(byte[] buf) {
    for (int i = 0; i < buf.length; i++) {
      if (buf[i] != 0) {
        return false;
      }
    }
    return true;
  }
}
