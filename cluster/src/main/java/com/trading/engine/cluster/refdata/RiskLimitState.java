package com.trading.engine.cluster.refdata;

import com.trading.engine.messages.sbe.AccountStatusEnum;

/**
 * Mutable in-memory representation of one per-account risk-limit record held by {@link
 * RiskLimitStore}. Industry-standard split from {@link AccountState} — pre-trade risk gates change
 * on a different cadence than account master data and are typically hierarchical (firm &gt; desk
 * &gt; trader &gt; account); collapsing them into the account record paints you into a corner. This
 * first cut is flat per-account; hierarchical scoping rolls into APP-58 AccountStore work.
 *
 * <p>All limit values are {@code long} fixed-point × 10⁻⁸ (consistent with {@code PRICE_SCALE}). A
 * value of {@code 0} on a numeric cap means "unlimited" unless the corresponding {@code *Enabled}
 * boolean is set, in which case the explicit enable flag gates the check (APP-62 introduces {@code
 * positionLimitEnabled} and {@code fatFingerEnabled} to eliminate the 0-as-disabled ambiguity for
 * the new caps).
 *
 * <p>APP-62 additions: {@code maxLongPosition}, {@code maxShortPosition}, {@code
 * positionLimitEnabled} (§4); {@code priceDeviationBps}, {@code fatFingerEnabled}, {@code
 * fatFingerFailClosed} (§5); {@code idleSessionTimeoutNanos} (§B); {@code proposerId}, {@code
 * approverId} (§H 4-eyes). {@code maxDailyLossBps} was REMOVED — it requires filled-position + mark
 * price which are produced by APP-180 (matching engine); it will be re-added by APP-180.
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

  /**
   * APP-62 §5 — per-account fat-finger tolerance in basis points. Per-symbol §I override available.
   */
  private long priceDeviationBps;

  /** APP-62 §5 — 0 = fat-finger check skipped; 1 = enforce. */
  private boolean fatFingerEnabled;

  /**
   * APP-62 §5 — 0 = skip on no-reference (open behaviour); 1 = reject limit orders until reference
   * established (default).
   */
  private boolean fatFingerFailClosed;

  /**
   * APP-62 §B — per-account idle-session-timeout override (nanoseconds); 0 = use system default.
   */
  private long idleSessionTimeoutNanos;

  /**
   * APP-62 §H — operator submitting the load (mandatory, non-empty). Backing byte array is
   * fixed-length.
   */
  private final byte[] proposerId = new byte[ACCOUNT_ID_BYTE_LEN];

  /** APP-62 §H — operator approving the load (mandatory, non-empty AND ≠ proposerId). */
  private final byte[] approverId = new byte[ACCOUNT_ID_BYTE_LEN];

  private AccountStatusEnum status = AccountStatusEnum.Active;
  private long transactTime;

  /** Sets the FK to AccountStore; must match an existing account when consumed by the handler. */
  public void setAccountId(long value) {
    this.accountId = value;
  }

  /** Sets the maximum single-order size in fixed-point 10⁻⁸; {@code 0} = unlimited. */
  public void setMaxOrderSize(long value) {
    this.maxOrderSize = value;
  }

  /**
   * Sets the maximum single-order notional (price × qty) in fixed-point 10⁻⁸; {@code 0} =
   * unlimited.
   */
  public void setMaxOrderNotional(long value) {
    this.maxOrderNotional = value;
  }

  /** Sets the maximum daily aggregate volume in fixed-point 10⁻⁸; {@code 0} = unlimited. */
  public void setMaxDailyVolume(long value) {
    this.maxDailyVolume = value;
  }

  /** Sets the maximum NewOrderSingle admissions per 1-second window; {@code 0} = unlimited. */
  public void setMaxOrdersPerSecond(long value) {
    this.maxOrdersPerSecond = value;
  }

  /** APP-62 §4 — sets the maximum working long position per (account, symbol), fixed-point 10⁻⁸. */
  public void setMaxLongPosition(long value) {
    this.maxLongPosition = value;
  }

  /**
   * APP-62 §4 — sets the maximum working short position per (account, symbol), fixed-point 10⁻⁸.
   */
  public void setMaxShortPosition(long value) {
    this.maxShortPosition = value;
  }

  /** APP-62 §4 — when {@code false} the position-limit check is skipped regardless of L/S caps. */
  public void setPositionLimitEnabled(boolean value) {
    this.positionLimitEnabled = value;
  }

  /** APP-62 §5 — sets the per-account fat-finger tolerance in basis points. */
  public void setPriceDeviationBps(long value) {
    this.priceDeviationBps = value;
  }

  /** APP-62 §5 — when {@code false} the fat-finger check is skipped regardless of bps. */
  public void setFatFingerEnabled(boolean value) {
    this.fatFingerEnabled = value;
  }

  /**
   * APP-62 §5 — when {@code true} (default), limit orders are rejected until a reference price is
   * established; when {@code false}, no-reference orders are admitted (legacy fail-open behaviour).
   */
  public void setFatFingerFailClosed(boolean value) {
    this.fatFingerFailClosed = value;
  }

  /**
   * APP-62 §B — sets the per-account idle-session-timeout override in nanoseconds; {@code 0} means
   * "use the LauncherConfig system default."
   */
  public void setIdleSessionTimeoutNanos(long value) {
    this.idleSessionTimeoutNanos = value;
  }

  /**
   * Copies up to {@code length} bytes from {@code src[srcOffset..]} into the backing {@code
   * proposerId} byte array. The destination is fixed at 16 bytes (SBE {@code Account} type).
   *
   * @param src source byte buffer
   * @param srcOffset start offset within {@code src}
   * @param length number of source bytes to consider; values &gt; 16 are silently truncated to the
   *     16-byte field width. Any tail bytes (when {@code length &lt; 16}) are zero-filled so the
   *     field has well-defined contents.
   */
  public void setProposerId(final byte[] src, int srcOffset, int length) {
    populateFixedField(proposerId, src, srcOffset, length);
  }

  /** See {@link #setProposerId(byte[], int, int)} — same contract, different target buffer. */
  public void setApproverId(final byte[] src, int srcOffset, int length) {
    populateFixedField(approverId, src, srcOffset, length);
  }

  public void setStatus(final AccountStatusEnum value) {
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

  /**
   * Returns the backing 16-byte buffer for the proposerId. Do not mutate outside {@link
   * #setProposerId}.
   */
  public byte[] proposerId() {
    return proposerId;
  }

  /**
   * Returns the backing 16-byte buffer for the approverId. Do not mutate outside {@link
   * #setApproverId}.
   */
  public byte[] approverId() {
    return approverId;
  }

  public AccountStatusEnum status() {
    return status;
  }

  public long transactTime() {
    return transactTime;
  }

  /**
   * Returns {@code true} when the proposerId byte buffer is all-zero (i.e., empty / not yet
   * populated by any {@link #setProposerId} call). Used by APP-62 §H 4-eyes ingress validation in
   * {@link LoadRiskLimitHandler} to reject loads that omit the proposer.
   */
  public boolean proposerIdIsEmpty() {
    return isAllZero(proposerId);
  }

  /**
   * Returns {@code true} when the approverId byte buffer is all-zero (i.e., empty / not yet
   * populated by any {@link #setApproverId} call). Used by APP-62 §H 4-eyes ingress validation.
   */
  public boolean approverIdIsEmpty() {
    return isAllZero(approverId);
  }

  /**
   * Returns true when {@code proposerId} byte-equals {@code approverId} (APP-62 §H 4-eyes
   * self-approval check).
   */
  public boolean proposerEqualsApprover() {
    return AccountIdentifierBytes.byteEquals(proposerId, approverId);
  }

  private static void populateFixedField(
      final byte[] dst, final byte[] src, int srcOffset, int length) {
    int copyLen = Math.min(length, dst.length);
    System.arraycopy(src, srcOffset, dst, 0, copyLen);
    for (int i = copyLen; i < dst.length; i++) {
      dst[i] = 0;
    }
  }

  private static boolean isAllZero(final byte[] buf) {
    return AccountIdentifierBytes.isAllZero(buf);
  }
}
