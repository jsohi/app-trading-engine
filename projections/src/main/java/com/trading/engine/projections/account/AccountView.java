package com.trading.engine.projections.account;

import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.AccountTypeEnum;
import com.trading.engine.messages.sbe.AcctIDSourceEnum;
import com.trading.engine.messages.sbe.ComplianceStatusEnum;

/**
 * Mutable internal read-model for a single account. Tracks the latest state from {@code
 * AccountLoadedEvent} (template 110) events, supporting upsert semantics where the same {@code
 * accountId} may be re-loaded with updated field values.
 *
 * <p><b>Ownership:</b> instances are owned exclusively by {@link AccountProjection}. All setters
 * are package-private — only {@code AccountProjection} mutates this object. Byte-array getters are
 * also package-private to prevent leaking mutable internal state.
 *
 * <p><b>Identity semantics:</b> do NOT override {@code equals()} or {@code hashCode()} on this
 * class. Consistent with {@link com.trading.engine.projections.order.OrderView} identity semantics.
 *
 * <p><b>Threading:</b> accessed single-threaded from the event-dispatch thread under the
 * projection's write lock. Query threads never see this object directly — they receive an immutable
 * {@link AccountReadModel} copy.
 *
 * <p><b>Allocation:</b> one instance per account, allocated on first {@code AccountLoadedEvent}.
 * Fields are mutated in-place on subsequent re-loads (upsert). Setters zero-fill byte arrays before
 * copy to prevent stale trailing bytes on shorter-value upserts.
 */
final class AccountView {

  // --- Fixed-length byte array fields matching SBE schema types ---

  /** FIX tag 1: account code. SBE Account char[16]. */
  private final byte[] accountCode = new byte[16];

  private int accountCodeLen;

  /** Custom tag 10026: account name. SBE Text char[64]. */
  private final byte[] accountName = new byte[64];

  private int accountNameLen;

  /** FIX tag 15: base currency. SBE Currency char[3]. */
  private final byte[] baseCurrency = new byte[3];

  private int baseCurrencyLen;

  // --- Primitive fields ---

  /** Custom tag 10024: primary account identifier. */
  private long accountId;

  /** Custom tag 10040: parent account for give-up arrangements. 0 = no parent. */
  private long parentAccountId;

  // --- Enum fields ---

  /** FIX tag 660: account identifier source scheme. */
  private AcctIDSourceEnum acctIdSource = AcctIDSourceEnum.NULL_VAL;

  /** Custom tag 10029: account type classification. */
  private AccountTypeEnum accountType = AccountTypeEnum.NULL_VAL;

  /** Custom tag 10027: administrative status. */
  private AccountStatusEnum status = AccountStatusEnum.NULL_VAL;

  /** Custom tag 10041: KYC/compliance status. */
  private ComplianceStatusEnum complianceStatus = ComplianceStatusEnum.NULL_VAL;

  /** Custom tag 10042: capability bitfield (bit 0 = CAN_TRADE, bit 1 = CAN_RFQ). */
  private long capabilities;

  /** FIX tag 60: transaction time. Epoch nanos. */
  private long transactTime;

  // --- Timestamps ---

  /** Event sequence number from the most recently applied AccountLoadedEvent. */
  private long sequenceNumber;

  /** Cluster timestamp (epoch nanos) of the most recently applied AccountLoadedEvent. */
  private long lastUpdatedAt;

  // --- Public getters (primitives and enums) ---

  /**
   * @return the primary account identifier (custom tag 10024)
   */
  public long accountId() {
    return accountId;
  }

  /**
   * @return the parent account identifier for give-up arrangements, 0 if none (custom tag 10040)
   */
  public long parentAccountId() {
    return parentAccountId;
  }

  /**
   * @return the account identifier source scheme (FIX tag 660)
   */
  public AcctIDSourceEnum acctIdSource() {
    return acctIdSource;
  }

  /**
   * @return the account type classification (custom tag 10029)
   */
  public AccountTypeEnum accountType() {
    return accountType;
  }

  /**
   * @return the administrative status (custom tag 10027)
   */
  public AccountStatusEnum status() {
    return status;
  }

  /**
   * @return the KYC/compliance status (custom tag 10041)
   */
  public ComplianceStatusEnum complianceStatus() {
    return complianceStatus;
  }

  /**
   * @return the raw capability bitfield (custom tag 10042; bit 0 = CAN_TRADE, bit 1 = CAN_RFQ)
   */
  public long capabilities() {
    return capabilities;
  }

  /**
   * @return the transaction time in epoch nanos (FIX tag 60)
   */
  public long transactTime() {
    return transactTime;
  }

  /**
   * @return the event sequence number from the most recently applied AccountLoadedEvent
   */
  public long sequenceNumber() {
    return sequenceNumber;
  }

  /**
   * @return the cluster timestamp (epoch nanos) of the most recently applied AccountLoadedEvent
   */
  public long lastUpdatedAt() {
    return lastUpdatedAt;
  }

  /**
   * Returns {@code true} if the account has the CAN_TRADE capability (bit 0).
   *
   * @return {@code true} if this account is permitted to trade
   */
  public boolean canTrade() {
    return (capabilities & 1L) != 0;
  }

  /**
   * Returns {@code true} if the account has the CAN_RFQ capability (bit 1).
   *
   * @return {@code true} if this account is permitted to request quotes
   */
  public boolean canRequestQuotes() {
    return (capabilities & (1L << 1)) != 0;
  }

  /**
   * Returns {@code true} if the account status is {@link AccountStatusEnum#Active}.
   *
   * @return {@code true} if active
   */
  public boolean isActive() {
    return status == AccountStatusEnum.Active;
  }

  // --- Package-private byte array getters (prevent leaking mutable state) ---

  byte[] accountCode() {
    return accountCode;
  }

  int accountCodeLen() {
    return accountCodeLen;
  }

  byte[] accountName() {
    return accountName;
  }

  int accountNameLen() {
    return accountNameLen;
  }

  byte[] baseCurrency() {
    return baseCurrency;
  }

  int baseCurrencyLen() {
    return baseCurrencyLen;
  }

  // --- Package-private setters (only AccountProjection mutates) ---
  // All readers use the corresponding length field (e.g., accountCodeLen) to determine
  // the valid byte range. Trailing bytes beyond the length are never read.

  void setAccountCode(final byte[] src, final int offset, final int length) {
    System.arraycopy(src, offset, accountCode, 0, length);
    accountCodeLen = length;
  }

  void setAccountName(final byte[] src, final int offset, final int length) {
    System.arraycopy(src, offset, accountName, 0, length);
    accountNameLen = length;
  }

  void setBaseCurrency(final byte[] src, final int offset, final int length) {
    System.arraycopy(src, offset, baseCurrency, 0, length);
    baseCurrencyLen = length;
  }

  void setAccountId(final long accountId) {
    this.accountId = accountId;
  }

  void setParentAccountId(final long parentAccountId) {
    this.parentAccountId = parentAccountId;
  }

  void setAcctIdSource(final AcctIDSourceEnum acctIdSource) {
    this.acctIdSource = acctIdSource;
  }

  void setAccountType(final AccountTypeEnum accountType) {
    this.accountType = accountType;
  }

  void setStatus(final AccountStatusEnum status) {
    this.status = status;
  }

  void setComplianceStatus(final ComplianceStatusEnum complianceStatus) {
    this.complianceStatus = complianceStatus;
  }

  void setCapabilities(final long capabilities) {
    this.capabilities = capabilities;
  }

  void setTransactTime(final long transactTime) {
    this.transactTime = transactTime;
  }

  void setSequenceNumber(final long sequenceNumber) {
    this.sequenceNumber = sequenceNumber;
  }

  void setLastUpdatedAt(final long lastUpdatedAt) {
    this.lastUpdatedAt = lastUpdatedAt;
  }
}
