package com.trading.engine.projections.account;

import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.AccountTypeEnum;
import com.trading.engine.messages.sbe.AcctIDSourceEnum;
import com.trading.engine.messages.sbe.ComplianceStatusEnum;
import java.util.Arrays;

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

  public long accountId() {
    return accountId;
  }

  public long parentAccountId() {
    return parentAccountId;
  }

  public AcctIDSourceEnum acctIdSource() {
    return acctIdSource;
  }

  public AccountTypeEnum accountType() {
    return accountType;
  }

  public AccountStatusEnum status() {
    return status;
  }

  public ComplianceStatusEnum complianceStatus() {
    return complianceStatus;
  }

  public long capabilities() {
    return capabilities;
  }

  public long transactTime() {
    return transactTime;
  }

  public long sequenceNumber() {
    return sequenceNumber;
  }

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
  // Byte array setters zero-fill before copy to prevent stale trailing bytes on upsert.

  void setAccountCode(final byte[] src, final int offset, final int length) {
    Arrays.fill(accountCode, (byte) 0);
    System.arraycopy(src, offset, accountCode, 0, length);
    accountCodeLen = length;
  }

  void setAccountName(final byte[] src, final int offset, final int length) {
    Arrays.fill(accountName, (byte) 0);
    System.arraycopy(src, offset, accountName, 0, length);
    accountNameLen = length;
  }

  void setBaseCurrency(final byte[] src, final int offset, final int length) {
    Arrays.fill(baseCurrency, (byte) 0);
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
