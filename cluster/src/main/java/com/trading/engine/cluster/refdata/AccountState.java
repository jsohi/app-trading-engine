package com.trading.engine.cluster.refdata;

import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.AccountTypeEnum;
import com.trading.engine.messages.sbe.AcctIDSourceEnum;
import com.trading.engine.messages.sbe.ComplianceStatusEnum;

/**
 * Mutable in-memory representation of one account held by {@link AccountStore}. Pure identity +
 * capabilities + status — risk limits live in the separate {@link RiskLimitStore} per industry
 * standard (CME Globex Credit Controls, Eurex T7, exchange-core).
 *
 * <p>FIX 4.4 alignment:
 *
 * <ul>
 *   <li>{@link #accountCode} ↔ FIX tag 1 {@code Account}
 *   <li>{@link #acctIdSource} ↔ FIX tag 660 {@code AcctIDSource}
 *   <li>{@link #parentAccountId} supports FIX 453 {@code NoPartyIDs} give-up arrangements
 *   <li>{@link #capabilities} is a uint64 bitfield (bit 0 = CAN_TRADE, bit 1 = CAN_RFQ, …); see
 *       {@link Capabilities} constants
 * </ul>
 */
public final class AccountState {

  /** Capability bit flags. */
  public static final class Capabilities {
    public static final long CAN_TRADE = 1L;
    public static final long CAN_RFQ = 1L << 1;

    // Reserved for future use: CAN_SHORT, CAN_MARGIN, CAN_OPTIONS, CAN_FUTURES, …

    private Capabilities() {}
  }

  private long accountId;
  private long parentAccountId;
  private final byte[] accountCode = new byte[16];
  private int accountCodeLength;
  private AcctIDSourceEnum acctIdSource = AcctIDSourceEnum.Internal;
  private final byte[] accountName = new byte[64];
  private int accountNameLength;
  private AccountTypeEnum accountType = AccountTypeEnum.Client;
  private final byte[] baseCurrency = new byte[3];
  private AccountStatusEnum status = AccountStatusEnum.Active;
  private ComplianceStatusEnum complianceStatus = ComplianceStatusEnum.OK;
  private long capabilities;
  private long transactTime;

  // ---------------------------------------------------------------------------
  // Mutators
  // ---------------------------------------------------------------------------

  public void setAccountId(final long value) {
    this.accountId = value;
  }

  public void setParentAccountId(final long value) {
    this.parentAccountId = value;
  }

  /** Copy {@code length} bytes (max 16) from {@code src[srcOffset..]}. */
  public void setAccountCode(final byte[] src, final int srcOffset, final int length) {
    final int copy = Math.min(length, accountCode.length);
    System.arraycopy(src, srcOffset, accountCode, 0, copy);
    this.accountCodeLength = copy;
  }

  public void setAcctIdSource(final AcctIDSourceEnum value) {
    this.acctIdSource = value;
  }

  /** Copy {@code length} bytes (max 64) from {@code src[srcOffset..]}. */
  public void setAccountName(final byte[] src, final int srcOffset, final int length) {
    final int copy = Math.min(length, accountName.length);
    System.arraycopy(src, srcOffset, accountName, 0, copy);
    this.accountNameLength = copy;
  }

  public void setAccountType(final AccountTypeEnum value) {
    this.accountType = value;
  }

  /** Copy 3 bytes from {@code src[srcOffset..]}. */
  public void setBaseCurrency(final byte[] src, final int srcOffset) {
    baseCurrency[0] = src[srcOffset];
    baseCurrency[1] = src[srcOffset + 1];
    baseCurrency[2] = src[srcOffset + 2];
  }

  public void setBaseCurrency(final byte b0, final byte b1, final byte b2) {
    baseCurrency[0] = b0;
    baseCurrency[1] = b1;
    baseCurrency[2] = b2;
  }

  public void setStatus(final AccountStatusEnum value) {
    this.status = value;
  }

  public void setComplianceStatus(final ComplianceStatusEnum value) {
    this.complianceStatus = value;
  }

  public void setCapabilities(final long value) {
    this.capabilities = value;
  }

  public void setTransactTime(final long value) {
    this.transactTime = value;
  }

  // ---------------------------------------------------------------------------
  // Accessors
  // ---------------------------------------------------------------------------

  public long accountId() {
    return accountId;
  }

  public long parentAccountId() {
    return parentAccountId;
  }

  public int accountCodeLength() {
    return accountCodeLength;
  }

  public byte accountCodeByte(final int index) {
    return accountCode[index];
  }

  /** Copy live account code bytes into {@code dst[dstOffset..]}. Returns bytes copied. */
  public int copyAccountCodeTo(final byte[] dst, final int dstOffset) {
    System.arraycopy(accountCode, 0, dst, dstOffset, accountCodeLength);
    return accountCodeLength;
  }

  public AcctIDSourceEnum acctIdSource() {
    return acctIdSource;
  }

  public int accountNameLength() {
    return accountNameLength;
  }

  public byte accountNameByte(final int index) {
    return accountName[index];
  }

  public int copyAccountNameTo(final byte[] dst, final int dstOffset) {
    System.arraycopy(accountName, 0, dst, dstOffset, accountNameLength);
    return accountNameLength;
  }

  public AccountTypeEnum accountType() {
    return accountType;
  }

  public byte baseCurrencyByte(final int index) {
    return baseCurrency[index];
  }

  public void copyBaseCurrencyTo(final byte[] dst, final int dstOffset) {
    dst[dstOffset] = baseCurrency[0];
    dst[dstOffset + 1] = baseCurrency[1];
    dst[dstOffset + 2] = baseCurrency[2];
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

  public boolean canTrade() {
    return (capabilities & Capabilities.CAN_TRADE) != 0L;
  }

  public boolean canRequestQuotes() {
    return (capabilities & Capabilities.CAN_RFQ) != 0L;
  }

  public long transactTime() {
    return transactTime;
  }
}
