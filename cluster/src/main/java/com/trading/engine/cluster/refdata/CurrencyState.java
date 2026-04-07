package com.trading.engine.cluster.refdata;

import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.CurrencyClassEnum;

/**
 * Mutable in-memory representation of one ISO 4217 currency held by {@link CurrencyStore}.
 * Pre-allocated and stored by reference inside the store's primary map; the store mutates the same
 * instance on upsert (no per-update allocation).
 *
 * <p><b>What this carries (and what it deliberately does NOT).</b> Pure ISO 4217 master data + a
 * class enum for FIAT/METAL/CRYPTO/FUND. <b>Pip definition</b> and <b>default settlement days</b>
 * are intentionally absent — they are properties of currency <i>pairs</i> (EUR/USD pip = 0.0001 vs
 * USD/JPY pip = 0.01) and live on a future {@code CurrencyPairStore}. Treating them as per-currency
 * would bake in an architectural error that production FX engines (EBS, Refinitiv, Hotspot, CME FX)
 * all avoid.
 */
public final class CurrencyState {

  /** ISO 4217 alpha-3 code (uppercase ASCII), e.g. "USD". 3 bytes, no null terminator. */
  private final byte[] ccyCode = new byte[3];

  /** ISO 4217 numeric code (e.g. USD=840, JPY=392). uint16 widened to int. */
  private int isoNumeric;

  /** Display name (e.g. "United States Dollar"). Up to 64 ASCII bytes (Text type). */
  private final byte[] name = new byte[64];

  private int nameLength;

  /** Minor unit decimals (USD=2, JPY=0, KWD=3). uint8 widened to int. */
  private int decimals;

  /** FIAT / METAL / CRYPTO / FUND. */
  private CurrencyClassEnum currencyClass = CurrencyClassEnum.Fiat;

  /** Active / Suspended / Closed (reuses {@link AccountStatusEnum}). */
  private AccountStatusEnum status = AccountStatusEnum.Active;

  /** Original LoadCurrency transactTime (epoch nanos). */
  private long transactTime;

  // ---------------------------------------------------------------------------
  // Mutators (called from LoadCurrencyHandler / restoreFrom; never on the hot path)
  // ---------------------------------------------------------------------------

  /** Copy {@code length} bytes (must be exactly 3) from {@code src[srcOffset..]}. */
  public void setCcyCode(final byte[] src, final int srcOffset) {
    ccyCode[0] = src[srcOffset];
    ccyCode[1] = src[srcOffset + 1];
    ccyCode[2] = src[srcOffset + 2];
  }

  /**
   * Set the 3-byte code from individual bytes — zero allocation (no array argument). Used by
   * loaders on the hot path to avoid allocating a 3-byte array per upsert.
   */
  public void setCcyCode(final byte b0, final byte b1, final byte b2) {
    ccyCode[0] = b0;
    ccyCode[1] = b1;
    ccyCode[2] = b2;
  }

  public void setIsoNumeric(final int value) {
    this.isoNumeric = value;
  }

  /**
   * Copy up to {@code length} bytes from {@code src[srcOffset..]} into the name buffer. The
   * recorded length is {@code min(length, name.length)}; bytes beyond the recorded length are
   * undefined.
   */
  public void setName(final byte[] src, final int srcOffset, final int length) {
    final int copyLength = Math.min(length, name.length);
    System.arraycopy(src, srcOffset, name, 0, copyLength);
    this.nameLength = copyLength;
  }

  public void setDecimals(final int value) {
    this.decimals = value;
  }

  public void setCurrencyClass(final CurrencyClassEnum value) {
    this.currencyClass = value;
  }

  public void setStatus(final AccountStatusEnum value) {
    this.status = value;
  }

  public void setTransactTime(final long value) {
    this.transactTime = value;
  }

  // ---------------------------------------------------------------------------
  // Accessors
  // ---------------------------------------------------------------------------

  public byte ccyCodeByte(final int index) {
    return ccyCode[index];
  }

  /** Copy the 3-byte code into {@code dst[dstOffset..dstOffset+3]}. */
  public void copyCcyCodeTo(final byte[] dst, final int dstOffset) {
    dst[dstOffset] = ccyCode[0];
    dst[dstOffset + 1] = ccyCode[1];
    dst[dstOffset + 2] = ccyCode[2];
  }

  public int isoNumeric() {
    return isoNumeric;
  }

  /** Copy the live name bytes into {@code dst[dstOffset..]}. Returns the number of bytes copied. */
  public int copyNameTo(final byte[] dst, final int dstOffset) {
    System.arraycopy(name, 0, dst, dstOffset, nameLength);
    return nameLength;
  }

  public int nameLength() {
    return nameLength;
  }

  public byte nameByte(final int index) {
    return name[index];
  }

  public int decimals() {
    return decimals;
  }

  public CurrencyClassEnum currencyClass() {
    return currencyClass;
  }

  public AccountStatusEnum status() {
    return status;
  }

  public long transactTime() {
    return transactTime;
  }
}
