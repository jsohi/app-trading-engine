package com.trading.engine.websocket;

/**
 * Packs and unpacks 16-byte SBE {@code char[16]} account codes into two {@code long} values for
 * zero-allocation comparison on the drain hot path.
 *
 * <p>Mirrors {@link com.trading.engine.projections.SymbolPacker} but handles 16-byte fields (FIX
 * tag 1, Account) instead of 8-byte fields (FIX tag 55, Symbol). The 16-byte account code is split
 * into two 8-byte halves: {@code high} (bytes 0-7) and {@code low} (bytes 8-15), each packed into a
 * {@code long} using little-endian byte order matching SBE wire encoding.
 *
 * <p><b>Threading.</b> All methods are stateless static. Thread-safe.
 *
 * <p><b>Allocation.</b> All methods are zero-allocation. {@link #packHigh(byte[], int)} and {@link
 * #packLow(byte[], int)} pack from raw bytes. {@link #pack(String, long[])} packs directly from
 * String chars into the output array without intermediate byte arrays.
 *
 * @see AccountExtractor
 */
public final class AccountPacker {

  /** Account code field length in bytes (SBE char[16]). */
  public static final int ACCOUNT_CODE_LENGTH = 16;

  private AccountPacker() {}

  /**
   * Pack bytes 0-7 of a 16-byte account code into a {@code long} (little-endian).
   *
   * <p>Zero-allocation — suitable for drain hot path.
   *
   * @param src the source byte array containing the SBE message
   * @param offset the absolute byte offset of the account code field within {@code src}
   * @return the packed high half
   */
  public static long packHigh(final byte[] src, final int offset) {
    return (src[offset] & 0xFFL)
        | ((src[offset + 1] & 0xFFL) << 8)
        | ((src[offset + 2] & 0xFFL) << 16)
        | ((src[offset + 3] & 0xFFL) << 24)
        | ((src[offset + 4] & 0xFFL) << 32)
        | ((src[offset + 5] & 0xFFL) << 40)
        | ((src[offset + 6] & 0xFFL) << 48)
        | ((src[offset + 7] & 0xFFL) << 56);
  }

  /**
   * Pack bytes 8-15 of a 16-byte account code into a {@code long} (little-endian).
   *
   * <p>Zero-allocation — suitable for drain hot path.
   *
   * @param src the source byte array containing the SBE message
   * @param offset the absolute byte offset of the account code field within {@code src} (same
   *     offset as {@link #packHigh} — this method reads from {@code offset + 8})
   * @return the packed low half
   */
  public static long packLow(final byte[] src, final int offset) {
    final int lo = offset + 8;
    return (src[lo] & 0xFFL)
        | ((src[lo + 1] & 0xFFL) << 8)
        | ((src[lo + 2] & 0xFFL) << 16)
        | ((src[lo + 3] & 0xFFL) << 24)
        | ((src[lo + 4] & 0xFFL) << 32)
        | ((src[lo + 5] & 0xFFL) << 40)
        | ((src[lo + 6] & 0xFFL) << 48)
        | ((src[lo + 7] & 0xFFL) << 56);
  }

  /**
   * Pack a String account code into two longs for use in entitlement storage. NUL-pads to 16 bytes
   * to match SBE wire encoding, ensuring packed values from this method equal packed values
   * extracted from SBE payloads.
   *
   * <p><b>Allocation.</b> Zero-allocation — packs directly from String chars into the output array.
   *
   * @param accountCode the account code string (1-16 ASCII characters)
   * @param out a pre-allocated {@code long[2]} array; {@code out[0]} receives high, {@code out[1]}
   *     receives low
   * @throws IllegalArgumentException if accountCode is null, empty, longer than 16 chars, or
   *     contains non-ASCII characters
   */
  public static void pack(final String accountCode, final long[] out) {
    if (out == null || out.length < 2) {
      throw new IllegalArgumentException("out array must be non-null with length >= 2");
    }
    if (accountCode == null || accountCode.isEmpty()) {
      throw new IllegalArgumentException("accountCode must not be null or empty");
    }
    final int len = accountCode.length();
    if (len > ACCOUNT_CODE_LENGTH) {
      throw new IllegalArgumentException("accountCode exceeds 16 characters: " + len);
    }

    // Pack directly from String chars into two longs — zero allocation.
    // Each char is validated as ASCII (0x00-0x7F) and placed at its little-endian bit position.
    // Chars beyond the string length are implicitly NUL (0x00) via the initial value of 0L,
    // matching SBE char[16] NUL-padding on the wire.
    long high = 0L;
    long low = 0L;
    for (int i = 0; i < len; i++) {
      final char c = accountCode.charAt(i);
      if (c > 0x7F) {
        throw new IllegalArgumentException(
            "accountCode contains non-ASCII character at index " + i);
      }
      if (i < 8) {
        high |= (c & 0xFFL) << (i * 8);
      } else {
        low |= (c & 0xFFL) << ((i - 8) * 8);
      }
    }
    out[0] = high;
    out[1] = low;
  }
}
