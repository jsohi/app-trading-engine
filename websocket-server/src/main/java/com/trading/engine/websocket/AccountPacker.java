package com.trading.engine.websocket;

import java.nio.charset.StandardCharsets;

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
 * <p><b>Allocation.</b> {@link #packHigh(byte[], int)} and {@link #packLow(byte[], int)} are
 * zero-allocation. {@link #pack(String)} allocates a temporary byte array (auth-time only, not hot
 * path).
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
   * <p><b>Allocation.</b> Allocates a temporary byte array. Auth-time only, not hot path.
   *
   * @param accountCode the account code string (1-16 ASCII characters)
   * @param out a pre-allocated {@code long[2]} array; {@code out[0]} receives high, {@code out[1]}
   *     receives low
   * @throws IllegalArgumentException if accountCode is null, empty, longer than 16 chars, or
   *     contains non-ASCII characters
   */
  public static void pack(final String accountCode, final long[] out) {
    if (accountCode == null || accountCode.isEmpty()) {
      throw new IllegalArgumentException("accountCode must not be null or empty");
    }
    if (accountCode.length() > ACCOUNT_CODE_LENGTH) {
      throw new IllegalArgumentException(
          "accountCode exceeds 16 characters: " + accountCode.length());
    }

    // Validate ASCII — non-ASCII chars would be silently replaced with '?' by getBytes(US_ASCII),
    // producing packed values that differ from SBE wire encoding and breaking entitlement checks.
    for (int i = 0; i < accountCode.length(); i++) {
      if (accountCode.charAt(i) > 0x7F) {
        throw new IllegalArgumentException(
            "accountCode contains non-ASCII character at index " + i);
      }
    }

    // NUL-pad to 16 bytes to match SBE char[16] wire encoding
    final var padded = new byte[ACCOUNT_CODE_LENGTH];
    final var bytes = accountCode.getBytes(StandardCharsets.US_ASCII);
    System.arraycopy(bytes, 0, padded, 0, bytes.length);

    out[0] = packHigh(padded, 0);
    out[1] = packLow(padded, 0);
  }
}
