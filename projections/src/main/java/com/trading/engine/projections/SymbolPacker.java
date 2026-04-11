package com.trading.engine.projections;

import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import org.agrona.DirectBuffer;

/**
 * Packs an 8-byte SBE {@code Symbol} (FIX tag 55, char[8]) into a {@code long} for use as a
 * primitive map key in {@link org.agrona.collections.Long2ObjectHashMap}.
 *
 * <p><b>Encoding:</b> little-endian, matching the SBE schema's {@code byteOrder="littleEndian"}.
 * Symbols shorter than 8 characters are NUL-padded on the right by SBE, so the packed long is
 * unique per distinct symbol string.
 *
 * <p><b>Threading:</b> all methods are stateless and thread-safe.
 *
 * <p><b>Allocation:</b> zero allocation on the {@link #pack(DirectBuffer, int)} hot path. The
 * {@link #pack(String)} overload allocates a temporary byte array (query path only).
 */
public final class SymbolPacker {

  /** SBE Symbol field length in bytes. */
  public static final int SYMBOL_LENGTH = 8;

  private SymbolPacker() {}

  /**
   * Packs 8 bytes from a {@link DirectBuffer} into a little-endian {@code long}.
   *
   * <p>Zero allocation — suitable for the event-dispatch hot path.
   *
   * @param buffer the buffer containing the SBE Symbol field
   * @param offset the offset of the first byte of the Symbol field
   * @return the packed symbol as a little-endian long
   */
  public static long pack(final DirectBuffer buffer, final int offset) {
    return buffer.getLong(offset, ByteOrder.LITTLE_ENDIAN);
  }

  /**
   * Packs 8 bytes from a byte array into a little-endian {@code long}.
   *
   * @param src the byte array containing the symbol bytes
   * @param offset the offset of the first byte
   * @return the packed symbol as a little-endian long
   */
  public static long pack(final byte[] src, final int offset) {
    // Manual little-endian packing to avoid UnsafeBuffer allocation
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
   * Packs a symbol string into a little-endian {@code long}. The string is NUL-padded to 8 bytes if
   * shorter, matching SBE wire encoding.
   *
   * <p>Allocates a temporary byte array — suitable for query paths only, not event dispatch.
   *
   * @param symbol the symbol string (must be 1–8 ASCII characters)
   * @return the packed symbol as a little-endian long
   * @throws IllegalArgumentException if the symbol is null, empty, or longer than 8 characters
   */
  public static long pack(final String symbol) {
    if (symbol == null || symbol.isEmpty() || symbol.length() > SYMBOL_LENGTH) {
      throw new IllegalArgumentException("Symbol must be 1-8 ASCII characters, got: " + symbol);
    }
    for (int i = 0; i < symbol.length(); i++) {
      if (symbol.charAt(i) > 0x7F) {
        throw new IllegalArgumentException("Symbol must be 1-8 ASCII characters, got: " + symbol);
      }
    }
    final byte[] padded = new byte[SYMBOL_LENGTH];
    final byte[] ascii = symbol.getBytes(StandardCharsets.US_ASCII);
    System.arraycopy(ascii, 0, padded, 0, ascii.length);
    // Remaining bytes are already NUL (0x00) from array initialization
    return pack(padded, 0);
  }

  /**
   * Unpacks a little-endian {@code long} back to a trimmed ASCII symbol string. The reverse of
   * {@link #pack(String)}.
   *
   * <p>Allocates a {@link String} — suitable for query paths only, not event dispatch.
   *
   * @param packed the packed symbol long
   * @return the trimmed symbol string (trailing NUL bytes removed)
   */
  public static String unpack(final long packed) {
    final byte[] bytes = new byte[SYMBOL_LENGTH];
    for (int i = 0; i < SYMBOL_LENGTH; i++) {
      bytes[i] = (byte) ((packed >>> (i * 8)) & 0xFF);
    }
    int end = SYMBOL_LENGTH;
    while (end > 0 && bytes[end - 1] == 0) {
      end--;
    }
    return end == 0 ? "" : new String(bytes, 0, end, StandardCharsets.US_ASCII);
  }
}
