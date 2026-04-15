package com.trading.engine.testsupport.buffer;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Utilities for constructing fixed-width SBE field values in tests.
 *
 * <p>SBE schema fields like Symbol (8 bytes, space-padded) and ClOrdID (20 bytes, zero-padded)
 * require specific padding. These helpers eliminate the 4+ duplicated {@code rightPadSymbol} /
 * {@code padBytes} methods scattered across test files.
 *
 * <p>Thread-safe — all methods are pure functions with no mutable state.
 *
 * <p>Allocates byte arrays on every call. Test infrastructure only.
 */
public final class SbeFieldUtil {

  /** SBE Symbol field length (8 bytes, space-padded per FIX convention). */
  public static final int SYMBOL_LENGTH = 8;

  private SbeFieldUtil() {}

  /**
   * Right-pads a string with zero bytes to the specified length. Used for SBE fixed-length char
   * fields (ClOrdID, OrderID, ExecID).
   *
   * @param value source string (ASCII); may be {@code null} (treated as empty)
   * @param len target byte array length; must be &gt; 0
   * @return zero-padded byte array of exactly {@code len} bytes
   */
  public static byte[] zeroPad(final String value, final int len) {
    final byte[] result = new byte[len];
    if (value != null) {
      final byte[] src = value.getBytes(StandardCharsets.US_ASCII);
      System.arraycopy(src, 0, result, 0, Math.min(src.length, len));
    }
    return result;
  }

  /**
   * Right-pads a string with space bytes (0x20) to the specified length. Used for SBE Symbol fields
   * (8 bytes, space-padded per FIX convention).
   *
   * @param value source string (ASCII); may be {@code null} (treated as empty)
   * @param len target byte array length; must be &gt; 0
   * @return space-padded byte array of exactly {@code len} bytes
   */
  public static byte[] spacePad(final String value, final int len) {
    final byte[] result = new byte[len];
    Arrays.fill(result, (byte) ' ');
    if (value != null) {
      final byte[] src = value.getBytes(StandardCharsets.US_ASCII);
      System.arraycopy(src, 0, result, 0, Math.min(src.length, len));
    }
    return result;
  }

  /**
   * Wraps a string as an {@link UnsafeBuffer} for SBE {@code DirectBuffer} fields.
   *
   * @param value ASCII string to wrap; must not be {@code null}
   * @return buffer wrapping the string's byte representation
   */
  public static UnsafeBuffer wrapAscii(final String value) {
    return new UnsafeBuffer(value.getBytes(StandardCharsets.US_ASCII));
  }

  /**
   * Space-pads a symbol to {@value #SYMBOL_LENGTH} bytes and wraps as {@link UnsafeBuffer}.
   * Convenience for the most common fixed-width field pattern in tests.
   *
   * @param symbol instrument symbol (FIX tag 55); max {@value #SYMBOL_LENGTH} ASCII characters
   * @return {@value #SYMBOL_LENGTH}-byte space-padded buffer
   */
  public static UnsafeBuffer wrapSymbol(final String symbol) {
    return new UnsafeBuffer(spacePad(symbol, SYMBOL_LENGTH));
  }
}
