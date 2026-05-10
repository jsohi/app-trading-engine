package com.trading.engine.fixbridge.json;

import io.netty.buffer.ByteBuf;

/**
 * Shared UTF-8 / ASCII JSON-string-value emitter used by the outbound writers.
 *
 * <p><b>Purpose.</b> Writes a JSON string value (the bytes between the surrounding {@code "}
 * delimiters — callers handle the quote bytes themselves) into a Netty {@link ByteBuf}, validating
 * each code unit per the locked wire-protocol contract: pure 7-bit-printable ASCII plus the
 * defensive multi-byte UTF-8 fallback for safety. Forbidden characters trigger {@link
 * IllegalStateException}; the caller is responsible for any rollback semantics.
 *
 * <p><b>Threading.</b> Stateless — every method is {@code static}. Safe to share across threads.
 *
 * <p><b>Allocation.</b> Zero on every call: the implementation iterates the supplied {@link String}
 * or {@code char[]} a {@code char} at a time and writes each as a UTF-8 byte sequence directly into
 * the destination buffer. No intermediate {@code byte[]}, no {@code String#getBytes}.
 *
 * <p><b>Lifecycle.</b> Class-init only.
 *
 * <p><b>Dependencies.</b> Netty {@link ByteBuf} (write-side API).
 *
 * <p><b>Validation.</b>
 *
 * <ul>
 *   <li>Rejects {@code "} (0x22) and {@code \\} (0x5C) — wire protocol forbids JSON escapes; the
 *       upstream is expected to have sanitised these (e.g. FIX text fields with {@code SOH}
 *       stripped).
 *   <li>Rejects code points in {@code 0x00..0x1F} (control bytes) which JSON forbids unescaped.
 *   <li>Rejects unpaired surrogate code units — the wire protocol is ASCII-only so surrogates
 *       cannot legitimately appear.
 *   <li>The multi-byte UTF-8 branches are present for defence-in-depth; the locked wire protocol
 *       constrains every legitimate value to 7-bit ASCII.
 * </ul>
 *
 * <p>Used by {@link BrowserEventWriter} (browser-event JSON writer) and by {@code
 * com.trading.engine.fixbridge.translator.FixToJsonTranslator} (FIX→JSON translator) — extracted to
 * eliminate duplication between the two emitters.
 */
public final class Utf8JsonStringEmitter {

  private Utf8JsonStringEmitter() {}

  /**
   * Append the contents of {@code s} as a UTF-8 byte sequence into {@code dst}, NOT including the
   * surrounding quotes (callers handle those).
   *
   * @param s string value to emit (must not be null)
   * @param dst destination buffer; bytes are appended at {@code dst.writerIndex()}
   * @throws IllegalArgumentException if {@code s} is null or contains a forbidden character
   */
  public static void appendStringValue(final String s, final ByteBuf dst) {
    if (s == null) {
      throw new IllegalArgumentException("null string value");
    }
    final int len = s.length();
    for (int i = 0; i < len; i++) {
      final char c = s.charAt(i);
      validateChar(c, i);
      writeUtf8(c, i, dst);
    }
  }

  /**
   * Append {@code chars[off..off+len)} into {@code dst} as UTF-8 / ASCII, validating each character
   * is JSON-safe (no embedded {@code "}, {@code \\}, or control bytes).
   *
   * @param chars source char array
   * @param off slice offset (inclusive)
   * @param len slice length
   * @param dst destination buffer
   * @throws IllegalStateException if any character is forbidden by the wire-protocol contract
   */
  public static void appendCharSlice(
      final char[] chars, final int off, final int len, final ByteBuf dst) {
    for (int i = 0; i < len; i++) {
      final char c = chars[off + i];
      validateCharStrict(c, i);
      writeUtf8Strict(c, i, dst);
    }
  }

  /**
   * Append the ASCII bytes in {@code src[off..off+len)} into {@code dst} as a JSON-safe string
   * value, substituting SOH (0x01) with {@code |} (0x7C) and rejecting embedded {@code "} (0x22) or
   * {@code \\} (0x5C).
   *
   * <p>This is the zero-alloc hot-path variant used by {@link
   * com.trading.engine.fixbridge.json.BrowserEventWriter#writeRawFixSlice} to emit a {@link
   * com.trading.engine.fixbridge.json.BrowserEvent.RawFixSlice} without constructing an
   * intermediate {@link String}. SOH bytes originate from the masked FIX wire; all other bytes must
   * already be 7-bit ASCII printable (PiiMask guarantees this for masked fields; FIX field values
   * are constrained to printable ASCII by the FIX 4.4 specification).
   *
   * <p>Zero allocation after construction.
   *
   * @param src source byte array containing masked FIX bytes (7-bit ASCII + SOH)
   * @param off start offset within {@code src}
   * @param len number of bytes to emit; must be {@code >= 0}
   * @param dst destination Netty {@link ByteBuf}; written at {@code dst.writerIndex()}
   * @throws IllegalStateException if any byte is a forbidden JSON character ({@code "} or {@code
   *     \\})
   */
  public static void appendRawFixBytes(
      final byte[] src, final int off, final int len, final ByteBuf dst) {
    for (int i = 0; i < len; i++) {
      final byte b = src[off + i];
      if (b == (byte) 0x01) {
        // SOH field delimiter → substitute with '|' as per the locked wire-format contract.
        dst.writeByte((byte) '|');
      } else if (b == (byte) '"' || b == (byte) '\\') {
        throw new IllegalStateException(
            "FIX byte slice contains forbidden JSON character at index " + i + ": " + (int) b);
      } else {
        // All other bytes are written verbatim; PiiMask + FIX 4.4 protocol constrain the input
        // to 7-bit printable ASCII, so no multi-byte UTF-8 branching is needed here.
        dst.writeByte(b);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Internals.
  // ---------------------------------------------------------------------------

  /** Throws {@link IllegalArgumentException} on forbidden chars (used by {@link String} path). */
  private static void validateChar(final char c, final int index) {
    if (c == '"' || c == '\\') {
      throw new IllegalArgumentException(
          "string value contains forbidden character at index " + index + ": " + (int) c);
    }
    if (c < 0x20) {
      throw new IllegalArgumentException(
          "string value contains control character at index " + index + ": " + (int) c);
    }
  }

  /**
   * Throws {@link IllegalStateException} on forbidden chars (used by char[] path which originates
   * from FIX gateway upstream — a violation indicates an upstream bug).
   */
  private static void validateCharStrict(final char c, final int index) {
    if (c == '"' || c == '\\') {
      throw new IllegalStateException(
          "string value contains forbidden character at index " + index + ": " + (int) c);
    }
    if (c < 0x20) {
      throw new IllegalStateException(
          "string value contains control character at index " + index + ": " + (int) c);
    }
  }

  /**
   * UTF-8 encode a {@code char} into {@code dst}. {@link IllegalArgumentException} on surrogate.
   */
  private static void writeUtf8(final char c, final int index, final ByteBuf dst) {
    if (c < 0x80) {
      dst.writeByte((byte) c);
    } else if (c < 0x800) {
      dst.writeByte((byte) (0xC0 | (c >>> 6)));
      dst.writeByte((byte) (0x80 | (c & 0x3F)));
    } else {
      if (Character.isSurrogate(c)) {
        throw new IllegalArgumentException(
            "string value contains surrogate code unit at index " + index);
      }
      dst.writeByte((byte) (0xE0 | (c >>> 12)));
      dst.writeByte((byte) (0x80 | ((c >>> 6) & 0x3F)));
      dst.writeByte((byte) (0x80 | (c & 0x3F)));
    }
  }

  /** UTF-8 encode a {@code char} into {@code dst}. {@link IllegalStateException} on surrogate. */
  private static void writeUtf8Strict(final char c, final int index, final ByteBuf dst) {
    if (c < 0x80) {
      dst.writeByte((byte) c);
    } else if (c < 0x800) {
      dst.writeByte((byte) (0xC0 | (c >>> 6)));
      dst.writeByte((byte) (0x80 | (c & 0x3F)));
    } else {
      if (Character.isSurrogate(c)) {
        throw new IllegalStateException(
            "string value contains surrogate code unit at index " + index);
      }
      dst.writeByte((byte) (0xE0 | (c >>> 12)));
      dst.writeByte((byte) (0x80 | ((c >>> 6) & 0x3F)));
      dst.writeByte((byte) (0x80 | (c & 0x3F)));
    }
  }
}
