package com.trading.engine.cluster.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link RfqRejectMessages}.
 *
 * <p>Verifies that every pre-allocated {@code byte[]} constant:
 *
 * <ul>
 *   <li>is exactly {@link RfqRejectMessages#MAX_TEXT_LEN} bytes long,
 *   <li>contains only 7-bit ASCII (0-127) or NUL (0) bytes,
 *   <li>is NUL-padded after its ASCII content (no non-NUL bytes after the first NUL).
 * </ul>
 *
 * <p>Also verifies that {@link RfqRejectMessages#pad} throws {@link IllegalArgumentException} for a
 * message that exceeds the 64-byte limit.
 *
 * <p><b>Threading:</b> single-threaded tests.
 */
class RfqRejectMessagesTest {

  // ---------------------------------------------------------------------------
  // Reflection helper: collect all public static byte[] fields from RfqRejectMessages
  // ---------------------------------------------------------------------------

  /**
   * Returns all {@code public static byte[]} field values from {@link RfqRejectMessages}. Uses
   * reflection so new constants added to the class are automatically picked up by every test here
   * — no manual enumeration required.
   */
  private static List<byte[]> allConstants() throws IllegalAccessException {
    final var result = new ArrayList<byte[]>();
    for (final Field field : RfqRejectMessages.class.getDeclaredFields()) {
      if (field.getType() == byte[].class
          && java.lang.reflect.Modifier.isStatic(field.getModifiers())
          && java.lang.reflect.Modifier.isPublic(field.getModifiers())) {
        result.add((byte[]) field.get(null));
      }
    }
    return result;
  }

  // ---------------------------------------------------------------------------
  // Length invariant: every constant must be exactly 64 bytes
  // ---------------------------------------------------------------------------

  /**
   * Reflectively iterates every {@code public static byte[]} field in {@link RfqRejectMessages}
   * and asserts that its length is exactly {@link RfqRejectMessages#MAX_TEXT_LEN} (64). The test
   * skips the non-array field {@code MAX_TEXT_LEN} because reflection filters to {@code byte[]}.
   */
  @Test
  void everyConstant_isExactly64BytesAscii() throws IllegalAccessException {
    final var constants = allConstants();
    assertTrue(constants.size() >= 15,
        "expected at least 15 byte[] constants (got " + constants.size() + ")");

    for (final byte[] constant : constants) {
      assertEquals(
          RfqRejectMessages.MAX_TEXT_LEN,
          constant.length,
          "constant must be exactly MAX_TEXT_LEN=" + RfqRejectMessages.MAX_TEXT_LEN + " bytes");

      // All bytes must be 7-bit ASCII (0..127); NUL (0) is allowed as padding.
      for (int i = 0; i < constant.length; i++) {
        final int b = constant[i] & 0xFF;
        assertTrue(b <= 127,
            "byte at index " + i + " is 0x" + Integer.toHexString(b) + " > 127 (not ASCII)");
      }
    }
  }

  // ---------------------------------------------------------------------------
  // NUL-padding invariant: no non-NUL byte may appear after the first NUL
  // ---------------------------------------------------------------------------

  /**
   * After the ASCII content ends (first NUL byte), every remaining byte in the constant must also
   * be NUL. This invariant guarantees the constant can be written directly into the SBE
   * {@code char[64]} {@code text} field without additional padding.
   */
  @Test
  void everyConstant_isNulPaddedAfterContent() throws IllegalAccessException {
    final var constants = allConstants();

    for (final byte[] constant : constants) {
      // Find the first NUL byte.
      int firstNul = -1;
      for (int i = 0; i < constant.length; i++) {
        if (constant[i] == 0) {
          firstNul = i;
          break;
        }
      }
      if (firstNul == -1) {
        // Constant fills the entire 64 bytes with non-NUL — acceptable if content is exactly 64.
        continue;
      }
      // Every byte after firstNul must also be NUL.
      for (int i = firstNul + 1; i < constant.length; i++) {
        assertEquals(
            (byte) 0,
            constant[i],
            "byte at index " + i + " is non-NUL after first NUL at index " + firstNul);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // pad() rejects strings exceeding MAX_TEXT_LEN
  // ---------------------------------------------------------------------------

  /**
   * Passing a string longer than {@link RfqRejectMessages#MAX_TEXT_LEN} to
   * {@link RfqRejectMessages#pad} must throw {@link IllegalArgumentException}.
   */
  @Test
  void pad_messageExceedingLimit_throwsIllegalArgumentException() {
    // Build a string that is exactly one character too long.
    final var tooLong = "A".repeat(RfqRejectMessages.MAX_TEXT_LEN + 1);

    final var ex = assertThrows(
        IllegalArgumentException.class,
        () -> RfqRejectMessages.pad(tooLong),
        "pad() must throw IllegalArgumentException for input longer than MAX_TEXT_LEN");

    assertTrue(
        ex.getMessage().contains("MAX_TEXT_LEN"),
        "exception message should mention MAX_TEXT_LEN, was: " + ex.getMessage());
  }

  // ---------------------------------------------------------------------------
  // pad() succeeds for a string of exactly MAX_TEXT_LEN characters
  // ---------------------------------------------------------------------------

  /**
   * A message of exactly {@link RfqRejectMessages#MAX_TEXT_LEN} ASCII characters must succeed and
   * produce a byte[] of length {@code MAX_TEXT_LEN} with no padding required.
   */
  @Test
  void pad_exactLimitMessage_producesFullArray() {
    final var exact = "X".repeat(RfqRejectMessages.MAX_TEXT_LEN);
    final byte[] result = RfqRejectMessages.pad(exact);
    assertEquals(RfqRejectMessages.MAX_TEXT_LEN, result.length);
    for (int i = 0; i < result.length; i++) {
      assertEquals((byte) 'X', result[i], "every byte should be 'X' at index " + i);
    }
  }

  // ---------------------------------------------------------------------------
  // spot-check two well-known constants for human-readable content
  // ---------------------------------------------------------------------------

  /**
   * {@link RfqRejectMessages#RATE_LIMIT} must start with ASCII bytes for "rate limit" and be
   * NUL-padded to 64 bytes.
   */
  @Test
  void rateLimit_startsWithExpectedAscii() {
    final byte[] expected = "rate limit".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], RfqRejectMessages.RATE_LIMIT[i],
          "RATE_LIMIT byte mismatch at index " + i);
    }
    // Remainder must be NUL.
    for (int i = expected.length; i < RfqRejectMessages.MAX_TEXT_LEN; i++) {
      assertEquals((byte) 0, RfqRejectMessages.RATE_LIMIT[i],
          "RATE_LIMIT must be NUL-padded after content at index " + i);
    }
  }

  /** {@link RfqRejectMessages#POOL_EXHAUSTED} must start with "pool exhausted". */
  @Test
  void poolExhausted_startsWithExpectedAscii() {
    final byte[] expected = "pool exhausted".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], RfqRejectMessages.POOL_EXHAUSTED[i],
          "POOL_EXHAUSTED byte mismatch at index " + i);
    }
  }
}
