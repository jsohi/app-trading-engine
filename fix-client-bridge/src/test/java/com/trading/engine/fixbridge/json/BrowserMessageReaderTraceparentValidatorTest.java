package com.trading.engine.fixbridge.json;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link BrowserMessageReader#isValidTraceparent(byte[], int, int)}.
 *
 * <p>Covers the W3C trace-context v00 layout: {@code HH-HHHH...H(32)-HHHH...H(16)-HH} — total 55
 * bytes, dashes at positions 2, 35, and 52, all hex segments lowercase {@code [0-9a-f]}.
 *
 * <p>Threading: stateless — all assertions are pure functional. Tests are independent.
 */
final class BrowserMessageReaderTraceparentValidatorTest {

  /** Canonical valid traceparent from the W3C trace-context spec examples. */
  private static final String VALID = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

  private static byte[] ascii(final String s) {
    return s.getBytes(StandardCharsets.US_ASCII);
  }

  // ---------------------------------------------------------------------------
  // Valid inputs.
  // ---------------------------------------------------------------------------

  @Test
  void isValidTraceparent_valid_returnsTrue() {
    final byte[] buf = ascii(VALID);
    assertTrue(BrowserMessageReader.isValidTraceparent(buf, 0, buf.length));
  }

  @Test
  void isValidTraceparent_validAtOffset0_returnsTrue() {
    // Explicit offset=0 — same as the previous test; belt-and-suspenders.
    final byte[] buf = ascii(VALID);
    assertTrue(BrowserMessageReader.isValidTraceparent(buf, 0, 55));
  }

  @Test
  void isValidTraceparent_validAtOffset100_returnsTrue() {
    // The function MUST honor the supplied offset — place the traceparent at byte 100.
    final byte[] buf = new byte[200];
    final byte[] value = ascii(VALID);
    System.arraycopy(value, 0, buf, 100, value.length);
    assertTrue(BrowserMessageReader.isValidTraceparent(buf, 100, 55));
  }

  // ---------------------------------------------------------------------------
  // Wrong lengths.
  // ---------------------------------------------------------------------------

  @Test
  void isValidTraceparent_length54_returnsFalse() {
    final byte[] buf = ascii(VALID.substring(0, 54));
    assertFalse(BrowserMessageReader.isValidTraceparent(buf, 0, 54));
  }

  @Test
  void isValidTraceparent_length56_returnsFalse() {
    final byte[] buf = ascii(VALID + "0");
    assertFalse(BrowserMessageReader.isValidTraceparent(buf, 0, 56));
  }

  @Test
  void isValidTraceparent_length0_returnsFalse() {
    assertFalse(BrowserMessageReader.isValidTraceparent(new byte[0], 0, 0));
  }

  // ---------------------------------------------------------------------------
  // Missing dashes — each of the three expected dash positions replaced.
  // ---------------------------------------------------------------------------

  @Test
  void isValidTraceparent_dashAtPosition2Replaced_returnsFalse() {
    // Replace '-' at index 2 with 'a' (a valid hex digit, but not a dash).
    final char[] chars = VALID.toCharArray();
    chars[2] = 'a';
    assertFalse(BrowserMessageReader.isValidTraceparent(ascii(new String(chars)), 0, 55));
  }

  @Test
  void isValidTraceparent_dashAtPosition35Replaced_returnsFalse() {
    final char[] chars = VALID.toCharArray();
    chars[35] = 'b';
    assertFalse(BrowserMessageReader.isValidTraceparent(ascii(new String(chars)), 0, 55));
  }

  @Test
  void isValidTraceparent_dashAtPosition52Replaced_returnsFalse() {
    final char[] chars = VALID.toCharArray();
    chars[52] = 'c';
    assertFalse(BrowserMessageReader.isValidTraceparent(ascii(new String(chars)), 0, 55));
  }

  // ---------------------------------------------------------------------------
  // Non-hex characters in segments.
  // ---------------------------------------------------------------------------

  @Test
  void isValidTraceparent_nonHexInVersion_returnsFalse() {
    // Replace first char '0' with 'g' — not in [0-9a-f].
    final char[] chars = VALID.toCharArray();
    chars[0] = 'g';
    assertFalse(BrowserMessageReader.isValidTraceparent(ascii(new String(chars)), 0, 55));
  }

  @Test
  void isValidTraceparent_uppercaseHexInVersion_returnsFalse() {
    // W3C spec requires lowercase only; 'A' in the version segment must reject.
    final char[] chars = VALID.toCharArray();
    chars[0] = 'A';
    assertFalse(BrowserMessageReader.isValidTraceparent(ascii(new String(chars)), 0, 55));
  }

  @Test
  void isValidTraceparent_uppercaseHexInTraceId_returnsFalse() {
    // Replace a lowercase hex char in the trace-id segment with its uppercase equivalent.
    // VALID[3..34] is the 32-hex trace-id. Index 3 = '0', index 4 = 'a' → 'A'.
    final char[] chars = VALID.toCharArray();
    chars[4] = 'A';
    assertFalse(BrowserMessageReader.isValidTraceparent(ascii(new String(chars)), 0, 55));
  }

  @Test
  void isValidTraceparent_nonHexInTraceId_returnsFalse() {
    // Replace a hex char in the trace-id segment with 'z'.
    final char[] chars = VALID.toCharArray();
    chars[10] = 'z';
    assertFalse(BrowserMessageReader.isValidTraceparent(ascii(new String(chars)), 0, 55));
  }

  @Test
  void isValidTraceparent_nonHexInParentId_returnsFalse() {
    // Parent-id segment starts at offset 36; replace first char with 'X'.
    final char[] chars = VALID.toCharArray();
    chars[36] = 'X';
    assertFalse(BrowserMessageReader.isValidTraceparent(ascii(new String(chars)), 0, 55));
  }

  @Test
  void isValidTraceparent_nonHexInFlags_returnsFalse() {
    // Flags segment starts at offset 53; replace with '!'.
    final char[] chars = VALID.toCharArray();
    chars[53] = '!';
    assertFalse(BrowserMessageReader.isValidTraceparent(ascii(new String(chars)), 0, 55));
  }

  @Test
  void isValidTraceparent_uppercaseHexInFlags_returnsFalse() {
    final char[] chars = VALID.toCharArray();
    // flags "01" → "0A" (uppercase)
    chars[54] = 'A';
    assertFalse(BrowserMessageReader.isValidTraceparent(ascii(new String(chars)), 0, 55));
  }
}
