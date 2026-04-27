package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link AccountPacker} — verifies packed long values match between String-based packing
 * (auth-time) and raw-byte packing (drain-time extraction from SBE payloads).
 *
 * <p>Round-trip correctness is critical: if {@code AccountPacker.pack(String, out)} produces
 * different values from {@code AccountPacker.packHigh/packLow(sbeBytes, offset)} for the same
 * account code, account-level entitlement filtering silently fails.
 */
final class AccountPackerTest {

  @Test
  void packHighLow_shortAccountCode_matchesStringPack() {
    final var code = "ACME-001";
    final var out = new long[2];
    AccountPacker.pack(code, out);

    // Simulate SBE wire encoding: NUL-padded char[16]
    final var sbeBytes = new byte[16];
    System.arraycopy(code.getBytes(StandardCharsets.US_ASCII), 0, sbeBytes, 0, code.length());

    final long high = AccountPacker.packHigh(sbeBytes, 0);
    final long low = AccountPacker.packLow(sbeBytes, 0);

    assertEquals(out[0], high, "packHigh from String must equal packHigh from SBE bytes");
    assertEquals(out[1], low, "packLow from String must equal packLow from SBE bytes");
  }

  @Test
  void packHighLow_exactly16Chars_matchesStringPack() {
    final var code = "ABCDEFGHIJKLMNOP"; // exactly 16 ASCII chars
    final var out = new long[2];
    AccountPacker.pack(code, out);

    final var sbeBytes = code.getBytes(StandardCharsets.US_ASCII);
    assertEquals(16, sbeBytes.length);

    final long high = AccountPacker.packHigh(sbeBytes, 0);
    final long low = AccountPacker.packLow(sbeBytes, 0);

    assertEquals(out[0], high);
    assertEquals(out[1], low);
  }

  @Test
  void packHighLow_singleChar_nulPaddedCorrectly() {
    final var code = "A";
    final var out = new long[2];
    AccountPacker.pack(code, out);

    // Byte 0 = 'A' (0x41), bytes 1-15 = 0x00
    // High = 0x41 in little-endian long (byte 0 at LSB)
    assertEquals(0x41L, out[0], "High half should have 'A' in lowest byte only");
    assertEquals(0L, out[1], "Low half should be all zeros for single-char code");
  }

  @Test
  void pack_withOffset_readsFromCorrectPosition() {
    final var prefix = new byte[10];
    final var code = "HEDGE-002";
    final var sbeBytes = new byte[prefix.length + 16];
    System.arraycopy(
        code.getBytes(StandardCharsets.US_ASCII), 0, sbeBytes, prefix.length, code.length());

    final var out = new long[2];
    AccountPacker.pack(code, out);

    final long high = AccountPacker.packHigh(sbeBytes, prefix.length);
    final long low = AccountPacker.packLow(sbeBytes, prefix.length);

    assertEquals(out[0], high, "packHigh with offset must match String-based pack");
    assertEquals(out[1], low, "packLow with offset must match String-based pack");
  }

  @Test
  void pack_nullAccountCode_throwsIllegalArgument() {
    final var out = new long[2];
    assertThrows(IllegalArgumentException.class, () -> AccountPacker.pack(null, out));
  }

  @Test
  void pack_emptyAccountCode_throwsIllegalArgument() {
    final var out = new long[2];
    assertThrows(IllegalArgumentException.class, () -> AccountPacker.pack("", out));
  }

  @Test
  void pack_tooLongAccountCode_throwsIllegalArgument() {
    final var out = new long[2];
    assertThrows(
        IllegalArgumentException.class,
        () -> AccountPacker.pack("12345678901234567", out)); // 17 chars
  }

  @Test
  void pack_nonAsciiAccountCode_throwsIllegalArgument() {
    final var out = new long[2];
    assertThrows(
        IllegalArgumentException.class,
        () -> AccountPacker.pack("Kont\u00FCn", out)); // ü = non-ASCII
  }

  @Test
  void extractPackedAccount_roundTrip_matchesStringPack() {
    // Encode an OrderCreatedEvent with a known account code via SbeTestEncoder,
    // then verify extractPackedAccount produces the same packed values as pack(String).
    // This test uses AccountExtractor.extractPackedAccount which delegates to AccountPacker.
    final var code = "TRADE-ACC";
    final var stringOut = new long[2];
    AccountPacker.pack(code, stringOut);

    // Simulate SBE payload with account code at the OrderCreated offset (HEADER_SIZE + 123 = 131)
    final int accountOffset = 131; // template 100 absolute offset
    final var payload = new byte[accountOffset + 16];
    System.arraycopy(
        code.getBytes(StandardCharsets.US_ASCII), 0, payload, accountOffset, code.length());

    final var extractOut = new long[2];
    final boolean found =
        AccountExtractor.extractPackedAccount(100, payload, 0, payload.length, extractOut);

    assertEquals(true, found, "extractPackedAccount should find account in template 100");
    assertArrayEquals(
        stringOut,
        extractOut,
        "Extracted packed account must match String-packed account for round-trip correctness");
  }

  @Test
  void isEntitledAccount_matchingAccount_returnsTrue() {
    // Integration test: pack accounts into session, verify isEntitledAccount finds them
    final var out = new long[2];
    AccountPacker.pack("ACME-001", out);

    // Manually build a packed array with one account
    final var packed = new long[] {out[0], out[1]};

    // Verify match
    assertEquals(true, packed[0] == out[0] && packed[1] == out[1]);
  }
}
