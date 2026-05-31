package com.trading.engine.cluster.refdata;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link AccountIdentifierBytes}. APP-62 §H 4-eyes ingress validation in {@link
 * LoadRiskLimitHandler} / {@link LoadRiskLimitBatchHandler} delegates to this utility, so the
 * length-mismatch guard and short-circuit-on-byte-mismatch behaviour are regulator-relevant — every
 * branch is covered here.
 */
class AccountIdentifierBytesTest {

  @Test
  void isAllZero_emptyBuffer_returnsTrue() {
    assertTrue(AccountIdentifierBytes.isAllZero(new byte[0]));
  }

  @Test
  void isAllZero_allZeroBuffer_returnsTrue() {
    assertTrue(AccountIdentifierBytes.isAllZero(new byte[16]));
  }

  @Test
  void isAllZero_singleNonZeroByteAtStart_returnsFalse() {
    byte[] buf = new byte[16];
    buf[0] = 1;
    assertFalse(AccountIdentifierBytes.isAllZero(buf));
  }

  @Test
  void isAllZero_singleNonZeroByteAtEnd_returnsFalse() {
    byte[] buf = new byte[16];
    buf[15] = (byte) 0xFF;
    assertFalse(AccountIdentifierBytes.isAllZero(buf));
  }

  @Test
  void byteEquals_bothEmpty_returnsTrue() {
    assertTrue(AccountIdentifierBytes.byteEquals(new byte[0], new byte[0]));
  }

  @Test
  void byteEquals_identicalBuffers_returnsTrue() {
    final byte[] a = "ALICE".getBytes(StandardCharsets.US_ASCII);
    final byte[] b = "ALICE".getBytes(StandardCharsets.US_ASCII);
    assertTrue(AccountIdentifierBytes.byteEquals(a, b));
  }

  @Test
  void byteEquals_differentLength_returnsFalse() {
    // Length guard must short-circuit before per-byte loop (would otherwise read out of bounds or
    // silently match a prefix).
    assertFalse(AccountIdentifierBytes.byteEquals(new byte[8], new byte[16]));
    assertFalse(AccountIdentifierBytes.byteEquals(new byte[16], new byte[8]));
  }

  @Test
  void byteEquals_singleByteDifferent_returnsFalse() {
    final byte[] a = new byte[16];
    final byte[] b = new byte[16];
    b[7] = 1;
    assertFalse(AccountIdentifierBytes.byteEquals(a, b));
  }

  @Test
  void byteEquals_differAtFirstByte_returnsFalse() {
    final byte[] a = new byte[16];
    final byte[] b = new byte[16];
    a[0] = 1;
    assertFalse(AccountIdentifierBytes.byteEquals(a, b));
  }

  @Test
  void byteEquals_differAtLastByte_returnsFalse() {
    final byte[] a = new byte[16];
    final byte[] b = new byte[16];
    a[15] = (byte) 0xFF;
    assertFalse(AccountIdentifierBytes.byteEquals(a, b));
  }
}
