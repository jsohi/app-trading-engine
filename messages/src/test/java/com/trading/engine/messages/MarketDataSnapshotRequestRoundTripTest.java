package com.trading.engine.messages;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.trading.engine.messages.sbe.MarketDataSnapshotRequestDecoder;
import com.trading.engine.messages.sbe.MarketDataSnapshotRequestEncoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import java.nio.charset.StandardCharsets;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/**
 * Codec round-trip tests for MarketDataSnapshotRequest (template 56).
 *
 * <p>Template 56 is a browser-to-publisher subscription request carrying a single {@code symbol}
 * (Symbol, FIX tag 55, 8-byte fixed ASCII). Tests verify that the ASCII bytes are written
 * left-justified and the trailing pad bytes are NUL, matching the SBE encoder invariant.
 *
 * <p>Threading model: Not thread-safe — single-threaded JUnit test execution only. Allocation:
 * Allocates one {@code UnsafeBuffer} per test, reused across encode and decode phases.
 */
final class MarketDataSnapshotRequestRoundTripTest {

  /** 8 KiB — more than sufficient for the 8-byte block + 8-byte header. */
  private static final int BUF_SIZE = 8_192;

  // -------------------------------------------------------------------------
  // Template 56 — short symbol (6-char, padded to 8)
  // -------------------------------------------------------------------------

  /**
   * Encodes a MarketDataSnapshotRequest with symbol "EURUSD" (6 chars) and asserts that the decoded
   * 8-byte field has the correct ASCII prefix and two trailing NUL bytes. Verifies templateId (56),
   * schemaId (1), and version (1) from the message header.
   */
  @Test
  void marketDataSnapshotRequest_roundTrip_sixCharSymbol() {
    final var symbol = "EURUSD";

    final var buf = new UnsafeBuffer(new byte[BUF_SIZE]);
    final var headerEncoder = new MessageHeaderEncoder();
    final var encoder = new MarketDataSnapshotRequestEncoder();
    encoder.wrapAndApplyHeader(buf, 0, headerEncoder);
    encoder.symbol(symbol);

    final var headerDecoder = new MessageHeaderDecoder();
    final var decoder = new MarketDataSnapshotRequestDecoder();
    decoder.wrapAndApplyHeader(buf, 0, headerDecoder);

    assertEquals(
        MarketDataSnapshotRequestDecoder.TEMPLATE_ID, headerDecoder.templateId(), "templateId");
    assertEquals(MarketDataSnapshotRequestDecoder.SCHEMA_ID, headerDecoder.schemaId(), "schemaId");
    assertEquals(
        MarketDataSnapshotRequestDecoder.SCHEMA_VERSION, headerDecoder.version(), "version");

    final var symDst = new byte[MarketDataSnapshotRequestDecoder.symbolLength()];
    decoder.getSymbol(symDst, 0);
    assertArrayEquals(
        padRight(symbol, MarketDataSnapshotRequestDecoder.symbolLength()),
        symDst,
        "symbol byte-identical round-trip");
  }

  // -------------------------------------------------------------------------
  // Template 56 — seven-char symbol (padded to 8)
  // -------------------------------------------------------------------------

  /**
   * Encodes a MarketDataSnapshotRequest with symbol "USDCNHX" (7 chars) and asserts that the
   * decoded field contains the seven ASCII bytes followed by one NUL byte.
   */
  @Test
  void marketDataSnapshotRequest_roundTrip_sevenCharSymbol() {
    final var symbol = "USDCNHX";

    final var buf = new UnsafeBuffer(new byte[BUF_SIZE]);
    final var headerEncoder = new MessageHeaderEncoder();
    final var encoder = new MarketDataSnapshotRequestEncoder();
    encoder.wrapAndApplyHeader(buf, 0, headerEncoder);
    encoder.symbol(symbol);

    final var headerDecoder = new MessageHeaderDecoder();
    final var decoder = new MarketDataSnapshotRequestDecoder();
    decoder.wrapAndApplyHeader(buf, 0, headerDecoder);

    final var symDst = new byte[MarketDataSnapshotRequestDecoder.symbolLength()];
    decoder.getSymbol(symDst, 0);
    assertArrayEquals(
        padRight(symbol, MarketDataSnapshotRequestDecoder.symbolLength()),
        symDst,
        "symbol 7-char padded to 8");
  }

  // -------------------------------------------------------------------------
  // Template 56 — exactly 8-char symbol (no padding)
  // -------------------------------------------------------------------------

  /**
   * Encodes a MarketDataSnapshotRequest with an 8-character symbol (field capacity). Asserts that
   * all 8 bytes decode identically to the input, with no trailing NUL appended.
   */
  @Test
  void marketDataSnapshotRequest_roundTrip_eightCharSymbol() {
    final var symbol = "XAUUSDXX"; // 8 chars exactly
    assertEquals(8, symbol.length(), "test setup: symbol must be exactly 8 chars");

    final var buf = new UnsafeBuffer(new byte[BUF_SIZE]);
    final var headerEncoder = new MessageHeaderEncoder();
    final var encoder = new MarketDataSnapshotRequestEncoder();
    encoder.wrapAndApplyHeader(buf, 0, headerEncoder);
    encoder.symbol(symbol);

    final var headerDecoder = new MessageHeaderDecoder();
    final var decoder = new MarketDataSnapshotRequestDecoder();
    decoder.wrapAndApplyHeader(buf, 0, headerDecoder);

    final var symDst = new byte[MarketDataSnapshotRequestDecoder.symbolLength()];
    decoder.getSymbol(symDst, 0);
    assertArrayEquals(
        symbol.getBytes(StandardCharsets.US_ASCII), symDst, "symbol 8-char no trailing NUL");
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  /**
   * Returns a NUL-padded byte array of exactly {@code width} bytes containing the ASCII encoding of
   * {@code value}. Mirrors the SBE encoder behaviour.
   *
   * @param value the string to encode; must be &lt;= {@code width} characters long
   * @param width the fixed field width in bytes
   * @return byte array of length {@code width}
   */
  private static byte[] padRight(final String value, final int width) {
    final var dst = new byte[width];
    if (value != null && !value.isEmpty()) {
      final byte[] src = value.getBytes(StandardCharsets.US_ASCII);
      System.arraycopy(src, 0, dst, 0, Math.min(src.length, width));
    }
    return dst;
  }
}
