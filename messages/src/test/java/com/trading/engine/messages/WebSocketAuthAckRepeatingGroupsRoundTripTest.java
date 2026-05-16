package com.trading.engine.messages;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.WebSocketAuthAckDecoder;
import com.trading.engine.messages.sbe.WebSocketAuthAckEncoder;
import java.nio.charset.StandardCharsets;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/**
 * Codec round-trip tests for WebSocketAuthAck (template 61) covering the two new Phase 3 repeating
 * groups: {@code symbolPreferences} (id=10052) and {@code panelLayout} (id=10053).
 *
 * <p>Four combinatorial cases are exercised:
 *
 * <ol>
 *   <li><strong>Both groups empty</strong> — zero entries in each group; group headers (4 B each)
 *       are still written. Scalar fields still round-trip.
 *   <li><strong>symbolPreferences non-empty, panelLayout empty</strong> — four symbols with
 *       distinct ASCII values; panelLayout count = 0.
 *   <li><strong>symbolPreferences empty, panelLayout non-empty</strong> — two entries ({@code
 *       order-en → right-to}, {@code blotter → center}); symbolPreferences count = 0.
 *   <li><strong>Both groups non-empty</strong> — combination of cases (b) and (c).
 * </ol>
 *
 * <p>In every case the five scalar fields ({@code sessionId}, {@code protocolVersion}, {@code
 * maxSubscriptions}, {@code serverHeartbeatIntervalMs}, {@code clientHeartbeatIntervalMs}) are set
 * to distinct non-default values and asserted to round-trip identically.
 *
 * <p>Threading model: Not thread-safe — single-threaded JUnit test execution only. Allocation:
 * Allocates one {@code UnsafeBuffer} per test, reused across encode and decode phases.
 */
final class WebSocketAuthAckRepeatingGroupsRoundTripTest {

  /**
   * 16 KiB — comfortably larger than the largest WebSocketAuthAck frame (header 8 B + block 28 B +
   * 4 symbols × 8 B + 2 panels × 16 B + group headers ≈ 120 B).
   */
  private static final int BUF_SIZE = 16_384;

  /** Distinct UUID halves reused across every test to verify the sessionId composite field. */
  private static final long SESSION_MSB = 0x1122_3344_5566_7788L;

  private static final long SESSION_LSB = 0x99AA_BBCC_DDEE_FF00L;

  // -------------------------------------------------------------------------
  // Case (a): both groups empty
  // -------------------------------------------------------------------------

  /**
   * Encodes WebSocketAuthAck with both {@code symbolPreferences} and {@code panelLayout} groups at
   * count=0. Verifies that all five scalar fields decode correctly and both group counts are zero.
   */
  @Test
  void webSocketAuthAck_roundTrip_bothGroupsEmpty() {
    final long serverHbMs = 5_000L;
    final long clientHbMs = 10_000L;
    final int protocolVersion = 1;
    final int maxSubs = 20;

    final var buf = new UnsafeBuffer(new byte[BUF_SIZE]);
    final var headerEncoder = new MessageHeaderEncoder();
    final var encoder = new WebSocketAuthAckEncoder();
    encoder.wrapAndApplyHeader(buf, 0, headerEncoder);
    encoder.sessionId().mostSignificantBits(SESSION_MSB).leastSignificantBits(SESSION_LSB);
    encoder
        .protocolVersion(protocolVersion)
        .maxSubscriptions(maxSubs)
        .serverHeartbeatIntervalMs(serverHbMs)
        .clientHeartbeatIntervalMs(clientHbMs);
    encoder.symbolPreferencesCount(0);
    encoder.panelLayoutCount(0);

    final var headerDecoder = new MessageHeaderDecoder();
    final var decoder = new WebSocketAuthAckDecoder();
    decoder.wrapAndApplyHeader(buf, 0, headerDecoder);

    assertEquals(WebSocketAuthAckDecoder.TEMPLATE_ID, headerDecoder.templateId(), "templateId");
    assertEquals(WebSocketAuthAckDecoder.SCHEMA_ID, headerDecoder.schemaId(), "schemaId");
    assertEquals(WebSocketAuthAckDecoder.SCHEMA_VERSION, headerDecoder.version(), "version");
    assertScalars(decoder, protocolVersion, maxSubs, serverHbMs, clientHbMs);

    final var symPrefs = decoder.symbolPreferences();
    assertEquals(0, symPrefs.count(), "symbolPreferences count");

    final var panelLayout = decoder.panelLayout();
    assertEquals(0, panelLayout.count(), "panelLayout count");
  }

  // -------------------------------------------------------------------------
  // Case (b): symbolPreferences non-empty, panelLayout empty
  // -------------------------------------------------------------------------

  /**
   * Encodes WebSocketAuthAck with four {@code symbolPreferences} entries (EURUSD, GBPUSD, USDJPY,
   * AUDUSD) and an empty {@code panelLayout} group. Verifies that iteration order is preserved and
   * the scalar block is intact.
   */
  @Test
  void webSocketAuthAck_roundTrip_symbolPreferencesNonEmpty_panelLayoutEmpty() {
    final var symbols = new String[] {"EURUSD", "GBPUSD", "USDJPY", "AUDUSD"};
    final long serverHbMs = 3_000L;
    final long clientHbMs = 6_000L;
    final int protocolVersion = 2;
    final int maxSubs = 16;

    final var buf = new UnsafeBuffer(new byte[BUF_SIZE]);
    final var headerEncoder = new MessageHeaderEncoder();
    final var encoder = new WebSocketAuthAckEncoder();
    encoder.wrapAndApplyHeader(buf, 0, headerEncoder);
    encoder.sessionId().mostSignificantBits(SESSION_MSB).leastSignificantBits(SESSION_LSB);
    encoder
        .protocolVersion(protocolVersion)
        .maxSubscriptions(maxSubs)
        .serverHeartbeatIntervalMs(serverHbMs)
        .clientHeartbeatIntervalMs(clientHbMs);

    final var symEnc = encoder.symbolPreferencesCount(symbols.length);
    for (final var symbol : symbols) {
      symEnc.next().symbol(symbol);
    }
    encoder.panelLayoutCount(0);

    final var headerDecoder = new MessageHeaderDecoder();
    final var decoder = new WebSocketAuthAckDecoder();
    decoder.wrapAndApplyHeader(buf, 0, headerDecoder);

    assertScalars(decoder, protocolVersion, maxSubs, serverHbMs, clientHbMs);

    final var symPrefs = decoder.symbolPreferences();
    assertEquals(4, symPrefs.count(), "symbolPreferences count");
    for (int i = 0; i < 4; i++) {
      symPrefs.next();
      final var dst = new byte[WebSocketAuthAckDecoder.SymbolPreferencesDecoder.symbolLength()];
      symPrefs.getSymbol(dst, 0);
      assertArrayEquals(
          padRight(symbols[i], WebSocketAuthAckDecoder.SymbolPreferencesDecoder.symbolLength()),
          dst,
          "symbolPreferences[" + i + "]");
    }

    final var panelLayout = decoder.panelLayout();
    assertEquals(0, panelLayout.count(), "panelLayout count");
  }

  // -------------------------------------------------------------------------
  // Case (c): symbolPreferences empty, panelLayout non-empty
  // -------------------------------------------------------------------------

  /**
   * Encodes WebSocketAuthAck with an empty {@code symbolPreferences} group and two {@code
   * panelLayout} entries ({@code order-en → right-to}, {@code blotter → center}). Verifies that
   * panelId and slot fields round-trip byte-identically and the scalar block is intact.
   */
  @Test
  void webSocketAuthAck_roundTrip_symbolPreferencesEmpty_panelLayoutNonEmpty() {
    // Panel IDs and slots are truncated to 8 chars to fit the Symbol field.
    final var panelIds = new String[] {"order-en", "blotter"};
    final var slots = new String[] {"right-to", "center"};
    final long serverHbMs = 4_000L;
    final long clientHbMs = 8_000L;
    final int protocolVersion = 3;
    final int maxSubs = 12;

    final var buf = new UnsafeBuffer(new byte[BUF_SIZE]);
    final var headerEncoder = new MessageHeaderEncoder();
    final var encoder = new WebSocketAuthAckEncoder();
    encoder.wrapAndApplyHeader(buf, 0, headerEncoder);
    encoder.sessionId().mostSignificantBits(SESSION_MSB).leastSignificantBits(SESSION_LSB);
    encoder
        .protocolVersion(protocolVersion)
        .maxSubscriptions(maxSubs)
        .serverHeartbeatIntervalMs(serverHbMs)
        .clientHeartbeatIntervalMs(clientHbMs);
    encoder.symbolPreferencesCount(0);

    final var panelEnc = encoder.panelLayoutCount(2);
    for (int i = 0; i < 2; i++) {
      panelEnc.next().panelId(panelIds[i]).slot(slots[i]);
    }

    final var headerDecoder = new MessageHeaderDecoder();
    final var decoder = new WebSocketAuthAckDecoder();
    decoder.wrapAndApplyHeader(buf, 0, headerDecoder);

    assertScalars(decoder, protocolVersion, maxSubs, serverHbMs, clientHbMs);

    final var symPrefs = decoder.symbolPreferences();
    assertEquals(0, symPrefs.count(), "symbolPreferences count");

    final var panelLayout = decoder.panelLayout();
    assertEquals(2, panelLayout.count(), "panelLayout count");
    for (int i = 0; i < 2; i++) {
      panelLayout.next();
      final var pidDst = new byte[WebSocketAuthAckDecoder.PanelLayoutDecoder.panelIdLength()];
      panelLayout.getPanelId(pidDst, 0);
      assertArrayEquals(
          padRight(panelIds[i], WebSocketAuthAckDecoder.PanelLayoutDecoder.panelIdLength()),
          pidDst,
          "panelId[" + i + "]");

      final var slotDst = new byte[WebSocketAuthAckDecoder.PanelLayoutDecoder.slotLength()];
      panelLayout.getSlot(slotDst, 0);
      assertArrayEquals(
          padRight(slots[i], WebSocketAuthAckDecoder.PanelLayoutDecoder.slotLength()),
          slotDst,
          "slot[" + i + "]");
    }
  }

  // -------------------------------------------------------------------------
  // Case (d): both groups non-empty
  // -------------------------------------------------------------------------

  /**
   * Encodes WebSocketAuthAck with both {@code symbolPreferences} (EURUSD, GBPUSD) and {@code
   * panelLayout} ({@code order-en → right-to}, {@code blotter → center}) non-empty. Verifies that
   * the decoder sequences correctly through both groups after the scalar block.
   */
  @Test
  void webSocketAuthAck_roundTrip_bothGroupsNonEmpty() {
    final var symbols = new String[] {"EURUSD", "GBPUSD"};
    final var panelIds = new String[] {"order-en", "blotter"};
    final var slots = new String[] {"right-to", "center"};
    final long serverHbMs = 7_500L;
    final long clientHbMs = 15_000L;
    final int protocolVersion = 4;
    final int maxSubs = 32;

    final var buf = new UnsafeBuffer(new byte[BUF_SIZE]);
    final var headerEncoder = new MessageHeaderEncoder();
    final var encoder = new WebSocketAuthAckEncoder();
    encoder.wrapAndApplyHeader(buf, 0, headerEncoder);
    encoder.sessionId().mostSignificantBits(SESSION_MSB).leastSignificantBits(SESSION_LSB);
    encoder
        .protocolVersion(protocolVersion)
        .maxSubscriptions(maxSubs)
        .serverHeartbeatIntervalMs(serverHbMs)
        .clientHeartbeatIntervalMs(clientHbMs);

    final var symEnc = encoder.symbolPreferencesCount(2);
    for (final var symbol : symbols) {
      symEnc.next().symbol(symbol);
    }

    final var panelEnc = encoder.panelLayoutCount(2);
    for (int i = 0; i < 2; i++) {
      panelEnc.next().panelId(panelIds[i]).slot(slots[i]);
    }

    final var headerDecoder = new MessageHeaderDecoder();
    final var decoder = new WebSocketAuthAckDecoder();
    decoder.wrapAndApplyHeader(buf, 0, headerDecoder);

    assertScalars(decoder, protocolVersion, maxSubs, serverHbMs, clientHbMs);

    final var symPrefs = decoder.symbolPreferences();
    assertEquals(2, symPrefs.count(), "symbolPreferences count");
    for (int i = 0; i < 2; i++) {
      symPrefs.next();
      final var dst = new byte[WebSocketAuthAckDecoder.SymbolPreferencesDecoder.symbolLength()];
      symPrefs.getSymbol(dst, 0);
      assertArrayEquals(
          padRight(symbols[i], WebSocketAuthAckDecoder.SymbolPreferencesDecoder.symbolLength()),
          dst,
          "symbolPreferences[" + i + "]");
    }

    final var panelLayout = decoder.panelLayout();
    assertEquals(2, panelLayout.count(), "panelLayout count");
    for (int i = 0; i < 2; i++) {
      panelLayout.next();
      final var pidDst = new byte[WebSocketAuthAckDecoder.PanelLayoutDecoder.panelIdLength()];
      panelLayout.getPanelId(pidDst, 0);
      assertArrayEquals(
          padRight(panelIds[i], WebSocketAuthAckDecoder.PanelLayoutDecoder.panelIdLength()),
          pidDst,
          "panelId[" + i + "]");

      final var slotDst = new byte[WebSocketAuthAckDecoder.PanelLayoutDecoder.slotLength()];
      panelLayout.getSlot(slotDst, 0);
      assertArrayEquals(
          padRight(slots[i], WebSocketAuthAckDecoder.PanelLayoutDecoder.slotLength()),
          slotDst,
          "slot[" + i + "]");
    }
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  /**
   * Asserts that the five scalar fields of the decoded {@link WebSocketAuthAckDecoder} match the
   * expected values and that the {@code sessionId} UUID halves equal {@link #SESSION_MSB} and
   * {@link #SESSION_LSB}.
   *
   * @param decoder the decoder positioned after {@code wrapAndApplyHeader}
   * @param protocolVersion expected protocol version
   * @param maxSubs expected maximum subscriptions
   * @param serverHbMs expected server heartbeat interval in ms
   * @param clientHbMs expected client heartbeat interval in ms
   */
  private static void assertScalars(
      final WebSocketAuthAckDecoder decoder,
      final int protocolVersion,
      final int maxSubs,
      final long serverHbMs,
      final long clientHbMs) {
    assertEquals(SESSION_MSB, decoder.sessionId().mostSignificantBits(), "sessionId MSB");
    assertEquals(SESSION_LSB, decoder.sessionId().leastSignificantBits(), "sessionId LSB");
    assertEquals(protocolVersion, decoder.protocolVersion(), "protocolVersion");
    assertEquals(maxSubs, decoder.maxSubscriptions(), "maxSubscriptions");
    assertEquals(serverHbMs, decoder.serverHeartbeatIntervalMs(), "serverHeartbeatIntervalMs");
    assertEquals(clientHbMs, decoder.clientHeartbeatIntervalMs(), "clientHeartbeatIntervalMs");
  }

  /**
   * Returns a NUL-padded byte array of exactly {@code width} bytes containing the ASCII encoding of
   * {@code value}. Mirrors the SBE encoder behaviour: characters are written left-justified and the
   * tail is zero-filled.
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
