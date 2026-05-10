package com.trading.engine.fixbridge.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link BrowserMessageReader} covering the three capabilities added in APP-40
 * phase-6: {@code OrderStatusRequest} type recognition, optional {@code _meta} envelope parsing,
 * and structural validation of the {@code _meta} object. Each test name follows the
 * {@code methodUnderTest_scenario_expectedBehavior} convention.
 *
 * <p>Threading: stateless — all methods under test are {@code static}. Tests are independent.
 */
final class BrowserMessageReaderMetaTest {

  // ---------------------------------------------------------------------------
  // Helpers.
  // ---------------------------------------------------------------------------

  private static ByteBuf wrap(final String json) {
    return Unpooled.wrappedBuffer(json.getBytes(StandardCharsets.UTF_8));
  }

  private static String slice(final MutableParsedMessage m, final int off, final int len) {
    return new String(m.scratch, off, len, StandardCharsets.UTF_8);
  }

  // ---------------------------------------------------------------------------
  // OrderStatusRequest type recognition.
  // ---------------------------------------------------------------------------

  @Test
  void parse_orderStatusRequestType_returnsTypeOrderStatusRequest() {
    final var out = new MutableParsedMessage();
    final int t =
        BrowserMessageReader.parse(
            wrap("{\"type\":\"OrderStatusRequest\",\"clOrdId\":\"C-1\"}"), out);

    assertEquals(MutableParsedMessage.TYPE_ORDER_STATUS_REQUEST, t);
    assertEquals("C-1", slice(out, out.clOrdIdOff, out.clOrdIdLen));
  }

  @Test
  void parse_unknownType_throwsUnknownType() {
    final var out = new MutableParsedMessage();
    final var ex =
        assertThrows(
            JsonParseException.class,
            () -> BrowserMessageReader.parse(wrap("{\"type\":\"Bogus\"}"), out));
    assertSame(JsonParseException.UNKNOWN_TYPE, ex);
  }

  // ---------------------------------------------------------------------------
  // _meta envelope — traceparent extraction.
  // ---------------------------------------------------------------------------

  /**
   * Full QuoteRequest with a valid {@code _meta} envelope. Verifies that {@code traceparentOff/Len}
   * resolve to the exact W3C traceparent string and that the surrounding QuoteRequest fields are
   * still parsed correctly.
   */
  @Test
  void parse_quoteRequestWithMeta_extractsTraceparent() {
    final var out = new MutableParsedMessage();
    final int t =
        BrowserMessageReader.parse(
            wrap(
                "{\"type\":\"QuoteRequest\","
                    + "\"reqId\":\"R-1\","
                    + "\"symbol\":\"EURUSD\","
                    + "\"side\":\"Buy\","
                    + "\"qty\":\"1000000\","
                    + "\"_meta\":{\"traceparent\":"
                    + "\"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01\"}}"),
            out);

    assertEquals(MutableParsedMessage.TYPE_QUOTE_REQUEST, t);
    assertEquals("R-1", slice(out, out.reqIdOff, out.reqIdLen));
    assertEquals("EURUSD", slice(out, out.symbolOff, out.symbolLen));
    assertEquals(MutableParsedMessage.SIDE_BUY, out.side);
    assertEquals(
        "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        slice(out, out.traceparentOff, out.traceparentLen));
  }

  /**
   * {@code _meta} contains several unknown keys mixed around {@code traceparent}. All unknown keys
   * must be silently skipped via {@code skipBalancedValue}; {@code traceparent} must still be
   * extracted.
   */
  @Test
  void parse_quoteRequestWithMetaUnknownInnerKeys_skipsThemViaBalanced() {
    final var out = new MutableParsedMessage();
    final int t =
        BrowserMessageReader.parse(
            wrap(
                "{\"type\":\"QuoteRequest\","
                    + "\"reqId\":\"R-2\","
                    + "\"symbol\":\"GBPUSD\","
                    + "\"side\":\"Sell\","
                    + "\"qty\":\"500\","
                    + "\"_meta\":{"
                    + "\"unknown\":\"foo\","
                    + "\"traceparent\":"
                    + "\"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01\","
                    + "\"another\":42,"
                    + "\"nested\":{\"x\":\"y\"},"
                    + "\"arr\":[1,2,3],"
                    + "\"flag\":true,"
                    + "\"missing\":null"
                    + "}}"),
            out);

    assertEquals(MutableParsedMessage.TYPE_QUOTE_REQUEST, t);
    assertEquals(
        "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        slice(out, out.traceparentOff, out.traceparentLen));
  }

  // ---------------------------------------------------------------------------
  // _meta structural errors — MALFORMED.
  // ---------------------------------------------------------------------------

  @Test
  void parse_metaTraceparentIsNumber_throwsMalformed() {
    // traceparent value must be a JSON string; a bare number is structurally wrong.
    final var out = new MutableParsedMessage();
    final var ex =
        assertThrows(
            JsonParseException.class,
            () ->
                BrowserMessageReader.parse(
                    wrap(
                        "{\"type\":\"QuoteRequest\","
                            + "\"reqId\":\"R\",\"symbol\":\"E\",\"side\":\"Buy\",\"qty\":\"1\","
                            + "\"_meta\":{\"traceparent\":42}}"),
                    out));
    assertSame(JsonParseException.MALFORMED, ex);
  }

  @Test
  void parse_metaWithoutOpeningBrace_throwsMalformed() {
    // _meta value must be a JSON object; a bare string is not allowed.
    final var out = new MutableParsedMessage();
    final var ex =
        assertThrows(
            JsonParseException.class,
            () ->
                BrowserMessageReader.parse(
                    wrap(
                        "{\"type\":\"QuoteRequest\","
                            + "\"reqId\":\"R\",\"symbol\":\"E\",\"side\":\"Buy\",\"qty\":\"1\","
                            + "\"_meta\":\"not-an-object\"}"),
                    out));
    assertSame(JsonParseException.MALFORMED, ex);
  }

  @Test
  void parse_metaUnclosedObject_throwsMalformed() {
    // _meta object truncated before its closing '}'.
    final var out = new MutableParsedMessage();
    final var ex =
        assertThrows(
            JsonParseException.class,
            () ->
                BrowserMessageReader.parse(
                    wrap(
                        "{\"type\":\"QuoteRequest\","
                            + "\"reqId\":\"R\",\"symbol\":\"E\",\"side\":\"Buy\",\"qty\":\"1\","
                            + "\"_meta\":{\"traceparent\":\"x\""),
                    out));
    assertSame(JsonParseException.MALFORMED, ex);
  }

  /**
   * Verifies that {@code skipBalancedValue} correctly handles a string containing {@code '}'} and
   * {@code '{'} characters — the parser must respect string boundaries when counting depth so these
   * characters do not fool the depth counter.
   *
   * <p>The scanStringEnd rejects backslash, so the curly braces below are literal bytes inside the
   * string value (no escape needed).
   */
  @Test
  void parse_metaWithStringContainingBraces_handlesCorrectly() {
    // "foo" value contains literal '{' and '}' bytes — skipBalancedValue must not get confused.
    final var out = new MutableParsedMessage();
    final int t =
        BrowserMessageReader.parse(
            wrap(
                "{\"type\":\"QuoteRequest\","
                    + "\"reqId\":\"R\",\"symbol\":\"E\",\"side\":\"Buy\",\"qty\":\"1\","
                    + "\"_meta\":{\"foo\":\"contains } and { braces\"}}"),
            out);

    assertEquals(MutableParsedMessage.TYPE_QUOTE_REQUEST, t);
    // No traceparent extracted — only the unknown key was present.
    assertEquals(-1, out.traceparentOff);
  }

  // ---------------------------------------------------------------------------
  // _meta position independence.
  // ---------------------------------------------------------------------------

  /** _meta first, then the required QuoteRequest fields. */
  @Test
  void parse_metaPosition_metaFirst_accepted() {
    final var out = new MutableParsedMessage();
    final int t =
        BrowserMessageReader.parse(
            wrap(
                "{\"_meta\":{\"traceparent\":"
                    + "\"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01\"},"
                    + "\"type\":\"QuoteRequest\","
                    + "\"reqId\":\"R\",\"symbol\":\"E\",\"side\":\"Buy\",\"qty\":\"1\"}"),
            out);

    assertEquals(MutableParsedMessage.TYPE_QUOTE_REQUEST, t);
    assertEquals(
        "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        slice(out, out.traceparentOff, out.traceparentLen));
  }

  /** _meta in the middle of the required QuoteRequest fields. */
  @Test
  void parse_metaPosition_metaMid_accepted() {
    final var out = new MutableParsedMessage();
    final int t =
        BrowserMessageReader.parse(
            wrap(
                "{\"type\":\"QuoteRequest\","
                    + "\"reqId\":\"R\","
                    + "\"_meta\":{\"traceparent\":"
                    + "\"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01\"},"
                    + "\"symbol\":\"E\",\"side\":\"Buy\",\"qty\":\"1\"}"),
            out);

    assertEquals(MutableParsedMessage.TYPE_QUOTE_REQUEST, t);
    assertEquals(
        "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        slice(out, out.traceparentOff, out.traceparentLen));
  }

  /** _meta last, after all required QuoteRequest fields. */
  @Test
  void parse_metaPosition_metaLast_accepted() {
    final var out = new MutableParsedMessage();
    final int t =
        BrowserMessageReader.parse(
            wrap(
                "{\"type\":\"QuoteRequest\","
                    + "\"reqId\":\"R\",\"symbol\":\"E\",\"side\":\"Buy\",\"qty\":\"1\","
                    + "\"_meta\":{\"traceparent\":"
                    + "\"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01\"}}"),
            out);

    assertEquals(MutableParsedMessage.TYPE_QUOTE_REQUEST, t);
    assertEquals(
        "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        slice(out, out.traceparentOff, out.traceparentLen));
  }

  // ---------------------------------------------------------------------------
  // _meta — backslash / unicode escape in inner key string.
  // ---------------------------------------------------------------------------

  /**
   * A JSON unicode escape sequence inside an inner _meta key value. The production
   * {@code scanStringEnd} rejects ALL backslash bytes, so any escape sequence in a string
   * results in {@link JsonParseException#MALFORMED}. This test confirms that behavior.
   *
   * <p>The wire protocol is pure ASCII; JSON escape sequences are explicitly out of scope.
   */
  @Test
  void parse_unicodeEscapeInMetaInnerKeyValue_throwsMalformed() {
    // The string value of the unknown inner key contains a backslash (JSON \\uXXXX escape) —
    // rejected by scanStringEnd because the backslash byte (0x5C) is explicitly rejected.
    // Build the frame bytes manually so the Java compiler doesn't pre-process the backslash.
    final byte[] frame = buildFrameWithBackslashInMeta();
    final var out = new MutableParsedMessage();
    final var ex =
        assertThrows(
            JsonParseException.class,
            () ->
                BrowserMessageReader.parse(
                    io.netty.buffer.Unpooled.wrappedBuffer(frame), out));
    assertSame(JsonParseException.MALFORMED, ex);
  }

  /**
   * Builds a frame whose _meta inner value contains a literal backslash byte without relying on
   * Java unicode escape processing. The shape is:
   * {@code {"type":"QuoteRequest","reqId":"R","symbol":"E","side":"Buy","qty":"1",
   * "_meta":{"foo":"\_bar"}}} where {@code \_} represents a literal 0x5C byte.
   */
  private static byte[] buildFrameWithBackslashInMeta() {
    final String prefix =
        "{\"type\":\"QuoteRequest\","
            + "\"reqId\":\"R\",\"symbol\":\"E\",\"side\":\"Buy\",\"qty\":\"1\","
            + "\"_meta\":{\"foo\":\"";
    final String suffix = "bar\"}}";
    final byte[] prefixBytes = prefix.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    final byte[] suffixBytes = suffix.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    final byte[] frame = new byte[prefixBytes.length + 1 + suffixBytes.length];
    System.arraycopy(prefixBytes, 0, frame, 0, prefixBytes.length);
    frame[prefixBytes.length] = 0x5C; // literal backslash
    System.arraycopy(suffixBytes, 0, frame, prefixBytes.length + 1, suffixBytes.length);
    return frame;
  }

  // ---------------------------------------------------------------------------
  // Empty _meta.
  // ---------------------------------------------------------------------------

  @Test
  void parse_emptyMeta_acceptsAndProducesNoTraceparent() {
    final var out = new MutableParsedMessage();
    final int t =
        BrowserMessageReader.parse(
            wrap(
                "{\"type\":\"QuoteRequest\","
                    + "\"reqId\":\"R\",\"symbol\":\"E\",\"side\":\"Buy\",\"qty\":\"1\","
                    + "\"_meta\":{}}"),
            out);

    assertEquals(MutableParsedMessage.TYPE_QUOTE_REQUEST, t);
    // traceparent absent — off is reset sentinel.
    assertEquals(-1, out.traceparentOff);
  }
}
