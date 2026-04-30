package com.trading.engine.fixbridge.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Verifies the strict, zero-allocation JSON parser {@link BrowserMessageReader} round-trips every
 * inbound wire-protocol message and rejects every documented failure class with the matching {@link
 * JsonParseException} singleton (locked §3, §11).
 *
 * <p>Test naming follows {@code methodUnderTest_scenario_expectedBehavior} per project conventions.
 */
final class BrowserMessageReaderTest {

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
  // Happy paths — every message kind.
  // ---------------------------------------------------------------------------

  @Test
  void parse_authMessage_populatesTokenSlice() {
    final var out = new MutableParsedMessage();
    final int t =
        BrowserMessageReader.parse(wrap("{\"type\":\"Auth\",\"token\":\"abc.def.ghi\"}"), out);

    assertEquals(MutableParsedMessage.TYPE_AUTH, t);
    assertEquals("abc.def.ghi", slice(out, out.tokenOff, out.tokenLen));
  }

  @Test
  void parse_quoteRequest_populatesAllFields() {
    final var out = new MutableParsedMessage();
    final int t =
        BrowserMessageReader.parse(
            wrap(
                "{\"type\":\"QuoteRequest\",\"reqId\":\"R-1\",\"symbol\":\"EURUSD\","
                    + "\"side\":\"Buy\",\"qty\":\"1000000.50\"}"),
            out);

    assertEquals(MutableParsedMessage.TYPE_QUOTE_REQUEST, t);
    assertEquals("R-1", slice(out, out.reqIdOff, out.reqIdLen));
    assertEquals("EURUSD", slice(out, out.symbolOff, out.symbolLen));
    assertEquals(MutableParsedMessage.SIDE_BUY, out.side);
    // 1_000_000.50 × 10^8 = 100_000_050_000_000
    assertEquals(100_000_050_000_000L, out.qty);
    assertEquals("1000000.50", slice(out, out.qtyOff, out.qtyLen));
  }

  @Test
  void parse_acceptQuote_populatesQuoteIdAndClOrdId() {
    final var out = new MutableParsedMessage();
    final int t =
        BrowserMessageReader.parse(
            wrap("{\"type\":\"AcceptQuote\",\"quoteId\":\"Q-7\",\"clOrdId\":\"C-9\"}"), out);

    assertEquals(MutableParsedMessage.TYPE_ACCEPT_QUOTE, t);
    assertEquals("Q-7", slice(out, out.quoteIdOff, out.quoteIdLen));
    assertEquals("C-9", slice(out, out.clOrdIdOff, out.clOrdIdLen));
  }

  @Test
  void parse_rejectQuote_populatesQuoteIdOnly() {
    final var out = new MutableParsedMessage();
    final int t =
        BrowserMessageReader.parse(wrap("{\"type\":\"RejectQuote\",\"quoteId\":\"Q-X\"}"), out);

    assertEquals(MutableParsedMessage.TYPE_REJECT_QUOTE, t);
    assertEquals("Q-X", slice(out, out.quoteIdOff, out.quoteIdLen));
    assertEquals(-1, out.clOrdIdOff);
  }

  @Test
  void parse_newOrderSingle_populatesAllFields() {
    final var out = new MutableParsedMessage();
    final int t =
        BrowserMessageReader.parse(
            wrap(
                "{\"type\":\"NewOrderSingle\",\"clOrdId\":\"C-1\",\"symbol\":\"EURUSD\","
                    + "\"side\":\"Sell\",\"qty\":\"1000000\",\"price\":\"1.10000000\","
                    + "\"ordType\":\"Limit\",\"timeInForce\":\"GTC\",\"account\":\"ACCT-1\"}"),
            out);

    assertEquals(MutableParsedMessage.TYPE_NEW_ORDER_SINGLE, t);
    assertEquals("C-1", slice(out, out.clOrdIdOff, out.clOrdIdLen));
    assertEquals("EURUSD", slice(out, out.symbolOff, out.symbolLen));
    assertEquals(MutableParsedMessage.SIDE_SELL, out.side);
    assertEquals(100_000_000_000_000L, out.qty);
    assertEquals("1.10000000", slice(out, out.priceOff, out.priceLen));
    assertEquals(MutableParsedMessage.ORDTYPE_LIMIT, out.ordType);
    assertEquals(MutableParsedMessage.TIF_GTC, out.timeInForce);
    assertEquals("ACCT-1", slice(out, out.accountOff, out.accountLen));
  }

  @Test
  void parse_cancelOrder_populatesAllFields() {
    final var out = new MutableParsedMessage();
    final int t =
        BrowserMessageReader.parse(
            wrap(
                "{\"type\":\"CancelOrder\",\"clOrdId\":\"C-2\",\"origClOrdId\":\"C-1\","
                    + "\"symbol\":\"EURUSD\",\"side\":\"Buy\"}"),
            out);

    assertEquals(MutableParsedMessage.TYPE_CANCEL_ORDER, t);
    assertEquals("C-2", slice(out, out.clOrdIdOff, out.clOrdIdLen));
    assertEquals("C-1", slice(out, out.origClOrdIdOff, out.origClOrdIdLen));
    assertEquals("EURUSD", slice(out, out.symbolOff, out.symbolLen));
    assertEquals(MutableParsedMessage.SIDE_BUY, out.side);
  }

  // ---------------------------------------------------------------------------
  // Side / OrdType / TimeInForce permutations.
  // ---------------------------------------------------------------------------

  @Test
  void parse_quoteRequestSellSide_decodesAsSellSentinel() {
    final var out = new MutableParsedMessage();
    BrowserMessageReader.parse(
        wrap(
            "{\"type\":\"QuoteRequest\",\"reqId\":\"R\",\"symbol\":\"E\",\"side\":\"Sell\","
                + "\"qty\":\"1\"}"),
        out);
    assertEquals(MutableParsedMessage.SIDE_SELL, out.side);
  }

  @Test
  void parse_marketOrdType_decodesAsMarketSentinel() {
    final var out = new MutableParsedMessage();
    BrowserMessageReader.parse(
        wrap(
            "{\"type\":\"NewOrderSingle\",\"clOrdId\":\"C\",\"symbol\":\"E\",\"side\":\"Buy\","
                + "\"qty\":\"1\",\"price\":\"1\",\"ordType\":\"Market\",\"timeInForce\":\"IOC\","
                + "\"account\":\"A\"}"),
        out);
    assertEquals(MutableParsedMessage.ORDTYPE_MARKET, out.ordType);
    assertEquals(MutableParsedMessage.TIF_IOC, out.timeInForce);
  }

  @Test
  void parse_allTimeInForceValues_eachDecodesToOwnSentinel() {
    final String[][] cases = {
      {"DAY", String.valueOf((char) MutableParsedMessage.TIF_DAY)},
      {"GTC", String.valueOf((char) MutableParsedMessage.TIF_GTC)},
      {"IOC", String.valueOf((char) MutableParsedMessage.TIF_IOC)},
      {"FOK", String.valueOf((char) MutableParsedMessage.TIF_FOK)},
      {"GTD", String.valueOf((char) MutableParsedMessage.TIF_GTD)},
    };
    final var out = new MutableParsedMessage();
    for (final String[] c : cases) {
      out.reset();
      BrowserMessageReader.parse(
          wrap(
              "{\"type\":\"NewOrderSingle\",\"clOrdId\":\"C\",\"symbol\":\"E\",\"side\":\"Buy\","
                  + "\"qty\":\"1\",\"price\":\"1\",\"ordType\":\"Limit\",\"timeInForce\":\""
                  + c[0]
                  + "\",\"account\":\"A\"}"),
          out);
      assertEquals(c[1].charAt(0), (char) out.timeInForce, "tif=" + c[0]);
    }
  }

  // ---------------------------------------------------------------------------
  // Numeric edge cases.
  // ---------------------------------------------------------------------------

  @Test
  void parse_qtyZero_decodesAsZero() {
    final var out = new MutableParsedMessage();
    BrowserMessageReader.parse(
        wrap(
            "{\"type\":\"QuoteRequest\",\"reqId\":\"R\",\"symbol\":\"E\",\"side\":\"Buy\","
                + "\"qty\":\"0\"}"),
        out);
    assertEquals(0L, out.qty);
  }

  @Test
  void parse_qtyMaxFractionalDigits_decodesExactly() {
    final var out = new MutableParsedMessage();
    BrowserMessageReader.parse(
        wrap(
            "{\"type\":\"QuoteRequest\",\"reqId\":\"R\",\"symbol\":\"E\",\"side\":\"Buy\","
                + "\"qty\":\"0.00000001\"}"),
        out);
    assertEquals(1L, out.qty);
  }

  @Test
  void parse_qtyNegative_decodesAsSignedInt64() {
    final var out = new MutableParsedMessage();
    BrowserMessageReader.parse(
        wrap(
            "{\"type\":\"QuoteRequest\",\"reqId\":\"R\",\"symbol\":\"E\",\"side\":\"Buy\","
                + "\"qty\":\"-1.5\"}"),
        out);
    assertEquals(-150_000_000L, out.qty);
  }

  @Test
  void parse_qtyTrailingZeroBeyondScale_isTolerated() {
    final var out = new MutableParsedMessage();
    BrowserMessageReader.parse(
        wrap(
            "{\"type\":\"QuoteRequest\",\"reqId\":\"R\",\"symbol\":\"E\",\"side\":\"Buy\","
                + "\"qty\":\"1.000000000\"}"),
        out);
    assertEquals(100_000_000L, out.qty);
  }

  @Test
  void parse_qtyNinthDigitNonZero_throwsPricePrecision() {
    final var out = new MutableParsedMessage();
    final var ex =
        assertThrows(
            JsonParseException.class,
            () ->
                BrowserMessageReader.parse(
                    wrap(
                        "{\"type\":\"QuoteRequest\",\"reqId\":\"R\",\"symbol\":\"E\","
                            + "\"side\":\"Buy\",\"qty\":\"1.000000005\"}"),
                    out));
    assertSame(JsonParseException.PRICE_PRECISION, ex);
  }

  @Test
  void parse_priceNinthDigitNonZero_throwsPricePrecision() {
    final var out = new MutableParsedMessage();
    final var ex =
        assertThrows(
            JsonParseException.class,
            () ->
                BrowserMessageReader.parse(
                    wrap(
                        "{\"type\":\"NewOrderSingle\",\"clOrdId\":\"C\",\"symbol\":\"E\","
                            + "\"side\":\"Buy\",\"qty\":\"1\",\"price\":\"1.234567891\","
                            + "\"ordType\":\"Limit\",\"timeInForce\":\"GTC\","
                            + "\"account\":\"A\"}"),
                    out));
    assertSame(JsonParseException.PRICE_PRECISION, ex);
  }

  @Test
  void parse_qtyOverflow_throwsMalformed() {
    final var out = new MutableParsedMessage();
    // 99_999_999_999.0 * 10^8 = 9_999_999_999_900_000_000 < Long.MAX_VALUE
    // (9_223_372_036_854_775_807)
    // 100_000_000_000.0 * 10^8 = 10_000_000_000_000_000_000 OVERFLOWS
    final var ex =
        assertThrows(
            JsonParseException.class,
            () ->
                BrowserMessageReader.parse(
                    wrap(
                        "{\"type\":\"QuoteRequest\",\"reqId\":\"R\",\"symbol\":\"E\","
                            + "\"side\":\"Buy\",\"qty\":\"100000000000.0\"}"),
                    out));
    assertSame(JsonParseException.MALFORMED, ex);
  }

  // ---------------------------------------------------------------------------
  // Negative paths.
  // ---------------------------------------------------------------------------

  @Test
  void parse_unknownTypeValue_throwsUnknownType() {
    final var out = new MutableParsedMessage();
    final var ex =
        assertThrows(
            JsonParseException.class,
            () -> BrowserMessageReader.parse(wrap("{\"type\":\"Bogus\"}"), out));
    assertSame(JsonParseException.UNKNOWN_TYPE, ex);
  }

  @Test
  void parse_emptyTypeValue_throwsUnknownType() {
    final var out = new MutableParsedMessage();
    final var ex =
        assertThrows(
            JsonParseException.class,
            () -> BrowserMessageReader.parse(wrap("{\"type\":\"\"}"), out));
    assertSame(JsonParseException.UNKNOWN_TYPE, ex);
  }

  @Test
  void parse_missingTypeKey_throwsUnknownType() {
    final var out = new MutableParsedMessage();
    final var ex =
        assertThrows(
            JsonParseException.class,
            () -> BrowserMessageReader.parse(wrap("{\"token\":\"x\"}"), out));
    assertSame(JsonParseException.UNKNOWN_TYPE, ex);
  }

  @Test
  void parse_unknownTopLevelKey_throwsMalformed() {
    final var out = new MutableParsedMessage();
    final var ex =
        assertThrows(
            JsonParseException.class,
            () ->
                BrowserMessageReader.parse(
                    wrap("{\"type\":\"Auth\",\"token\":\"x\",\"unknownField\":\"y\"}"), out));
    assertSame(JsonParseException.MALFORMED, ex);
  }

  @Test
  void parse_truncatedJson_throwsMalformed() {
    final var out = new MutableParsedMessage();
    final var ex =
        assertThrows(
            JsonParseException.class,
            () -> BrowserMessageReader.parse(wrap("{\"type\":\"Auth\""), out));
    assertSame(JsonParseException.MALFORMED, ex);
  }

  @Test
  void parse_missingColon_throwsMalformed() {
    final var out = new MutableParsedMessage();
    final var ex =
        assertThrows(
            JsonParseException.class,
            () -> BrowserMessageReader.parse(wrap("{\"type\" \"Auth\"}"), out));
    assertSame(JsonParseException.MALFORMED, ex);
  }

  @Test
  void parse_missingCommaBetweenKeys_throwsMalformed() {
    final var out = new MutableParsedMessage();
    final var ex =
        assertThrows(
            JsonParseException.class,
            () -> BrowserMessageReader.parse(wrap("{\"type\":\"Auth\" \"token\":\"x\"}"), out));
    assertSame(JsonParseException.MALFORMED, ex);
  }

  @Test
  void parse_trailingDataAfterClose_throwsMalformed() {
    final var out = new MutableParsedMessage();
    final var ex =
        assertThrows(
            JsonParseException.class,
            () ->
                BrowserMessageReader.parse(
                    wrap("{\"type\":\"Auth\",\"token\":\"x\"}garbage"), out));
    assertSame(JsonParseException.MALFORMED, ex);
  }

  @Test
  void parse_nestedObjectValue_throwsMalformed() {
    // depth 2+ rejection
    final var out = new MutableParsedMessage();
    final var ex =
        assertThrows(
            JsonParseException.class,
            () ->
                BrowserMessageReader.parse(
                    wrap("{\"type\":\"Auth\",\"token\":{\"x\":\"y\"}}"), out));
    assertSame(JsonParseException.MALFORMED, ex);
  }

  @Test
  void parse_nestedArrayValue_throwsMalformed() {
    final var out = new MutableParsedMessage();
    final var ex =
        assertThrows(
            JsonParseException.class,
            () -> BrowserMessageReader.parse(wrap("{\"type\":\"Auth\",\"token\":[\"x\"]}"), out));
    assertSame(JsonParseException.MALFORMED, ex);
  }

  @Test
  void parse_numericValueNotString_throwsMalformed() {
    final var out = new MutableParsedMessage();
    final var ex =
        assertThrows(
            JsonParseException.class,
            () ->
                BrowserMessageReader.parse(
                    wrap(
                        "{\"type\":\"QuoteRequest\",\"reqId\":\"R\",\"symbol\":\"E\","
                            + "\"side\":\"Buy\",\"qty\":1}"),
                    out));
    assertSame(JsonParseException.MALFORMED, ex);
  }

  @Test
  void parse_invalidSide_throwsMalformed() {
    final var out = new MutableParsedMessage();
    final var ex =
        assertThrows(
            JsonParseException.class,
            () ->
                BrowserMessageReader.parse(
                    wrap(
                        "{\"type\":\"QuoteRequest\",\"reqId\":\"R\",\"symbol\":\"E\","
                            + "\"side\":\"Up\",\"qty\":\"1\"}"),
                    out));
    assertSame(JsonParseException.MALFORMED, ex);
  }

  @Test
  void parse_invalidOrdType_throwsMalformed() {
    final var out = new MutableParsedMessage();
    final var ex =
        assertThrows(
            JsonParseException.class,
            () ->
                BrowserMessageReader.parse(
                    wrap(
                        "{\"type\":\"NewOrderSingle\",\"clOrdId\":\"C\",\"symbol\":\"E\","
                            + "\"side\":\"Buy\",\"qty\":\"1\",\"price\":\"1\","
                            + "\"ordType\":\"Stop\",\"timeInForce\":\"GTC\","
                            + "\"account\":\"A\"}"),
                    out));
    assertSame(JsonParseException.MALFORMED, ex);
  }

  @Test
  void parse_invalidTimeInForce_throwsMalformed() {
    final var out = new MutableParsedMessage();
    final var ex =
        assertThrows(
            JsonParseException.class,
            () ->
                BrowserMessageReader.parse(
                    wrap(
                        "{\"type\":\"NewOrderSingle\",\"clOrdId\":\"C\",\"symbol\":\"E\","
                            + "\"side\":\"Buy\",\"qty\":\"1\",\"price\":\"1\","
                            + "\"ordType\":\"Limit\",\"timeInForce\":\"XYZ\","
                            + "\"account\":\"A\"}"),
                    out));
    assertSame(JsonParseException.MALFORMED, ex);
  }

  @Test
  void parse_oversizeFrame_throwsTooLarge() {
    final var out = new MutableParsedMessage();
    // Build a frame > MAX_BYTES
    final var sb = new StringBuilder(BrowserMessageReader.MAX_BYTES + 16);
    sb.append("{\"type\":\"QuoteRequest\",\"reqId\":\"R\",\"symbol\":\"");
    while (sb.length() < BrowserMessageReader.MAX_BYTES + 1) {
      sb.append('X');
    }
    sb.append("\",\"side\":\"Buy\",\"qty\":\"1\"}");
    final var ex =
        assertThrows(
            JsonParseException.class, () -> BrowserMessageReader.parse(wrap(sb.toString()), out));
    assertSame(JsonParseException.TOO_LARGE, ex);
  }

  @Test
  void parse_emptyObject_throwsUnknownType() {
    final var out = new MutableParsedMessage();
    final var ex =
        assertThrows(JsonParseException.class, () -> BrowserMessageReader.parse(wrap("{}"), out));
    assertSame(JsonParseException.UNKNOWN_TYPE, ex);
  }

  @Test
  void parse_unescapedControlCharInValue_throwsMalformed() {
    final var out = new MutableParsedMessage();
    // Embed a literal newline (0x0A) inside a string value.
    final byte[] bad = ("{\"type\":\"Auth\",\"token\":\"a\nb\"}").getBytes(StandardCharsets.UTF_8);
    final var ex =
        assertThrows(
            JsonParseException.class,
            () -> BrowserMessageReader.parse(Unpooled.wrappedBuffer(bad), out));
    assertSame(JsonParseException.MALFORMED, ex);
  }

  @Test
  void parse_backslashInValue_throwsMalformed() {
    // Wire protocol forbids escapes — strings are pure ASCII pass-through.
    final var out = new MutableParsedMessage();
    final var ex =
        assertThrows(
            JsonParseException.class,
            () ->
                BrowserMessageReader.parse(wrap("{\"type\":\"Auth\",\"token\":\"a\\\"b\"}"), out));
    assertSame(JsonParseException.MALFORMED, ex);
  }

  @Test
  void parse_whitespaceTolerant_decodesSuccessfully() {
    final var out = new MutableParsedMessage();
    final int t =
        BrowserMessageReader.parse(wrap("  {  \"type\" : \"Auth\" , \"token\" : \"x\" }  "), out);
    assertEquals(MutableParsedMessage.TYPE_AUTH, t);
    assertEquals("x", slice(out, out.tokenOff, out.tokenLen));
  }

  // ---------------------------------------------------------------------------
  // Resetting / re-parse.
  // ---------------------------------------------------------------------------

  @Test
  void parse_reusedFlyweight_resetsBetweenInvocations() {
    final var out = new MutableParsedMessage();
    BrowserMessageReader.parse(wrap("{\"type\":\"Auth\",\"token\":\"first\"}"), out);
    assertEquals("first", slice(out, out.tokenOff, out.tokenLen));

    BrowserMessageReader.parse(wrap("{\"type\":\"RejectQuote\",\"quoteId\":\"Q\"}"), out);

    assertEquals(MutableParsedMessage.TYPE_REJECT_QUOTE, out.type);
    assertEquals(-1, out.tokenOff);
    assertEquals("Q", slice(out, out.quoteIdOff, out.quoteIdLen));
  }

  @Test
  void mutableParsedMessage_sliceBoundsCheck_throwsOnBadOffset() {
    final var m = new MutableParsedMessage();
    assertThrows(IndexOutOfBoundsException.class, () -> m.sliceAsString(0, m.scratch.length + 1));
  }

  @Test
  void parse_decimalDoubleSign_throwsMalformed() {
    final var out = new MutableParsedMessage();
    final var ex =
        assertThrows(
            JsonParseException.class,
            () ->
                BrowserMessageReader.parse(
                    wrap(
                        "{\"type\":\"QuoteRequest\",\"reqId\":\"R\",\"symbol\":\"E\","
                            + "\"side\":\"Buy\",\"qty\":\"--1\"}"),
                    out));
    assertSame(JsonParseException.MALFORMED, ex);
  }

  @Test
  void parse_decimalLeadingDot_decodesCorrectly() {
    final var out = new MutableParsedMessage();
    BrowserMessageReader.parse(
        wrap(
            "{\"type\":\"QuoteRequest\",\"reqId\":\"R\",\"symbol\":\"E\",\"side\":\"Buy\","
                + "\"qty\":\".5\"}"),
        out);
    assertEquals(50_000_000L, out.qty);
  }

  @Test
  void parse_decimalTrailingDot_decodesAsWhole() {
    final var out = new MutableParsedMessage();
    BrowserMessageReader.parse(
        wrap(
            "{\"type\":\"QuoteRequest\",\"reqId\":\"R\",\"symbol\":\"E\",\"side\":\"Buy\","
                + "\"qty\":\"5.\"}"),
        out);
    assertEquals(500_000_000L, out.qty);
  }

  @Test
  void parse_emptyDecimal_throwsMalformed() {
    final var out = new MutableParsedMessage();
    final var ex =
        assertThrows(
            JsonParseException.class,
            () ->
                BrowserMessageReader.parse(
                    wrap(
                        "{\"type\":\"QuoteRequest\",\"reqId\":\"R\",\"symbol\":\"E\","
                            + "\"side\":\"Buy\",\"qty\":\"\"}"),
                    out));
    assertSame(JsonParseException.MALFORMED, ex);
  }

  @Test
  void parse_jsonParseExceptionReason_matchesSingleton() {
    assertEquals("malformed", JsonParseException.MALFORMED.reason());
    assertEquals("unknown-type", JsonParseException.UNKNOWN_TYPE.reason());
    assertEquals("too-large", JsonParseException.TOO_LARGE.reason());
    assertEquals("price-precision", JsonParseException.PRICE_PRECISION.reason());
  }

  // ---------------------------------------------------------------------------
  // Sanity: scratch length fits.
  // ---------------------------------------------------------------------------

  @Test
  void mutableParsedMessage_scratchSize_matchesReaderCap() {
    assertEquals(BrowserMessageReader.MAX_BYTES, new MutableParsedMessage().scratch.length);
    assertTrue(BrowserMessageReader.MAX_BYTES == 65536);
  }
}
