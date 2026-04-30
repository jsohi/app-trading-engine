package com.trading.engine.fixbridge.translator;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.trading.engine.fix.builder.NewOrderSingleEncoder;
import com.trading.engine.fix.builder.OrderCancelRequestEncoder;
import com.trading.engine.fix.builder.QuoteRequestEncoder;
import com.trading.engine.fix.decoder.NewOrderSingleDecoder;
import com.trading.engine.fix.decoder.OrderCancelRequestDecoder;
import com.trading.engine.fix.decoder.QuoteRequestDecoder;
import com.trading.engine.fixbridge.json.BrowserMessageReader;
import com.trading.engine.fixbridge.json.JsonParseException;
import com.trading.engine.fixbridge.json.MutableParsedMessage;
import com.trading.engine.gateway.FixedPoint;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import org.agrona.concurrent.EpochNanoClock;
import org.junit.jupiter.api.Test;
import uk.co.real_logic.artio.fields.DecimalFloat;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

/**
 * Verifies {@link JsonToFixTranslator} produces byte-exact FIX 4.4 wire output for each inbound
 * browser command (locked §11), and applies the locked §4 ClOrdID format on the mint path. Each
 * test:
 *
 * <ol>
 *   <li>Builds a {@link MutableParsedMessage} flyweight via {@link BrowserMessageReader}.
 *   <li>Calls the matching {@code translate*} method against a fresh Artio encoder.
 *   <li>Encodes the populated encoder to wire bytes, then decodes via Artio's heap decoder.
 *   <li>Asserts the decoded fields match the expected wire shape.
 * </ol>
 *
 * <p>This pattern matches {@code SbeToFixTranslatorTest} in the {@code :gateway} module — encode →
 * decode → assert is the standard Artio idiom because the encoders write straight to wire and the
 * decoders are the canonical readers of that wire.
 */
final class JsonToFixTranslatorTest {

  /** Fixed wall-clock that returns a stable epoch-nanosecond timestamp for deterministic tests. */
  private static final EpochNanoClock FIXED_CLOCK = () -> 1_712_491_200_000_000_000L;

  // 2024-04-07T12:00:00Z — corresponds to FIXED_CLOCK above.

  /** Bridge process tag used in ClOrdID minting tests. */
  private static final long INSTANCE_TAG = 0xABCDEFL; // 6 hex digits

  /** Browser session id used in ClOrdID minting tests. */
  private static final long SESSION_ID = 0x1234567L; // 7 hex digits

  // ---------------------------------------------------------------------------
  // Helpers.
  // ---------------------------------------------------------------------------

  private static MutableParsedMessage parse(final String json) {
    final var msg = new MutableParsedMessage();
    final var src = Unpooled.wrappedBuffer(json.getBytes(StandardCharsets.UTF_8));
    BrowserMessageReader.parse(src, msg);
    return msg;
  }

  private static MutableAsciiBuffer encodeWire(final NewOrderSingleEncoder out) {
    final var hdr = out.header();
    hdr.senderCompID("BRIDGE").targetCompID("ACCEPTOR").msgSeqNum(1);
    hdr.sendingTime("20240407-12:00:00".getBytes(StandardCharsets.US_ASCII));
    final var wire = new MutableAsciiBuffer(new byte[2048]);
    final long encoded = out.encode(wire, 0);
    final int wireOffset = (int) (encoded >>> 32);
    final int wireLen = (int) encoded;
    final var view = new MutableAsciiBuffer(new byte[wireLen]);
    wire.getBytes(wireOffset, view, 0, wireLen);
    return view;
  }

  private static MutableAsciiBuffer encodeWire(final OrderCancelRequestEncoder out) {
    final var hdr = out.header();
    hdr.senderCompID("BRIDGE").targetCompID("ACCEPTOR").msgSeqNum(1);
    hdr.sendingTime("20240407-12:00:00".getBytes(StandardCharsets.US_ASCII));
    final var wire = new MutableAsciiBuffer(new byte[2048]);
    final long encoded = out.encode(wire, 0);
    final int wireOffset = (int) (encoded >>> 32);
    final int wireLen = (int) encoded;
    final var view = new MutableAsciiBuffer(new byte[wireLen]);
    wire.getBytes(wireOffset, view, 0, wireLen);
    return view;
  }

  private static MutableAsciiBuffer encodeWire(final QuoteRequestEncoder out) {
    final var hdr = out.header();
    hdr.senderCompID("BRIDGE").targetCompID("ACCEPTOR").msgSeqNum(1);
    hdr.sendingTime("20240407-12:00:00".getBytes(StandardCharsets.US_ASCII));
    final var wire = new MutableAsciiBuffer(new byte[2048]);
    final long encoded = out.encode(wire, 0);
    final int wireOffset = (int) (encoded >>> 32);
    final int wireLen = (int) encoded;
    final var view = new MutableAsciiBuffer(new byte[wireLen]);
    wire.getBytes(wireOffset, view, 0, wireLen);
    return view;
  }

  private static String charsToString(final char[] arr, final int len) {
    return new String(arr, 0, len);
  }

  // ===========================================================================
  // NewOrderSingle (35=D)
  // ===========================================================================

  @Test
  void translateNewOrderSingle_browserSuppliedClOrdId_passesThrough() {
    final var in =
        parse(
            "{\"type\":\"NewOrderSingle\",\"clOrdId\":\"BROWSER-1\",\"symbol\":\"EURUSD\","
                + "\"side\":\"Sell\",\"qty\":\"1000000\",\"price\":\"1.10000000\","
                + "\"ordType\":\"Limit\",\"timeInForce\":\"GTC\",\"account\":\"ACCT-1\"}");
    final var translator = new JsonToFixTranslator(FIXED_CLOCK);
    final var enc = new NewOrderSingleEncoder();

    final int rc = translator.translateNewOrderSingle(in, enc, SESSION_ID, INSTANCE_TAG, 1L);
    assertEquals(0, rc);

    final var wire = encodeWire(enc);
    final var dec = new NewOrderSingleDecoder();
    dec.decode(wire, 0, wire.capacity());

    assertEquals("BROWSER-1", charsToString(dec.clOrdID(), dec.clOrdIDLength()));
    assertEquals("EURUSD", charsToString(dec.symbol(), dec.symbolLength()));
    assertEquals('2', dec.side()); // Sell
    assertEquals('2', dec.ordType()); // Limit
    assertEquals('1', dec.timeInForce()); // GTC
    assertEquals('1', dec.handlInst()); // Pinned to '1' (Automated)
    assertEquals("ACCT-1", charsToString(dec.account(), dec.accountLength()));
    // qty 1_000_000 round-trips through FixedPoint (scale 8) → DecimalFloat normalises trailing
    // zeros so the wire form is (1_000_000, 0). Round-trip integrity asserted via the
    // value × 10^-scale equivalence: 1_000_000 × 10^0 == 1_000_000.
    assertEquals(1_000_000L, dec.orderQty().value());
    assertEquals(0, dec.orderQty().scale());
    // price 1.10 → FixedPoint (110_000_000, 8); DecimalFloat normalises to (11, 1) i.e. 1.1.
    assertEquals(11L, dec.price().value());
    assertEquals(1, dec.price().scale());
  }

  @Test
  void translateNewOrderSingle_omittedClOrdId_mintsLocked4Format() {
    // counter=1234 → "01234" decimal; instanceTag=0xABCDEF → "abcdef"; sessionId=0x1234567 →
    // "1234567"
    // Expected ClOrdID bytes: "abcdef-1234567-01234"
    final var in =
        parse(
            "{\"type\":\"NewOrderSingle\",\"symbol\":\"EURUSD\","
                + "\"side\":\"Buy\",\"qty\":\"100\",\"price\":\"1.0\","
                + "\"ordType\":\"Limit\",\"timeInForce\":\"IOC\",\"account\":\"A1\"}");
    final var translator = new JsonToFixTranslator(FIXED_CLOCK);
    final var enc = new NewOrderSingleEncoder();
    translator.translateNewOrderSingle(in, enc, SESSION_ID, INSTANCE_TAG, 1234L);

    final var wire = encodeWire(enc);
    final var dec = new NewOrderSingleDecoder();
    dec.decode(wire, 0, wire.capacity());

    assertEquals("abcdef-1234567-01234", charsToString(dec.clOrdID(), dec.clOrdIDLength()));
  }

  @Test
  void translateNewOrderSingle_browserSuppliedClOrdIdOver20Bytes_throws() {
    final var in =
        parse(
            "{\"type\":\"NewOrderSingle\",\"clOrdId\":\"THIS-CLOR-DID-IS-WAAAY-TOO-LONG\","
                + "\"symbol\":\"EURUSD\",\"side\":\"Buy\",\"qty\":\"100\",\"price\":\"1.0\","
                + "\"ordType\":\"Limit\",\"timeInForce\":\"IOC\",\"account\":\"A1\"}");
    final var translator = new JsonToFixTranslator(FIXED_CLOCK);
    final var enc = new NewOrderSingleEncoder();
    assertThrows(
        IllegalArgumentException.class,
        () -> translator.translateNewOrderSingle(in, enc, SESSION_ID, INSTANCE_TAG, 1L));
  }

  @Test
  void translateNewOrderSingle_wrongType_throws() {
    final var in =
        parse(
            "{\"type\":\"QuoteRequest\",\"reqId\":\"R\",\"symbol\":\"X\","
                + "\"side\":\"Buy\",\"qty\":\"1\"}");
    final var translator = new JsonToFixTranslator(FIXED_CLOCK);
    final var enc = new NewOrderSingleEncoder();
    assertThrows(
        IllegalArgumentException.class,
        () -> translator.translateNewOrderSingle(in, enc, SESSION_ID, INSTANCE_TAG, 1L));
  }

  // ===========================================================================
  // AcceptQuote → NewOrderSingle (35=D, OrdType=D)
  // ===========================================================================

  @Test
  void translateAcceptQuote_buySide_emitsPreviouslyQuotedNosWithOfferPrice() {
    final var in = parse("{\"type\":\"AcceptQuote\",\"quoteId\":\"Q-1\",\"clOrdId\":\"BC-1\"}");
    final var snap = new QuoteSnapshot();
    final var symbol = "EURUSD".getBytes(StandardCharsets.US_ASCII);
    final var bid = new DecimalFloat();
    final var ask = new DecimalFloat();
    FixedPoint.toDecimalFloat(110_000_000L, bid); // 1.10
    FixedPoint.toDecimalFloat(110_500_000L, ask); // 1.105

    snap.bind(
        symbol,
        0,
        symbol.length,
        MutableParsedMessage.SIDE_BUY,
        100_000_000_000L /* 1000.0 */,
        bid,
        ask,
        Long.MAX_VALUE);

    final var translator = new JsonToFixTranslator(FIXED_CLOCK);
    final var enc = new NewOrderSingleEncoder();
    final long token =
        translator.translateAcceptQuote(in, enc, snap, SESSION_ID, INSTANCE_TAG, 1L, 4242L);

    assertEquals(4242L, token);

    final var wire = encodeWire(enc);
    final var dec = new NewOrderSingleDecoder();
    dec.decode(wire, 0, wire.capacity());

    assertEquals("BC-1", charsToString(dec.clOrdID(), dec.clOrdIDLength()));
    assertEquals("EURUSD", charsToString(dec.symbol(), dec.symbolLength()));
    assertEquals('1', dec.side()); // Buy
    assertEquals('D', dec.ordType()); // Previously Quoted (locked §2)
    assertEquals('3', dec.timeInForce()); // IOC for previously-quoted
    assertEquals("Q-1", charsToString(dec.quoteID(), dec.quoteIDLength()));
    // Ask price (1.105) for Buy AcceptQuote — locked §9 round-trip via FixedPoint;
    // DecimalFloat normalises (110_500_000, 8) → (1105, 3) i.e. 1.105.
    assertEquals(1105L, dec.price().value());
    assertEquals(3, dec.price().scale());
    // Qty 1000.0 → FixedPoint (100_000_000_000, 8) → normalised (1000, 0).
    assertEquals(1000L, dec.orderQty().value());
    assertEquals(0, dec.orderQty().scale());
  }

  @Test
  void translateAcceptQuote_sellSide_takesBidPrice() {
    final var in = parse("{\"type\":\"AcceptQuote\",\"quoteId\":\"Q-2\",\"clOrdId\":\"SC-1\"}");
    final var snap = new QuoteSnapshot();
    final var symbol = "EURUSD".getBytes(StandardCharsets.US_ASCII);
    final var bid = new DecimalFloat();
    final var ask = new DecimalFloat();
    FixedPoint.toDecimalFloat(110_000_000L, bid);
    FixedPoint.toDecimalFloat(110_500_000L, ask);

    snap.bind(
        symbol,
        0,
        symbol.length,
        MutableParsedMessage.SIDE_SELL,
        50_000_000_000L,
        bid,
        ask,
        Long.MAX_VALUE);

    final var translator = new JsonToFixTranslator(FIXED_CLOCK);
    final var enc = new NewOrderSingleEncoder();
    translator.translateAcceptQuote(in, enc, snap, SESSION_ID, INSTANCE_TAG, 1L, 1L);

    final var wire = encodeWire(enc);
    final var dec = new NewOrderSingleDecoder();
    dec.decode(wire, 0, wire.capacity());

    assertEquals('2', dec.side()); // Sell
    // Bid 1.10 → normalised (11, 1).
    assertEquals(11L, dec.price().value());
    assertEquals(1, dec.price().scale());
  }

  @Test
  void translateAcceptQuote_unboundSnapshot_throws() {
    final var in = parse("{\"type\":\"AcceptQuote\",\"quoteId\":\"Q-X\",\"clOrdId\":\"C-X\"}");
    final var translator = new JsonToFixTranslator(FIXED_CLOCK);
    final var enc = new NewOrderSingleEncoder();
    final var emptySnap = new QuoteSnapshot();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            translator.translateAcceptQuote(in, enc, emptySnap, SESSION_ID, INSTANCE_TAG, 1L, 1L));
  }

  @Test
  void translateAcceptQuote_nullSnapshot_throws() {
    final var in = parse("{\"type\":\"AcceptQuote\",\"quoteId\":\"Q-X\",\"clOrdId\":\"C-X\"}");
    final var translator = new JsonToFixTranslator(FIXED_CLOCK);
    final var enc = new NewOrderSingleEncoder();
    assertThrows(
        IllegalArgumentException.class,
        () -> translator.translateAcceptQuote(in, enc, null, SESSION_ID, INSTANCE_TAG, 1L, 1L));
  }

  @Test
  void translateAcceptQuote_returnsCallerToken() {
    final var in = parse("{\"type\":\"AcceptQuote\",\"quoteId\":\"Q-1\",\"clOrdId\":\"C-1\"}");
    final var snap = new QuoteSnapshot();
    snap.bind(
        "X".getBytes(StandardCharsets.US_ASCII),
        0,
        1,
        MutableParsedMessage.SIDE_BUY,
        1_000L,
        null,
        null,
        Long.MAX_VALUE);

    final var translator = new JsonToFixTranslator(FIXED_CLOCK);
    final var enc = new NewOrderSingleEncoder();
    // Token is round-tripped verbatim — used by the dispatcher for cache eviction post-trySend.
    assertEquals(
        99L, translator.translateAcceptQuote(in, enc, snap, SESSION_ID, INSTANCE_TAG, 1L, 99L));
  }

  // ===========================================================================
  // CancelOrder (35=F)
  // ===========================================================================

  @Test
  void translateCancelOrder_browserSuppliedClOrdId_emitsValidFix() {
    final var in =
        parse(
            "{\"type\":\"CancelOrder\",\"clOrdId\":\"CXL-1\",\"origClOrdId\":\"ORIG-9\","
                + "\"symbol\":\"GBPUSD\",\"side\":\"Buy\"}");
    final var translator = new JsonToFixTranslator(FIXED_CLOCK);
    final var enc = new OrderCancelRequestEncoder();
    translator.translateCancelOrder(in, enc, SESSION_ID, INSTANCE_TAG, 5L);

    final var wire = encodeWire(enc);
    final var dec = new OrderCancelRequestDecoder();
    dec.decode(wire, 0, wire.capacity());

    assertEquals("CXL-1", charsToString(dec.clOrdID(), dec.clOrdIDLength()));
    assertEquals("ORIG-9", charsToString(dec.origClOrdID(), dec.origClOrdIDLength()));
    assertEquals("GBPUSD", charsToString(dec.symbol(), dec.symbolLength()));
    assertEquals('1', dec.side());
  }

  @Test
  void translateCancelOrder_omittedClOrdId_mintsLocked4Format() {
    final var in =
        parse(
            "{\"type\":\"CancelOrder\",\"origClOrdId\":\"ORIG-7\","
                + "\"symbol\":\"USDJPY\",\"side\":\"Sell\"}");
    final var translator = new JsonToFixTranslator(FIXED_CLOCK);
    final var enc = new OrderCancelRequestEncoder();
    translator.translateCancelOrder(in, enc, SESSION_ID, INSTANCE_TAG, 7L);

    final var wire = encodeWire(enc);
    final var dec = new OrderCancelRequestDecoder();
    dec.decode(wire, 0, wire.capacity());

    assertEquals("abcdef-1234567-00007", charsToString(dec.clOrdID(), dec.clOrdIDLength()));
  }

  // ===========================================================================
  // QuoteRequest (35=R)
  // ===========================================================================

  @Test
  void translateQuoteRequest_singleLegFromBrowserTuple_emitsCorrectFix() {
    final var in =
        parse(
            "{\"type\":\"QuoteRequest\",\"reqId\":\"R-1\",\"symbol\":\"EURUSD\","
                + "\"side\":\"Buy\",\"qty\":\"1000000\"}");
    final var translator = new JsonToFixTranslator(FIXED_CLOCK);
    final var enc = new QuoteRequestEncoder();
    translator.translateQuoteRequest(in, enc, SESSION_ID, INSTANCE_TAG, 1L);

    final var wire = encodeWire(enc);
    final var dec = new QuoteRequestDecoder();
    dec.decode(wire, 0, wire.capacity());

    assertEquals("R-1", charsToString(dec.quoteReqID(), dec.quoteReqIDLength()));
    final QuoteRequestDecoder.RelatedSymGroupDecoder rs = dec.relatedSymGroup();
    rs.next();
    assertEquals("EURUSD", charsToString(rs.symbol(), rs.symbolLength()));
    assertEquals('1', rs.side());
    assertEquals(1_000_000L, rs.orderQty().value());
    assertEquals(0, rs.orderQty().scale());
  }

  // ===========================================================================
  // RejectQuote (no FIX)
  // ===========================================================================

  @Test
  void handleRejectQuote_returnsNoFixSentinel() {
    final var in = parse("{\"type\":\"RejectQuote\",\"quoteId\":\"Q-1\"}");
    final var translator = new JsonToFixTranslator(FIXED_CLOCK);
    assertEquals(JsonToFixTranslator.NO_FIX_BYTES, translator.handleRejectQuote(in));
  }

  @Test
  void handleRejectQuote_wrongType_throws() {
    final var in = parse("{\"type\":\"AcceptQuote\",\"quoteId\":\"Q-1\",\"clOrdId\":\"C-1\"}");
    final var translator = new JsonToFixTranslator(FIXED_CLOCK);
    assertThrows(IllegalArgumentException.class, () -> translator.handleRejectQuote(in));
  }

  // ===========================================================================
  // mintClOrdId — locked §4 format invariant + parse round-trip.
  // ===========================================================================

  @Test
  void mintClOrdId_writesExactly20BytesInLocked4Format() {
    final var dst = new byte[24];
    JsonToFixTranslator.mintClOrdId(0xABCDEFL, 0x1234567L, 99999L, dst, 0);
    final var expected = "abcdef-1234567-99999".getBytes(StandardCharsets.US_ASCII);
    final var actual = new byte[20];
    System.arraycopy(dst, 0, actual, 0, 20);
    assertArrayEquals(expected, actual);
  }

  @Test
  void mintClOrdId_counterModulo100k_truncatesHighDigits() {
    final var dst = new byte[20];
    JsonToFixTranslator.mintClOrdId(0L, 0L, 1_234_567L, dst, 0);
    // Counter mod 100_000 = 34_567 → "34567"
    final var expected = "000000-0000000-34567".getBytes(StandardCharsets.US_ASCII);
    assertArrayEquals(expected, dst);
  }

  @Test
  void mintClOrdId_parseRoundTripWithLongParseLong_recoversValues() {
    // Locked §4 invariant: Long.parseLong on the (offset, end, radix) slices recovers the
    // three components. Use the public Java API verbatim — proves no custom parser is needed.
    final var dst = new byte[20];
    final long instanceTag = 0xFEDCBAL;
    final long sessionId = 0x0ABCDEFL;
    final long counter = 42L;
    JsonToFixTranslator.mintClOrdId(instanceTag, sessionId, counter, dst, 0);

    // The locked spec example is "Long.parseLong(buf, 0, 6, 16)" etc. We use
    // String.valueOf(buf) here because Java's CharSequence-overload of parseLong takes a
    // CharSequence, and a byte[] is not one. Functionally equivalent for ASCII.
    final var s = new String(dst, StandardCharsets.US_ASCII);
    assertEquals(instanceTag, Long.parseLong(s, 0, 6, 16));
    assertEquals(sessionId, Long.parseLong(s, 7, 14, 16));
    assertEquals(counter, Long.parseLong(s, 15, 20, 10));
  }

  @Test
  void mintClOrdId_dstTooSmall_throws() {
    final var dst = new byte[10];
    assertThrows(
        IllegalArgumentException.class, () -> JsonToFixTranslator.mintClOrdId(0L, 0L, 0L, dst, 0));
  }

  // ===========================================================================
  // parseDecimalToDecimalFloat — locked §3 + property-style coverage.
  // ===========================================================================

  @Test
  void parseDecimalToDecimalFloat_simplePositiveDecimal_yieldsValueAndScale() {
    final var df = new DecimalFloat();
    final var buf = "1.10000000".getBytes(StandardCharsets.US_ASCII);
    JsonToFixTranslator.parseDecimalToDecimalFloat(buf, 0, buf.length, df);
    // DecimalFloat.set normalises trailing zeros: raw (110_000_000, 8) → (11, 1) i.e. 1.1.
    assertEquals(11L, df.value());
    assertEquals(1, df.scale());
  }

  @Test
  void parseDecimalToDecimalFloat_negativeNoFraction_yieldsScaleZero() {
    final var df = new DecimalFloat();
    final var buf = "-1234".getBytes(StandardCharsets.US_ASCII);
    JsonToFixTranslator.parseDecimalToDecimalFloat(buf, 0, buf.length, df);
    assertEquals(-1234L, df.value());
    assertEquals(0, df.scale());
  }

  @Test
  void parseDecimalToDecimalFloat_trailingZeroAtScale9_tolerated() {
    final var df = new DecimalFloat();
    final var buf = "1.100000000".getBytes(StandardCharsets.US_ASCII); // 9 frac digits, last 0
    JsonToFixTranslator.parseDecimalToDecimalFloat(buf, 0, buf.length, df);
    // Trailing zero beyond 8 frac digits is tolerated (parser does not extend scale); the
    // DecimalFloat normaliser then strips the in-scale trailing zeros: raw (110_000_000, 8) →
    // (11, 1).
    assertEquals(11L, df.value());
    assertEquals(1, df.scale());
  }

  @Test
  void parseDecimalToDecimalFloat_nonzeroAtScale9_throwsPricePrecision() {
    final var df = new DecimalFloat();
    final var buf = "1.100000001".getBytes(StandardCharsets.US_ASCII);
    final var thrown =
        assertThrows(
            JsonParseException.class,
            () -> JsonToFixTranslator.parseDecimalToDecimalFloat(buf, 0, buf.length, df));
    assertSame(JsonParseException.PRICE_PRECISION, thrown);
  }

  @Test
  void parseDecimalToDecimalFloat_emptySlice_throwsMalformed() {
    final var df = new DecimalFloat();
    final var buf = new byte[0];
    final var thrown =
        assertThrows(
            JsonParseException.class,
            () -> JsonToFixTranslator.parseDecimalToDecimalFloat(buf, 0, 0, df));
    assertSame(JsonParseException.MALFORMED, thrown);
  }

  @Test
  void parseDecimalToDecimalFloat_doubleDecimalPoint_throwsMalformed() {
    final var df = new DecimalFloat();
    final var buf = "1.2.3".getBytes(StandardCharsets.US_ASCII);
    final var thrown =
        assertThrows(
            JsonParseException.class,
            () -> JsonToFixTranslator.parseDecimalToDecimalFloat(buf, 0, buf.length, df));
    assertSame(JsonParseException.MALFORMED, thrown);
  }

  @Test
  void parseDecimalToDecimalFloat_nonDigit_throwsMalformed() {
    final var df = new DecimalFloat();
    final var buf = "1.2x".getBytes(StandardCharsets.US_ASCII);
    final var thrown =
        assertThrows(
            JsonParseException.class,
            () -> JsonToFixTranslator.parseDecimalToDecimalFloat(buf, 0, buf.length, df));
    assertSame(JsonParseException.MALFORMED, thrown);
  }

  @Test
  void parseDecimalToDecimalFloat_trailingDecimalPoint_throwsMalformed() {
    // RFC 8259 §6: `5.` is malformed — a `.` must be followed by at least one digit. The
    // BrowserMessageReader already rejects this (see its decodeFixedPoint); this regression test
    // pins the same strictness contract on parseDecimalToDecimalFloat so the bridge rejects
    // identical inputs at every layer.
    final var df = new DecimalFloat();
    final var buf = "5.".getBytes(StandardCharsets.US_ASCII);
    final var thrown =
        assertThrows(
            JsonParseException.class,
            () -> JsonToFixTranslator.parseDecimalToDecimalFloat(buf, 0, buf.length, df));
    assertSame(JsonParseException.MALFORMED, thrown);
  }

  @Test
  void parseDecimalToDecimalFloat_negativeTrailingDecimalPoint_throwsMalformed() {
    final var df = new DecimalFloat();
    final var buf = "-5.".getBytes(StandardCharsets.US_ASCII);
    final var thrown =
        assertThrows(
            JsonParseException.class,
            () -> JsonToFixTranslator.parseDecimalToDecimalFloat(buf, 0, buf.length, df));
    assertSame(JsonParseException.MALFORMED, thrown);
  }

  // ===========================================================================
  // Constructor.
  // ===========================================================================

  @Test
  void constructor_nullClock_throws() {
    assertThrows(IllegalArgumentException.class, () -> new JsonToFixTranslator(null));
  }
}
