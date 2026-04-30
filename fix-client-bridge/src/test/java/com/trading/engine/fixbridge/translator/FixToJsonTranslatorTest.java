package com.trading.engine.fixbridge.translator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.fix.builder.BusinessMessageRejectEncoder;
import com.trading.engine.fix.builder.ExecutionReportEncoder;
import com.trading.engine.fix.builder.OrderCancelRejectEncoder;
import com.trading.engine.fix.builder.QuoteEncoder;
import com.trading.engine.fix.builder.QuoteRequestRejectEncoder;
import com.trading.engine.fix.builder.RejectEncoder;
import com.trading.engine.fix.decoder.BusinessMessageRejectDecoder;
import com.trading.engine.fix.decoder.ExecutionReportDecoder;
import com.trading.engine.fix.decoder.OrderCancelRejectDecoder;
import com.trading.engine.fix.decoder.QuoteDecoder;
import com.trading.engine.fix.decoder.QuoteRequestRejectDecoder;
import com.trading.engine.fix.decoder.RejectDecoder;
import com.trading.engine.gateway.FixedPoint;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import org.agrona.concurrent.EpochNanoClock;
import org.junit.jupiter.api.Test;
import uk.co.real_logic.artio.fields.DecimalFloat;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

/**
 * Verifies {@link FixToJsonTranslator} produces byte-exact JSON wire output for each inbound FIX
 * 4.4 message type per locked §17. Each test:
 *
 * <ol>
 *   <li>Builds a populated FIX encoder.
 *   <li>Encodes to wire bytes via {@code encode(MutableAsciiBuffer, 0)}.
 *   <li>Decodes the wire bytes into the matching {@code Decoder}.
 *   <li>Invokes the matching {@code translateXxx} into a Netty {@link ByteBuf}.
 *   <li>Asserts the resulting JSON bytes match the expected wire form.
 * </ol>
 */
final class FixToJsonTranslatorTest {

  /** Stable wall-clock for deterministic expiry computations. */
  private static final long FIXED_NS = 1_712_491_200_000_000_000L;

  private static final EpochNanoClock FIXED_CLOCK = () -> FIXED_NS;

  // ---------------------------------------------------------------------------
  // Helpers.
  // ---------------------------------------------------------------------------

  private static FixToJsonTranslator newTranslator() {
    return new FixToJsonTranslator(new DecimalStringEmitter(), FIXED_CLOCK);
  }

  private static FixToJsonTranslator newTranslator(final EpochNanoClock clock) {
    return new FixToJsonTranslator(new DecimalStringEmitter(), clock);
  }

  /**
   * Encodes the given encoder to a fresh wire buffer and decodes via {@code decode}. Returns the
   * decoded view ready for {@code translateXxx}.
   */
  private static MutableAsciiBuffer encodeFor(final ExecutionReportEncoder enc) {
    final var hdr = enc.header();
    hdr.senderCompID("ACCEPTOR").targetCompID("BRIDGE").msgSeqNum(1);
    hdr.sendingTime("20240407-12:00:00".getBytes(StandardCharsets.US_ASCII));
    final var wire = new MutableAsciiBuffer(new byte[4096]);
    final long encoded = enc.encode(wire, 0);
    final int wireOffset = (int) (encoded >>> 32);
    final int wireLen = (int) encoded;
    final var view = new MutableAsciiBuffer(new byte[wireLen]);
    wire.getBytes(wireOffset, view, 0, wireLen);
    return view;
  }

  private static MutableAsciiBuffer encodeFor(final QuoteEncoder enc) {
    final var hdr = enc.header();
    hdr.senderCompID("ACCEPTOR").targetCompID("BRIDGE").msgSeqNum(1);
    hdr.sendingTime("20240407-12:00:00".getBytes(StandardCharsets.US_ASCII));
    final var wire = new MutableAsciiBuffer(new byte[4096]);
    final long encoded = enc.encode(wire, 0);
    final int wireOffset = (int) (encoded >>> 32);
    final int wireLen = (int) encoded;
    final var view = new MutableAsciiBuffer(new byte[wireLen]);
    wire.getBytes(wireOffset, view, 0, wireLen);
    return view;
  }

  private static MutableAsciiBuffer encodeFor(final OrderCancelRejectEncoder enc) {
    final var hdr = enc.header();
    hdr.senderCompID("ACCEPTOR").targetCompID("BRIDGE").msgSeqNum(1);
    hdr.sendingTime("20240407-12:00:00".getBytes(StandardCharsets.US_ASCII));
    final var wire = new MutableAsciiBuffer(new byte[4096]);
    final long encoded = enc.encode(wire, 0);
    final int wireOffset = (int) (encoded >>> 32);
    final int wireLen = (int) encoded;
    final var view = new MutableAsciiBuffer(new byte[wireLen]);
    wire.getBytes(wireOffset, view, 0, wireLen);
    return view;
  }

  private static MutableAsciiBuffer encodeFor(final QuoteRequestRejectEncoder enc) {
    final var hdr = enc.header();
    hdr.senderCompID("ACCEPTOR").targetCompID("BRIDGE").msgSeqNum(1);
    hdr.sendingTime("20240407-12:00:00".getBytes(StandardCharsets.US_ASCII));
    final var wire = new MutableAsciiBuffer(new byte[4096]);
    final long encoded = enc.encode(wire, 0);
    final int wireOffset = (int) (encoded >>> 32);
    final int wireLen = (int) encoded;
    final var view = new MutableAsciiBuffer(new byte[wireLen]);
    wire.getBytes(wireOffset, view, 0, wireLen);
    return view;
  }

  private static MutableAsciiBuffer encodeFor(final BusinessMessageRejectEncoder enc) {
    final var hdr = enc.header();
    hdr.senderCompID("ACCEPTOR").targetCompID("BRIDGE").msgSeqNum(1);
    hdr.sendingTime("20240407-12:00:00".getBytes(StandardCharsets.US_ASCII));
    final var wire = new MutableAsciiBuffer(new byte[4096]);
    final long encoded = enc.encode(wire, 0);
    final int wireOffset = (int) (encoded >>> 32);
    final int wireLen = (int) encoded;
    final var view = new MutableAsciiBuffer(new byte[wireLen]);
    wire.getBytes(wireOffset, view, 0, wireLen);
    return view;
  }

  private static MutableAsciiBuffer encodeFor(final RejectEncoder enc) {
    final var hdr = enc.header();
    hdr.senderCompID("ACCEPTOR").targetCompID("BRIDGE").msgSeqNum(1);
    hdr.sendingTime("20240407-12:00:00".getBytes(StandardCharsets.US_ASCII));
    final var wire = new MutableAsciiBuffer(new byte[4096]);
    final long encoded = enc.encode(wire, 0);
    final int wireOffset = (int) (encoded >>> 32);
    final int wireLen = (int) encoded;
    final var view = new MutableAsciiBuffer(new byte[wireLen]);
    wire.getBytes(wireOffset, view, 0, wireLen);
    return view;
  }

  private static String readAll(final ByteBuf buf) {
    final var out = new byte[buf.readableBytes()];
    buf.readBytes(out);
    return new String(out, StandardCharsets.UTF_8);
  }

  // ===========================================================================
  // ExecutionReport (35=8)
  // ===========================================================================

  @Test
  void translateExecutionReport_filledOrder_emitsCanonicalJson() {
    final var enc = new ExecutionReportEncoder();
    enc.orderID("ORD-1");
    enc.execID("EXEC-1");
    enc.clOrdID("CL-1");
    enc.execType('F'); // Trade
    enc.ordStatus('2'); // Filled
    enc.side('1'); // Buy
    final var qty = new DecimalFloat();
    FixedPoint.toDecimalFloat(100_000_000_000L, qty); // 1000.0
    enc.leavesQty(qty);
    enc.cumQty(qty);
    final var px = new DecimalFloat();
    FixedPoint.toDecimalFloat(110_000_000L, px); // 1.10
    enc.avgPx(px);
    enc.instrument().symbol("EURUSD");
    final var ts = "20240407-12:00:00".getBytes(StandardCharsets.US_ASCII);
    enc.transactTime(ts, 0, ts.length);

    final var wire = encodeFor(enc);
    final var dec = new ExecutionReportDecoder();
    dec.decode(wire, 0, wire.capacity());

    final var out = Unpooled.buffer(512);
    final int written = newTranslator().translateExecutionReport(dec, out);
    assertTrue(written > 0);
    final var json = readAll(out);
    assertEquals(
        "{\"type\":\"ExecutionReport\""
            + ",\"clOrdId\":\"CL-1\""
            + ",\"execId\":\"EXEC-1\""
            + ",\"execType\":\"F\""
            + ",\"ordStatus\":\"2\""
            + ",\"symbol\":\"EURUSD\""
            + ",\"side\":\"Buy\""
            + ",\"cumQty\":\"1000.00000000\""
            + ",\"leavesQty\":\"1000.00000000\""
            + ",\"avgPx\":\"1.10000000\""
            + "}",
        json);
  }

  @Test
  void translateExecutionReport_sellSide_emitsSellLiteral() {
    final var enc = new ExecutionReportEncoder();
    enc.orderID("O").execID("E").clOrdID("C");
    enc.execType('0').ordStatus('0').side('2'); // Sell
    final var zero = new DecimalFloat();
    FixedPoint.toDecimalFloat(0L, zero);
    enc.leavesQty(zero).cumQty(zero).avgPx(zero);
    enc.instrument().symbol("X");
    enc.transactTime("20240407-12:00:00".getBytes(StandardCharsets.US_ASCII));

    final var wire = encodeFor(enc);
    final var dec = new ExecutionReportDecoder();
    dec.decode(wire, 0, wire.capacity());

    final var out = Unpooled.buffer(256);
    newTranslator().translateExecutionReport(dec, out);
    final var json = readAll(out);
    assertTrue(json.contains("\"side\":\"Sell\""), json);
  }

  // ===========================================================================
  // Quote (35=S) — see also FixToJsonQuoteExpiryTest for the §8 paths.
  // ===========================================================================

  @Test
  void translateQuote_buySide_emitsOfferAsPrice() {
    // Build a Quote with QuoteReqID/QuoteID/Symbol/BidPx/OfferPx/OrderQty/Side.
    final var enc = new QuoteEncoder();
    enc.quoteReqID("R-1");
    enc.quoteID("Q-1");
    enc.instrument().symbol("EURUSD");
    enc.side('1'); // Buy from requester perspective
    final var bid = new DecimalFloat();
    FixedPoint.toDecimalFloat(110_000_000L, bid);
    enc.bidPx(bid);
    final var offer = new DecimalFloat();
    FixedPoint.toDecimalFloat(110_500_000L, offer);
    enc.offerPx(offer);
    final var q = new DecimalFloat();
    FixedPoint.toDecimalFloat(100_000_000_000L, q);
    enc.orderQtyData().orderQty(q);
    // ValidUntilTime — far in the future relative to FIXED_CLOCK so the parsed value passes the
    // safety margin.
    enc.validUntilTime("20990101-00:00:00".getBytes(StandardCharsets.US_ASCII));

    final var wire = encodeFor(enc);
    final var dec = new QuoteDecoder();
    dec.decode(wire, 0, wire.capacity());

    final var out = Unpooled.buffer(512);
    newTranslator().translateQuote(dec, out);
    final var json = readAll(out);
    // Buy AcceptQuote takes offer side (1.105), so price field on outbound Quote uses offer.
    assertTrue(json.startsWith("{\"type\":\"Quote\""), json);
    assertTrue(json.contains("\"reqId\":\"R-1\""), json);
    assertTrue(json.contains("\"quoteId\":\"Q-1\""), json);
    assertTrue(json.contains("\"symbol\":\"EURUSD\""), json);
    assertTrue(json.contains("\"side\":\"Buy\""), json);
    assertTrue(json.contains("\"qty\":\"1000.00000000\""), json);
    assertTrue(json.contains("\"price\":\"1.10500000\""), json);
    assertTrue(json.contains("\"expiryNs\":"), json);
  }

  @Test
  void translateQuote_sellSide_emitsBidAsPrice() {
    final var enc = new QuoteEncoder();
    enc.quoteReqID("R-1");
    enc.quoteID("Q-2");
    enc.instrument().symbol("EURUSD");
    enc.side('2'); // Sell from requester perspective
    final var bid = new DecimalFloat();
    FixedPoint.toDecimalFloat(110_000_000L, bid);
    enc.bidPx(bid);
    final var offer = new DecimalFloat();
    FixedPoint.toDecimalFloat(110_500_000L, offer);
    enc.offerPx(offer);
    final var q = new DecimalFloat();
    FixedPoint.toDecimalFloat(50_000_000_000L, q);
    enc.orderQtyData().orderQty(q);
    enc.validUntilTime("20990101-00:00:00".getBytes(StandardCharsets.US_ASCII));

    final var wire = encodeFor(enc);
    final var dec = new QuoteDecoder();
    dec.decode(wire, 0, wire.capacity());

    final var out = Unpooled.buffer(512);
    newTranslator().translateQuote(dec, out);
    final var json = readAll(out);
    assertTrue(json.contains("\"side\":\"Sell\""), json);
    assertTrue(json.contains("\"price\":\"1.10000000\""), json); // Bid for Sell
  }

  // ===========================================================================
  // OrderCancelReject (35=9)
  // ===========================================================================

  @Test
  void translateOrderCancelReject_withText_emitsCancelRejectPrefix() {
    final var enc = new OrderCancelRejectEncoder();
    enc.orderID("O-1");
    enc.clOrdID("CXL-1");
    enc.origClOrdID("ORIG-1");
    enc.ordStatus('8'); // Rejected
    enc.cxlRejResponseTo('1');
    enc.text("unknown order");

    final var wire = encodeFor(enc);
    final var dec = new OrderCancelRejectDecoder();
    dec.decode(wire, 0, wire.capacity());

    final var out = Unpooled.buffer(256);
    newTranslator().translateOrderCancelReject(dec, out);
    final var json = readAll(out);
    assertEquals(
        "{\"type\":\"OrderReject\""
            + ",\"clOrdId\":\"CXL-1\""
            + ",\"reason\":\"cancel-reject:unknown order\""
            + "}",
        json);
  }

  @Test
  void translateOrderCancelReject_emptyText_stillEmitsPrefix() {
    final var enc = new OrderCancelRejectEncoder();
    enc.orderID("O").clOrdID("CXL").origClOrdID("ORIG").ordStatus('8').cxlRejResponseTo('1');

    final var wire = encodeFor(enc);
    final var dec = new OrderCancelRejectDecoder();
    dec.decode(wire, 0, wire.capacity());

    final var out = Unpooled.buffer(256);
    newTranslator().translateOrderCancelReject(dec, out);
    final var json = readAll(out);
    assertTrue(json.contains("\"reason\":\"cancel-reject:\""), json);
  }

  // ===========================================================================
  // QuoteRequestReject (35=AG)
  // ===========================================================================

  @Test
  void translateQuoteRequestReject_emitsErrorWithReceivedReqId() {
    final var enc = new QuoteRequestRejectEncoder();
    enc.quoteReqID("R-7");
    enc.quoteRequestRejectReason(1);
    enc.text("instrument unknown");

    final var wire = encodeFor(enc);
    final var dec = new QuoteRequestRejectDecoder();
    dec.decode(wire, 0, wire.capacity());

    final var out = Unpooled.buffer(256);
    newTranslator().translateQuoteRequestReject(dec, out);
    final var json = readAll(out);
    assertEquals(
        "{\"type\":\"Error\""
            + ",\"reason\":\"quote-rejected:instrument unknown\""
            + ",\"received\":\"QuoteRequest:R-7\""
            + "}",
        json);
  }

  // ===========================================================================
  // BusinessMessageReject (35=j)
  // ===========================================================================

  @Test
  void translateBusinessMessageReject_emitsFixRejectPrefix() {
    final var enc = new BusinessMessageRejectEncoder();
    enc.refMsgType("D");
    enc.businessRejectReason(0);
    enc.text("conditionally required field missing");

    final var wire = encodeFor(enc);
    final var dec = new BusinessMessageRejectDecoder();
    dec.decode(wire, 0, wire.capacity());

    final var out = Unpooled.buffer(256);
    newTranslator().translateBusinessMessageReject(dec, out);
    final var json = readAll(out);
    assertEquals(
        "{\"type\":\"Error\""
            + ",\"reason\":\"fix-reject:conditionally required field missing\""
            + "}",
        json);
  }

  // ===========================================================================
  // Reject (35=3)
  // ===========================================================================

  @Test
  void translateReject_emitsFixRejectPrefix() {
    final var enc = new RejectEncoder();
    enc.refSeqNum(42);
    enc.sessionRejectReason(99);
    enc.text("session error");

    final var wire = encodeFor(enc);
    final var dec = new RejectDecoder();
    dec.decode(wire, 0, wire.capacity());

    final var out = Unpooled.buffer(256);
    newTranslator().translateReject(dec, out);
    final var json = readAll(out);
    assertEquals("{\"type\":\"Error\",\"reason\":\"fix-reject:session error\"}", json);
  }

  // ===========================================================================
  // Constructor.
  // ===========================================================================

  @Test
  void constructor_nullEmitter_throws() {
    assertThrows(IllegalArgumentException.class, () -> new FixToJsonTranslator(null, FIXED_CLOCK));
  }

  @Test
  void constructor_nullClock_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new FixToJsonTranslator(new DecimalStringEmitter(), null));
  }
}
