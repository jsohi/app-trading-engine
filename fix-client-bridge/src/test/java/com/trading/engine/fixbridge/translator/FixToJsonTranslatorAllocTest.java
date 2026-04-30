package com.trading.engine.fixbridge.translator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.fix.builder.ExecutionReportEncoder;
import com.trading.engine.fix.builder.OrderCancelRejectEncoder;
import com.trading.engine.fix.builder.QuoteEncoder;
import com.trading.engine.fix.decoder.ExecutionReportDecoder;
import com.trading.engine.fix.decoder.OrderCancelRejectDecoder;
import com.trading.engine.fix.decoder.QuoteDecoder;
import com.trading.engine.gateway.FixedPoint;
import io.netty.buffer.Unpooled;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import org.agrona.concurrent.EpochNanoClock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import uk.co.real_logic.artio.fields.DecimalFloat;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

/**
 * Allocation regression tripwire for {@link FixToJsonTranslator}. Same {@code
 * GarbageCollectorMXBean.getCollectionCount()} delta pattern as the {@code JsonToFixTranslatorAlloc
 * Test} sibling.
 *
 * <p>Gated by {@code -DrunAllocTests=true}.
 */
@EnabledIfSystemProperty(named = "runAllocTests", matches = "true")
final class FixToJsonTranslatorAllocTest {

  private static final int WARMUP_ITERATIONS = 5_000;
  private static final int MEASURED_ITERATIONS = 10_000;

  private static final EpochNanoClock CLOCK = () -> 1_712_491_200_000_000_000L;

  // ---------------------------------------------------------------------------
  // Helpers — build a one-shot decoder that the alloc loop reuses across all
  // iterations. We pre-construct the decoder OUTSIDE the steady-state loop so
  // its setup cost is excluded from the measurement.
  // ---------------------------------------------------------------------------

  private static ExecutionReportDecoder buildExecutionReportDecoder() {
    final var enc = new ExecutionReportEncoder();
    enc.orderID("ORD-1").execID("EXEC-1").clOrdID("CL-1");
    enc.execType('F').ordStatus('2').side('1');
    final var qty = new DecimalFloat();
    FixedPoint.toDecimalFloat(100_000_000_000L, qty);
    enc.leavesQty(qty).cumQty(qty);
    final var px = new DecimalFloat();
    FixedPoint.toDecimalFloat(110_000_000L, px);
    enc.avgPx(px);
    enc.instrument().symbol("EURUSD");
    enc.transactTime("20240407-12:00:00".getBytes(StandardCharsets.US_ASCII));
    return decode(enc);
  }

  private static QuoteDecoder buildQuoteDecoder() {
    final var enc = new QuoteEncoder();
    enc.quoteReqID("R-1").quoteID("Q-1");
    enc.instrument().symbol("EURUSD");
    enc.side('1');
    final var bid = new DecimalFloat();
    FixedPoint.toDecimalFloat(110_000_000L, bid);
    enc.bidPx(bid);
    final var offer = new DecimalFloat();
    FixedPoint.toDecimalFloat(110_500_000L, offer);
    enc.offerPx(offer);
    final var qty = new DecimalFloat();
    FixedPoint.toDecimalFloat(100_000_000_000L, qty);
    enc.orderQtyData().orderQty(qty);
    enc.validUntilTime("20990101-00:00:00".getBytes(StandardCharsets.US_ASCII));
    final var hdr = enc.header();
    hdr.senderCompID("ACCEPTOR").targetCompID("BRIDGE").msgSeqNum(1);
    hdr.sendingTime("20240407-12:00:00".getBytes(StandardCharsets.US_ASCII));
    final var wire = new MutableAsciiBuffer(new byte[4096]);
    final long encoded = enc.encode(wire, 0);
    final int wireOffset = (int) (encoded >>> 32);
    final int wireLen = (int) encoded;
    final var view = new MutableAsciiBuffer(new byte[wireLen]);
    wire.getBytes(wireOffset, view, 0, wireLen);
    final var dec = new QuoteDecoder();
    dec.decode(view, 0, view.capacity());
    return dec;
  }

  private static OrderCancelRejectDecoder buildOrderCancelRejectDecoder() {
    final var enc = new OrderCancelRejectEncoder();
    enc.orderID("O-1").clOrdID("CXL-1").origClOrdID("ORIG-1").ordStatus('8').cxlRejResponseTo('1');
    enc.text("unknown order");
    final var hdr = enc.header();
    hdr.senderCompID("ACCEPTOR").targetCompID("BRIDGE").msgSeqNum(1);
    hdr.sendingTime("20240407-12:00:00".getBytes(StandardCharsets.US_ASCII));
    final var wire = new MutableAsciiBuffer(new byte[2048]);
    final long encoded = enc.encode(wire, 0);
    final int wireOffset = (int) (encoded >>> 32);
    final int wireLen = (int) encoded;
    final var view = new MutableAsciiBuffer(new byte[wireLen]);
    wire.getBytes(wireOffset, view, 0, wireLen);
    final var dec = new OrderCancelRejectDecoder();
    dec.decode(view, 0, view.capacity());
    return dec;
  }

  private static ExecutionReportDecoder decode(final ExecutionReportEncoder enc) {
    final var hdr = enc.header();
    hdr.senderCompID("ACCEPTOR").targetCompID("BRIDGE").msgSeqNum(1);
    hdr.sendingTime("20240407-12:00:00".getBytes(StandardCharsets.US_ASCII));
    final var wire = new MutableAsciiBuffer(new byte[4096]);
    final long encoded = enc.encode(wire, 0);
    final int wireOffset = (int) (encoded >>> 32);
    final int wireLen = (int) encoded;
    final var view = new MutableAsciiBuffer(new byte[wireLen]);
    wire.getBytes(wireOffset, view, 0, wireLen);
    final var dec = new ExecutionReportDecoder();
    dec.decode(view, 0, view.capacity());
    return dec;
  }

  @Test
  void translateExecutionReport_steadyState_doesNotAdvanceGcCount() {
    final var dec = buildExecutionReportDecoder();
    final var t = new FixToJsonTranslator(new DecimalStringEmitter(), CLOCK);
    final var out = Unpooled.buffer(512);

    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      out.clear();
      t.translateExecutionReport(dec, out);
    }

    final long beforeGc = totalGcCount();
    for (int i = 0; i < MEASURED_ITERATIONS; i++) {
      out.clear();
      final int n = t.translateExecutionReport(dec, out);
      assertTrue(n > 0);
    }
    final long afterGc = totalGcCount();
    assertEquals(
        beforeGc,
        afterGc,
        "translateExecutionReport advanced GC count " + beforeGc + "→" + afterGc);
  }

  @Test
  void translateQuote_steadyState_doesNotAdvanceGcCount() {
    final var dec = buildQuoteDecoder();
    final var t = new FixToJsonTranslator(new DecimalStringEmitter(), CLOCK);
    final var out = Unpooled.buffer(512);

    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      out.clear();
      t.translateQuote(dec, out);
    }

    final long beforeGc = totalGcCount();
    for (int i = 0; i < MEASURED_ITERATIONS; i++) {
      out.clear();
      t.translateQuote(dec, out);
    }
    final long afterGc = totalGcCount();
    assertEquals(beforeGc, afterGc);
  }

  @Test
  void translateOrderCancelReject_steadyState_doesNotAdvanceGcCount() {
    final var dec = buildOrderCancelRejectDecoder();
    final var t = new FixToJsonTranslator(new DecimalStringEmitter(), CLOCK);
    final var out = Unpooled.buffer(512);

    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      out.clear();
      t.translateOrderCancelReject(dec, out);
    }

    final long beforeGc = totalGcCount();
    for (int i = 0; i < MEASURED_ITERATIONS; i++) {
      out.clear();
      t.translateOrderCancelReject(dec, out);
    }
    final long afterGc = totalGcCount();
    assertEquals(beforeGc, afterGc);
  }

  private static long totalGcCount() {
    long total = 0L;
    final var beans = ManagementFactory.getGarbageCollectorMXBeans();
    for (final var bean : beans) {
      final long c = bean.getCollectionCount();
      if (c >= 0L) {
        total += c;
      }
    }
    return total;
  }
}
