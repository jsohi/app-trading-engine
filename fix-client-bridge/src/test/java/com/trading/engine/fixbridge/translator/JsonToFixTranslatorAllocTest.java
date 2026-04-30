package com.trading.engine.fixbridge.translator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.fix.builder.NewOrderSingleEncoder;
import com.trading.engine.fix.builder.OrderCancelRequestEncoder;
import com.trading.engine.fix.builder.QuoteRequestEncoder;
import com.trading.engine.fixbridge.json.BrowserMessageReader;
import com.trading.engine.fixbridge.json.MutableParsedMessage;
import com.trading.engine.gateway.FixedPoint;
import io.netty.buffer.Unpooled;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.agrona.concurrent.EpochNanoClock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import uk.co.real_logic.artio.fields.DecimalFloat;

/**
 * Allocation regression tripwire for {@link JsonToFixTranslator}. Mirrors the {@code
 * GarbageCollectorMXBean.getCollectionCount()} delta pattern used by other {@code *AllocTest}s in
 * this module.
 *
 * <p>Gated by {@code -DrunAllocTests=true} (locked §21, §23).
 */
@EnabledIfSystemProperty(named = "runAllocTests", matches = "true")
final class JsonToFixTranslatorAllocTest {

  private static final int WARMUP_ITERATIONS = 5_000;
  private static final int MEASURED_ITERATIONS = 10_000;

  private static final EpochNanoClock CLOCK = () -> 1_712_491_200_000_000_000L;

  private static final long INSTANCE_TAG = 0xABCDEFL;
  private static final long SESSION_ID = 0x1234567L;

  private static MutableParsedMessage parseFresh(final byte[] frame) {
    final var msg = new MutableParsedMessage();
    final var src = Unpooled.wrappedBuffer(frame);
    BrowserMessageReader.parse(src, msg);
    return msg;
  }

  @Test
  void translateNewOrderSingle_steadyState_doesNotAdvanceGcCount() {
    final var frame =
        ("{\"type\":\"NewOrderSingle\",\"clOrdId\":\"BROWSER-1\",\"symbol\":\"EURUSD\","
                + "\"side\":\"Sell\",\"qty\":\"1000000\",\"price\":\"1.10000000\","
                + "\"ordType\":\"Limit\",\"timeInForce\":\"GTC\",\"account\":\"ACCT-1\"}")
            .getBytes(StandardCharsets.UTF_8);
    final var msg = parseFresh(frame);
    final var translator = new JsonToFixTranslator(CLOCK);
    final var enc = new NewOrderSingleEncoder();

    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      translator.translateNewOrderSingle(msg, enc, SESSION_ID, INSTANCE_TAG, i);
    }

    final long beforeGc = totalGcCount();
    for (int i = 0; i < MEASURED_ITERATIONS; i++) {
      final int rc = translator.translateNewOrderSingle(msg, enc, SESSION_ID, INSTANCE_TAG, i);
      assertEquals(0, rc);
    }
    final long afterGc = totalGcCount();
    assertEquals(
        beforeGc, afterGc, "translateNewOrderSingle advanced GC count " + beforeGc + "→" + afterGc);
  }

  @Test
  void translateNewOrderSingle_mintingPath_steadyState_doesNotAdvanceGcCount() {
    final var frame =
        ("{\"type\":\"NewOrderSingle\",\"symbol\":\"EURUSD\","
                + "\"side\":\"Sell\",\"qty\":\"1000000\",\"price\":\"1.10000000\","
                + "\"ordType\":\"Limit\",\"timeInForce\":\"GTC\",\"account\":\"ACCT-1\"}")
            .getBytes(StandardCharsets.UTF_8);
    final var msg = parseFresh(frame);
    final var translator = new JsonToFixTranslator(CLOCK);
    final var enc = new NewOrderSingleEncoder();

    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      translator.translateNewOrderSingle(msg, enc, SESSION_ID, INSTANCE_TAG, i);
    }

    final long beforeGc = totalGcCount();
    for (int i = 0; i < MEASURED_ITERATIONS; i++) {
      translator.translateNewOrderSingle(msg, enc, SESSION_ID, INSTANCE_TAG, i);
    }
    final long afterGc = totalGcCount();
    assertEquals(beforeGc, afterGc);
  }

  @Test
  void translateAcceptQuote_steadyState_doesNotAdvanceGcCount() {
    final var frame =
        "{\"type\":\"AcceptQuote\",\"quoteId\":\"Q-1\",\"clOrdId\":\"BC-1\"}"
            .getBytes(StandardCharsets.UTF_8);
    final var msg = parseFresh(frame);
    final var translator = new JsonToFixTranslator(CLOCK);
    final var enc = new NewOrderSingleEncoder();

    final var snap = new QuoteSnapshot();
    final var bid = new DecimalFloat();
    final var ask = new DecimalFloat();
    FixedPoint.toDecimalFloat(110_000_000L, bid);
    FixedPoint.toDecimalFloat(110_500_000L, ask);
    snap.bind(
        "EURUSD".getBytes(StandardCharsets.US_ASCII),
        0,
        6,
        MutableParsedMessage.SIDE_BUY,
        100_000_000_000L,
        bid,
        ask,
        Long.MAX_VALUE);

    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      translator.translateAcceptQuote(msg, enc, snap, SESSION_ID, INSTANCE_TAG, i, i);
    }

    final long beforeGc = totalGcCount();
    for (int i = 0; i < MEASURED_ITERATIONS; i++) {
      translator.translateAcceptQuote(msg, enc, snap, SESSION_ID, INSTANCE_TAG, i, i);
    }
    final long afterGc = totalGcCount();
    assertEquals(beforeGc, afterGc);
  }

  @Test
  void translateCancelOrder_steadyState_doesNotAdvanceGcCount() {
    final var frame =
        ("{\"type\":\"CancelOrder\",\"clOrdId\":\"CXL-1\",\"origClOrdId\":\"ORIG-9\","
                + "\"symbol\":\"GBPUSD\",\"side\":\"Buy\"}")
            .getBytes(StandardCharsets.UTF_8);
    final var msg = parseFresh(frame);
    final var translator = new JsonToFixTranslator(CLOCK);
    final var enc = new OrderCancelRequestEncoder();

    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      translator.translateCancelOrder(msg, enc, SESSION_ID, INSTANCE_TAG, i);
    }

    final long beforeGc = totalGcCount();
    for (int i = 0; i < MEASURED_ITERATIONS; i++) {
      translator.translateCancelOrder(msg, enc, SESSION_ID, INSTANCE_TAG, i);
    }
    final long afterGc = totalGcCount();
    assertEquals(beforeGc, afterGc);
  }

  @Test
  void translateQuoteRequest_steadyState_doesNotAdvanceGcCount() {
    final var frame =
        ("{\"type\":\"QuoteRequest\",\"reqId\":\"R-1\",\"symbol\":\"EURUSD\","
                + "\"side\":\"Buy\",\"qty\":\"1000000\"}")
            .getBytes(StandardCharsets.UTF_8);
    final var msg = parseFresh(frame);
    final var translator = new JsonToFixTranslator(CLOCK);
    final var enc = new QuoteRequestEncoder();

    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      translator.translateQuoteRequest(msg, enc, SESSION_ID, INSTANCE_TAG, i);
    }

    final long beforeGc = totalGcCount();
    for (int i = 0; i < MEASURED_ITERATIONS; i++) {
      translator.translateQuoteRequest(msg, enc, SESSION_ID, INSTANCE_TAG, i);
    }
    final long afterGc = totalGcCount();
    assertEquals(beforeGc, afterGc);
  }

  @Test
  void mintClOrdId_steadyState_doesNotAdvanceGcCount() {
    final var dst = new byte[32];

    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      JsonToFixTranslator.mintClOrdId(INSTANCE_TAG, SESSION_ID, i, dst, 0);
    }

    final long beforeGc = totalGcCount();
    for (int i = 0; i < MEASURED_ITERATIONS; i++) {
      JsonToFixTranslator.mintClOrdId(INSTANCE_TAG, SESSION_ID, i, dst, 0);
    }
    final long afterGc = totalGcCount();
    assertEquals(beforeGc, afterGc);
  }

  @Test
  void parseDecimalToDecimalFloat_steadyState_doesNotAdvanceGcCount() {
    final var buf = "1234.56789012".getBytes(StandardCharsets.US_ASCII);
    final var df = new DecimalFloat();

    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      JsonToFixTranslator.parseDecimalToDecimalFloat(buf, 0, buf.length, df);
    }

    final long beforeGc = totalGcCount();
    for (int i = 0; i < MEASURED_ITERATIONS; i++) {
      JsonToFixTranslator.parseDecimalToDecimalFloat(buf, 0, buf.length, df);
    }
    final long afterGc = totalGcCount();
    assertTrue(df.value() != 0L); // sanity: parser ran
    assertEquals(beforeGc, afterGc);
  }

  private static long totalGcCount() {
    long total = 0L;
    final List<GarbageCollectorMXBean> beans = ManagementFactory.getGarbageCollectorMXBeans();
    for (final GarbageCollectorMXBean bean : beans) {
      final long c = bean.getCollectionCount();
      if (c >= 0L) {
        total += c;
      }
    }
    return total;
  }
}
