package com.trading.engine.fixbridge.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.buffer.Unpooled;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Allocation regression tripwire for {@link BrowserMessageReader}. Mirrors the {@code
 * GarbageCollectorMXBean.getCollectionCount()}-delta pattern from {@code
 * gateway/.../NoAllocationTest.java}: warm the JIT, sample GC count, run the parser in a tight
 * loop, sample again, assert no GC advanced.
 *
 * <p>Gated by {@code -DrunAllocTests=true} (locked §21, §23) — opt-in only because GC counts can be
 * advanced by unrelated background processes on a shared CI host.
 */
@EnabledIfSystemProperty(named = "runAllocTests", matches = "true")
final class BrowserMessageReaderAllocTest {

  private static final int WARMUP_ITERATIONS = 5_000;
  private static final int MEASURED_ITERATIONS = 10_000;

  @Test
  void parseQuoteRequest_steadyState_doesNotAdvanceGcCount() {
    final byte[] frame =
        ("{\"type\":\"QuoteRequest\",\"reqId\":\"R-1\",\"symbol\":\"EURUSD\","
                + "\"side\":\"Buy\",\"qty\":\"1000000.50\"}")
            .getBytes(StandardCharsets.UTF_8);
    runLoop(frame);
  }

  @Test
  void parseNewOrderSingle_steadyState_doesNotAdvanceGcCount() {
    final byte[] frame =
        ("{\"type\":\"NewOrderSingle\",\"clOrdId\":\"C-1\",\"symbol\":\"EURUSD\","
                + "\"side\":\"Sell\",\"qty\":\"1000000\",\"price\":\"1.10000000\","
                + "\"ordType\":\"Limit\",\"timeInForce\":\"GTC\",\"account\":\"ACCT-1\"}")
            .getBytes(StandardCharsets.UTF_8);
    runLoop(frame);
  }

  @Test
  void parseAcceptQuote_steadyState_doesNotAdvanceGcCount() {
    final byte[] frame =
        "{\"type\":\"AcceptQuote\",\"quoteId\":\"Q-7\",\"clOrdId\":\"C-9\"}"
            .getBytes(StandardCharsets.UTF_8);
    runLoop(frame);
  }

  private static void runLoop(final byte[] frame) {
    final var out = new MutableParsedMessage();
    final var src = Unpooled.wrappedBuffer(frame);

    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      src.readerIndex(0);
      BrowserMessageReader.parse(src, out);
    }

    final long beforeGc = totalGcCount();
    for (int i = 0; i < MEASURED_ITERATIONS; i++) {
      src.readerIndex(0);
      final int t = BrowserMessageReader.parse(src, out);
      assertTrue(t > 0); // sanity
    }
    final long afterGc = totalGcCount();
    assertEquals(
        beforeGc,
        afterGc,
        "BrowserMessageReader.parse advanced GC count from " + beforeGc + " to " + afterGc);
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
