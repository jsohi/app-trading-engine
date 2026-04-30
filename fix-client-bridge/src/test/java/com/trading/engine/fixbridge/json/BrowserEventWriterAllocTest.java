package com.trading.engine.fixbridge.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.fixbridge.translator.DecimalStringEmitter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Allocation regression tripwire for {@link BrowserEventWriter}. Asserts {@code
 * GarbageCollectorMXBean.getCollectionCount()} does not advance during {@code 10_000} steady-state
 * iterations across each writer method.
 *
 * <p>Note: the test {@link BrowserEvent} record fixtures ARE allocated once in setup outside the
 * timed loop; the writer reuses the same record references and writes their fields to a reusable
 * {@code ByteBuf} that is cleared between iterations.
 *
 * <p>Gated by {@code -DrunAllocTests=true} (locked §21, §23).
 */
@EnabledIfSystemProperty(named = "runAllocTests", matches = "true")
final class BrowserEventWriterAllocTest {

  private static final int WARMUP_ITERATIONS = 5_000;
  private static final int MEASURED_ITERATIONS = 10_000;

  @Test
  void writeQuote_steadyState_doesNotAdvanceGcCount() {
    final var writer = new BrowserEventWriter(new DecimalStringEmitter());
    final ByteBuf dst = Unpooled.buffer(256);
    final var quote =
        new BrowserEvent.Quote(
            "R-1",
            "Q-7",
            "EURUSD",
            "Buy",
            100_000_050_000_000L,
            110_000_000L,
            1_700_000_000_000_000_000L);
    runLoop(
        () -> {
          dst.clear();
          final int n = writer.writeQuote(quote, dst);
          assertTrue(n > 0);
        });
  }

  @Test
  void writeExecutionReport_steadyState_doesNotAdvanceGcCount() {
    final var writer = new BrowserEventWriter(new DecimalStringEmitter());
    final ByteBuf dst = Unpooled.buffer(256);
    final var er =
        new BrowserEvent.ExecutionReport(
            "C-1", "EX-1", 'F', '2', "EURUSD", "Buy", 100L, 0L, 110_000_000L);
    runLoop(
        () -> {
          dst.clear();
          writer.writeExecutionReport(er, dst);
        });
  }

  @Test
  void writeOrderReject_steadyState_doesNotAdvanceGcCount() {
    final var writer = new BrowserEventWriter(new DecimalStringEmitter());
    final ByteBuf dst = Unpooled.buffer(128);
    final var rej = new BrowserEvent.OrderReject("C-1", "bridge-down");
    runLoop(
        () -> {
          dst.clear();
          writer.writeOrderReject(rej, dst);
        });
  }

  @Test
  void writeBridgeStatus_steadyState_doesNotAdvanceGcCount() {
    final var writer = new BrowserEventWriter(new DecimalStringEmitter());
    final ByteBuf dst = Unpooled.buffer(128);
    final var status = new BrowserEvent.BridgeStatus(true, false, "ready");
    runLoop(
        () -> {
          dst.clear();
          writer.writeBridgeStatus(status, dst);
        });
  }

  private static void runLoop(final Runnable body) {
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      body.run();
    }
    final long beforeGc = totalGcCount();
    for (int i = 0; i < MEASURED_ITERATIONS; i++) {
      body.run();
    }
    final long afterGc = totalGcCount();
    assertEquals(beforeGc, afterGc, "writer advanced GC count " + beforeGc + "→" + afterGc);
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
