package com.trading.engine.fixbridge.translator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.gateway.FixedPoint;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import uk.co.real_logic.artio.fields.DecimalFloat;

/**
 * Allocation regression tripwire for {@link DecimalStringEmitter}. Asserts {@code
 * GarbageCollectorMXBean.getCollectionCount()} does not advance during {@code 10_000} steady-state
 * iterations of {@code emitInt64FixedPoint} and {@code emitDecimalFloat}.
 *
 * <p>Gated by {@code -DrunAllocTests=true} (locked §21, §23).
 */
@EnabledIfSystemProperty(named = "runAllocTests", matches = "true")
final class DecimalStringEmitterAllocTest {

  private static final int WARMUP_ITERATIONS = 5_000;
  private static final int MEASURED_ITERATIONS = 10_000;

  @Test
  void emitInt64_steadyState_doesNotAdvanceGcCount() {
    final var emitter = new DecimalStringEmitter();
    final ByteBuf dst = Unpooled.buffer(64);

    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      dst.clear();
      emitter.emitInt64FixedPoint(100_000_050_000_000L, dst);
      emitter.emitInt64FixedPoint(-100_000_050_000_000L, dst);
      emitter.emitInt64FixedPoint(Long.MAX_VALUE, dst);
    }

    final long beforeGc = totalGcCount();
    for (int i = 0; i < MEASURED_ITERATIONS; i++) {
      dst.clear();
      final int n = emitter.emitInt64FixedPoint(100_000_050_000_000L, dst);
      assertTrue(n > 0);
      emitter.emitInt64FixedPoint(-100_000_050_000_000L, dst);
      emitter.emitInt64FixedPoint(Long.MAX_VALUE, dst);
    }
    final long afterGc = totalGcCount();
    assertEquals(
        beforeGc, afterGc, "emitInt64FixedPoint advanced GC count " + beforeGc + "→" + afterGc);
  }

  @Test
  void emitDecimalFloat_steadyState_doesNotAdvanceGcCount() {
    final var emitter = new DecimalStringEmitter();
    final ByteBuf dst = Unpooled.buffer(64);
    final DecimalFloat df = new DecimalFloat();
    FixedPoint.toDecimalFloat(100_000_050_000_000L, df);

    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      dst.clear();
      emitter.emitDecimalFloat(df, dst);
    }

    final long beforeGc = totalGcCount();
    for (int i = 0; i < MEASURED_ITERATIONS; i++) {
      dst.clear();
      emitter.emitDecimalFloat(df, dst);
    }
    final long afterGc = totalGcCount();
    assertEquals(
        beforeGc, afterGc, "emitDecimalFloat advanced GC count " + beforeGc + "→" + afterGc);
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
