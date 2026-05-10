package com.trading.engine.fixbridge.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.management.ManagementFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Allocation regression tripwire for {@link InboundReadGate#onAfterInboundDispatch} and {@link
 * InboundReadGate#onAfterDrain}.
 *
 * <p>Asserts {@code GarbageCollectorMXBean.getCollectionCount()} does not advance during {@code
 * 100_000} steady-state iterations of each hook on an empty-queue (resume) path after JIT warm-up.
 *
 * <p>Gated by {@code -DrunAllocTests=true} so the regular {@code test} task skips it.
 *
 * <p><b>Threading.</b> Single-threaded — {@link InboundReadGate} is not thread-safe per its
 * contract; the test owns it exclusively via {@link io.netty.channel.embedded.EmbeddedChannel}.
 *
 * <p><b>Allocation.</b> The expected hot path is zero-alloc after construction.
 */
@EnabledIfSystemProperty(named = "runAllocTests", matches = "true")
final class InboundReadGateAllocTest {

  private static final int WARMUP_ITERATIONS = 5_000;
  private static final int MEASURED_ITERATIONS = 100_000;

  @Test
  void onAfterInboundDispatch_emptyQueue_zeroAlloc() {
    final var queue = new OutboundQueue(100);
    final var gate = new InboundReadGate(queue);
    final var channel = new io.netty.channel.embedded.EmbeddedChannel(gate);
    final var ctx = channel.pipeline().context(gate);

    // Warmup.
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      gate.onAfterInboundDispatch(ctx);
    }

    final long beforeGc = totalGcCount();
    for (int i = 0; i < MEASURED_ITERATIONS; i++) {
      gate.onAfterInboundDispatch(ctx);
    }
    final long afterGc = totalGcCount();

    assertEquals(
        beforeGc,
        afterGc,
        "InboundReadGate.onAfterInboundDispatch advanced GC count " + beforeGc + "→" + afterGc);

    channel.finishAndReleaseAll();
  }

  @Test
  void onAfterDrain_emptyQueue_autoReadAlreadyEnabled_zeroAlloc() {
    final var queue = new OutboundQueue(100);
    final var gate = new InboundReadGate(queue);
    final var channel = new io.netty.channel.embedded.EmbeddedChannel(gate);
    final var ctx = channel.pipeline().context(gate);

    // Warmup.
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      gate.onAfterDrain(ctx);
    }

    final long beforeGc = totalGcCount();
    for (int i = 0; i < MEASURED_ITERATIONS; i++) {
      gate.onAfterDrain(ctx);
    }
    final long afterGc = totalGcCount();

    assertEquals(
        beforeGc,
        afterGc,
        "InboundReadGate.onAfterDrain advanced GC count " + beforeGc + "→" + afterGc);

    channel.finishAndReleaseAll();
  }

  // ---------------------------------------------------------------------------
  // GC count helper.
  // ---------------------------------------------------------------------------

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
