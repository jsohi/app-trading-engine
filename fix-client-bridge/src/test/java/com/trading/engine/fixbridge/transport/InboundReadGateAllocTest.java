package com.trading.engine.fixbridge.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.fixbridge.json.BrowserEvent;
import io.netty.channel.embedded.EmbeddedChannel;
import java.lang.management.ManagementFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Allocation regression tripwire for {@link InboundReadGate#onAfterInboundDispatch} and {@link
 * InboundReadGate#onAfterDrain}.
 *
 * <p>Asserts {@code GarbageCollectorMXBean.getCollectionCount()} does not advance during {@code
 * 100_000} steady-state iterations of each hook for three queue-fill regimes, plus a dedicated test
 * for the pause→resume auto-read flip cycle.
 *
 * <p>Coverage matrix:
 *
 * <ul>
 *   <li>Below resume threshold (queue empty / 0%) — both hooks no-op past the depth gate.
 *   <li>Between resume and pause thresholds (~60%) — neither flip fires; both hooks read {@code
 *       queue.size()} and return.
 *   <li>Above pause threshold (90%) — {@code onAfterInboundDispatch} would normally pause, but the
 *       cached {@link InboundReadGate#isAutoReadEnabled()} flag latches after the first call so
 *       subsequent iterations short-circuit; this test asserts the cached-state branch is also
 *       zero-alloc.
 *   <li>Pause→resume cycle — alternating fill/drain across the hysteresis band exercises the {@code
 *       ctx.channel().config().setAutoRead(...)} + {@code ctx.read()} flip path; the Netty config
 *       call must not allocate.
 * </ul>
 *
 * <p>Gated by {@code -DrunAllocTests=true} so the regular {@code test} task skips it.
 *
 * <p><b>Threading.</b> Single-threaded — {@link InboundReadGate} is not thread-safe per its
 * contract; the test owns it exclusively via {@link EmbeddedChannel}.
 *
 * <p><b>Allocation.</b> The expected hot path is zero-alloc after construction in every regime.
 */
@EnabledIfSystemProperty(named = "runAllocTests", matches = "true")
final class InboundReadGateAllocTest {

  private static final int WARMUP_ITERATIONS = 5_000;
  private static final int MEASURED_ITERATIONS = 100_000;

  /** Stable BrowserEvent used to fill the queue without per-iteration allocation. */
  private static final BrowserEvent FILL_EVENT = new BrowserEvent.Error("fill");

  // ─── Regime 1: empty queue (below resume threshold) ─────────────────────────

  @Test
  void onAfterInboundDispatch_emptyQueue_zeroAlloc() {
    final var queue = new OutboundQueue(100);
    final var gate = new InboundReadGate(queue);
    final var channel = new EmbeddedChannel(gate);
    final var ctx = channel.pipeline().context(gate);

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
        "InboundReadGate.onAfterInboundDispatch (empty queue) advanced GC count "
            + beforeGc
            + "→"
            + afterGc);

    channel.finishAndReleaseAll();
  }

  @Test
  void onAfterDrain_emptyQueue_autoReadAlreadyEnabled_zeroAlloc() {
    final var queue = new OutboundQueue(100);
    final var gate = new InboundReadGate(queue);
    final var channel = new EmbeddedChannel(gate);
    final var ctx = channel.pipeline().context(gate);

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
        "InboundReadGate.onAfterDrain (empty queue) advanced GC count " + beforeGc + "→" + afterGc);

    channel.finishAndReleaseAll();
  }

  // ─── Regime 2: queue inside hysteresis band (between resume and pause) ──────

  @Test
  void onAfterInboundDispatch_queueInsideHysteresisBand_zeroAlloc() {
    // Capacity 100 → pauseAt=80, resumeAt=50. Pre-fill 60 entries — below pause, above resume so
    // neither hook flips state. Both hooks read queue.size() and short-circuit.
    final var queue = new OutboundQueue(100);
    for (int i = 0; i < 60; i++) {
      queue.offer(FILL_EVENT);
    }
    final var gate = new InboundReadGate(queue);
    final var channel = new EmbeddedChannel(gate);
    final var ctx = channel.pipeline().context(gate);

    // Sanity: gate starts enabled, queue depth 60 is below pause threshold (80).
    assertTrue(gate.isAutoReadEnabled(), "Pre-condition: gate starts enabled");

    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      gate.onAfterInboundDispatch(ctx);
      gate.onAfterDrain(ctx);
    }

    // Sanity: still enabled (no flip occurred during warm-up because depth is in band).
    assertTrue(
        gate.isAutoReadEnabled(),
        "Hysteresis band must not trigger auto-read flips during warm-up");

    final long beforeGc = totalGcCount();
    for (int i = 0; i < MEASURED_ITERATIONS; i++) {
      gate.onAfterInboundDispatch(ctx);
      gate.onAfterDrain(ctx);
    }
    final long afterGc = totalGcCount();

    assertEquals(
        beforeGc,
        afterGc,
        "InboundReadGate hooks (band 50-80%) advanced GC count " + beforeGc + "→" + afterGc);

    channel.finishAndReleaseAll();
  }

  // ─── Regime 3: queue above pause threshold (latched paused state) ───────────

  @Test
  void onAfterInboundDispatch_queueAbovePauseThreshold_latchedPaused_zeroAlloc() {
    // Capacity 100 → pauseAt=80. Pre-fill 90 entries so the FIRST call latches paused; from then
    // on the cached !autoReadEnabled flag short-circuits the queue.size() check entirely. This
    // exercises the cached-state branch on the hot path.
    final var queue = new OutboundQueue(100);
    for (int i = 0; i < 90; i++) {
      queue.offer(FILL_EVENT);
    }
    final var gate = new InboundReadGate(queue);
    final var channel = new EmbeddedChannel(gate);
    final var ctx = channel.pipeline().context(gate);

    // Trigger the initial latch — the first call flips autoRead off.
    gate.onAfterInboundDispatch(ctx);
    assertFalse(gate.isAutoReadEnabled(), "Pre-condition: gate must be paused at 90/100");

    // From here every call short-circuits on the cached flag (no queue.size() read, no flip).
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
        "InboundReadGate.onAfterInboundDispatch (latched paused) advanced GC count "
            + beforeGc
            + "→"
            + afterGc);

    channel.finishAndReleaseAll();
  }

  // ─── Auto-read flip cycle (pause → resume) ──────────────────────────────────

  @Test
  void onAfterInboundDispatch_pauseResumeFlipCycle_zeroAlloc() {
    // Drives the hysteresis flip cycle: fill above pause threshold (latch paused), drain below
    // resume threshold (latch resumed), repeat. Both flips invoke ctx.channel().config().
    // setAutoRead(...) and the resume path additionally calls ctx.read() — this test asserts
    // that neither Netty call allocates on the steady-state cycle.
    final var queue = new OutboundQueue(10); // pauseAt=8, resumeAt=5
    final var gate = new InboundReadGate(queue);
    final var channel = new EmbeddedChannel(gate);
    final var ctx = channel.pipeline().context(gate);

    // Warm-up: complete several pause→resume cycles.
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      // Fill above the pause threshold (9/10 entries → ≥8).
      while (queue.size() < 9) {
        queue.offer(FILL_EVENT);
      }
      gate.onAfterInboundDispatch(ctx);
      // Drain below the resume threshold (5).
      while (queue.size() > 4) {
        queue.poll();
      }
      gate.onAfterDrain(ctx);
    }

    final long beforeGc = totalGcCount();
    for (int i = 0; i < MEASURED_ITERATIONS; i++) {
      while (queue.size() < 9) {
        queue.offer(FILL_EVENT);
      }
      gate.onAfterInboundDispatch(ctx);
      while (queue.size() > 4) {
        queue.poll();
      }
      gate.onAfterDrain(ctx);
    }
    final long afterGc = totalGcCount();

    assertEquals(
        beforeGc,
        afterGc,
        "InboundReadGate pause/resume flip cycle advanced GC count " + beforeGc + "→" + afterGc);

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
