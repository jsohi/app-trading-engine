package com.trading.engine.fixbridge.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link InboundReadGate} — per-session TCP auto-read backpressure gate.
 *
 * <p>Uses Netty's {@link EmbeddedChannel} to access a real {@link ChannelHandlerContext} driven by
 * a real event loop. The {@link OutboundQueue} is used directly to control the depth that the gate
 * reads.
 *
 * <p><b>Threading.</b> Single-threaded — EmbeddedChannel processes all pipeline calls inline on the
 * calling thread.
 *
 * <p><b>Allocation.</b> Test-only — allocation is acceptable.
 */
final class InboundReadGateTest {

  // --- Constructor threshold computation ---

  @Test
  void constructor_defaultPercents_pauseAt80resumeAt50() {
    final var queue = new OutboundQueue(100);
    final var gate = new InboundReadGate(queue);
    assertEquals(80, gate.pauseAtSize());
    assertEquals(50, gate.resumeAtSize());
  }

  @Test
  void constructor_customPercents_computesThresholds() {
    final var queue = new OutboundQueue(100);
    final var gate = new InboundReadGate(queue, 90, 40);
    assertEquals(90, gate.pauseAtSize());
    assertEquals(40, gate.resumeAtSize());
  }

  // --- Constructor validation ---

  @Test
  void constructor_pausePercentZero_throwsIllegalArgument() {
    final var queue = new OutboundQueue(100);
    assertThrows(IllegalArgumentException.class, () -> new InboundReadGate(queue, 0, 40));
  }

  @Test
  void constructor_pausePercentOver100_throwsIllegalArgument() {
    final var queue = new OutboundQueue(100);
    assertThrows(IllegalArgumentException.class, () -> new InboundReadGate(queue, 101, 40));
  }

  @Test
  void constructor_resumePercentNegative_throwsIllegalArgument() {
    final var queue = new OutboundQueue(100);
    assertThrows(IllegalArgumentException.class, () -> new InboundReadGate(queue, 80, -1));
  }

  @Test
  void constructor_resumePercentEqualsPause_throwsIllegalArgument() {
    final var queue = new OutboundQueue(100);
    // resumePercent >= pausePercent → invalid (band would be zero)
    assertThrows(IllegalArgumentException.class, () -> new InboundReadGate(queue, 80, 80));
  }

  @Test
  void constructor_resumePercentGreaterThanPause_throwsIllegalArgument() {
    final var queue = new OutboundQueue(100);
    assertThrows(IllegalArgumentException.class, () -> new InboundReadGate(queue, 80, 90));
  }

  // --- Pause when queue crosses high-water mark ---

  @Test
  void onAfterInboundDispatch_queueAbovePauseThreshold_pausesAutoRead() {
    final var queue = new OutboundQueue(100);
    final var gate = new InboundReadGate(queue, 80, 50);
    final var channel = new EmbeddedChannel(gate);

    // Ensure autoRead starts enabled.
    assertTrue(channel.config().isAutoRead());
    assertTrue(gate.isAutoReadEnabled());

    // Fill queue to just below pause (79) → no pause.
    fillQueue(queue, 79);
    final var ctx = channel.pipeline().context(gate);
    gate.onAfterInboundDispatch(ctx);
    assertTrue(channel.config().isAutoRead(), "Should not pause at 79/100");
    assertTrue(gate.isAutoReadEnabled());

    // Add one more (80) → pause triggers.
    fillQueue(queue, 1);
    gate.onAfterInboundDispatch(ctx);
    assertFalse(channel.config().isAutoRead(), "Should pause at 80/100");
    assertFalse(gate.isAutoReadEnabled());

    channel.finishAndReleaseAll();
  }

  // --- Resume when queue drains below low-water mark ---

  @Test
  void onAfterDrain_queueBelowResume_resumesAutoRead() {
    final var queue = new OutboundQueue(100);
    final var gate = new InboundReadGate(queue, 80, 50);
    final var channel = new EmbeddedChannel(gate);
    final var ctx = channel.pipeline().context(gate);

    // Get into paused state.
    fillQueue(queue, 80);
    gate.onAfterInboundDispatch(ctx);
    assertFalse(gate.isAutoReadEnabled(), "Precondition: gate must be paused");

    // Drain to 51 → still paused.
    drainQueue(queue, 29); // 80 - 29 = 51
    gate.onAfterDrain(ctx);
    assertFalse(gate.isAutoReadEnabled(), "Should remain paused at 51/100");

    // Drain one more to 50 → resume.
    drainQueue(queue, 1); // 50
    gate.onAfterDrain(ctx);
    assertTrue(gate.isAutoReadEnabled(), "Should resume at 50/100");
    assertTrue(channel.config().isAutoRead());

    channel.finishAndReleaseAll();
  }

  // --- Hysteresis: no flip in 51..79 band ---

  @Test
  void hysteresis_drainToMidBand_doesNotResumeAutoRead() {
    final var queue = new OutboundQueue(100);
    final var gate = new InboundReadGate(queue, 80, 50);
    final var channel = new EmbeddedChannel(gate);
    final var ctx = channel.pipeline().context(gate);

    // Pause.
    fillQueue(queue, 80);
    gate.onAfterInboundDispatch(ctx);
    assertFalse(gate.isAutoReadEnabled());

    // Drain to 60 (mid-band) — should not resume.
    drainQueue(queue, 20); // 60
    gate.onAfterDrain(ctx);
    assertFalse(gate.isAutoReadEnabled(), "Should remain paused at 60/100 (hysteresis band)");

    channel.finishAndReleaseAll();
  }

  @Test
  void hysteresis_drainTo51_doesNotResumeAutoRead() {
    final var queue = new OutboundQueue(100);
    final var gate = new InboundReadGate(queue, 80, 50);
    final var channel = new EmbeddedChannel(gate);
    final var ctx = channel.pipeline().context(gate);

    fillQueue(queue, 80);
    gate.onAfterInboundDispatch(ctx);

    drainQueue(queue, 29); // 51
    gate.onAfterDrain(ctx);
    assertFalse(gate.isAutoReadEnabled(), "Should remain paused at 51/100");

    channel.finishAndReleaseAll();
  }

  // --- Idempotence: no double flip ---

  @Test
  void onAfterInboundDispatch_alreadyPaused_doesNotFlip() {
    final var queue = new OutboundQueue(100);
    final var gate = new InboundReadGate(queue, 80, 50);
    final var channel = new EmbeddedChannel(gate);
    final var ctx = channel.pipeline().context(gate);

    // Pause once.
    fillQueue(queue, 80);
    gate.onAfterInboundDispatch(ctx);
    assertFalse(gate.isAutoReadEnabled());

    // Call again with queue still above pause — should stay paused (no toggle).
    gate.onAfterInboundDispatch(ctx);
    assertFalse(gate.isAutoReadEnabled(), "Second call must be idempotent");

    channel.finishAndReleaseAll();
  }

  @Test
  void onAfterDrain_alreadyEnabled_doesNotFlip() {
    final var queue = new OutboundQueue(100);
    final var gate = new InboundReadGate(queue, 80, 50);
    final var channel = new EmbeddedChannel(gate);
    final var ctx = channel.pipeline().context(gate);

    // Gate starts enabled; drain on an empty queue should be a no-op.
    assertTrue(gate.isAutoReadEnabled());
    gate.onAfterDrain(ctx);
    assertTrue(gate.isAutoReadEnabled(), "Drain while already enabled must be idempotent");

    channel.finishAndReleaseAll();
  }

  // --- Null check ---

  @Test
  void constructor_nullQueue_throwsNullPointerException() {
    assertThrows(NullPointerException.class, () -> new InboundReadGate(null));
  }

  // --- Helpers ---

  private static void fillQueue(final OutboundQueue queue, final int count) {
    for (int i = 0; i < count; i++) {
      queue.offer(new com.trading.engine.fixbridge.json.BrowserEvent.Error("fill-" + i));
    }
  }

  private static void drainQueue(final OutboundQueue queue, final int count) {
    for (int i = 0; i < count; i++) {
      queue.poll();
    }
  }

  /**
   * Verify that the ctx obtained via {@code pipeline().context(gate)} is usable from the test
   * thread — a structural precondition that prevents silent no-ops if the context lookup fails.
   */
  @Test
  void contextLookup_gateReachableFromPipeline() {
    final var queue = new OutboundQueue(100);
    final var gate = new InboundReadGate(queue);
    final var channel = new EmbeddedChannel(gate);
    final ChannelHandlerContext ctx = channel.pipeline().context(gate);
    assertTrue(ctx != null, "Gate must be reachable via pipeline context");
    channel.finishAndReleaseAll();
  }
}
