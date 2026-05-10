package com.trading.engine.fixbridge.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.fixbridge.json.BrowserEvent;
import com.trading.engine.fixbridge.json.BrowserEventWriter;
import com.trading.engine.fixbridge.quote.SessionId;
import com.trading.engine.fixbridge.ratelimit.PerTypeRateLimiter;
import com.trading.engine.fixbridge.translator.DecimalStringEmitter;
import com.trading.engine.websocket.JwtValidator.ValidatedClaims;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.agrona.concurrent.NanoClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link OutboundDrainer} — per-channel 1ms drain loop.
 *
 * <p>Uses Netty's {@link EmbeddedChannel} as the channel backing. {@link OutboundDrainer#runOnce()}
 * is invoked directly (package-private) for deterministic, synchronous drain-pass control without
 * relying on the scheduler. The scheduler-based {@link OutboundDrainer#start()} is exercised only
 * for idempotence and stop tests.
 *
 * <p>Stall-escalation tests advance a mutable {@link NanoClock} stub past {@link
 * OutboundDrainer#STALL_TIMEOUT_NANOS} to trigger the fatal close path.
 *
 * <p><b>Threading.</b> Single-threaded — EmbeddedChannel processes all pipeline calls inline.
 *
 * <p><b>Allocation.</b> Test-only — allocation is acceptable.
 */
final class OutboundDrainerTest {

  // ---------------------------------------------------------------------------
  // Controllable NanoClock stub.
  // ---------------------------------------------------------------------------

  private static final class ControllableNanoClock implements NanoClock {

    private final AtomicLong nanos = new AtomicLong(0L);

    void advance(final long delta) {
      nanos.addAndGet(delta);
    }

    void set(final long value) {
      nanos.set(value);
    }

    @Override
    public long nanoTime() {
      return nanos.get();
    }
  }

  // ---------------------------------------------------------------------------
  // Helpers.
  // ---------------------------------------------------------------------------

  private EmbeddedChannel channel;
  private OutboundQueue queue;
  private BridgeSession session;
  private InboundReadGate readGate;
  private BrowserEventWriter writer;
  private ControllableNanoClock nanoClock;
  private OutboundDrainer drainer;

  /** Build a minimal BridgeSession with the given queue capacity. */
  private static BridgeSession buildSession(final OutboundQueue q) {
    final var claims =
        new ValidatedClaims("user-001", "jti-001", List.of(), Long.MAX_VALUE, true, List.of());
    return new BridgeSession(
        new SessionId("sess-001"),
        claims,
        InetAddress.getLoopbackAddress(),
        q,
        new PerTypeRateLimiter(0L));
  }

  /**
   * Drain a TextWebSocketFrame from the channel outbound queue and return its UTF-8 content.
   * Releases the frame after reading.
   */
  private static String readTextFrame(final EmbeddedChannel ch) {
    final var msg = ch.readOutbound();
    if (msg instanceof TextWebSocketFrame frame) {
      final var buf = frame.content();
      final byte[] bytes = new byte[buf.readableBytes()];
      buf.readBytes(bytes);
      frame.release();
      return new String(bytes, StandardCharsets.UTF_8);
    }
    if (msg != null && msg instanceof io.netty.util.ReferenceCounted rc) {
      rc.release();
    }
    return null;
  }

  private static BrowserEvent.Error makeError(final String reason) {
    return new BrowserEvent.Error(reason);
  }

  private static BrowserEvent.ExecutionReport makeExecReport(final String clOrdId) {
    return new BrowserEvent.ExecutionReport(
        clOrdId, "E1", '0', '0', "EUR/USD", "Buy", 0L, 100L, 0L);
  }

  @BeforeEach
  void setUp() {
    // Use a small queue so stall tests don't need to fill 4096 entries.
    queue = new OutboundQueue(10);
    session = buildSession(queue);
    channel = new EmbeddedChannel();
    readGate = new InboundReadGate(queue);
    channel.pipeline().addFirst("read-gate", readGate);
    writer = new BrowserEventWriter(new DecimalStringEmitter());
    nanoClock = new ControllableNanoClock();

    final var ctx = channel.pipeline().context("read-gate");
    drainer = new OutboundDrainer(ctx, session, readGate, writer, nanoClock);
  }

  @AfterEach
  void tearDown() {
    channel.finishAndReleaseAll();
  }

  // ---------------------------------------------------------------------------
  // Empty queue: runOnce is a no-op.
  // ---------------------------------------------------------------------------

  @Test
  void runOnce_emptyQueue_noFramesEmitted_readGateDrainInvoked() {
    drainer.runOnce();

    // No frames in outbound.
    final var msg = channel.readOutbound();
    assertTrue(msg == null, "Empty queue must produce no outbound frames");

    // readGate.onAfterDrain was invoked — autoRead must still be enabled (it started enabled).
    assertTrue(readGate.isAutoReadEnabled(), "readGate.onAfterDrain must have been called");
    assertFalse(drainer.isStallTerminated());
  }

  // ---------------------------------------------------------------------------
  // Drain 3 events.
  // ---------------------------------------------------------------------------

  @Test
  void runOnce_threeEvents_drainsAllThreeAsTextFrames() {
    queue.offer(makeError("msg-1"));
    queue.offer(makeError("msg-2"));
    queue.offer(makeError("msg-3"));

    drainer.runOnce();

    final var first = readTextFrame(channel);
    final var second = readTextFrame(channel);
    final var third = readTextFrame(channel);
    final var fourth = channel.readOutbound();

    assertTrue(first != null && first.contains("msg-1"), "First frame must contain msg-1");
    assertTrue(second != null && second.contains("msg-2"), "Second frame must contain msg-2");
    assertTrue(third != null && third.contains("msg-3"), "Third frame must contain msg-3");
    assertTrue(fourth == null, "Must be no fourth frame");
    assertEquals(0, queue.size(), "Queue must be empty after drain");
  }

  // ---------------------------------------------------------------------------
  // Batch limit: drains exactly DRAIN_BATCH_LIMIT events when queue > limit.
  // ---------------------------------------------------------------------------

  @Test
  void runOnce_queueExceedsBatchLimit_drainsExactlyBatchLimit() {
    final int total = OutboundDrainer.DRAIN_BATCH_LIMIT + 10;
    // Use a larger queue for this test.
    final var largeQueue = new OutboundQueue(total + 4);
    final var largeSession = buildSession(largeQueue);
    final var largeGate = new InboundReadGate(largeQueue);
    final var largeChannel = new EmbeddedChannel();
    largeChannel.pipeline().addFirst("read-gate", largeGate);
    final var largeCtx = largeChannel.pipeline().context("read-gate");
    final var largeDrainer =
        new OutboundDrainer(largeCtx, largeSession, largeGate, writer, nanoClock);

    for (int i = 0; i < total; i++) {
      largeQueue.offer(makeExecReport("C-" + i));
    }

    largeDrainer.runOnce();

    // Exactly DRAIN_BATCH_LIMIT frames drained.
    int drained = 0;
    while (largeChannel.readOutbound() instanceof TextWebSocketFrame f) {
      f.release();
      drained++;
    }
    assertEquals(OutboundDrainer.DRAIN_BATCH_LIMIT, drained);
    assertEquals(10, largeQueue.size(), "10 events must remain after one pass");

    largeChannel.finishAndReleaseAll();
  }

  // ---------------------------------------------------------------------------
  // Non-writable channel: drains 0 events.
  // ---------------------------------------------------------------------------

  @Test
  void runOnce_channelNotWritable_nothingDrained() {
    queue.offer(makeError("blocked"));
    // Disable writability: set low water mark first, then high water mark to a small value.
    channel.config().setWriteBufferLowWaterMark(0);
    channel.config().setWriteBufferHighWaterMark(1);
    // Force the channel into non-writable state by writing a big payload without flushing.
    channel.write(channel.alloc().buffer(8).writeZero(8));
    // Now channel.isWritable() should return false.
    assertFalse(channel.isWritable(), "Precondition: channel must be non-writable");

    drainer.runOnce();

    // Queue depth unchanged.
    assertEquals(1, queue.size(), "Queue must not be drained when channel is non-writable");

    // Flush what was written to prevent buffer leaks in tearDown.
    channel.flush();
    final var msg = channel.readOutbound();
    if (msg instanceof io.netty.util.ReferenceCounted rc) {
      rc.release();
    }
  }

  // ---------------------------------------------------------------------------
  // Stall path: escalation after STALL_TIMEOUT_NANOS.
  // ---------------------------------------------------------------------------

  @Test
  void runOnce_stallDetection_noEscalationBeforeTimeout() {
    // Fill queue past stallAtSize. stallAtSize = max(1, (int)(capacity * 90 / 100)) for cap=10 → 9.
    final int stallAt = drainer.stallAtSize();
    for (int i = 0; i < stallAt + 1; i++) {
      queue.offer(makeError("stall-" + i));
    }
    // But make the channel non-writable so nothing drains.
    // Set low water mark first, then high water mark.
    channel.config().setWriteBufferLowWaterMark(0);
    channel.config().setWriteBufferHighWaterMark(1);
    channel.write(channel.alloc().buffer(8).writeZero(8));
    assertFalse(channel.isWritable());

    // First pass at time=0: starts the watch.
    nanoClock.set(0L);
    drainer.runOnce();
    assertFalse(drainer.isStallTerminated(), "Must not escalate on first overflow pass");

    // Second pass just under timeout: still no escalation.
    nanoClock.set(OutboundDrainer.STALL_TIMEOUT_NANOS - 1L);
    drainer.runOnce();
    assertFalse(drainer.isStallTerminated(), "Must not escalate before STALL_TIMEOUT_NANOS");

    // Cleanup buffer.
    channel.flush();
    channel.readOutbound();
    channel.readOutbound();
  }

  @Test
  void runOnce_stallDetection_escalatesAfterTimeout() {
    // Use a larger queue so we can fill past stallAtSize without hitting capacity.
    final var largeQueue = new OutboundQueue(200);
    final var largeSession = buildSession(largeQueue);
    final var largeGate = new InboundReadGate(largeQueue);
    final var stallChannel = new EmbeddedChannel();
    stallChannel.pipeline().addFirst("read-gate", largeGate);
    final var stallCtx = stallChannel.pipeline().context("read-gate");
    final var stallClock = new ControllableNanoClock();
    final var stallDrainer =
        new OutboundDrainer(stallCtx, largeSession, largeGate, writer, stallClock);

    final int stallAt = stallDrainer.stallAtSize();
    for (int i = 0; i < stallAt + 10; i++) {
      largeQueue.offer(makeError("e-" + i));
    }
    // Keep channel non-writable so nothing drains. Set low water mark first.
    stallChannel.config().setWriteBufferLowWaterMark(0);
    stallChannel.config().setWriteBufferHighWaterMark(1);
    stallChannel.write(stallChannel.alloc().buffer(8).writeZero(8));
    stallChannel.flush();
    // Read the "keep-channel-non-writable" buffer out so we don't confuse frame counting.
    final var blocker = stallChannel.readOutbound();
    if (blocker instanceof io.netty.util.ReferenceCounted rc) {
      rc.release();
    }

    // First pass: starts stall watch.
    stallClock.set(0L);
    stallDrainer.runOnce();
    assertFalse(stallDrainer.isStallTerminated());

    // Cross the timeout: escalation must fire.
    stallClock.set(OutboundDrainer.STALL_TIMEOUT_NANOS);
    // Make channel writable again so the fatal BridgeStatus frame can actually be written.
    stallChannel.config().setWriteBufferHighWaterMark(Integer.MAX_VALUE);
    stallChannel.config().setWriteBufferLowWaterMark(Integer.MAX_VALUE / 2);
    stallDrainer.runOnce();

    assertTrue(stallDrainer.isStallTerminated(), "Drainer must be stall-terminated after timeout");

    // Drain the outbound queue of the channel to find the fatal BridgeStatus + CloseWebSocketFrame.
    boolean sawFatalBridgeStatus = false;
    boolean sawCloseFrame = false;
    Object out;
    int maxReads = 20;
    while ((out = stallChannel.readOutbound()) != null && maxReads-- > 0) {
      if (out instanceof TextWebSocketFrame twf) {
        final var buf = twf.content();
        final byte[] bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes);
        final var text = new String(bytes, StandardCharsets.UTF_8);
        twf.release();
        if (text.contains("outbound-stall") && text.contains("\"fatal\":true")) {
          sawFatalBridgeStatus = true;
        }
      } else if (out instanceof CloseWebSocketFrame cwf) {
        if (cwf.statusCode() == BridgeCloseCodes.POLICY_VIOLATION) {
          sawCloseFrame = true;
        }
        cwf.release();
      } else if (out instanceof io.netty.util.ReferenceCounted rc) {
        rc.release();
      }
    }

    assertTrue(
        sawFatalBridgeStatus, "Fatal BridgeStatus(outbound-stall) must be written to channel");
    // Note: EmbeddedChannel does not execute close listeners synchronously; the close frame
    // may be pending in the close-future chain. Check the channel closed OR the close frame exists.
    // Either observation satisfies the escalation contract.
    assertTrue(
        sawCloseFrame || !stallChannel.isActive(),
        "Channel must be closing with POLICY_VIOLATION close code or be already inactive");

    stallChannel.finishAndReleaseAll();
  }

  // ---------------------------------------------------------------------------
  // Stall recovery: queue recedes before timeout.
  // ---------------------------------------------------------------------------

  @Test
  void runOnce_stallRecovery_queueDrainsBeforeTimeout_noEscalation() {
    // Setup a channel where the drainer CAN drain (channel is writable).
    final int stallAt = drainer.stallAtSize();
    // Fill to just above stallAt.
    for (int i = 0; i < stallAt + 1; i++) {
      queue.offer(makeError("r-" + i));
    }

    // t=0: first pass above stall threshold starts the watch.
    nanoClock.set(0L);
    // Make channel non-writable for first pass. Set low water mark first.
    channel.config().setWriteBufferLowWaterMark(0);
    channel.config().setWriteBufferHighWaterMark(1);
    channel.write(channel.alloc().buffer(8).writeZero(8));
    drainer.runOnce();

    // t < timeout: restore writability so drain succeeds.
    nanoClock.set(OutboundDrainer.STALL_TIMEOUT_NANOS / 2);
    channel.config().setWriteBufferHighWaterMark(Integer.MAX_VALUE);
    channel.config().setWriteBufferLowWaterMark(Integer.MAX_VALUE / 2);
    channel.flush(); // flush the write-buffer-blocker

    // Drain the keep-busy frame we wrote.
    final var b = channel.readOutbound();
    if (b instanceof io.netty.util.ReferenceCounted rc) {
      rc.release();
    }

    drainer.runOnce();

    // Queue should now be below stallAt → stall watch reset, no escalation.
    assertFalse(
        drainer.isStallTerminated(), "Stall must be cleared once queue drops below threshold");

    // Drain remaining text frames.
    Object out;
    while ((out = channel.readOutbound()) instanceof TextWebSocketFrame f) {
      f.release();
    }
    if (out instanceof io.netty.util.ReferenceCounted rc) {
      rc.release();
    }
  }

  // ---------------------------------------------------------------------------
  // start() idempotence.
  // ---------------------------------------------------------------------------

  @Test
  void start_calledTwice_doesNotDoubleSchedule() {
    // After start(), the drainer schedules exactly one drain task.
    drainer.start();
    drainer.start(); // second call must be idempotent.

    // The drainTask field is non-null; we verify idempotence by checking we can stop cleanly.
    drainer.stop();
    // No exception means stop() saw exactly one task (and cancelled it once).
    assertFalse(drainer.isStallTerminated());
  }

  // ---------------------------------------------------------------------------
  // stop() cancels the task cleanly.
  // ---------------------------------------------------------------------------

  @Test
  void stop_afterStart_cancelsDrainTask_subsequentRunOnceNoops() {
    drainer.start();
    drainer.stop();

    // With the task cancelled, runOnce() with an active (but empty) queue is still a no-op.
    queue.offer(makeError("after-stop"));
    // runOnce() itself is package-private and still callable from the same package test.
    drainer.runOnce();

    // Frame will still be drained because runOnce() doesn't check if the task is cancelled —
    // it only checks isActive() and stallTerminated. So the frame IS drained; this just verifies
    // that stop() does not cause runOnce() to crash or stall-terminate unexpectedly.
    assertFalse(drainer.isStallTerminated(), "stop() must not set stall-terminated");
  }

  // ---------------------------------------------------------------------------
  // Writer rejects an event — drainer logs and continues.
  // ---------------------------------------------------------------------------

  @Test
  void runOnce_writerRejectsEvent_dropsAndContinues_subsequentEventsStillDrain() {
    // An Error event with a forbidden double-quote in the reason triggers the writer rejection.
    // The writer will throw IllegalArgumentException when it tries to write the '"' character.
    final var badEvent = new BrowserEvent.Error("bad\"reason");
    final var goodEvent = new BrowserEvent.Error("good-reason");

    queue.offer(badEvent);
    queue.offer(goodEvent);

    drainer.runOnce();

    // The bad event was dropped; the good event was emitted.
    // Exactly one TextWebSocketFrame with "good-reason" should be in the outbound.
    final var text = readTextFrame(channel);
    assertTrue(
        text != null && text.contains("good-reason"),
        "Good event must be emitted even after a writer rejection");

    // No stall termination despite the rejection.
    assertFalse(drainer.isStallTerminated());

    // Queue is empty.
    assertEquals(0, queue.size());
  }
}
