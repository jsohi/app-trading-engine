package com.trading.engine.fixbridge.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.trading.engine.fixbridge.json.BrowserEvent;
import com.trading.engine.fixbridge.json.BrowserEventWriter;
import com.trading.engine.fixbridge.quote.SessionId;
import com.trading.engine.fixbridge.ratelimit.PerTypeRateLimiter;
import com.trading.engine.fixbridge.translator.DecimalStringEmitter;
import com.trading.engine.websocket.JwtValidator.ValidatedClaims;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.List;
import org.agrona.concurrent.NanoClock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Allocation regression tripwire for {@link OutboundDrainer#runOnce}.
 *
 * <p>Tests two scenarios:
 *
 * <ul>
 *   <li>Empty-queue drain pass: asserts zero GC-count advancement over 100_000 iterations after
 *       warm-up. This is the steady-state idle case.
 *   <li>Full-queue drain pass: per-event allocations (one {@link TextWebSocketFrame} per event
 *       wrapping a Netty-pooled {@link io.netty.buffer.ByteBuf}) are expected and NOT asserted as
 *       zero. Instead the test verifies the drainer produces the expected number of frames and does
 *       not crash — a "smoke" assertion only.
 * </ul>
 *
 * <p>Gated by {@code -DrunAllocTests=true} so the regular {@code test} task skips it.
 *
 * <p><b>Threading.</b> Single-threaded — {@link OutboundDrainer} is not thread-safe per its
 * contract; the test drives it directly via {@code runOnce()}.
 */
@EnabledIfSystemProperty(named = "runAllocTests", matches = "true")
final class OutboundDrainerAllocTest {

  private static final int WARMUP_ITERATIONS = 5_000;
  private static final int MEASURED_ITERATIONS = 100_000;

  // Stable stable events reused across iterations.
  private static final BrowserEvent STABLE_ERROR = new BrowserEvent.Error("ok");

  private static BridgeSession buildSession(final OutboundQueue q) {
    final var claims =
        new ValidatedClaims("user-001", "jti-001", List.of(), Long.MAX_VALUE, true, List.of());
    return new BridgeSession(
        new SessionId("sess-alloc"),
        claims,
        InetAddress.getLoopbackAddress(),
        q,
        new PerTypeRateLimiter(0L));
  }

  @Test
  void runOnce_emptyQueue_noFramesWritten_zeroAlloc() {
    final var queue = new OutboundQueue(16);
    final var session = buildSession(queue);
    final var channel = new EmbeddedChannel();
    final var gate = new InboundReadGate(queue);
    channel.pipeline().addFirst("read-gate", gate);
    final var ctx = channel.pipeline().context("read-gate");
    final NanoClock clock = System::nanoTime;
    final var writer = new BrowserEventWriter(new DecimalStringEmitter());
    final var drainer = new OutboundDrainer(ctx, session, gate, writer, clock);

    // Warmup.
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      drainer.runOnce();
    }

    final long beforeGc = totalGcCount();
    for (int i = 0; i < MEASURED_ITERATIONS; i++) {
      drainer.runOnce();
    }
    final long afterGc = totalGcCount();

    assertEquals(
        beforeGc,
        afterGc,
        "OutboundDrainer.runOnce (empty queue) advanced GC count " + beforeGc + "→" + afterGc);

    channel.finishAndReleaseAll();
  }

  @Test
  void runOnce_singleEventPerPass_frameWrittenToChannel_smokeOnly() {
    // Full-queue drains ARE expected to allocate (one TextWebSocketFrame per event).
    // This test is a smoke check that the drainer writes the correct number of frames
    // without crashing — NOT a zero-alloc assertion.
    final int passes = 1_000;
    final var queue = new OutboundQueue(16);
    final var session = buildSession(queue);
    final var channel = new EmbeddedChannel();
    final var gate = new InboundReadGate(queue);
    channel.pipeline().addFirst("read-gate", gate);
    final var ctx = channel.pipeline().context("read-gate");
    final NanoClock clock = System::nanoTime;
    final var writer = new BrowserEventWriter(new DecimalStringEmitter());
    final var drainer = new OutboundDrainer(ctx, session, gate, writer, clock);

    int frameCount = 0;
    for (int i = 0; i < passes; i++) {
      queue.offer(STABLE_ERROR);
      drainer.runOnce();
      final var out = channel.readOutbound();
      if (out instanceof TextWebSocketFrame f) {
        frameCount++;
        f.release();
      }
    }

    assertEquals(passes, frameCount, "One TextWebSocketFrame must be written per drain pass");

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
