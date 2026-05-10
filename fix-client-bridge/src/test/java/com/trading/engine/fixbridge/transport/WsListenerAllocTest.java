package com.trading.engine.fixbridge.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.management.ThreadMXBean;
import com.trading.engine.fixbridge.audit.AuditLogger;
import com.trading.engine.fixbridge.quote.SessionId;
import com.trading.engine.fixbridge.ratelimit.PerTypeRateLimiter;
import com.trading.engine.websocket.JwtValidator.ValidatedClaims;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.NanoClock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Allocation regression tripwire for {@link WsListener#channelRead0} on the steady-state happy
 * path: {@code parse → rate-limit admit → dispatcher.dispatch → readGate.onAfterInboundDispatch}.
 *
 * <p>Drives a stable {@link TextWebSocketFrame} carrying a valid {@code QuoteRequest} payload
 * through {@link WsListener#channelRead0} {@code 100_000} times after JIT warm-up, with a {@link
 * BridgeFrameDispatcher#NOOP} dispatcher to isolate listener-internal allocation from downstream
 * routing.
 *
 * <p><b>Bytes-per-iter UPPER BOUND, not strict zero-alloc.</b> Every iteration must construct a
 * fresh {@link TextWebSocketFrame} because Netty's {@link
 * io.netty.channel.SimpleChannelInboundHandler#channelRead} releases the inbound frame after
 * dispatch — the frame wrapper itself cannot be reused across iterations. That driver-side
 * allocation is OUTSIDE the SUT but cannot be eliminated.
 *
 * <p>To still catch SUT regressions, the test uses {@link ThreadMXBean#getThreadAllocatedBytes} to
 * pin a per-iteration upper bound. Baseline (current code, JDK 25, locally measured): ~120
 * bytes/iter (TextWebSocketFrame ~32 + retainedDuplicate ~32 + small Netty pipeline bookkeeping).
 * The 1024 bytes/iter ceiling absorbs JVM-version variance while catching any new per-frame
 * allocation in {@link WsListener} itself (e.g. an accidental {@link String} allocation, a
 * defensive copy of the parsed slice, or a capturing lambda on the rate-limiter or audit path).
 *
 * <p>A second sanity check asserts that the queue stays empty (NOOP dispatcher does not enqueue)
 * and the read-gate stays enabled (queue depth never crosses the pause threshold).
 *
 * <p>Gated by {@code -DrunAllocTests=true} so the regular {@code test} task skips it.
 *
 * <p><b>Threading.</b> Single-threaded — {@link WsListener} is per-channel and not thread-safe; the
 * test owns the {@link EmbeddedChannel} exclusively.
 *
 * <p><b>Platform note.</b> {@link com.sun.management.ThreadMXBean#getThreadAllocatedBytes} is a
 * HotSpot-specific API. The test fails fast with a clear message if the running JVM does not
 * support it; production deployments and CI run on HotSpot/OpenJDK.
 */
@EnabledIfSystemProperty(named = "runAllocTests", matches = "true")
final class WsListenerAllocTest {

  private static final int WARMUP_ITERATIONS = 5_000;
  private static final int MEASURED_ITERATIONS = 100_000;

  /**
   * Per-iteration allocation upper bound in bytes. ~120 bytes/iter measured locally (frame +
   * duplicate + Netty bookkeeping); 1024 byte ceiling catches any new SUT allocation while
   * absorbing JVM-version variance.
   */
  private static final long PER_ITER_BUDGET_BYTES = 1024L;

  /** Fixed wall-clock and monotonic-clock starting reading. */
  private static final long FIXED_NOW_NS = 1_700_000_000_000_000_000L;

  /**
   * Pre-encoded valid QuoteRequest payload. Re-used across every iteration via {@link
   * ByteBuf#retainedDuplicate()} so the source bytes are copied at most once at test setup.
   */
  private static final String VALID_QUOTE_REQUEST_JSON =
      "{\"type\":\"QuoteRequest\",\"reqId\":\"R-ALLOC\","
          + "\"symbol\":\"EUR/USD\",\"side\":\"Buy\",\"qty\":\"1.00000000\"}";

  @Test
  void channelRead0_validQuoteRequest_steadyState_perIterAllocationUnderBudget() throws Exception {
    final var rawBean = ManagementFactory.getThreadMXBean();
    assertTrue(
        rawBean instanceof ThreadMXBean,
        "HotSpot ThreadMXBean.getThreadAllocatedBytes is required (running on non-HotSpot JVM?)");
    final ThreadMXBean threadBean = (ThreadMXBean) rawBean;
    assertTrue(
        threadBean.isThreadAllocatedMemorySupported(),
        "JVM does not support thread allocated memory tracking");
    if (!threadBean.isThreadAllocatedMemoryEnabled()) {
      threadBean.setThreadAllocatedMemoryEnabled(true);
    }

    final var queue = new OutboundQueue(64);
    final var limiter = new PerTypeRateLimiter(FIXED_NOW_NS);
    final var session = buildSession(queue, limiter);

    // Advancing clock — array holder so the lambda allocation is one-shot at construction.
    // QuoteRequest's normal-mode rate is 5/s with burst 10; first-60s mode is 2/s with burst 2.
    // Advance +1s/iter → bucket credits 5 tokens, only 1 consumed → never depletes.
    final long[] nowHolder = {FIXED_NOW_NS};
    final NanoClock advancingNanoClock = () -> nowHolder[0];
    final EpochNanoClock advancingEpochClock = () -> nowHolder[0];

    final var readGate = new InboundReadGate(queue);
    final var listener =
        new WsListener(
            BridgeFrameDispatcher.NOOP,
            readGate,
            advancingEpochClock,
            advancingNanoClock,
            AuditLogger.Noop.INSTANCE);

    final var channel = new EmbeddedChannel(listener);
    channel.attr(BridgeSession.ATTRIBUTE_KEY).set(session);

    // One backing buffer for the JSON payload — duplicated (zero-copy) per iteration.
    final ByteBuf payload = Unpooled.copiedBuffer(VALID_QUOTE_REQUEST_JSON, StandardCharsets.UTF_8);

    // Warm-up — JIT-compile the parse + rate-limit + dispatch + post-dispatch hot path.
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      runOneIteration(channel, payload, nowHolder);
    }

    // Sanity (post-warm-up): queue empty, gate enabled.
    assertEquals(0, queue.size(), "NOOP dispatcher must not enqueue events");
    assertTrue(readGate.isAutoReadEnabled(), "Gate must remain enabled (queue empty)");

    final long threadId = Thread.currentThread().threadId();
    final long beforeBytes = threadBean.getThreadAllocatedBytes(threadId);
    for (int i = 0; i < MEASURED_ITERATIONS; i++) {
      runOneIteration(channel, payload, nowHolder);
    }
    final long afterBytes = threadBean.getThreadAllocatedBytes(threadId);

    final long deltaBytes = afterBytes - beforeBytes;
    final long perIterBytes = deltaBytes / MEASURED_ITERATIONS;

    assertTrue(
        perIterBytes <= PER_ITER_BUDGET_BYTES,
        "WsListener.channelRead0 per-iter allocation regression: "
            + perIterBytes
            + " bytes/iter exceeds budget "
            + PER_ITER_BUDGET_BYTES
            + " (total delta "
            + deltaBytes
            + " over "
            + MEASURED_ITERATIONS
            + " iterations)");

    payload.release();
    channel.finishAndReleaseAll();
  }

  /** One iteration of the hot path — re-used by warm-up and measured loops. */
  private static void runOneIteration(
      final EmbeddedChannel channel, final ByteBuf payload, final long[] nowHolder) {
    nowHolder[0] += 1_000_000_000L;
    payload.readerIndex(0);
    // SimpleChannelInboundHandler.channelRead releases the frame after dispatch, so a fresh
    // wrapper is required per iteration. The retainedDuplicate shares the underlying byte[]
    // (no copy) — only the small wrapper objects are allocated by the test driver.
    final var frame = new TextWebSocketFrame(payload.retainedDuplicate());
    channel.writeInbound(frame);
    // Drain anything the listener may have enqueued onto the channel outbound (none for the
    // happy path with a NOOP dispatcher — defensive in case of an enqueued event).
    Object out;
    while ((out = channel.readOutbound()) != null) {
      if (out instanceof io.netty.util.ReferenceCounted rc) {
        rc.release();
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Helpers.
  // ---------------------------------------------------------------------------

  private static BridgeSession buildSession(
      final OutboundQueue queue, final PerTypeRateLimiter limiter) {
    final var claims =
        new ValidatedClaims(
            "user-alloc",
            "jti-alloc",
            List.of("ACME-001"),
            Long.MAX_VALUE,
            false /* ipPinned=false to bypass IP-pin comparison on the hot path */,
            List.of());
    return new BridgeSession(
        new SessionId("sess-alloc"), claims, InetAddress.getLoopbackAddress(), queue, limiter);
  }
}
