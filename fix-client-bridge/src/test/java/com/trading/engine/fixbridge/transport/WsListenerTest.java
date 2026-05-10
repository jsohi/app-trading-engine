package com.trading.engine.fixbridge.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.fixbridge.audit.AuditAction;
import com.trading.engine.fixbridge.audit.AuditLogger;
import com.trading.engine.fixbridge.json.BrowserEvent;
import com.trading.engine.fixbridge.json.MutableParsedMessage;
import com.trading.engine.fixbridge.json.OrderRejectReason;
import com.trading.engine.fixbridge.quote.SessionId;
import com.trading.engine.fixbridge.ratelimit.PerTypeRateLimiter;
import com.trading.engine.websocket.JwtValidator.ValidatedClaims;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.NanoClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link WsListener} — the post-authentication WebSocket frame router.
 *
 * <p>Uses {@link EmbeddedChannel} to drive the SUT. Each test manually attaches a {@link
 * BridgeSession} to the channel attribute to simulate the post-auth state that {@link
 * com.trading.engine.fixbridge.auth.JwtAuthHandler} would normally establish.
 *
 * <p><b>Threading.</b> Single-threaded — EmbeddedChannel is synchronous.
 *
 * <p><b>Allocation.</b> Test-only.
 */
final class WsListenerTest {

  private static final String LOOPBACK = "127.0.0.1";
  private static final long FIXED_NOW_NS = 1_000_000_000L; // 1 s (within first-60s window)

  // ─── Recording AuditLogger ────────────────────────────────────────────────

  /** In-test recording AuditLogger — captures every record() call for assertions. */
  private static final class RecordingAuditLogger implements AuditLogger {

    final List<AuditAction> actions = new ArrayList<>();
    final List<String> failureReasons = new ArrayList<>();

    @Override
    public void record(
        final long tsNs,
        final String userId,
        final String jti,
        final String sourceIp,
        final AuditAction action,
        final String symbol,
        final String side,
        final long qty,
        final long price,
        final String ordType,
        final String tif,
        final String account,
        final String clOrdId,
        final String origClOrdId,
        final String quoteId,
        final String result,
        final String failureReason,
        final String traceparent) {
      actions.add(action);
      failureReasons.add(failureReason);
    }

    @Override
    public boolean isWritable() {
      return true;
    }
  }

  // ─── Counting InboundReadGate wrapper ─────────────────────────────────────
  // InboundReadGate is final so we cannot subclass it. Instead, wrap the SAM
  // dispatcher in a BridgeFrameDispatcher that increments a counter and also
  // use a shared AtomicInteger to verify the readGate side via the OutboundQueue
  // size check. For the "readGate notified" test we use a custom dispatcher
  // that delegates to a counting lambda and verify indirectly via queue state.
  // The actual gate call is verified through channel autoRead state inspection.

  // ─── Tracking dispatcher ──────────────────────────────────────────────────

  private static final class RecordingDispatcher implements BridgeFrameDispatcher {
    int callCount = 0;
    int lastMessageType = -1;

    @Override
    public void dispatch(
        final BridgeSession session,
        final MutableParsedMessage parsed,
        final int messageType,
        final long nowNs) {
      callCount++;
      lastMessageType = messageType;
    }
  }

  // ─── Shared helpers ───────────────────────────────────────────────────────

  private RecordingAuditLogger auditLogger;
  private EpochNanoClock fixedEpochClock;
  private NanoClock fixedClock;

  @BeforeEach
  void setUp() {
    auditLogger = new RecordingAuditLogger();
    fixedEpochClock = () -> FIXED_NOW_NS;
    fixedClock = () -> FIXED_NOW_NS;
  }

  /**
   * Build a {@link BridgeSession} using the given IP as the pinned address. {@code ipPinned=true}
   * unless explicitly requested otherwise.
   */
  private static BridgeSession buildSession(
      final String pinnedIp,
      final boolean ipPinned,
      final List<String> roles,
      final OutboundQueue queue,
      final PerTypeRateLimiter limiter)
      throws UnknownHostException {
    final var claims =
        new ValidatedClaims("user-1", "jti-1", List.of("ACME-001"), 9999999999L, ipPinned, roles);
    final var pinned = InetAddress.getByName(pinnedIp);
    return new BridgeSession(new SessionId("sess-1"), claims, pinned, queue, limiter);
  }

  /** Build a text frame from an ASCII JSON string. */
  private static TextWebSocketFrame textFrame(final String json) {
    return new TextWebSocketFrame(Unpooled.copiedBuffer(json, StandardCharsets.UTF_8));
  }

  // ─── Valid QuoteRequest → dispatcher invoked ──────────────────────────────

  @Test
  void channelRead0_validQuoteRequest_dispatcherInvokedOnce() throws Exception {
    final var queue = new OutboundQueue(64);
    final var limiter = new PerTypeRateLimiter(FIXED_NOW_NS);
    final var session = buildSession(LOOPBACK, false, List.of(), queue, limiter);

    final var dispatcher = new RecordingDispatcher();
    final var readGate = new InboundReadGate(queue);
    final var listener =
        new WsListener(dispatcher, readGate, fixedEpochClock, fixedClock, auditLogger);

    final var channel = new EmbeddedChannel(listener);
    channel.attr(BridgeSession.ATTRIBUTE_KEY).set(session);

    channel.writeInbound(
        textFrame(
            "{\"type\":\"QuoteRequest\",\"reqId\":\"R1\","
                + "\"symbol\":\"EUR/USD\",\"side\":\"Buy\",\"qty\":\"1.00000000\"}"));

    assertEquals(1, dispatcher.callCount, "Dispatcher should be invoked once");
    assertEquals(MutableParsedMessage.TYPE_QUOTE_REQUEST, dispatcher.lastMessageType);
    // Channel must still be open.
    assertTrue(channel.isOpen());
    // readGate.onAfterInboundDispatch is called internally after dispatch.
    // Verified indirectly: gate remains enabled (queue depth still 0 → no pause).
    assertTrue(readGate.isAutoReadEnabled(), "Gate must remain enabled when queue is empty");

    channel.finishAndReleaseAll();
  }

  // ─── Rate-limited NewOrderSingle → OrderReject enqueued ──────────────────

  @Test
  void channelRead0_newOrderRateLimited_orderRejectEnqueuedWithAuditEntry() throws Exception {
    // Use authNanos = 0 so we are in the first-60s window.
    final long authNanos = 0L;
    final var queue = new OutboundQueue(64);
    final var limiter = new PerTypeRateLimiter(authNanos);
    final var session = buildSession(LOOPBACK, false, List.of(), queue, limiter);

    // Exhaust the NOS initial-window bucket (burst=2 in first-60s window).
    // NOS_INITIAL_BURST = 2 per PerTypeRateLimiter constants.
    limiter.tryConsume(PerTypeRateLimiter.CommandType.NEW_ORDER_SINGLE, FIXED_NOW_NS);
    limiter.tryConsume(PerTypeRateLimiter.CommandType.NEW_ORDER_SINGLE, FIXED_NOW_NS);
    // Third attempt within same nanosecond should be rejected.

    final var dispatcher = new RecordingDispatcher();
    final var readGate = new InboundReadGate(queue);
    final var listener =
        new WsListener(dispatcher, readGate, fixedEpochClock, fixedClock, auditLogger);

    final var channel = new EmbeddedChannel(listener);
    channel.attr(BridgeSession.ATTRIBUTE_KEY).set(session);

    channel.writeInbound(
        textFrame(
            "{\"type\":\"NewOrderSingle\",\"clOrdId\":\"ORD-1\",\"symbol\":\"EUR/USD\","
                + "\"side\":\"Buy\",\"qty\":\"1.00000000\",\"price\":\"1.10000000\","
                + "\"ordType\":\"Limit\",\"timeInForce\":\"DAY\",\"account\":\"ACME-001\"}"));

    // Dispatcher should NOT be invoked (rate-limited).
    assertEquals(0, dispatcher.callCount);

    // Queue must contain an OrderReject.
    assertEquals(1, queue.size());
    final var event = queue.poll();
    assertNotNull(event);
    assertTrue(event instanceof BrowserEvent.OrderReject, "Expected OrderReject, got: " + event);
    final var reject = (BrowserEvent.OrderReject) event;
    // In the first-60s window the reason should be RATE_LIMIT_INITIAL_WINDOW.
    assertEquals(OrderRejectReason.RATE_LIMIT_INITIAL_WINDOW.wireValue(), reject.reason());

    // Audit logger must have received a RATE_LIMIT_HIT entry.
    assertTrue(
        auditLogger.actions.contains(AuditAction.RATE_LIMIT_HIT),
        "Audit log must record RATE_LIMIT_HIT");

    channel.finishAndReleaseAll();
  }

  // ─── Malformed JSON → Error enqueued; channel stays open ─────────────────

  @Test
  void channelRead0_malformedJson_errorEnqueuedChannelStaysOpen() throws Exception {
    final var queue = new OutboundQueue(64);
    final var limiter = new PerTypeRateLimiter(FIXED_NOW_NS);
    final var session = buildSession(LOOPBACK, false, List.of(), queue, limiter);

    final var dispatcher = new RecordingDispatcher();
    final var readGate = new InboundReadGate(queue);
    final var listener =
        new WsListener(dispatcher, readGate, fixedEpochClock, fixedClock, auditLogger);

    final var channel = new EmbeddedChannel(listener);
    channel.attr(BridgeSession.ATTRIBUTE_KEY).set(session);

    channel.writeInbound(textFrame("{not valid json at all!!!"));

    assertEquals(0, dispatcher.callCount);
    // An Error event must be in the queue.
    assertEquals(1, queue.size());
    assertTrue(queue.poll() instanceof BrowserEvent.Error);
    // Channel must remain open.
    assertTrue(channel.isOpen());

    channel.finishAndReleaseAll();
  }

  // ─── IP-pin violation → channel closed with 4008 ─────────────────────────

  @Test
  void channelRead0_ipPinTrue_loopbackMatch_noClose() throws Exception {
    // TODO(APP-40d): add a real-socket integration test for the actual ip-pin violation path
    // (where the remote InetSocketAddress does NOT match the pinned IP). EmbeddedChannel uses
    // LocalAddress so WsListener's checkIpPin short-circuits to fail-open; a violation cannot
    // be triggered here without a real TCP socket.
    // Pin the session to 192.168.1.100; EmbeddedChannel's remote is loopback.
    final var queue = new OutboundQueue(64);
    final var limiter = new PerTypeRateLimiter(FIXED_NOW_NS);
    final var session =
        buildSession("192.168.1.100", true /* ipPinned */, List.of(), queue, limiter);

    final var dispatcher = new RecordingDispatcher();
    final var readGate = new InboundReadGate(queue);
    final var listener =
        new WsListener(dispatcher, readGate, fixedEpochClock, fixedClock, auditLogger);

    final var channel = new EmbeddedChannel(listener);
    // Override remote address so it differs from the pinned IP.
    // EmbeddedChannel's remoteAddress returns a LocalAddress, not InetSocketAddress,
    // so any remote that IS an InetSocketAddress triggers the comparison. We use
    // EmbeddedChannel.connect() which routes through the pipeline but won't actually
    // bind. Instead, wrap the channel with a remote InetSocketAddress set via
    // config — but EmbeddedChannel doesn't easily support that.
    // WsListener's checkIpPin: if !(addr instanceof InetSocketAddress) → fail-open (return true).
    // So in an EmbeddedChannel (LocalAddress) the IP check is skipped.
    // We test the violation using a custom channel with InetSocketAddress remote.
    channel.finishAndReleaseAll();

    // Use a direct approach: configure the channel's remoteAddress via the internal pipeline.
    // EmbeddedChannel doesn't natively expose setRemoteAddress. Test the check via a
    // different angle — trust that the production code path is covered by the guard at line 205
    // in WsListener: "if (!(addr instanceof InetSocketAddress inet)) return true". We verify
    // that with a session pinned to loopback and loopback remote, no close occurs.
    final var queue2 = new OutboundQueue(64);
    final var limiter2 = new PerTypeRateLimiter(FIXED_NOW_NS);
    final var sessionMatch = buildSession(LOOPBACK, true, List.of(), queue2, limiter2);
    final var listener2 =
        new WsListener(
            dispatcher, new InboundReadGate(queue2), fixedEpochClock, fixedClock, auditLogger);
    final var channel2 = new EmbeddedChannel(listener2);
    channel2.attr(BridgeSession.ATTRIBUTE_KEY).set(sessionMatch);

    channel2.writeInbound(
        textFrame(
            "{\"type\":\"QuoteRequest\",\"reqId\":\"R1\","
                + "\"symbol\":\"EUR/USD\",\"side\":\"Buy\",\"qty\":\"1.00000000\"}"));
    // EmbeddedChannel remote is LocalAddress → ip check skipped → no close.
    assertTrue(channel2.isOpen(), "EmbeddedChannel uses LocalAddress so pin check is skipped");
    channel2.finishAndReleaseAll();
  }

  // ─── IP-pin disabled → no close even if address differs ──────────────────

  @Test
  void channelRead0_ipPinDisabled_noCloseOnAddressMismatch() throws Exception {
    final var queue = new OutboundQueue(64);
    final var limiter = new PerTypeRateLimiter(FIXED_NOW_NS);
    // ipPinned=false → no enforcement regardless of address.
    final var session = buildSession("192.168.1.100", false, List.of(), queue, limiter);

    final var dispatcher = new RecordingDispatcher();
    final var readGate = new InboundReadGate(queue);
    final var listener =
        new WsListener(dispatcher, readGate, fixedEpochClock, fixedClock, auditLogger);

    final var channel = new EmbeddedChannel(listener);
    channel.attr(BridgeSession.ATTRIBUTE_KEY).set(session);

    channel.writeInbound(
        textFrame(
            "{\"type\":\"QuoteRequest\",\"reqId\":\"R1\","
                + "\"symbol\":\"EUR/USD\",\"side\":\"Buy\",\"qty\":\"1.00000000\"}"));

    assertTrue(channel.isOpen(), "Channel must remain open when ip_pinned=false");
    assertEquals(1, dispatcher.callCount);
    channel.finishAndReleaseAll();
  }

  // ─── BinaryWebSocketFrame → channel closed with 4008 ────────────────────

  @Test
  void channelRead0_binaryFrame_channelClosedWith4008() throws Exception {
    final var queue = new OutboundQueue(64);
    final var limiter = new PerTypeRateLimiter(FIXED_NOW_NS);
    final var session = buildSession(LOOPBACK, false, List.of(), queue, limiter);

    final var dispatcher = new RecordingDispatcher();
    final var readGate = new InboundReadGate(queue);
    final var listener =
        new WsListener(dispatcher, readGate, fixedEpochClock, fixedClock, auditLogger);

    final var channel = new EmbeddedChannel(listener);
    channel.attr(BridgeSession.ATTRIBUTE_KEY).set(session);

    channel.writeInbound(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(new byte[] {1, 2, 3})));

    // A CloseWebSocketFrame with code 4008 should be written outbound.
    final Object outbound = channel.readOutbound();
    assertNotNull(outbound, "Expected outbound CloseWebSocketFrame");
    assertTrue(
        outbound instanceof CloseWebSocketFrame, "Expected CloseWebSocketFrame, got: " + outbound);
    final var closeFrame = (CloseWebSocketFrame) outbound;
    assertEquals(BridgeCloseCodes.POLICY_VIOLATION, closeFrame.statusCode());
    closeFrame.release();

    channel.finishAndReleaseAll();
  }

  // ─── CloseWebSocketFrame inbound → echoed outbound ───────────────────────

  @Test
  void channelRead0_closeFrame_echoedAndChannelClosed() throws Exception {
    final var queue = new OutboundQueue(64);
    final var limiter = new PerTypeRateLimiter(FIXED_NOW_NS);
    final var session = buildSession(LOOPBACK, false, List.of(), queue, limiter);

    final var dispatcher = new RecordingDispatcher();
    final var readGate = new InboundReadGate(queue);
    final var listener =
        new WsListener(dispatcher, readGate, fixedEpochClock, fixedClock, auditLogger);

    final var channel = new EmbeddedChannel(listener);
    channel.attr(BridgeSession.ATTRIBUTE_KEY).set(session);

    channel.writeInbound(new CloseWebSocketFrame(1000, "bye"));

    // An outbound CloseWebSocketFrame should have been written.
    final Object outbound = channel.readOutbound();
    assertNotNull(outbound, "Expected an outbound CloseWebSocketFrame");
    assertTrue(outbound instanceof CloseWebSocketFrame);
    ((CloseWebSocketFrame) outbound).release();

    channel.finishAndReleaseAll();
  }

  // ─── PingWebSocketFrame → PongWebSocketFrame echoed ─────────────────────

  @Test
  void channelRead0_pingFrame_pongEchoed() throws Exception {
    final var queue = new OutboundQueue(64);
    final var limiter = new PerTypeRateLimiter(FIXED_NOW_NS);
    final var session = buildSession(LOOPBACK, false, List.of(), queue, limiter);

    final var dispatcher = new RecordingDispatcher();
    final var readGate = new InboundReadGate(queue);
    final var listener =
        new WsListener(dispatcher, readGate, fixedEpochClock, fixedClock, auditLogger);

    final var channel = new EmbeddedChannel(listener);
    channel.attr(BridgeSession.ATTRIBUTE_KEY).set(session);

    channel.writeInbound(
        new PingWebSocketFrame(Unpooled.copiedBuffer("ping", StandardCharsets.UTF_8)));

    final Object outbound = channel.readOutbound();
    assertNotNull(outbound, "Expected a PongWebSocketFrame");
    assertTrue(outbound instanceof PongWebSocketFrame, "Expected Pong, got: " + outbound);
    ((PongWebSocketFrame) outbound).release();

    channel.finishAndReleaseAll();
  }

  // ─── readGate.onAfterInboundDispatch called after dispatch ───────────────

  @Test
  void channelRead0_afterDispatch_readGateNotifiedViaAutoReadState() throws Exception {
    // Verify the gate is invoked by filling the queue to the pause threshold (80/100)
    // so the gate latches autoRead=false on the next onAfterInboundDispatch call.
    // We use a capacity-10 queue with 80% = 8-entry pause threshold.
    final var queue = new OutboundQueue(10); // pauseAt=8, resumeAt=5
    final var limiter = new PerTypeRateLimiter(FIXED_NOW_NS);
    final var session = buildSession(LOOPBACK, false, List.of(), queue, limiter);

    // Pre-fill 8 events so the next dispatch call should pause.
    for (int i = 0; i < 8; i++) {
      queue.offer(new com.trading.engine.fixbridge.json.BrowserEvent.Error("fill-" + i));
    }

    final var dispatcher = new RecordingDispatcher();
    final var gate = new InboundReadGate(queue, 80, 50);
    final var listener = new WsListener(dispatcher, gate, fixedEpochClock, fixedClock, auditLogger);

    final var channel = new EmbeddedChannel(listener);
    channel.attr(BridgeSession.ATTRIBUTE_KEY).set(session);

    // Pre-condition: gate starts enabled.
    assertTrue(gate.isAutoReadEnabled());

    channel.writeInbound(
        textFrame(
            "{\"type\":\"QuoteRequest\",\"reqId\":\"R2\","
                + "\"symbol\":\"USD/JPY\",\"side\":\"Sell\",\"qty\":\"1.00000000\"}"));

    // After dispatch the gate checks the queue (still 8 entries — pause threshold).
    // If onAfterInboundDispatch was called the gate should now be paused.
    assertFalse(
        gate.isAutoReadEnabled(),
        "readGate.onAfterInboundDispatch must have been called (gate must be paused at 8/10)");

    channel.finishAndReleaseAll();
  }
}
