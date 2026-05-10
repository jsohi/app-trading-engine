package com.trading.engine.fixbridge.transport;

import com.trading.engine.fixbridge.audit.AuditAction;
import com.trading.engine.fixbridge.audit.AuditLogger;
import com.trading.engine.fixbridge.json.BrowserEvent;
import com.trading.engine.fixbridge.json.BrowserMessageReader;
import com.trading.engine.fixbridge.json.JsonParseException;
import com.trading.engine.fixbridge.json.MutableParsedMessage;
import com.trading.engine.fixbridge.json.OrderRejectReason;
import com.trading.engine.fixbridge.ratelimit.PerTypeRateLimiter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import java.net.InetSocketAddress;
import java.util.Objects;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.NanoClock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Post-authentication frame router.
 *
 * <p>Sits at the tail of the bridge's Netty pipeline once {@link
 * com.trading.engine.fixbridge.auth.JwtAuthHandler JwtAuthHandler} has minted a {@link
 * BridgeSession}. Responsibilities:
 *
 * <ol>
 *   <li>Enforce remote-IP pinning when the session's JWT carries {@code ip_pinned=true} (default
 *       per fail-secure §3.3 / §20.4c).
 *   <li>Parse each {@link TextWebSocketFrame} via {@link BrowserMessageReader} into the per-channel
 *       {@link MutableParsedMessage} flyweight (zero-alloc).
 *   <li>Apply {@link PerTypeRateLimiter} to command-bearing types ({@code QuoteRequest}, {@code
 *       AcceptQuote}, {@code RejectQuote}, {@code NewOrderSingle}, {@code CancelOrder}).
 *   <li>Hand the parsed message to the supplied {@link BridgeFrameDispatcher} (a SAM seam — the
 *       full translator + Artio wiring lands in subsequent days).
 *   <li>Notify the {@link InboundReadGate} after each dispatch so backpressure can latch the
 *       channel paused if the outbound queue is filling.
 * </ol>
 *
 * <p><b>Threading.</b> Per-channel instance, NOT {@code @Sharable}. All callbacks fire on the
 * channel's event loop.
 *
 * <p><b>Allocation.</b> Zero on the hot path. The flyweight, parser, and limiter are all per-
 * instance and stateful. The only allocation occurs on protocol violations (close-frame text) which
 * are cold by definition.
 *
 * <p><b>Reference counting.</b> Extends {@link SimpleChannelInboundHandler} so Netty automatically
 * releases inbound frames after {@link #channelRead0} returns.
 */
public final class WsListener extends SimpleChannelInboundHandler<WebSocketFrame> {

  private static final Logger LOG = LogManager.getLogger(WsListener.class);

  private final BridgeFrameDispatcher dispatcher;
  private final InboundReadGate readGate;
  private final EpochNanoClock epochNanoClock;
  private final NanoClock nanoClock;
  private final AuditLogger auditLogger;

  /**
   * Per-channel parser flyweight. Allocated once at handler construction; its 64 KiB scratch buffer
   * is reused across every inbound frame.
   */
  private final MutableParsedMessage parsed = new MutableParsedMessage();

  /**
   * Construct a per-channel listener.
   *
   * @param dispatcher SAM hook invoked after parse + rate-limit admit (use {@link
   *     BridgeFrameDispatcher#NOOP} until the real dispatch path lands)
   * @param readGate the per-channel inbound read gate (post-dispatch backpressure check)
   * @param epochNanoClock wall-clock used as the {@code tsNs} for {@link AuditLogger#record} —
   *     {@link AuditLogger}'s contract is epoch nanoseconds (NOT monotonic), so audit entries are
   *     correlatable with wall-clock incident timelines
   * @param nanoClock monotonic clock used as the rate-limiter timestamp (separate from the audit
   *     clock so monotonic rate semantics aren't broken by wall-clock NTP adjustments)
   * @param auditLogger audit sink — receives {@link AuditAction#RATE_LIMIT_HIT} entries when the
   *     per-type limiter rejects an inbound command
   */
  public WsListener(
      final BridgeFrameDispatcher dispatcher,
      final InboundReadGate readGate,
      final EpochNanoClock epochNanoClock,
      final NanoClock nanoClock,
      final AuditLogger auditLogger) {
    this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
    this.readGate = Objects.requireNonNull(readGate, "readGate");
    this.epochNanoClock = Objects.requireNonNull(epochNanoClock, "epochNanoClock");
    this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
    this.auditLogger = Objects.requireNonNull(auditLogger, "auditLogger");
  }

  @Override
  protected void channelRead0(final ChannelHandlerContext ctx, final WebSocketFrame frame) {
    // Control frames (Ping/Pong/Close) are handled by Netty's WebSocketServerProtocolHandler
    // upstream; if one reaches us it's because the upstream handler chose to forward it. Pong
    // frames are silently absorbed (heartbeat liveness is tracked at the protocol layer); Close
    // frames trigger a clean shutdown; Ping frames are echoed as Pong.
    if (frame instanceof PingWebSocketFrame) {
      ctx.writeAndFlush(new PongWebSocketFrame(frame.content().retainedDuplicate()));
      return;
    }
    if (frame instanceof PongWebSocketFrame) {
      return;
    }
    if (frame instanceof CloseWebSocketFrame closeFrame) {
      ctx.writeAndFlush(closeFrame.retain()).addListener(future -> ctx.close());
      return;
    }
    if (!(frame instanceof TextWebSocketFrame textFrame)) {
      // BinaryWebSocketFrame and ContinuationWebSocketFrame are not part of the bridge's wire
      // protocol — closes the channel as a policy violation per §3.1 strictness contract.
      LOG.warn("Unexpected frame type {} from {}", frame.getClass().getSimpleName(), remoteIp(ctx));
      closeWithCode(ctx, BridgeCloseCodes.POLICY_VIOLATION, "non-text-frame");
      return;
    }

    final var session = ctx.channel().attr(BridgeSession.ATTRIBUTE_KEY).get();
    if (session == null) {
      // Defensive — JwtAuthHandler should have either installed a session or closed the channel
      // before we are reachable. Treat as policy violation.
      LOG.error("WsListener invoked without BridgeSession (auth pipeline regression)");
      closeWithCode(ctx, BridgeCloseCodes.POLICY_VIOLATION, "no-session");
      return;
    }

    if (!checkIpPin(ctx, session)) {
      return;
    }

    final long nowNs = nanoClock.nanoTime();

    final int messageType;
    try {
      messageType = BrowserMessageReader.parse(textFrame.content(), parsed);
    } catch (final JsonParseException e) {
      // Malformed / oversized / unknown-type / precision-violation. Send an Error event and
      // continue — repeated parse failures are downgraded into a policy-violation close by the
      // upstream rate limiter (future work; APP-40a does not enforce a per-failure quota).
      session.enqueue(new BrowserEvent.Error(e.reason()));
      readGate.onAfterInboundDispatch(ctx);
      return;
    }

    final var commandType = mapCommandType(messageType);
    if (commandType != null) {
      final var outcome = session.perTypeRateLimiter().tryConsume(commandType, nowNs);
      if (outcome != PerTypeRateLimiter.Outcome.ALLOWED) {
        // Translate to the configured taxonomy reason (initial-window vs steady-state).
        final var reason =
            outcome == PerTypeRateLimiter.Outcome.REJECTED_INITIAL_WINDOW
                ? OrderRejectReason.RATE_LIMIT_INITIAL_WINDOW
                : OrderRejectReason.RATE_LIMIT_EXCEEDED;
        // Best-effort audit trail — auditLogger.Noop is the default until the launcher (tracked
        // separately) binds Log4j2. Audit tsNs uses the EpochNanoClock (wall-clock nanoseconds)
        // not the rate-limiter's monotonic nowNs — AuditLogger.record's contract is epoch-ns so
        // entries correlate with wall-clock incident timelines.
        if (auditLogger.isWritable()) {
          auditLogger.record(
              epochNanoClock.nanoTime(),
              session.claims().sub(),
              session.claims().jti(),
              remoteIp(ctx),
              AuditAction.RATE_LIMIT_HIT,
              null,
              null,
              0L,
              0L,
              null,
              null,
              null,
              null,
              null,
              null,
              reason.wireValue(),
              null,
              null);
        }
        // RATE_LIMIT_* applies to order-issuing commands; clOrdId comes from the parsed flyweight
        // when present (NewOrderSingle/CancelOrder/AcceptQuote always carry one). Otherwise the
        // taxonomy reason is the only field surfaced.
        final var clOrdId =
            parsed.clOrdIdOff >= 0
                ? new String(parsed.scratch, parsed.clOrdIdOff, parsed.clOrdIdLen)
                : "";
        session.enqueue(new BrowserEvent.OrderReject(clOrdId, reason));
        readGate.onAfterInboundDispatch(ctx);
        return;
      }
    }

    dispatcher.dispatch(session, parsed, messageType, nowNs);
    readGate.onAfterInboundDispatch(ctx);
  }

  /**
   * Verify the inbound frame's remote IP against the pinned address captured at handshake. Returns
   * {@code true} if the channel may proceed; {@code false} after triggering a policy- violation
   * close.
   *
   * <p>If the session's JWT carries {@code ip_pinned=false} the check is skipped — pinning is
   * opt-out, fail-secure default per §20.4c.
   */
  private boolean checkIpPin(final ChannelHandlerContext ctx, final BridgeSession session) {
    if (!session.claims().ipPinned()) {
      return true;
    }
    final var addr = ctx.channel().remoteAddress();
    if (!(addr instanceof InetSocketAddress inet)) {
      // Unknown remote address shape (test channel, embedded loopback) — fail-open here only when
      // pinning is moot; this branch is unreachable under real Netty NIO/Epoll/KQueue transports
      // where remote addresses are always InetSocketAddress.
      return true;
    }
    final var current = inet.getAddress();
    if (!session.pinnedRemoteAddress().equals(current)) {
      LOG.warn(
          "IP-pin violation for sub={} session={}: pinned={} observed={}",
          session.claims().sub(),
          session.sessionId(),
          session.pinnedRemoteAddress(),
          current);
      closeWithCode(ctx, BridgeCloseCodes.POLICY_VIOLATION, "ip-pin");
      return false;
    }
    return true;
  }

  private static PerTypeRateLimiter.CommandType mapCommandType(final int parsedType) {
    switch (parsedType) {
      case MutableParsedMessage.TYPE_QUOTE_REQUEST:
        return PerTypeRateLimiter.CommandType.QUOTE_REQUEST;
      case MutableParsedMessage.TYPE_ACCEPT_QUOTE:
        return PerTypeRateLimiter.CommandType.ACCEPT_QUOTE;
      case MutableParsedMessage.TYPE_REJECT_QUOTE:
        return PerTypeRateLimiter.CommandType.REJECT_QUOTE;
      case MutableParsedMessage.TYPE_NEW_ORDER_SINGLE:
        return PerTypeRateLimiter.CommandType.NEW_ORDER_SINGLE;
      case MutableParsedMessage.TYPE_CANCEL_ORDER:
        return PerTypeRateLimiter.CommandType.CANCEL_ORDER;
      default:
        // OrderStatusRequest is excluded (recovery path); Auth is consumed by JwtAuthHandler.
        return null;
    }
  }

  private static void closeWithCode(
      final ChannelHandlerContext ctx, final int closeCode, final String reasonText) {
    final var close = new CloseWebSocketFrame(closeCode, reasonText);
    ctx.writeAndFlush(close).addListener(future -> ctx.close());
  }

  private static String remoteIp(final ChannelHandlerContext ctx) {
    final var addr = ctx.channel().remoteAddress();
    if (addr instanceof InetSocketAddress inet) {
      return inet.getAddress().getHostAddress();
    }
    return addr != null ? addr.toString() : "unknown";
  }
}
