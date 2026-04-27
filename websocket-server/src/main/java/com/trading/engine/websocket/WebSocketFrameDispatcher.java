package com.trading.engine.websocket;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.util.ReferenceCountUtil;
import org.agrona.concurrent.NanoClock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Per-channel post-auth frame router. Added to the pipeline dynamically by {@link JwtAuthHandler}
 * after successful authentication. Routes incoming WebSocket frames by SBE templateId.
 *
 * <p><b>Routing:</b>
 *
 * <ul>
 *   <li>60 → re-authentication (token refresh before expiry)
 *   <li>62 → subscribe (add symbol + eventType subscriptions)
 *   <li>63 → unsubscribe (remove subscriptions; empty = unsubscribe all)
 *   <li>65 → client heartbeat
 *   <li>68 → gap request (stub — TODO(APP-35): PR 4)
 *   <li>69 → session resume (stub — TODO(APP-35): PR 4)
 *   <li>71 → client ack
 *   <li>default → warn + close after 3 consecutive unknowns
 * </ul>
 *
 * <p><b>Threading.</b> Per-channel instance, NOT {@code @Sharable}. Runs on the channel's Netty
 * event loop thread only. SBE decoders are reusable fields re-wrapped per {@code channelRead}.
 *
 * <p><b>Allocation.</b> SBE decoders reused per-channel. {@link org.agrona.concurrent.UnsafeBuffer}
 * wraps {@code ByteBuf.nioBuffer()} — zero-copy, valid only within {@code channelRead} scope.
 *
 * @see JwtAuthHandler
 * @see <a href="docs/websocket-architecture.md">WebSocket Architecture — Section 3</a>
 */
public final class WebSocketFrameDispatcher extends ChannelInboundHandlerAdapter {

  private static final Logger LOG = LogManager.getLogger(WebSocketFrameDispatcher.class);

  private final WebSocketSessionManager sessionManager;
  private final JwtValidator jwtValidator;
  private final JtiRevocationCache jtiCache;
  private final UserEntitlementService entitlementService;
  private final WebSocketServerConfig config;
  private final WebSocketMetrics metrics;
  private final NanoClock nanoClock;

  /**
   * Create a per-channel frame dispatcher.
   *
   * @param sessionManager session registry for session lookup
   * @param jwtValidator JWT validator for re-authentication
   * @param jtiCache JTI revocation cache for re-auth JTI checks
   * @param entitlementService account entitlement validator for re-auth refresh
   * @param config server configuration
   * @param metrics metrics instance
   * @param nanoClock monotonic clock for heartbeat timestamps
   */
  public WebSocketFrameDispatcher(
      final WebSocketSessionManager sessionManager,
      final JwtValidator jwtValidator,
      final JtiRevocationCache jtiCache,
      final UserEntitlementService entitlementService,
      final WebSocketServerConfig config,
      final WebSocketMetrics metrics,
      final NanoClock nanoClock) {
    this.sessionManager = sessionManager;
    this.jwtValidator = jwtValidator;
    this.jtiCache = jtiCache;
    this.entitlementService = entitlementService;
    this.config = config;
    this.metrics = metrics;
    this.nanoClock = nanoClock;
  }

  // TODO(APP-35): Full routing implementation in Phase B2.
  // This stub ensures JwtAuthHandler compiles and can dynamically add the dispatcher.

  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
    if (msg instanceof TextWebSocketFrame) {
      // TextWebSocketFrame: release to prevent ByteBuf leak, log warning
      ReferenceCountUtil.release(msg);
      LOG.warn("TextWebSocketFrame received post-auth — not supported, releasing");
      return;
    }
    if (!(msg instanceof BinaryWebSocketFrame)) {
      ReferenceCountUtil.release(msg);
      return;
    }
    // Stub: release frame — full routing implemented in Phase B2
    ReferenceCountUtil.release(msg);
  }

  @Override
  public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
    LOG.error(
        "Unexpected exception in frame dispatcher for {}", ctx.channel().remoteAddress(), cause);
    ctx.close();
  }
}
