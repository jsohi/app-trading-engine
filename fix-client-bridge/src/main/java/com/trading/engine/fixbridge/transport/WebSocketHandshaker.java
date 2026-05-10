package com.trading.engine.fixbridge.transport;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.Objects;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * CSWSH (Cross-Site WebSocket Hijacking) defence — exact-match {@code Origin} header allowlist
 * applied <i>before</i> the WebSocket upgrade completes (§3.1 / §3.3).
 *
 * <p>Mirrors {@code OriginValidationHandler} from {@code :websocket-server}. Intentionally NOT
 * shared between the two modules: each module's pipeline owns its own copy so a future schema
 * divergence (different default response, different audit hook, different metric emission) does not
 * have to be co-ordinated across modules.
 *
 * <p>HTTP upgrade requests with a missing, empty, or non-allowlisted {@code Origin} header are
 * rejected with HTTP {@code 403 Forbidden} and the channel is closed. An empty allowlist rejects
 * every request — this is the fail-safe default that protects against operator misconfiguration.
 *
 * <p><b>Threading.</b> {@code @Sharable} singleton across all channels. The {@code Set<String>} is
 * held as a {@code volatile} reference so the future {@link #reload(Set)} entry point (e.g.
 * SIGHUP-driven config reload) is publication-safe across event loops.
 *
 * <p><b>Allocation.</b> One Set lookup per upgrade — cold path (per-connection, not per-message).
 *
 * @see com.trading.engine.fixbridge.FixClientBridgeConfig#allowedOrigins()
 */
@ChannelHandler.Sharable
public final class WebSocketHandshaker extends ChannelInboundHandlerAdapter {

  private static final Logger LOG = LogManager.getLogger(WebSocketHandshaker.class);

  private volatile Set<String> allowedOrigins;

  /**
   * Construct a handshaker bound to the supplied allowlist (defensively copied).
   *
   * @param allowedOrigins exact-match allowlist; never {@code null}, may be empty (= reject all)
   */
  public WebSocketHandshaker(final Set<String> allowedOrigins) {
    Objects.requireNonNull(allowedOrigins, "allowedOrigins");
    this.allowedOrigins = Set.copyOf(allowedOrigins);
  }

  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
    if (msg instanceof FullHttpRequest request) {
      final var origin = request.headers().get("Origin");
      if (origin == null || origin.isEmpty()) {
        LOG.warn(
            "WS upgrade rejected: missing Origin header from {}", ctx.channel().remoteAddress());
        reject(ctx, request);
        return;
      }
      if (!allowedOrigins.contains(origin)) {
        LOG.warn(
            "WS upgrade rejected: Origin '{}' not allow-listed (from {})",
            origin,
            ctx.channel().remoteAddress());
        reject(ctx, request);
        return;
      }
    }
    ctx.fireChannelRead(msg);
  }

  /**
   * Atomically swap the allowlist. Subsequent upgrades observe the new set immediately because the
   * field is {@code volatile}.
   *
   * @param newOrigins new allowlist (defensively copied)
   */
  public void reload(final Set<String> newOrigins) {
    Objects.requireNonNull(newOrigins, "newOrigins");
    this.allowedOrigins = Set.copyOf(newOrigins);
    LOG.info("Origin allowlist reloaded: {} origins", newOrigins.size());
  }

  /**
   * @return the current allowlist size (visible for diagnostics / tests)
   */
  public int allowlistSize() {
    return allowedOrigins.size();
  }

  private static void reject(final ChannelHandlerContext ctx, final FullHttpRequest request) {
    // Capture the protocol version BEFORE releasing the request, otherwise the response
    // construction would be use-after-release.
    final var protocolVersion = request.protocolVersion();
    request.release();
    final var response = new DefaultFullHttpResponse(protocolVersion, HttpResponseStatus.FORBIDDEN);
    ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
  }
}
