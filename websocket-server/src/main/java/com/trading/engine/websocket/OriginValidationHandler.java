package com.trading.engine.websocket;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * CSWSH (Cross-Site WebSocket Hijacking) prevention via Origin header validation.
 *
 * <p>Validates the HTTP {@code Origin} header on WebSocket upgrade requests against an exact-match
 * whitelist. Rejects missing, null, or non-whitelisted origins with HTTP 403 Forbidden.
 *
 * <p><b>Configuration reload.</b> The whitelist is backed by a {@code volatile Set<String>} and can
 * be reloaded at runtime via {@link #reloadOrigins(List)} (e.g., on SIGHUP signal). The reload is
 * atomic — a single volatile write swaps the entire set reference. All subsequent requests see the
 * new whitelist immediately.
 *
 * <p><b>Empty whitelist.</b> When the whitelist is empty, all origins are rejected. This is the
 * fail-safe default — production deployments must configure allowed origins.
 *
 * <p><b>Threading.</b> Shared singleton ({@code @Sharable}) across all channels. The {@code
 * volatile Set} ensures visibility of reloads from the SIGHUP handler thread to all event loop
 * threads.
 *
 * <p><b>Allocation.</b> Set lookup per upgrade request (one-time per connection, not hot path).
 *
 * @see <a href="docs/websocket-architecture.md">WebSocket Architecture — Section 4</a>
 */
@ChannelHandler.Sharable
public final class OriginValidationHandler extends ChannelInboundHandlerAdapter {

  private static final Logger LOG = LogManager.getLogger(OriginValidationHandler.class);

  private volatile Set<String> allowedOrigins;

  /**
   * Create an origin validation handler with the initial whitelist.
   *
   * @param config the server configuration (for originsWhitelist)
   */
  public OriginValidationHandler(final WebSocketServerConfig config) {
    Objects.requireNonNull(config, "config");
    this.allowedOrigins = Set.copyOf(config.originsWhitelist());
  }

  /**
   * Validate the Origin header on HTTP upgrade requests. Missing, empty, or non-whitelisted origins
   * are rejected with HTTP 403 Forbidden and the channel is closed.
   *
   * @param ctx the channel handler context
   * @param msg the inbound message (checked for {@link FullHttpRequest})
   * @throws Exception if the super call throws
   */
  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
    if (msg instanceof FullHttpRequest request) {
      final var origin = request.headers().get("Origin");

      if (origin == null || origin.isEmpty()) {
        LOG.warn(
            "WebSocket upgrade rejected: missing Origin header from {}",
            ctx.channel().remoteAddress());
        reject(ctx, request);
        return;
      }

      if (!allowedOrigins.contains(origin)) {
        LOG.warn(
            "WebSocket upgrade rejected: Origin '{}' not in whitelist from {}",
            origin,
            ctx.channel().remoteAddress());
        reject(ctx, request);
        return;
      }
    }

    ctx.fireChannelRead(msg);
  }

  private static void reject(final ChannelHandlerContext ctx, final FullHttpRequest request) {
    // Read protocol version BEFORE releasing the request to avoid use-after-release.
    final var protocolVersion = request.protocolVersion();
    request.release();
    final var response = new DefaultFullHttpResponse(protocolVersion, HttpResponseStatus.FORBIDDEN);
    ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
  }

  /**
   * Reload the origin whitelist at runtime (e.g., on SIGHUP signal). Atomic replacement via
   * volatile write — all subsequent requests see the new whitelist immediately.
   *
   * @param newOrigins the new list of allowed origins
   */
  public void reloadOrigins(final List<String> newOrigins) {
    Objects.requireNonNull(newOrigins, "newOrigins");
    this.allowedOrigins = Set.copyOf(newOrigins);
    LOG.info("Origin whitelist reloaded: {} origins", newOrigins.size());
  }

  /**
   * @return the current number of allowed origins in the whitelist
   */
  public int whitelistSize() {
    return allowedOrigins.size();
  }
}
