package com.trading.engine.websocket;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpResponse;

/**
 * Injects security response headers on the HTTP upgrade response before WebSocket handshake
 * completes.
 *
 * <p><b>Headers injected</b> (per architecture doc Section 4):
 *
 * <ul>
 *   <li>{@code Strict-Transport-Security: max-age=31536000; includeSubDomains; preload}
 *   <li>{@code Content-Security-Policy: connect-src 'self'; script-src 'self'; frame-ancestors
 *       'none'; default-src 'self'}
 *   <li>{@code X-Frame-Options: DENY}
 *   <li>{@code X-Content-Type-Options: nosniff}
 * </ul>
 *
 * <p>This handler is an enhancement over the architecture doc pipeline — Section 4 specifies these
 * headers but doesn't include a dedicated handler in the pipeline definition.
 *
 * <p><b>Threading.</b> Stateless singleton ({@code @Sharable}) — safe to share across all channels.
 *
 * <p><b>Allocation.</b> Header strings are constants — zero allocation per response.
 *
 * @see <a href="docs/websocket-architecture.md">WebSocket Architecture — Section 4</a>
 */
@ChannelHandler.Sharable
public final class SecurityHeaderHandler extends ChannelDuplexHandler {

  private static final String HSTS_VALUE = "max-age=31536000; includeSubDomains; preload";
  private static final String CSP_VALUE =
      "connect-src 'self'; script-src 'self'; frame-ancestors 'none'; default-src 'self'";

  /**
   * Intercept outbound HTTP responses and inject security headers (HSTS, CSP, X-Frame-Options,
   * X-Content-Type-Options) before the response is written to the channel.
   *
   * @param ctx the channel handler context
   * @param msg the outbound message (checked for {@link HttpResponse})
   * @param promise the write promise
   * @throws Exception if the super call throws
   */
  @Override
  public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise)
      throws Exception {
    if (msg instanceof HttpResponse response) {
      final var headers = response.headers();
      headers.set("Strict-Transport-Security", HSTS_VALUE);
      headers.set("Content-Security-Policy", CSP_VALUE);
      headers.set("X-Frame-Options", "DENY");
      headers.set("X-Content-Type-Options", "nosniff");
    }
    super.write(ctx, msg, promise);
  }
}
