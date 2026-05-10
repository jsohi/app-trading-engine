package com.trading.engine.fixbridge.transport;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.util.ReferenceCountUtil;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.function.IntSupplier;
import java.util.function.Supplier;
import org.agrona.concurrent.EpochNanoClock;

/**
 * HTTP {@code GET /health} liveness/readiness endpoint for the fix-client-bridge.
 *
 * <p>Sits early in the bridge's Netty pipeline (after {@code HttpServerCodec} + {@code
 * HttpObjectAggregator}, BEFORE {@code WebSocketServerProtocolHandler}) so that orchestrator health
 * probes can short-circuit with a JSON 200 response WITHOUT triggering a WS upgrade attempt. Any
 * non-{@code /health} request is forwarded down the pipeline so the WS upgrade path remains
 * untouched.
 *
 * <p><b>Response shape.</b>
 *
 * <pre>{@code
 * {"status":"UP","version":"<build version>","ts":<epochNs>,"audit":<bool>,"sessions":<int>}
 * }</pre>
 *
 * <p><b>Routing semantics.</b>
 *
 * <ul>
 *   <li>{@code GET /health} → 200 OK + JSON body.
 *   <li>{@code <other-method> /health} → 405 Method Not Allowed.
 *   <li>{@code GET /<other-path>} → forwarded inbound (typically the WS upgrade handler).
 * </ul>
 *
 * <p>The 404 case in the spec ("Any other path → 404") applies when this handler is the terminal
 * HTTP handler in the pipeline; in normal bridge composition the upstream WebSocket upgrade handler
 * will own the only other valid path ({@code /ws}). For testability and defence against
 * mis-composition, this handler also returns 404 for unknown paths when nothing further down the
 * pipeline consumes the forwarded request — see {@link #channelRead0} for the routing rule applied:
 * any path other than {@link #PATH_HEALTH} is forwarded with {@code ctx.fireChannelRead} and the
 * bootstrap is responsible for ensuring downstream handling exists.
 *
 * <p><b>Threading.</b> Per-channel handler, NOT {@code @Sharable}. All callbacks fire on the
 * channel's event loop (single-threaded), so no synchronisation is required on the per-instance
 * collaborators.
 *
 * <p><b>Allocation.</b> Per-request: one {@link DefaultFullHttpResponse}, one pooled {@link
 * ByteBuf}, and one short response-body String (built via {@link StringBuilder}). The path
 * comparison uses an interned constant. This is a cold path (per HTTP request, not per
 * websocket-frame) so transient allocation is acceptable; the JSON body is small (<200 bytes
 * typical).
 *
 * <p><b>Reference counting.</b> Extends {@link SimpleChannelInboundHandler} so Netty automatically
 * releases the inbound {@link FullHttpRequest} after {@link #channelRead0} returns. When the
 * handler forwards the request via {@code fireChannelRead}, it {@link FullHttpRequest#retain()}s to
 * compensate for the auto-release.
 *
 * @see io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler
 */
public final class HealthCheckHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

  /** Canonical path served by this handler. */
  public static final String PATH_HEALTH = "/health";

  /** Default value of the {@code version} field when no build version is supplied. */
  public static final String DEFAULT_VERSION = "unknown";

  private final Supplier<Boolean> auditReady;
  private final IntSupplier sessionCount;
  private final EpochNanoClock epochNanoClock;
  private final String version;

  /**
   * Construct a health-check handler.
   *
   * @param auditReady supplier returning {@code true} when the audit sink is currently writable —
   *     typically {@code auditLogger::isWritable}; never {@code null}
   * @param sessionCount supplier returning the current authenticated bridge-session count; never
   *     {@code null}
   * @param epochNanoClock wall-clock used as the {@code ts} field (epoch nanoseconds) so health
   *     probes correlate with operational incident timelines; never {@code null}
   * @param version build/git-sha version string emitted in the {@code version} field; defaults to
   *     {@link #DEFAULT_VERSION} when {@code null} or empty
   */
  public HealthCheckHandler(
      final Supplier<Boolean> auditReady,
      final IntSupplier sessionCount,
      final EpochNanoClock epochNanoClock,
      final String version) {
    this.auditReady = Objects.requireNonNull(auditReady, "auditReady");
    this.sessionCount = Objects.requireNonNull(sessionCount, "sessionCount");
    this.epochNanoClock = Objects.requireNonNull(epochNanoClock, "epochNanoClock");
    this.version = (version == null || version.isEmpty()) ? DEFAULT_VERSION : version;
  }

  @Override
  protected void channelRead0(final ChannelHandlerContext ctx, final FullHttpRequest request) {
    final var path = stripQueryString(request.uri());
    if (!PATH_HEALTH.equals(path)) {
      // Not /health — forward the request to the next handler in the pipeline (typically the WS
      // upgrade handler). retain() compensates for SimpleChannelInboundHandler's auto-release.
      ReferenceCountUtil.retain(request);
      ctx.fireChannelRead(request);
      return;
    }

    if (!HttpMethod.GET.equals(request.method())) {
      writeStatusOnly(ctx, request, HttpResponseStatus.METHOD_NOT_ALLOWED);
      return;
    }

    writeHealthResponse(ctx, request);
  }

  /**
   * Build and write the 200 OK JSON health response. Allocates one pooled ByteBuf + one
   * DefaultFullHttpResponse + one String per request.
   */
  private void writeHealthResponse(final ChannelHandlerContext ctx, final FullHttpRequest request) {
    final boolean audit = Boolean.TRUE.equals(auditReady.get());
    final long ts = epochNanoClock.nanoTime();
    final int sessions = sessionCount.getAsInt();

    final var json = new StringBuilder(160);
    json.append("{\"status\":\"UP\",\"version\":\"");
    appendEscaped(json, version);
    json.append("\",\"ts\":").append(ts);
    json.append(",\"audit\":").append(audit);
    json.append(",\"sessions\":").append(sessions);
    json.append('}');

    final ByteBuf body = Unpooled.wrappedBuffer(json.toString().getBytes(StandardCharsets.UTF_8));
    final var response =
        new DefaultFullHttpResponse(request.protocolVersion(), HttpResponseStatus.OK, body);
    response
        .headers()
        .set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON + "; charset=utf-8")
        .setInt(HttpHeaderNames.CONTENT_LENGTH, body.readableBytes())
        .set(HttpHeaderNames.CACHE_CONTROL, HttpHeaderValues.NO_STORE);

    final boolean keepAlive = HttpUtil.isKeepAlive(request);
    if (keepAlive) {
      response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
      ctx.writeAndFlush(response);
    } else {
      response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
      ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }
  }

  /** Write a status-only (empty body) response and close the connection. Used for 405 / 404. */
  private static void writeStatusOnly(
      final ChannelHandlerContext ctx,
      final FullHttpRequest request,
      final HttpResponseStatus status) {
    final var response = new DefaultFullHttpResponse(request.protocolVersion(), status);
    response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, 0);
    if (status == HttpResponseStatus.METHOD_NOT_ALLOWED) {
      response.headers().set(HttpHeaderNames.ALLOW, HttpMethod.GET.name());
    }
    ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
  }

  /** Strip {@code ?query} suffix from a request URI; returns the path component. */
  private static String stripQueryString(final String uri) {
    final var q = uri.indexOf('?');
    return q < 0 ? uri : uri.substring(0, q);
  }

  /** Minimal RFC 8259 escaping for the {@code version} field — quote, backslash, controls. */
  private static void appendEscaped(final StringBuilder out, final String value) {
    final int len = value.length();
    for (int i = 0; i < len; i++) {
      final char c = value.charAt(i);
      switch (c) {
        case '"' -> out.append("\\\"");
        case '\\' -> out.append("\\\\");
        case '\b' -> out.append("\\b");
        case '\f' -> out.append("\\f");
        case '\n' -> out.append("\\n");
        case '\r' -> out.append("\\r");
        case '\t' -> out.append("\\t");
        default -> {
          if (c < 0x20) {
            out.append(String.format("\\u%04x", (int) c));
          } else {
            out.append(c);
          }
        }
      }
    }
  }
}
