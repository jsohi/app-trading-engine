package com.trading.engine.fixbridge.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.ReferenceCountUtil;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.agrona.concurrent.EpochNanoClock;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link HealthCheckHandler}.
 *
 * <p>Drives the handler via Netty's {@link EmbeddedChannel} so the test runs without a real network
 * transport. Uses a hand-rolled {@link EpochNanoClock} stub returning a fixed timestamp so
 * assertions on the {@code ts} field are deterministic.
 *
 * <p>Coverage:
 *
 * <ul>
 *   <li>{@code GET /health} → 200 OK + JSON containing all expected fields and headers.
 *   <li>{@code GET /health?probe=1} (with query string) → 200 (path stripping works).
 *   <li>{@code GET /unknown} → forwarded inbound (delegated to next pipeline handler).
 *   <li>{@code POST /health} → 405 Method Not Allowed with {@code Allow: GET}.
 *   <li>{@code audit=false} reflected in the JSON body.
 *   <li>Constructor null-arg validation.
 * </ul>
 *
 * <p><b>Threading.</b> Single-threaded — EmbeddedChannel is not thread-safe, but each test creates
 * its own channel.
 */
final class HealthCheckHandlerTest {

  private static final long FIXED_TS_NS = 1_700_000_000_000_000_000L;
  private static final EpochNanoClock FIXED_CLOCK = () -> FIXED_TS_NS;

  // ---------------------------------------------------------------------------
  // Constructor
  // ---------------------------------------------------------------------------

  @Test
  void constructor_nullAuditReady_throws() {
    assertThrows(
        NullPointerException.class, () -> new HealthCheckHandler(null, () -> 0, FIXED_CLOCK, "v1"));
  }

  @Test
  void constructor_nullSessionCount_throws() {
    assertThrows(
        NullPointerException.class,
        () -> new HealthCheckHandler(() -> true, null, FIXED_CLOCK, "v1"));
  }

  @Test
  void constructor_nullClock_throws() {
    assertThrows(
        NullPointerException.class, () -> new HealthCheckHandler(() -> true, () -> 0, null, "v1"));
  }

  @Test
  void constructor_nullVersion_defaultsToUnknown() {
    final var channel =
        new EmbeddedChannel(new HealthCheckHandler(() -> true, () -> 0, FIXED_CLOCK, null));
    channel.writeInbound(buildRequest(HttpMethod.GET, "/health"));
    final FullHttpResponse response = channel.readOutbound();
    final var body = bodyAsString(response);
    assertTrue(body.contains("\"version\":\"unknown\""), "default version expected: " + body);
    response.release();
    channel.finishAndReleaseAll();
  }

  @Test
  void constructor_emptyVersion_defaultsToUnknown() {
    final var channel =
        new EmbeddedChannel(new HealthCheckHandler(() -> true, () -> 0, FIXED_CLOCK, ""));
    channel.writeInbound(buildRequest(HttpMethod.GET, "/health"));
    final FullHttpResponse response = channel.readOutbound();
    final var body = bodyAsString(response);
    assertTrue(body.contains("\"version\":\"unknown\""), "default version expected: " + body);
    response.release();
    channel.finishAndReleaseAll();
  }

  // ---------------------------------------------------------------------------
  // GET /health → 200 + JSON
  // ---------------------------------------------------------------------------

  @Test
  void getHealth_returns200WithCompleteJsonBody() {
    final var auditReady = new AtomicBoolean(true);
    final var sessions = new AtomicInteger(7);
    final var ts = new AtomicLong(FIXED_TS_NS);
    final var handler =
        new HealthCheckHandler(auditReady::get, sessions::get, ts::get, "0.1.0-abc123");
    final var channel = new EmbeddedChannel(handler);

    channel.writeInbound(buildRequest(HttpMethod.GET, "/health"));

    final FullHttpResponse response = channel.readOutbound();
    assertNotNull(response, "expected an outbound 200 response");
    assertEquals(HttpResponseStatus.OK, response.status());
    assertEquals(
        HttpHeaderValues.APPLICATION_JSON + "; charset=utf-8",
        response.headers().get(HttpHeaderNames.CONTENT_TYPE));
    assertEquals(
        HttpHeaderValues.NO_STORE.toString(),
        response.headers().get(HttpHeaderNames.CACHE_CONTROL));

    final var body = bodyAsString(response);
    assertTrue(body.contains("\"status\":\"UP\""), "status missing: " + body);
    assertTrue(body.contains("\"version\":\"0.1.0-abc123\""), "version missing: " + body);
    assertTrue(body.contains("\"ts\":" + FIXED_TS_NS), "ts missing: " + body);
    assertTrue(body.contains("\"audit\":true"), "audit missing: " + body);
    assertTrue(body.contains("\"sessions\":7"), "sessions missing: " + body);

    response.release();
    channel.finishAndReleaseAll();
  }

  @Test
  void getHealth_withQueryString_stripsAndReturns200() {
    final var channel =
        new EmbeddedChannel(new HealthCheckHandler(() -> true, () -> 1, FIXED_CLOCK, "v1"));
    channel.writeInbound(buildRequest(HttpMethod.GET, "/health?probe=1"));
    final FullHttpResponse response = channel.readOutbound();
    assertEquals(HttpResponseStatus.OK, response.status());
    response.release();
    channel.finishAndReleaseAll();
  }

  // ---------------------------------------------------------------------------
  // audit=false reflected in JSON
  // ---------------------------------------------------------------------------

  @Test
  void getHealth_auditFalse_reflectedInJson() {
    final var channel =
        new EmbeddedChannel(new HealthCheckHandler(() -> false, () -> 0, FIXED_CLOCK, "v1"));
    channel.writeInbound(buildRequest(HttpMethod.GET, "/health"));
    final FullHttpResponse response = channel.readOutbound();
    final var body = bodyAsString(response);
    assertTrue(body.contains("\"audit\":false"), "audit=false missing: " + body);
    response.release();
    channel.finishAndReleaseAll();
  }

  @Test
  void getHealth_auditSupplierReturnsNull_treatedAsFalse() {
    // Defensive — Supplier<Boolean>.get() may return null; the handler must coerce to false.
    final var channel =
        new EmbeddedChannel(new HealthCheckHandler(() -> null, () -> 0, FIXED_CLOCK, "v1"));
    channel.writeInbound(buildRequest(HttpMethod.GET, "/health"));
    final FullHttpResponse response = channel.readOutbound();
    final var body = bodyAsString(response);
    assertTrue(body.contains("\"audit\":false"), "null audit must serialise as false: " + body);
    response.release();
    channel.finishAndReleaseAll();
  }

  // ---------------------------------------------------------------------------
  // Unknown path → forwarded inbound (next handler responsibility)
  // ---------------------------------------------------------------------------

  @Test
  void getUnknownPath_forwardedToNextHandler() {
    final var channel =
        new EmbeddedChannel(new HealthCheckHandler(() -> true, () -> 0, FIXED_CLOCK, "v1"));
    final var request = buildRequest(HttpMethod.GET, "/unknown");
    channel.writeInbound(request);

    // No outbound response from this handler — the request was forwarded.
    final Object response = channel.readOutbound();
    assertNull(response, "non-/health requests must not be answered by HealthCheckHandler");

    // The request was forwarded inbound; SimpleChannelInboundHandler's auto-release would normally
    // fire after channelRead0 returns, but our retain() compensates.
    final Object forwarded = channel.readInbound();
    assertNotNull(forwarded, "request should have been forwarded inbound");
    ReferenceCountUtil.release(forwarded);
    channel.finishAndReleaseAll();
  }

  // ---------------------------------------------------------------------------
  // POST /health → 405
  // ---------------------------------------------------------------------------

  @Test
  void postHealth_returns405WithAllowHeader() {
    final var channel =
        new EmbeddedChannel(new HealthCheckHandler(() -> true, () -> 0, FIXED_CLOCK, "v1"));
    channel.writeInbound(buildRequest(HttpMethod.POST, "/health"));

    final FullHttpResponse response = channel.readOutbound();
    assertNotNull(response);
    assertEquals(HttpResponseStatus.METHOD_NOT_ALLOWED, response.status());
    assertEquals(
        HttpMethod.GET.name(),
        response.headers().get(HttpHeaderNames.ALLOW),
        "405 must include Allow: GET per RFC 9110 §15.5.6");
    assertEquals(0, response.content().readableBytes(), "405 response body must be empty");
    response.release();
    channel.finishAndReleaseAll();
  }

  @Test
  void deleteHealth_returns405() {
    final var channel =
        new EmbeddedChannel(new HealthCheckHandler(() -> true, () -> 0, FIXED_CLOCK, "v1"));
    channel.writeInbound(buildRequest(HttpMethod.DELETE, "/health"));
    final FullHttpResponse response = channel.readOutbound();
    assertNotNull(response);
    assertEquals(HttpResponseStatus.METHOD_NOT_ALLOWED, response.status());
    response.release();
    channel.finishAndReleaseAll();
  }

  // ---------------------------------------------------------------------------
  // JSON escaping — version field
  // ---------------------------------------------------------------------------

  @Test
  void getHealth_versionContainingQuoteAndBackslash_isEscaped() {
    final var channel =
        new EmbeddedChannel(
            new HealthCheckHandler(() -> true, () -> 0, FIXED_CLOCK, "v\"1\\backslash"));
    channel.writeInbound(buildRequest(HttpMethod.GET, "/health"));
    final FullHttpResponse response = channel.readOutbound();
    final var body = bodyAsString(response);
    assertTrue(body.contains("\"version\":\"v\\\"1\\\\backslash\""), "escaping wrong: " + body);
    response.release();
    channel.finishAndReleaseAll();
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static FullHttpRequest buildRequest(final HttpMethod method, final String uri) {
    return new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, uri, Unpooled.EMPTY_BUFFER);
  }

  private static String bodyAsString(final FullHttpResponse response) {
    return response.content().toString(StandardCharsets.UTF_8);
  }
}
