package com.trading.engine.fixbridge.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link WebSocketHandshaker} — CSWSH defence via exact-match Origin allowlist.
 *
 * <p>Uses Netty's {@link EmbeddedChannel} to drive the handler without a real network transport. On
 * rejection the handler writes an HTTP 403 response and closes the channel; on accept the {@link
 * io.netty.handler.codec.http.FullHttpRequest} is forwarded inbound.
 *
 * <p><b>Threading.</b> Single-threaded — EmbeddedChannel is not thread-safe but test isolation
 * guarantees single-threaded execution per test.
 *
 * <p><b>Allocation.</b> Cold-path (per-connection) — allocation is acceptable in tests.
 */
final class WebSocketHandshakerTest {

  private static final String ALLOWED_ORIGIN = "https://example.com";

  // Helper: build a GET /ws HTTP/1.1 request with a given Origin header value.
  private static DefaultFullHttpRequest buildRequest(final String origin) {
    final var request =
        new DefaultFullHttpRequest(
            HttpVersion.HTTP_1_1, HttpMethod.GET, "/ws", Unpooled.EMPTY_BUFFER);
    if (origin != null) {
      request.headers().set(HttpHeaderNames.ORIGIN, origin);
    }
    return request;
  }

  // --- Empty allowlist rejects every request ---

  @Test
  void channelRead_emptyAllowlist_rejectsAnyOrigin() {
    final var handshaker = new WebSocketHandshaker(Set.of());
    final var channel = new EmbeddedChannel(handshaker);

    channel.writeInbound(buildRequest(ALLOWED_ORIGIN));

    // No forwarded inbound (the request was released and channel closed after rejection).
    assertNull(channel.readInbound());
    // A 403 response was written outbound.
    final FullHttpResponse response = channel.readOutbound();
    assertNotNull(response, "Expected an outbound 403 response");
    assertEquals(HttpResponseStatus.FORBIDDEN, response.status());
    response.release();
    channel.finishAndReleaseAll();
  }

  // --- Matching Origin accepted ---

  @Test
  void channelRead_matchingOrigin_requestForwardedInbound() {
    final var handshaker = new WebSocketHandshaker(Set.of(ALLOWED_ORIGIN));
    final var channel = new EmbeddedChannel(handshaker);

    final var request = buildRequest(ALLOWED_ORIGIN);
    // Retain to survive the pipeline forward so we can read it back.
    request.retain();
    channel.writeInbound(request);

    // The request should have been forwarded to the next handler (i.e. readable inbound).
    final Object forwarded = channel.readInbound();
    assertNotNull(forwarded, "Request should have been forwarded on matching Origin");
    // Release both the retained ref and any remaining pipeline ref.
    if (forwarded instanceof io.netty.util.ReferenceCounted rc) {
      rc.release();
    }
    channel.finishAndReleaseAll();
  }

  // --- Non-matching Origin rejected ---

  @Test
  void channelRead_nonMatchingOrigin_rejectsWithForbidden() {
    final var handshaker = new WebSocketHandshaker(Set.of(ALLOWED_ORIGIN));
    final var channel = new EmbeddedChannel(handshaker);

    channel.writeInbound(buildRequest("https://evil.example"));

    assertNull(channel.readInbound());
    final FullHttpResponse response = channel.readOutbound();
    assertNotNull(response);
    assertEquals(HttpResponseStatus.FORBIDDEN, response.status());
    response.release();
    channel.finishAndReleaseAll();
  }

  // --- Missing Origin rejected ---

  @Test
  void channelRead_missingOriginHeader_rejectsWithForbidden() {
    final var handshaker = new WebSocketHandshaker(Set.of(ALLOWED_ORIGIN));
    final var channel = new EmbeddedChannel(handshaker);

    // buildRequest with null does not add Origin header.
    channel.writeInbound(buildRequest(null));

    assertNull(channel.readInbound());
    final FullHttpResponse response = channel.readOutbound();
    assertNotNull(response);
    assertEquals(HttpResponseStatus.FORBIDDEN, response.status());
    response.release();
    channel.finishAndReleaseAll();
  }

  // --- reload() atomically swaps the allowlist ---

  @Test
  void reload_swapsAllowlist_oldOriginRejectedNewOriginAccepted() {
    final var handshaker = new WebSocketHandshaker(Set.of(ALLOWED_ORIGIN));

    // Before reload: old origin accepted.
    final var channelBefore = new EmbeddedChannel(handshaker);
    final var reqBefore = buildRequest(ALLOWED_ORIGIN);
    reqBefore.retain();
    channelBefore.writeInbound(reqBefore);
    final Object forwardedBefore = channelBefore.readInbound();
    assertNotNull(forwardedBefore, "Old origin should be accepted before reload");
    if (forwardedBefore instanceof io.netty.util.ReferenceCounted rc) {
      rc.release();
    }
    channelBefore.finishAndReleaseAll();

    // Perform the reload.
    final var newOrigin = "https://new.example";
    handshaker.reload(Set.of(newOrigin));

    // After reload: old origin rejected.
    final var channelAfterOld = new EmbeddedChannel(handshaker);
    channelAfterOld.writeInbound(buildRequest(ALLOWED_ORIGIN));
    assertNull(channelAfterOld.readInbound(), "Old origin should be rejected after reload");
    final FullHttpResponse resp1 = channelAfterOld.readOutbound();
    if (resp1 != null) {
      resp1.release();
    }
    channelAfterOld.finishAndReleaseAll();

    // After reload: new origin accepted.
    final var channelAfterNew = new EmbeddedChannel(handshaker);
    final var reqNew = buildRequest(newOrigin);
    reqNew.retain();
    channelAfterNew.writeInbound(reqNew);
    final Object forwardedNew = channelAfterNew.readInbound();
    assertNotNull(forwardedNew, "New origin should be accepted after reload");
    if (forwardedNew instanceof io.netty.util.ReferenceCounted rc) {
      rc.release();
    }
    channelAfterNew.finishAndReleaseAll();
  }

  // --- allowlistSize() ---

  @Test
  void allowlistSize_matchesInputSetSize() {
    final var handshaker = new WebSocketHandshaker(Set.of("https://a.test", "https://b.test"));
    assertEquals(2, handshaker.allowlistSize());
  }

  @Test
  void allowlistSize_emptySet_returnsZero() {
    final var handshaker = new WebSocketHandshaker(Set.of());
    assertEquals(0, handshaker.allowlistSize());
  }

  @Test
  void allowlistSize_afterReload_reflectsNewSize() {
    final var handshaker = new WebSocketHandshaker(Set.of("https://a.test", "https://b.test"));
    handshaker.reload(Set.of("https://x.test"));
    assertEquals(1, handshaker.allowlistSize());
  }
}
