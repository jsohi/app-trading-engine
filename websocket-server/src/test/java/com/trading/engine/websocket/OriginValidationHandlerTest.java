package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.ResourceLeakDetector;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link OriginValidationHandler} — verifies CSWSH prevention via Origin header
 * validation, including whitelist matching, rejection of missing/unknown origins, pass-through for
 * non-HTTP messages, dynamic reload, and whitelist sizing.
 */
final class OriginValidationHandlerTest {

  private final java.util.List<EmbeddedChannel> openChannels = new java.util.ArrayList<>();
  private WebSocketServerConfig config;
  private OriginValidationHandler handler;

  @BeforeAll
  static void setLeakDetection() {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
  }

  @BeforeEach
  void setUp() {
    config =
        WebSocketServerConfig.builder()
            .originsWhitelist(List.of("https://app.example.com"))
            .build();
    handler = new OriginValidationHandler(config);
  }

  @AfterEach
  void tearDown() {
    for (final var ch : openChannels) {
      ch.finishAndReleaseAll();
    }
    openChannels.clear();
  }

  private EmbeddedChannel trackChannel(final EmbeddedChannel ch) {
    openChannels.add(ch);
    return ch;
  }

  @Test
  void channelRead_validOrigin_passesThrough() {
    final var channel = trackChannel(new EmbeddedChannel(handler));
    final var request = createUpgradeRequest("https://app.example.com");

    channel.writeInbound(request);

    final var passed = (FullHttpRequest) channel.readInbound();
    assertNotNull(passed, "Request with a valid Origin must be forwarded through the pipeline");
    passed.release();
  }

  @Test
  void channelRead_missingOrigin_rejectsWithForbidden() {
    final var channel = trackChannel(new EmbeddedChannel(handler));
    final var request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/ws");

    channel.writeInbound(request);

    final var response = (FullHttpResponse) channel.readOutbound();
    assertNotNull(response, "A response must be written for a missing Origin header");
    assertEquals(
        HttpResponseStatus.FORBIDDEN,
        response.status(),
        "Missing Origin must result in 403 Forbidden");
    response.release();

    assertNull(
        channel.readInbound(),
        "Request with missing Origin must not be forwarded through the pipeline");
  }

  @Test
  void channelRead_nonWhitelistedOrigin_rejectsWithForbidden() {
    final var channel = trackChannel(new EmbeddedChannel(handler));
    final var request = createUpgradeRequest("https://evil.com");

    channel.writeInbound(request);

    final var response = (FullHttpResponse) channel.readOutbound();
    assertNotNull(response, "A response must be written for a non-whitelisted Origin");
    assertEquals(
        HttpResponseStatus.FORBIDDEN,
        response.status(),
        "Non-whitelisted Origin must result in 403 Forbidden");
    response.release();

    assertNull(
        channel.readInbound(),
        "Request with non-whitelisted Origin must not be forwarded through the pipeline");
  }

  @Test
  void channelRead_nonHttpMessage_passesThrough() {
    final var channel = trackChannel(new EmbeddedChannel(handler));
    final var nonHttpMsg = "plain-text-message";

    channel.writeInbound(nonHttpMsg);

    final var passed = channel.readInbound();
    assertNotNull(passed, "Non-FullHttpRequest messages must pass through unchanged");
    assertEquals(nonHttpMsg, passed, "The forwarded message must be the original object");
  }

  @Test
  void reloadOrigins_newList_updatesWhitelist() {
    // Initially, only "https://app.example.com" is allowed.
    final var channel1 = trackChannel(new EmbeddedChannel(handler));
    final var blockedRequest = createUpgradeRequest("https://new-app.example.com");
    channel1.writeInbound(blockedRequest);

    final var blockedResponse = (FullHttpResponse) channel1.readOutbound();
    assertNotNull(blockedResponse, "New origin must be rejected before reload");
    assertEquals(HttpResponseStatus.FORBIDDEN, blockedResponse.status());
    blockedResponse.release();

    // Reload with the new origin
    handler.reloadOrigins(List.of("https://app.example.com", "https://new-app.example.com"));

    // Now the new origin should be accepted
    final var channel2 = trackChannel(new EmbeddedChannel(handler));
    final var allowedRequest = createUpgradeRequest("https://new-app.example.com");
    channel2.writeInbound(allowedRequest);

    final var passed = (FullHttpRequest) channel2.readInbound();
    assertNotNull(passed, "New origin must be accepted after reload");
    passed.release();
  }

  @Test
  void whitelistSize_afterConstruction_matchesConfig() {
    final int size = handler.whitelistSize();

    assertEquals(1, size, "Whitelist size must match the number of origins configured (1)");
  }

  /**
   * Create a minimal HTTP upgrade request with the given Origin header.
   *
   * @param origin the Origin header value
   * @return a FullHttpRequest with the Origin header set
   */
  private static FullHttpRequest createUpgradeRequest(final String origin) {
    final var request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/ws");
    request.headers().set("Origin", origin);
    return request;
  }
}
