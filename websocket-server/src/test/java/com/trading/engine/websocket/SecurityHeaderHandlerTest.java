package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.ResourceLeakDetector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link SecurityHeaderHandler} — verifies that security headers (HSTS, CSP,
 * X-Frame-Options, X-Content-Type-Options) are injected on HTTP responses and that non-HTTP
 * messages pass through unmodified.
 *
 * <p>Uses Netty's {@link EmbeddedChannel} for isolated pipeline testing.
 */
final class SecurityHeaderHandlerTest {

  private EmbeddedChannel channel;

  @BeforeAll
  static void enableLeakDetection() {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
  }

  @BeforeEach
  void setUp() {
    channel = new EmbeddedChannel(new SecurityHeaderHandler());
  }

  @AfterEach
  void tearDown() {
    if (channel != null) {
      channel.finishAndReleaseAll();
    }
  }

  @Test
  void write_httpResponse_injectsHstsHeader() {
    final var response =
        new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.SWITCHING_PROTOCOLS);

    channel.writeOutbound(response);

    final var written = (DefaultFullHttpResponse) channel.readOutbound();
    assertNotNull(written);
    assertEquals(
        "max-age=31536000; includeSubDomains; preload",
        written.headers().get("Strict-Transport-Security"));
    written.release();
  }

  @Test
  void write_httpResponse_injectsCspHeader() {
    final var response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);

    channel.writeOutbound(response);

    final var written = (DefaultFullHttpResponse) channel.readOutbound();
    assertNotNull(written);
    assertEquals(
        "connect-src 'self'; script-src 'self'; frame-ancestors 'none'; default-src 'self'",
        written.headers().get("Content-Security-Policy"));
    written.release();
  }

  @Test
  void write_httpResponse_injectsXFrameOptionsHeader() {
    final var response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);

    channel.writeOutbound(response);

    final var written = (DefaultFullHttpResponse) channel.readOutbound();
    assertNotNull(written);
    assertEquals("DENY", written.headers().get("X-Frame-Options"));
    written.release();
  }

  @Test
  void write_httpResponse_injectsXContentTypeOptionsHeader() {
    final var response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);

    channel.writeOutbound(response);

    final var written = (DefaultFullHttpResponse) channel.readOutbound();
    assertNotNull(written);
    assertEquals("nosniff", written.headers().get("X-Content-Type-Options"));
    written.release();
  }

  @Test
  void write_nonHttpMessage_passesThrough() {
    final var plainMessage = "not an HTTP response";

    channel.writeOutbound(plainMessage);

    final var written = (String) channel.readOutbound();
    assertEquals(plainMessage, written);
  }
}
