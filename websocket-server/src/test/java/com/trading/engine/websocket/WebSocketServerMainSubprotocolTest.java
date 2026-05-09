/*
 * APP-36 §A3 — server-side subprotocol pinning verification.
 *
 * Drives a Netty EmbeddedChannel through a WebSocket upgrade with the
 * Sec-WebSocket-Protocol: trading-ws.v1 header and asserts the server
 * echoes the same value in the handshake response. Without this echo,
 * APP-36's client-side hard-assert (closes PROTOCOL_VIOLATION on
 * mismatch) cannot be satisfied.
 *
 * Threading: single-threaded JUnit; EmbeddedChannel runs synchronously.
 *
 * Allocation: per-test channel + buffers; not on a hot path.
 *
 * Plan reference: APP-36 §2.5 / §A3 / §6 row 6.
 */
package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.util.ReferenceCountUtil;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Pins the WebSocketServerProtocolHandler subprotocol echo for {@code trading-ws.v1}.
 *
 * <p>Mirrors the handler configuration in {@link WebSocketServerMain} line ~230: {@code new
 * WebSocketServerProtocolHandler("/", "trading-ws.v1", false, 65_536, false, true, 30_000)}.
 */
final class WebSocketServerMainSubprotocolTest {

  private static final String SUBPROTOCOL = "trading-ws.v1";
  private static final int MAX_FRAME_SIZE = 65_536;

  @Test
  @DisplayName("upgrade_withSubprotocolHeader_serverEchoesTradingWsV1")
  void upgrade_withSubprotocolHeader_serverEchoesTradingWsV1() throws Exception {
    final var response =
        runUpgrade(
            request -> {
              request.headers().set("Sec-WebSocket-Protocol", SUBPROTOCOL);
            });
    try {
      assertEquals(101, response.status().code(), "expected HTTP 101 Switching Protocols");
      final var echoed = response.headers().get("Sec-WebSocket-Protocol");
      assertEquals(SUBPROTOCOL, echoed, "server must echo trading-ws.v1");
    } finally {
      ReferenceCountUtil.release(response);
    }
  }

  @Test
  @DisplayName("upgrade_missingSubprotocolHeader_serverDoesNotInventOne")
  void upgrade_missingSubprotocolHeader_serverDoesNotInventOne() throws Exception {
    // If the client doesn't send Sec-WebSocket-Protocol, the server MUST NOT
    // echo any subprotocol back (per RFC 6455 §4.2.2). The client-side hard-
    // assert will then close PROTOCOL_VIOLATION because ws.protocol === '' ≠
    // 'trading-ws.v1'.
    final var response = runUpgrade(request -> {});
    try {
      // Per CodeRabbit (MINOR): assert the upgrade itself succeeded
      // before checking header absence — otherwise a non-101 response
      // (e.g. 4xx from the server) would also satisfy "no echoed
      // subprotocol" and the test would pass for the wrong reason.
      assertEquals(101, response.status().code(), "expected HTTP 101 Switching Protocols");
      final var echoed = response.headers().get("Sec-WebSocket-Protocol");
      assertTrue(
          echoed == null || echoed.isEmpty(),
          "server must not echo a subprotocol when client did not request one; got: " + echoed);
    } finally {
      ReferenceCountUtil.release(response);
    }
  }

  // ─── Helpers ───────────────────────────────────────────────────────

  /**
   * Drives an HTTP/1.1 WebSocket upgrade through a Netty {@code EmbeddedChannel} with the
   * production handler configuration and returns the server's {@link FullHttpResponse}. Caller
   * releases the returned buffer.
   */
  private static FullHttpResponse runUpgrade(final Consumer<DefaultFullHttpRequest> mutator) {
    // Server channel: mirrors WebSocketServerMain.java:230.
    final var serverCh =
        new EmbeddedChannel(
            new HttpServerCodec(),
            new HttpObjectAggregator(MAX_FRAME_SIZE),
            new WebSocketServerProtocolHandler(
                "/", SUBPROTOCOL, false, MAX_FRAME_SIZE, false, true, 30_000));

    // Client-side encoder channel: serializes the FullHttpRequest into the
    // wire bytes the server channel's HttpServerCodec expects. Without this
    // round-trip Netty's HttpServerCodec ignores already-parsed FullHttpRequest
    // instances written via writeInbound().
    final var encoderCh = new EmbeddedChannel(new HttpClientCodec());

    try {
      final var request =
          new DefaultFullHttpRequest(
              HttpVersion.HTTP_1_1,
              HttpMethod.GET,
              "/",
              Unpooled.copiedBuffer("", StandardCharsets.UTF_8));
      request.headers().set(HttpHeaderNames.HOST, "localhost");
      request.headers().set(HttpHeaderNames.UPGRADE, "websocket");
      request.headers().set(HttpHeaderNames.CONNECTION, "Upgrade");
      // RFC 6455 §4.1: deterministic 16-byte base64 nonce for tests.
      request.headers().set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==");
      request.headers().set("Sec-WebSocket-Version", "13");
      mutator.accept(request);

      // Encode → wire bytes.
      encoderCh.writeOutbound(request);
      // Drain the encoded outbound queue into the server channel; each
      // chunk is consumed once. `final var raw` per loop iteration so
      // the variable is immutable inside its scope.
      for (Object raw = encoderCh.readOutbound(); raw != null; raw = encoderCh.readOutbound()) {
        serverCh.writeInbound(raw);
      }

      // Server has now produced its handshake response. Decode via a
      // paired client-codec channel so HTTP aggregation matches what
      // a real browser would observe.
      final var decoderCh =
          new EmbeddedChannel(new HttpClientCodec(), new HttpObjectAggregator(MAX_FRAME_SIZE));
      try {
        for (Object outRaw = serverCh.readOutbound();
            outRaw != null;
            outRaw = serverCh.readOutbound()) {
          decoderCh.writeInbound(outRaw);
        }
        final var aggregated = (FullHttpResponse) decoderCh.readInbound();
        assertNotNull(aggregated, "no aggregated upgrade response");
        return aggregated;
      } finally {
        decoderCh.finishAndReleaseAll();
      }
    } finally {
      encoderCh.finishAndReleaseAll();
      serverCh.finishAndReleaseAll();
    }
  }
}
