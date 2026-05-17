package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.MarketDataSnapshotRequestDecoder;
import com.trading.engine.messages.sbe.MarketDataSnapshotRequestEncoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.WebSocketErrorCode;
import com.trading.engine.messages.sbe.WebSocketErrorDecoder;
import com.trading.engine.projections.SymbolPacker;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.util.ResourceLeakDetector;
import java.util.List;
import java.util.Map;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.LongHashSet;
import org.agrona.concurrent.SystemNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Category B (soft-error, session preserved) tests for {@link MarketDataAdmissionPipeline}.
 *
 * <p>In the Category B paths the session is preserved (channel stays open) and the pipeline emits a
 * {@code WebSocketError} binary frame with the matching {@link WebSocketErrorCode}:
 *
 * <ul>
 *   <li>{@link WebSocketErrorCode#SymbolUnknown} — symbol absent from {@link SymbolEntitlementMap};
 *       counter {@code websocket.dispatcher.symbol.unknown}.
 *   <li>{@link WebSocketErrorCode#EntitlementDenied} — symbol in map but not in the session's
 *       per-account entitled set; counter {@code websocket.subscription.entitlement.denied}.
 * </ul>
 *
 * <p><b>Threading model.</b> Single-threaded EmbeddedChannel — all I/O on the test thread.
 *
 * <p><b>Wiring.</b> The entitlement map contains only {@code EURUSD → [ACME]}. Tests for {@code
 * SymbolUnknown} request a symbol not in the map ({@code XYZABC}). Tests for {@code
 * EntitlementDenied} request {@code EURUSD} but publish a session entitlement set that only
 * contains {@code GBPUSD} — so the symbol is known but the session's account is denied.
 */
final class MarketDataSnapshotRequestSoftErrorTest {

  private static final int VALID_FRAME_SIZE =
      MessageHeaderEncoder.ENCODED_LENGTH + MarketDataSnapshotRequestDecoder.BLOCK_LENGTH;

  private EmbeddedChannel channel;
  private WebSocketSession session;
  private SimpleMeterRegistry registry;
  private WebSocketMetrics metrics;
  private MarketDataAdmissionPipeline pipeline;

  @BeforeAll
  static void setLeakDetection() {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
  }

  @BeforeEach
  void setUp() {
    registry = new SimpleMeterRegistry();
    metrics = new WebSocketMetrics(registry);

    final SnapshotRequestPublisher successPublisher = (buf, offset, length) -> 1L;
    final var entitlementMap = new SymbolEntitlementMap(Map.of("EURUSD", List.of("ACME")));

    pipeline =
        new MarketDataAdmissionPipeline(
            entitlementMap, successPublisher, metrics, SystemNanoClock.INSTANCE);

    channel =
        new EmbeddedChannel(
            new ChannelInboundHandlerAdapter() {
              @Override
              public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
                // no-op pipeline sink
              }
            });
    final long nowNs = SystemNanoClock.INSTANCE.nanoTime();
    session = new WebSocketSession(channel, nowNs, "127.0.0.1");
    session.initSubscriptionFilter(100, metrics);
    session.initSnapshotTokenBucket(nowNs);
  }

  @AfterEach
  void tearDown() {
    channel.finishAndReleaseAll();
  }

  /**
   * When the requested symbol is NOT present in the {@link SymbolEntitlementMap} (Category B), the
   * pipeline must:
   *
   * <ol>
   *   <li>Return {@link MarketDataAdmissionPipeline.Outcome#SYMBOL_UNKNOWN}.
   *   <li>Write a {@code WebSocketError(SymbolUnknown)} binary frame.
   *   <li>Increment {@code websocket.dispatcher.symbol.unknown}.
   *   <li>Keep the channel open.
   * </ol>
   */
  @Test
  void admit_symbolNotInEntitlementMap_returnsSymbolUnknownAndKeepsChannelOpen() {
    // "UNKNWN00" (8 chars, all ASCII, no NUL padding) is not in the entitlement map.
    // Must use exactly 8 printable ASCII chars so the pipeline's isAsciiNonNull check passes.
    final var content = buildFrame("UNKNWN00");
    final var ctx = channel.pipeline().firstContext();

    final double unknownBefore =
        registry.get("websocket.dispatcher.symbol.unknown").counter().count();

    final var outcome =
        pipeline.admit(ctx, session, content, MarketDataSnapshotRequestDecoder.BLOCK_LENGTH);

    final double unknownAfter =
        registry.get("websocket.dispatcher.symbol.unknown").counter().count();
    content.release();

    assertEquals(
        MarketDataAdmissionPipeline.Outcome.SYMBOL_UNKNOWN,
        outcome,
        "Symbol absent from entitlement map must produce SYMBOL_UNKNOWN");
    assertTrue(
        unknownAfter > unknownBefore, "websocket.dispatcher.symbol.unknown counter must increment");
    assertTrue(channel.isActive(), "Channel must remain open for soft error");

    assertWebSocketError(WebSocketErrorCode.SymbolUnknown, "symbol not in map");
  }

  /**
   * When the requested symbol IS in the {@link SymbolEntitlementMap} but the session's per-account
   * entitlement set does not include it (Category B entitlement check), the pipeline must:
   *
   * <ol>
   *   <li>Return {@link MarketDataAdmissionPipeline.Outcome#ENTITLEMENT_DENIED}.
   *   <li>Write a {@code WebSocketError(EntitlementDenied)} binary frame.
   *   <li>Increment {@code websocket.subscription.entitlement.denied}.
   *   <li>Keep the channel open.
   * </ol>
   *
   * <p>Setup: entitlement map has {@code EURUSD → ACME}. Session's entitled set is published with
   * only {@code GBPUSD} — so EURUSD is known but not entitled for this session.
   */
  @Test
  void admit_symbolInMapButNotInSessionEntitlement_returnsEntitlementDeniedAndKeepsChannelOpen() {
    // Publish entitlement for GBPUSD only — EURUSD is NOT entitled in this session.
    final long packedGbp = SymbolPacker.pack("GBPUSD");
    final var entitled = new LongHashSet(4);
    entitled.add(packedGbp);
    session.subscriptionFilter().publishEntitledSymbols(entitled);

    // Request EURUSD — which is in the map but not in this session's entitled set.
    final var content = buildFrame("EURUSD");
    final var ctx = channel.pipeline().firstContext();

    final double deniedBefore =
        registry.get("websocket.subscription.entitlement.denied").counter().count();

    final var outcome =
        pipeline.admit(ctx, session, content, MarketDataSnapshotRequestDecoder.BLOCK_LENGTH);

    final double deniedAfter =
        registry.get("websocket.subscription.entitlement.denied").counter().count();
    content.release();

    assertEquals(
        MarketDataAdmissionPipeline.Outcome.ENTITLEMENT_DENIED,
        outcome,
        "Symbol in map but not entitled must produce ENTITLEMENT_DENIED");
    assertTrue(
        deniedAfter > deniedBefore,
        "websocket.subscription.entitlement.denied counter must increment");
    assertTrue(channel.isActive(), "Channel must remain open for entitlement denial");

    assertWebSocketError(WebSocketErrorCode.EntitlementDenied, "entitlement denied");
  }

  // --- Helpers ---

  /**
   * Build a correctly-encoded {@code MarketDataSnapshotRequest} for the given symbol. Caller must
   * release the returned ByteBuf.
   */
  private static io.netty.buffer.ByteBuf buildFrame(final String symbol) {
    final var buf = new ExpandableArrayBuffer(VALID_FRAME_SIZE);
    final var headerEncoder = new MessageHeaderEncoder();
    final var encoder = new MarketDataSnapshotRequestEncoder();
    encoder.wrapAndApplyHeader(buf, 0, headerEncoder);
    encoder.symbol(symbol);
    return Unpooled.wrappedBuffer(buf.byteArray(), 0, VALID_FRAME_SIZE);
  }

  /**
   * Read the first outbound frame from the channel and assert it is a {@code
   * WebSocketError(errorCode)} binary frame.
   *
   * @param expected the expected error code
   * @param label diagnostic label for assertion messages
   */
  private void assertWebSocketError(final WebSocketErrorCode expected, final String label) {
    final var outbound = channel.readOutbound();
    assertNotNull(outbound, "Pipeline must write a WebSocketError response for: " + label);
    assertInstanceOf(
        BinaryWebSocketFrame.class,
        outbound,
        "Response frame must be BinaryWebSocketFrame for: " + label);

    final var responseFrame = (BinaryWebSocketFrame) outbound;
    try {
      final var responseBuf = new UnsafeBuffer(responseFrame.content().nioBuffer());
      final var headerDecoder = new MessageHeaderDecoder();
      headerDecoder.wrap(responseBuf, 0);

      assertEquals(
          WebSocketErrorDecoder.TEMPLATE_ID,
          headerDecoder.templateId(),
          "Response must be WebSocketError (templateId=" + WebSocketErrorDecoder.TEMPLATE_ID + ")");

      final var errorDecoder = new WebSocketErrorDecoder();
      errorDecoder.wrapAndApplyHeader(responseBuf, 0, headerDecoder);

      assertEquals(
          expected,
          errorDecoder.errorCode(),
          "WebSocketError code must be " + expected + " for: " + label);
    } finally {
      responseFrame.release();
    }
  }
}
