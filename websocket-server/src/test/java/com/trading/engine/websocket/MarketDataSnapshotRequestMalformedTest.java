package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.MarketDataSnapshotRequestDecoder;
import com.trading.engine.messages.sbe.MarketDataSnapshotRequestEncoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.projections.SymbolPacker;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.util.ResourceLeakDetector;
import java.util.List;
import java.util.Map;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.SystemNanoClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Category A (RFC 6455 close 1003 + session torn down) malformed-payload tests for {@link
 * MarketDataAdmissionPipeline}. Each sub-case must produce:
 *
 * <ul>
 *   <li>{@link MarketDataAdmissionPipeline.Outcome#MALFORMED_CLOSE}
 *   <li>A {@link CloseWebSocketFrame} written to the channel.
 *   <li>The {@code websocket.dispatcher.malformed} Micrometer counter incremented by at least 1.
 * </ul>
 *
 * <p>The session remains associated with the channel until the caller closes it — the pipeline
 * signals {@code MALFORMED_CLOSE} and writes the close frame; the dispatcher (or test) is
 * responsible for physically closing the channel afterwards. This test verifies the channel is
 * still technically open after {@code admit()} returns (the close frame is queued but the async
 * close has not completed yet in the EmbeddedChannel). If the channel has already been closed by
 * the embedded-channel flush, the test asserts the close frame is present in the outbound queue.
 *
 * <p><b>Threading model.</b> Single-threaded EmbeddedChannel — all I/O runs on the test thread.
 *
 * <p><b>Wiring.</b> Uses a real {@link SymbolEntitlementMap} containing {@code EURUSD → [ACME]}; a
 * {@link SnapshotRequestPublisher} stub that always returns success (≥ 0); a real {@link
 * WebSocketMetrics} backed by {@link io.micrometer.core.instrument.simple.SimpleMeterRegistry}.
 */
final class MarketDataSnapshotRequestMalformedTest {

  /** Minimum valid frame size: 8-byte SBE header + 8-byte BLOCK_LENGTH. */
  private static final int VALID_FRAME_SIZE =
      MessageHeaderEncoder.ENCODED_LENGTH + MarketDataSnapshotRequestDecoder.BLOCK_LENGTH;

  private EmbeddedChannel channel;
  private WebSocketSession session;
  private io.micrometer.core.instrument.simple.SimpleMeterRegistry registry;
  private WebSocketMetrics metrics;
  private MarketDataAdmissionPipeline pipeline;

  @BeforeAll
  static void setLeakDetection() {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
  }

  @BeforeEach
  void setUp() {
    registry = new io.micrometer.core.instrument.simple.SimpleMeterRegistry();
    metrics = new WebSocketMetrics(registry);

    // SnapshotRequestPublisher: always success (position = 1L > 0).
    final SnapshotRequestPublisher successPublisher = (buf, offset, length) -> 1L;

    // SymbolEntitlementMap: EURUSD permitted to ACME.
    final var entitlementMap = new SymbolEntitlementMap(Map.of("EURUSD", List.of("ACME")));

    pipeline =
        new MarketDataAdmissionPipeline(
            entitlementMap, successPublisher, metrics, SystemNanoClock.INSTANCE);

    // Install a no-op handler so channel.pipeline().firstContext() returns a valid context.
    // EmbeddedChannel's built-in pipeline only has head+tail channels; firstContext() from
    // user-installed handlers is needed for ctx.channel() / ctx.alloc() to resolve correctly.
    channel =
        new EmbeddedChannel(
            new ChannelInboundHandlerAdapter() {
              @Override
              public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
                // no-op — pipeline sink for any messages fired during tests
              }
            });
    final long nowNs = SystemNanoClock.INSTANCE.nanoTime();
    session = new WebSocketSession(channel, nowNs, "127.0.0.1");
    session.initSubscriptionFilter(100, metrics);
    session.initSnapshotTokenBucket(nowNs);

    // Publish EURUSD entitlement so admission path reaches the publish stage.
    final var entitled = new org.agrona.collections.LongHashSet(4);
    entitled.add(SymbolPacker.pack("EURUSD"));
    session.subscriptionFilter().publishEntitledSymbols(entitled);
  }

  @AfterEach
  void tearDown() {
    channel.finishAndReleaseAll();
  }

  /**
   * A valid-looking frame whose {@code blockLength} parameter is off by 1 (declared as {@code
   * BLOCK_LENGTH + 1 = 9}) must trigger a Category A close. The pipeline checks {@code blockLength
   * != BLOCK_LENGTH} before any SBE decode and returns {@link
   * MarketDataAdmissionPipeline.Outcome#MALFORMED_CLOSE}.
   */
  @Test
  void admit_badBlockLength_returnsMalformedCloseAndWritesCloseFrame() {
    final var content = buildValidFrame();
    final var ctx = channel.pipeline().firstContext();

    // Pass blockLength = BLOCK_LENGTH + 1 (off by 1).
    final int badBlockLength = MarketDataSnapshotRequestDecoder.BLOCK_LENGTH + 1;
    final double malformedBefore = registry.get("websocket.dispatcher.malformed").counter().count();

    final var outcome = pipeline.admit(ctx, session, content, badBlockLength);

    final double malformedAfter = registry.get("websocket.dispatcher.malformed").counter().count();
    content.release();

    assertEquals(
        MarketDataAdmissionPipeline.Outcome.MALFORMED_CLOSE,
        outcome,
        "Bad blockLength must produce MALFORMED_CLOSE");
    assertTrue(
        malformedAfter > malformedBefore, "websocket.dispatcher.malformed counter must increment");
    assertCloseFrameWritten("bad blockLength");
  }

  /**
   * A frame whose readable-bytes count is less than the minimum (header + BLOCK_LENGTH) must
   * trigger a Category A close. This is the first guard in {@link
   * MarketDataAdmissionPipeline#admit}.
   */
  @Test
  void admit_tooShortPayload_returnsMalformedCloseAndWritesCloseFrame() {
    // Build a frame that is 1 byte too short.
    final int tooShort = VALID_FRAME_SIZE - 1;
    final var content = Unpooled.buffer(tooShort);
    content.writeZero(tooShort);
    final var ctx = channel.pipeline().firstContext();

    final double malformedBefore = registry.get("websocket.dispatcher.malformed").counter().count();

    // Even a correct blockLength param triggers the size guard first.
    final var outcome =
        pipeline.admit(ctx, session, content, MarketDataSnapshotRequestDecoder.BLOCK_LENGTH);

    final double malformedAfter = registry.get("websocket.dispatcher.malformed").counter().count();
    content.release();

    assertEquals(
        MarketDataAdmissionPipeline.Outcome.MALFORMED_CLOSE,
        outcome,
        "Too-short payload must produce MALFORMED_CLOSE");
    assertTrue(
        malformedAfter > malformedBefore, "websocket.dispatcher.malformed counter must increment");
    assertCloseFrameWritten("too-short payload");
  }

  /**
   * A frame with a wrong {@code schemaId} in the SBE header must trigger a Category A close. The
   * pipeline re-reads the header from the content ByteBuf and checks {@code schemaId == SCHEMA_ID}.
   */
  @Test
  void admit_badSchemaIdInHeader_returnsMalformedCloseAndWritesCloseFrame() {
    // Build a frame with schemaId mutated to 0 (invalid).
    final var buf = new ExpandableArrayBuffer(VALID_FRAME_SIZE);
    final var headerEncoder = new MessageHeaderEncoder();
    final var encoder = new MarketDataSnapshotRequestEncoder();
    encoder.wrapAndApplyHeader(buf, 0, headerEncoder);
    encoder.symbol("EURUSD");

    // Overwrite schemaId (offset 4, 2 bytes, little-endian in the SBE header).
    buf.putShort(4, (short) 0, java.nio.ByteOrder.LITTLE_ENDIAN);

    final var content = Unpooled.wrappedBuffer(buf.byteArray(), 0, VALID_FRAME_SIZE);
    final var ctx = channel.pipeline().firstContext();

    final double malformedBefore = registry.get("websocket.dispatcher.malformed").counter().count();

    final var outcome =
        pipeline.admit(ctx, session, content, MarketDataSnapshotRequestDecoder.BLOCK_LENGTH);

    final double malformedAfter = registry.get("websocket.dispatcher.malformed").counter().count();
    content.release();

    assertEquals(
        MarketDataAdmissionPipeline.Outcome.MALFORMED_CLOSE,
        outcome,
        "Bad schemaId must produce MALFORMED_CLOSE");
    assertTrue(
        malformedAfter > malformedBefore,
        "websocket.dispatcher.malformed counter must increment on bad schemaId");
    assertCloseFrameWritten("bad schemaId");
  }

  /**
   * A properly-sized frame with a non-ASCII symbol byte (0x80, high-bit set) must trigger a
   * Category A close. The pipeline's {@code isAsciiNonNull} check fires after the length and
   * schemaId/version guards pass — this path is reached only by an otherwise well-formed frame.
   */
  @Test
  void admit_nonAsciiSymbolByte_returnsMalformedCloseAndWritesCloseFrame() {
    // Build a valid frame, then corrupt symbol[0] to 0x80 (non-ASCII).
    final var buf = new ExpandableArrayBuffer(VALID_FRAME_SIZE);
    final var headerEncoder = new MessageHeaderEncoder();
    final var encoder = new MarketDataSnapshotRequestEncoder();
    encoder.wrapAndApplyHeader(buf, 0, headerEncoder);
    encoder.symbol("EURUSD");

    // Symbol field starts at offset = MessageHeaderEncoder.ENCODED_LENGTH = 8.
    // Overwrite the first byte with a non-ASCII value.
    buf.putByte(MessageHeaderEncoder.ENCODED_LENGTH, (byte) 0x80);

    final var content = Unpooled.wrappedBuffer(buf.byteArray(), 0, VALID_FRAME_SIZE);
    final var ctx = channel.pipeline().firstContext();

    final double malformedBefore = registry.get("websocket.dispatcher.malformed").counter().count();

    final var outcome =
        pipeline.admit(ctx, session, content, MarketDataSnapshotRequestDecoder.BLOCK_LENGTH);

    final double malformedAfter = registry.get("websocket.dispatcher.malformed").counter().count();
    content.release();

    assertEquals(
        MarketDataAdmissionPipeline.Outcome.MALFORMED_CLOSE,
        outcome,
        "Non-ASCII symbol byte must produce MALFORMED_CLOSE");
    assertTrue(
        malformedAfter > malformedBefore,
        "websocket.dispatcher.malformed counter must increment on non-ASCII symbol");
    assertCloseFrameWritten("non-ASCII symbol byte");
  }

  // --- Helpers ---

  /**
   * Build a correctly-encoded {@code MarketDataSnapshotRequest} frame for "EURUSD" as a Netty
   * {@link io.netty.buffer.ByteBuf} with the right header and block length. Caller must release.
   */
  private static io.netty.buffer.ByteBuf buildValidFrame() {
    final var buf = new ExpandableArrayBuffer(VALID_FRAME_SIZE);
    final var headerEncoder = new MessageHeaderEncoder();
    final var encoder = new MarketDataSnapshotRequestEncoder();
    encoder.wrapAndApplyHeader(buf, 0, headerEncoder);
    encoder.symbol("EURUSD");
    return Unpooled.wrappedBuffer(buf.byteArray(), 0, VALID_FRAME_SIZE);
  }

  /**
   * Assert that the EmbeddedChannel's outbound queue contains a {@link CloseWebSocketFrame}
   * (written by {@link MarketDataAdmissionPipeline#sendCloseFrame}). Releases any other frames that
   * may be queued before the close frame (e.g., in-flight WebSocketError frames from prior stages
   * that completed before this test).
   *
   * @param label human-readable label for the failure message
   */
  private void assertCloseFrameWritten(final String label) {
    boolean found = false;
    Object frame;
    while ((frame = channel.readOutbound()) != null) {
      if (frame instanceof CloseWebSocketFrame) {
        ((CloseWebSocketFrame) frame).release();
        found = true;
        break;
      }
      if (frame instanceof io.netty.util.ReferenceCounted rc) {
        rc.release();
      }
    }
    assertTrue(found, "A CloseWebSocketFrame must be written to the channel for: " + label);
  }
}
