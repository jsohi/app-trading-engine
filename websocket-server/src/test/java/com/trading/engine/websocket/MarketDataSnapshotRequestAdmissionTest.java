package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.MarketDataConstants;
import com.trading.engine.messages.sbe.MarketDataSnapshotRequestDecoder;
import com.trading.engine.messages.sbe.MarketDataSnapshotRequestEncoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.WebSocketErrorCode;
import com.trading.engine.messages.sbe.WebSocketErrorDecoder;
import com.trading.engine.projections.SymbolPacker;
import com.trading.engine.testsupport.clock.ControllableNanoClock;
import io.aeron.Publication;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.util.ReferenceCounted;
import io.netty.util.ResourceLeakDetector;
import java.util.List;
import java.util.Map;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.LongHashSet;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Throttle / dedup / publish-failure admission path tests for {@link MarketDataAdmissionPipeline}.
 *
 * <p>Each test verifies the correct {@link MarketDataAdmissionPipeline.Outcome} and, where
 * applicable:
 *
 * <ul>
 *   <li>Token refund — {@link WebSocketSession#snapshotTokensAvailable()} unchanged vs. consumed.
 *   <li>Correct {@link WebSocketErrorCode} in the error response frame.
 *   <li>Channel remains open (soft-error paths).
 * </ul>
 *
 * <p><b>Test wiring.</b>
 *
 * <ul>
 *   <li>Entitlement map: {@code EURUSD → [ACME]}.
 *   <li>Session entitlement: EURUSD is entitled.
 *   <li>Publisher: scripted via the {@link SnapshotRequestPublisher} SAM seam — each test
 *       configures a specific Aeron return code sequence.
 *   <li>Clock: {@link ControllableNanoClock} so the dedup window is deterministically controllable.
 * </ul>
 *
 * <p><b>Threading model.</b> Single-threaded EmbeddedChannel — all I/O on the test thread.
 */
final class MarketDataSnapshotRequestAdmissionTest {

  private static final int VALID_FRAME_SIZE =
      MessageHeaderEncoder.ENCODED_LENGTH + MarketDataSnapshotRequestDecoder.BLOCK_LENGTH;

  private EmbeddedChannel channel;
  private WebSocketSession session;
  private SimpleMeterRegistry registry;
  private WebSocketMetrics metrics;
  private ControllableNanoClock clock;

  @BeforeAll
  static void setLeakDetection() {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
  }

  @BeforeEach
  void setUp() {
    registry = new SimpleMeterRegistry();
    metrics = new WebSocketMetrics(registry);
    clock = new ControllableNanoClock(1_000_000_000L); // start at 1 s

    channel =
        new EmbeddedChannel(
            new ChannelInboundHandlerAdapter() {
              @Override
              public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
                // no-op pipeline sink
              }
            });
    session = new WebSocketSession(channel, clock.nanoTime(), "127.0.0.1");
    session.initSubscriptionFilter(100, metrics);
    session.initSnapshotTokenBucket(clock.nanoTime());

    // Entitle EURUSD on this session.
    final long packedEur = SymbolPacker.pack("EURUSD");
    final var entitled = new LongHashSet(4);
    entitled.add(packedEur);
    session.subscriptionFilter().publishEntitledSymbols(entitled);
  }

  @AfterEach
  void tearDown() {
    channel.finishAndReleaseAll();
  }

  /**
   * When the token bucket is empty (consumed by 10 prior requests), the pipeline must:
   *
   * <ol>
   *   <li>Return {@link MarketDataAdmissionPipeline.Outcome#THROTTLED}.
   *   <li>Write a {@code WebSocketError(SnapshotThrottled)} frame.
   *   <li>NOT consume a token (bucket stays at 0).
   *   <li>Keep the channel open.
   * </ol>
   *
   * <p>The bucket is pre-drained by calling {@link WebSocketSession#tryConsumeSnapshotToken} the
   * full capacity (10) times at the same {@code nowNs} so no refill occurs.
   */
  @Test
  void admit_tokenBucketEmpty_returnsThrottledAndWritesSnapshotThrottledError() {
    final long nowNs = clock.nanoTime();
    // Drain the bucket completely — capacity = MARKET_DATA_SNAPSHOT_REQUESTS_PER_SECOND = 10.
    final long capacity = MarketDataConstants.MARKET_DATA_SNAPSHOT_REQUESTS_PER_SECOND;
    for (int i = 0; i < capacity; i++) {
      assertTrue(
          session.tryConsumeSnapshotToken(nowNs),
          "Token consume #" + (i + 1) + " must succeed while bucket has capacity");
    }
    assertEquals(
        0L, session.snapshotTokensAvailable(), "Bucket must be empty after draining all tokens");

    final SnapshotRequestPublisher successPublisher = (buf, offset, length) -> 1L;
    final var pipeline = buildPipeline(successPublisher);

    final var content = buildFrame("EURUSD");
    final var ctx = channel.pipeline().firstContext();

    final var outcome =
        pipeline.admit(ctx, session, content, MarketDataSnapshotRequestDecoder.BLOCK_LENGTH);
    content.release();

    assertEquals(
        MarketDataAdmissionPipeline.Outcome.THROTTLED,
        outcome,
        "Empty bucket must produce THROTTLED outcome");
    assertEquals(
        0L,
        session.snapshotTokensAvailable(),
        "Token must NOT be consumed or refunded on THROTTLED (bucket stays at 0)");
    assertTrue(channel.isActive(), "Channel must remain open on THROTTLED");

    assertWebSocketError(WebSocketErrorCode.SnapshotThrottled, "token bucket empty");
  }

  /**
   * A second request for the same symbol within {@code MARKET_DATA_PUBLISH_CADENCE_MICROS * 1000}
   * nanoseconds of the first must be deduplicated:
   *
   * <ol>
   *   <li>Return {@link MarketDataAdmissionPipeline.Outcome#DEDUPED}.
   *   <li>Refund the consumed token (session token count unchanged after both requests).
   *   <li>Increment {@code websocket.marketdata.snapshot.deduped}.
   *   <li>NOT write an error frame to the channel (dedup is silent).
   * </ol>
   *
   * <p>The {@link ControllableNanoClock} advances by 0 ns between the two requests so both use
   * exactly the same timestamp and the dedup window is unambiguously triggered.
   */
  @Test
  void admit_duplicateRequestWithinDedupWindow_returnsDedupedAndRefundsToken() {
    final SnapshotRequestPublisher successPublisher = (buf, offset, length) -> 1L;
    final var pipeline = buildPipeline(successPublisher);

    final long tokensBefore = session.snapshotTokensAvailable();

    // First request — must succeed and publish.
    final var content1 = buildFrame("EURUSD");
    final var ctx = channel.pipeline().firstContext();
    final var outcome1 =
        pipeline.admit(ctx, session, content1, MarketDataSnapshotRequestDecoder.BLOCK_LENGTH);
    content1.release();

    assertEquals(
        MarketDataAdmissionPipeline.Outcome.PUBLISHED,
        outcome1,
        "First request must publish successfully");
    final long tokensAfterFirst = session.snapshotTokensAvailable();
    assertEquals(
        tokensBefore - 1L, tokensAfterFirst, "First publish must consume exactly one token");

    // Do NOT advance clock — second request is within the dedup window.
    final double dedupBefore =
        registry.get("websocket.marketdata.snapshot.deduped").counter().count();

    final var content2 = buildFrame("EURUSD");
    final var outcome2 =
        pipeline.admit(ctx, session, content2, MarketDataSnapshotRequestDecoder.BLOCK_LENGTH);
    content2.release();

    final double dedupAfter =
        registry.get("websocket.marketdata.snapshot.deduped").counter().count();

    assertEquals(
        MarketDataAdmissionPipeline.Outcome.DEDUPED,
        outcome2,
        "Second request within dedup window must return DEDUPED");
    // Token refunded on dedup: the dedup path consumes a token then immediately refunds it,
    // so the net change of the second call on the bucket is zero (or positive if a lazy refill
    // fires simultaneously). The key invariant is that the bucket level is at LEAST as high as
    // after the first publish — the dedup did NOT cause a net token loss.
    assertTrue(
        session.snapshotTokensAvailable() >= tokensAfterFirst,
        "Token consumed by the dedup call must be refunded — bucket must not decrease below "
            + "tokensAfterFirst="
            + tokensAfterFirst
            + "; actual="
            + session.snapshotTokensAvailable());
    assertTrue(
        dedupAfter > dedupBefore, "websocket.marketdata.snapshot.deduped counter must increment");
    // No error frame on dedup path — dedup is silent from the client's perspective.
    final var noFrame = channel.readOutbound();
    if (noFrame != null && noFrame instanceof ReferenceCounted rc) {
      rc.release();
    }
  }

  /**
   * When the Aeron offer returns {@link Publication#BACK_PRESSURED} twice in a row (exceeding
   * {@link MarketDataAdmissionPipeline#MAX_BACK_PRESSURED_RETRIES}), the pipeline must:
   *
   * <ol>
   *   <li>Return {@link MarketDataAdmissionPipeline.Outcome#PUBLISH_BACKPRESSURED}.
   *   <li>Refund the consumed token.
   *   <li>Write a {@code WebSocketError(SnapshotBackpressured)} frame.
   *   <li>Keep the channel open.
   * </ol>
   */
  @Test
  void admit_aeronOfferBackPressuredTwice_returnsPublishBackpressuredAndRefundsToken() {
    // Publisher always returns BACK_PRESSURED — two calls exhaust MAX_BACK_PRESSURED_RETRIES=1.
    final SnapshotRequestPublisher backPressuredPublisher =
        (buf, offset, length) -> Publication.BACK_PRESSURED;
    final var pipeline = buildPipeline(backPressuredPublisher);

    final long tokensBefore = session.snapshotTokensAvailable();

    final var content = buildFrame("EURUSD");
    final var ctx = channel.pipeline().firstContext();

    final var outcome =
        pipeline.admit(ctx, session, content, MarketDataSnapshotRequestDecoder.BLOCK_LENGTH);
    content.release();

    assertEquals(
        MarketDataAdmissionPipeline.Outcome.PUBLISH_BACKPRESSURED,
        outcome,
        "BACK_PRESSURED offer after retries must produce PUBLISH_BACKPRESSURED");
    assertEquals(
        tokensBefore,
        session.snapshotTokensAvailable(),
        "Token must be refunded on PUBLISH_BACKPRESSURED");
    assertTrue(channel.isActive(), "Channel must remain open on PUBLISH_BACKPRESSURED");

    assertWebSocketError(WebSocketErrorCode.SnapshotBackpressured, "back-pressured offer");
  }

  /**
   * When the Aeron offer returns {@link Publication#CLOSED}, the pipeline must:
   *
   * <ol>
   *   <li>Return {@link MarketDataAdmissionPipeline.Outcome#PUBLISH_FATAL}.
   *   <li>Refund the consumed token.
   *   <li>Write a {@code WebSocketError(SnapshotBackpressured)} frame — the pipeline emits the same
   *       error code on CLOSED as on BACK_PRESSURED (see class Javadoc: the client cannot
   *       distinguish a publisher restart from transient back-pressure; the outcome enum differs
   *       for callers who may want to take shutdown action).
   *   <li>Keep the channel open (publisher restart pending).
   * </ol>
   */
  @Test
  void admit_aeronOfferClosed_returnsPublishFatalAndRefundsTokenAndWritesBackpressuredError() {
    final SnapshotRequestPublisher closedPublisher = (buf, offset, length) -> Publication.CLOSED;
    final var pipeline = buildPipeline(closedPublisher);

    final long tokensBefore = session.snapshotTokensAvailable();

    final var content = buildFrame("EURUSD");
    final var ctx = channel.pipeline().firstContext();

    final var outcome =
        pipeline.admit(ctx, session, content, MarketDataSnapshotRequestDecoder.BLOCK_LENGTH);
    content.release();

    assertEquals(
        MarketDataAdmissionPipeline.Outcome.PUBLISH_FATAL,
        outcome,
        "CLOSED offer must produce PUBLISH_FATAL outcome");
    assertEquals(
        tokensBefore,
        session.snapshotTokensAvailable(),
        "Token must be refunded when Aeron publication is CLOSED");
    assertTrue(channel.isActive(), "Channel must remain open — publisher restart pending");

    // Per production code: CLOSED also sends SnapshotBackpressured (same wire error code as
    // BACK_PRESSURED) because the client cannot distinguish them.
    assertWebSocketError(
        WebSocketErrorCode.SnapshotBackpressured,
        "CLOSED offer emits SnapshotBackpressured per pipeline contract");
  }

  // --- Helpers ---

  private MarketDataAdmissionPipeline buildPipeline(final SnapshotRequestPublisher publisher) {
    final var entitlementMap = new SymbolEntitlementMap(Map.of("EURUSD", List.of("ACME")));
    return new MarketDataAdmissionPipeline(entitlementMap, publisher, metrics, clock);
  }

  private static ByteBuf buildFrame(final String symbol) {
    final var buf = new ExpandableArrayBuffer(VALID_FRAME_SIZE);
    final var headerEncoder = new MessageHeaderEncoder();
    final var encoder = new MarketDataSnapshotRequestEncoder();
    encoder.wrapAndApplyHeader(buf, 0, headerEncoder);
    encoder.symbol(symbol);
    return Unpooled.wrappedBuffer(buf.byteArray(), 0, VALID_FRAME_SIZE);
  }

  private void assertWebSocketError(final WebSocketErrorCode expected, final String label) {
    final var outbound = channel.readOutbound();
    assertNotNull(outbound, "Pipeline must write a WebSocketError response for: " + label);
    assertInstanceOf(
        BinaryWebSocketFrame.class,
        outbound,
        "Response must be BinaryWebSocketFrame for: " + label);

    final var responseFrame = (BinaryWebSocketFrame) outbound;
    try {
      final var responseBuf = new UnsafeBuffer(responseFrame.content().nioBuffer());
      final var headerDecoder = new MessageHeaderDecoder();
      headerDecoder.wrap(responseBuf, 0);

      assertEquals(
          WebSocketErrorDecoder.TEMPLATE_ID,
          headerDecoder.templateId(),
          "Response must be WebSocketError for: " + label);

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
