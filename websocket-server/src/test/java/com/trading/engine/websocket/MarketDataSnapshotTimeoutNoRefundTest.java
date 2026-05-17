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
import io.netty.util.ResourceLeakDetector;
import java.util.List;
import java.util.Map;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.LongHashSet;
import org.agrona.concurrent.SystemNanoClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Documentation-level test asserting the <b>timeout-no-refund contract</b> for snapshot requests.
 *
 * <h2>Contract specification</h2>
 *
 * The publish to Aeron stream 205 succeeds ({@link MarketDataAdmissionPipeline.Outcome#PUBLISHED})
 * and a token is consumed from the session's bucket. If the publisher subsequently does NOT deliver
 * the snapshot {@code MarketDataTick} within {@link
 * com.trading.engine.messages.MarketDataConstants#MARKET_DATA_SNAPSHOT_TIMEOUT_MS} ms, the
 * rate-limit token is <b>NOT refunded</b>.
 *
 * <h2>Why no {@code markSnapshotTimeout} method?</h2>
 *
 * Timeout tracking is enforced in the BROWSER ({@code feedState$} transitions to {@code QUIET}) and
 * optionally by the publisher's own watchdog — NOT by this pipeline. Evidence:
 *
 * <ul>
 *   <li>{@link MarketDataAdmissionPipeline} has no {@code markSnapshotTimeout} method — this test
 *       confirms by inspection and compilation that no such method exists on the class.
 *   <li>The stream-205 publish already committed a slot in the publisher's bounded queue — the
 *       publisher consumed CPU scheduling the response. Refunding would allow a hostile client to
 *       spam snapshot requests by deliberately timing them out (timeout-as-rate-limit-escape).
 *   <li>The {@code websocket.marketdata.snapshot.timeout} counter is incremented by the {@link
 *       WebSocketMetrics} on external timeout notification (if wired), not by this pipeline.
 * </ul>
 *
 * <h2>This test asserts</h2>
 *
 * <ol>
 *   <li>{@link MarketDataAdmissionPipeline} compiles without a {@code markSnapshotTimeout} method
 *       (proof by absence — the class has no such method; a future accidental addition would break
 *       the API contract and should be flagged).
 *   <li>A successful publish path ({@link MarketDataAdmissionPipeline.Outcome#PUBLISHED}) consumes
 *       exactly one token and does NOT refund it.
 * </ol>
 *
 * <p><b>Threading model.</b> Single-threaded EmbeddedChannel — all I/O on the test thread.
 */
final class MarketDataSnapshotTimeoutNoRefundTest {

  private static final int VALID_FRAME_SIZE =
      MessageHeaderEncoder.ENCODED_LENGTH + MarketDataSnapshotRequestDecoder.BLOCK_LENGTH;

  private EmbeddedChannel channel;
  private WebSocketSession session;

  @BeforeAll
  static void setLeakDetection() {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
  }

  @BeforeEach
  void setUp() {
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
    session.initSubscriptionFilter(100);
    session.initSnapshotTokenBucket(nowNs);

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
   * Confirms that {@link MarketDataAdmissionPipeline} does NOT have a {@code markSnapshotTimeout}
   * method. The absence of the method is the architectural proof that timeout-handling is not the
   * pipeline's responsibility — it belongs to the browser and/or publisher watchdog.
   *
   * <p>A successful publish consumes exactly one token and does NOT refund it, demonstrating the
   * timeout-no-refund contract: once the stream-205 offer succeeds the rate-limit slot is consumed
   * regardless of whether the publisher delivers the snapshot within the timeout budget.
   */
  @Test
  void publishedOutcome_consumesOneTokenWithoutRefund_andPipelineHasNoTimeoutMethod() {
    // --- Proof by absence: MarketDataAdmissionPipeline must not have markSnapshotTimeout ---
    //
    // This assertion is enforced at compile time (the method simply does not exist), and we
    // document it here so reviewers understand why there is no explicit refund-on-timeout code.
    // If a future commit accidentally adds markSnapshotTimeout, the code referencing it here
    // would fail to compile (or could be caught by a reflection-based assertion).
    //
    // We use reflection to make the contract explicit in test output rather than relying purely
    // on compilation:
    // Mutable local acceptable here: the try/catch structure requires two assignment paths,
    // which is inherently incompatible with final; this is not a hot path.
    boolean hasMarkSnapshotTimeout;
    try {
      MarketDataAdmissionPipeline.class.getDeclaredMethod("markSnapshotTimeout");
      hasMarkSnapshotTimeout = true;
    } catch (final NoSuchMethodException e) {
      hasMarkSnapshotTimeout = false;
    }
    assertTrue(
        !hasMarkSnapshotTimeout,
        "MarketDataAdmissionPipeline must NOT have a markSnapshotTimeout method — "
            + "timeout handling lives in the browser / publisher watchdog, not this pipeline. "
            + "Adding this method would break the timeout-no-refund rate-limit accounting contract.");

    // --- Successful publish path: token consumed, NOT refunded ---
    //
    // After a PUBLISHED outcome, session.snapshotTokensAvailable() decreases by 1.
    // No timeout event arrives (this test is single-shot); the token remains consumed.
    // This mirrors the production scenario where the publisher eventually delivers the
    // snapshot tick — the token is not refunded even if the snapshot is slow.
    final var metrics = WebSocketMetrics.createWithDefaults();
    final SnapshotRequestPublisher successPublisher = (buf, offset, length) -> 1L;
    final var entitlementMap = new SymbolEntitlementMap(Map.of("EURUSD", List.of("ACME")));
    final var pipeline =
        new MarketDataAdmissionPipeline(
            entitlementMap, successPublisher, metrics, SystemNanoClock.INSTANCE);

    final long tokensBefore = session.snapshotTokensAvailable();
    assertTrue(tokensBefore > 0L, "Bucket must have tokens before the request");

    final var buf = new ExpandableArrayBuffer(VALID_FRAME_SIZE);
    final var headerEncoder = new MessageHeaderEncoder();
    final var encoder = new MarketDataSnapshotRequestEncoder();
    encoder.wrapAndApplyHeader(buf, 0, headerEncoder);
    encoder.symbol("EURUSD");
    final var content = Unpooled.wrappedBuffer(buf.byteArray(), 0, VALID_FRAME_SIZE);

    final var ctx = channel.pipeline().firstContext();
    final var outcome =
        pipeline.admit(ctx, session, content, MarketDataSnapshotRequestDecoder.BLOCK_LENGTH);
    content.release();

    assertEquals(
        MarketDataAdmissionPipeline.Outcome.PUBLISHED,
        outcome,
        "Successful publish must return PUBLISHED");

    final long tokensAfter = session.snapshotTokensAvailable();
    assertEquals(
        tokensBefore - 1L,
        tokensAfter,
        "Exactly one token must be consumed on PUBLISHED outcome — "
            + "no refund occurs regardless of whether the publisher delivers the snapshot "
            + "(timeout-no-refund contract: the stream-205 slot was consumed)");
  }
}
