package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.clock.TradingClocks;
import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.AccountTypeEnum;
import com.trading.engine.messages.sbe.AcctIDSourceEnum;
import com.trading.engine.messages.sbe.ComplianceStatusEnum;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.WebSocketErrorCode;
import com.trading.engine.messages.sbe.WebSocketErrorDecoder;
import com.trading.engine.projections.account.AccountReadModel;
import com.trading.engine.testsupport.sbe.SbeTestEncoder;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.util.ResourceLeakDetector;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.agrona.concurrent.SystemNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link WebSocketFrameDispatcher} — verifies that a {@code WebSocketSubscribe} (template
 * 62) carrying {@code MAX_SUBSCRIPTIONS_PER_CLIENT + 1} symbols causes the dispatcher to emit a
 * {@code WebSocketError(InvalidSubscription)} frame and keep the channel open (partial accept — the
 * N+1-th subscription is rejected, the session remains connected).
 *
 * <p>The dispatcher adds subscriptions one by one until the filter signals capacity exhaustion via
 * {@link SubscriptionFilter#addSubscription} returning {@code false}. On the first false it calls
 * {@link WebSocketServerConfig#maxSubscriptionsPerClient()} to confirm the limit, sends the error
 * frame, and stops processing further symbols in the batch.
 *
 * <p><b>Threading model.</b> Single-threaded — {@link EmbeddedChannel} executes all I/O on the test
 * thread.
 *
 * <p><b>Allocation.</b> Non-hot-path test; allocations are acceptable.
 */
final class WebSocketFrameDispatcherMaxSymbolsTest {

  /** Configured cap used by the dispatcher under test. Must be > 0. */
  private static final int MAX_SUBSCRIPTIONS = 3;

  private WebSocketServerConfig config;
  private WebSocketSessionManager sessionManager;
  private WebSocketMetrics metrics;
  private EmbeddedChannel channel;
  private WebSocketSession session;

  @BeforeAll
  static void setLeakDetection() {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
  }

  @BeforeEach
  void setUp() {
    config =
        WebSocketServerConfig.builder()
            .jwtAudience("wss://trading.test/ws")
            .issuerRegistry(Map.of("iss", "https://auth.test/.well-known/jwks.json"))
            .maxPendingAuth(64)
            .maxSubscriptionsPerClient(MAX_SUBSCRIPTIONS)
            .build();

    metrics = WebSocketMetrics.createWithDefaults();

    final var clock = SystemNanoClock.INSTANCE;
    sessionManager = new WebSocketSessionManager(config, metrics, clock);

    final var jtiCache = new JtiRevocationCache(1000, 15, clock);
    // Minimal stub validator — empty processor map so no JWKS fetches happen. Re-auth
    // (templateId 60) is not exercised by this test, so the validator is never invoked.
    final var jwtValidator =
        JwtValidator.forTesting(Map.of(), "wss://trading.test/ws", TradingClocks.epochNanoClock());
    final var entitlementService =
        new UserEntitlementService(
            code ->
                new AccountReadModel(
                    1L,
                    0L,
                    code,
                    AcctIDSourceEnum.Other,
                    "Test",
                    AccountTypeEnum.Client,
                    "USD",
                    AccountStatusEnum.Active,
                    ComplianceStatusEnum.OK,
                    0L,
                    true,
                    true,
                    0L,
                    0L,
                    0L,
                    List.of(),
                    List.of()));

    channel = new EmbeddedChannel();
    session = sessionManager.tryRegister(channel);
    sessionManager.setUserId(session, "user-001");
    session.jti("jti-001");
    session.entitledAccounts(Set.of("ACME-001"));
    session.initSubscriptionFilter(MAX_SUBSCRIPTIONS, metrics);

    final var commandDispatcher =
        new CommandDispatcher(
            config,
            metrics,
            clock,
            new ManyToOneConcurrentArrayQueue<>(16),
            new CommandDispatcher.EgressEntryAllocator() {
              private final CommandEntryPool pool = new CommandEntryPool(16, 256);

              @Override
              public EgressEntry tryAcquire() {
                return pool.tryAcquire();
              }

              @Override
              public void release(final EgressEntry entry) {
                pool.release(entry);
              }
            });
    final var symbolEntitlementMap =
        new SymbolEntitlementMap(Map.of("EURUSD", List.of("ACME-001")));
    final var admissionPipeline =
        new MarketDataAdmissionPipeline(
            symbolEntitlementMap, (buf, offset, length) -> 1L, metrics, clock);
    channel
        .pipeline()
        .addLast(
            new WebSocketFrameDispatcher(
                sessionManager,
                jwtValidator,
                jtiCache,
                entitlementService,
                config,
                metrics,
                clock,
                Runnable::run,
                commandDispatcher,
                admissionPipeline));
  }

  @AfterEach
  void tearDown() {
    if (channel.isOpen()) {
      channel.close();
    }
  }

  /**
   * Send a {@code WebSocketSubscribe} frame with {@code MAX_SUBSCRIPTIONS + 1} distinct symbols.
   * The dispatcher must:
   *
   * <ol>
   *   <li>Accept the first {@code MAX_SUBSCRIPTIONS} symbols into the filter.
   *   <li>Reject the {@code (MAX_SUBSCRIPTIONS + 1)}-th symbol with a {@code
   *       WebSocketError(InvalidSubscription)} frame written to the channel.
   *   <li>Keep the channel open (partial accept — client must reduce its subscription list).
   * </ol>
   *
   * <p>Symbols are generated as "SYM001" … "SYMnnn" (zero-padded to 6 chars) so they are valid
   * 8-byte ASCII when padded by the SBE encoder.
   */
  @Test
  void channelRead_subscribeNPlusOneSymbols_emitsInvalidSubscriptionError() {
    final int symbolCount = MAX_SUBSCRIPTIONS + 1;
    final var symbols = new String[symbolCount];
    final long[] eventTypes = new long[symbolCount];
    for (int i = 0; i < symbolCount; i++) {
      symbols[i] = String.format("SYM%03d", i + 1); // e.g. "SYM001" … "SYM004"
      eventTypes[i] = 0x01L; // orders bit
    }

    final var buf = new ExpandableArrayBuffer(512);
    final int len = SbeTestEncoder.encodeWebSocketSubscribe(buf, 0, symbols, eventTypes);
    final var frame = new BinaryWebSocketFrame(Unpooled.wrappedBuffer(buf.byteArray(), 0, len));

    channel.writeInbound(frame);

    // --- Assert subscription count capped at MAX_SUBSCRIPTIONS ---
    assertEquals(
        MAX_SUBSCRIPTIONS,
        session.subscriptionFilter().subscriptionCount(),
        "Subscription count must be exactly MAX_SUBSCRIPTIONS after N+1 subscribe request");

    // --- Assert channel still open (partial accept, not a fatal error) ---
    assertTrue(channel.isOpen(), "Channel must remain open after subscription-capacity rejection");

    // --- Assert WebSocketError(InvalidSubscription) was written ---
    final var outbound = channel.readOutbound();
    assertNotNull(outbound, "Dispatcher must write a WebSocketError response frame");
    assertInstanceOf(
        BinaryWebSocketFrame.class, outbound, "Response must be a BinaryWebSocketFrame");

    final var responseFrame = (BinaryWebSocketFrame) outbound;
    try {
      final var responseBuf = new UnsafeBuffer(responseFrame.content().nioBuffer());
      final var headerDecoder = new MessageHeaderDecoder();
      headerDecoder.wrap(responseBuf, 0);

      assertEquals(
          WebSocketErrorDecoder.TEMPLATE_ID,
          headerDecoder.templateId(),
          "Response frame must be a WebSocketError (templateId=67)");

      final var errorDecoder = new WebSocketErrorDecoder();
      errorDecoder.wrapAndApplyHeader(responseBuf, 0, headerDecoder);

      assertEquals(
          WebSocketErrorCode.InvalidSubscription,
          errorDecoder.errorCode(),
          "WebSocketError code must be InvalidSubscription");
    } finally {
      responseFrame.release();
    }
  }
}
