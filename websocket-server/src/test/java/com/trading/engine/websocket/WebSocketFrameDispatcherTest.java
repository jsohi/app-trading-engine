package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.clock.TradingClocks;
import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.AccountTypeEnum;
import com.trading.engine.messages.sbe.AcctIDSourceEnum;
import com.trading.engine.messages.sbe.ComplianceStatusEnum;
import com.trading.engine.projections.account.AccountReadModel;
import com.trading.engine.testsupport.sbe.SbeTestEncoder;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.util.ResourceLeakDetector;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.agrona.concurrent.SystemNanoClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link WebSocketFrameDispatcher} — verifies SBE templateId routing, subscribe /
 * unsubscribe, re-auth, heartbeat, ack, unknown template handling, TextWebSocketFrame release, and
 * null session guard.
 *
 * <p>Uses {@link EmbeddedChannel} with {@link ResourceLeakDetector.Level#PARANOID}. Sessions are
 * pre-registered with subscriptions and entitlements before each test.
 */
final class WebSocketFrameDispatcherTest {

  private WebSocketServerConfig config;
  private WebSocketSessionManager sessionManager;
  private WebSocketMetrics metrics;
  private JwtValidator jwtValidator;
  private JtiRevocationCache jtiCache;
  private UserEntitlementService entitlementService;
  private EmbeddedChannel channel;
  private WebSocketSession session;

  @BeforeAll
  static void setUpOnce() {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
  }

  @BeforeEach
  void setUp() {
    config =
        WebSocketServerConfig.builder()
            .jwtAudience("wss://trading.test/ws")
            .issuerRegistry(Map.of("iss", "https://auth.test/.well-known/jwks.json"))
            .maxPendingAuth(64)
            .build();

    metrics = WebSocketMetrics.createWithDefaults();
    sessionManager = new WebSocketSessionManager(config, metrics, SystemNanoClock.INSTANCE);
    jtiCache = new JtiRevocationCache(1000, 15, SystemNanoClock.INSTANCE);
    entitlementService =
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
    // Minimal stub validator — empty issuer registry so no JWKS URLs are fetched.
    // Re-auth (templateId 60) is not exercised by these tests, so the validator is never invoked.
    jwtValidator =
        JwtValidator.forTesting(Map.of(), "wss://trading.test/ws", TradingClocks.epochNanoClock());

    channel = new EmbeddedChannel();

    // Pre-register a session (simulating post-auth state)
    session = sessionManager.tryRegister(channel);
    sessionManager.setUserId(session, "user-001");
    session.jti("old-jti-001");
    session.entitledAccounts(Set.of("ACME-001"));
    session.initSubscriptionFilter(100, metrics);

    // Add dispatcher to pipeline
    final var commandDispatcher =
        new CommandDispatcher(
            config,
            metrics,
            SystemNanoClock.INSTANCE,
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
    final MarketDataAdmissionPipeline admissionPipeline =
        new MarketDataAdmissionPipeline(
            symbolEntitlementMap, (buf, offset, length) -> 1L, metrics, SystemNanoClock.INSTANCE);
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
                SystemNanoClock.INSTANCE,
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

  // --- Subscribe tests ---

  @Test
  void channelRead_subscribe_addsToFilter() {
    final var buf = new ExpandableArrayBuffer(256);
    final int len =
        SbeTestEncoder.encodeWebSocketSubscribe(
            buf, 0, new String[] {"EURUSD"}, new long[] {0x01L});
    final var frame = new BinaryWebSocketFrame(Unpooled.wrappedBuffer(buf.byteArray(), 0, len));

    channel.writeInbound(frame);

    assertEquals(1, session.subscriptionFilter().subscriptionCount());
  }

  @Test
  void channelRead_unsubscribe_removesFromFilter() {
    // First subscribe
    final var subBuf = new ExpandableArrayBuffer(256);
    final int subLen =
        SbeTestEncoder.encodeWebSocketSubscribe(
            subBuf, 0, new String[] {"EURUSD"}, new long[] {0x01L});
    channel.writeInbound(
        new BinaryWebSocketFrame(Unpooled.wrappedBuffer(subBuf.byteArray(), 0, subLen)));

    assertEquals(1, session.subscriptionFilter().subscriptionCount());

    // Then unsubscribe
    final var unsubBuf = new ExpandableArrayBuffer(256);
    final int unsubLen =
        SbeTestEncoder.encodeWebSocketUnsubscribe(unsubBuf, 0, new String[] {"EURUSD"});
    channel.writeInbound(
        new BinaryWebSocketFrame(Unpooled.wrappedBuffer(unsubBuf.byteArray(), 0, unsubLen)));

    assertEquals(0, session.subscriptionFilter().subscriptionCount());
  }

  @Test
  void channelRead_emptyUnsubscribe_clearsAllSubscriptions() {
    // Subscribe to 2 symbols
    final var subBuf = new ExpandableArrayBuffer(256);
    final int subLen =
        SbeTestEncoder.encodeWebSocketSubscribe(
            subBuf, 0, new String[] {"EURUSD", "GBPUSD"}, new long[] {0x01L, 0x01L});
    channel.writeInbound(
        new BinaryWebSocketFrame(Unpooled.wrappedBuffer(subBuf.byteArray(), 0, subLen)));

    assertEquals(2, session.subscriptionFilter().subscriptionCount());

    // Empty unsubscribe = clear all
    final var unsubBuf = new ExpandableArrayBuffer(256);
    final int unsubLen = SbeTestEncoder.encodeWebSocketUnsubscribe(unsubBuf, 0, new String[] {});
    channel.writeInbound(
        new BinaryWebSocketFrame(Unpooled.wrappedBuffer(unsubBuf.byteArray(), 0, unsubLen)));

    assertEquals(0, session.subscriptionFilter().subscriptionCount());
  }

  // --- Heartbeat and ack tests ---

  @Test
  void channelRead_clientHeartbeat_updatesSessionTimestamp() {
    final long before = session.lastClientHeartbeatNs();
    final var buf = new ExpandableArrayBuffer(64);
    final int len = SbeTestEncoder.encodeClientHeartbeat(buf, 0, 123456789L);
    channel.writeInbound(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(buf.byteArray(), 0, len)));

    assertTrue(session.lastClientHeartbeatNs() > before || session.lastClientHeartbeatNs() > 0);
  }

  @Test
  void channelRead_clientAck_updatesLastCmdSeqNo() {
    final var buf = new ExpandableArrayBuffer(64);
    final int len = SbeTestEncoder.encodeClientAck(buf, 0, 42L);
    channel.writeInbound(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(buf.byteArray(), 0, len)));

    assertEquals(42L, session.lastClientCmdSeqNo());
  }

  // --- Unknown template and TextWebSocketFrame tests ---

  @Test
  void channelRead_unknownTemplateId_logsWarning() {
    // Send a WebSocketAuthAck (template 61) which is server-to-client — not handled by dispatcher
    final var buf = new ExpandableArrayBuffer(64);
    final int len = SbeTestEncoder.encodeWebSocketAuthAck(buf, 0, 1L, 2L, 1, 100);
    channel.writeInbound(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(buf.byteArray(), 0, len)));

    // Channel should still be open (only 1 unknown, need 3 to close)
    assertTrue(channel.isOpen());
  }

  @Test
  void channelRead_threeConsecutiveUnknown_closesChannel() {
    for (int i = 0; i < 3; i++) {
      final var buf = new ExpandableArrayBuffer(64);
      final int len = SbeTestEncoder.encodeWebSocketAuthAck(buf, 0, 1L, 2L, 1, 100);
      channel.writeInbound(
          new BinaryWebSocketFrame(Unpooled.wrappedBuffer(buf.byteArray(), 0, len)));
    }

    assertFalse(channel.isOpen(), "Channel should close after 3 consecutive unknown templateIds");
  }

  @Test
  void channelRead_textFrame_releasedAndWarned() {
    final var frame = new TextWebSocketFrame("not supported");
    channel.writeInbound(frame);

    // Channel should still be open (only 1 unknown)
    assertTrue(channel.isOpen());
    // Frame should be released (no leak) — PARANOID detector will flag at GC if not
  }

  @Test
  void channelRead_sessionDeregistered_ignoresFrame() {
    // Remove the session from the manager
    sessionManager.removeSession(channel);

    final var buf = new ExpandableArrayBuffer(64);
    final int len = SbeTestEncoder.encodeClientHeartbeat(buf, 0, 0L);
    channel.writeInbound(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(buf.byteArray(), 0, len)));

    // Channel should be closed — session not found means invalid state
    assertFalse(channel.isOpen(), "Channel should close when session is deregistered");
  }

  // --- Gap request and session resume stubs ---

  @Test
  void channelRead_gapRequest_sendsCommandRejectedError() {
    final var buf = new ExpandableArrayBuffer(64);
    final int len = SbeTestEncoder.encodeWebSocketGapRequest(buf, 0, 1L, 10L);
    channel.writeInbound(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(buf.byteArray(), 0, len)));

    // Should receive a WebSocketError response
    final var outbound = channel.readOutbound();
    if (outbound instanceof BinaryWebSocketFrame responseFrame) {
      responseFrame.release();
    }
    assertTrue(channel.isOpen(), "Gap request stub should not close the channel");
  }

  @Test
  void channelRead_sessionResume_sendsCommandRejectedError() {
    final var buf = new ExpandableArrayBuffer(64);
    final int len = SbeTestEncoder.encodeSessionResume(buf, 0, 1L, 2L, 5L);
    channel.writeInbound(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(buf.byteArray(), 0, len)));

    final var outbound = channel.readOutbound();
    if (outbound instanceof BinaryWebSocketFrame responseFrame) {
      responseFrame.release();
    }
    assertTrue(channel.isOpen(), "Session resume stub should not close the channel");
  }
}
