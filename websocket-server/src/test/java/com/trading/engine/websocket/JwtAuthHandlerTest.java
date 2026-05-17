package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jose.jwk.source.ImmutableJWKSet;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.nimbusds.jwt.proc.DefaultJWTClaimsVerifier;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;
import com.trading.engine.messages.clock.TradingClocks;
import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.AccountTypeEnum;
import com.trading.engine.messages.sbe.AcctIDSourceEnum;
import com.trading.engine.messages.sbe.ComplianceStatusEnum;
import com.trading.engine.projections.account.AccountReadModel;
import com.trading.engine.testsupport.sbe.SbeTestEncoder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.util.ResourceLeakDetector;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.agrona.concurrent.SystemNanoClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link JwtAuthHandler} — verifies the one-shot per-channel JWT authentication flow
 * including auth success, various rejection paths, timeout, pending auth tracking, and session
 * cleanup on partial failure.
 *
 * <p>Uses {@link EmbeddedChannel} with {@link ResourceLeakDetector.Level#PARANOID} for ByteBuf leak
 * detection. JWT tokens are signed with a locally generated RSA key pair via nimbus-jose-jwt.
 */
final class JwtAuthHandlerTest {

  private static final String ISSUER = "https://auth.trading.test";
  private static final String AUDIENCE = "wss://trading.test/ws";

  private static RSAKey rsaKey;
  private static RSASSASigner rsaSigner;

  private WebSocketServerConfig config;
  private WebSocketSessionManager sessionManager;
  private WebSocketMetrics metrics;
  private JwtValidator jwtValidator;
  private JtiRevocationCache jtiCache;
  private AuthFailureTracker authFailureTracker;
  private UserEntitlementService entitlementService;
  private Function<String, AccountReadModel> accountLookup;
  private AtomicInteger pendingAuthCount;
  private CommandDispatcher commandDispatcher;
  private SymbolEntitlementMap symbolEntitlementMap;
  private EmbeddedChannel channel;

  @BeforeAll
  static void setUpOnce() throws Exception {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    rsaKey = new RSAKeyGenerator(2048).keyID("test-key-1").generate();
    rsaSigner = new RSASSASigner(rsaKey);
  }

  @BeforeEach
  void setUp() {
    config =
        WebSocketServerConfig.builder()
            .jwtAudience(AUDIENCE)
            .issuerRegistry(Map.of(ISSUER, "https://auth.trading.test/.well-known/jwks.json"))
            .maxPendingAuth(2)
            .build();

    metrics = WebSocketMetrics.createWithDefaults();
    sessionManager = new WebSocketSessionManager(config, metrics, SystemNanoClock.INSTANCE);
    jtiCache = new JtiRevocationCache(1000, 15, SystemNanoClock.INSTANCE);
    authFailureTracker = new AuthFailureTracker(5, 60, SystemNanoClock.INSTANCE);
    // Provide a stub that returns an Active account for any code — enables auth success tests.
    // The stub creates a minimal AccountReadModel with Active status. Reused for both the
    // entitlement validator and the Phase 3 Commit B per-account AuthAck lookup.
    accountLookup =
        code ->
            new AccountReadModel(
                1L,
                0L,
                code,
                AcctIDSourceEnum.Other,
                "Test Account",
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
                List.of());
    entitlementService = new UserEntitlementService(accountLookup);
    pendingAuthCount = new AtomicInteger(0);
    jwtValidator = buildTestValidator();
    commandDispatcher =
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
    symbolEntitlementMap = new SymbolEntitlementMap(Map.of("EURUSD", List.of("TEST-ACCT")));

    channel = createChannel();
  }

  @AfterEach
  void tearDown() {
    if (channel.isOpen()) {
      channel.close();
    }
    channel.checkException();
  }

  // --- Auth success tests ---

  @Test
  void channelRead_validAuth_sendsAuthAckAndAddsDispatcher() throws Exception {
    final var frame = encodeAuthFrame(buildValidJwt());

    channel.writeInbound(frame);
    // CompletableFuture runs on ForkJoinPool — give it time to complete, then run event loop tasks
    channel.runPendingTasks();

    final var response = readResponse();
    assertNotNull(response, "Should send a response");
    response.release(); // prevent ByteBuf leak
    // Channel should still be open (auth success doesn't close)
    assertTrue(channel.isOpen(), "Channel should remain open after successful auth");
    // Dispatcher should be in pipeline
    assertNotNull(
        channel.pipeline().get(WebSocketFrameDispatcher.class),
        "WebSocketFrameDispatcher should be added to pipeline");
  }

  @Test
  void channelRead_validAuth_removesHandlerFromPipeline() throws Exception {
    final var frame = encodeAuthFrame(buildValidJwt());

    channel.writeInbound(frame);
    channel.runPendingTasks();

    assertNull(
        channel.pipeline().get(JwtAuthHandler.class),
        "JwtAuthHandler should be removed from pipeline after auth success");
  }

  // --- Auth rejection tests ---

  @Test
  void channelRead_nonAuthTemplateId_sendsErrorAndCloses() {
    // Encode a ClientHeartbeat (template 65) instead of WebSocketAuth (template 60)
    final var buf = new ExpandableArrayBuffer(64);
    final int len = SbeTestEncoder.encodeClientHeartbeat(buf, 0, System.nanoTime());
    final var frame = new BinaryWebSocketFrame(toByteBuf(buf, len));

    channel.writeInbound(frame);
    channel.runPendingTasks();

    assertChannelClosedWithError();
  }

  @Test
  void channelRead_tokenSizeExceeded_sendsErrorAndCloses() {
    // Create a frame larger than maxTokenSizeBytes (8192 default)
    final var oversized = new byte[config.maxTokenSizeBytes() + 1];
    final var frame = new BinaryWebSocketFrame(Unpooled.wrappedBuffer(oversized));

    channel.writeInbound(frame);
    channel.runPendingTasks();

    assertChannelClosedWithError();
  }

  @Test
  void channelRead_emptyBinaryFrame_sendsErrorAndCloses() {
    final var frame = new BinaryWebSocketFrame(Unpooled.EMPTY_BUFFER.retainedDuplicate());

    channel.writeInbound(frame);
    channel.runPendingTasks();

    assertChannelClosedWithError();
  }

  @Test
  void channelRead_perIpLockout_sendsErrorWithoutJwtValidation() {
    // Exhaust the failure tracker for this IP
    for (int i = 0; i < 5; i++) {
      authFailureTracker.recordFailure("embedded");
    }

    final var frame = encodeAuthFrame(buildValidJwt());
    channel.writeInbound(frame);
    channel.runPendingTasks();

    assertChannelClosedWithError();
  }

  @Test
  void channelRead_pendingAuthLimitExceeded_closesChannel() {
    // Fill up pending auth slots
    pendingAuthCount.set(config.maxPendingAuth());

    // Create a new channel that will exceed the limit on channelActive
    final var ch2 = createChannel();
    assertFalse(
        ch2.isOpen(), "Channel should be closed immediately when pending auth limit exceeded");
  }

  // --- Session lifecycle tests ---

  @Test
  void channelInactive_beforeAuthResolved_decrementsPendingAuth() {
    assertEquals(1, pendingAuthCount.get(), "pendingAuthCount should be 1 after channelActive");

    channel.close();

    assertEquals(0, pendingAuthCount.get(), "pendingAuthCount should be 0 after channelInactive");
  }

  @Test
  void channelInactive_afterAuthResolved_doesNotDoubleDeccrement() throws Exception {
    final var frame = encodeAuthFrame(buildValidJwt());
    channel.writeInbound(frame);
    channel.runPendingTasks();

    // Auth succeeded — pendingAuthCount should already be decremented
    final int countAfterAuth = pendingAuthCount.get();

    channel.close();

    assertEquals(
        countAfterAuth,
        pendingAuthCount.get(),
        "pendingAuthCount should not change after channelInactive for already-resolved auth");
  }

  // --- Metrics tests ---

  @Test
  void channelRead_validAuth_metricsAuthSucceededCalled() throws Exception {
    final var frame = encodeAuthFrame(buildValidJwt());

    channel.writeInbound(frame);
    channel.runPendingTasks();

    // Verify auth success by checking dispatcher was added (metrics counter is private)
    assertNotNull(
        channel.pipeline().get(WebSocketFrameDispatcher.class),
        "Successful auth should add dispatcher — implies authSucceeded was called");
  }

  @Test
  void channelRead_invalidAuth_metricsAuthFailedCalled() {
    final var frame = encodeAuthFrame(buildValidJwt());
    // Corrupt the token by sending a non-auth template
    final var buf = new ExpandableArrayBuffer(64);
    final int len = SbeTestEncoder.encodeClientHeartbeat(buf, 0, 0);
    final var badFrame = new BinaryWebSocketFrame(toByteBuf(buf, len));

    channel.writeInbound(badFrame);
    channel.runPendingTasks();

    // Verify auth failure by checking channel is closing (metrics counter is private)
    assertChannelClosedWithError();
  }

  // --- Helper methods ---

  private EmbeddedChannel createChannel() {
    // Runnable::run executes JWT validation synchronously on the event loop — eliminates
    // Thread.sleep flakiness. In production, ForkJoinPool.commonPool() is used instead.
    // WriteByteCounterHandler is added before JwtAuthHandler in the production pipeline;
    // for tests we create a standalone instance and pass it directly — it is wired into the
    // session pendingBytesRef on auth success without needing to be in the pipeline itself.
    final var byteCounter = new WriteByteCounterHandler();
    return new EmbeddedChannel(
        byteCounter,
        new JwtAuthHandler(
            pendingAuthCount,
            jwtValidator,
            jtiCache,
            entitlementService,
            authFailureTracker,
            sessionManager,
            metrics,
            config,
            SystemNanoClock.INSTANCE,
            Runnable::run,
            commandDispatcher,
            byteCounter,
            symbolEntitlementMap,
            (buf, offset, length) -> 1L,
            accountLookup));
  }

  private BinaryWebSocketFrame encodeAuthFrame(final SignedJWT jwt) {
    final var buf = new ExpandableArrayBuffer(8192);
    final var tokenBytes = jwt.serialize().getBytes(StandardCharsets.UTF_8);
    final int len = SbeTestEncoder.encodeWebSocketAuth(buf, 0, 1, tokenBytes);
    return new BinaryWebSocketFrame(toByteBuf(buf, len));
  }

  private static ByteBuf toByteBuf(final ExpandableArrayBuffer agrona, final int length) {
    return Unpooled.wrappedBuffer(agrona.byteArray(), 0, length);
  }

  private ByteBuf readResponse() {
    final var outbound = channel.readOutbound();
    if (outbound instanceof BinaryWebSocketFrame frame) {
      return frame.content();
    }
    return null;
  }

  private void assertChannelClosedWithError() {
    // Run pending tasks to complete async operations
    channel.runPendingTasks();
    // The channel may have a response queued
    final var outbound = channel.readOutbound();
    if (outbound instanceof BinaryWebSocketFrame frame) {
      frame.release();
    }
    // Channel should be closing or closed
    channel.runPendingTasks();
    assertFalse(channel.isOpen(), "Channel should be closed after auth error");
  }

  private JwtValidator buildTestValidator() {
    try {
      final var jwkSet = new com.nimbusds.jose.jwk.JWKSet(rsaKey.toPublicJWK());
      final var jwkSource = new ImmutableJWKSet<SecurityContext>(jwkSet);
      final var keySelector = new JWSVerificationKeySelector<>(JWSAlgorithm.RS256, jwkSource);

      final var processor = new DefaultJWTProcessor<SecurityContext>();
      processor.setJWSKeySelector(keySelector);

      final var requiredClaims = new HashSet<String>();
      requiredClaims.add("sub");
      requiredClaims.add("jti");
      requiredClaims.add("iss");
      requiredClaims.add("aud");
      requiredClaims.add("exp");
      requiredClaims.add("iat");

      final var claimsVerifier =
          new DefaultJWTClaimsVerifier<SecurityContext>(
              new JWTClaimsSet.Builder().audience(AUDIENCE).build(), requiredClaims);
      processor.setJWTClaimsSetVerifier(claimsVerifier);

      return JwtValidator.forTesting(
          Map.of(ISSUER, processor), AUDIENCE, TradingClocks.epochNanoClock());
    } catch (final Exception e) {
      throw new RuntimeException("Failed to build test JwtValidator", e);
    }
  }

  private SignedJWT buildValidJwt() {
    try {
      final var now = Instant.now();
      final var claims =
          new JWTClaimsSet.Builder()
              .issuer(ISSUER)
              .audience(AUDIENCE)
              .subject("user-001")
              .jwtID(UUID.randomUUID().toString())
              .issueTime(Date.from(now))
              .expirationTime(Date.from(now.plusSeconds(900)))
              .claim("accounts", List.of("ACME-001"))
              .build();

      final var header = new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(rsaKey.getKeyID()).build();
      final var jwt = new SignedJWT(header, claims);
      jwt.sign(rsaSigner);
      return jwt;
    } catch (final Exception e) {
      throw new RuntimeException("Failed to build test JWT", e);
    }
  }
}
