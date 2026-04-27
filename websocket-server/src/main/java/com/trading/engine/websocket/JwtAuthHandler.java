package com.trading.engine.websocket;

import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.WebSocketAuthAckEncoder;
import com.trading.engine.messages.sbe.WebSocketAuthDecoder;
import com.trading.engine.messages.sbe.WebSocketErrorCode;
import com.trading.engine.messages.sbe.WebSocketErrorEncoder;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.util.ReferenceCountUtil;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * One-shot per-channel JWT authentication handler. Gates WebSocket access by validating the first
 * {@code WebSocketAuth} (template 60) binary frame, then removes itself from the pipeline and
 * dynamically adds {@link WebSocketFrameDispatcher} for post-auth frame routing.
 *
 * <p><b>Pipeline position:</b> After {@code origin-validator}, before any application handler. All
 * frames received before successful auth are rejected.
 *
 * <p><b>Auth flow:</b>
 *
 * <ol>
 *   <li>Client sends {@code WebSocketAuth} frame with protocol version + JWT token
 *   <li>Handler validates: size limit, templateId, protocol version, IP lockout, JWT signature +
 *       claims, JTI revocation, account entitlements, session registration
 *   <li>On success: sends {@code WebSocketAuthAck}, adds {@code WebSocketFrameDispatcher}, removes
 *       self
 *   <li>On failure: sends {@code WebSocketError(AuthenticationFailed)}, closes channel
 * </ol>
 *
 * <p><b>Security.</b> Single error code ({@code AuthenticationFailed}) for all rejection reasons —
 * prevents auth oracle attacks. Detailed reason codes in server logs only. JWT validation is
 * offloaded to {@code ForkJoinPool.commonPool()} via {@link CompletableFuture} to avoid blocking
 * the Netty event loop during JWKS cache misses.
 *
 * <p><b>Threading.</b> Per-channel instance, NOT {@code @Sharable}. All pipeline mutations happen
 * on the channel's event loop thread via {@code ctx.executor()}. The shared {@code
 * pendingAuthCount} uses {@link AtomicInteger} for cross-event-loop safety.
 *
 * <p><b>Allocation.</b> {@link ExpandableArrayBuffer} pre-sized to 128 bytes for SBE response
 * encoding. SBE decoders/encoders are per-channel fields (re-wrapped per message, not shared).
 *
 * @see WebSocketFrameDispatcher
 * @see JwtValidator
 * @see <a href="docs/websocket-architecture.md">WebSocket Architecture — Section 3/4</a>
 */
public final class JwtAuthHandler extends ChannelInboundHandlerAdapter {

  private static final Logger LOG = LogManager.getLogger(JwtAuthHandler.class);

  /** Expected protocol version — clients must match or receive VersionMismatch error. */
  private static final int EXPECTED_PROTOCOL_VERSION = 1;

  /** Auth timeout in seconds — matches architecture doc Section 3/4. */
  private static final int AUTH_TIMEOUT_SECONDS = 5;

  // --- Shared across all channels (thread-safe) ---
  private final AtomicInteger pendingAuthCount;
  private final JwtValidator jwtValidator;
  private final JtiRevocationCache jtiCache;
  private final UserEntitlementService entitlementService;
  private final AuthFailureTracker authFailureTracker;
  private final WebSocketSessionManager sessionManager;
  private final WebSocketMetrics metrics;
  private final WebSocketServerConfig config;
  private final NanoClock nanoClock;
  private final Executor validationExecutor;

  // --- Per-channel state ---
  private volatile boolean authResolved;
  private ScheduledFuture<?> authTimeoutFuture;
  private WebSocketSession registeredSession;

  // --- Pre-allocated response encoding buffers (per-channel, not shared) ---
  private final ExpandableArrayBuffer responseBuf = new ExpandableArrayBuffer(128);
  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final WebSocketAuthDecoder authDecoder = new WebSocketAuthDecoder();

  /**
   * Create a per-channel auth handler.
   *
   * @param pendingAuthCount shared counter across all channels for unauthenticated connection
   *     tracking
   * @param jwtValidator JWT RS256 validator with JWKS caching
   * @param jtiCache JTI revocation cache for replay prevention
   * @param entitlementService account entitlement validator
   * @param authFailureTracker per-IP auth failure rate limiter
   * @param sessionManager session registry with capacity limits
   * @param metrics metrics instance for auth success/failure/lockout counters
   * @param config server configuration (maxTokenSizeBytes, maxPendingAuth, etc.)
   * @param nanoClock monotonic clock for auth latency measurement and dispatcher heartbeats
   * @param validationExecutor executor for async JWT validation; use {@code
   *     ForkJoinPool.commonPool()} in production, {@code Runnable::run} in tests for deterministic
   *     behavior without Thread.sleep
   */
  public JwtAuthHandler(
      final AtomicInteger pendingAuthCount,
      final JwtValidator jwtValidator,
      final JtiRevocationCache jtiCache,
      final UserEntitlementService entitlementService,
      final AuthFailureTracker authFailureTracker,
      final WebSocketSessionManager sessionManager,
      final WebSocketMetrics metrics,
      final WebSocketServerConfig config,
      final NanoClock nanoClock,
      final Executor validationExecutor) {
    this.pendingAuthCount = pendingAuthCount;
    this.jwtValidator = jwtValidator;
    this.jtiCache = jtiCache;
    this.entitlementService = entitlementService;
    this.authFailureTracker = authFailureTracker;
    this.sessionManager = sessionManager;
    this.metrics = metrics;
    this.config = config;
    this.nanoClock = nanoClock;
    this.validationExecutor = validationExecutor;
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) throws Exception {
    final int pending = pendingAuthCount.incrementAndGet();
    if (pending > config.maxPendingAuth()) {
      pendingAuthCount.decrementAndGet();
      LOG.warn("Pending auth limit exceeded ({}/{}), closing", pending, config.maxPendingAuth());
      ctx.close();
      return;
    }

    // Schedule auth timeout — if client doesn't authenticate within 5s, close.
    authTimeoutFuture =
        ctx.executor()
            .schedule(
                () -> {
                  if (!authResolved) {
                    LOG.warn("Auth timeout ({}s) for {}", AUTH_TIMEOUT_SECONDS, remoteIp(ctx));
                    sendErrorAndClose(ctx, WebSocketErrorCode.AuthenticationFailed);
                  }
                },
                AUTH_TIMEOUT_SECONDS,
                TimeUnit.SECONDS);

    ctx.fireChannelActive();
  }

  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
    // Defense-in-depth: ignore frames after auth resolved (e.g., post-timeout race)
    if (authResolved) {
      ReferenceCountUtil.release(msg);
      return;
    }

    if (!(msg instanceof BinaryWebSocketFrame frame)) {
      ReferenceCountUtil.release(msg);
      rejectAuth(ctx, remoteIp(ctx), "non-binary frame received before auth");
      return;
    }

    try {
      handleAuthFrame(ctx, frame);
    } finally {
      frame.release();
    }
  }

  private void handleAuthFrame(final ChannelHandlerContext ctx, final BinaryWebSocketFrame frame) {
    final var content = frame.content();
    final int readableBytes = content.readableBytes();
    final var remoteIp = remoteIp(ctx);

    // 1. Token size check — reject oversized frames before any parsing
    if (readableBytes > config.maxTokenSizeBytes()) {
      rejectAuth(ctx, remoteIp, "frame size exceeds maxTokenSizeBytes");
      return;
    }

    // 2. Decode SBE header — reject if not WebSocketAuth (template 60)
    if (readableBytes < MessageHeaderDecoder.ENCODED_LENGTH) {
      rejectAuth(ctx, remoteIp, "frame too small for SBE header");
      return;
    }

    final var buf = new UnsafeBuffer(content.nioBuffer());
    headerDecoder.wrap(buf, 0);

    final int templateId = headerDecoder.templateId();
    if (templateId != WebSocketAuthDecoder.TEMPLATE_ID) {
      rejectAuth(ctx, remoteIp, "expected templateId=60, got=" + templateId);
      return;
    }

    // 3. Decode WebSocketAuth
    authDecoder.wrap(
        buf,
        MessageHeaderDecoder.ENCODED_LENGTH,
        headerDecoder.blockLength(),
        headerDecoder.version());

    final int protocolVersion = authDecoder.protocolVersion();

    // 4. Protocol version check
    if (protocolVersion != EXPECTED_PROTOCOL_VERSION) {
      LOG.warn(
          "Protocol version mismatch: expected={}, got={}, ip={}",
          EXPECTED_PROTOCOL_VERSION,
          protocolVersion,
          remoteIp);
      sendErrorAndClose(ctx, WebSocketErrorCode.VersionMismatch);
      return;
    }

    // 5. Per-IP lockout check — before JWT validation to avoid CPU cost
    if (authFailureTracker.isBlocked(remoteIp)) {
      metrics.authLockout();
      rejectAuth(ctx, remoteIp, "IP locked out");
      return;
    }

    // 6. Extract token bytes → String
    final int tokenLen = authDecoder.tokenLength();
    if (tokenLen <= 0 || tokenLen > config.maxTokenSizeBytes()) {
      rejectAuth(ctx, remoteIp, "invalid token length=" + tokenLen);
      return;
    }
    final var tokenBytes = new byte[tokenLen];
    authDecoder.getToken(tokenBytes, 0, tokenLen);
    final var tokenString = new String(tokenBytes, StandardCharsets.UTF_8);

    // 7. Auth latency timer start
    final long authStartNs = nanoClock.nanoTime();

    // 8. Offload JWT validation to avoid blocking Netty event loop during JWKS cache miss.
    // ForkJoinPool.commonPool() is acceptable: auth is cold path (max 64 concurrent via
    // pendingAuthCount). Completion callback runs on the channel's event loop thread.
    CompletableFuture.supplyAsync(() -> jwtValidator.validate(tokenString), validationExecutor)
        .whenCompleteAsync(
            (claims, ex) -> {
              if (authResolved) {
                return; // timeout fired while validation was in flight
              }
              if (ex != null) {
                final var cause = ex.getCause() != null ? ex.getCause() : ex;
                rejectAuth(ctx, remoteIp, "JWT validation failed: " + cause.getMessage());
                return;
              }
              continueAuthOnEventLoop(ctx, claims, remoteIp, authStartNs);
            },
            ctx.executor());
  }

  /**
   * Continues the auth flow on the event loop after async JWT validation completes. Steps 9-20 from
   * the plan.
   */
  private void continueAuthOnEventLoop(
      final ChannelHandlerContext ctx,
      final JwtValidator.ValidatedClaims claims,
      final String remoteIp,
      final long authStartNs) {

    // 9. JTI revocation check
    if (jtiCache.isRevoked(claims.jti())) {
      rejectAuth(ctx, remoteIp, "revoked JTI");
      return;
    }

    // 10. Account entitlement validation
    final var validatedAccounts = entitlementService.validateAccounts(claims.accounts());
    if (validatedAccounts.isEmpty()) {
      rejectAuth(ctx, remoteIp, "no valid accounts");
      return;
    }

    // 11. Session registration — reject if global capacity exceeded
    final var channel = ctx.channel();
    registeredSession = sessionManager.tryRegister(channel);
    if (registeredSession == null) {
      rejectAuth(ctx, remoteIp, "global session capacity exceeded");
      return;
    }

    // 12. Set userId — reject if per-user limit exceeded; deregister on failure
    if (!sessionManager.setUserId(registeredSession, claims.sub())) {
      sessionManager.removeSession(channel);
      registeredSession = null;
      rejectAuth(ctx, remoteIp, "per-user session limit exceeded");
      return;
    }

    // 13. Store session state
    registeredSession.jti(claims.jti());
    registeredSession.entitledAccounts(validatedAccounts);
    registeredSession.initSubscriptionFilter(config.maxSubscriptionsPerClient());

    // 14. Cancel auth timeout
    cancelTimeout();

    // 15. Record auth latency
    final long authDurationNs = nanoClock.nanoTime() - authStartNs;
    metrics.authLatency().record(authDurationNs, TimeUnit.NANOSECONDS);

    // 16. Send WebSocketAuthAck
    final var sessionId = registeredSession.sessionId();
    sendAuthAck(ctx, sessionId.getMostSignificantBits(), sessionId.getLeastSignificantBits());

    // 17. Add WebSocketFrameDispatcher to pipeline after this handler
    ctx.pipeline()
        .addAfter(
            ctx.name(),
            "frame-dispatcher",
            new WebSocketFrameDispatcher(
                sessionManager,
                jwtValidator,
                jtiCache,
                entitlementService,
                config,
                metrics,
                nanoClock));

    // 18. Remove self from pipeline
    ctx.pipeline().remove(this);

    // 19. Finalize
    authResolved = true;
    registeredSession = null; // prevent stale reference in channelInactive
    pendingAuthCount.decrementAndGet();

    // 20. Metrics
    metrics.authSucceeded();

    LOG.info(
        "Auth success: userId={}, sessionId={}, accounts={}",
        claims.sub(),
        sessionId,
        validatedAccounts.size());
  }

  @Override
  public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
    cancelTimeout();
    if (!authResolved) {
      // Set authResolved to prevent the async CompletableFuture callback from proceeding
      // into continueAuthOnEventLoop on a dead channel — would cause double-decrement of
      // pendingAuthCount and attempt to register a session against a closed channel.
      authResolved = true;
      pendingAuthCount.decrementAndGet();
    }
    if (registeredSession != null) {
      sessionManager.removeSession(ctx.channel());
      registeredSession = null;
    }
    ctx.fireChannelInactive();
  }

  @Override
  public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
    LOG.error("Unexpected exception in auth handler for {}", remoteIp(ctx), cause);
    if (!authResolved) {
      sendErrorAndClose(ctx, WebSocketErrorCode.AuthenticationFailed);
    }
  }

  // --- Private helpers ---

  private void rejectAuth(
      final ChannelHandlerContext ctx, final String remoteIp, final String reason) {
    LOG.warn("Auth rejected: reason={}, ip={}", reason, remoteIp);
    authFailureTracker.recordFailure(remoteIp);
    metrics.authFailed();
    sendErrorAndClose(ctx, WebSocketErrorCode.AuthenticationFailed);
  }

  private void sendErrorAndClose(
      final ChannelHandlerContext ctx, final WebSocketErrorCode errorCode) {
    if (!ctx.channel().isActive()) {
      resolveAuth();
      return;
    }

    final var errorText = ErrorTextRegistry.textFor(errorCode);
    final var enc = new WebSocketErrorEncoder();
    final var header = new MessageHeaderEncoder();
    enc.wrapAndApplyHeader(responseBuf, 0, header);
    enc.errorCode(errorCode);
    enc.putErrorText(errorText, 0, errorText.length);

    final int encodedLen = MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
    final var nettyBuf = ctx.alloc().buffer(encodedLen);
    boolean written = false;
    try {
      nettyBuf.writeBytes(responseBuf.byteArray(), 0, encodedLen);
      ctx.writeAndFlush(new BinaryWebSocketFrame(nettyBuf)).addListener(f -> ctx.close());
      written = true;
    } finally {
      if (!written) {
        nettyBuf.release();
      }
    }
    resolveAuth();
  }

  private void sendAuthAck(
      final ChannelHandlerContext ctx, final long sessionIdMsb, final long sessionIdLsb) {
    final var enc = new WebSocketAuthAckEncoder();
    final var header = new MessageHeaderEncoder();
    enc.wrapAndApplyHeader(responseBuf, 0, header);
    enc.sessionId().mostSignificantBits(sessionIdMsb).leastSignificantBits(sessionIdLsb);
    enc.protocolVersion(EXPECTED_PROTOCOL_VERSION);
    enc.maxSubscriptions(config.maxSubscriptionsPerClient());

    final int encodedLen = MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
    final var nettyBuf = ctx.alloc().buffer(encodedLen);
    boolean written = false;
    try {
      nettyBuf.writeBytes(responseBuf.byteArray(), 0, encodedLen);
      ctx.writeAndFlush(new BinaryWebSocketFrame(nettyBuf));
      written = true;
    } finally {
      if (!written) {
        nettyBuf.release();
      }
    }
  }

  private void resolveAuth() {
    authResolved = true;
    pendingAuthCount.decrementAndGet();
    cancelTimeout();
  }

  private void cancelTimeout() {
    if (authTimeoutFuture != null && !authTimeoutFuture.isDone()) {
      authTimeoutFuture.cancel(false);
    }
  }

  private static String remoteIp(final ChannelHandlerContext ctx) {
    final var addr = ctx.channel().remoteAddress();
    if (addr instanceof InetSocketAddress inet) {
      return inet.getAddress().getHostAddress();
    }
    return addr != null ? addr.toString() : "unknown";
  }
}
