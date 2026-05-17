package com.trading.engine.websocket;

import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.WebSocketAuthAckEncoder;
import com.trading.engine.messages.sbe.WebSocketAuthDecoder;
import com.trading.engine.messages.sbe.WebSocketErrorCode;
import com.trading.engine.messages.sbe.WebSocketErrorEncoder;
import com.trading.engine.projections.account.AccountReadModel;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.util.ReferenceCountUtil;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
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

  /** Singleton command dispatcher passed to the per-channel FrameDispatcher on auth success. */
  private final CommandDispatcher commandDispatcher;

  /** This channel's WriteByteCounterHandler, captured for wiring into the session post-auth. */
  private final WriteByteCounterHandler byteCounter;

  /** Phase 3 — symbol → permitted-accounts map loaded at launcher boot. Required. */
  private final SymbolEntitlementMap symbolEntitlementMap;

  /**
   * Phase 3 — SAM seam over the stream-205 snapshot-request Aeron publication used to construct the
   * per-channel {@link MarketDataAdmissionPipeline}. Required.
   */
  private final SnapshotRequestPublisher snapshotRequestPublisher;

  /**
   * Phase 3 Commit B — account-code → {@link AccountReadModel} lookup. Used in {@link #sendAuthAck}
   * to materialize per-account {@code symbolPreferences} and {@code panelLayout} into the {@code
   * WebSocketAuthAck} (template 61) groups. Required — the launcher YAML pipeline is the only
   * source of truth; a missing primary account is a server-side bug, not a runtime branch.
   */
  private final Function<String, AccountReadModel> accountLookup;

  // --- Per-channel state ---
  private volatile boolean authResolved;
  private ScheduledFuture<?> authTimeoutFuture;
  private WebSocketSession registeredSession;

  // --- Pre-allocated response encoding buffers (per-channel, not shared) ---
  private final ExpandableArrayBuffer responseBuf = new ExpandableArrayBuffer(128);
  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final WebSocketAuthDecoder authDecoder = new WebSocketAuthDecoder();

  /**
   * Canonical Phase 3 per-channel auth handler. All collaborators required — there is no legacy
   * overload. The post-auth path wires the per-channel {@link MarketDataAdmissionPipeline} on the
   * dispatcher AND calls {@link WebSocketSession#initSnapshotTokenBucket(long)} + {@link
   * WebSocketSession#publishSymbolEntitlements(SymbolEntitlementMap, java.util.Set)} on the
   * session.
   *
   * @param pendingAuthCount shared counter
   * @param jwtValidator JWT validator
   * @param jtiCache JTI cache
   * @param entitlementService entitlement validator
   * @param authFailureTracker auth failure tracker
   * @param sessionManager session manager
   * @param metrics metrics
   * @param config config
   * @param nanoClock clock
   * @param validationExecutor JWT validation executor
   * @param commandDispatcher singleton command dispatcher
   * @param byteCounter per-channel byte counter installed earlier in the pipeline
   * @param symbolEntitlementMap launcher-loaded symbol → permitted-accounts map
   * @param snapshotRequestPublisher SAM seam over the stream-205 Aeron publication
   * @param accountLookup account-code → {@link AccountReadModel} lookup for per-account {@code
   *     symbolPreferences} + {@code panelLayout} in the AuthAck (template 61)
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
      final Executor validationExecutor,
      final CommandDispatcher commandDispatcher,
      final WriteByteCounterHandler byteCounter,
      final SymbolEntitlementMap symbolEntitlementMap,
      final SnapshotRequestPublisher snapshotRequestPublisher,
      final Function<String, AccountReadModel> accountLookup) {
    this.pendingAuthCount = Objects.requireNonNull(pendingAuthCount, "pendingAuthCount");
    this.jwtValidator = Objects.requireNonNull(jwtValidator, "jwtValidator");
    this.jtiCache = Objects.requireNonNull(jtiCache, "jtiCache");
    this.entitlementService = Objects.requireNonNull(entitlementService, "entitlementService");
    this.authFailureTracker = Objects.requireNonNull(authFailureTracker, "authFailureTracker");
    this.sessionManager = Objects.requireNonNull(sessionManager, "sessionManager");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    this.config = Objects.requireNonNull(config, "config");
    this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
    this.validationExecutor = Objects.requireNonNull(validationExecutor, "validationExecutor");
    this.commandDispatcher = Objects.requireNonNull(commandDispatcher, "commandDispatcher");
    this.byteCounter = Objects.requireNonNull(byteCounter, "byteCounter");
    this.symbolEntitlementMap =
        Objects.requireNonNull(symbolEntitlementMap, "symbolEntitlementMap");
    this.snapshotRequestPublisher =
        Objects.requireNonNull(snapshotRequestPublisher, "snapshotRequestPublisher");
    this.accountLookup = Objects.requireNonNull(accountLookup, "accountLookup");
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) throws Exception {
    final int pending = pendingAuthCount.incrementAndGet();
    if (pending > config.maxPendingAuth()) {
      // Set authResolved before close to prevent channelInactive from double-decrementing.
      authResolved = true;
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

    assert !(content instanceof CompositeByteBuf)
        : "Composite ByteBuf not supported — nioBuffer() would copy";
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
    // originalAuthJti is set ONCE on first auth — preserved across re-auth so SessionResume can
    // verify the original login is still valid.
    registeredSession.originalAuthJti(claims.jti());
    registeredSession.entitledAccounts(validatedAccounts);
    // Wire the subscription filter WITH the metrics sink so the Phase 3 Commit A entitlement-
    // Phase 3: initialise the per-session subscription filter (with metrics), reliable-stream
    // tracker, snapshot-request token bucket, and publish the per-account entitled-symbols set.
    // The entitlement publish is load-bearing for SubscriptionFilter.matches() — without it the
    // empty default fails closed and every market-data fan-out drops for this session.
    registeredSession.initSubscriptionFilter(config.maxSubscriptionsPerClient(), metrics);
    registeredSession.initReliableStreamTracker(
        config.replayBufferFrames(), config.replayBufferFrameSize(), metrics);
    registeredSession.initSnapshotTokenBucket(nanoClock.nanoTime());
    registeredSession.publishSymbolEntitlements(symbolEntitlementMap, validatedAccounts);
    registeredSession.pendingBytesRef(byteCounter.pendingBytesRef());

    // 14. Cancel auth timeout
    cancelTimeout();

    // 15. Record auth latency
    final long authDurationNs = nanoClock.nanoTime() - authStartNs;
    metrics.authLatency().record(authDurationNs, TimeUnit.NANOSECONDS);

    // 16. Send WebSocketAuthAck.
    // The "primary" account whose UI preferences ship in AuthAck is the first JWT-claim account
    // that is also entitled — preserves the issuer's intended ordering so a UI cohort change
    // (e.g. trader switches desks) is reflected by re-ordering claim accounts at the IdP, no
    // server-side state needed. validatedAccounts is guaranteed non-empty (early-return above)
    // and UserEntitlementService.validateAccounts only returns codes with a non-null
    // AccountReadModel
    // lookup, so the lookup below is fail-fast non-null.
    String primaryAccountCode = null;
    for (final var code : claims.accounts()) {
      if (validatedAccounts.contains(code)) {
        primaryAccountCode = code;
        break;
      }
    }
    final var primaryAccount = accountLookup.apply(primaryAccountCode);
    final var sessionId = registeredSession.sessionId();
    sendAuthAck(
        ctx,
        sessionId.getMostSignificantBits(),
        sessionId.getLeastSignificantBits(),
        primaryAccount);

    // 17. Add WebSocketFrameDispatcher to pipeline after this handler. The per-channel
    // MarketDataAdmissionPipeline is constructed inline and passed to the dispatcher's
    // canonical ctor — required, fail-closed.
    final var frameDispatcher =
        new WebSocketFrameDispatcher(
            sessionManager,
            jwtValidator,
            jtiCache,
            entitlementService,
            config,
            metrics,
            nanoClock,
            validationExecutor,
            commandDispatcher,
            new MarketDataAdmissionPipeline(
                symbolEntitlementMap, snapshotRequestPublisher, metrics, nanoClock));
    ctx.pipeline().addAfter(ctx.name(), "frame-dispatcher", frameDispatcher);

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
    // Release on any exception path BEFORE the write happens; the
    // successful path transfers ownership to writeAndFlush(). Avoids
    // a `boolean written` mutable local in favour of catch+rethrow
    // per CLAUDE.md "all locals must be `final`".
    try {
      nettyBuf.writeBytes(responseBuf.byteArray(), 0, encodedLen);
      ctx.writeAndFlush(new BinaryWebSocketFrame(nettyBuf)).addListener(f -> ctx.close());
    } catch (final Throwable t) {
      nettyBuf.release();
      throw t;
    }
    resolveAuth();
  }

  private void sendAuthAck(
      final ChannelHandlerContext ctx,
      final long sessionIdMsb,
      final long sessionIdLsb,
      final AccountReadModel primaryAccount) {
    final var enc = new WebSocketAuthAckEncoder();
    final var header = new MessageHeaderEncoder();
    enc.wrapAndApplyHeader(responseBuf, 0, header);
    enc.sessionId().mostSignificantBits(sessionIdMsb).leastSignificantBits(sessionIdLsb);
    enc.protocolVersion(EXPECTED_PROTOCOL_VERSION);
    enc.maxSubscriptions(config.maxSubscriptionsPerClient());
    // APP-36 §A1: server-asserted heartbeat cadence published in AuthAck.
    // serverHeartbeatIntervalMs (id=4) — outbound WebSocketHeartbeat cadence.
    // clientHeartbeatIntervalMs (id=5) — negotiated cadence == clientTimeoutMs/2,
    // so server hard-disconnect threshold remains 2× the published client cadence
    // per APP-36 §2.8. WebSocketServerConfig.validate() rejects values that would
    // narrow into a negative int (uint32 wire range); Math.toIntExact provides
    // a defensive fail-fast even if validation is bypassed.
    enc.serverHeartbeatIntervalMs(Math.toIntExact(config.heartbeatIntervalMs()));
    enc.clientHeartbeatIntervalMs(Math.toIntExact(config.negotiatedClientHeartbeatIntervalMs()));

    // Phase 3 Commit B: per-account UI preferences. Empty groups when no slots configured —
    // the worker falls back to DEFAULT_SUBSCRIBE_SYMBOLS / the default OrderEntryForm slot.
    // YAML AccountRecord guarantees both lists are non-null (immutable List.copyOf at load),
    // so unconditional iteration is safe; SBE group encoder accepts count=0.
    final var prefs = primaryAccount.symbolPreferences();
    final var prefsEnc = enc.symbolPreferencesCount(prefs.size());
    for (final var symbol : prefs) {
      prefsEnc.next().symbol(symbol);
    }
    final var panels = primaryAccount.panelLayout();
    final var panelEnc = enc.panelLayoutCount(panels.size());
    for (final var slot : panels) {
      panelEnc.next().panelId(slot.panelId()).slot(slot.slot());
    }

    final int encodedLen = MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
    // The client's worker pipeline is FrameParser-first: every inbound binary
    // frame after upgrade goes through the 13-byte best-effort envelope. AuthAck
    // is no exception — the next handler installed downstream (WebSocketFrameDispatcher)
    // also reads framed bytes. Wrap the SBE payload in a best-effort envelope
    // (totalLength u32 | seqNo i64 = 0 | flags u8 = 0) so the client's existing
    // FrameParser → MessageRouter → AuthClient pipeline sees template 61.
    final int frameLen = BEST_EFFORT_HEADER_LENGTH + encodedLen;
    final var nettyBuf = ctx.alloc().buffer(frameLen);
    // Release on any exception path BEFORE the write happens; successful
    // writeAndFlush transfers buffer ownership. catch+rethrow avoids a
    // mutable `boolean written` local per CLAUDE.md.
    try {
      nettyBuf.writeIntLE(frameLen); // totalLength = envelope + payload
      nettyBuf.writeLongLE(0L); // seqNo = 0 on best-effort
      nettyBuf.writeByte(0); // flags = 0 (best-effort, no CRC, no replay/snapshot)
      nettyBuf.writeBytes(responseBuf.byteArray(), 0, encodedLen);
      ctx.writeAndFlush(new BinaryWebSocketFrame(nettyBuf));
    } catch (final Throwable t) {
      nettyBuf.release();
      throw t;
    }
  }

  /** Best-effort envelope: totalLength u32 LE | seqNo i64 LE | flags u8. */
  private static final int BEST_EFFORT_HEADER_LENGTH = 13;

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
