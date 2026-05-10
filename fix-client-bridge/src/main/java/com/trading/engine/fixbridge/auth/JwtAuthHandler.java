package com.trading.engine.fixbridge.auth;

import com.trading.engine.fixbridge.FixClientBridgeConfig;
import com.trading.engine.fixbridge.audit.AuditAction;
import com.trading.engine.fixbridge.audit.AuditLogger;
import com.trading.engine.fixbridge.json.BrowserEvent;
import com.trading.engine.fixbridge.json.BrowserMessageReader;
import com.trading.engine.fixbridge.json.JsonParseException;
import com.trading.engine.fixbridge.json.MutableParsedMessage;
import com.trading.engine.fixbridge.quote.SessionId;
import com.trading.engine.fixbridge.ratelimit.PerTypeRateLimiter;
import com.trading.engine.fixbridge.transport.BridgeCloseCodes;
import com.trading.engine.fixbridge.transport.BridgeFrameDispatcher;
import com.trading.engine.fixbridge.transport.BridgeSession;
import com.trading.engine.fixbridge.transport.InboundReadGate;
import com.trading.engine.fixbridge.transport.OutboundQueue;
import com.trading.engine.fixbridge.transport.WsListener;
import com.trading.engine.websocket.AuthFailureTracker;
import com.trading.engine.websocket.JwtValidator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.util.ReferenceCountUtil;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.NanoClock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * One-shot per-channel JWT authentication handler for the FIX client bridge (§3.3).
 *
 * <p>Reads exactly one inbound text frame after the WebSocket upgrade completes. The frame must be
 * a JSON {@code {"type":"Auth","token":"…"}}; anything else (binary, malformed, missing token,
 * oversize) is rejected with close code {@link BridgeCloseCodes#POLICY_VIOLATION}. The {@code
 * token} is verified via {@link JwtValidator#validate(String)} on a worker pool to keep the Netty
 * event loop unblocked during JWKS cache misses; on success the handler:
 *
 * <ol>
 *   <li>Checks {@link com.trading.engine.fixbridge.auth.JtiRevocationCache JtiRevocationCache} for
 *       the {@code jti} (revoked → close {@link BridgeCloseCodes#AUTH_EXPIRED}).
 *   <li>Mints a {@link SessionId} (UUID-derived).
 *   <li>Captures the remote IP for pinning when the JWT carries {@code ip_pinned=true} (fail-secure
 *       default per §20.4c).
 *   <li>Allocates {@link OutboundQueue}, {@link PerTypeRateLimiter}, and {@link InboundReadGate}
 *       sized from {@link FixClientBridgeConfig}.
 *   <li>Stashes a {@link BridgeSession} on the channel under {@link BridgeSession#ATTRIBUTE_KEY}.
 *   <li>Hot-swaps the pipeline: removes itself, adds the {@link InboundReadGate} and {@link
 *       WsListener} so subsequent frames are handled by the post-auth path.
 * </ol>
 *
 * <p>Auth-failure side-effects:
 *
 * <ul>
 *   <li>{@link AuthFailureTracker} is incremented on every rejection except auth timeout.
 *   <li>If {@link AuthFailureTracker#isBlocked(String)} returns {@code true} <i>before</i> JWT
 *       validation, no validation occurs (cheap-out under DDoS).
 *   <li>The handler emits a single {@link BrowserEvent.Error} taxonomy reason on the wire so the
 *       browser can distinguish auth-expired (4001) vs policy-violation (4008) by close-code alone
 *       — no information about <i>why</i> the JWT failed leaks (auth-oracle prevention).
 * </ul>
 *
 * <p><b>Threading.</b> Per-channel instance, NOT {@code @Sharable}. JWT validation is offloaded to
 * {@code validationExecutor}; the completion callback re-enters on the channel event loop via
 * {@code ctx.executor()}.
 *
 * <p><b>Allocation.</b> The flyweight {@link MutableParsedMessage} is per-instance and reused once.
 * UUID minting allocates one {@link SessionId} on success — cold path. Token byte→String decode
 * allocates one {@link String} per auth attempt; this is unavoidable with Nimbus.
 *
 * @see JwtValidator
 * @see JtiRevocationCache
 * @see AuthFailureTracker
 */
public final class JwtAuthHandler extends SimpleChannelInboundHandler<TextWebSocketFrame> {

  private static final Logger LOG = LogManager.getLogger(JwtAuthHandler.class);

  private final FixClientBridgeConfig config;
  private final JwtValidator jwtValidator;
  private final JtiRevocationCache jtiCache;
  private final AuthFailureTracker authFailureTracker;
  private final EpochNanoClock epochNanoClock;
  private final NanoClock nanoClock;
  private final Executor validationExecutor;
  private final BridgeFrameDispatcher dispatcher;
  private final AuditLogger auditLogger;

  /**
   * Per-channel parser flyweight reused if the first frame is malformed (we still close the channel
   * after one rejection — there's no retry — but the field is held for symmetry with {@link
   * WsListener} and to keep the parser's allocation contract zero on the cold path).
   */
  private final MutableParsedMessage parsed = new MutableParsedMessage();

  /** Set to {@code true} once authentication has terminated (success or failure). */
  private volatile boolean authResolved;

  /** Auth deadline timer (cancelled on success / failure). */
  private ScheduledFuture<?> authTimeoutFuture;

  /**
   * Construct a per-channel auth handler.
   *
   * @param config bridge configuration (for auth timeout, JTI cache wiring is via the cache
   *     parameter, queue capacities, audit role, etc.)
   * @param jwtValidator shared JWT RS256 validator with JWKS caching (reused from {@code
   *     :websocket-server})
   * @param jtiCache per-process revocation cache (must be created on the Netty boss-loop thread and
   *     shared with the bridge's revocation endpoint)
   * @param authFailureTracker per-IP auth-failure tarpit; shared across channels
   * @param epochNanoClock injected wall-clock used for JTI revocation TTL checks (epoch
   *     nanoseconds; see {@code TradingClocks.epochNanoClock()})
   * @param nanoClock monotonic clock used for per-type rate limiter and audit timestamps
   * @param validationExecutor executor for the async JWT validation; use {@code
   *     ForkJoinPool.commonPool()} in production, {@code Runnable::run} in tests for deterministic
   *     synchronous behaviour without {@link Thread#sleep}
   * @param dispatcher post-auth frame dispatcher SAM (use {@link BridgeFrameDispatcher#NOOP} for
   *     APP-40a integration tests until the real translator + Artio wiring lands)
   * @param auditLogger audit sink for AUTH_SUCCESS / AUTH_FAIL / AUTH_TIMEOUT events
   */
  public JwtAuthHandler(
      final FixClientBridgeConfig config,
      final JwtValidator jwtValidator,
      final JtiRevocationCache jtiCache,
      final AuthFailureTracker authFailureTracker,
      final EpochNanoClock epochNanoClock,
      final NanoClock nanoClock,
      final Executor validationExecutor,
      final BridgeFrameDispatcher dispatcher,
      final AuditLogger auditLogger) {
    this.config = Objects.requireNonNull(config, "config");
    this.jwtValidator = Objects.requireNonNull(jwtValidator, "jwtValidator");
    this.jtiCache = Objects.requireNonNull(jtiCache, "jtiCache");
    this.authFailureTracker = Objects.requireNonNull(authFailureTracker, "authFailureTracker");
    this.epochNanoClock = Objects.requireNonNull(epochNanoClock, "epochNanoClock");
    this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
    this.validationExecutor = Objects.requireNonNull(validationExecutor, "validationExecutor");
    this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
    this.auditLogger = Objects.requireNonNull(auditLogger, "auditLogger");
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) throws Exception {
    // Schedule the auth deadline. If the first frame doesn't arrive within
    // config.authTimeoutSeconds() the handler emits a cold Error+Close pair.
    final var timeoutSeconds = config.authTimeoutSeconds();
    authTimeoutFuture =
        ctx.executor()
            .schedule(
                () -> {
                  if (!authResolved) {
                    LOG.warn("Auth timeout ({}s) for {}", timeoutSeconds, remoteIp(ctx));
                    if (auditLogger.isWritable()) {
                      auditLogger.record(
                          nanoClock.nanoTime(),
                          null,
                          null,
                          remoteIp(ctx),
                          AuditAction.AUTH_TIMEOUT,
                          null,
                          null,
                          null,
                          null,
                          null,
                          null,
                          null,
                          null,
                          null,
                          null,
                          "auth-timeout",
                          null,
                          null);
                    }
                    sendErrorAndClose(ctx, "auth-timeout", BridgeCloseCodes.POLICY_VIOLATION);
                  }
                },
                timeoutSeconds,
                TimeUnit.SECONDS);
    ctx.fireChannelActive();
  }

  @Override
  protected void channelRead0(final ChannelHandlerContext ctx, final TextWebSocketFrame frame) {
    if (authResolved) {
      // Defensive — Netty's auto-release covers the frame because we extend
      // SimpleChannelInboundHandler. Drop silently; channel will close imminently.
      return;
    }

    final var remoteIp = remoteIp(ctx);

    // 1. Per-IP tarpit check first — cheaper than parsing or JWT validation under DDoS.
    if (authFailureTracker.isBlocked(remoteIp)) {
      LOG.warn("Auth rejected: IP {} is in tarpit", remoteIp);
      sendErrorAndClose(ctx, "auth-failed", BridgeCloseCodes.POLICY_VIOLATION);
      return;
    }

    // 2. Parse the inbound JSON. Anything other than a well-formed Auth message → reject.
    final int messageType;
    try {
      messageType = BrowserMessageReader.parse(frame.content(), parsed);
    } catch (final JsonParseException e) {
      LOG.warn("Auth rejected: parse error '{}' from {}", e.reason(), remoteIp);
      authFailureTracker.recordFailure(remoteIp);
      auditAuthFail(remoteIp, "parse:" + e.reason());
      sendErrorAndClose(ctx, "auth-failed", BridgeCloseCodes.POLICY_VIOLATION);
      return;
    }

    if (messageType != MutableParsedMessage.TYPE_AUTH) {
      LOG.warn("Auth rejected: first frame type {} (expected AUTH) from {}", messageType, remoteIp);
      authFailureTracker.recordFailure(remoteIp);
      auditAuthFail(remoteIp, "first-frame-not-auth");
      sendErrorAndClose(ctx, "auth-failed", BridgeCloseCodes.POLICY_VIOLATION);
      return;
    }

    if (parsed.tokenOff < 0 || parsed.tokenLen <= 0) {
      LOG.warn("Auth rejected: empty/missing token from {}", remoteIp);
      authFailureTracker.recordFailure(remoteIp);
      auditAuthFail(remoteIp, "empty-token");
      sendErrorAndClose(ctx, "auth-failed", BridgeCloseCodes.POLICY_VIOLATION);
      return;
    }

    // 3. Decode the token slice into a String. Nimbus needs a String; one allocation per auth is
    // acceptable (cold path).
    final var tokenString =
        new String(parsed.scratch, parsed.tokenOff, parsed.tokenLen, StandardCharsets.UTF_8);

    // 4. Offload signature + claims verification — JWKS cache misses can block on HTTPS GETs.
    CompletableFuture.supplyAsync(() -> jwtValidator.validate(tokenString), validationExecutor)
        .whenCompleteAsync(
            (claims, ex) -> {
              if (authResolved) {
                return; // timeout fired while validation was in flight
              }
              if (ex != null) {
                final var cause = ex.getCause() != null ? ex.getCause() : ex;
                LOG.warn(
                    "Auth rejected: JWT validation failed for {}: {}",
                    remoteIp,
                    cause.getMessage());
                authFailureTracker.recordFailure(remoteIp);
                auditAuthFail(remoteIp, "jwt-invalid");
                sendErrorAndClose(ctx, "auth-failed", BridgeCloseCodes.POLICY_VIOLATION);
                return;
              }
              completeAuthOnEventLoop(ctx, remoteIp, claims);
            },
            ctx.executor());
  }

  /**
   * Continue auth on the channel event loop after async validation succeeds. Steps 5–11 from the
   * class-level Javadoc.
   */
  private void completeAuthOnEventLoop(
      final ChannelHandlerContext ctx,
      final String remoteIp,
      final JwtValidator.ValidatedClaims claims) {

    // 5. JTI revocation check.
    final long nowEpochNs = epochNanoClock.nanoTime();
    if (jtiCache.isRevoked(claims.jti(), nowEpochNs)) {
      LOG.warn("Auth rejected: JTI {} is revoked (sub={})", claims.jti(), claims.sub());
      authFailureTracker.recordFailure(remoteIp);
      auditAuthFail(remoteIp, "jti-revoked");
      // 4001 — the token was valid but has been revoked; the browser interprets this as
      // "credentials retired" and re-prompts for sign-in rather than treating as a hard error.
      sendErrorAndClose(ctx, "auth-expired", BridgeCloseCodes.AUTH_EXPIRED);
      return;
    }

    // 6. Mint session id. UUID is acceptable here per CLAUDE.md §WebSocket Server (Non-Det).
    final var sessionId = new SessionId(UUID.randomUUID().toString());

    // 7. Capture pinned remote address (always recorded; only enforced when claims.ipPinned()).
    final InetAddress pinnedAddress;
    final var addr = ctx.channel().remoteAddress();
    if (addr instanceof InetSocketAddress inet) {
      pinnedAddress = inet.getAddress();
    } else {
      // Embedded test channels expose synthetic addresses. Pinning is moot in those — record a
      // wildcard-loopback so the BridgeSession ctor's non-null contract holds without enabling
      // a spoofable comparison. Real transports always supply InetSocketAddress.
      pinnedAddress = InetAddress.getLoopbackAddress();
    }

    // 8. Build per-session collaborators sized from config.
    final var outboundQueue = new OutboundQueue(config.outboundQueueCapacityPerSession());
    final var perTypeLimiter = new PerTypeRateLimiter(nanoClock.nanoTime());
    final var session =
        new BridgeSession(sessionId, claims, pinnedAddress, outboundQueue, perTypeLimiter);

    // 9. Stash the session on the channel — every downstream handler reads it from here.
    ctx.channel().attr(BridgeSession.ATTRIBUTE_KEY).set(session);

    // 10. Hot-swap the pipeline: install the read gate + listener after this handler, then
    // remove this handler. Order of operations:
    //   - addAfter readGate (so it sees inbound frames first; on Netty all inbound handlers fire
    //     in pipeline order, but the read-gate adapter only intercepts via its post-dispatch
    //     callback so install order between gate & listener is moot for inbound traffic).
    //   - addAfter listener (after the gate; listener dispatches & invokes gate.onAfterDispatch)
    //   - remove self
    final var readGate = new InboundReadGate(outboundQueue);
    final var listener = new WsListener(dispatcher, readGate, nanoClock, auditLogger);
    final var pipeline = ctx.pipeline();
    pipeline.addAfter(ctx.name(), "ws-read-gate", readGate);
    pipeline.addAfter("ws-read-gate", "ws-listener", listener);
    pipeline.remove(this);

    // 11. Cancel the timeout, mark resolved, audit, log.
    cancelTimeout();
    authResolved = true;
    if (auditLogger.isWritable()) {
      auditLogger.record(
          nanoClock.nanoTime(),
          claims.sub(),
          claims.jti(),
          remoteIp,
          AuditAction.AUTH_SUCCESS,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          "ok",
          null,
          null);
    }
    LOG.info(
        "Auth success: sub={} sessionId={} ip={} ipPinned={} roles={}",
        claims.sub(),
        sessionId,
        remoteIp,
        claims.ipPinned(),
        claims.roles().size());
  }

  @Override
  public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
    cancelTimeout();
    if (!authResolved) {
      authResolved = true;
    }
    ctx.fireChannelInactive();
  }

  @Override
  public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
    LOG.error("Unexpected exception in JwtAuthHandler for {}", remoteIp(ctx), cause);
    if (!authResolved) {
      sendErrorAndClose(ctx, "auth-failed", BridgeCloseCodes.POLICY_VIOLATION);
    } else {
      ctx.close();
    }
  }

  // --- Helpers ---------------------------------------------------------------------------------

  private void sendErrorAndClose(
      final ChannelHandlerContext ctx, final String reason, final int closeCode) {
    if (!ctx.channel().isActive()) {
      cancelTimeout();
      authResolved = true;
      return;
    }
    final var error = new BrowserEvent.Error(reason);
    // Cold path: we don't have a per-channel BrowserEventWriter wired in yet (Day 5 work). For
    // now emit a hand-rolled minimal JSON object — taxonomy strings are pure ASCII and bounded
    // by the closed enum, so naive concatenation is safe. NOTE: replaced with the real writer in
    // APP-40a Day 5 once the writer's BrowserEvent.Error overload is exercised.
    final var json = "{\"type\":\"Error\",\"reason\":\"" + error.reason() + "\"}";
    final var text = new TextWebSocketFrame(json);
    ctx.writeAndFlush(text)
        .addListener(
            future -> {
              final var close = new CloseWebSocketFrame(closeCode, reason);
              ctx.writeAndFlush(close).addListener(closeFuture -> ctx.close());
            });
    cancelTimeout();
    authResolved = true;
  }

  private void cancelTimeout() {
    if (authTimeoutFuture != null && !authTimeoutFuture.isDone()) {
      authTimeoutFuture.cancel(false);
    }
  }

  private void auditAuthFail(final String remoteIp, final String failureReason) {
    if (!auditLogger.isWritable()) {
      return;
    }
    auditLogger.record(
        nanoClock.nanoTime(),
        null,
        null,
        remoteIp,
        AuditAction.AUTH_FAIL,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        "fail",
        failureReason,
        null);
  }

  private static String remoteIp(final ChannelHandlerContext ctx) {
    final var addr = ctx.channel().remoteAddress();
    if (addr instanceof InetSocketAddress inet) {
      return inet.getAddress().getHostAddress();
    }
    return addr != null ? addr.toString() : "unknown";
  }

  /**
   * Defensive cleanup: any inbound frame that reaches {@link #channelRead0} after the handler has
   * resolved authentication should already have been forwarded to {@link WsListener}; if Netty
   * hands us one in flight, release it via the parent's standard machinery.
   *
   * @param msg the inbound message
   */
  static void releaseUnused(final Object msg) {
    ReferenceCountUtil.release(msg);
  }
}
