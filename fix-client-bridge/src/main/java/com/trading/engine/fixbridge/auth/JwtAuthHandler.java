package com.trading.engine.fixbridge.auth;

import com.trading.engine.fixbridge.FixClientBridgeConfig;
import com.trading.engine.fixbridge.audit.AuditAction;
import com.trading.engine.fixbridge.audit.AuditLogger;
import com.trading.engine.fixbridge.json.BrowserEvent;
import com.trading.engine.fixbridge.json.BrowserEventWriter;
import com.trading.engine.fixbridge.json.BrowserMessageReader;
import com.trading.engine.fixbridge.json.JsonParseException;
import com.trading.engine.fixbridge.json.MutableParsedMessage;
import com.trading.engine.fixbridge.json.OrderRejectReason;
import com.trading.engine.fixbridge.quote.SessionId;
import com.trading.engine.fixbridge.ratelimit.PerTypeRateLimiter;
import com.trading.engine.fixbridge.transport.AccountLimitsSource;
import com.trading.engine.fixbridge.transport.BridgeCloseCodes;
import com.trading.engine.fixbridge.transport.BridgeFrameDispatcher;
import com.trading.engine.fixbridge.transport.BridgeSession;
import com.trading.engine.fixbridge.transport.InboundReadGate;
import com.trading.engine.fixbridge.transport.OutboundDrainer;
import com.trading.engine.fixbridge.transport.OutboundQueue;
import com.trading.engine.fixbridge.transport.WsListener;
import com.trading.engine.websocket.AuthFailureTracker;
import com.trading.engine.websocket.JwtValidator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;
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
 * <p><b>JWT credential lifetime in heap.</b> The decoded JWT {@link String} resides on the heap
 * until garbage collection — the standard JWT-validator pattern. Implementations that demand
 * scrubbed credentials would need a {@code char[]} carrier and explicit zeroing after {@link
 * JwtValidator#validate} returns; Nimbus's API does not expose that path, so the bridge accepts the
 * residual-heap window as a known limitation. Audit and log paths NEVER print the token string (see
 * {@link #channelRead0} — only its length and the validation failure reason are logged), so the
 * heap residue is the only exposure.
 *
 * @see JwtValidator
 * @see JtiRevocationCache
 * @see AuthFailureTracker
 */
public final class JwtAuthHandler extends SimpleChannelInboundHandler<TextWebSocketFrame> {

  private static final Logger LOG = LogManager.getLogger(JwtAuthHandler.class);

  /**
   * Channel attribute carrying the {@code DPoP} HTTP header value captured during the WebSocket
   * upgrade handshake. The launcher-side handshake handler MUST set this attribute on the channel
   * BEFORE the handshake completes; this handler reads it during {@link #completeAuthOnEventLoop}
   * and passes it to the {@link DpopValidator}. May be {@code null} (or unset) when the worker did
   * not send a DPoP proof — DpopValidator decides what to do based on whether the bearer JWT
   * carries a {@code cnf.jkt} claim.
   */
  public static final AttributeKey<String> DPOP_HEADER_ATTR =
      AttributeKey.valueOf("com.trading.engine.fixbridge.auth.JwtAuthHandler.DPOP_HEADER");

  private final FixClientBridgeConfig config;
  private final JwtValidator jwtValidator;
  private final JtiRevocationCache jtiCache;
  private final AuthFailureTracker authFailureTracker;
  private final EpochNanoClock epochNanoClock;
  private final NanoClock nanoClock;
  private final Executor validationExecutor;
  private final BridgeFrameDispatcher.Factory dispatcherFactory;
  private final AuditLogger auditLogger;
  private final DpopValidator dpopValidator;

  /**
   * Per-channel writer used for cold-path Auth-failure {@code Error} frames. Created lazily on
   * first use because failure responses are vanishingly rare on a healthy connection — keeps the
   * per-channel allocation footprint at zero for the common (success) path.
   */
  private final BrowserEventWriter eventWriter;

  /**
   * Pull source for {@link BrowserEvent.AccountLimits} push frames emitted on AUTH_SUCCESS (§3.14).
   * Defaults to {@link AccountLimitsSource#NOOP} when the launcher has not yet wired the cluster
   * client (Day 5+ soft-dependency).
   */
  private final AccountLimitsSource accountLimitsSource;

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
   * @param dispatcherFactory per-session dispatcher factory invoked once per authenticated channel
   *     (Gemini critical finding on PR #70 R3 — RoutingBridgeFrameDispatcher is per- session, NOT
   *     thread-safe, and sharing one instance across channels would corrupt per- session state).
   *     Use {@link BridgeFrameDispatcher.Factory#NOOP_FACTORY} for tests until the launcher wires
   *     the real per-session factory.
   * @param auditLogger audit sink for AUTH_SUCCESS / AUTH_FAIL / AUTH_TIMEOUT events
   * @param eventWriter outbound JSON writer used for cold-path Auth-failure {@code Error} frames
   *     (replaces the hand-rolled JSON Day 4-c emitted)
   * @param accountLimitsSource source of {@link BrowserEvent.AccountLimits} push frames emitted on
   *     AUTH_SUCCESS — use {@link AccountLimitsSource#NOOP} until the launcher's cluster client is
   *     wired (APP-40b)
   * @param dpopValidator DPoP runtime validator (§3.3 / §B-r2-7) — invoked after JWT validation
   *     passes and before JTI revocation check; runs against the {@code DPoP} HTTP header the
   *     launcher captured into {@link #DPOP_HEADER_ATTR}. Use {@link DpopValidator#NOOP} for
   *     deployments that don't enforce DPoP (the default).
   */
  public JwtAuthHandler(
      final FixClientBridgeConfig config,
      final JwtValidator jwtValidator,
      final JtiRevocationCache jtiCache,
      final AuthFailureTracker authFailureTracker,
      final EpochNanoClock epochNanoClock,
      final NanoClock nanoClock,
      final Executor validationExecutor,
      final BridgeFrameDispatcher.Factory dispatcherFactory,
      final AuditLogger auditLogger,
      final BrowserEventWriter eventWriter,
      final AccountLimitsSource accountLimitsSource,
      final DpopValidator dpopValidator) {
    this.config = Objects.requireNonNull(config, "config");
    this.jwtValidator = Objects.requireNonNull(jwtValidator, "jwtValidator");
    this.jtiCache = Objects.requireNonNull(jtiCache, "jtiCache");
    this.authFailureTracker = Objects.requireNonNull(authFailureTracker, "authFailureTracker");
    this.epochNanoClock = Objects.requireNonNull(epochNanoClock, "epochNanoClock");
    this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
    this.validationExecutor = Objects.requireNonNull(validationExecutor, "validationExecutor");
    this.dispatcherFactory = Objects.requireNonNull(dispatcherFactory, "dispatcherFactory");
    this.auditLogger = Objects.requireNonNull(auditLogger, "auditLogger");
    this.eventWriter = Objects.requireNonNull(eventWriter, "eventWriter");
    this.accountLimitsSource = Objects.requireNonNull(accountLimitsSource, "accountLimitsSource");
    this.dpopValidator = Objects.requireNonNull(dpopValidator, "dpopValidator");
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
                          epochNanoClock.nanoTime(),
                          null,
                          null,
                          remoteIp(ctx),
                          AuditAction.AUTH_TIMEOUT,
                          null,
                          null,
                          0L,
                          0L,
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

    // 4b. Race-guard against channelInactive firing while the async JWT validation was in flight.
    // The whenCompleteAsync callback at the call site already checks `authResolved` before entering
    // here, but channelInactive could fire BETWEEN that check and this method's pipeline mutation,
    // leaving ctx.name() unwired and causing pipeline.addAfter(ctx.name(), ...) to throw
    // NoSuchElementException. Re-check both flags inside the event loop and bail silently if the
    // channel has already gone inactive.
    if (authResolved || !ctx.channel().isActive()) {
      return;
    }

    // 4c. DPoP runtime check (§3.3 / §B-r2-7). Validate the DPoP proof captured by the
    // launcher's handshake handler against the bearer JWT's cnf.jkt claim. The validator's NOOP
    // default returns VALID for every call (deployments without DPoP enforcement); production
    // deployments that require DPoP bind a real cryptographic impl that returns STALE_DPOP on
    // worker key rotation or INVALID on proof failure / replay.
    final var dpopHeader = ctx.channel().attr(DPOP_HEADER_ATTR).get();
    final var dpopResult = dpopValidator.validate(claims, dpopHeader);
    if (dpopResult == DpopValidator.Result.STALE_DPOP) {
      LOG.warn("Auth rejected: STALE_DPOP for sub={} (worker key rotation)", claims.sub());
      authFailureTracker.recordFailure(remoteIp);
      auditAuthFail(remoteIp, "stale-dpop");
      // 4001 — taxonomy says STALE_DPOP is TRANSIENT; the worker silently re-mints token+DPoP
      // pair instead of prompting the user (per §B-r2-7).
      sendErrorAndClose(
          ctx, OrderRejectReason.STALE_DPOP.wireValue(), BridgeCloseCodes.AUTH_EXPIRED);
      return;
    }
    if (dpopResult == DpopValidator.Result.INVALID) {
      LOG.warn("Auth rejected: DPoP proof INVALID for sub={}", claims.sub());
      authFailureTracker.recordFailure(remoteIp);
      auditAuthFail(remoteIp, "dpop-invalid");
      // 4008 — single auth-failed error code (no oracle leak per §3.3).
      sendErrorAndClose(ctx, "auth-failed", BridgeCloseCodes.POLICY_VIOLATION);
      return;
    }

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
    // Wrap the pipeline mutation in a try/catch so a channelInactive that races with the success
    // path (closing the channel between the isActive check at the top and the addAfter call here)
    // doesn't surface a NoSuchElementException through exceptionCaught and into a recursive
    // sendErrorAndClose on a dead channel. This is the residual-window guard for race M5.
    // Per-session dispatcher: built by the launcher-supplied factory. RoutingBridgeFrameDispatcher
    // is per-session (NOT thread-safe) — sharing across channels would corrupt per-session state
    // and break ClOrdID uniqueness (Gemini critical finding on PR #70 R3). The remoteIpSupplier
    // resolves to the captured handshake-time IP — for IP-pinned sessions this is stable; for
    // non-pinned the dispatcher's audit row will reflect the handshake IP (per-frame re-resolution
    // is a future enhancement when the launcher's IP-pin enforcer exposes a live reference).
    final var dispatcherRemoteIp = remoteIp; // capture for Supplier (effectively final)
    final BridgeFrameDispatcher dispatcher =
        dispatcherFactory.create(session, () -> dispatcherRemoteIp);

    final var readGate = new InboundReadGate(outboundQueue);
    final var listener =
        new WsListener(dispatcher, readGate, epochNanoClock, nanoClock, auditLogger);
    final var pipeline = ctx.pipeline();
    try {
      pipeline.addAfter(ctx.name(), "ws-read-gate", readGate);
      pipeline.addAfter("ws-read-gate", "ws-listener", listener);
      pipeline.remove(this);
    } catch (final NoSuchElementException ex) {
      // Pipeline was torn down between the isActive check and here — the channel is gone.
      // Mark resolved and exit; channelInactive's bookkeeping covers the rest.
      LOG.debug("Pipeline torn down during auth completion (channel closed mid-handshake)");
      authResolved = true;
      return;
    }

    // Per-channel outbound drainer (§3.1). Schedules a 1ms drain task on the channel event loop
    // that ships queued events through eventWriter → TextWebSocketFrame → channel.write, then
    // notifies readGate.onAfterDrain so the inbound auto-read gate releases when the queue
    // recedes below 50% of capacity. Stop on close-future so the worker loop doesn't keep
    // ticking against a dead channel.
    final var drainer = new OutboundDrainer(ctx, session, readGate, eventWriter, nanoClock);
    drainer.start();
    ctx.channel().closeFuture().addListener(f -> drainer.stop());

    // 11. Cancel the timeout, mark resolved, audit, log.
    cancelTimeout();
    authResolved = true;
    if (auditLogger.isWritable()) {
      auditLogger.record(
          epochNanoClock.nanoTime(),
          claims.sub(),
          claims.jti(),
          remoteIp,
          AuditAction.AUTH_SUCCESS,
          null,
          null,
          0L,
          0L,
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

    // 12. AccountLimits push (§3.14). One frame per entitled account; the source impl pushes
    // pessimistic defaults when a particular account cannot be resolved so the UI's
    // disabled-by-default submit gating still receives a frame per claim. Ordering: pushed AFTER
    // the read gate + listener are installed so the per-channel OutboundDrainer (scheduled by
    // BridgeNettyBootstrap on channelActive) can flush the limits frames before the user sees
    // the first interactive prompt.
    //
    // The Sink wraps OutboundQueue.offer with a TERMINAL escalation per §3.1 step 5 — if the
    // per-session queue is already saturated at auth time (highly unusual; only possible if a
    // backpressured channel is racing with auth) the bridge MUST escalate to a fatal
    // BridgeStatus and close the channel rather than silently dropping the AccountLimits frame
    // (Gemini medium finding on PR #70 R3).
    final var queue = session.outboundQueue();
    final AccountLimitsSource.Sink sink =
        event -> {
          final var result = queue.offer(event);
          if (result == OutboundQueue.OfferResult.TERMINAL) {
            // Queue is full of critical events with no RawFix to drop. We cannot deliver
            // AccountLimits — the UI would be left disabled-by-default with no way to know.
            // Escalate per §3.1 step 5: emit a fatal BridgeStatus (which will itself be dropped
            // by the same overflow, but the close-frame will still surface the failure) and
            // close the channel. We log here too so the operator has an audit trail beyond the
            // wire status.
            LOG.error(
                "AccountLimits push hit TERMINAL outbound overflow at AUTH_SUCCESS for sub={}"
                    + " sessionId={} — closing channel",
                claims.sub(),
                sessionId);
            ctx.channel()
                .close()
                .addListener(
                    f ->
                        LOG.warn(
                            "Closed channel for sub={} sessionId={} after AccountLimits TERMINAL"
                                + " overflow",
                            claims.sub(),
                            sessionId));
          }
          return result;
        };
    accountLimitsSource.pushFor(claims, session, sink);
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
    // Cold path — Day 5 swapped the hand-rolled JSON for the real BrowserEventWriter so the
    // Error event uses the same writer-validated escaping rules as every other outbound frame.
    final var error = new BrowserEvent.Error(reason);
    final var buf = ctx.alloc().buffer();
    TextWebSocketFrame text = null;
    try {
      eventWriter.writeError(error, buf);
      text = new TextWebSocketFrame(buf);
    } catch (final RuntimeException ex) {
      // Writer rejected the reason string (forbidden character / control byte). The buffer
      // hasn't been wrapped in a frame yet, so release it to avoid a leak; force the close
      // without the friendly Error frame so the channel still terminates promptly.
      buf.release();
      LOG.error("Auth error writer rejected reason='{}' — closing without Error frame", reason, ex);
      final var close = new CloseWebSocketFrame(closeCode, reason);
      ctx.writeAndFlush(close).addListener(closeFuture -> ctx.close());
      cancelTimeout();
      authResolved = true;
      return;
    }
    // Track ownership transfer so a synchronous throw from ctx.writeAndFlush (rare but possible
    // before the frame is accepted by the pipeline) doesn't leak the pooled buffer (Gemini
    // medium finding on PR #70 R5).
    final var frame = text;
    text = null;
    try {
      ctx.writeAndFlush(frame)
          .addListener(
              future -> {
                final var close = new CloseWebSocketFrame(closeCode, reason);
                ctx.writeAndFlush(close).addListener(closeFuture -> ctx.close());
              });
    } catch (final RuntimeException ex) {
      // writeAndFlush threw before accepting the frame — release it ourselves.
      frame.release();
      LOG.error("ctx.writeAndFlush failed for Error frame; closing channel", ex);
      ctx.close();
      cancelTimeout();
      authResolved = true;
      return;
    }
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
        epochNanoClock.nanoTime(),
        null,
        null,
        remoteIp,
        AuditAction.AUTH_FAIL,
        null,
        null,
        0L,
        0L,
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
