package com.trading.engine.websocket;

import com.trading.engine.messages.sbe.ClientAckDecoder;
import com.trading.engine.messages.sbe.ClientHeartbeatDecoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.ReplayCompleteEncoder;
import com.trading.engine.messages.sbe.SessionResumeDecoder;
import com.trading.engine.messages.sbe.WebSocketAuthDecoder;
import com.trading.engine.messages.sbe.WebSocketErrorCode;
import com.trading.engine.messages.sbe.WebSocketErrorEncoder;
import com.trading.engine.messages.sbe.WebSocketGapRequestDecoder;
import com.trading.engine.messages.sbe.WebSocketSubscribeDecoder;
import com.trading.engine.messages.sbe.WebSocketUnsubscribeDecoder;
import com.trading.engine.projections.SymbolPacker;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.util.ReferenceCountUtil;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Per-channel post-auth frame router. Added to the pipeline dynamically by {@link JwtAuthHandler}
 * after successful authentication. Routes incoming WebSocket frames by SBE templateId.
 *
 * <p><b>Routing:</b>
 *
 * <ul>
 *   <li>1 → quote request (forwarded to cluster via {@link CommandDispatcher})
 *   <li>4 → new order single (forwarded to cluster via {@link CommandDispatcher})
 *   <li>6 → cancel order request (forwarded to cluster via {@link CommandDispatcher})
 *   <li>60 → re-authentication (token refresh before expiry)
 *   <li>62 → subscribe (add symbol + eventType subscriptions)
 *   <li>63 → unsubscribe (remove subscriptions; empty = unsubscribe all)
 *   <li>65 → client heartbeat
 *   <li>68 → gap request (replay missing reliable frames from {@link ReliableStreamTracker})
 *   <li>69 → session resume (validate originalAuthJti, replay missed frames)
 *   <li>71 → client ack
 *   <li>default → warn + close after 3 consecutive unknowns
 * </ul>
 *
 * <p><b>Threading.</b> Per-channel instance, NOT {@code @Sharable}. Runs on the channel's Netty
 * event loop thread only. SBE decoders are reusable fields re-wrapped per {@code channelRead}.
 *
 * <p><b>Allocation.</b> SBE decoders reused per-channel. {@link UnsafeBuffer} wraps {@code
 * ByteBuf.nioBuffer()} — zero-copy, valid only within {@code channelRead} scope. Response encoding
 * uses a pre-allocated {@link ExpandableArrayBuffer}.
 *
 * @see JwtAuthHandler
 * @see <a href="docs/websocket-architecture.md">WebSocket Architecture — Section 3</a>
 */
public final class WebSocketFrameDispatcher extends ChannelInboundHandlerAdapter {

  private static final Logger LOG = LogManager.getLogger(WebSocketFrameDispatcher.class);

  /** Close channel after this many consecutive unknown templateIds. */
  private static final int MAX_CONSECUTIVE_UNKNOWN = 3;

  private final WebSocketSessionManager sessionManager;
  private final JwtValidator jwtValidator;
  private final JtiRevocationCache jtiCache;
  private final UserEntitlementService entitlementService;
  private final WebSocketServerConfig config;
  private final WebSocketMetrics metrics;
  private final NanoClock nanoClock;
  private final Executor validationExecutor;
  private final CommandDispatcher commandDispatcher;

  /**
   * Per-channel admission pipeline for {@code MarketDataSnapshotRequest} (template 56). Required —
   * supplied via the canonical constructor.
   */
  private final MarketDataAdmissionPipeline marketDataAdmissionPipeline;

  // --- Reusable SBE decoders (per-channel, re-wrapped per channelRead) ---
  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final WebSocketSubscribeDecoder subscribeDecoder = new WebSocketSubscribeDecoder();
  private final WebSocketUnsubscribeDecoder unsubscribeDecoder = new WebSocketUnsubscribeDecoder();
  private final WebSocketAuthDecoder authDecoder = new WebSocketAuthDecoder();
  private final ClientHeartbeatDecoder heartbeatDecoder = new ClientHeartbeatDecoder();
  private final ClientAckDecoder ackDecoder = new ClientAckDecoder();
  private final WebSocketGapRequestDecoder gapDecoder = new WebSocketGapRequestDecoder();
  private final SessionResumeDecoder resumeDecoder = new SessionResumeDecoder();
  private final UnsafeBuffer wrapBuffer = new UnsafeBuffer(new byte[0]);
  private final ExpandableArrayBuffer responseBuf = new ExpandableArrayBuffer(128);

  // /review R5 (LOW): pre-allocate the response-side encoders so cold-path sendError +
  // sendReplayComplete don't construct fresh SBE encoder + MessageHeaderEncoder per call.
  // These dispatcher instances are per-channel; each is wrapped via `wrapAndApplyHeader` on
  // every emit, so reusing the field is safe (the wrap re-establishes the cursor at offset 0).
  private final MessageHeaderEncoder responseHeaderEncoder = new MessageHeaderEncoder();
  private final ReplayCompleteEncoder replayCompleteEncoder = new ReplayCompleteEncoder();
  private final WebSocketErrorEncoder errorEncoder = new WebSocketErrorEncoder();
  private final byte[] symbolDecodeBuffer = new byte[8];

  // --- Per-channel mutable state ---
  private int consecutiveUnknownCount;

  /**
   * Create a per-channel frame dispatcher.
   *
   * @param sessionManager session registry for session lookup
   * @param jwtValidator JWT validator for re-authentication
   * @param jtiCache JTI revocation cache for re-auth JTI checks
   * @param entitlementService account entitlement validator for re-auth refresh
   * @param config server configuration
   * @param metrics metrics instance
   * @param nanoClock monotonic clock for heartbeat timestamps
   * @param validationExecutor executor for async JWT re-auth validation; use {@code
   *     ForkJoinPool.commonPool()} in production, {@code Runnable::run} in tests
   * @param commandDispatcher dispatcher for browser-to-cluster commands
   */
  public WebSocketFrameDispatcher(
      final WebSocketSessionManager sessionManager,
      final JwtValidator jwtValidator,
      final JtiRevocationCache jtiCache,
      final UserEntitlementService entitlementService,
      final WebSocketServerConfig config,
      final WebSocketMetrics metrics,
      final NanoClock nanoClock,
      final Executor validationExecutor,
      final CommandDispatcher commandDispatcher,
      final MarketDataAdmissionPipeline marketDataAdmissionPipeline) {
    this.sessionManager = Objects.requireNonNull(sessionManager, "sessionManager");
    this.jwtValidator = Objects.requireNonNull(jwtValidator, "jwtValidator");
    this.jtiCache = Objects.requireNonNull(jtiCache, "jtiCache");
    this.entitlementService = Objects.requireNonNull(entitlementService, "entitlementService");
    this.config = Objects.requireNonNull(config, "config");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
    this.validationExecutor = Objects.requireNonNull(validationExecutor, "validationExecutor");
    this.commandDispatcher = Objects.requireNonNull(commandDispatcher, "commandDispatcher");
    this.marketDataAdmissionPipeline =
        Objects.requireNonNull(marketDataAdmissionPipeline, "marketDataAdmissionPipeline");
  }

  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
    // TextWebSocketFrame: release to prevent ByteBuf leak, log warning
    if (msg instanceof TextWebSocketFrame) {
      ReferenceCountUtil.release(msg);
      LOG.warn("TextWebSocketFrame received post-auth — not supported, releasing");
      consecutiveUnknownCount++;
      if (consecutiveUnknownCount >= MAX_CONSECUTIVE_UNKNOWN) {
        LOG.warn("Closing channel after {} consecutive unknown frames", consecutiveUnknownCount);
        ctx.close();
      }
      return;
    }

    if (!(msg instanceof BinaryWebSocketFrame frame)) {
      ReferenceCountUtil.release(msg);
      return;
    }

    try {
      // Null session guard — session may have been deregistered by timeout/admin
      final var session = sessionManager.findSession(ctx.channel());
      if (session == null) {
        LOG.warn("Session not found for channel — closing");
        ctx.close();
        return;
      }

      final var content = frame.content();
      if (content.readableBytes() < MessageHeaderDecoder.ENCODED_LENGTH) {
        LOG.warn("Frame too small for SBE header: {} bytes", content.readableBytes());
        consecutiveUnknownCount++;
        if (consecutiveUnknownCount >= MAX_CONSECUTIVE_UNKNOWN) {
          ctx.close();
        }
        return;
      }

      // Wrap ByteBuf in UnsafeBuffer — zero-copy, valid only within this channelRead scope.
      // Assertion documents the zero-copy assumption: BinaryWebSocketFrame from Netty's
      // WebSocketDecoder always wraps a single non-composite ByteBuf.
      assert !(content instanceof CompositeByteBuf)
          : "Composite ByteBuf not supported — nioBuffer() would copy";
      wrapBuffer.wrap(content.nioBuffer());
      headerDecoder.wrap(wrapBuffer, 0);

      final int templateId = headerDecoder.templateId();
      final int blockLength = headerDecoder.blockLength();
      final int version = headerDecoder.version();

      boolean known = true;
      switch (templateId) {
        case 1, 4, 6 ->
            commandDispatcher.dispatch(ctx, session, content, templateId, blockLength, version);
        case 56 ->
            // Outcome controlled inside the pipeline; MALFORMED_CLOSE already closed the channel.
            marketDataAdmissionPipeline.admit(ctx, session, content, blockLength);
        case 60 -> handleReAuth(ctx, session, blockLength, version);
        case 62 -> handleSubscribe(ctx, session, blockLength, version);
        case 63 -> handleUnsubscribe(session, blockLength, version);
        case 65 -> handleClientHeartbeat(session, blockLength, version);
        case 68 -> handleGapRequest(ctx, session, blockLength, version);
        case 69 -> handleSessionResume(ctx, session, blockLength, version);
        case 71 -> handleClientAck(session, blockLength, version);
        default -> {
          known = false;
          consecutiveUnknownCount++;
          LOG.warn("Unknown templateId={} from session={}", templateId, session.sessionId());
          if (consecutiveUnknownCount >= MAX_CONSECUTIVE_UNKNOWN) {
            LOG.warn(
                "Closing channel after {} consecutive unknown templateIds",
                consecutiveUnknownCount);
            ctx.close();
          }
        }
      }

      if (known) {
        consecutiveUnknownCount = 0;
      }
    } finally {
      frame.release();
    }
  }

  // --- Template handlers ---

  private void handleReAuth(
      final ChannelHandlerContext ctx,
      final WebSocketSession session,
      final int blockLength,
      final int version) {
    authDecoder.wrap(wrapBuffer, MessageHeaderDecoder.ENCODED_LENGTH, blockLength, version);

    final int tokenLen = authDecoder.tokenLength();
    // Validate token length before allocation — prevents OOM from malicious clients.
    if (tokenLen <= 0 || tokenLen > config.maxTokenSizeBytes()) {
      LOG.warn("Re-auth token length invalid: {}", tokenLen);
      sendError(ctx, WebSocketErrorCode.AuthenticationFailed);
      return;
    }
    final var tokenBytes = new byte[tokenLen];
    authDecoder.getToken(tokenBytes, 0, tokenLen);
    final var tokenString = new String(tokenBytes, StandardCharsets.UTF_8);

    // Offload JWT validation to avoid blocking the Netty event loop during JWKS cache miss
    // (up to 10s). Completion callback runs on the channel's event loop via ctx.executor().
    final var sessionUserId = session.userId();
    CompletableFuture.supplyAsync(() -> jwtValidator.validate(tokenString), validationExecutor)
        .whenCompleteAsync(
            (claims, ex) -> {
              if (ex != null) {
                final var cause = ex.getCause() != null ? ex.getCause() : ex;
                LOG.warn("Re-auth JWT validation failed: {}", cause.getMessage());
                sendError(ctx, WebSocketErrorCode.AuthenticationFailed);
                return;
              }

              // Verify sub matches existing session — prevents session hijacking
              if (!claims.sub().equals(sessionUserId)) {
                LOG.warn(
                    "Re-auth sub mismatch: session={}, token sub={}", sessionUserId, claims.sub());
                sendError(ctx, WebSocketErrorCode.AuthenticationFailed);
                return; // Do NOT close — existing session continues with old token
              }

              // Check new JTI is not revoked
              if (jtiCache.isRevoked(claims.jti())) {
                LOG.warn("Re-auth with revoked JTI for userId={}", sessionUserId);
                sendError(ctx, WebSocketErrorCode.AuthenticationFailed);
                return;
              }

              // Revoke old JTI
              final var oldJti = session.jti();
              if (oldJti != null && !oldJti.isEmpty()) {
                jtiCache.revoke(oldJti);
              }

              // Refresh entitlements
              final var validatedAccounts = entitlementService.validateAccounts(claims.accounts());
              if (validatedAccounts.isEmpty()) {
                LOG.warn("Re-auth: all accounts invalid for userId={}", sessionUserId);
                sendError(ctx, WebSocketErrorCode.AuthorizationFailed);
                return;
              }

              // Update CURRENT jti only — originalAuthJti is preserved across re-auth.
              session.jti(claims.jti());
              session.entitledAccounts(validatedAccounts);

              LOG.info("Re-auth success: userId={}", sessionUserId);
            },
            ctx.executor());
  }

  private void handleSubscribe(
      final ChannelHandlerContext ctx,
      final WebSocketSession session,
      final int blockLength,
      final int version) {
    subscribeDecoder.wrap(wrapBuffer, MessageHeaderDecoder.ENCODED_LENGTH, blockLength, version);

    final var filter = session.subscriptionFilter();
    if (filter == null) {
      return; // pre-auth — should not happen since dispatcher is post-auth
    }

    final var symbols = subscribeDecoder.symbols();
    while (symbols.hasNext()) {
      symbols.next();
      // Extract 8-byte symbol and pack into long
      for (int i = 0; i < 8; i++) {
        symbolDecodeBuffer[i] = symbols.symbol(i);
      }
      final long packedSymbol = SymbolPacker.pack(symbolDecodeBuffer, 0);

      // Mask eventTypes to valid bits only (0x1F = bits 0-4)
      final long eventTypes = symbols.eventTypes() & 0x1FL;

      if (!filter.addSubscription(packedSymbol, (int) eventTypes)) {
        LOG.debug(
            "Subscription limit reached ({}) for session={}",
            config.maxSubscriptionsPerClient(),
            session.sessionId());
        sendError(ctx, WebSocketErrorCode.InvalidSubscription);
        break; // partial accept — stop adding, notify client
      }
    }
    LOG.debug("Subscribe: {} symbols for session={}", symbols.count(), session.sessionId());
  }

  private void handleUnsubscribe(
      final WebSocketSession session, final int blockLength, final int version) {
    unsubscribeDecoder.wrap(wrapBuffer, MessageHeaderDecoder.ENCODED_LENGTH, blockLength, version);

    final var filter = session.subscriptionFilter();
    if (filter == null) {
      return;
    }

    final var symbols = unsubscribeDecoder.symbols();
    if (symbols.count() == 0) {
      // Empty symbols group = unsubscribe all
      filter.clear();
      LOG.debug("Unsubscribe all for session={}", session.sessionId());
      return;
    }

    while (symbols.hasNext()) {
      symbols.next();
      for (int i = 0; i < 8; i++) {
        symbolDecodeBuffer[i] = symbols.symbol(i);
      }
      final long packedSymbol = SymbolPacker.pack(symbolDecodeBuffer, 0);
      filter.removeSubscription(packedSymbol);
    }
    LOG.debug("Unsubscribe: {} symbols for session={}", symbols.count(), session.sessionId());
  }

  private void handleClientHeartbeat(
      final WebSocketSession session, final int blockLength, final int version) {
    heartbeatDecoder.wrap(wrapBuffer, MessageHeaderDecoder.ENCODED_LENGTH, blockLength, version);
    session.updateHeartbeat(nanoClock.nanoTime());
  }

  /**
   * Replay missed reliable frames in {@code [fromSeqNo, toSeqNo]} from the per-session ring buffer.
   * Validates request bounds; missing seqNos within bounds emit {@code
   * WebSocketError(BufferOverflow)}; out-of-bounds requests emit {@code
   * WebSocketError(CommandRejected)}.
   */
  private void handleGapRequest(
      final ChannelHandlerContext ctx,
      final WebSocketSession session,
      final int blockLength,
      final int version) {
    metrics.gapRequestReceived();
    gapDecoder.wrap(wrapBuffer, MessageHeaderDecoder.ENCODED_LENGTH, blockLength, version);
    final long fromSeqNo = gapDecoder.fromSeqNo();
    final long toSeqNo = gapDecoder.toSeqNo();
    final var tracker = session.reliableStreamTracker();
    if (tracker == null) {
      sendError(ctx, WebSocketErrorCode.CommandRejected);
      return;
    }
    if (fromSeqNo < 1
        || toSeqNo < 1
        || fromSeqNo > toSeqNo
        || toSeqNo > session.reliableSeqCounter()
        || (toSeqNo - fromSeqNo + 1) > tracker.capacity()) {
      LOG.warn(
          "GapRequest out of bounds: from={} to={} highSeqNo={} sessionId={}",
          fromSeqNo,
          toSeqNo,
          session.reliableSeqCounter(),
          session.sessionId());
      sendError(ctx, WebSocketErrorCode.CommandRejected);
      return;
    }

    replayRange(ctx, session, session, fromSeqNo, toSeqNo);
  }

  /**
   * Resume an existing session within its grace window. Validates session UUID + originalAuthJti
   * binding, then triggers a gap-replay from {@code lastSeqNo + 1} through the session's current
   * reliableSeqCounter. The schema-enforced auth (handler runs only post-auth via the pipeline)
   * provides the JWT validation; this handler enforces the additional binding that the client's
   * ORIGINAL login token has not been revoked.
   */
  private void handleSessionResume(
      final ChannelHandlerContext ctx,
      final WebSocketSession session,
      final int blockLength,
      final int version) {
    metrics.sessionResumeReceived();
    resumeDecoder.wrap(wrapBuffer, MessageHeaderDecoder.ENCODED_LENGTH, blockLength, version);
    final var sessionIdDec = resumeDecoder.sessionId();
    final long msb = sessionIdDec.mostSignificantBits();
    final long lsb = sessionIdDec.leastSignificantBits();
    final long lastSeqNo = resumeDecoder.lastSeqNo();
    final var requestedId = new UUID(msb, lsb);

    final var target = sessionManager.findById(requestedId);
    if (target == null) {
      LOG.warn("SessionResume: target sessionId={} not found", requestedId);
      sendError(ctx, WebSocketErrorCode.CommandRejected);
      return;
    }
    // sub-binding: the resuming session (this `session`) must belong to the same userId as the
    // target.
    if (!target.userId().equals(session.userId())) {
      LOG.warn(
          "SessionResume: sub mismatch (target user={}, current user={})",
          target.userId(),
          session.userId());
      sendError(ctx, WebSocketErrorCode.CommandRejected);
      return;
    }
    // originalAuthJti must still be on file and not revoked.
    final var originalJti = target.originalAuthJti();
    if (originalJti == null || jtiCache.isRevoked(originalJti)) {
      LOG.warn("SessionResume: originalAuthJti missing/revoked for sessionId={}", requestedId);
      sendError(ctx, WebSocketErrorCode.CommandRejected);
      return;
    }
    // Sanity bound on lastSeqNo — must not exceed what we've issued.
    if (lastSeqNo < 0 || lastSeqNo > target.reliableSeqCounter()) {
      LOG.warn(
          "SessionResume: lastSeqNo={} > target.reliableSeqCounter={} for sessionId={}",
          lastSeqNo,
          target.reliableSeqCounter(),
          requestedId);
      sendError(ctx, WebSocketErrorCode.CommandRejected);
      return;
    }

    final long fromSeqNo = lastSeqNo + 1;
    final long toSeqNo = target.reliableSeqCounter();
    if (fromSeqNo > toSeqNo) {
      // No gap. Send a ReplayComplete marker.
      sendReplayComplete(ctx, session);
      return;
    }
    final var tracker = target.reliableStreamTracker();
    if (tracker == null) {
      sendError(ctx, WebSocketErrorCode.CommandRejected);
      return;
    }
    if ((toSeqNo - fromSeqNo + 1) > tracker.capacity()) {
      // Gap exceeds replay-buffer capacity → client must re-snapshot.
      sendError(ctx, WebSocketErrorCode.BufferOverflow);
      return;
    }
    // Reuse the gap-replay machinery, but read frames from the disconnected `target`'s tracker
    // while routing replayInProgress + writes to the *current* session whose channel we're
    // replying on. Mismatching these would let SlowConsumerHandler disconnect the resuming
    // session as a slow consumer mid-replay (Gemini PR #62).
    replayRange(ctx, session, target, fromSeqNo, toSeqNo);
  }

  /**
   * Inline replay loop: iterate from..to inclusive, re-encoding present frames via {@link
   * FrameParser#encodeReliableReplay}. Missing entries emit {@code BufferOverflow}. Sends {@link
   * ReplayCompleteEncoder} at the end.
   *
   * <p>Reads source frames from {@code source.reliableStreamTracker()} but writes to {@code
   * ctx.channel()} and manages {@code currentSession.replayInProgress} — for a normal gap-request
   * these are the same session, but for {@code handleSessionResume} they differ: {@code source} is
   * the disconnected session whose ring buffer holds the replayed frames, while {@code
   * currentSession} is the freshly authenticated session whose channel is receiving them. The
   * slow-consumer flag must apply to whichever session owns the receiving channel — otherwise the
   * new session can be incorrectly disconnected as a slow consumer mid-replay (Gemini PR #62).
   *
   * @param ctx the receiving channel context
   * @param currentSession the session whose channel + slow-consumer state owns this replay
   * @param source the session whose tracker holds the frames to replay (== currentSession for
   *     normal gap-requests, != currentSession for session-resume)
   */
  private void replayRange(
      final ChannelHandlerContext ctx,
      final WebSocketSession currentSession,
      final WebSocketSession source,
      final long fromSeqNo,
      final long toSeqNo) {
    final var tracker = source.reliableStreamTracker();
    final var ch = ctx.channel();
    if (tracker == null || !ch.isActive()) {
      return;
    }
    currentSession.replayInProgress(true);
    try {
      // Use the tracker's payload capacity for a one-time scratch alloc per range.
      final var scratch = new byte[tracker.payloadCapacity()];
      for (long s = fromSeqNo; s <= toSeqNo; s++) {
        final int len = tracker.lookupLength(s);
        if (len < 0) {
          // Missing — emit BufferOverflow once and stop the replay (the gap is unrecoverable).
          sendError(ctx, WebSocketErrorCode.BufferOverflow);
          break;
        }
        tracker.copyPayload(s, scratch, 0);
        final var buf =
            ch.alloc()
                .buffer(
                    FrameParser.RELIABLE_HEADER_SIZE + len, FrameParser.RELIABLE_HEADER_SIZE + len);
        // Agent B R3 F-1: catch+rethrow instead of mutable `boolean written` flag
        // (CLAUDE.md §Local Variable Style — buffer-scan/accumulator carve-out does not
        // cover try-finally guard flags; mirrors JwtAuthHandler.sendAuthAck idiom).
        // Netty takes ownership of `buf` on successful `ch.write`; release only on a
        // pre-write exception.
        try {
          FrameParser.encodeReliableReplay(buf, s, scratch, 0, len);
          ch.write(new BinaryWebSocketFrame(buf));
          metrics.replaySent(len);
        } catch (final Throwable t) {
          buf.release();
          throw t;
        }
      }
      ch.flush();
      sendReplayComplete(ctx, currentSession);
    } finally {
      currentSession.replayInProgress(false);
    }
  }

  private void sendReplayComplete(final ChannelHandlerContext ctx, final WebSocketSession session) {
    final var ch = ctx.channel();
    if (!ch.isActive()) {
      return;
    }
    replayCompleteEncoder.wrapAndApplyHeader(responseBuf, 0, responseHeaderEncoder);
    final int encodedLen =
        MessageHeaderEncoder.ENCODED_LENGTH + replayCompleteEncoder.encodedLength();
    final var nettyBuf = ch.alloc().buffer(encodedLen);
    // Agent B R3 F-1: catch+rethrow instead of mutable `boolean written`.
    try {
      nettyBuf.writeBytes(responseBuf.byteArray(), 0, encodedLen);
      ch.writeAndFlush(new BinaryWebSocketFrame(nettyBuf));
    } catch (final Throwable t) {
      nettyBuf.release();
      throw t;
    }
  }

  private void handleClientAck(
      final WebSocketSession session, final int blockLength, final int version) {
    ackDecoder.wrap(wrapBuffer, MessageHeaderDecoder.ENCODED_LENGTH, blockLength, version);
    session.lastClientCmdSeqNo(ackDecoder.lastReceivedSeqNo());
  }

  // --- Response helpers ---

  private void sendError(final ChannelHandlerContext ctx, final WebSocketErrorCode errorCode) {
    if (!ctx.channel().isActive()) {
      return;
    }
    final var errorText = ErrorTextRegistry.textFor(errorCode);
    errorEncoder.wrapAndApplyHeader(responseBuf, 0, responseHeaderEncoder);
    errorEncoder.errorCode(errorCode);
    errorEncoder.putErrorText(errorText, 0, errorText.length);

    final int encodedLen = MessageHeaderEncoder.ENCODED_LENGTH + errorEncoder.encodedLength();
    final var nettyBuf = ctx.alloc().buffer(encodedLen);
    // Agent B R3 F-1: catch+rethrow instead of mutable `boolean written`.
    try {
      nettyBuf.writeBytes(responseBuf.byteArray(), 0, encodedLen);
      ctx.writeAndFlush(new BinaryWebSocketFrame(nettyBuf));
    } catch (final Throwable t) {
      nettyBuf.release();
      throw t;
    }
  }

  @Override
  public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
    // Clean up session from manager to prevent memory leaks
    sessionManager.removeSession(ctx.channel());
    ctx.fireChannelInactive();
  }

  @Override
  public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
    LOG.error(
        "Unexpected exception in frame dispatcher for {}", ctx.channel().remoteAddress(), cause);
    ctx.close();
  }
}
