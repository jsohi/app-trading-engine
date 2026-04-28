package com.trading.engine.websocket;

import com.trading.engine.messages.sbe.ClientAckDecoder;
import com.trading.engine.messages.sbe.ClientHeartbeatDecoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.WebSocketAuthDecoder;
import com.trading.engine.messages.sbe.WebSocketErrorCode;
import com.trading.engine.messages.sbe.WebSocketErrorEncoder;
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
 *   <li>60 → re-authentication (token refresh before expiry)
 *   <li>62 → subscribe (add symbol + eventType subscriptions)
 *   <li>63 → unsubscribe (remove subscriptions; empty = unsubscribe all)
 *   <li>65 → client heartbeat
 *   <li>68 → gap request (stub — TODO(APP-35): PR 4)
 *   <li>69 → session resume (stub — TODO(APP-35): PR 4)
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

  // --- Reusable SBE decoders (per-channel, re-wrapped per channelRead) ---
  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final WebSocketSubscribeDecoder subscribeDecoder = new WebSocketSubscribeDecoder();
  private final WebSocketUnsubscribeDecoder unsubscribeDecoder = new WebSocketUnsubscribeDecoder();
  private final WebSocketAuthDecoder authDecoder = new WebSocketAuthDecoder();
  private final ClientHeartbeatDecoder heartbeatDecoder = new ClientHeartbeatDecoder();
  private final ClientAckDecoder ackDecoder = new ClientAckDecoder();
  private final UnsafeBuffer wrapBuffer = new UnsafeBuffer(new byte[0]);
  private final ExpandableArrayBuffer responseBuf = new ExpandableArrayBuffer(128);
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
   */
  public WebSocketFrameDispatcher(
      final WebSocketSessionManager sessionManager,
      final JwtValidator jwtValidator,
      final JtiRevocationCache jtiCache,
      final UserEntitlementService entitlementService,
      final WebSocketServerConfig config,
      final WebSocketMetrics metrics,
      final NanoClock nanoClock,
      final Executor validationExecutor) {
    this.sessionManager = sessionManager;
    this.jwtValidator = jwtValidator;
    this.jtiCache = jtiCache;
    this.entitlementService = entitlementService;
    this.config = config;
    this.metrics = metrics;
    this.nanoClock = nanoClock;
    this.validationExecutor = validationExecutor;
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
        return;
      }

      final var content = frame.content();
      if (content.readableBytes() < MessageHeaderDecoder.ENCODED_LENGTH) {
        LOG.warn("Frame too small for SBE header: {} bytes", content.readableBytes());
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
        case 60 -> handleReAuth(ctx, session, blockLength, version);
        case 62 -> handleSubscribe(ctx, session, blockLength, version);
        case 63 -> handleUnsubscribe(session, blockLength, version);
        case 65 -> handleClientHeartbeat(session, blockLength, version);
        case 68 -> handleGapRequest(ctx);
        case 69 -> handleSessionResume(ctx);
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

              // Update session
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

  private void handleGapRequest(final ChannelHandlerContext ctx) {
    // TODO(APP-35): implement with ReliableStreamTracker in PR 4
    LOG.warn("GapRequest received but not yet implemented");
    sendError(ctx, WebSocketErrorCode.CommandRejected);
  }

  private void handleSessionResume(final ChannelHandlerContext ctx) {
    // TODO(APP-35): implement session resume in PR 4
    LOG.warn("SessionResume received but not yet implemented");
    sendError(ctx, WebSocketErrorCode.CommandRejected);
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
      ctx.writeAndFlush(new BinaryWebSocketFrame(nettyBuf));
      written = true;
    } finally {
      if (!written) {
        nettyBuf.release();
      }
    }
  }

  @Override
  public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
    LOG.error(
        "Unexpected exception in frame dispatcher for {}", ctx.channel().remoteAddress(), cause);
    ctx.close();
  }
}
