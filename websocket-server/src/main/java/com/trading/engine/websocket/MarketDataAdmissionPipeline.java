package com.trading.engine.websocket;

import static com.trading.engine.messages.MarketDataConstants.MARKET_DATA_PUBLISH_CADENCE_MICROS;

import com.trading.engine.messages.sbe.MarketDataSnapshotRequestDecoder;
import com.trading.engine.messages.sbe.MarketDataSnapshotRequestEncoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.WebSocketErrorCode;
import com.trading.engine.messages.sbe.WebSocketErrorEncoder;
import com.trading.engine.projections.SymbolPacker;
import io.aeron.Publication;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Per-session admission pipeline for {@code MarketDataSnapshotRequest} (template 56) frames.
 * Extracted from {@link WebSocketFrameDispatcher} so the 4-stage fail-closed admission contract is
 * a single auditable unit and so the dedicated alloc-tripwire / admission tests can exercise it
 * without a full dispatcher pipeline.
 *
 * <h2>Admission contract — fail-closed, ordered</h2>
 *
 * <ol>
 *   <li><b>(a) Malformed-payload guard</b> — split into two categories:
 *       <ul>
 *         <li><b>Category A — RFC 6455 close 1003 + session torn down.</b> SBE header validator
 *             fails (bad {@code templateId}/{@code schemaId}/{@code version}/{@code blockLength});
 *             buffer-length validator fails; {@code symbol} field decode fails (non-ASCII or
 *             embedded NUL). Counter {@code dispatcher.malformed}. The pipeline returns {@link
 *             Outcome#MALFORMED_CLOSE} so the caller closes the channel.
 *         <li><b>Category B — soft error, session preserved.</b> Symbol decodes cleanly but is NOT
 *             in {@link SymbolEntitlementMap}. Returns {@link Outcome#SYMBOL_UNKNOWN} after
 *             emitting {@code WebSocketError(code=SymbolUnknown)}; counter {@code
 *             dispatcher.symbol.unknown}.
 *       </ul>
 *       <b>Critical security distinction</b>: an attacker spamming {@code XXXXXX} symbol names
 *       against a legitimate session can only consume that session's rate-limit budget — it CANNOT
 *       terminate the session, eliminating the DoS primitive that a "close socket on unentitled
 *       symbol" design would have created.
 *   <li><b>(b) Entitlement check</b> — {@code session.subscriptionFilter().entitledSymbolsByAccount
 *       .contains(packedSymbol)}; deny with {@code WebSocketError(code=EntitlementDenied)} on miss.
 *       Returns {@link Outcome#ENTITLEMENT_DENIED}; counter {@code subscription.entitlement.
 *       denied}. Prevents a non-entitled client from draining publisher CPU by spamming snapshot
 *       requests.
 *   <li><b>(c) Token-bucket consume</b> — {@link WebSocketSession#tryConsumeSnapshotToken(long)};
 *       reject with {@code WebSocketError(code=SnapshotThrottled)} on miss. Returns {@link
 *       Outcome#THROTTLED}.
 *   <li><b>(d) Per-symbol dedup within drain cycle</b> — per-session {@link Long2LongHashMap} maps
 *       packed-symbol → {@code lastRequestedNanos}. If {@code nowNs - lastRequestedNs <
 *       MARKET_DATA_PUBLISH_CADENCE_MICROS × 1_000} the request is a duplicate within the current
 *       publisher drain window: refund the token, increment {@code marketdata.snapshot.deduped},
 *       return {@link Outcome#DEDUPED}.
 *   <li><b>(e) Publish on stream 205</b> via {@link SnapshotRequestPublisher#offer(org.agrona.
 *       DirectBuffer, int, int)} with five-case Aeron offer return-code handling:
 *       <ul>
 *         <li>{@link Publication#BACK_PRESSURED} → retry once; on second miss drop + {@code
 *             WebSocketError(code=SnapshotBackpressured)} + <b>refund token</b>.
 *         <li>{@link Publication#NOT_CONNECTED} / {@link Publication#ADMIN_ACTION} → drop + {@code
 *             WebSocketError(code=SnapshotBackpressured)} + <b>refund token</b>.
 *         <li>{@link Publication#MAX_POSITION_EXCEEDED} → fatal log + drop + <b>refund token</b>.
 *         <li>{@link Publication#CLOSED} → fatal log + drop + <b>refund token</b>; pipeline returns
 *             {@link Outcome#PUBLISH_FATAL}.
 *         <li>≥ 0 → success; return {@link Outcome#PUBLISHED}.
 *       </ul>
 *       The token is <b>NOT</b> refunded on snapshot-timeout (a separate path) — the publish to
 *       stream 205 already succeeded; the publisher consumed a slot. Timeout handling lives in the
 *       publisher / browser, not in this pipeline.
 * </ol>
 *
 * <h2>Threading model</h2>
 *
 * One pipeline instance per WebSocket channel (constructed inside the per-channel {@link
 * WebSocketFrameDispatcher}). All methods are invoked from that channel's own Netty event loop only
 * — single-threaded. The dedup map and the SBE encoder / scratch buffers are mutable fields without
 * synchronisation.
 *
 * <h2>Allocation</h2>
 *
 * Zero allocation on the admission hot path after construction. All SBE encoders/decoders, the
 * scratch {@link ExpandableArrayBuffer}, the dedup {@link Long2LongHashMap}, and the symbol decode
 * byte array are final fields bound at construction. The {@code BinaryWebSocketFrame} + wrapping
 * {@link ByteBuf} for the error response are per-error allocations — error emissions are cold-path
 * per design.
 */
public final class MarketDataAdmissionPipeline {

  private static final Logger LOG = LogManager.getLogger(MarketDataAdmissionPipeline.class);

  /** Maximum bounded retries on {@link Publication#BACK_PRESSURED} before drop + refund. */
  static final int MAX_BACK_PRESSURED_RETRIES = 1;

  /**
   * Dedup window in nanoseconds — requests for the same symbol within this window of the previous
   * request are coalesced (token refunded, no publish on stream 205). Equals one publisher drain
   * cadence so the next drain naturally serves all coalesced requests with a single tick.
   */
  static final long DEDUP_WINDOW_NANOS = MARKET_DATA_PUBLISH_CADENCE_MICROS * 1_000L;

  /** Sentinel for a never-requested symbol in the dedup map. */
  private static final long NEVER_REQUESTED = Long.MIN_VALUE;

  /**
   * Outcome of a single {@link #admit(ChannelHandlerContext, WebSocketSession, ByteBuf, int)} call.
   */
  public enum Outcome {
    /** Malformed payload — caller must close the channel (Category A, RFC 6455 close 1003). */
    MALFORMED_CLOSE,
    /** Symbol absent from {@link SymbolEntitlementMap} — soft error, session preserved. */
    SYMBOL_UNKNOWN,
    /** Symbol present in map but the session's account is not entitled — soft error. */
    ENTITLEMENT_DENIED,
    /** Token bucket empty — soft error, session preserved. */
    THROTTLED,
    /** Duplicate request within the publisher drain window — token refunded, no publish. */
    DEDUPED,
    /** Stream-205 offer succeeded. */
    PUBLISHED,
    /** Stream-205 offer returned a transient error after retries — token refunded, soft error. */
    PUBLISH_BACKPRESSURED,
    /**
     * Stream-205 publication closed — fatal log; the session is preserved (publisher restart
     * pending).
     */
    PUBLISH_FATAL
  }

  // --- Injected collaborators ---
  private final SymbolEntitlementMap entitlementMap;
  private final SnapshotRequestPublisher publisher;
  private final WebSocketMetrics metrics;
  private final NanoClock nanoClock;

  // --- Reusable SBE decoders / encoders (per-channel) ---
  private final MessageHeaderDecoder requestHeaderDecoder = new MessageHeaderDecoder();
  private final MarketDataSnapshotRequestDecoder requestDecoder =
      new MarketDataSnapshotRequestDecoder();
  private final MarketDataSnapshotRequestEncoder requestEncoder =
      new MarketDataSnapshotRequestEncoder();
  private final MessageHeaderEncoder requestHeaderEncoder = new MessageHeaderEncoder();
  private final WebSocketErrorEncoder errorEncoder = new WebSocketErrorEncoder();
  private final MessageHeaderEncoder errorHeaderEncoder = new MessageHeaderEncoder();

  // --- Pre-allocated scratch ---
  private final ExpandableArrayBuffer requestEncodeBuf = new ExpandableArrayBuffer(64);
  private final ExpandableArrayBuffer errorEncodeBuf = new ExpandableArrayBuffer(64);
  private final UnsafeBuffer wrapBuffer = new UnsafeBuffer(new byte[0]);
  private final UnsafeBuffer offerBuffer = new UnsafeBuffer(new byte[0]);
  private final byte[] symbolDecodeBuffer = new byte[8];

  // --- Per-session dedup map (packed-symbol → lastRequestedNanos) ---
  private final Long2LongHashMap dedupMap = new Long2LongHashMap(NEVER_REQUESTED);

  /**
   * Construct a per-channel admission pipeline.
   *
   * @param entitlementMap the launcher-loaded {@link SymbolEntitlementMap} (immutable after boot)
   * @param publisher SAM seam over the stream-205 {@code Publication.offer(...)}
   * @param metrics metrics sink for the seven admission counters
   * @param nanoClock monotonic clock used for both the token-bucket consume / refill and the dedup
   *     window calculation; inject {@code SystemNanoClock.INSTANCE} in production
   */
  public MarketDataAdmissionPipeline(
      final SymbolEntitlementMap entitlementMap,
      final SnapshotRequestPublisher publisher,
      final WebSocketMetrics metrics,
      final NanoClock nanoClock) {
    this.entitlementMap = Objects.requireNonNull(entitlementMap, "entitlementMap");
    this.publisher = Objects.requireNonNull(publisher, "publisher");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
  }

  /**
   * Admit a single {@code MarketDataSnapshotRequest} frame through the 4-stage pipeline.
   *
   * @param ctx the channel context — used for sending {@code WebSocketError} responses
   * @param session the per-channel {@link WebSocketSession} (token bucket + subscription filter)
   * @param content the WebSocket frame payload — caller retains ownership and is responsible for
   *     releasing the underlying {@link ByteBuf}
   * @param blockLength the SBE block length parsed by the dispatcher's outer header decode
   * @return the admission {@link Outcome}; callers MUST close the channel on {@link
   *     Outcome#MALFORMED_CLOSE}
   */
  public Outcome admit(
      final ChannelHandlerContext ctx,
      final WebSocketSession session,
      final ByteBuf content,
      final int blockLength) {
    // ── (a-0) CompositeByteBuf defence — Agent B review F-2 ──
    // ByteBuf.nioBuffer() on a CompositeByteBuf silently allocates a heap copy of the merged
    // contents (Netty Javadoc). The dispatcher's channelRead has an `assert` against composites
    // but Java assertions are disabled by default in production. Guard here too so a future
    // pipeline change that adds WebSocketFrameAggregator (which CAN produce composites for
    // fragmented frames) does not silently start allocating on the hot path.
    if (content instanceof CompositeByteBuf) {
      metrics.dispatcherMalformed();
      sendCloseFrame(ctx, "snapshot-request CompositeByteBuf not supported");
      return Outcome.MALFORMED_CLOSE;
    }
    // ── (a) Category A — SBE header / length sanity ──
    if (content.readableBytes()
        < MessageHeaderDecoder.ENCODED_LENGTH + MarketDataSnapshotRequestDecoder.BLOCK_LENGTH) {
      metrics.dispatcherMalformed();
      sendCloseFrame(ctx, "snapshot-request frame too small for declared block-length");
      return Outcome.MALFORMED_CLOSE;
    }
    if (blockLength != MarketDataSnapshotRequestDecoder.BLOCK_LENGTH) {
      metrics.dispatcherMalformed();
      sendCloseFrame(
          ctx,
          "snapshot-request blockLength mismatch (expected="
              + MarketDataSnapshotRequestDecoder.BLOCK_LENGTH
              + ", got="
              + blockLength
              + ")");
      return Outcome.MALFORMED_CLOSE;
    }

    wrapBuffer.wrap(content.nioBuffer());
    requestHeaderDecoder.wrap(wrapBuffer, 0);
    final int schemaId = requestHeaderDecoder.schemaId();
    final int version = requestHeaderDecoder.version();
    if (schemaId != MarketDataSnapshotRequestDecoder.SCHEMA_ID
        || version != MarketDataSnapshotRequestDecoder.SCHEMA_VERSION) {
      metrics.dispatcherMalformed();
      sendCloseFrame(
          ctx,
          "snapshot-request schemaId/version mismatch (schemaId="
              + schemaId
              + ", version="
              + version
              + ")");
      return Outcome.MALFORMED_CLOSE;
    }

    requestDecoder.wrap(wrapBuffer, MessageHeaderDecoder.ENCODED_LENGTH, blockLength, version);
    requestDecoder.getSymbol(symbolDecodeBuffer, 0);
    if (!isAsciiNonNull(symbolDecodeBuffer)) {
      metrics.dispatcherMalformed();
      sendCloseFrame(ctx, "snapshot-request symbol has non-ASCII byte or embedded NUL");
      return Outcome.MALFORMED_CLOSE;
    }
    final long packedSymbol = SymbolPacker.pack(symbolDecodeBuffer, 0);

    // ── (a) Category B — symbol present in SymbolEntitlementMap ──
    if (entitlementMap.permittedAccountsFor(packedSymbol).isEmpty()) {
      metrics.dispatcherSymbolUnknown();
      sendError(ctx, WebSocketErrorCode.SymbolUnknown);
      return Outcome.SYMBOL_UNKNOWN;
    }

    // ── (b) Entitlement check — per-account symbol entitlement ──
    final var filter = session.subscriptionFilter();
    if (filter == null || !filter.entitledSymbolsByAccount().contains(packedSymbol)) {
      metrics.symbolEntitlementDenied();
      sendError(ctx, WebSocketErrorCode.EntitlementDenied);
      return Outcome.ENTITLEMENT_DENIED;
    }

    final long nowNs = nanoClock.nanoTime();

    // ── (c) Token-bucket consume ──
    if (!session.tryConsumeSnapshotToken(nowNs)) {
      sendError(ctx, WebSocketErrorCode.SnapshotThrottled);
      return Outcome.THROTTLED;
    }

    // ── (d) Per-symbol dedup within drain cycle ──
    // After the token was consumed for the dup'd request, refund it (net zero across the dup
    // call). The first request's consume is NOT refunded — the publisher already served a slot
    // for that one. Token-bucket ordering (c) → (d) ensures THROTTLED takes precedence over
    // DEDUPED so a malicious client can't bypass rate-limiting via dedup'd requests.
    final long lastNs = dedupMap.get(packedSymbol);
    if (lastNs != NEVER_REQUESTED && (nowNs - lastNs) < DEDUP_WINDOW_NANOS) {
      session.refundSnapshotToken();
      metrics.marketDataSnapshotDeduped();
      return Outcome.DEDUPED;
    }

    // Record the request timestamp BEFORE publish so a successful publish atomically updates the
    // dedup window. If the publish fails we still want the dedup window to apply to subsequent
    // attempts (avoids hot-loop spam after a transient publisher hiccup).
    dedupMap.put(packedSymbol, nowNs);

    // ── (e) Publish on stream 205 ──
    requestEncoder.wrapAndApplyHeader(requestEncodeBuf, 0, requestHeaderEncoder);
    requestEncoder.putSymbol(symbolDecodeBuffer, 0);
    final int encodedLen = MessageHeaderEncoder.ENCODED_LENGTH + requestEncoder.encodedLength();
    offerBuffer.wrap(requestEncodeBuf.byteArray(), 0, encodedLen);
    long offerResult = publisher.offer(offerBuffer, 0, encodedLen);
    int retries = 0;
    while (offerResult == Publication.BACK_PRESSURED && retries < MAX_BACK_PRESSURED_RETRIES) {
      retries++;
      offerResult = publisher.offer(offerBuffer, 0, encodedLen);
    }

    if (offerResult >= 0L) {
      return Outcome.PUBLISHED;
    }
    if (offerResult == Publication.BACK_PRESSURED
        || offerResult == Publication.NOT_CONNECTED
        || offerResult == Publication.ADMIN_ACTION) {
      session.refundSnapshotToken();
      sendError(ctx, WebSocketErrorCode.SnapshotBackpressured);
      return Outcome.PUBLISH_BACKPRESSURED;
    }
    if (offerResult == Publication.MAX_POSITION_EXCEEDED) {
      session.refundSnapshotToken();
      // Cold-path error log — explicit US_ASCII charset matches the SBE Symbol wire-format
      // convention (printable ASCII char[8]); platform-default charset would mis-render on a
      // non-ASCII JVM locale. The new String() allocation is acceptable on this catastrophic-
      // error branch per the websocket-server cold-path carve-out (Agent B review R1-F3).
      LOG.error(
          "Stream-205 publish hit MAX_POSITION_EXCEEDED — session={} symbol={}",
          session.sessionId(),
          new String(symbolDecodeBuffer, StandardCharsets.US_ASCII));
      sendError(ctx, WebSocketErrorCode.SnapshotBackpressured);
      return Outcome.PUBLISH_BACKPRESSURED;
    }
    if (offerResult == Publication.CLOSED) {
      session.refundSnapshotToken();
      // Cold-path error log — see MAX_POSITION_EXCEEDED branch above for the charset rationale.
      LOG.error(
          "Stream-205 publication CLOSED — session={} symbol={}",
          session.sessionId(),
          new String(symbolDecodeBuffer, StandardCharsets.US_ASCII));
      sendError(ctx, WebSocketErrorCode.SnapshotBackpressured);
      return Outcome.PUBLISH_FATAL;
    }
    // Unknown negative return — defensive: refund + soft-error.
    session.refundSnapshotToken();
    sendError(ctx, WebSocketErrorCode.SnapshotBackpressured);
    return Outcome.PUBLISH_BACKPRESSURED;
  }

  // ────────────────────────────────────────────────────────────────────────
  // Helpers

  /**
   * Validates the decoded 8-byte symbol against the SBE char[8] convention:
   *
   * <ul>
   *   <li>The first byte MUST be printable ASCII (0x20–0x7E); a leading NUL means no symbol at all
   *       and the frame is malformed.
   *   <li>Subsequent bytes are either printable ASCII (continuing the symbol) or NUL (trailing
   *       padding). Once a NUL is seen, all remaining bytes MUST also be NUL — an embedded NUL
   *       followed by a non-NUL byte indicates a corrupt / tampered frame.
   * </ul>
   *
   * <p>Catches: leading NUL (no symbol); non-ASCII / control bytes (0x00–0x1F except trailing NUL,
   * 0x7F, ≥ 0x80); embedded NUL (e.g. {@code "AB\0CD\0\0\0"}). Accepts: a fully-populated 8-char
   * symbol like {@code "EURUSDJP"}; a short symbol with trailing NUL padding like {@code
   * "EURUSD\0\0"}.
   */
  private static boolean isAsciiNonNull(final byte[] symbol) {
    if (symbol.length == 0) {
      return false;
    }
    final int first = symbol[0] & 0xFF;
    if (first < 0x20 || first > 0x7E) {
      return false;
    }
    boolean sawTrailingNul = false;
    for (int i = 1; i < symbol.length; i++) {
      final int b = symbol[i] & 0xFF;
      if (b == 0x00) {
        sawTrailingNul = true;
      } else if (sawTrailingNul) {
        // NUL followed by non-NUL → embedded NUL → malformed.
        return false;
      } else if (b < 0x20 || b > 0x7E) {
        return false;
      }
    }
    return true;
  }

  private void sendError(final ChannelHandlerContext ctx, final WebSocketErrorCode errorCode) {
    if (!ctx.channel().isActive()) {
      return;
    }
    final var errorText = ErrorTextRegistry.textFor(errorCode);
    errorEncoder.wrapAndApplyHeader(errorEncodeBuf, 0, errorHeaderEncoder);
    errorEncoder.errorCode(errorCode);
    errorEncoder.putErrorText(errorText, 0, errorText.length);
    final int encodedLen = MessageHeaderEncoder.ENCODED_LENGTH + errorEncoder.encodedLength();
    final var nettyBuf = ctx.alloc().buffer(encodedLen);
    boolean written = false;
    try {
      nettyBuf.writeBytes(errorEncodeBuf.byteArray(), 0, encodedLen);
      ctx.writeAndFlush(new BinaryWebSocketFrame(nettyBuf));
      written = true;
    } finally {
      if (!written) {
        nettyBuf.release();
      }
    }
  }

  /**
   * Send an RFC 6455 close frame with status code 1003 (unsupported data) carrying the reason
   * string, then close the channel. The dispatcher chooses MALFORMED_CLOSE only for Category A
   * malformed-payload failures — see class Javadoc.
   */
  private void sendCloseFrame(final ChannelHandlerContext ctx, final String reason) {
    final var ch = ctx.channel();
    if (!ch.isActive()) {
      return;
    }
    LOG.warn("Closing channel — malformed snapshot-request: {}", reason);
    ch.writeAndFlush(new CloseWebSocketFrame(1003, reason)).addListener(future -> closeChannel(ch));
  }

  private static void closeChannel(final Channel ch) {
    if (ch.isActive()) {
      ch.close();
    }
  }

  // Package-private accessors for tests.
  Long2LongHashMap dedupMap() {
    return dedupMap;
  }
}
