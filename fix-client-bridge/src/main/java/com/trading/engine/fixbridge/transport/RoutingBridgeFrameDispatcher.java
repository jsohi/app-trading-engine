package com.trading.engine.fixbridge.transport;

import com.trading.engine.fixbridge.audit.AuditAction;
import com.trading.engine.fixbridge.audit.AuditLogger;
import com.trading.engine.fixbridge.json.MutableParsedMessage;
import com.trading.engine.fixbridge.quote.SessionQuoteIndex;
import java.util.Objects;
import org.agrona.concurrent.EpochNanoClock;

/**
 * Per-session real implementation of {@link BridgeFrameDispatcher} (§3.13 / §3.15).
 *
 * <p>Replaces the {@link BridgeFrameDispatcher#NOOP} stub from Day 4-c with the actual routing:
 *
 * <ol>
 *   <li>QuoteRequest → {@link SessionQuoteIndex#onQuoteRequest} for per-session correlation, then
 *       {@link FixCommandSink#sendQuoteRequest}, then audit.
 *   <li>AcceptQuote → {@link FixCommandSink#sendAcceptQuote} (the sink consults the per-session
 *       quote cache via the launcher's wiring), then audit.
 *   <li>RejectQuote → {@link FixCommandSink#handleRejectQuote} (no FIX wire activity), then audit.
 *   <li>NewOrderSingle → {@link FixCommandSink#sendNewOrderSingle}, then audit.
 *   <li>CancelOrder → {@link FixCommandSink#sendCancelOrder}, then audit.
 *   <li>OrderStatusRequest → {@link FixCommandSink#sendOrderStatusRequest} (projection-side, no
 *       FIX), then audit.
 * </ol>
 *
 * <p>The dispatcher does NOT perform per-type rate limiting — that's already done by {@link
 * WsListener} before it invokes the dispatcher. The dispatcher does NOT perform any session-state
 * mutation beyond the quote-index update for QuoteRequest; everything else is stateless routing.
 *
 * <p><b>Threading.</b> Per-session instance, owned by the channel's Netty event loop. Not
 * thread-safe.
 *
 * <p><b>Allocation.</b> Zero on the hot dispatch path <i>when audit is disabled</i> (the default
 * {@link AuditLogger.Noop} short-circuits via {@link AuditLogger#isWritable}). With audit enabled
 * the dispatcher allocates a small set of {@link String} slices per audited command: one for {@code
 * reqId} on QuoteRequest (the {@link SessionQuoteIndex#onQuoteRequest} key copy — sessions reqIds
 * need a stable hash key) and up to six in {@link #audit} for
 * symbol/clOrdId/origClOrdId/quoteId/account/traceparent. Sub/jti come from immutable session state
 * — no copy. The audit allocations are the documented price for a regulator-grade audit trail and
 * only fire when the launcher's eventual Log4j2 binding flips {@link AuditLogger#isWritable} to
 * {@code true}.
 */
public final class RoutingBridgeFrameDispatcher implements BridgeFrameDispatcher {

  private final FixCommandSink sink;
  private final SessionQuoteIndex quoteIndex;
  private final AuditLogger auditLogger;
  private final EpochNanoClock epochNanoClock;
  private final String remoteIp;

  /**
   * Construct a routing dispatcher.
   *
   * @param sink the FIX wire-send seam — provided by the launcher
   * @param quoteIndex the per-process session-quote correlation index (§3.2)
   * @param auditLogger audit sink — the dispatcher records action-level entries on every routed
   *     command
   * @param epochNanoClock wall-clock used as the {@code tsNs} for {@link AuditLogger#record} — the
   *     audit-logger contract is epoch nanoseconds (not monotonic), so audit records correlate with
   *     wall-clock incident timelines
   * @param remoteIp the remote IP captured at handshake (used as the {@code sourceIp} field on
   *     every audit entry)
   */
  public RoutingBridgeFrameDispatcher(
      final FixCommandSink sink,
      final SessionQuoteIndex quoteIndex,
      final AuditLogger auditLogger,
      final EpochNanoClock epochNanoClock,
      final String remoteIp) {
    this.sink = Objects.requireNonNull(sink, "sink");
    this.quoteIndex = Objects.requireNonNull(quoteIndex, "quoteIndex");
    this.auditLogger = Objects.requireNonNull(auditLogger, "auditLogger");
    this.epochNanoClock = Objects.requireNonNull(epochNanoClock, "epochNanoClock");
    this.remoteIp = Objects.requireNonNull(remoteIp, "remoteIp");
  }

  @Override
  public void dispatch(
      final BridgeSession session,
      final MutableParsedMessage parsed,
      final int messageType,
      final long nowNs) {

    switch (messageType) {
      case MutableParsedMessage.TYPE_QUOTE_REQUEST -> dispatchQuoteRequest(session, parsed, nowNs);
      case MutableParsedMessage.TYPE_ACCEPT_QUOTE -> dispatchAcceptQuote(session, parsed, nowNs);
      case MutableParsedMessage.TYPE_REJECT_QUOTE -> dispatchRejectQuote(session, parsed, nowNs);
      case MutableParsedMessage.TYPE_NEW_ORDER_SINGLE ->
          dispatchNewOrderSingle(session, parsed, nowNs);
      case MutableParsedMessage.TYPE_CANCEL_ORDER -> dispatchCancelOrder(session, parsed, nowNs);
      case MutableParsedMessage.TYPE_ORDER_STATUS_REQUEST ->
          dispatchOrderStatusRequest(session, parsed, nowNs);
      default -> {
        // TYPE_AUTH is consumed by JwtAuthHandler before WsListener installs the post-auth
        // pipeline; receiving it here means a programming error in the listener wiring.
        // TYPE_NONE means the listener invoked dispatch on an empty parse — same.
      }
    }
  }

  private void dispatchQuoteRequest(
      final BridgeSession session, final MutableParsedMessage parsed, final long nowNs) {
    // Update the cross-session correlation index FIRST so a Quote arriving back on a different
    // worker thread can find the originating session even if the FIX send race-loses.
    if (parsed.reqIdOff >= 0 && parsed.reqIdLen > 0) {
      // The flyweight slice is borrow-only; the index needs a stable key, so it copies internally.
      // (SessionQuoteIndex was built to expect this contract.)
      final var reqId = new String(parsed.scratch, parsed.reqIdOff, parsed.reqIdLen);
      quoteIndex.onQuoteRequest(reqId, session.sessionId(), nowNs);
    }
    sink.sendQuoteRequest(parsed, nowNs);
    audit(session, AuditAction.QUOTE_REQUEST_RECEIVED, parsed, nowNs);
  }

  private void dispatchAcceptQuote(
      final BridgeSession session, final MutableParsedMessage parsed, final long nowNs) {
    sink.sendAcceptQuote(parsed, nowNs);
    audit(session, AuditAction.ACCEPT_QUOTE_RECEIVED, parsed, nowNs);
  }

  private void dispatchRejectQuote(
      final BridgeSession session, final MutableParsedMessage parsed, final long nowNs) {
    sink.handleRejectQuote(parsed, nowNs);
    audit(session, AuditAction.REJECT_QUOTE_RECEIVED, parsed, nowNs);
  }

  private void dispatchNewOrderSingle(
      final BridgeSession session, final MutableParsedMessage parsed, final long nowNs) {
    sink.sendNewOrderSingle(parsed, nowNs);
    audit(session, AuditAction.NEW_ORDER_RECEIVED, parsed, nowNs);
  }

  private void dispatchCancelOrder(
      final BridgeSession session, final MutableParsedMessage parsed, final long nowNs) {
    sink.sendCancelOrder(parsed, nowNs);
    audit(session, AuditAction.CANCEL_ORDER_RECEIVED, parsed, nowNs);
  }

  private void dispatchOrderStatusRequest(
      final BridgeSession session, final MutableParsedMessage parsed, final long nowNs) {
    sink.sendOrderStatusRequest(parsed, nowNs);
    audit(session, AuditAction.ORDER_STATUS_REQUEST, parsed, nowNs);
  }

  /**
   * Emit one audit entry for the routed command. The audit fields are sliced from {@code parsed}
   * via {@link AuditLogger#isWritable()}-gated calls so the {@link AuditLogger.Noop} default pays
   * only one volatile read per dispatch.
   */
  private void audit(
      final BridgeSession session,
      final AuditAction action,
      final MutableParsedMessage parsed,
      final long nowNs) {
    if (!auditLogger.isWritable()) {
      return;
    }
    final var symbol = sliceOrNull(parsed.scratch, parsed.symbolOff, parsed.symbolLen);
    final var clOrdId = sliceOrNull(parsed.scratch, parsed.clOrdIdOff, parsed.clOrdIdLen);
    final var origClOrdId =
        sliceOrNull(parsed.scratch, parsed.origClOrdIdOff, parsed.origClOrdIdLen);
    final var quoteId = sliceOrNull(parsed.scratch, parsed.quoteIdOff, parsed.quoteIdLen);
    final var account = sliceOrNull(parsed.scratch, parsed.accountOff, parsed.accountLen);
    final var traceparent =
        sliceOrNull(parsed.scratch, parsed.traceparentOff, parsed.traceparentLen);
    // tsNs uses the EpochNanoClock (wall-clock nanoseconds) — AuditLogger.record's contract is
    // epoch-ns so audit entries correlate with wall-clock incident timelines. The nowNs param
    // remains a monotonic dispatch timestamp from the listener and is unused here.
    auditLogger.record(
        epochNanoClock.nanoTime(),
        session.claims().sub(),
        session.claims().jti(),
        remoteIp,
        action,
        symbol,
        sideStringOrNull(parsed.side),
        null,
        null,
        ordTypeStringOrNull(parsed.ordType),
        tifStringOrNull(parsed.timeInForce),
        account,
        clOrdId,
        origClOrdId,
        quoteId,
        "received",
        null,
        traceparent);
  }

  private static String sliceOrNull(final byte[] buf, final int off, final int len) {
    if (off < 0 || len <= 0) {
      return null;
    }
    return new String(buf, off, len);
  }

  private static String sideStringOrNull(final byte side) {
    if (side == MutableParsedMessage.SIDE_BUY) {
      return "Buy";
    }
    if (side == MutableParsedMessage.SIDE_SELL) {
      return "Sell";
    }
    return null;
  }

  private static String ordTypeStringOrNull(final byte ordType) {
    if (ordType == MutableParsedMessage.ORDTYPE_MARKET) {
      return "Market";
    }
    if (ordType == MutableParsedMessage.ORDTYPE_LIMIT) {
      return "Limit";
    }
    if (ordType == MutableParsedMessage.ORDTYPE_PREVIOUSLY_QUOTED) {
      return "PreviouslyQuoted";
    }
    return null;
  }

  private static String tifStringOrNull(final byte tif) {
    return switch (tif) {
      case MutableParsedMessage.TIF_DAY -> "DAY";
      case MutableParsedMessage.TIF_GTC -> "GTC";
      case MutableParsedMessage.TIF_IOC -> "IOC";
      case MutableParsedMessage.TIF_FOK -> "FOK";
      case MutableParsedMessage.TIF_GTD -> "GTD";
      default -> null;
    };
  }
}
