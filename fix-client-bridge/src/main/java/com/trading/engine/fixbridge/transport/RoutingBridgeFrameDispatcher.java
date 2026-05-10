package com.trading.engine.fixbridge.transport;

import com.trading.engine.fixbridge.audit.AuditAction;
import com.trading.engine.fixbridge.audit.AuditLogger;
import com.trading.engine.fixbridge.json.BrowserEvent;
import com.trading.engine.fixbridge.json.MutableParsedMessage;
import com.trading.engine.fixbridge.json.OrderRejectReason;
import com.trading.engine.fixbridge.quote.SessionQuoteIndex;
import com.trading.engine.fixbridge.quote.SessionQuoteIndex.QuoteRequestRegistration;
import java.util.Objects;
import java.util.function.Supplier;
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
 * <p><b>Allocation.</b> Two distinct allocation profiles:
 *
 * <ul>
 *   <li><b>QuoteRequest path</b>: ALWAYS allocates one {@link String} per call (the {@code reqId}
 *       slice copied for the {@link SessionQuoteIndex#onQuoteRequest} hash key). This is
 *       unavoidable until the index API accepts {@code (byte[], off, len)} directly; it fires
 *       regardless of audit-logger state because the index update is functional, not audit-only.
 *   <li><b>All other dispatch paths</b>: zero allocation when audit is disabled (the default {@link
 *       AuditLogger.Noop} short-circuits via {@link AuditLogger#isWritable}). With audit enabled
 *       the dispatcher allocates up to six {@link String} slices per audited command in {@link
 *       #audit} for symbol/clOrdId/origClOrdId/quoteId/account/traceparent. Sub/jti come from
 *       immutable session state — no copy. The audit allocations are the documented price for a
 *       regulator-grade audit trail and only fire when the launcher's eventual Log4j2 binding flips
 *       {@link AuditLogger#isWritable} to {@code true}.
 * </ul>
 */
public final class RoutingBridgeFrameDispatcher implements BridgeFrameDispatcher {

  private final FixCommandSink sink;
  private final SessionQuoteIndex quoteIndex;
  private final AuditLogger auditLogger;
  private final EpochNanoClock epochNanoClock;
  private final Supplier<String> remoteIpSupplier;

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
   * @param remoteIpSupplier supplies the current remote IP — invoked once per audited dispatch so
   *     audit entries reflect the current peer IP rather than a stale handshake-time snapshot.
   *     Implementations MUST cache to avoid per-call String allocation (the launcher's binding
   *     wraps the per-channel IP-pin enforcer's pre-resolved String reference).
   */
  public RoutingBridgeFrameDispatcher(
      final FixCommandSink sink,
      final SessionQuoteIndex quoteIndex,
      final AuditLogger auditLogger,
      final EpochNanoClock epochNanoClock,
      final Supplier<String> remoteIpSupplier) {
    this.sink = Objects.requireNonNull(sink, "sink");
    this.quoteIndex = Objects.requireNonNull(quoteIndex, "quoteIndex");
    this.auditLogger = Objects.requireNonNull(auditLogger, "auditLogger");
    this.epochNanoClock = Objects.requireNonNull(epochNanoClock, "epochNanoClock");
    this.remoteIpSupplier = Objects.requireNonNull(remoteIpSupplier, "remoteIpSupplier");
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
      final var registration = quoteIndex.onQuoteRequest(reqId, session.sessionId(), nowNs);
      if (registration == QuoteRequestRegistration.DUPLICATE_REQID) {
        // §3.2: same (reqId, sessionId) inside the dedupe window → reject without forwarding to
        // FIX. Surfaced as Error{reason:"duplicate-reqId", received:"QuoteRequest:<reqId>"} so the
        // browser can correlate (QuoteRequest carries no clOrdId — Error is the right vehicle vs
        // OrderReject which is clOrdId-scoped).
        session.enqueue(
            new BrowserEvent.Error(
                OrderRejectReason.DUPLICATE_REQID.wireValue(), "QuoteRequest:" + reqId));
        audit(session, AuditAction.QUOTE_REQUEST_RECEIVED, parsed, nowNs);
        return;
      }
    }
    sink.sendQuoteRequest(parsed, nowNs);
    audit(session, AuditAction.QUOTE_REQUEST_RECEIVED, parsed, nowNs);
  }

  private void dispatchAcceptQuote(
      final BridgeSession session, final MutableParsedMessage parsed, final long nowNs) {
    if (!isQuoteOwnedByCurrentSession(session, parsed)) {
      // §3.2: cross-session quote-id steal. The owning session is either (a) gone, or (b) a
      // different live session of the same/another sub. Either way the bridge MUST NOT forward to
      // FIX — the trader who actually saw the quote could have a different intent. Reject with
      // OrderReject{clOrdId, reason:"quote-not-owned"}.
      emitQuoteNotOwnedReject(session, parsed);
      audit(session, AuditAction.ACCEPT_QUOTE_RECEIVED, parsed, nowNs);
      return;
    }
    sink.sendAcceptQuote(parsed, nowNs);
    audit(session, AuditAction.ACCEPT_QUOTE_RECEIVED, parsed, nowNs);
  }

  private void dispatchRejectQuote(
      final BridgeSession session, final MutableParsedMessage parsed, final long nowNs) {
    if (!isQuoteOwnedByCurrentSession(session, parsed)) {
      // §3.2: same protection as AcceptQuote — only the originating session can reject its own
      // quote. Forwarding a stranger's RejectQuote would let one tab evict another tab's pending
      // quote silently.
      emitQuoteNotOwnedReject(session, parsed);
      audit(session, AuditAction.REJECT_QUOTE_RECEIVED, parsed, nowNs);
      return;
    }
    sink.handleRejectQuote(parsed, nowNs);
    audit(session, AuditAction.REJECT_QUOTE_RECEIVED, parsed, nowNs);
  }

  /**
   * §3.2 ownership check. Returns {@code true} when {@code parsed.quoteId} is bound to the current
   * session in {@link SessionQuoteIndex#isOwnedBy}. Allocates one {@link String} per call (the
   * quoteId slice copy) regardless of audit state — same constraint as the QuoteRequest path
   * (cross-session correlation needs a stable key). Returns {@code true} when the quoteId slice is
   * absent so a malformed-but-quoteId-less inbound still falls through to the sink, where
   * downstream validation (parser strictness) will surface the error appropriately.
   */
  private boolean isQuoteOwnedByCurrentSession(
      final BridgeSession session, final MutableParsedMessage parsed) {
    if (parsed.quoteIdOff < 0 || parsed.quoteIdLen <= 0) {
      return true;
    }
    final var quoteId = new String(parsed.scratch, parsed.quoteIdOff, parsed.quoteIdLen);
    return quoteIndex.isOwnedBy(quoteId, session.sessionId());
  }

  /**
   * Build + enqueue {@link BrowserEvent.OrderReject} with reason {@code QUOTE_NOT_OWNED} for the
   * current AcceptQuote/RejectQuote frame. Uses the parsed clOrdId slice when present (AcceptQuote
   * carries one; RejectQuote does not, in which case the empty string is used as a placeholder
   * matching the OrderReject record's non-null clOrdId contract).
   */
  private void emitQuoteNotOwnedReject(
      final BridgeSession session, final MutableParsedMessage parsed) {
    final var clOrdId =
        parsed.clOrdIdOff >= 0 && parsed.clOrdIdLen > 0
            ? new String(parsed.scratch, parsed.clOrdIdOff, parsed.clOrdIdLen)
            : "";
    session.enqueue(new BrowserEvent.OrderReject(clOrdId, OrderRejectReason.QUOTE_NOT_OWNED));
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
    // qty: parsed.qty carries the eagerly-decoded fixed-point value (Long.MIN_VALUE when absent);
    // normalise the absent sentinel to 0L per the AuditLogger#record contract. price: the parsed
    // flyweight retains only the priceOff/priceLen ASCII slice (the wire avoids double-rounding
    // through int64); routing audit entries pass 0L because surfacing the parsed price would
    // require a fresh DecimalStringParser invocation on every dispatch — TODO(APP-40b): wire a
    // shared zero-alloc parser if compliance demands the priced field on the audit row.
    final long qty = parsed.qty == Long.MIN_VALUE ? 0L : parsed.qty;
    auditLogger.record(
        epochNanoClock.nanoTime(),
        session.claims().sub(),
        session.claims().jti(),
        remoteIpSupplier.get(),
        action,
        symbol,
        sideStringOrNull(parsed.side),
        qty,
        0L,
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
