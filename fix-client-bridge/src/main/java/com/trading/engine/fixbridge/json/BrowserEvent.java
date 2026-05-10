package com.trading.engine.fixbridge.json;

/**
 * Sealed model of the bridge-to-browser JSON event kinds.
 *
 * <p><b>Purpose.</b> Test-facing immutable record types for the outbound wire. Mirrors {@link
 * BrowserMessage} on the inbound side: the production hot path does NOT instantiate these — it uses
 * {@code BrowserEventWriter} which writes JSON-bytes directly into the outbound Netty buffer — but
 * the records exist as the canonical specification of which fields each event carries and are
 * reused by tests as the input fixtures for the byte-exact writer assertions.
 *
 * <p><b>Threading.</b> Records are immutable; safe to share across threads.
 *
 * <p><b>Allocation.</b> Records allocate on construction; never used on the hot path.
 *
 * <p><b>Lifecycle.</b> Per-instance.
 *
 * <p><b>Dependencies.</b> JDK only.
 *
 * <p><b>Wire formats</b> (numerics use the canonical 8-frac-digit form emitted by {@link
 * com.trading.engine.fixbridge.translator.DecimalStringEmitter}):
 *
 * <pre>
 * Quote              : {"type":"Quote","reqId":"...","quoteId":"...","symbol":"...",
 *                       "side":"Buy|Sell","qty":"&lt;dec&gt;","price":"&lt;dec&gt;",
 *                       "expiryNs":&lt;long&gt;}
 * ExecutionReport    : {"type":"ExecutionReport","clOrdId":"...","execId":"...",
 *                       "execType":"&lt;char&gt;","ordStatus":"&lt;char&gt;","symbol":"...",
 *                       "side":"Buy|Sell","cumQty":"&lt;dec&gt;","leavesQty":"&lt;dec&gt;",
 *                       "avgPx":"&lt;dec&gt;"}
 * OrderReject        : {"type":"OrderReject","clOrdId":"...","reason":"..."}
 * BridgeStatus       : {"type":"BridgeStatus","fixSessionUp":&lt;bool&gt;,"fatal":&lt;bool&gt;,
 *                       "reason":"...","newOrders":&lt;bool&gt;,"newQuotes":&lt;bool&gt;,
 *                       "protocolVersion":&lt;int&gt;,"serverOrderTimeoutMs":&lt;long&gt;}
 * RawFix             : {"type":"RawFix","direction":"in|out","fix":"..."}
 * AuthExpired        : {"type":"AuthExpired"}
 * Error              : {"type":"Error","reason":"...","received":"..."}
 * AccountLimits      : {"type":"AccountLimits","account":"...",
 *                       "maxQty":"&lt;dec&gt;","maxNotional":"&lt;dec&gt;",
 *                       "priceDeviationBps":&lt;int&gt;,
 *                       "maxOrdersPerSecond":&lt;int&gt;}
 * SessionTerminated  : {"type":"SessionTerminated"}
 * OrderReconciled    : {"type":"OrderReconciled","clOrdId":"...","status":"...",
 *                       "cumQty":"&lt;dec&gt;","leavesQty":"&lt;dec&gt;","avgPx":"&lt;dec&gt;"}
 * OrderStatusReply   : {"type":"OrderStatusReply","clOrdId":"...","status":"...",
 *                       "cumQty":"&lt;dec&gt;","leavesQty":"&lt;dec&gt;","avgPx":"&lt;dec&gt;",
 *                       "lastExecId":"..."}
 * </pre>
 *
 * <p>{@code expiryNs} on {@code Quote} is an unwrapped JSON integer (epoch nanoseconds) — clients
 * compare it against their own monotonic-shifted clock; the bridge is the authoritative source.
 *
 * <p>Numeric fields are JSON strings so that fixed-point precision survives clients that decode via
 * {@code JSON.parse} into IEEE-754 doubles.
 */
public sealed interface BrowserEvent
    permits BrowserEvent.Quote,
        BrowserEvent.ExecutionReport,
        BrowserEvent.OrderReject,
        BrowserEvent.BridgeStatus,
        BrowserEvent.RawFix,
        BrowserEvent.AuthExpired,
        BrowserEvent.Error,
        BrowserEvent.AccountLimits,
        BrowserEvent.SessionTerminated,
        BrowserEvent.OrderReconciled,
        BrowserEvent.OrderStatusReply {

  /**
   * Inbound dealer quote — routed per-session via {@code reqId → sessionId} (§3.2).
   *
   * @param reqId echo of the original {@code QuoteRequest.reqId} so the browser can correlate
   * @param quoteId server-assigned quote identifier (globally unique post-restart per §3.2)
   * @param symbol FIX {@code Symbol (55)}
   * @param side FIX {@code Side (54)} — {@code "Buy"} or {@code "Sell"}
   * @param qtyInt64 quantity, fixed-point int64 (scale {@code 10^-8})
   * @param priceInt64 price, fixed-point int64 (scale {@code 10^-8})
   * @param expiryNs absolute epoch-nanosecond TTL after which the bridge will not honour an {@code
   *     AcceptQuote}
   */
  record Quote(
      String reqId,
      String quoteId,
      String symbol,
      String side,
      long qtyInt64,
      long priceInt64,
      long expiryNs)
      implements BrowserEvent {}

  /**
   * FIX {@code ExecutionReport (35=8)} forwarded to the browser.
   *
   * @param clOrdId originating client order id
   * @param execId server-assigned execution id
   * @param execType FIX {@code ExecType (150)} char (single ASCII byte)
   * @param ordStatus FIX {@code OrdStatus (39)} char
   * @param symbol FIX {@code Symbol (55)}
   * @param side FIX {@code Side (54)} — {@code "Buy"} or {@code "Sell"}
   * @param cumQtyInt64 cumulative filled quantity, fixed-point int64
   * @param leavesQtyInt64 remaining open quantity, fixed-point int64
   * @param avgPxInt64 average fill price, fixed-point int64
   */
  record ExecutionReport(
      String clOrdId,
      String execId,
      char execType,
      char ordStatus,
      String symbol,
      String side,
      long cumQtyInt64,
      long leavesQtyInt64,
      long avgPxInt64)
      implements BrowserEvent {}

  /**
   * Negative reply for any of: cancel-rejected, backpressure, bridge-down, quote-expired (locked
   * §5, §7, §11).
   *
   * <p>The {@code reason} field is the wire-format string emitted in JSON. Production callers
   * SHOULD construct via the {@link #OrderReject(String, OrderRejectReason)} typed convenience
   * constructor so the closed taxonomy in {@link OrderRejectReason} is enforced at compile time.
   * The string-only constructor remains for forward-compat with existing tests; the writer asserts
   * taxonomy membership at write time when {@code asserted} mode is enabled.
   *
   * @param clOrdId originating client order id (always present)
   * @param reason short-form textual reason — must NOT contain raw double-quote / backslash; the
   *     writer copies it verbatim. Wire protocol: pure ASCII, ≤128 bytes. Production code MUST pick
   *     from {@link OrderRejectReason}.
   */
  record OrderReject(String clOrdId, String reason) implements BrowserEvent {

    /**
     * Typed convenience constructor enforcing the closed taxonomy. Equivalent to {@code new
     * OrderReject(clOrdId, reason.wireValue())}.
     *
     * @param clOrdId originating client order id
     * @param reason taxonomy enum value
     */
    public OrderReject(final String clOrdId, final OrderRejectReason reason) {
      this(clOrdId, reason.wireValue());
    }
  }

  /**
   * FIX session liveness + bridge feature-gate snapshot. Sent on AUTHENTICATED and on any change.
   *
   * <p>The {@code newOrders} / {@code newQuotes} fields are the runtime kill-switch (§4.19); {@code
   * protocolVersion} is the server-side bridge protocol version (§4.1 worker checks it against
   * {@code MIN_SERVER_PROTOCOL_VERSION}); {@code serverOrderTimeoutMs} is the cluster's
   * order-state-timeout, used by the UI's STUCK_LONG transition (§4.5).
   *
   * @param fixSessionUp current Artio session state
   * @param fatal whether this status is terminal — browser should expect channel close
   * @param reason short textual reason (e.g. {@code "shutdown"}, {@code "outbound-overflow"},
   *     {@code "audit-degraded"}, {@code "audit-unavailable"})
   * @param newOrders feature gate — if {@code false}, UI must disable order entry
   * @param newQuotes feature gate — if {@code false}, UI must disable quote requests
   * @param protocolVersion server-side bridge protocol version (≥ {@code 1})
   * @param serverOrderTimeoutMs cluster's order TTL in milliseconds, used to bound STUCK_LONG
   */
  record BridgeStatus(
      boolean fixSessionUp,
      boolean fatal,
      String reason,
      boolean newOrders,
      boolean newQuotes,
      int protocolVersion,
      long serverOrderTimeoutMs)
      implements BrowserEvent {

    /**
     * Backwards-compat constructor — pre-extension callers omit feature gates and protocol
     * metadata. Defaults: {@code newOrders=true}, {@code newQuotes=true}, {@code
     * protocolVersion=1}, {@code serverOrderTimeoutMs=0L} (signals "use UI default"). Migration
     * path for existing call sites until they switch to the seven-arg form.
     *
     * @param fixSessionUp current Artio session state
     * @param fatal whether this status is terminal
     * @param reason short textual reason
     */
    public BridgeStatus(final boolean fixSessionUp, final boolean fatal, final String reason) {
      this(fixSessionUp, fatal, reason, true, true, 1, 0L);
    }
  }

  /**
   * FIX-tap mirror of an inbound or outbound FIX message (debug only — gated by {@code
   * bridgeDebug}).
   *
   * @param direction either {@code "in"} or {@code "out"}
   * @param fix FIX raw message with {@code SOH} replaced by {@code |}, JSON-escaped where needed
   */
  record RawFix(String direction, String fix) implements BrowserEvent {}

  /** JWT lifetime exhausted — sent immediately before WS close (code 4001) per locked §13. */
  record AuthExpired() implements BrowserEvent {
    /** Singleton instance — there is no per-channel state to carry. */
    public static final AuthExpired INSTANCE = new AuthExpired();
  }

  /**
   * Generic protocol-level error. Used by the inbound dispatcher when a message is unparseable, has
   * an unknown type, or the FIX gateway is unreachable for a non-order command (locked §7). Also
   * used to forward FIX {@code QuoteRequestReject} / {@code BusinessMessageReject} / session-level
   * {@code Reject} (locked §17), in which case {@link #received} carries the inbound-message
   * context (e.g. {@code "QuoteRequest:<reqId>"}) so the browser can correlate.
   *
   * <p>Also used for the {@code auth-timeout} signal sent immediately before WS close (code 4008)
   * when the first {@code Auth} frame is not received within 5s (§3.3 / §B-r2-11), and for the
   * {@code quote-orphaned} signal routed to the oldest-connected surviving session of the same
   * {@code sub} when a Quote arrives for a session that has gone away (§3.2).
   *
   * @param reason short textual reason matching {@link JsonParseException#reason()} or one of the
   *     named bridge taxonomy strings ({@code "bridge-down"}, {@code "backpressure"}, {@code
   *     "auth-timeout"}, {@code "quote-orphaned"}, {@code "malformed-traceparent"}, etc.)
   * @param received nullable correlation hint identifying the inbound message that triggered this
   *     error (e.g. {@code "QuoteRequest:R-7"}). Present iff the writer needs to round-trip an
   *     inbound id; absent ({@code null}) for taxonomy-only errors that do not reference a specific
   *     prior inbound. The writer omits the JSON {@code "received"} key entirely when null.
   */
  record Error(String reason, String received) implements BrowserEvent {

    /**
     * Convenience constructor for taxonomy-only errors with no {@code received} correlation hint.
     *
     * @param reason short textual reason
     */
    public Error(final String reason) {
      this(reason, null);
    }
  }

  /**
   * Per-session pre-trade limits pushed by the cluster on AUTHENTICATED transition and on any
   * change. Sourced from the existing {@code AccountStore} (§3.14). The UI gates submit buttons
   * against these values; the server is authoritative.
   *
   * @param account FIX {@code Account (1)} this set of limits applies to
   * @param maxQtyInt64 max single-order quantity, fixed-point int64
   * @param maxNotionalInt64 max single-order notional, fixed-point int64
   * @param priceDeviationBps max allowed price deviation from last, in basis points
   * @param maxOrdersPerSecond per-account command rate limit (token bucket, sustained rate)
   */
  record AccountLimits(
      String account,
      long maxQtyInt64,
      long maxNotionalInt64,
      int priceDeviationBps,
      int maxOrdersPerSecond)
      implements BrowserEvent {}

  /**
   * Sent by the bridge to all-other sessions of a {@code sub} when {@code signOut()} fires on one
   * session (§3.3 / §3.7 / §4.9). Receiving sessions transition the worker to {@code
   * DOWN_REQUIRES_USER_ACTION} and surface a "Signed out in another tab" passive notice. Sent
   * immediately before WS close with code 4002.
   */
  record SessionTerminated() implements BrowserEvent {
    /** Singleton instance — there is no per-channel state to carry. */
    public static final SessionTerminated INSTANCE = new SessionTerminated();
  }

  /**
   * Cluster-driven reconciliation event for an order whose UI state has been STUCK or STUCK_LONG
   * (§4.5). Emitted by {@code :cluster} when the order's TTL fires and the bridge forwards to the
   * originating session. Carries the cluster-authoritative final state so the UI can clear the
   * sticky banner and update the row.
   *
   * @param clOrdId originating client order id
   * @param status one of {@code "Filled" | "PartiallyFilled" | "Cancelled" | "Rejected" | "Working"
   *     | "Unknown"}
   * @param cumQtyInt64 cumulative filled quantity, fixed-point int64
   * @param leavesQtyInt64 remaining open quantity, fixed-point int64
   * @param avgPxInt64 average fill price, fixed-point int64
   */
  record OrderReconciled(
      String clOrdId, String status, long cumQtyInt64, long leavesQtyInt64, long avgPxInt64)
      implements BrowserEvent {}

  /**
   * Reply to an inbound {@code OrderStatusRequest} (§3.15). Synthesised by the bridge from the
   * cluster's {@code OrderQueryByClOrdId} projection result.
   *
   * @param clOrdId originating client order id (echo of the request)
   * @param status one of {@code "Filled" | "PartiallyFilled" | "Cancelled" | "Rejected" | "Working"
   *     | "Unknown"}; {@code "Unknown"} is emitted on the 5s server-side timeout
   * @param cumQtyInt64 cumulative filled quantity, fixed-point int64; meaningful for non-{@code
   *     Unknown} statuses
   * @param leavesQtyInt64 remaining open quantity, fixed-point int64
   * @param avgPxInt64 average fill price, fixed-point int64
   * @param lastExecId most recent execution id known to the cluster, or {@code null} if the order
   *     has no executions
   */
  record OrderStatusReply(
      String clOrdId,
      String status,
      long cumQtyInt64,
      long leavesQtyInt64,
      long avgPxInt64,
      String lastExecId)
      implements BrowserEvent {}
}
