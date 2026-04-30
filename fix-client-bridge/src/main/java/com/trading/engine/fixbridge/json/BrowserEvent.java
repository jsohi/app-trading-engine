package com.trading.engine.fixbridge.json;

/**
 * Sealed model of the seven outbound bridge-to-browser JSON event kinds.
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
 * Quote            : {"type":"Quote","reqId":"...","quoteId":"...","symbol":"...",
 *                     "side":"Buy|Sell","qty":"&lt;dec&gt;","price":"&lt;dec&gt;",
 *                     "expiryNs":&lt;long&gt;}
 * ExecutionReport  : {"type":"ExecutionReport","clOrdId":"...","execId":"...",
 *                     "execType":"&lt;char&gt;","ordStatus":"&lt;char&gt;","symbol":"...",
 *                     "side":"Buy|Sell","cumQty":"&lt;dec&gt;","leavesQty":"&lt;dec&gt;",
 *                     "avgPx":"&lt;dec&gt;"}
 * OrderReject      : {"type":"OrderReject","clOrdId":"...","reason":"..."}
 * BridgeStatus     : {"type":"BridgeStatus","fixSessionUp":&lt;bool&gt;,"fatal":&lt;bool&gt;,
 *                     "reason":"..."}
 * RawFix           : {"type":"RawFix","direction":"in|out","fix":"..."}
 * AuthExpired      : {"type":"AuthExpired"}
 * Error            : {"type":"Error","reason":"..."}
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
        BrowserEvent.Error {

  /**
   * Inbound dealer quote — broadcast to all authenticated sessions per locked §22 (per-session
   * correlation deferred to follow-up).
   *
   * @param reqId echo of the original {@code QuoteRequest.reqId} so the browser can correlate
   * @param quoteId server-assigned quote identifier
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
   * @param clOrdId originating client order id (always present)
   * @param reason short-form textual reason — must NOT contain raw double-quote / backslash; the
   *     writer copies it verbatim. Wire protocol: pure ASCII, ≤128 bytes.
   */
  record OrderReject(String clOrdId, String reason) implements BrowserEvent {}

  /**
   * FIX session liveness status. {@code fatal=true} bypasses the per-session retain queue (locked
   * §6 — reserved-slot semantics).
   *
   * @param fixSessionUp current Artio session state
   * @param fatal whether this status is terminal — browser should expect channel close
   * @param reason short textual reason (e.g. {@code "shutdown"}, {@code "outbound-overflow"})
   */
  record BridgeStatus(boolean fixSessionUp, boolean fatal, String reason) implements BrowserEvent {}

  /**
   * FIX-tap mirror of an inbound or outbound FIX message (debug only — gated by {@code
   * bridgeDebug}).
   *
   * @param direction either {@code "in"} or {@code "out"}
   * @param fix FIX raw message with {@code SOH} replaced by {@code |}, JSON-escaped where needed
   */
  record RawFix(String direction, String fix) implements BrowserEvent {}

  /** JWT lifetime exhausted — sent immediately before WS close per locked §13. */
  record AuthExpired() implements BrowserEvent {
    /** Singleton instance — there is no per-channel state to carry. */
    public static final AuthExpired INSTANCE = new AuthExpired();
  }

  /**
   * Generic protocol-level error. Used by the inbound dispatcher when a message is unparseable, has
   * an unknown type, or the FIX gateway is unreachable for a non-order command (locked §7).
   *
   * @param reason short textual reason matching {@link JsonParseException#reason()} or one of the
   *     named bridge taxonomy strings ({@code "bridge-down"}, {@code "backpressure"}, etc.)
   */
  record Error(String reason) implements BrowserEvent {}
}
