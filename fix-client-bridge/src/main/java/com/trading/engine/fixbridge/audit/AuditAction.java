package com.trading.engine.fixbridge.audit;

/**
 * Closed taxonomy of bridge-side audit-action types (§3.7). Each constant's {@link #wireValue()}
 * is the canonical string written to the audit JSONL stream.
 *
 * <p><b>Scope.</b> Bridge-side actions ONLY — events the {@code :fix-client-bridge} Netty
 * dispatcher directly observes. Cluster-side actions (e.g. {@code pre_trade_block}, {@code
 * quote_validation_failure}, {@code order_validation_failure}) live in a separate cluster audit
 * channel and are not represented here.
 *
 * <p><b>Threading.</b> Enum constants are immutable; safe to share across threads.
 *
 * <p><b>Allocation.</b> Zero on {@link #wireValue()}.
 *
 * <p><b>Dependencies.</b> JDK only.
 */
public enum AuditAction {

  /** First-frame Auth succeeded — token validated, JTI not revoked, IP-pin set. */
  AUTH_SUCCESS("auth_success"),

  /** First-frame Auth failed — invalid token, expired, JTI revoked, or origin/IP rejected. */
  AUTH_FAIL("auth_fail"),

  /** First Auth frame not received within 5s — channel closed with code 4008 (§B-r2-11). */
  AUTH_TIMEOUT("auth_timeout"),

  /** Inbound {@link com.trading.engine.fixbridge.json.BrowserMessage.QuoteRequest} received. */
  QUOTE_REQUEST_RECEIVED("quote_request_received"),

  /** Outbound {@code Quote} routed to the originating session. */
  QUOTE_EMITTED_TO_SESSION("quote_emitted_to_session"),

  /** Inbound {@link com.trading.engine.fixbridge.json.BrowserMessage.AcceptQuote} received. */
  ACCEPT_QUOTE_RECEIVED("accept_quote_received"),

  /** Inbound {@link com.trading.engine.fixbridge.json.BrowserMessage.RejectQuote} received. */
  REJECT_QUOTE_RECEIVED("reject_quote_received"),

  /** Inbound {@link com.trading.engine.fixbridge.json.BrowserMessage.NewOrderSingleCmd} received. */
  NEW_ORDER_RECEIVED("new_order_received"),

  /** Inbound {@link com.trading.engine.fixbridge.json.BrowserMessage.CancelOrder} received. */
  CANCEL_ORDER_RECEIVED("cancel_order_received"),

  /** Inbound {@link com.trading.engine.fixbridge.json.BrowserMessage.OrderStatusRequest} received. */
  ORDER_STATUS_REQUEST("order_status_request"),

  /** Cluster reconciliation forwarded to the originating session (§4.5 / OrderReconciled). */
  ORDER_RECONCILED("order_reconciled"),

  /** Kill-switch ({@code Ctrl+Shift+K}) pressed in the UI — every press audited. */
  KILL_SWITCH_PRESS("kill_switch_press"),

  /** {@code bridgeDebug} flag flipped (entry into / exit from RawFix emission). */
  BRIDGE_DEBUG_TOGGLE("bridge_debug_toggle"),

  /** Per-type rate-limit token bucket exhausted; command rejected with rate-limit reason. */
  RATE_LIMIT_HIT("rate_limit_hit"),

  /** A Quote could not be routed to its originating session (session gone) — orphan. */
  QUOTE_ORPHANED("quote_orphaned"),

  /**
   * SessionTerminated emitted to other sessions of the same {@code sub} on signOut/JTI-revocation
   * (§3.3, §3.7).
   */
  SESSION_TERMINATED("session_terminated"),

  /** Inbound frame exceeded {@code MAX_BYTES} (64 KiB) — channel closed before parse. */
  FRAME_OVERSIZED_DROP("frame_oversized_drop");

  private final String wireValue;

  AuditAction(final String wireValue) {
    this.wireValue = wireValue;
  }

  /**
   * Canonical wire string emitted to the audit JSONL stream.
   *
   * @return the action name (e.g. {@code "auth_success"})
   */
  public String wireValue() {
    return wireValue;
  }
}
