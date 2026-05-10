package com.trading.engine.fixbridge.audit;

/**
 * Tamper-evident audit-event sink for the fix-client-bridge.
 *
 * <p><b>Purpose.</b> Single emit point for every regulated security/order action observed by the
 * bridge: auth lifecycle, command receipt, kill-switch presses, rate-limit hits, quote orphans,
 * session terminations, and {@code RawFix} debug-flag toggles. APP-40a defines the interface and
 * ships a no-op default ({@link Noop}) so bridge dispatch code compiles. APP-40b binds the real
 * Log4j2-async implementation ({@code Log4jAuditLogger}) which writes the JSONL hash chain, fans
 * out via TLS-syslog, and trips the §3.7 two-stage circuit breaker.
 *
 * <p><b>Threading.</b> Implementations MUST be safe for concurrent invocation by every Netty I/O
 * thread; the production binding uses a Log4j2 async appender backed by an LMAX Disruptor.
 *
 * <p><b>Allocation.</b> The interface contract is non-allocating on the hot dispatch path —
 * implementations MUST format and serialise without touching the heap. The {@link Noop}
 * implementation is genuinely zero-alloc; the Log4j2 async binding writes via a pre-built ASCII
 * pattern.
 *
 * <p><b>Lifecycle.</b> One instance per bridge process, injected at launcher boot.
 *
 * <p><b>Dependencies.</b> JDK only on the interface side; the production binding pulls in Log4j2.
 *
 * <p><b>Action taxonomy.</b> See {@link AuditAction}. Bridge-side actions are scoped strictly to
 * events the bridge directly observes; cluster-side actions ({@code pre_trade_block}, {@code
 * quote_validation_failure}, {@code order_validation_failure}) live in a separate cluster audit
 * channel and are NOT routed through this interface.
 *
 * @see AuditAction
 */
public interface AuditLogger {

  /**
   * Record a single audit event. Implementations MUST NOT block the calling thread on I/O —
   * production binding uses an async appender; failures are escalated through the §3.7 circuit
   * breaker rather than surfacing exceptions to the caller.
   *
   * <p>The string fields ({@code userId}, {@code jti}, etc.) MUST be pre-sanitised by the caller —
   * the implementation's pattern layout assumes ASCII-safe input. Newline characters in
   * user-supplied fields would break JSONL parsability and are rejected by the production binding
   * via {@link #escapeUserField(CharSequence)} (or its production equivalent).
   *
   * @param tsNs event timestamp, epoch nanoseconds (sourced from the bridge's injected {@code
   *     EpochNanoClock})
   * @param userId JWT {@code sub} claim of the session originating the event, or {@code null} for
   *     pre-auth events ({@code auth_fail}, {@code auth_timeout}, {@code frame_oversized_drop})
   * @param jti JWT {@code jti} claim, or {@code null} for pre-auth events
   * @param sourceIp remote IP address of the WebSocket peer (e.g. {@code "127.0.0.1"})
   * @param action one of the bridge-side {@link AuditAction} values
   * @param symbol FIX {@code Symbol (55)} associated with the action, or {@code null} if N/A
   * @param side FIX {@code Side (54)} ({@code "Buy"}/{@code "Sell"}), or {@code null}
   * @param qty fixed-point quantity at scale 10^-8 ({@link
   *     com.trading.engine.messages.FixedPointScale#PRICE_SCALE}); use 0L when N/A
   * @param price fixed-point price at scale 10^-8; use 0L when N/A
   * @param ordType FIX {@code OrdType (40)} string, or {@code null}
   * @param tif FIX {@code TimeInForce (59)} string, or {@code null}
   * @param account FIX {@code Account (1)} string, or {@code null}
   * @param clOrdId FIX {@code ClOrdID (11)} of the order action, or {@code null}
   * @param origClOrdId FIX {@code OrigClOrdID (41)} of a cancel target, or {@code null}
   * @param quoteId quote correlation id, or {@code null}
   * @param result short-form result tag (e.g. {@code "ok"}, {@code "rejected"}, {@code
   *     "throttled"})
   * @param failureReason taxonomy reason string for non-{@code ok} results, or {@code null}
   * @param traceparent W3C trace-context string ({@code 55} chars) sourced from the inbound {@code
   *     _meta.traceparent} field, or {@code null} when not present
   */
  void record(
      long tsNs,
      String userId,
      String jti,
      String sourceIp,
      AuditAction action,
      String symbol,
      String side,
      long qty,
      long price,
      String ordType,
      String tif,
      String account,
      String clOrdId,
      String origClOrdId,
      String quoteId,
      String result,
      String failureReason,
      String traceparent);

  /**
   * Returns whether the underlying audit sink is currently writable. Implementations SHOULD answer
   * in {@code O(1)} (cached health probe) — the Netty event loop polls this from the {@link
   * com.trading.engine.fixbridge.audit.AuditLogger} contract perspective. A {@code false} answer
   * triggers the §3.7 stage-1 warning {@code BridgeStatus{fatal:false,reason:"audit-degraded"}}
   * fan-out from the bridge's status emitter.
   *
   * @return {@code true} if writes have succeeded within the operational window (10s for the
   *     production binding); {@code false} otherwise
   */
  boolean isWritable();

  // ---------------------------------------------------------------------------
  // No-op default — used in APP-40a until APP-40b binds Log4jAuditLogger.
  // ---------------------------------------------------------------------------

  /**
   * Zero-allocation no-op implementation. Used by APP-40a so the bridge dispatcher can compile and
   * pass tests without depending on the (heavy) Log4j2 async appender + hash-chain machinery that
   * lands in APP-40b. The production binding replaces this in {@code FixClientBridgeLauncher}.
   *
   * <p><b>Threading.</b> Stateless and immutable; safe for concurrent use.
   *
   * <p><b>Allocation.</b> Zero on every call.
   *
   * <p><b>Lifecycle.</b> Singleton via {@link #INSTANCE}.
   */
  final class Noop implements AuditLogger {

    /** Process-wide singleton. Use this rather than instantiating. */
    public static final Noop INSTANCE = new Noop();

    private Noop() {}

    @Override
    public void record(
        final long tsNs,
        final String userId,
        final String jti,
        final String sourceIp,
        final AuditAction action,
        final String symbol,
        final String side,
        final long qty,
        final long price,
        final String ordType,
        final String tif,
        final String account,
        final String clOrdId,
        final String origClOrdId,
        final String quoteId,
        final String result,
        final String failureReason,
        final String traceparent) {
      // No-op by design — APP-40a compile-time stub.
    }

    @Override
    public boolean isWritable() {
      // Always reports writable; the §3.7 circuit breaker stays armed once Log4jAuditLogger lands.
      return true;
    }
  }
}
