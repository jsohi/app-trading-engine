package com.trading.engine.cluster.handler;

/**
 * Single-method seam for routing per-session activity into the cluster's per-session metric maps
 * (APP-151 phase 5). The production implementation is {@link NewOrderSingleHandler} — which owns
 * all five {@code sessionMetric*} maps and the close-time GFLog summary path — but the interface
 * exists so peer handlers (currently {@link QuoteRequestHandler}; future cancel/replace,
 * admin-cancel, fill handlers) depend on a small contract instead of the concrete class.
 *
 * <p><b>Threading.</b> Cluster duty-cycle thread only — same threading invariant as every other
 * handler interaction. No synchronisation required because all calls land on the single
 * deterministic thread.
 *
 * <p><b>Allocation.</b> Implementations must be zero-allocation on the hot path. The reference
 * implementation in {@link NewOrderSingleHandler} performs a single {@code Long2LongHashMap.get} +
 * {@code put} pair per call.
 *
 * @see NewOrderSingleHandler
 * @see QuoteRequestHandler
 */
@FunctionalInterface
public interface SessionMetricsRecorder {

  /**
   * Increment this session's quote-request counter by 1. Called from {@link
   * QuoteRequestHandler#onCommand} BEFORE the length precondition so malformed quotes still count
   * toward the session's lifetime activity (matches the order-side rejection-counter pattern).
   *
   * @param sessionId Aeron cluster session id ({@code ClientSession#id()})
   */
  void recordQuoteRequest(long sessionId);
}
