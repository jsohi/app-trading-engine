package com.trading.engine.gateway;

/**
 * Correlates a ClOrdID (or QuoteReqID) to the gateway-assigned FIX session key that originated the
 * request. Used by the {@link ClusterEgressListener} to route cluster responses back to the correct
 * Artio session.
 *
 * <p><b>Allocation.</b> The signature uses {@code byte[]} + offset + length so callers can pass an
 * SBE scratch buffer directly, avoiding {@link String} allocation on the hot path.
 *
 * <p><b>Threading.</b> Implementations are expected to be called on the single-threaded gateway
 * duty-cycle thread and do not need to be thread-safe.
 */
@FunctionalInterface
public interface SessionLookup {

  /** Sentinel returned when no session is associated with the given identifier. */
  long NULL_SESSION = -1L;

  /**
   * Find the gateway session key for the order or quote with the given ClOrdID.
   *
   * @param clOrdId null-padding-trimmed ClOrdID bytes
   * @param offset start offset within {@code clOrdId}
   * @param length number of significant bytes
   * @return gateway session key, or {@link #NULL_SESSION} if the session disconnected or the
   *     identifier is unknown
   */
  long findByClOrdId(byte[] clOrdId, int offset, int length);
}
