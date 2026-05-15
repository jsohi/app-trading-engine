package com.trading.engine.websocket;

/**
 * Thrown by {@link OidcDiscoveryClient} on any startup-time discovery failure: unreachable URI,
 * malformed JSON, missing {@code jwks_uri}, oversized response body, or a cross-host {@code
 * jwks_uri} that fails the RFC 8414 §3 host-match invariant.
 *
 * <p>Always a startup-fatal condition — the launcher catches this and exits before binding any
 * listening sockets, so the server never enters a half-initialised state.
 */
public final class OidcDiscoveryException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public OidcDiscoveryException(final String message) {
    super(message);
  }

  public OidcDiscoveryException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
