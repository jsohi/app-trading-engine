package com.trading.engine.gateway;

import uk.co.real_logic.artio.builder.Encoder;
import uk.co.real_logic.artio.session.Session;

/**
 * Production implementation of {@link GatewaySession} that delegates to Artio's concrete {@link
 * Session}. Zero allocation — all methods are simple field reads or method forwarding.
 *
 * <p><b>Threading.</b> Not thread-safe — single-threaded gateway duty-cycle thread only.
 */
public final class ArtioGatewaySession implements GatewaySession {

  private final Session session;

  public ArtioGatewaySession(final Session session) {
    if (session == null) {
      throw new NullPointerException("session");
    }
    this.session = session;
  }

  @Override
  public long id() {
    return session.id();
  }

  @Override
  public boolean isConnected() {
    return session.isConnected();
  }

  @Override
  public long trySend(final Encoder encoder) {
    return session.trySend(encoder);
  }

  @Override
  public int lastReceivedMsgSeqNum() {
    return session.lastReceivedMsgSeqNum();
  }

  @Override
  public long logoutAndDisconnect() {
    return session.logoutAndDisconnect();
  }

  @Override
  public String connectedHost() {
    return session.connectedHost();
  }

  /** Returns the underlying Artio session (escape hatch for Artio-specific operations). */
  public Session unwrap() {
    return session;
  }
}
