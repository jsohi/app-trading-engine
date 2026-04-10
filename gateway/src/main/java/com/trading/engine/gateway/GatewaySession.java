package com.trading.engine.gateway;

import uk.co.real_logic.artio.session.Session;

/**
 * Abstraction over Artio's concrete {@link Session} class. Exposes only the methods used by the
 * gateway hot path, enabling unit testing with simple test doubles instead of requiring full Artio
 * infrastructure.
 *
 * <p>Production code uses {@link ArtioGatewaySession} which delegates to the real Artio Session.
 * Tests use inline implementations that record calls.
 *
 * <p><b>Industry standard.</b> This pattern (wrapping vendor-specific session objects behind a thin
 * interface) is standard practice at LMAX, Adaptive, and other Aeron/Artio-based trading systems.
 * It isolates business logic from framework internals.
 *
 * <p><b>Threading.</b> Implementations are not expected to be thread-safe — single-threaded gateway
 * duty-cycle thread only.
 */
public interface GatewaySession {

  /** Artio session ID, unique per connection. */
  long id();

  /** Returns {@code true} if the session is connected and can accept messages. */
  boolean isConnected();

  /**
   * Attempt to send a FIX message to the counterparty.
   *
   * @param encoder Artio FIX encoder with fields already set
   * @return stream position on success (>= 0), or a negative error code on failure
   */
  long trySend(uk.co.real_logic.artio.builder.Encoder encoder);

  /** Last received MsgSeqNum from the counterparty. Used for reject RefSeqNum (tag 45). */
  int lastReceivedMsgSeqNum();

  /** Initiate logout and disconnect. Used during graceful shutdown and capacity rejection. */
  long logoutAndDisconnect();

  /** The remote host this session is connected from. */
  String connectedHost();

  /**
   * Returns the SenderCompID (tag 49) of the counterparty that initiated this session. Used for
   * per-CompID session capacity enforcement.
   *
   * <p>For acceptor sessions, this is the client's CompID from the Logon message. For initiator
   * sessions, this is the local CompID.
   */
  String senderCompId();
}
