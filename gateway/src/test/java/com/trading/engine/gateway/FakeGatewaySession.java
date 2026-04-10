package com.trading.engine.gateway;

import java.util.ArrayList;
import java.util.List;
import uk.co.real_logic.artio.builder.Encoder;

/**
 * Test double for {@link GatewaySession}. Records all {@link #trySend} calls and allows
 * configuration of connection state and back-pressure simulation.
 *
 * <p>Used by gateway unit tests to verify message dispatch, reject emission, and egress callback
 * behavior without requiring Artio session infrastructure.
 */
final class FakeGatewaySession implements GatewaySession {

  private final long id;
  private String senderCompId = "SENDER";
  private boolean connected = true;
  private int lastReceivedMsgSeqNum;
  private long trySendResult = 1L; // default: successful send
  private boolean logoutCalled;

  /** Encoders passed to {@link #trySend}, in call order. */
  final List<Encoder> sentEncoders = new ArrayList<>();

  FakeGatewaySession(final long id) {
    this.id = id;
  }

  @Override
  public long id() {
    return id;
  }

  @Override
  public boolean isConnected() {
    return connected;
  }

  @Override
  public long trySend(final Encoder encoder) {
    sentEncoders.add(encoder);
    return trySendResult;
  }

  @Override
  public int lastReceivedMsgSeqNum() {
    return lastReceivedMsgSeqNum;
  }

  @Override
  public long logoutAndDisconnect() {
    logoutCalled = true;
    connected = false;
    return 1L;
  }

  @Override
  public String connectedHost() {
    return "127.0.0.1";
  }

  @Override
  public String senderCompId() {
    return senderCompId;
  }

  // --- Test configuration ---

  FakeGatewaySession setConnected(final boolean connected) {
    this.connected = connected;
    return this;
  }

  FakeGatewaySession setLastReceivedMsgSeqNum(final int seqNum) {
    this.lastReceivedMsgSeqNum = seqNum;
    return this;
  }

  FakeGatewaySession setTrySendResult(final long result) {
    this.trySendResult = result;
    return this;
  }

  boolean isLogoutCalled() {
    return logoutCalled;
  }

  FakeGatewaySession setSenderCompId(final String compId) {
    this.senderCompId = compId;
    return this;
  }
}
