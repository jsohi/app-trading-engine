package com.trading.engine.websocket;

import io.netty.channel.Channel;
import java.util.Objects;
import java.util.UUID;

/**
 * Per-client WebSocket session state. Created on successful authentication, held for the grace
 * period after disconnect, and destroyed on expiry.
 *
 * <p><b>Thread safety.</b> Owned by the Netty event loop thread. Not shared across threads. The
 * AeronEgressThread writes to the {@link org.agrona.concurrent.ManyToOneConcurrentArrayQueue} and
 * the drain handler reads session state — but the session object itself is only accessed from the
 * Netty thread.
 *
 * <p><b>Allocation.</b> One-time allocation per session. UUID generated via {@code
 * UUID.randomUUID()} (acceptable — WebSocket server is not a cluster service).
 *
 * @see <a href="docs/websocket-architecture.md">WebSocket Architecture — Section 3</a>
 */
public final class WebSocketSession {

  private final UUID sessionId;
  private final Channel channel;
  private String userId;
  private long jti;
  private long reliableSeqCounter;
  private long lastClientCmdSeqNo;
  private long lastClientHeartbeatNs;
  private boolean replayInProgress;
  private long gracePeriodStartNs;
  private boolean disconnected;

  /**
   * Create a new session for an authenticated client.
   *
   * @param channel the Netty channel for this client
   * @param nowNs current monotonic time in nanoseconds (for heartbeat tracking)
   */
  public WebSocketSession(final Channel channel, final long nowNs) {
    this.sessionId = UUID.randomUUID();
    this.channel = Objects.requireNonNull(channel, "channel");
    this.lastClientHeartbeatNs = nowNs;
  }

  /**
   * @return the unique session identifier
   */
  public UUID sessionId() {
    return sessionId;
  }

  /**
   * @return the Netty channel for this client
   */
  public Channel channel() {
    return channel;
  }

  /**
   * @return the authenticated user identifier (JWT {@code sub} claim)
   */
  public String userId() {
    return userId;
  }

  /**
   * @param userId the authenticated user identifier
   */
  public void userId(final String userId) {
    this.userId = userId;
  }

  /**
   * @return the JWT {@code jti} claim hash for session hijack prevention
   */
  public long jti() {
    return jti;
  }

  /**
   * @param jti the JWT jti claim hash
   */
  public void jti(final long jti) {
    this.jti = jti;
  }

  /**
   * Assign the next reliable sequence number for outbound messages.
   *
   * @return the next sequence number (post-increment)
   */
  public long nextReliableSeqNo() {
    return ++reliableSeqCounter;
  }

  /**
   * @return the current reliable sequence counter (last assigned)
   */
  public long reliableSeqCounter() {
    return reliableSeqCounter;
  }

  /**
   * @return the last client command sequence number received
   */
  public long lastClientCmdSeqNo() {
    return lastClientCmdSeqNo;
  }

  /**
   * @param seqNo the client command sequence number
   */
  public void lastClientCmdSeqNo(final long seqNo) {
    this.lastClientCmdSeqNo = seqNo;
  }

  /**
   * @return the monotonic timestamp of the last client heartbeat
   */
  public long lastClientHeartbeatNs() {
    return lastClientHeartbeatNs;
  }

  /**
   * @param nowNs the current monotonic time
   */
  public void updateHeartbeat(final long nowNs) {
    this.lastClientHeartbeatNs = nowNs;
  }

  /**
   * @return true if a gap replay is in progress for this session
   */
  public boolean isReplayInProgress() {
    return replayInProgress;
  }

  /**
   * @param replayInProgress true to pause live delivery during gap replay
   */
  public void replayInProgress(final boolean replayInProgress) {
    this.replayInProgress = replayInProgress;
  }

  /**
   * @return true if the client has disconnected (grace period may still be active)
   */
  public boolean isDisconnected() {
    return disconnected;
  }

  /**
   * Mark this session as disconnected and start the grace period.
   *
   * @param nowNs current monotonic time
   */
  public void markDisconnected(final long nowNs) {
    this.disconnected = true;
    this.gracePeriodStartNs = nowNs;
  }

  /**
   * @return the monotonic timestamp when the grace period started
   */
  public long gracePeriodStartNs() {
    return gracePeriodStartNs;
  }
}
