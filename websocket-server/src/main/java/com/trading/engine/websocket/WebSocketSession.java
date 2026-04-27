package com.trading.engine.websocket;

import io.netty.channel.Channel;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Per-client WebSocket session state. Created on successful authentication, held for the grace
 * period after disconnect, and destroyed on expiry. Stores JWT {@code jti} for revocation tracking,
 * a per-session {@link SubscriptionFilter} for event delivery filtering, and an entitled accounts
 * set from {@link UserEntitlementService} for account-level drain-path filtering.
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
  private final String remoteIp;
  private String userId;
  private String jti;
  // Volatile: written once by channel event loop at auth time, read by drain handler event loop
  // via matches() call. SubscriptionFilter's internal volatile snapshot handles per-mutation
  // visibility; this volatile ensures the drain handler sees the non-null reference after init.
  private volatile SubscriptionFilter subscriptionFilter;
  // Volatile: written by channel event loop at auth/re-auth time, read by drain handler event loop
  // during account entitlement checks via AccountExtractor.
  private volatile Set<String> entitledAccounts = Set.of();
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
   * @param remoteIp the remote IP address captured at registration time; stored so that it remains
   *     available after the channel disconnects (when {@code channel.remoteAddress()} returns null)
   */
  public WebSocketSession(final Channel channel, final long nowNs, final String remoteIp) {
    this.sessionId = UUID.randomUUID();
    this.channel = Objects.requireNonNull(channel, "channel");
    this.remoteIp = Objects.requireNonNull(remoteIp, "remoteIp");
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
   * @return the remote IP address captured at registration time (remains valid after disconnect)
   */
  public String remoteIp() {
    return remoteIp;
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
   * @return the JWT {@code jti} claim (full string for collision-resistant revocation tracking)
   */
  public String jti() {
    return jti;
  }

  /**
   * @param jti the JWT jti claim (full string, not a hash)
   */
  public void jti(final String jti) {
    this.jti = jti;
  }

  /**
   * @return the subscription filter for this session, or null if not yet initialized (pre-auth)
   */
  public SubscriptionFilter subscriptionFilter() {
    return subscriptionFilter;
  }

  /**
   * Initialize the subscription filter after successful authentication.
   *
   * @param maxSubscriptions the maximum number of symbol subscriptions allowed per session
   */
  public void initSubscriptionFilter(final int maxSubscriptions) {
    this.subscriptionFilter = new SubscriptionFilter(maxSubscriptions);
  }

  /**
   * @return the set of account codes this session is entitled to access (from JWT accounts claim)
   */
  public Set<String> entitledAccounts() {
    return entitledAccounts;
  }

  /**
   * Set the entitled account codes after validation by {@link UserEntitlementService}.
   *
   * @param entitledAccounts unmodifiable set of validated active account codes
   */
  public void entitledAccounts(final Set<String> entitledAccounts) {
    this.entitledAccounts = Objects.requireNonNull(entitledAccounts, "entitledAccounts");
  }

  /**
   * Assign the next reliable sequence number for outbound messages.
   *
   * @return the next sequence number (pre-increment — starts at 1)
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
    // Architecture doc: grace period does NOT hold subscriptions or entitlements.
    // Clear to stop receiving events during grace period.
    if (subscriptionFilter != null) {
      subscriptionFilter.clear();
    }
    entitledAccounts = Set.of();
  }

  /**
   * @return the monotonic timestamp when the grace period started
   */
  public long gracePeriodStartNs() {
    return gracePeriodStartNs;
  }
}
