package com.trading.engine.websocket;

import io.netty.channel.Channel;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.function.Consumer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.Object2IntHashMap;
import org.agrona.concurrent.NanoClock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Manages WebSocket client sessions with capacity enforcement, heartbeat monitoring, and reconnect
 * throttling.
 *
 * <p><b>Capacity limits</b> (all configurable via {@link WebSocketServerConfig}):
 *
 * <ul>
 *   <li>Global: 256 concurrent sessions (including grace-period sessions)
 *   <li>Per-IP: 10 concurrent connections from the same remote address
 *   <li>Per-user: 4 concurrent sessions for the same JWT {@code sub} claim
 * </ul>
 *
 * <p><b>Heartbeat timeout.</b> Clients must send {@code ClientHeartbeat} (template 65) at least
 * every 20 seconds. Sessions that miss a heartbeat are disconnected with {@code
 * WebSocketError(HeartbeatTimeout)}.
 *
 * <p><b>Grace period.</b> After disconnect, session state (including replay buffer) is held for 30
 * seconds. Grace-period sessions count toward the global limit. Subscriptions are cleared on
 * disconnect (transient — client re-subscribes after reconnect).
 *
 * <p><b>Reconnect throttle.</b> Maximum 10 reconnects per minute per user (anti-thundering-herd).
 *
 * <p><b>Threading.</b> Not thread-safe — all methods must be called from the Netty event loop
 * thread only.
 *
 * <p><b>Allocation.</b> Agrona primitive-keyed maps avoid boxing. Session creation allocates
 * (acceptable — one-time per connection, not hot path).
 *
 * @see <a href="docs/websocket-architecture.md">WebSocket Architecture — Section 3</a>
 */
public final class WebSocketSessionManager {

  private static final Logger LOG = LogManager.getLogger(WebSocketSessionManager.class);

  private final WebSocketServerConfig config;
  private final WebSocketMetrics metrics;
  private final NanoClock nanoClock;

  /** Active sessions keyed by Netty channel ID hash. */
  private final Long2ObjectHashMap<WebSocketSession> sessions;

  /** Per-IP connection count. Key: IP address string hash. Sentinel: -1. */
  private final Object2IntHashMap<String> perIpCount;

  /** Per-user connection count. Key: JWT sub claim. Sentinel: -1. */
  private final Object2IntHashMap<String> perUserCount;

  /**
   * Create a new session manager.
   *
   * @param config the server configuration (for capacity limits and timeouts)
   * @param metrics the metrics instance (for connection gauge)
   * @param nanoClock monotonic clock for heartbeat and grace period tracking
   */
  public WebSocketSessionManager(
      final WebSocketServerConfig config,
      final WebSocketMetrics metrics,
      final NanoClock nanoClock) {
    this.config = Objects.requireNonNull(config, "config");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
    this.sessions = new Long2ObjectHashMap<>();
    this.perIpCount = new Object2IntHashMap<>(-1);
    this.perUserCount = new Object2IntHashMap<>(-1);
  }

  /**
   * Try to register a new session. Enforces global, per-IP, and per-user limits.
   *
   * @param channel the Netty channel for this client
   * @return the created session, or null if capacity is exceeded
   */
  public WebSocketSession tryRegister(final Channel channel) {
    Objects.requireNonNull(channel, "channel");

    // Global limit
    if (sessions.size() >= config.maxConcurrentSessions()) {
      LOG.warn("Global session limit reached ({})", config.maxConcurrentSessions());
      return null;
    }

    // Per-IP limit
    final var remoteAddr = extractIp(channel);
    final int ipCount = perIpCount.getOrDefault(remoteAddr, 0);
    if (ipCount >= config.maxConnectionsPerIp()) {
      LOG.warn(
          "Per-IP session limit reached ({}) for {}", config.maxConnectionsPerIp(), remoteAddr);
      return null;
    }

    final long nowNs = nanoClock.nanoTime();
    final var session = new WebSocketSession(channel, nowNs);
    final long channelId = channel.id().hashCode();
    sessions.put(channelId, session);
    perIpCount.put(remoteAddr, ipCount + 1);
    metrics.connectionOpened();

    LOG.info(
        "Session registered: sessionId={} channelId={} ip={}",
        session.sessionId(),
        channelId,
        remoteAddr);
    return session;
  }

  /**
   * Set the user ID on a session after JWT authentication. Enforces per-user limit.
   *
   * @param session the session to update
   * @param userId the JWT {@code sub} claim
   * @return true if the per-user limit allows this session, false if exceeded
   */
  public boolean setUserId(final WebSocketSession session, final String userId) {
    Objects.requireNonNull(session, "session");
    Objects.requireNonNull(userId, "userId");

    final int userCount = perUserCount.getOrDefault(userId, 0);
    if (userCount >= config.maxConnectionsPerUser()) {
      LOG.warn(
          "Per-user session limit reached ({}) for user {}",
          config.maxConnectionsPerUser(),
          userId);
      return false;
    }
    session.userId(userId);
    perUserCount.put(userId, userCount + 1);
    return true;
  }

  /**
   * Remove a session on disconnect. Decrements IP and user counters.
   *
   * @param channel the disconnected Netty channel
   */
  public void removeSession(final Channel channel) {
    Objects.requireNonNull(channel, "channel");

    final long channelId = channel.id().hashCode();
    final var session = sessions.remove(channelId);
    if (session == null) {
      return;
    }

    // Decrement per-IP
    final var remoteAddr = extractIp(channel);
    final int ipCount = perIpCount.getOrDefault(remoteAddr, 0);
    if (ipCount <= 1) {
      perIpCount.remove(remoteAddr);
    } else {
      perIpCount.put(remoteAddr, ipCount - 1);
    }

    // Decrement per-user
    final var userId = session.userId();
    if (userId != null) {
      final int userCount = perUserCount.getOrDefault(userId, 0);
      if (userCount <= 1) {
        perUserCount.remove(userId);
      } else {
        perUserCount.put(userId, userCount - 1);
      }
    }

    metrics.connectionClosed();
    LOG.info("Session removed: sessionId={} channelId={}", session.sessionId(), channelId);
  }

  /**
   * Find a session by channel.
   *
   * @param channel the Netty channel
   * @return the session, or null if not found
   */
  public WebSocketSession findSession(final Channel channel) {
    Objects.requireNonNull(channel, "channel");
    return sessions.get(channel.id().hashCode());
  }

  /**
   * Iterate over all active sessions. Used by the drain handler to fan-out egress messages to all
   * connected channels.
   *
   * <p><b>Threading.</b> Must be called from the Netty event loop thread only.
   *
   * @param action the action to perform on each session
   */
  public void forEachSession(final Consumer<WebSocketSession> action) {
    Objects.requireNonNull(action, "action");
    for (final WebSocketSession session : sessions.values()) {
      action.accept(session);
    }
  }

  /**
   * @return the current number of active sessions (including grace-period)
   */
  public int activeSessionCount() {
    return sessions.size();
  }

  private static String extractIp(final Channel channel) {
    final var addr = channel.remoteAddress();
    if (addr instanceof InetSocketAddress inet) {
      return inet.getAddress().getHostAddress();
    }
    return addr != null ? addr.toString() : "unknown";
  }
}
