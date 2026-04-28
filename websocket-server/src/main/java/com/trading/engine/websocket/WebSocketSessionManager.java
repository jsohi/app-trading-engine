package com.trading.engine.websocket;

import io.netty.channel.Channel;
import io.netty.channel.ChannelId;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.agrona.concurrent.NanoClock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Manages WebSocket client sessions with capacity enforcement and per-IP/per-user counting.
 *
 * <p><b>Capacity limits</b> (all configurable via {@link WebSocketServerConfig}):
 *
 * <ul>
 *   <li>Global: 256 concurrent sessions (including grace-period sessions)
 *   <li>Per-IP: 10 concurrent connections from the same remote address
 *   <li>Per-user: 4 concurrent sessions for the same JWT {@code sub} claim
 * </ul>
 *
 * <p><b>Threading.</b> Thread-safe. Uses {@link ConcurrentHashMap} for sessions (keyed by {@link
 * ChannelId}), per-IP counts, and per-user counts. Session lifecycle (register/remove) may be
 * called from different Netty worker threads. The drain handler calls {@link #forEachSession} from
 * a single worker event loop thread, which iterates a weakly-consistent view of the sessions map.
 *
 * <p><b>Allocation.</b> {@link ConcurrentHashMap} entries are allocated per session (acceptable —
 * one-time per connection, not hot path). {@link AtomicInteger} instances are allocated per unique
 * IP and per unique user.
 *
 * @see <a href="docs/websocket-architecture.md">WebSocket Architecture — Section 3</a>
 */
public final class WebSocketSessionManager {

  private static final Logger LOG = LogManager.getLogger(WebSocketSessionManager.class);

  private final WebSocketServerConfig config;
  private final WebSocketMetrics metrics;
  private final NanoClock nanoClock;

  /**
   * Active sessions keyed by {@link ChannelId}. Uses {@code ChannelId.equals()/hashCode()} which is
   * designed for map keying, avoiding the collision-prone {@code channel.id().hashCode()} int
   * widened to long.
   */
  private final ConcurrentHashMap<ChannelId, WebSocketSession> sessions;

  /** Per-IP connection count. Key: IP address string. */
  private final ConcurrentHashMap<String, AtomicInteger> perIpCount;

  /** Per-user connection count. Key: JWT sub claim. */
  private final ConcurrentHashMap<String, AtomicInteger> perUserCount;

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
    this.sessions = new ConcurrentHashMap<>();
    this.perIpCount = new ConcurrentHashMap<>();
    this.perUserCount = new ConcurrentHashMap<>();
  }

  /**
   * Try to register a new session. Enforces global and per-IP limits.
   *
   * <p>The remote IP address is captured at registration time and stored in the session so that
   * {@link #removeSession} can use the stored IP even after the channel has disconnected (when
   * {@code channel.remoteAddress()} may return null).
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

    // Per-IP limit — atomic increment-then-check to prevent TOCTOU race where concurrent
    // registrations from the same IP both pass the check and exceed the limit.
    final var remoteAddr = extractIp(channel);
    final var ipCounter = perIpCount.computeIfAbsent(remoteAddr, k -> new AtomicInteger(0));
    final int newIpCount = ipCounter.incrementAndGet();
    if (newIpCount > config.maxConnectionsPerIp()) {
      ipCounter.decrementAndGet(); // roll back — limit exceeded
      LOG.warn(
          "Per-IP session limit reached ({}) for {}", config.maxConnectionsPerIp(), remoteAddr);
      return null;
    }

    final long nowNs = nanoClock.nanoTime();
    final var session = new WebSocketSession(channel, nowNs, remoteAddr);
    final var channelId = channel.id();
    sessions.put(channelId, session);
    metrics.connectionOpened();

    LOG.info(
        "Session registered: sessionId={} channelId={} ip={}",
        session.sessionId(),
        channelId,
        remoteAddr);
    return session;
  }

  /**
   * Set the user ID on a session after JWT authentication. Enforces per-user limit. Idempotent — if
   * the session already has a userId set, the call is a no-op and returns true (the user was
   * already counted).
   *
   * @param session the session to update
   * @param userId the JWT {@code sub} claim
   * @return true if the per-user limit allows this session, false if exceeded
   */
  public boolean setUserId(final WebSocketSession session, final String userId) {
    Objects.requireNonNull(session, "session");
    Objects.requireNonNull(userId, "userId");

    // Idempotent: if userId is already set on this session, skip re-increment
    if (session.userId() != null) {
      return true;
    }

    // Atomic increment-then-check to prevent TOCTOU race on per-user limit.
    final var userCounter = perUserCount.computeIfAbsent(userId, k -> new AtomicInteger(0));
    final int newUserCount = userCounter.incrementAndGet();
    if (newUserCount > config.maxConnectionsPerUser()) {
      userCounter.decrementAndGet(); // roll back — limit exceeded
      LOG.warn(
          "Per-user session limit reached ({}) for user {}",
          config.maxConnectionsPerUser(),
          userId);
      return false;
    }
    session.userId(userId);
    return true;
  }

  /**
   * Remove a session on disconnect. Decrements IP and user counters. Uses the IP address stored in
   * the session at registration time (not from the channel, which may already be disconnected).
   *
   * @param channel the disconnected Netty channel
   */
  public void removeSession(final Channel channel) {
    Objects.requireNonNull(channel, "channel");

    final var channelId = channel.id();
    final var session = sessions.remove(channelId);
    if (session == null) {
      return;
    }

    // Decrement per-IP using the IP stored at registration time.
    // updateAndGet with Math.max(0, ...) prevents negative counters on double-removal.
    final var remoteAddr = session.remoteIp();
    // Decrement per-IP count. Use computeIfPresent to atomically decrement and remove if zero,
    // preventing both the G19 race (remove while another thread increments) and the unbounded
    // growth of zero-count entries (Gemini R2-4). The lambda runs under the segment lock, so
    // concurrent computeIfAbsent on the same key will see the removal atomically.
    perIpCount.computeIfPresent(
        remoteAddr,
        (k, counter) -> {
          final int remaining = counter.updateAndGet(v -> Math.max(0, v - 1));
          return remaining > 0 ? counter : null; // remove entry when count reaches 0
        });

    // Decrement per-user (same atomic pattern)
    final var userId = session.userId();
    if (userId != null) {
      perUserCount.computeIfPresent(
          userId,
          (k, counter) -> {
            final int remaining = counter.updateAndGet(v -> Math.max(0, v - 1));
            return remaining > 0 ? counter : null;
          });
    }

    // Clear original-auth jti so a new session on a recycled channel doesn't inherit it.
    session.clearOriginalAuthJti();

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
    return sessions.get(channel.id());
  }

  /**
   * Find a session by its UUID. Linear scan over the session map — used by the cold-path {@code
   * SessionResume} handler and the CommandAck back-channel drain loop. The session count is bounded
   * by {@link WebSocketServerConfig#maxConcurrentSessions} (default 256), so the O(N) cost is
   * acceptable.
   *
   * @param sessionId the session UUID to look up
   * @return the session, or {@code null} if no session with that UUID is currently registered
   */
  public WebSocketSession findById(final UUID sessionId) {
    Objects.requireNonNull(sessionId, "sessionId");
    for (final var s : sessions.values()) {
      if (sessionId.equals(s.sessionId())) {
        return s;
      }
    }
    return null;
  }

  /**
   * Iterate over all active sessions. Used by the drain handler to fan-out egress messages to all
   * connected channels.
   *
   * <p><b>Threading.</b> Uses {@link ConcurrentHashMap#values()} which provides a weakly-consistent
   * iterator — safe for concurrent modification from other threads. Sessions added or removed
   * during iteration may or may not be visible.
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
   * Returns a directly iterable view of all sessions for for-loop iteration in the drain handler.
   * Avoids the per-message lambda/Runnable allocation of {@link #forEachSession(Consumer)}.
   *
   * <p><b>Threading.</b> Uses {@link ConcurrentHashMap#values()} which provides a weakly-consistent
   * view — same semantics as {@link #forEachSession(Consumer)}.
   *
   * @return an iterable of all active sessions
   */
  public Iterable<WebSocketSession> sessions() {
    return sessions.values();
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
