package com.trading.engine.websocket;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.agrona.concurrent.NanoClock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * DDoS protection via dual-level connection rate limiting. Replaces the architecture doc's {@code
 * ChannelTrafficShapingHandler} with a custom handler that shapes connection establishment rate
 * (not per-channel bandwidth), which is what DDoS mitigation actually requires.
 *
 * <p><b>Rate limits</b> (configurable via {@link WebSocketServerConfig}):
 *
 * <ul>
 *   <li>Per-IP: max 10 new connections/second
 *   <li>Global: max 256 new connections/second
 * </ul>
 *
 * <p>Exceeded connections are closed immediately on {@code channelActive()}, before any TLS
 * handshake or WebSocket upgrade processing.
 *
 * <p><b>Implementation.</b> Token bucket with per-second refill. Tokens are checked and decremented
 * atomically on {@code channelActive()}. The bucket refills to the configured rate every second.
 *
 * <p><b>Threading.</b> NOT {@code @Sharable} — a new lightweight instance is created per channel in
 * the pipeline initializer. All instances delegate to a shared {@link RateLimiterState} that uses
 * {@link AtomicLong} for the global token counter and {@link ConcurrentHashMap} for per-IP tokens,
 * making concurrent access from multiple Netty worker threads safe.
 *
 * <p><b>Allocation.</b> Per-channel handler instance is lightweight (single reference to shared
 * state). The shared state uses {@link ConcurrentHashMap} for per-IP tracking (one entry per unique
 * IP, bounded by connection rate — stale entries are cleared on refill).
 *
 * @see <a href="docs/websocket-architecture.md">WebSocket Architecture — Section 4</a>
 */
public final class ConnectionRateLimiter extends ChannelInboundHandlerAdapter {

  private static final Logger LOG = LogManager.getLogger(ConnectionRateLimiter.class);

  private final RateLimiterState state;

  /**
   * Create a connection rate limiter that delegates to shared thread-safe state.
   *
   * @param state the shared rate limiter state (one per server, shared across all channels)
   */
  public ConnectionRateLimiter(final RateLimiterState state) {
    this.state = Objects.requireNonNull(state, "state");
  }

  /**
   * Check connection rate limits on new channel activation. Refills token buckets if a new second
   * has elapsed, then verifies both global and per-IP limits. Channels exceeding either limit are
   * closed immediately before any TLS or WebSocket processing.
   *
   * @param ctx the channel handler context
   * @throws Exception if the super call throws
   */
  @Override
  public void channelActive(final ChannelHandlerContext ctx) throws Exception {
    final var ip = extractIp(ctx);

    if (!state.tryAcquire(ip)) {
      LOG.warn(
          "Connection rate limit exceeded for {} — closing channel {}", ip, ctx.channel().id());
      ctx.close();
      return;
    }

    super.channelActive(ctx);
  }

  private static String extractIp(final ChannelHandlerContext ctx) {
    final var addr = ctx.channel().remoteAddress();
    if (addr instanceof InetSocketAddress inet) {
      return inet.getAddress().getHostAddress();
    }
    return addr != null ? addr.toString() : "unknown";
  }

  /**
   * Thread-safe shared state for connection rate limiting. A single instance is created at server
   * startup and shared across all per-channel {@link ConnectionRateLimiter} handler instances.
   *
   * <p>Uses {@link AtomicLong} for the global token counter and {@link ConcurrentHashMap} with
   * {@link AtomicLong} values for per-IP token counters. The refill check uses a CAS loop on the
   * {@code lastRefillNs} to ensure exactly one thread performs the refill per second.
   *
   * <p><b>Thread safety.</b> All fields are either final or accessed via atomic operations. Safe
   * for concurrent access from multiple Netty worker threads.
   */
  public static final class RateLimiterState {

    private final int perIpLimit;
    private final int globalLimit;
    private final NanoClock nanoClock;
    private final AtomicLong globalTokens;
    private final ConcurrentHashMap<String, AtomicLong> perIpTokens;
    private final AtomicLong lastRefillNs;

    /**
     * Create shared rate limiter state.
     *
     * @param config the server configuration (for rate limits)
     * @param nanoClock monotonic clock for bucket refill timing
     */
    public RateLimiterState(final WebSocketServerConfig config, final NanoClock nanoClock) {
      Objects.requireNonNull(config, "config");
      this.perIpLimit = config.perIpNewConnectionsPerSec();
      this.globalLimit = config.globalNewConnectionsPerSec();
      this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
      this.globalTokens = new AtomicLong(globalLimit);
      this.perIpTokens = new ConcurrentHashMap<>();
      this.lastRefillNs = new AtomicLong(nanoClock.nanoTime());
    }

    /**
     * Try to acquire a connection token for the given IP. Refills buckets if a new second has
     * elapsed, then atomically checks and decrements both global and per-IP limits.
     *
     * @param ip the remote IP address string
     * @return true if the connection is allowed, false if rate limited
     */
    public boolean tryAcquire(final String ip) {
      refillIfNeeded();

      // Global check — CAS loop to atomically decrement
      long current;
      do {
        current = globalTokens.get();
        if (current <= 0) {
          return false;
        }
      } while (!globalTokens.compareAndSet(current, current - 1));

      // Per-IP check — CAS loop on the per-IP AtomicLong
      final var ipCounter = perIpTokens.computeIfAbsent(ip, k -> new AtomicLong(perIpLimit));
      long ipCurrent;
      do {
        ipCurrent = ipCounter.get();
        if (ipCurrent <= 0) {
          // Undo the global decrement since the per-IP check failed
          globalTokens.incrementAndGet();
          return false;
        }
      } while (!ipCounter.compareAndSet(ipCurrent, ipCurrent - 1));

      return true;
    }

    private void refillIfNeeded() {
      final long nowNs = nanoClock.nanoTime();
      final long lastNs = lastRefillNs.get();
      if (nowNs - lastNs >= 1_000_000_000L) { // 1 second
        // CAS to ensure only one thread refills per second
        if (lastRefillNs.compareAndSet(lastNs, nowNs)) {
          globalTokens.set(globalLimit);
          perIpTokens.clear();
        }
      }
    }
  }
}
