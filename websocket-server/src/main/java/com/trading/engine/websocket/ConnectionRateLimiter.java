package com.trading.engine.websocket;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.net.InetSocketAddress;
import java.util.Objects;
import org.agrona.collections.Object2LongHashMap;
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
 * <p><b>Threading.</b> Runs on the Netty boss/worker event loop. The per-IP map and global counter
 * are accessed from the event loop thread only (Netty guarantees single-threaded channel
 * lifecycle).
 *
 * <p><b>Allocation.</b> Agrona {@code Object2LongHashMap} avoids boxing. One map entry per unique
 * IP (bounded by the connection rate — stale entries are naturally evicted on refill).
 *
 * @see <a href="docs/websocket-architecture.md">WebSocket Architecture — Section 4</a>
 */
// @Sharable: a single instance is shared across all channels in the pipeline. The per-IP map and
// global counter are only accessed from channelActive(), which is called from the boss thread's
// accept loop — effectively single-threaded. If multiple boss threads are ever configured, these
// fields would need synchronization.
@ChannelHandler.Sharable
public final class ConnectionRateLimiter extends ChannelInboundHandlerAdapter {

  private static final Logger LOG = LogManager.getLogger(ConnectionRateLimiter.class);
  private static final long SENTINEL = Long.MIN_VALUE;

  private final int perIpLimit;
  private final int globalLimit;
  private final NanoClock nanoClock;

  /** Per-IP token count. Key: IP string. Value: tokens remaining in current second. */
  private final Object2LongHashMap<String> perIpTokens;

  private long globalTokens;
  private long lastRefillNs;

  /**
   * Create a connection rate limiter.
   *
   * @param config the server configuration (for rate limits)
   * @param nanoClock monotonic clock for bucket refill timing
   */
  public ConnectionRateLimiter(final WebSocketServerConfig config, final NanoClock nanoClock) {
    Objects.requireNonNull(config, "config");
    this.perIpLimit = config.perIpNewConnectionsPerSec();
    this.globalLimit = config.globalNewConnectionsPerSec();
    this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
    this.perIpTokens = new Object2LongHashMap<>(SENTINEL);
    this.globalTokens = globalLimit;
    this.lastRefillNs = nanoClock.nanoTime();
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
    refillIfNeeded();

    // Global check
    if (globalTokens <= 0) {
      LOG.warn("Global connection rate limit exceeded — closing channel {}", ctx.channel().id());
      ctx.close();
      return;
    }

    // Per-IP check
    final var ip = extractIp(ctx);
    final long ipTokens = perIpTokens.getOrDefault(ip, perIpLimit);
    if (ipTokens <= 0) {
      LOG.warn(
          "Per-IP connection rate limit exceeded for {} — closing channel {}",
          ip,
          ctx.channel().id());
      ctx.close();
      return;
    }

    // Consume tokens
    globalTokens--;
    perIpTokens.put(ip, ipTokens - 1);

    super.channelActive(ctx);
  }

  private void refillIfNeeded() {
    final long nowNs = nanoClock.nanoTime();
    final long elapsedNs = nowNs - lastRefillNs;
    if (elapsedNs >= 1_000_000_000L) { // 1 second
      globalTokens = globalLimit;
      perIpTokens.clear();
      lastRefillNs = nowNs;
    }
  }

  private static String extractIp(final ChannelHandlerContext ctx) {
    final var addr = ctx.channel().remoteAddress();
    if (addr instanceof InetSocketAddress inet) {
      return inet.getAddress().getHostAddress();
    }
    return addr != null ? addr.toString() : "unknown";
  }
}
