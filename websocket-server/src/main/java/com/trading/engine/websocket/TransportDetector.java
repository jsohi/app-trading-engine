package com.trading.engine.websocket;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Detects and selects the optimal Netty native transport at runtime.
 *
 * <p>Tries Epoll (Linux), then KQueue (macOS), then falls back to NIO. Native transports provide
 * 10-30% lower syscall overhead compared to NIO. The detection is performed once at startup via
 * {@link #detect()}.
 *
 * <p><b>Thread safety.</b> Immutable result record. Detection method is stateless.
 *
 * <p><b>Allocation.</b> One-time allocation at startup (event loop groups). No hot-path allocation.
 *
 * @see <a href="docs/websocket-architecture.md">WebSocket Architecture — Section 6</a>
 */
public final class TransportDetector {

  private static final Logger LOG = LogManager.getLogger(TransportDetector.class);

  private TransportDetector() {}

  /**
   * Result of transport detection: the event loop groups and server channel class to use.
   *
   * @param bossGroup event loop group for accepting connections (1 thread)
   * @param workerGroup event loop group for I/O (N threads)
   * @param channelClass the server socket channel class matching the selected transport
   * @param transportName human-readable name for logging ("epoll", "kqueue", "nio")
   */
  public record Result(
      EventLoopGroup bossGroup,
      EventLoopGroup workerGroup,
      Class<? extends ServerChannel> channelClass,
      String transportName) {}

  /**
   * Detect the best available native transport and create event loop groups.
   *
   * <p>Worker thread count: {@code max(2, Runtime.getRuntime().availableProcessors() / 2)}.
   *
   * @return a {@link Result} with the selected transport's event loop groups and channel class
   */
  public static Result detect() {
    final int workerThreads = Math.max(2, Runtime.getRuntime().availableProcessors() / 2);

    // Try Epoll (Linux)
    try {
      final var epollClass = Class.forName("io.netty.channel.epoll.Epoll");
      final boolean available = (boolean) epollClass.getMethod("isAvailable").invoke(null);
      if (available) {
        final var bossGroupClass = Class.forName("io.netty.channel.epoll.EpollEventLoopGroup");
        final var channelClazz = Class.forName("io.netty.channel.epoll.EpollServerSocketChannel");
        final var bossGroup =
            (EventLoopGroup) bossGroupClass.getConstructor(int.class).newInstance(1);
        final var workerGroup =
            (EventLoopGroup) bossGroupClass.getConstructor(int.class).newInstance(workerThreads);
        @SuppressWarnings("unchecked")
        final var channelClass = (Class<? extends ServerChannel>) channelClazz;
        LOG.info("Netty transport: epoll (Linux native), {} worker threads", workerThreads);
        return new Result(bossGroup, workerGroup, channelClass, "epoll");
      }
    } catch (final Exception e) {
      LOG.debug("Epoll not available: {}", e.getMessage());
    }

    // Try KQueue (macOS)
    try {
      final var kqueueClass = Class.forName("io.netty.channel.kqueue.KQueue");
      final boolean available = (boolean) kqueueClass.getMethod("isAvailable").invoke(null);
      if (available) {
        final var bossGroupClass = Class.forName("io.netty.channel.kqueue.KQueueEventLoopGroup");
        final var channelClazz = Class.forName("io.netty.channel.kqueue.KQueueServerSocketChannel");
        final var bossGroup =
            (EventLoopGroup) bossGroupClass.getConstructor(int.class).newInstance(1);
        final var workerGroup =
            (EventLoopGroup) bossGroupClass.getConstructor(int.class).newInstance(workerThreads);
        @SuppressWarnings("unchecked")
        final var channelClass = (Class<? extends ServerChannel>) channelClazz;
        LOG.info("Netty transport: kqueue (macOS native), {} worker threads", workerThreads);
        return new Result(bossGroup, workerGroup, channelClass, "kqueue");
      }
    } catch (final Exception e) {
      LOG.debug("KQueue not available: {}", e.getMessage());
    }

    // Fallback: NIO
    final var bossGroup = new NioEventLoopGroup(1);
    final var workerGroup = new NioEventLoopGroup(workerThreads);
    LOG.info("Netty transport: NIO (Java fallback), {} worker threads", workerThreads);
    return new Result(bossGroup, workerGroup, NioServerSocketChannel.class, "nio");
  }
}
