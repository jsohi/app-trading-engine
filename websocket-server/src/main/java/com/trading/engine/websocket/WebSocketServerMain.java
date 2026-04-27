package com.trading.engine.websocket;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import java.io.File;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.agrona.concurrent.SystemNanoClock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Netty-based WebSocket server with TLS 1.3 (BoringSSL) and the full pipeline from {@code
 * docs/websocket-architecture.md} Section 1.
 *
 * <p><b>Pipeline:</b>
 *
 * <pre>
 * SslHandler (TLS 1.3, BoringSSL, 3 cipher suites, OCSP)
 *   → HttpServerCodec
 *   → HttpObjectAggregator(65536)
 *   → SecurityHeaderHandler (HSTS, CSP, XFO, nosniff) — shared singleton
 *   → WebSocketServerProtocolHandler (allowExtensions=false — CRIME/BREACH prevention)
 *   → ConnectionRateLimiter (per-IP 10/sec, global 256/sec) — shared singleton
 *   → OriginValidationHandler (CSWSH whitelist) — shared singleton
 * </pre>
 *
 * <p>The drain handler is no longer in the pipeline — it is scheduled as a standalone task on the
 * worker event loop at 1ms fixed rate after the server binds.
 *
 * <p>JwtAuthHandler and WebSocketFrameDispatcher are added in PR 3.
 *
 * <p><b>Transport.</b> Uses native Epoll (Linux) or KQueue (macOS) when available, falling back to
 * NIO. Boss group: 1 thread (accept). Worker group: N = max(2, availableProcessors/2).
 *
 * <p><b>TLS.</b> BoringSSL via {@code SslProvider.OPENSSL}. TLS 1.3 only. Cipher suites: {@code
 * TLS_AES_256_GCM_SHA384}, {@code TLS_CHACHA20_POLY1305_SHA256}, {@code TLS_AES_128_GCM_SHA256}.
 * Self-signed cert for dev when no cert path configured.
 *
 * <p><b>Threading.</b> Netty boss and worker event loops. This class is not thread-safe — call
 * {@link #start()} and {@link #close()} from the launcher thread only.
 *
 * @see <a href="docs/websocket-architecture.md">WebSocket Architecture — Section 1</a>
 */
public final class WebSocketServerMain implements AutoCloseable {

  private static final Logger LOG = LogManager.getLogger(WebSocketServerMain.class);

  /** Timeout in seconds for graceful event loop shutdown. */
  private static final long SHUTDOWN_TIMEOUT_SECONDS = 5;

  private final WebSocketServerConfig config;
  private final ManyToOneConcurrentArrayQueue<EgressEntry> queue;
  private final WebSocketEgressListener egressListener;
  private final WebSocketSessionManager sessionManager;
  private final WebSocketMetrics metrics;
  private Channel serverChannel;
  private TransportDetector.Result transport;

  /**
   * Create the WebSocket server (not yet started).
   *
   * @param config server configuration
   * @param queue the egress queue (shared with AeronEgressThread)
   * @param egressListener the egress listener (for entry pool returns)
   * @param sessionManager the session manager
   * @param metrics metrics instance
   */
  public WebSocketServerMain(
      final WebSocketServerConfig config,
      final ManyToOneConcurrentArrayQueue<EgressEntry> queue,
      final WebSocketEgressListener egressListener,
      final WebSocketSessionManager sessionManager,
      final WebSocketMetrics metrics) {
    this.config = Objects.requireNonNull(config, "config");
    this.queue = Objects.requireNonNull(queue, "queue");
    this.egressListener = Objects.requireNonNull(egressListener, "egressListener");
    this.sessionManager = Objects.requireNonNull(sessionManager, "sessionManager");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
  }

  /**
   * Start the Netty server and bind to the configured port.
   *
   * @throws Exception if TLS initialization or port binding fails
   */
  public void start() throws Exception {
    // Note: ResourceLeakDetector level should be set by the caller (TradingEngineLauncher or test
    // harness) before starting the server. Tests use PARANOID via @BeforeAll; production uses
    // DISABLED via the launcher. This avoids a JVM-wide side effect inside start().

    transport = TransportDetector.detect();
    final var sslCtx = buildSslContext();
    final var nanoClock = SystemNanoClock.INSTANCE;

    // Create shared @Sharable handler instances once — reused across all channels.
    // ConnectionRateLimiter is NOT @Sharable — a new instance per channel, sharing thread-safe
    // state.
    final var securityHeaderHandler = new SecurityHeaderHandler();
    final var rateLimiterState = new ConnectionRateLimiter.RateLimiterState(config, nanoClock);
    final var originValidator = new OriginValidationHandler(config);

    final var bootstrap = new ServerBootstrap();
    bootstrap
        .group(transport.bossGroup(), transport.workerGroup())
        .channel(transport.channelClass())
        .option(ChannelOption.SO_BACKLOG, 128)
        .childOption(ChannelOption.SO_KEEPALIVE, true)
        .childOption(
            ChannelOption.WRITE_BUFFER_WATER_MARK,
            new WriteBufferWaterMark(
                config.writeBufferLowWaterMark(), config.writeBufferHighWaterMark()))
        .childHandler(
            new ChannelInitializer<SocketChannel>() {
              @Override
              protected void initChannel(final SocketChannel ch) {
                final var pipeline = ch.pipeline();
                pipeline.addLast("ssl", sslCtx.newHandler(ch.alloc()));
                pipeline.addLast("http-codec", new HttpServerCodec());
                pipeline.addLast("http-aggregator", new HttpObjectAggregator(65_536));
                pipeline.addLast("security-headers", securityHeaderHandler);
                // allowExtensions=false: CRIME/BREACH prevention (permessage-deflate disabled)
                pipeline.addLast(
                    "ws-protocol",
                    new WebSocketServerProtocolHandler(
                        "/", null, false, 65_536, false, true, 30_000));
                pipeline.addLast("rate-limiter", new ConnectionRateLimiter(rateLimiterState));
                pipeline.addLast("origin-validator", originValidator);
                // PR 3: JwtAuthHandler + WebSocketFrameDispatcher will be added here
              }
            });

    serverChannel = bootstrap.bind(config.port()).sync().channel();

    // Schedule the drain handler on the worker event loop — single instance serves all channels.
    // Uses scheduleWithFixedDelay (not scheduleAtFixedRate) to prevent catch-up storms when a
    // drain cycle takes longer than 1ms. The drain call is wrapped in a try-catch to prevent
    // exceptions from killing the scheduled task.
    final var drainHandler =
        new WebSocketDrainHandler(queue, egressListener, sessionManager, metrics, nanoClock);
    transport
        .workerGroup()
        .next()
        .scheduleWithFixedDelay(
            () -> {
              try {
                drainHandler.drain();
              } catch (final Exception e) {
                LOG.error("Drain handler exception — task continues", e);
              }
            },
            0,
            1,
            TimeUnit.MILLISECONDS);

    LOG.info(
        "WebSocket server started on port {} (transport={}, TLS 1.3, {})",
        config.port(),
        transport.transportName(),
        config.tlsCertPath().isEmpty() ? "self-signed cert" : "cert=" + config.tlsCertPath());
  }

  private SslContext buildSslContext() throws Exception {
    final SslContextBuilder builder;
    if (config.tlsCertPath().isEmpty()) {
      // Dev mode: self-signed certificate
      final var ssc = new SelfSignedCertificate();
      builder = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey());
      LOG.warn("Using self-signed TLS certificate — NOT for production use");
    } else {
      builder =
          SslContextBuilder.forServer(
              new File(config.tlsCertPath()), new File(config.tlsKeyPath()));
    }

    return builder
        .sslProvider(SslProvider.OPENSSL)
        .protocols("TLSv1.3")
        .ciphers(config.cipherSuites())
        .build();
  }

  /**
   * @return true if the server is bound and listening
   */
  public boolean isRunning() {
    return serverChannel != null && serverChannel.isActive();
  }

  /**
   * @return the bound port, or -1 if not started
   */
  public int port() {
    if (serverChannel != null && serverChannel.localAddress() instanceof InetSocketAddress addr) {
      return addr.getPort();
    }
    return -1;
  }

  /** Shut down the Netty server and release all resources. Awaits event loop termination. */
  @Override
  public void close() {
    if (serverChannel != null) {
      try {
        serverChannel.close().sync();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    if (transport != null) {
      try {
        transport
            .bossGroup()
            .shutdownGracefully(0, SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)
            .await(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      try {
        transport
            .workerGroup()
            .shutdownGracefully(0, SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)
            .await(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    LOG.info("WebSocket server stopped");
  }
}
