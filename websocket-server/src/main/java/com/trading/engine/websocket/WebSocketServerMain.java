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
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
 *   → JwtAuthHandler (per-channel one-shot auth gate; installs WriteByteCounterHandler +
 *     WebSocketFrameDispatcher on success)
 * </pre>
 *
 * <p>The drain handler is no longer in the pipeline — it is scheduled as a standalone task on the
 * worker event loop at 1ms fixed rate after the server binds.
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
  private final ManyToOneConcurrentArrayQueue<EgressEntry> commandQueue;
  private final ManyToOneConcurrentArrayQueue<EgressEntry> ackQueue;
  private final CommandEntryPool commandEntryPool;
  private final WebSocketEgressListener egressListener;
  private final WebSocketSessionManager sessionManager;
  private final WebSocketMetrics metrics;
  private final JwtValidator jwtValidator;
  private final JtiRevocationCache jtiCache;
  private final UserEntitlementService entitlementService;
  private final AuthFailureTracker authFailureTracker;

  /**
   * Phase 3 Commit A — symbol → permitted-accounts map for the per-channel admission pipeline +
   * session entitlement publish. May be {@code null} in legacy wirings (pre-Phase-3 tests).
   */
  private final SymbolEntitlementMap symbolEntitlementMap;

  /**
   * Phase 3 Commit A — SAM seam over the stream-205 Aeron publication used to construct each
   * channel's {@link MarketDataAdmissionPipeline}. May be {@code null} in legacy wirings.
   */
  private final SnapshotRequestPublisher snapshotRequestPublisher;

  private final AtomicInteger pendingAuthCount = new AtomicInteger(0);
  private CommandDispatcher commandDispatcher;
  private Channel serverChannel;
  private TransportDetector.Result transport;

  /**
   * Create the WebSocket server (not yet started). Used by tests that don't exercise the
   * browser→cluster command path; commandQueue/ackQueue/commandEntryPool default to internal empty
   * wiring and the dispatcher is constructed but no commands ever reach the cluster.
   *
   * @param config server configuration
   * @param queue the egress queue (shared with AeronEgressThread)
   * @param egressListener the egress listener
   * @param sessionManager the session manager
   * @param metrics metrics instance
   * @param jwtValidator JWT validator
   * @param jtiCache JTI revocation cache
   * @param entitlementService account entitlement validator
   * @param authFailureTracker per-IP auth failure tracker
   */
  public WebSocketServerMain(
      final WebSocketServerConfig config,
      final ManyToOneConcurrentArrayQueue<EgressEntry> queue,
      final WebSocketEgressListener egressListener,
      final WebSocketSessionManager sessionManager,
      final WebSocketMetrics metrics,
      final JwtValidator jwtValidator,
      final JtiRevocationCache jtiCache,
      final UserEntitlementService entitlementService,
      final AuthFailureTracker authFailureTracker) {
    this(
        config,
        queue,
        new ManyToOneConcurrentArrayQueue<>(config.commandQueueCapacity()),
        new ManyToOneConcurrentArrayQueue<>(config.commandAckQueueCapacity()),
        new CommandEntryPool(config.commandQueueCapacity(), config.replayBufferFrameSize()),
        egressListener,
        sessionManager,
        metrics,
        jwtValidator,
        jtiCache,
        entitlementService,
        authFailureTracker,
        null,
        null);
  }

  /**
   * Create the WebSocket server with full command/ack wiring.
   *
   * @param config server configuration
   * @param queue the egress queue (shared with AeronEgressThread)
   * @param commandQueue the browser→cluster command queue (shared with AeronEgressThread)
   * @param ackQueue the cluster→browser ack back-channel queue (shared with AeronEgressThread)
   * @param commandEntryPool the dedicated pool of EgressEntry objects for the command path
   * @param egressListener the egress listener (for entry pool returns)
   * @param sessionManager the session manager
   * @param metrics metrics instance
   * @param jwtValidator JWT RS256 validator with JWKS caching (closed on server shutdown)
   * @param jtiCache JTI revocation cache for replay prevention
   * @param entitlementService account entitlement validator
   * @param authFailureTracker per-IP auth failure rate limiter
   */
  public WebSocketServerMain(
      final WebSocketServerConfig config,
      final ManyToOneConcurrentArrayQueue<EgressEntry> queue,
      final ManyToOneConcurrentArrayQueue<EgressEntry> commandQueue,
      final ManyToOneConcurrentArrayQueue<EgressEntry> ackQueue,
      final CommandEntryPool commandEntryPool,
      final WebSocketEgressListener egressListener,
      final WebSocketSessionManager sessionManager,
      final WebSocketMetrics metrics,
      final JwtValidator jwtValidator,
      final JtiRevocationCache jtiCache,
      final UserEntitlementService entitlementService,
      final AuthFailureTracker authFailureTracker) {
    this(
        config,
        queue,
        commandQueue,
        ackQueue,
        commandEntryPool,
        egressListener,
        sessionManager,
        metrics,
        jwtValidator,
        jtiCache,
        entitlementService,
        authFailureTracker,
        null,
        null);
  }

  /**
   * Full Phase 3 Commit A constructor adding the symbol-entitlement map and snapshot-request
   * publisher SAM seam. Both nullable for legacy / test wirings; when both are non-null the
   * per-channel {@link MarketDataAdmissionPipeline} is constructed at auth time and template-56
   * snapshot requests admit through the 4-stage fail-closed pipeline.
   *
   * @param config server configuration
   * @param queue the egress queue (shared with AeronEgressThread)
   * @param commandQueue browser→cluster command queue
   * @param ackQueue cluster→browser ack back-channel queue
   * @param commandEntryPool dedicated EgressEntry pool for the command path
   * @param egressListener egress listener
   * @param sessionManager session manager
   * @param metrics metrics
   * @param jwtValidator JWT validator
   * @param jtiCache JTI cache
   * @param entitlementService entitlement validator
   * @param authFailureTracker auth failure tracker
   * @param symbolEntitlementMap launcher-loaded symbol → permitted-accounts map (may be null)
   * @param snapshotRequestPublisher SAM seam over stream-205 Aeron publication (may be null)
   */
  public WebSocketServerMain(
      final WebSocketServerConfig config,
      final ManyToOneConcurrentArrayQueue<EgressEntry> queue,
      final ManyToOneConcurrentArrayQueue<EgressEntry> commandQueue,
      final ManyToOneConcurrentArrayQueue<EgressEntry> ackQueue,
      final CommandEntryPool commandEntryPool,
      final WebSocketEgressListener egressListener,
      final WebSocketSessionManager sessionManager,
      final WebSocketMetrics metrics,
      final JwtValidator jwtValidator,
      final JtiRevocationCache jtiCache,
      final UserEntitlementService entitlementService,
      final AuthFailureTracker authFailureTracker,
      final SymbolEntitlementMap symbolEntitlementMap,
      final SnapshotRequestPublisher snapshotRequestPublisher) {
    this.config = Objects.requireNonNull(config, "config");
    this.queue = Objects.requireNonNull(queue, "queue");
    this.commandQueue = Objects.requireNonNull(commandQueue, "commandQueue");
    this.ackQueue = Objects.requireNonNull(ackQueue, "ackQueue");
    this.commandEntryPool = Objects.requireNonNull(commandEntryPool, "commandEntryPool");
    this.egressListener = Objects.requireNonNull(egressListener, "egressListener");
    this.sessionManager = Objects.requireNonNull(sessionManager, "sessionManager");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    this.jwtValidator = Objects.requireNonNull(jwtValidator, "jwtValidator");
    this.jtiCache = Objects.requireNonNull(jtiCache, "jtiCache");
    this.entitlementService = Objects.requireNonNull(entitlementService, "entitlementService");
    this.authFailureTracker = Objects.requireNonNull(authFailureTracker, "authFailureTracker");
    this.symbolEntitlementMap = symbolEntitlementMap;
    this.snapshotRequestPublisher = snapshotRequestPublisher;
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

    // Create the singleton command dispatcher — used by every channel.
    commandDispatcher =
        new CommandDispatcher(
            config,
            metrics,
            nanoClock,
            commandQueue,
            new CommandDispatcher.EgressEntryAllocator() {
              @Override
              public EgressEntry tryAcquire() {
                return commandEntryPool.tryAcquire();
              }

              @Override
              public void release(final EgressEntry entry) {
                commandEntryPool.release(entry);
              }
            });

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
                    // APP-36 §A3: subprotocol pinning. Server echoes
                    // "trading-ws.v1" on the upgrade response so the client
                    // can hard-assert the protocol contract. Mismatch (or
                    // absent echo) → client closes PROTOCOL_VIOLATION.
                    // Bump to "trading-ws.vN" only on breaking changes to
                    // the frame envelope, header layout, or non-additive
                    // Auth/Ack semantics. SBE schema-version bumps inside
                    // templates do NOT bump the subprotocol — those are
                    // caught by APP-36 §2.11 schema-id check.
                    new WebSocketServerProtocolHandler(
                        "/", "trading-ws.v1", false, 65_536, false, true, 30_000));
                pipeline.addLast("rate-limiter", new ConnectionRateLimiter(rateLimiterState));
                pipeline.addLast("origin-validator", originValidator);
                // WriteByteCounterHandler must be installed BEFORE the auth handler so its
                // tally is updated for ALL outbound writes, including auth-time error frames.
                final var byteCounter = new WriteByteCounterHandler();
                pipeline.addLast("byte-counter", byteCounter);
                // JwtAuthHandler: per-channel one-shot auth gate. Dynamically adds
                // WebSocketFrameDispatcher on auth success and removes itself.
                pipeline.addLast(
                    "auth-handler",
                    new JwtAuthHandler(
                        pendingAuthCount,
                        jwtValidator,
                        jtiCache,
                        entitlementService,
                        authFailureTracker,
                        sessionManager,
                        metrics,
                        config,
                        nanoClock,
                        ForkJoinPool.commonPool(),
                        commandDispatcher,
                        byteCounter,
                        symbolEntitlementMap,
                        snapshotRequestPublisher));
              }
            });

    serverChannel = bootstrap.bind(config.port()).sync().channel();

    // Schedule the drain handler on the worker event loop — single instance serves all channels.
    // Uses scheduleWithFixedDelay (not scheduleAtFixedRate) to prevent catch-up storms when a
    // drain cycle takes longer than 1ms. The drain call is wrapped in a try-catch to prevent
    // exceptions from killing the scheduled task.
    final var drainHandler =
        new WebSocketDrainHandler(
            queue, ackQueue, commandEntryPool, egressListener, sessionManager, metrics, nanoClock);
    final var slowConsumerHandler =
        new SlowConsumerHandler(sessionManager, config, metrics, nanoClock);
    transport
        .workerGroup()
        .next()
        .scheduleWithFixedDelay(
            () -> {
              try {
                drainHandler.drain();
                slowConsumerHandler.scan();
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
    if (commandDispatcher != null) {
      try {
        commandDispatcher.close();
      } catch (final RuntimeException e) {
        LOG.warn("Error closing CommandDispatcher", e);
      }
    }
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
    // Close JwtValidator to release JWKS HTTP client resources
    try {
      jwtValidator.close();
    } catch (final Exception e) {
      LOG.error("Error closing JwtValidator", e);
    }
    LOG.info("WebSocket server stopped");
  }
}
