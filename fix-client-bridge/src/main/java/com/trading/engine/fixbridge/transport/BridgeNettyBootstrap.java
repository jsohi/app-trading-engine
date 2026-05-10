package com.trading.engine.fixbridge.transport;

import com.trading.engine.fixbridge.FixClientBridgeConfig;
import com.trading.engine.fixbridge.audit.AuditLogger;
import com.trading.engine.fixbridge.auth.DpopValidator;
import com.trading.engine.fixbridge.auth.JtiRevocationCache;
import com.trading.engine.fixbridge.auth.JwtAuthHandler;
import com.trading.engine.fixbridge.json.BrowserEventWriter;
import com.trading.engine.fixbridge.translator.DecimalStringEmitter;
import com.trading.engine.websocket.AuthFailureTracker;
import com.trading.engine.websocket.JwtValidator;
import com.trading.engine.websocket.TransportDetector;
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
import io.netty.handler.timeout.IdleStateHandler;
import java.io.File;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.NanoClock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * FIX client bridge Netty server bootstrap (§3.1).
 *
 * <p>Builds the WebSocket server pipeline that fronts the bridge:
 *
 * <pre>
 * SslHandler (TLS 1.3, BoringSSL, self-signed in dev)
 *   → HttpServerCodec
 *   → HttpObjectAggregator(maxJsonBytes)
 *   → WebSocketHandshaker (Origin allowlist, CSWSH defence) — shared singleton
 *   → WebSocketServerProtocolHandler ("/ws", subprotocol "trading-bridge.v1",
 *     allowExtensions=false, maxFramePayloadLength=maxJsonBytes)
 *   → IdleStateHandler (reader/writer idle from config)
 *   → JwtAuthHandler (per-channel one-shot; on success installs InboundReadGate + WsListener)
 * </pre>
 *
 * <p><b>Transport.</b> Reuses {@link TransportDetector} from {@code :websocket-server} for
 * Epoll/KQueue/NIO selection so the bridge inherits the same native-transport behaviour as the
 * direct browser-facing server.
 *
 * <p><b>TLS.</b> WSS-only per plan §3.1: any {@code ws://} URL is a misconfiguration and is caught
 * by the OS-level absence of any plaintext bind path here (the bootstrap unconditionally installs
 * {@link SslContext}). Production deployments supply real cert/key paths via {@link
 * FixClientBridgeConfig#tlsCertPath()} and {@link FixClientBridgeConfig#tlsKeyPath()}; dev runs
 * default to a runtime-generated {@link SelfSignedCertificate} only when the operator opts in via
 * {@link FixClientBridgeConfig#allowSelfSignedCert()}. Misconfiguration (no cert AND no self-signed
 * flag) fails fast at {@link FixClientBridgeConfig}'s compact ctor — the bootstrap never reaches a
 * fail-OPEN state.
 *
 * <p><b>Threading.</b> NOT thread-safe. {@link #start()} and {@link #close()} must be called from
 * the launcher thread only. Channel handlers are created per channel by the {@link
 * ChannelInitializer}.
 *
 * <p><b>Lifecycle.</b> Constructed by the bridge launcher with all collaborators wired up, then
 * {@link #start()} once. {@link #close()} releases the boss/worker event-loop groups.
 *
 * @see FixClientBridgeConfig
 * @see JwtAuthHandler
 * @see WsListener
 */
public final class BridgeNettyBootstrap implements AutoCloseable {

  private static final Logger LOG = LogManager.getLogger(BridgeNettyBootstrap.class);

  /** Subprotocol pinning per APP-36 §A3 — a breaking change bumps the version suffix. */
  public static final String SUBPROTOCOL = "trading-bridge.v1";

  /** WebSocket upgrade path. */
  public static final String WS_PATH = "/ws";

  /** Graceful shutdown deadline for event loops. */
  private static final long SHUTDOWN_TIMEOUT_SECONDS = 5L;

  /**
   * Netty write-buffer hysteresis. Mirror of {@code :websocket-server} defaults so a bridge channel
   * that runs into TCP congestion looks identical (from a backpressure perspective) to a direct
   * browser connection.
   */
  private static final int WRITE_LOW_WATER_BYTES = 131_072;

  private static final int WRITE_HIGH_WATER_BYTES = 2_097_152;

  private final FixClientBridgeConfig config;
  private final JwtValidator jwtValidator;
  private final JtiRevocationCache jtiCache;
  private final AuthFailureTracker authFailureTracker;
  private final EpochNanoClock epochNanoClock;
  private final NanoClock nanoClock;
  private final Executor jwtValidationExecutor;
  private final BridgeFrameDispatcher dispatcher;
  private final AuditLogger auditLogger;
  private final AccountLimitsSource accountLimitsSource;

  private TransportDetector.Result transport;
  private Channel serverChannel;

  /**
   * Construct the bootstrap. The bridge launcher passes in shared collaborators so the same JWT
   * validator + JTI cache + audit sink are reused by every channel.
   *
   * @param config bridge configuration
   * @param jwtValidator shared validator (preflight already invoked by caller)
   * @param jtiCache shared JTI revocation cache
   * @param authFailureTracker per-IP tarpit (shared across all channels)
   * @param epochNanoClock wall-clock used for revocation TTL checks
   * @param nanoClock monotonic clock used for rate-limiter and audit timestamps
   * @param jwtValidationExecutor executor for async JWT validation (typically {@code
   *     ForkJoinPool.commonPool()})
   * @param dispatcher post-auth frame dispatcher SAM (use {@link BridgeFrameDispatcher#NOOP} for
   *     bring-up; the real dispatcher lands in subsequent days)
   * @param auditLogger audit sink
   * @param accountLimitsSource source of {@link
   *     com.trading.engine.fixbridge.json.BrowserEvent.AccountLimits} push frames emitted on
   *     AUTH_SUCCESS — use {@link AccountLimitsSource#NOOP} until the launcher's cluster client is
   *     wired (APP-40b)
   */
  public BridgeNettyBootstrap(
      final FixClientBridgeConfig config,
      final JwtValidator jwtValidator,
      final JtiRevocationCache jtiCache,
      final AuthFailureTracker authFailureTracker,
      final EpochNanoClock epochNanoClock,
      final NanoClock nanoClock,
      final Executor jwtValidationExecutor,
      final BridgeFrameDispatcher dispatcher,
      final AuditLogger auditLogger,
      final AccountLimitsSource accountLimitsSource) {
    this.config = Objects.requireNonNull(config, "config");
    this.jwtValidator = Objects.requireNonNull(jwtValidator, "jwtValidator");
    this.jtiCache = Objects.requireNonNull(jtiCache, "jtiCache");
    this.authFailureTracker = Objects.requireNonNull(authFailureTracker, "authFailureTracker");
    this.epochNanoClock = Objects.requireNonNull(epochNanoClock, "epochNanoClock");
    this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
    this.jwtValidationExecutor =
        Objects.requireNonNull(jwtValidationExecutor, "jwtValidationExecutor");
    this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
    this.auditLogger = Objects.requireNonNull(auditLogger, "auditLogger");
    this.accountLimitsSource = Objects.requireNonNull(accountLimitsSource, "accountLimitsSource");
  }

  /**
   * Start the Netty server and bind to the configured address/port.
   *
   * @throws Exception if TLS initialisation or port binding fails
   */
  public void start() throws Exception {
    transport = TransportDetector.detect();
    final var sslCtx = buildSslContext(config);
    final var allowed = new HashSet<>(config.allowedOrigins());
    final var handshaker = new WebSocketHandshaker(allowed);

    final var bootstrap = new ServerBootstrap();
    bootstrap
        .group(transport.bossGroup(), transport.workerGroup())
        .channel(transport.channelClass())
        .option(ChannelOption.SO_BACKLOG, 128)
        .childOption(ChannelOption.SO_KEEPALIVE, true)
        .childOption(
            ChannelOption.WRITE_BUFFER_WATER_MARK,
            new WriteBufferWaterMark(WRITE_LOW_WATER_BYTES, WRITE_HIGH_WATER_BYTES))
        .childHandler(
            new ChannelInitializer<SocketChannel>() {
              @Override
              protected void initChannel(final SocketChannel ch) {
                final var pipeline = ch.pipeline();
                pipeline.addLast("ssl", sslCtx.newHandler(ch.alloc()));
                pipeline.addLast("http-codec", new HttpServerCodec());
                pipeline.addLast(
                    "http-aggregator", new HttpObjectAggregator(config.maxJsonBytes()));
                pipeline.addLast("origin-validator", handshaker);
                // Netty 4.1 WebSocketServerProtocolHandler 7-arg ctor signature:
                //   (String websocketPath, String subprotocols,
                //    boolean allowExtensions, int maxFrameSize,
                //    boolean allowMaskMismatch, boolean checkStartsWith,
                //    long handshakeTimeoutMillis)
                // allowExtensions=false: CRIME/BREACH prevention. Subprotocol is pinned so the
                // browser can hard-assert on the upgrade response (§3.1 / APP-36 §A3).
                // checkStartsWith=false: HARD-MATCH the path (only "/ws", not "/wsfoo" or
                // "/ws/admin") — security regression fix per CodeRabbit critical finding on
                // PR #70 (prior arg order had checkStartsWith=true accidentally).
                // allowMaskMismatch=true: tolerate browser quirks where the client mask handling
                // diverges from RFC 6455 §5.3 (rare in modern browsers but defensive).
                pipeline.addLast(
                    "ws-protocol",
                    new WebSocketServerProtocolHandler(
                        WS_PATH,
                        SUBPROTOCOL,
                        /* allowExtensions */ false,
                        config.maxJsonBytes(),
                        /* allowMaskMismatch */ true,
                        /* checkStartsWith */ false,
                        config.handshakeTimeoutMillis()));
                pipeline.addLast(
                    "idle",
                    new IdleStateHandler(
                        config.idleReaderSeconds(), config.idleWriterSeconds(), 0));
                // Per-channel BrowserEventWriter — paired with a per-channel
                // DecimalStringEmitter scratch (zero-alloc on hot path). The writer is shared
                // between JwtAuthHandler (cold-path Error frames) and OutboundDrainer (hot-path
                // event serialisation) — the channel event loop guarantees they're never invoked
                // concurrently, satisfying the writer's not-thread-safe contract.
                final var eventWriter = new BrowserEventWriter(new DecimalStringEmitter());
                pipeline.addLast(
                    "auth-handler",
                    new JwtAuthHandler(
                        config,
                        jwtValidator,
                        jtiCache,
                        authFailureTracker,
                        epochNanoClock,
                        nanoClock,
                        jwtValidationExecutor,
                        dispatcher,
                        auditLogger,
                        eventWriter,
                        accountLimitsSource,
                        DpopValidator.NOOP));
              }
            });

    serverChannel =
        bootstrap.bind(new InetSocketAddress(config.bindAddress(), config.port())).sync().channel();

    LOG.info(
        "FIX client bridge WS listener bound to {}:{} (transport={}, allowedOrigins={})",
        config.bindAddress(),
        config.port(),
        transport.transportName(),
        allowed.size());
  }

  /**
   * @return {@code true} if the server is bound and listening
   */
  public boolean isRunning() {
    return serverChannel != null && serverChannel.isActive();
  }

  /**
   * @return the bound port, or {@code -1} if not started
   */
  public int boundPort() {
    if (serverChannel != null && serverChannel.localAddress() instanceof InetSocketAddress addr) {
      return addr.getPort();
    }
    return -1;
  }

  /** Shut down the Netty server and release event-loop resources. */
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
            .shutdownGracefully(0L, SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)
            .await(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      try {
        transport
            .workerGroup()
            .shutdownGracefully(0L, SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)
            .await(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    LOG.info("FIX client bridge WS listener stopped");
  }

  /**
   * Build the TLS context. Three modes (compact-ctor validation rejects all other shapes):
   *
   * <ol>
   *   <li>Real cert mode — {@code config.tlsCertPath()} and {@code config.tlsKeyPath()} both set.
   *       Loads the PEM files via {@link SslContextBuilder#forServer(File, File)}. This is the only
   *       acceptable production path.
   *   <li>Self-signed dev mode — both cert/key paths null AND {@code allowSelfSignedCert=true}.
   *       Generates a runtime {@link SelfSignedCertificate}. Permitted only when the operator has
   *       explicitly opted in via the config flag; dev/test runs default here so contributors don't
   *       need to provision a cert.
   *   <li>Misconfiguration — both cert/key null AND {@code allowSelfSignedCert=false}. The compact
   *       constructor of {@link FixClientBridgeConfig} rejects this with a fail-fast {@link
   *       IllegalArgumentException}, so this method never sees that case.
   * </ol>
   *
   * @param config the validated bridge configuration
   * @return an SslContext ready for the Netty pipeline
   * @throws Exception if the SelfSignedCertificate generator or the SslContext builder fails
   */
  private static SslContext buildSslContext(final FixClientBridgeConfig config) throws Exception {
    final var certPath = config.tlsCertPath();
    final var keyPath = config.tlsKeyPath();
    if (certPath != null && !certPath.isEmpty() && keyPath != null && !keyPath.isEmpty()) {
      LOG.info("FIX client bridge TLS: real cert mode (cert={} key={})", certPath, keyPath);
      return SslContextBuilder.forServer(new File(certPath), new File(keyPath))
          .sslProvider(SslProvider.OPENSSL)
          .protocols("TLSv1.3")
          .build();
    }
    // Self-signed dev mode — compact ctor guarantees allowSelfSignedCert=true here, otherwise it
    // would have thrown before we got this far.
    final var ssc = new SelfSignedCertificate();
    LOG.warn(
        "FIX client bridge TLS: self-signed dev cert (allowSelfSignedCert=true). "
            + "Production deploys MUST set tlsCertPath/tlsKeyPath and allowSelfSignedCert=false.");
    return SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey())
        .sslProvider(SslProvider.OPENSSL)
        .protocols("TLSv1.3")
        .build();
  }
}
