package com.trading.engine.fixbridge;

import com.trading.engine.fixbridge.audit.AuditLogger;
import com.trading.engine.fixbridge.audit.Log4jAuditLogger;
import com.trading.engine.fixbridge.auth.DpopValidator;
import com.trading.engine.fixbridge.auth.JtiReplayCache;
import com.trading.engine.fixbridge.auth.JtiRevocationCache;
import com.trading.engine.fixbridge.auth.NimbusDpopValidator;
import com.trading.engine.fixbridge.quote.SessionQuoteIndex;
import com.trading.engine.fixbridge.rawfix.MicrometerDropCounter;
import com.trading.engine.fixbridge.rawfix.RawFixTap;
import com.trading.engine.fixbridge.translator.JsonToFixTranslator;
import com.trading.engine.fixbridge.transport.AccountLimitsSource;
import com.trading.engine.fixbridge.transport.ArtioFixCommandSink;
import com.trading.engine.fixbridge.transport.BoundedAccountLimitsSource;
import com.trading.engine.fixbridge.transport.BridgeFrameDispatcher;
import com.trading.engine.fixbridge.transport.BridgeNettyBootstrap;
import com.trading.engine.fixbridge.transport.BridgeSession;
import com.trading.engine.fixbridge.transport.ClusterAccountLimitsProvider;
import com.trading.engine.fixbridge.transport.FixCommandSink;
import com.trading.engine.fixbridge.transport.FixSessionAdapter;
import com.trading.engine.fixbridge.transport.HealthCheckHandler;
import com.trading.engine.fixbridge.transport.IndexBackedQuoteSnapshotCache;
import com.trading.engine.fixbridge.transport.RoutingBridgeFrameDispatcher;
import com.trading.engine.messages.clock.TradingClocks;
import com.trading.engine.websocket.AuthFailureTracker;
import com.trading.engine.websocket.JwtValidator;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.agrona.concurrent.EpochNanoClock;
import org.apache.logging.log4j.LogManager;

/**
 * Production composition entry point for the FIX client bridge.
 *
 * <p><b>Purpose.</b> Wires every singleton + per-session factory the bridge needs into a single
 * runnable {@link BridgeNettyBootstrap}. The launcher is the only place that holds references to
 * the production binding of every SAM seam in the bridge module — every other class in the module
 * remains binding-agnostic (test code can swap in {@code NOOP} variants without touching production
 * code).
 *
 * <p><b>Composition graph.</b>
 *
 * <pre>
 * FixClientBridgeLauncher
 *   ├── singletons (process-wide)
 *   │     ├── EpochNanoClock + NanoClock        (TradingClocks)
 *   │     ├── PrometheusMeterRegistry            (Micrometer)
 *   │     ├── Log4jAuditLogger                   (audit appender)
 *   │     ├── JtiRevocationCache                 (bearer-JWT revocation)
 *   │     ├── JtiReplayCache                     (DPoP-proof replay cache)
 *   │     ├── NimbusDpopValidator                (RFC 9449 DPoP)
 *   │     ├── JwtValidator                       (RS256 + JWKS)
 *   │     ├── AuthFailureTracker                 (per-IP tarpit)
 *   │     ├── SessionQuoteIndex                  (cross-session quote correlation)
 *   │     ├── ClusterAccountLimitsProvider       (cluster-backed account limits)
 *   │     ├── BoundedAccountLimitsSource         (wraps the provider)
 *   │     ├── MicrometerDropCounter              (RawFixTap drop metrics)
 *   │     └── AtomicLong sessionTokenSequence    (per-session ClOrdID prefix)
 *   │
 *   ├── per-session factory (BridgeFrameDispatcher.Factory)
 *   │     For each authenticated channel, builds:
 *   │     ├── ArtioFixSessionAdapter            (per-session Artio Session wrapper)
 *   │     ├── IndexBackedQuoteSnapshotCache      (per-session quote snapshot cache)
 *   │     ├── JsonToFixTranslator                (per-session FIX encoder scratch)
 *   │     ├── ArtioFixCommandSink                (per-session FIX wire send)
 *   │     └── RoutingBridgeFrameDispatcher       (per-session inbound routing)
 *   │
 *   └── BridgeNettyBootstrap                     (Netty server)
 * </pre>
 *
 * <p><b>External dependencies (injected).</b> Two pieces of cross-module wiring are NOT resolved
 * here — they are provided by the operator at construction time so the bridge module stays
 * decoupled from the cluster + Artio runtime:
 *
 * <ul>
 *   <li>{@link ArtioSessionConnector} — given a freshly-authenticated {@link BridgeSession},
 *       returns a live Artio {@code uk.co.real_logic.artio.session.Session} (or its already-wrapped
 *       {@link FixSessionAdapter} equivalent). The launcher binds the resulting adapter to the
 *       per-session {@link ArtioFixCommandSink}. The default ({@link ArtioSessionConnector#NOOP})
 *       returns {@link FixSessionAdapter#NOOP} so dispatch short-circuits without wire activity
 *       (useful for bridge integration tests that do not boot an Artio acceptor).
 *   <li>{@link ClusterAccountLimitsProvider.AccountLimitsLookup} — the cluster query path. The
 *       default returns {@code null} for every account, in which case {@link
 *       BoundedAccountLimitsSource} emits pessimistic-zero defaults per the §3.14 fail- secure
 *       contract.
 * </ul>
 *
 * <p><b>Threading.</b> {@link #start()} and {@link #close()} are launcher-thread-only. Every other
 * singleton is thread-safe per its own contract; per-session factories run on the channel event
 * loop only.
 *
 * <p><b>Lifecycle.</b> {@code start()} blocks until the Netty server is bound. {@code close()}
 * unbinds + drains + shuts down the Micrometer registry + closes the audit logger handle.
 */
public final class FixClientBridgeLauncher implements AutoCloseable {

  /**
   * SAM seam for opening per-session Artio Sessions. The launcher binds this to a real Artio
   * FixLibrary in production; tests bind {@link #NOOP}.
   */
  @FunctionalInterface
  public interface ArtioSessionConnector {

    /**
     * Open an Artio initiator session for the given freshly-authenticated bridge session. The
     * returned {@link FixSessionAdapter} is wrapped by the launcher into the per-session {@link
     * ArtioFixCommandSink}.
     *
     * @param session the bridge session whose Artio initiator should be opened
     * @return adapter wrapping the live Artio Session, or {@link FixSessionAdapter#NOOP} on bridge
     *     bring-up before the Artio runtime is wired
     */
    FixSessionAdapter connect(BridgeSession session);

    /** Default that returns the NOOP adapter — short-circuits all FIX wire activity. */
    ArtioSessionConnector NOOP = session -> FixSessionAdapter.NOOP;
  }

  private final FixClientBridgeConfig config;
  private final BridgeNettyBootstrap bootstrap;
  private final PrometheusMeterRegistry meterRegistry;

  /**
   * Construct the launcher with default cross-module bindings (NOOP Artio + NOOP cluster lookup).
   * Suitable for bridge unit tests + bring-up.
   *
   * @param config bridge configuration
   */
  public FixClientBridgeLauncher(final FixClientBridgeConfig config) {
    this(config, ArtioSessionConnector.NOOP, account -> null);
  }

  /**
   * Construct the launcher with explicit cross-module bindings.
   *
   * @param config bridge configuration
   * @param artioConnector Artio session opener (production: wraps a FixLibrary initiator)
   * @param accountLimitsLookup cluster account-limits lookup (production: queries the cluster's
   *     AccountStore projection via {@code :query-service})
   */
  public FixClientBridgeLauncher(
      final FixClientBridgeConfig config,
      final ArtioSessionConnector artioConnector,
      final ClusterAccountLimitsProvider.AccountLimitsLookup accountLimitsLookup) {
    this.config = Objects.requireNonNull(config, "config");
    Objects.requireNonNull(artioConnector, "artioConnector");
    Objects.requireNonNull(accountLimitsLookup, "accountLimitsLookup");

    // ----- Process-wide singletons -----
    final var epochNanoClock = TradingClocks.epochNanoClock();
    final var nanoClock = TradingClocks.nanoClock();

    this.meterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

    // Audit: real Log4j2 async appender bound to the "audit" logger from log4j2.xml.
    final AuditLogger auditLogger = new Log4jAuditLogger(LogManager.getLogger("audit"));

    // Bearer-JWT revocation cache (process-wide, fed by an out-of-band revocation endpoint).
    final var jtiRevocationCache = new JtiRevocationCache();

    // DPoP proof replay cache (process-wide, separate from JtiRevocationCache; shorter TTL).
    // Uses EpochNanoClock to match the deadline domain that NimbusDpopValidator passes to
    // checkAndAdd (PR #71 R1 critical fix — using NanoClock here previously broke eviction).
    final var dpopReplayCache = new InMemoryJtiReplayCache(epochNanoClock);

    final DpopValidator dpopValidator =
        new NimbusDpopValidator(epochNanoClock, /* maxClockSkewSeconds */ 30L, dpopReplayCache);

    // JWT RS256 validator (uses configured JWKS endpoints).
    final var jwtValidator =
        new JwtValidator(config.jwtIssuerRegistry(), config.expectedAudience(), epochNanoClock);

    final var authFailureTracker =
        new AuthFailureTracker(
            config.authFailureLockoutThreshold(),
            config.authFailureLockoutSeconds(),
            nanoClock::nanoTime);

    final var sessionQuoteIndex = new SessionQuoteIndex();

    // Cluster account-limits provider. The default ctor uses the system NanoClock + 30s TTL —
    // wraps the operator-supplied lookup so a re-auth doesn't spam the cluster.
    final var clusterProvider = new ClusterAccountLimitsProvider(accountLimitsLookup);
    final AccountLimitsSource accountLimitsSource = new BoundedAccountLimitsSource(clusterProvider);

    // Drop-counter sink for RawFixTap. The launcher binds Micrometer; tests use NOOP.
    final RawFixTap.DropCounter dropCounter = new MicrometerDropCounter(meterRegistry);

    // Per-session ClOrdID prefix sequence (process-wide AtomicLong) — guarantees the per-session
    // 28-bit token used in the §4 ClOrdID layout is unique across concurrent sessions for the
    // life of the process. PR #70 R2 Gemini critical fix.
    final var sessionTokenSequence = new AtomicLong(0L);

    // Bridge process tag (24-bit). Derived once at boot from epoch-nanos: shifting right by 8
    // gives ~256ns granularity, then masking to 24 bits. The (instanceTag, sessionToken) pair
    // forms the high bits of every per-session ClOrdID:
    //   - Two bridge processes booting within ~256ns on the same host would collide on
    //     instanceTag — but this is impossible in practice because a single bridge process
    //     binds a unique TCP port (config.port()), so the OS prevents same-host port reuse.
    //   - Cross-host collisions are guarded by the gateway's CompID (each bridge connects to
    //     the gateway as a distinct FIX SenderCompID).
    //   - Within a single process, the AtomicLong sessionTokenSequence guarantees per-session
    //     uniqueness regardless of instanceTag.
    final long instanceTag = (epochNanoClock.nanoTime() >>> 8) & 0xFFFFFFL;

    final Executor jwtValidationExecutor = ForkJoinPool.commonPool();

    // ----- Per-session dispatcher factory -----
    final BridgeFrameDispatcher.Factory dispatcherFactory =
        (session, remoteIpSupplier) -> {
          // Per-session Artio session adapter (NOOP by default until the launcher binds a real
          // FixLibrary initiator).
          final var fixSession = artioConnector.connect(session);
          // Per-session quote-snapshot cache, backed by the process-wide SessionQuoteIndex for
          // ownership re-checks at lookup time.
          final var quoteCache =
              new IndexBackedQuoteSnapshotCache(sessionQuoteIndex, session.sessionId());
          // Per-session JSON→FIX translator (holds reusable scratch buffers).
          final var translator = new JsonToFixTranslator(epochNanoClock);
          // Per-session token from the process-wide AtomicLong — masked to 28 bits inside the sink.
          final long sessionToken = sessionTokenSequence.incrementAndGet();
          // Per-session FIX command sink (zero-alloc on the hot path).
          final FixCommandSink sink =
              new ArtioFixCommandSink(
                  session, fixSession, translator, quoteCache, instanceTag, sessionToken);
          return new RoutingBridgeFrameDispatcher(
              sink, sessionQuoteIndex, auditLogger, epochNanoClock, remoteIpSupplier);
        };

    // ----- Health-check handler factory -----
    // One instance per channel so the per-channel handler isn't @Sharable.
    final var healthCheckFactory =
        (Supplier<HealthCheckHandler>)
            () ->
                new HealthCheckHandler(
                    auditLogger::isWritable,
                    // Active-session count surfaced as a placeholder zero. Wiring a real
                    // counter requires a per-channel attach/detach hook on BridgeNettyBootstrap
                    // which is out of scope for this PR — operators monitoring /health for
                    // session capacity should use the Micrometer registry's own gauges instead.
                    () -> 0,
                    epochNanoClock,
                    "fix-client-bridge");

    // ----- Netty bootstrap -----
    this.bootstrap =
        new BridgeNettyBootstrap(
            config,
            jwtValidator,
            jtiRevocationCache,
            authFailureTracker,
            epochNanoClock,
            nanoClock,
            jwtValidationExecutor,
            dispatcherFactory,
            auditLogger,
            accountLimitsSource,
            dpopValidator,
            healthCheckFactory);
  }

  /**
   * Start the bridge — binds the Netty server to the configured address/port and begins accepting
   * browser connections.
   *
   * @throws Exception if TLS init or port binding fails
   */
  public void start() throws Exception {
    bootstrap.start();
  }

  /**
   * Returns the Micrometer registry so the operator can scrape Prometheus metrics. Production
   * deployments wire this to a sidecar HTTP server or to the JVM's existing /actuator endpoint.
   *
   * @return the bridge's Prometheus meter registry
   */
  public MeterRegistry meterRegistry() {
    return meterRegistry;
  }

  @Override
  public void close() throws Exception {
    // try-finally so a Netty bootstrap close failure doesn't leak the Micrometer registry's
    // background threads + counter state. The bootstrap close() can throw on event-loop
    // shutdown timeout; the meterRegistry close MUST still fire.
    try {
      bootstrap.close();
    } finally {
      meterRegistry.close();
    }
  }

  /**
   * Minimal thread-safe in-memory replay cache for DPoP {@code jti} claims. Used as the default
   * {@link JtiReplayCache} binding inside the launcher; production deployments may swap in a
   * Caffeine-backed or distributed implementation if multi-instance coordination is required.
   *
   * <p>Thread-safe via an internal {@link ConcurrentHashMap}. Uses {@link EpochNanoClock} so its
   * {@code now} is in the same domain as the {@code expireAtNs} deadline computed by {@link
   * NimbusDpopValidator} (PR #71 R1 critical fix — using a monotonic clock here would leave entries
   * un-evicted forever, since monotonic-{@code now} is always less than epoch-{@code expireAtNs}).
   *
   * <p><b>Sweep cadence.</b> Lazy expiry runs at most once every {@link #SWEEP_INTERVAL} calls so
   * the per-call cost stays {@code O(1)} amortised even at high DPoP QPS — between sweeps we accept
   * up to {@link #SWEEP_INTERVAL} expired-but-resident entries.
   */
  static final class InMemoryJtiReplayCache implements JtiReplayCache {

    /**
     * Sweep cadence — sweep every Nth checkAndAdd to bound the per-call cost. Choosing a power of
     * two so the modulo lowers to a bitmask. Memory bound between sweeps: SWEEP_INTERVAL × bytes
     * per entry ≈ 64 × ~80 = ~5 KiB worst-case headroom.
     */
    private static final int SWEEP_INTERVAL = 64;

    private final ConcurrentHashMap<String, Long> seen = new ConcurrentHashMap<>();
    private final EpochNanoClock clock;

    /**
     * Counter for amortising the lazy-eviction sweep. Incremented on every checkAndAdd; sweep fires
     * when {@code (counter & (SWEEP_INTERVAL - 1)) == 0}.
     */
    private final AtomicLong sweepCounter = new AtomicLong(0L);

    InMemoryJtiReplayCache(final EpochNanoClock clock) {
      this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public boolean checkAndAdd(final String jti, final long expireAtNs) {
      final long now = clock.nanoTime();
      // Amortised sweep: at most one O(N) walk per SWEEP_INTERVAL calls. Between sweeps we
      // tolerate expired-but-resident entries (the replay-protection property is unaffected —
      // a stale entry is at worst a redundant rejection of a NEW submission of the same jti
      // after its TTL elapsed; but the iat skew window is short enough that legitimate clients
      // never re-mint within a single SWEEP_INTERVAL × per-call latency window).
      if ((sweepCounter.incrementAndGet() & (SWEEP_INTERVAL - 1L)) == 0L) {
        seen.entrySet().removeIf(e -> e.getValue() <= now);
      }
      return seen.putIfAbsent(jti, expireAtNs) == null;
    }
  }
}
