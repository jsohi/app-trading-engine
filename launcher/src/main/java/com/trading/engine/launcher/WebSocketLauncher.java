package com.trading.engine.launcher;

import com.trading.engine.messages.MarketDataConstants;
import com.trading.engine.messages.clock.TradingClocks;
import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.AccountTypeEnum;
import com.trading.engine.messages.sbe.AcctIDSourceEnum;
import com.trading.engine.messages.sbe.ComplianceStatusEnum;
import com.trading.engine.projections.account.AccountReadModel;
import com.trading.engine.websocket.AeronEgressThread;
import com.trading.engine.websocket.AuthFailureTracker;
import com.trading.engine.websocket.CommandEntryPool;
import com.trading.engine.websocket.EgressEntry;
import com.trading.engine.websocket.JtiRevocationCache;
import com.trading.engine.websocket.JwtValidator;
import com.trading.engine.websocket.Log4j2DiskFullErrorHandler;
import com.trading.engine.websocket.MarketDataIngressHandler;
import com.trading.engine.websocket.MarketDataPoller;
import com.trading.engine.websocket.MarketDataSubscriptionLivenessTracker;
import com.trading.engine.websocket.SnapshotRequestPublisher;
import com.trading.engine.websocket.SymbolEntitlementMap;
import com.trading.engine.websocket.SymbolEntitlementYamlLoader;
import com.trading.engine.websocket.UserEntitlementService;
import com.trading.engine.websocket.WebSocketClusterClient;
import com.trading.engine.websocket.WebSocketEgressListener;
import com.trading.engine.websocket.WebSocketMetrics;
import com.trading.engine.websocket.WebSocketServerConfig;
import com.trading.engine.websocket.WebSocketServerMain;
import com.trading.engine.websocket.WebSocketSessionManager;
import com.trading.refdata.ReferenceDataLoadException;
import com.trading.refdata.account.AccountRecord;
import com.trading.refdata.account.YamlAccountLoader;
import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.agrona.concurrent.SystemNanoClock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.layout.PatternLayout;

/**
 * Static factory that wires and starts the Netty WebSocket server with its own Aeron Cluster client
 * session. Mirrors the {@link GatewayLauncher} pattern — encapsulates all construction behind a
 * single {@link #launch} method.
 *
 * <p><b>Construction order.</b> Config → Metrics → Queue → EgressListener → ClusterClient →
 * init(deferred wiring) → SessionManager → EgressThread → Netty Server → start.
 *
 * <p><b>Circular dependency.</b> {@code WebSocketEgressListener} needs a {@code ClusterClient}
 * reference to signal reconnection, but {@code ClusterClient} needs the listener for egress
 * polling. Resolved via deferred init: listener is constructed without the client, then {@link
 * WebSocketEgressListener#init(WebSocketClusterClient)} is called after the client is built. Same
 * pattern as {@code FixGateway.init(clusterClient, egressListener)} in the gateway module.
 *
 * <p><b>Aeron directory.</b> Shares the gateway's external Media Driver ({@code
 * aeronDirs[gwIndex]}) for IPC. Multiple cluster sessions per JVM are fully supported.
 *
 * <p><b>Metrics.</b> Creates a {@link WebSocketMetrics} instance internally. The Prometheus
 * registry for production scraping will be wired when the metrics endpoint is added (PR 3/4).
 *
 * <p><b>Threading.</b> Creates: "aeron-egress" thread (cluster polling) + Netty boss (1 thread) +
 * Netty worker (N threads).
 *
 * <p><b>Allocation.</b> All hot-path objects pre-allocated during launch().
 *
 * @see <a href="docs/websocket-architecture.md">WebSocket Architecture — Section 1</a>
 */
public final class WebSocketLauncher {

  private static final Logger LOG = LogManager.getLogger(WebSocketLauncher.class);

  private WebSocketLauncher() {} // static factory only

  /**
   * Wire and start the WebSocket server.
   *
   * @param aeronDir Aeron CnC directory for the shared Media Driver (gateway's)
   * @param ingressEndpoints comma-separated cluster ingress endpoints
   * @param configPath path to the WebSocket server YAML config file
   * @return a {@link WebSocketComponents} lifecycle handle
   * @throws Exception if config loading, TLS initialization, or port binding fails
   */
  public static WebSocketComponents launch(
      final String aeronDir, final String ingressEndpoints, final Path configPath)
      throws Exception {

    Objects.requireNonNull(aeronDir, "aeronDir");
    Objects.requireNonNull(ingressEndpoints, "ingressEndpoints");
    Objects.requireNonNull(configPath, "configPath");

    LOG.info("Launching WebSocket server: config={} aeronDir={}", configPath, aeronDir);

    // Note: Netty ResourceLeakDetector level is set by the websocket-server module at
    // startup (DISABLED in production). Tests override to PARANOID via @BeforeAll or JVM arg
    // -Dio.netty.leakDetection.level=PARANOID.

    // 1. Config
    final var config = WebSocketServerConfig.fromYaml(configPath);

    // 2. Metrics (uses WebSocketMetrics.createWithDefaults() — SimpleMeterRegistry for dev/test)
    final var metrics = WebSocketMetrics.createWithDefaults();

    // 2a. Bind Log4j2DiskFullErrorHandler to every non-console appender so disk-full / IO errors
    // reroute to ConsoleAppender + increment log.appender.failure counter. Failure to install is
    // logged but never fatal — the process must still come up if Log4j2 reflection breaks.
    try {
      final var loggerContext = (LoggerContext) LogManager.getContext(false);
      final var fallback =
          ConsoleAppender.createDefaultAppenderForLayout(PatternLayout.createDefaultLayout());
      if (!fallback.isStarted()) {
        fallback.start();
      }
      final int installed =
          Log4j2DiskFullErrorHandler.installAll(loggerContext, fallback, metrics.registry());
      LOG.info("Log4j2DiskFullErrorHandler bound to {} appenders", installed);
    } catch (final RuntimeException e) {
      LOG.warn(
          "Failed to install Log4j2DiskFullErrorHandler — appender errors will fall back "
              + "to Log4j2 DefaultErrorHandler",
          e);
    }

    // 3. Egress queues (MpscArrayQueue: Aeron → Netty, return: Netty → Aeron pool)
    final var egressQueue =
        new ManyToOneConcurrentArrayQueue<EgressEntry>(config.egressQueueCapacity());
    final var returnQueue =
        new ManyToOneConcurrentArrayQueue<EgressEntry>(config.egressQueueCapacity());

    // 4. Egress listener (constructed without clusterClient — deferred init below)
    final var egressListener =
        new WebSocketEgressListener(
            egressQueue,
            returnQueue,
            metrics,
            config.egressQueueCapacity(),
            config.replayBufferFrameSize());

    // 5. Cluster client (own Aeron session, shares gateway media driver)
    final var clusterClient =
        WebSocketClusterClient.builder()
            .aeronDirectoryName(aeronDir)
            .ingressEndpoints(ingressEndpoints)
            .egressChannel("aeron:udp?endpoint=localhost:0")
            .egressListener(egressListener)
            .errorHandler(throwable -> LOG.error("WebSocketClusterClient fatal error", throwable))
            .build();

    // 6. Deferred init — wire the circular dependency (listener → client for reconnect signaling)
    egressListener.init(clusterClient);

    // 7. Session manager
    final var sessionManager =
        new WebSocketSessionManager(config, metrics, SystemNanoClock.INSTANCE);

    // 7b. Phase 3 market-data wiring — separate Aeron client (same media driver) to attach the
    //     stream-204 ingress subscription + stream-205 snapshot-request publication. The cluster
    //     client owns its own internal Aeron client; we open a sibling client here rather than
    //     trying to share it, because the cluster client treats its Aeron handle as private.
    //     Both clients share the same external Media Driver via the aeronDir CnC.
    //
    //     Failure mode: any Phase 3 Aeron resource failing to open is fatal — we close the egress
    //     listener + cluster client and rethrow. There is no graceful-degrade path because the
    //     dispatcher's MarketDataAdmissionPipeline cannot function without the stream-205
    //     publication, and the egress thread cannot deliver market-data without the stream-204
    //     subscription. Always-on; no kill switch (per the plan).
    // Gemini cloud-review R3 G-7: Aeron.connect must be guarded by a try-catch that closes the
    // clusterClient if it throws. Use two phases so partial-success on Aeron.connect (followed by
    // a later subscription/publication open failure) ALSO closes the connected Aeron client.
    final Aeron marketDataAeron;
    try {
      marketDataAeron =
          Aeron.connect(
              new Aeron.Context()
                  .aeronDirectoryName(aeronDir)
                  .errorHandler(
                      throwable -> LOG.error("Market-data Aeron client error", throwable)));
    } catch (final Exception ex) {
      LOG.error("Market-data Aeron.connect failed", ex);
      try {
        clusterClient.close();
      } catch (final Exception closeEx) {
        LOG.error("Error closing WebSocketClusterClient during partial-failure cleanup", closeEx);
      }
      throw ex;
    }

    final Subscription marketDataSubscription;
    final ExclusivePublication snapshotRequestPublication;
    final SymbolEntitlementMap symbolEntitlementMap;
    try {
      marketDataSubscription =
          marketDataAeron.addSubscription(
              MarketDataConstants.MARKET_DATA_CHANNEL, MarketDataConstants.MARKET_DATA_STREAM_ID);
      snapshotRequestPublication =
          marketDataAeron.addExclusivePublication(
              MarketDataConstants.MARKET_DATA_CHANNEL,
              MarketDataConstants.MARKET_DATA_SNAPSHOT_REQUEST_STREAM_ID);
      symbolEntitlementMap = loadSymbolEntitlementMap();
    } catch (final Exception ex) {
      LOG.error("Market-data Aeron resource open failed (post-connect)", ex);
      try {
        marketDataAeron.close();
      } catch (final Exception closeEx) {
        LOG.error("Error closing market-data Aeron client during partial-failure cleanup", closeEx);
      }
      try {
        clusterClient.close();
      } catch (final Exception closeEx) {
        LOG.error("Error closing WebSocketClusterClient during partial-failure cleanup", closeEx);
      }
      throw ex;
    }

    // Liveness tracker — transition callback synthesises template-57 frames onto the egress queue.
    // The emitter is a final field on the tracker (bound at construction); the tracker invokes it
    // on every state transition. Single-thread invariant: all entry points fire on the aeron-egress
    // agent thread.
    final var feedStateEmitter =
        new AeronEgressThread.FeedStateChangeEmitter(
            egressListener, egressQueue, metrics, SystemNanoClock.INSTANCE);
    final var livenessTracker =
        new MarketDataSubscriptionLivenessTracker(SystemNanoClock.INSTANCE, feedStateEmitter);
    final var marketDataIngressHandler =
        new MarketDataIngressHandler(
            egressQueue, egressListener, livenessTracker, metrics, SystemNanoClock.INSTANCE);

    // Bind the SAM-seam poller to the Subscription.poll method-reference once — zero per-cycle
    // allocation thereafter.
    final MarketDataPoller marketDataPoller = marketDataSubscription::poll;

    // Bind the snapshot-request publisher seam to the Aeron offer method-reference once.
    final SnapshotRequestPublisher snapshotRequestPublisher = snapshotRequestPublication::offer;

    // 7c. Browser→cluster command pipeline queues + pool — must be SHARED between the egress
    // thread (consumer of commandQueue, producer of ackQueue) and WebSocketServerMain (producer
    // of commandQueue via the per-channel CommandDispatcher, consumer of ackQueue via the drain
    // handler). Earlier wirings created separate instances on each side, which severed the
    // command pipeline (Gemini cloud-review R3 G-6).
    final var commandQueue =
        new ManyToOneConcurrentArrayQueue<EgressEntry>(config.commandQueueCapacity());
    final var ackQueue =
        new ManyToOneConcurrentArrayQueue<EgressEntry>(config.commandAckQueueCapacity());
    final var commandEntryPool =
        new CommandEntryPool(config.commandQueueCapacity(), config.replayBufferFrameSize());

    // 8. Aeron egress thread — full Phase 3 constructor with market-data wiring AND the shared
    // command/ack/pool instances so the browser→cluster command pump and the THROTTLED-ack back-
    // channel are wired end-to-end.
    final var egressThread =
        new AeronEgressThread(
            clusterClient,
            egressQueue,
            commandQueue,
            ackQueue,
            commandEntryPool,
            metrics,
            config.egressQueueCapacity(),
            marketDataPoller,
            marketDataIngressHandler,
            livenessTracker,
            egressListener,
            SystemNanoClock.INSTANCE);
    egressThread.start();

    // 8b. Auth dependencies
    final var jwtValidator =
        new JwtValidator(
            config.issuerRegistry(), config.jwtAudience(), TradingClocks.epochNanoClock());
    final var jtiCache =
        new JtiRevocationCache(
            config.maxRevokedJtis(), config.revocationTtlMinutes(), SystemNanoClock.INSTANCE);
    // Build the account lookup table from the same accounts.yaml the cluster's
    // ReferenceDataOrchestrator loads. Resolves the YAML path the same way
    // LauncherConfig does — `-Daccounts.file` or default "accounts.yaml". Loaded
    // once at startup; lookups are non-blocking Map.get().
    //
    // APP-244 (Web UI Production Hardening umbrella) will replace this with a
    // live AccountProjection driven by cluster egress events so account state
    // changes propagate without a launcher restart. The original APP-237
    // reference in this comment was a stale handoff-doc misnomer — APP-237 in
    // Linear is security-audit/SAST, unrelated. Phase 3 corrected the mapping.
    // Until then, the YAML is the authoritative source for both the cluster (via
    // ReferenceDataOrchestrator) and this lookup, so they stay in sync by
    // construction.
    final var accountLookup = loadAccountLookupFromYaml();
    final var entitlementService = new UserEntitlementService(accountLookup::get);
    final var authFailureTracker =
        new AuthFailureTracker(
            config.authFailureLockoutThreshold(),
            config.authFailureLockoutSeconds(),
            SystemNanoClock.INSTANCE);

    // 9. Netty WebSocket server (binds port). Wrap in try-catch to clean up the egress thread
    // and cluster client on partial failure — they are already started and must be closed.
    // Pass the Phase 3 Commit A collaborators (Agent B review R1-F1/F2) so the per-channel
    // MarketDataAdmissionPipeline is installed at auth time and the per-session entitlement +
    // token-bucket state is initialised. Without these the admission pipeline is dead code in
    // production (template-56 frames always rejected with CommandRejected).
    final var server =
        new WebSocketServerMain(
            config,
            egressQueue,
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
            symbolEntitlementMap,
            snapshotRequestPublisher,
            accountLookup::get);
    try {
      server.start();
    } catch (final Exception ex) {
      LOG.error(
          "WebSocket server start failed — cleaning up server, egress thread, market-data Aeron"
              + " resources, and cluster client",
          ex);
      try {
        server.close(); // Shuts down EventLoopGroup threads created by TransportDetector.detect()
      } catch (final Exception closeEx) {
        LOG.error("Error closing WebSocketServerMain during partial-failure cleanup", closeEx);
      }
      try {
        egressThread.close();
      } catch (final Exception closeEx) {
        LOG.error("Error closing AeronEgressThread during partial-failure cleanup", closeEx);
      }
      // Agent A/B review F-4 / F-1: close the Phase 3 market-data Aeron resources too — they were
      // opened above the server.start() guard and would otherwise leak on startup failure.
      // marketDataAeron.close() closes the child Subscription + Publication, but we close them
      // explicitly first to match the production shutdown order (resource → owner).
      try {
        snapshotRequestPublication.close();
      } catch (final Exception closeEx) {
        LOG.error(
            "Error closing snapshot-request publication during partial-failure cleanup", closeEx);
      }
      try {
        marketDataSubscription.close();
      } catch (final Exception closeEx) {
        LOG.error("Error closing market-data subscription during partial-failure cleanup", closeEx);
      }
      try {
        marketDataAeron.close();
      } catch (final Exception closeEx) {
        LOG.error("Error closing market-data Aeron client during partial-failure cleanup", closeEx);
      }
      try {
        clusterClient.close();
      } catch (final Exception closeEx) {
        LOG.error("Error closing WebSocketClusterClient during partial-failure cleanup", closeEx);
      }
      throw ex;
    }

    LOG.info(
        "WebSocket server launched: port={} queueCapacity={}",
        config.port(),
        config.egressQueueCapacity());

    return new WebSocketComponents(
        server,
        egressThread,
        clusterClient,
        marketDataAeron,
        marketDataSubscription,
        snapshotRequestPublication,
        symbolEntitlementMap,
        snapshotRequestPublisher);
  }

  /**
   * Load the symbol-entitlement YAML and build the immutable {@link SymbolEntitlementMap}. Resolves
   * the file path from the {@code symbols.file} system property (default {@code symbols.yaml}) —
   * same rule shape used for {@code accounts.yaml} so launcher operators can override consistently.
   *
   * @return populated immutable {@link SymbolEntitlementMap}
   * @throws IOException if the YAML file is missing or unreadable
   */
  private static SymbolEntitlementMap loadSymbolEntitlementMap() throws IOException {
    final var path = Path.of(System.getProperty("symbols.file", "symbols.yaml"));
    return new SymbolEntitlementYamlLoader(path).load();
  }

  /**
   * Load accounts from the same YAML file the cluster consumes via {@code
   * ReferenceDataOrchestrator}, and project each {@link AccountRecord} into the {@link
   * AccountReadModel} shape {@link UserEntitlementService} requires.
   *
   * <p>Resolves the file path from the {@code accounts.file} system property (default {@code
   * accounts.yaml}) — identical to {@link LauncherConfig}'s rule, so both wiring paths read the
   * same source of truth.
   *
   * @return immutable {@code Map<accountCode, AccountReadModel>}; throws on YAML errors so a
   *     misconfigured launcher fails loud at startup rather than silently rejecting every JWT.
   */
  private static Map<String, AccountReadModel> loadAccountLookupFromYaml()
      throws ReferenceDataLoadException {
    final var path = Path.of(System.getProperty("accounts.file", "accounts.yaml"));
    final var loader = new YamlAccountLoader(path);
    final var records = loader.load();
    final var map = new HashMap<String, AccountReadModel>(records.size() * 2);
    for (final var rec : records) {
      map.put(rec.accountCode(), toReadModel(rec));
    }
    LOG.info("Loaded {} account(s) from {} for WebSocket entitlement lookup", map.size(), path);
    return Map.copyOf(map);
  }

  /**
   * Project a YAML-loaded {@link AccountRecord} into an {@link AccountReadModel} so {@link
   * UserEntitlementService#validateAccounts} can apply its status check. Enum string lookups mirror
   * the SBE codec — unknown values map to {@code NULL_VAL} so the validator's "non-active" branch
   * rejects them.
   */
  private static AccountReadModel toReadModel(final AccountRecord rec) {
    final long capabilities = rec.capabilities();
    final boolean canTrade = (capabilities & 1L) != 0L;
    final boolean canRequestQuotes = (capabilities & 2L) != 0L;
    // Phase 3 Commit B: bridge the refdata PanelSlot type to the projections PanelSlot type.
    // Both modules define a parallel record with identical shape because the projections module
    // does not (and should not) depend on reference-data — the YAML-loaded refdata.AccountRecord
    // is the cold-path source-of-truth; the projections.AccountReadModel is the read-side view
    // consumed by JwtAuthHandler.sendAuthAck. The launcher is the natural translation seam.
    final var translatedPanels =
        new ArrayList<AccountReadModel.PanelSlot>(rec.panelLayout().size());
    for (final var rp : rec.panelLayout()) {
      translatedPanels.add(new AccountReadModel.PanelSlot(rp.panelId(), rp.slot()));
    }
    return new AccountReadModel(
        rec.accountId(),
        rec.parentAccountId(),
        rec.accountCode(),
        parseAcctIdSource(rec.acctIdSource()),
        rec.accountName(),
        parseAccountType(rec.accountType()),
        rec.baseCurrency(),
        parseAccountStatus(rec.status()),
        parseComplianceStatus(rec.complianceStatus()),
        capabilities,
        canTrade,
        canRequestQuotes,
        0L,
        0L,
        0L,
        rec.symbolPreferences(),
        List.copyOf(translatedPanels));
  }

  /**
   * Per-class cache of enum-constant arrays. {@link Class#getEnumConstants()} returns a defensive
   * clone on every call; with O(N×4) startup parser invocations across accounts × enum families
   * that adds avoidable allocation pressure on launcher boot. {@link ClassValue} memoises the array
   * per enum {@link Class} with the right lifecycle semantics (one entry per loaded class; GC'd
   * when the class is unloaded). Cold path; the cache itself is never read from any cluster/gateway
   * hot path.
   */
  private static final ClassValue<Enum<?>[]> ENUM_CONSTANTS_CACHE =
      new ClassValue<>() {
        @Override
        protected Enum<?>[] computeValue(final Class<?> type) {
          return (Enum<?>[]) type.getEnumConstants();
        }
      };

  // Each parser delegates to the generic parseEnumOrNull helper. Maps
  // null/blank/unknown YAML strings to the supplied NULL_VAL fallback so the
  // entitlement validator's "non-active" branch rejects them cleanly. The
  // null guard inside parseEnumOrNull avoids the NPE that `Enum.valueOf(null)`
  // would throw and crash launcher startup. Case-insensitive comparison
  // (lowercased candidate vs lowercased enum constant name, both via
  // Locale.ROOT) handles every YAML casing variation without bias toward the
  // SBE generator's PascalCase choice — "Active" / "ACTIVE" / "active" all
  // resolve to the same constant.

  /**
   * Parses {@code raw} into the enum constant {@code E}, or returns {@code nullValue} on null,
   * blank, or unknown input. Case-insensitive — compares lowercased candidate to lowercased enum
   * constant names so YAML casing variation ("Active" vs "ACTIVE" vs "active") does not silently
   * degrade to {@code nullValue}. The SBE codec generator produces PascalCase enum constants (e.g.
   * {@code Active}, {@code Suspended}); a uniform-case {@link Enum#valueOf(Class, String)} would
   * have to pick one and the other forms would fail — case-insensitive comparison is the only
   * correct choice. Package-private for unit tests; production callers go through the four typed
   * wrappers below.
   *
   * <p>The {@code Locale.ROOT} {@code toLowerCase} avoids the Turkish dotted-i hazard ({@code "I"}
   * → {@code "ı"} on a {@code tr_TR} JVM under default-locale folding).
   *
   * <p><b>Allocation per call:</b> 1× {@code trim()} string + 1× {@code toLowerCase()} string for
   * the input + 1× {@code toLowerCase()} string per enum constant scanned (worst-case N). The
   * enum-constant ARRAY itself is cached via {@link #ENUM_CONSTANTS_CACHE}, so the per-call
   * defensive-clone of {@link Class#getEnumConstants()} is avoided — but the per-constant
   * lowercased NAMES are not cached (would require a parallel {@link ClassValue}; not worth the
   * complexity on the cold path). Cold path — launcher startup only; do NOT call from any
   * cluster/gateway hot path.
   *
   * @param raw the raw YAML string (may be null or blank)
   * @param type the enum class to look up against (must not be null)
   * @param nullValue the fallback enum constant returned on null / blank / unknown input
   * @param <E> the enum type
   * @return the parsed enum constant, or {@code nullValue} on any non-match
   */
  static <E extends Enum<E>> E parseEnumOrNull(
      final String raw, final Class<E> type, final E nullValue) {
    Objects.requireNonNull(type, "type");
    Objects.requireNonNull(nullValue, "nullValue");
    if (raw == null) {
      return nullValue;
    }
    final var trimmed = raw.trim();
    if (trimmed.isEmpty()) {
      return nullValue;
    }
    final var needle = trimmed.toLowerCase(Locale.ROOT);
    @SuppressWarnings("unchecked")
    final E[] candidates = (E[]) ENUM_CONSTANTS_CACHE.get(type);
    for (final var candidate : candidates) {
      if (candidate.name().toLowerCase(Locale.ROOT).equals(needle)) {
        return candidate;
      }
    }
    return nullValue;
  }

  private static AccountStatusEnum parseAccountStatus(final String s) {
    return parseEnumOrNull(s, AccountStatusEnum.class, AccountStatusEnum.NULL_VAL);
  }

  private static AccountTypeEnum parseAccountType(final String s) {
    return parseEnumOrNull(s, AccountTypeEnum.class, AccountTypeEnum.NULL_VAL);
  }

  private static AcctIDSourceEnum parseAcctIdSource(final String s) {
    return parseEnumOrNull(s, AcctIDSourceEnum.class, AcctIDSourceEnum.NULL_VAL);
  }

  private static ComplianceStatusEnum parseComplianceStatus(final String s) {
    return parseEnumOrNull(s, ComplianceStatusEnum.class, ComplianceStatusEnum.NULL_VAL);
  }
}
