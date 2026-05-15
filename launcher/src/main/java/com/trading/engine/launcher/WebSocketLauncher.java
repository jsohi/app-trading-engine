package com.trading.engine.launcher;

import com.trading.engine.messages.clock.TradingClocks;
import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.AccountTypeEnum;
import com.trading.engine.messages.sbe.AcctIDSourceEnum;
import com.trading.engine.messages.sbe.ComplianceStatusEnum;
import com.trading.engine.projections.account.AccountReadModel;
import com.trading.engine.websocket.AeronEgressThread;
import com.trading.engine.websocket.AuthFailureTracker;
import com.trading.engine.websocket.EgressEntry;
import com.trading.engine.websocket.JtiRevocationCache;
import com.trading.engine.websocket.JwtValidator;
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
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.agrona.concurrent.SystemNanoClock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

    // 8. Aeron egress thread (starts polling immediately)
    final var egressThread =
        new AeronEgressThread(clusterClient, egressQueue, metrics, config.egressQueueCapacity());
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
    // APP-237 will replace this with a live AccountProjection driven by cluster
    // egress events so account state changes propagate without a launcher restart.
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
    final var server =
        new WebSocketServerMain(
            config,
            egressQueue,
            egressListener,
            sessionManager,
            metrics,
            jwtValidator,
            jtiCache,
            entitlementService,
            authFailureTracker);
    try {
      server.start();
    } catch (final Exception ex) {
      LOG.error(
          "WebSocket server start failed — cleaning up server, egress thread, and cluster client",
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

    return new WebSocketComponents(server, egressThread, clusterClient);
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
    final Map<String, AccountReadModel> map = new HashMap<>(records.size() * 2);
    for (final var rec : records) {
      map.put(rec.accountCode(), toReadModel(rec));
    }
    LOG.info(
        "Loaded {} account(s) from {} for WebSocket entitlement lookup", map.size(), path);
    return Map.copyOf(map);
  }

  /**
   * Project a YAML-loaded {@link AccountRecord} into an {@link AccountReadModel} so {@link
   * UserEntitlementService#validateAccounts} can apply its status check. Enum string lookups
   * mirror the SBE codec — unknown values map to {@code NULL_VAL} so the validator's "non-active"
   * branch rejects them.
   */
  private static AccountReadModel toReadModel(final AccountRecord rec) {
    final long capabilities = rec.capabilities();
    final boolean canTrade = (capabilities & 1L) != 0L;
    final boolean canRequestQuotes = (capabilities & 2L) != 0L;
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
        0L);
  }

  private static AccountStatusEnum parseAccountStatus(final String s) {
    try {
      return AccountStatusEnum.valueOf(s);
    } catch (final IllegalArgumentException ex) {
      return AccountStatusEnum.NULL_VAL;
    }
  }

  private static AccountTypeEnum parseAccountType(final String s) {
    try {
      return AccountTypeEnum.valueOf(s);
    } catch (final IllegalArgumentException ex) {
      return AccountTypeEnum.NULL_VAL;
    }
  }

  private static AcctIDSourceEnum parseAcctIdSource(final String s) {
    try {
      return AcctIDSourceEnum.valueOf(s);
    } catch (final IllegalArgumentException ex) {
      return AcctIDSourceEnum.NULL_VAL;
    }
  }

  private static ComplianceStatusEnum parseComplianceStatus(final String s) {
    try {
      return ComplianceStatusEnum.valueOf(s);
    } catch (final IllegalArgumentException ex) {
      return ComplianceStatusEnum.NULL_VAL;
    }
  }
}
