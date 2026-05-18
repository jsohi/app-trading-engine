package com.trading.engine.launcher;

import com.trading.engine.websocket.AeronEgressThread;
import com.trading.engine.websocket.SnapshotRequestPublisher;
import com.trading.engine.websocket.SymbolEntitlementMap;
import com.trading.engine.websocket.WebSocketClusterClient;
import com.trading.engine.websocket.WebSocketServerMain;
import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import java.util.Objects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Lifecycle holder for WebSocket server components. Groups the cluster client, egress thread, and
 * Netty server for coordinated startup and shutdown.
 *
 * <p><b>Shutdown order.</b> Graceful drain: close MetricsHttpServer (stops Prometheus scrapes
 * BEFORE the meters they read disappear — avoids spurious 500s during the shutdown window) → close
 * Netty server (stops accepting new connections) → stop AeronEgressThread (stops polling cluster
 * egress) → close market-data Aeron resources (snapshot-request publication first, then ingress
 * subscription, then sibling Aeron client) → close WebSocketClusterClient (closes cluster session).
 * The market-data resources are closed BEFORE the cluster client because the egress thread is
 * already stopped (no more polling) — the publication/subscription teardown is then safe.
 *
 * <p><b>Threading.</b> Created and closed from the launcher main thread and shutdown hook thread
 * respectively. Fields are final — safe for cross-thread access.
 *
 * @see <a href="docs/websocket-architecture.md">WebSocket Architecture — Section 5</a>
 */
public final class WebSocketComponents implements AutoCloseable {

  private static final Logger LOG = LogManager.getLogger(WebSocketComponents.class);

  private final WebSocketServerMain server;
  private final AeronEgressThread egressThread;
  private final WebSocketClusterClient clusterClient;
  private final Aeron marketDataAeron;
  private final Subscription marketDataSubscription;
  private final ExclusivePublication snapshotRequestPublication;
  private final SymbolEntitlementMap symbolEntitlementMap;
  private final SnapshotRequestPublisher snapshotRequestPublisher;
  private final MetricsHttpServer metricsServer;

  /**
   * Create a lifecycle holder for the WebSocket server components including Phase 3 market-data
   * resources. All collaborators required.
   *
   * @param server the Netty WebSocket server
   * @param egressThread the Aeron egress polling thread
   * @param clusterClient the Aeron cluster client
   * @param marketDataAeron the sibling Aeron client that owns the market-data subscription +
   *     snapshot-request publication; may be {@code null} for legacy paths
   * @param marketDataSubscription Aeron subscription on the market-data IPC stream ({@code
   *     MARKET_DATA_STREAM_ID = 204}); may be {@code null}
   * @param snapshotRequestPublication Aeron exclusive publication on the snapshot-request stream
   *     ({@code MARKET_DATA_SNAPSHOT_REQUEST_STREAM_ID = 205})
   * @param symbolEntitlementMap immutable per-symbol → permitted-accounts map loaded at launcher
   *     boot
   * @param snapshotRequestPublisher SAM seam over the snapshot-request publication's {@code
   *     offer(...)}
   * @param metricsServer the Prometheus metrics HTTP endpoint; closed first during shutdown so
   *     scrapes stop before downstream meters disappear
   */
  public WebSocketComponents(
      final WebSocketServerMain server,
      final AeronEgressThread egressThread,
      final WebSocketClusterClient clusterClient,
      final Aeron marketDataAeron,
      final Subscription marketDataSubscription,
      final ExclusivePublication snapshotRequestPublication,
      final SymbolEntitlementMap symbolEntitlementMap,
      final SnapshotRequestPublisher snapshotRequestPublisher,
      final MetricsHttpServer metricsServer) {
    this.server = Objects.requireNonNull(server, "server");
    this.egressThread = Objects.requireNonNull(egressThread, "egressThread");
    this.clusterClient = Objects.requireNonNull(clusterClient, "clusterClient");
    this.marketDataAeron = Objects.requireNonNull(marketDataAeron, "marketDataAeron");
    this.marketDataSubscription =
        Objects.requireNonNull(marketDataSubscription, "marketDataSubscription");
    this.snapshotRequestPublication =
        Objects.requireNonNull(snapshotRequestPublication, "snapshotRequestPublication");
    this.symbolEntitlementMap =
        Objects.requireNonNull(symbolEntitlementMap, "symbolEntitlementMap");
    this.snapshotRequestPublisher =
        Objects.requireNonNull(snapshotRequestPublisher, "snapshotRequestPublisher");
    this.metricsServer = Objects.requireNonNull(metricsServer, "metricsServer");
  }

  /**
   * @return the cluster client for readiness polling
   */
  public WebSocketClusterClient clusterClient() {
    return clusterClient;
  }

  /**
   * @return the Netty server for port/status checks
   */
  public WebSocketServerMain server() {
    return server;
  }

  /**
   * @return the {@link SymbolEntitlementMap} for downstream wiring (e.g. JwtAuthHandler), or {@code
   *     null} if the Phase 3 market-data path was not wired
   */
  public SymbolEntitlementMap symbolEntitlementMap() {
    return symbolEntitlementMap;
  }

  /**
   * @return the {@link SnapshotRequestPublisher} SAM for installing the dispatcher's admission
   *     pipeline on each channel, or {@code null} if the Phase 3 market-data path was not wired
   */
  public SnapshotRequestPublisher snapshotRequestPublisher() {
    return snapshotRequestPublisher;
  }

  /**
   * Shut down all WebSocket components in order: server → egress thread → market-data resources →
   * cluster client.
   *
   * <p>This is called from the shutdown hook thread. Each close is wrapped in try-catch to ensure
   * subsequent components are still closed even if one fails.
   */
  @Override
  public void close() {
    LOG.info("Shutting down WebSocket server components...");
    // Metrics endpoint first — stops Prometheus scrapes BEFORE the downstream meters disappear so
    // a scrape that races the shutdown does not see a partially-torn-down registry.
    try {
      metricsServer.close();
    } catch (final Exception e) {
      LOG.error("Error closing MetricsHttpServer", e);
    }
    try {
      server.close();
    } catch (final Exception e) {
      LOG.error("Error closing WebSocket server", e);
    }
    try {
      egressThread.close();
    } catch (final Exception e) {
      LOG.error("Error closing AeronEgressThread", e);
    }
    // Close Phase 3 market-data resources before the cluster client — the egress thread is
    // stopped so neither resource is still in use. Publication first, then subscription, then
    // the Aeron client they share — matches Aeron lifecycle conventions (resource → owner).
    try {
      snapshotRequestPublication.close();
    } catch (final Exception e) {
      LOG.error("Error closing snapshot-request publication", e);
    }
    try {
      marketDataSubscription.close();
    } catch (final Exception e) {
      LOG.error("Error closing market-data subscription", e);
    }
    try {
      marketDataAeron.close();
    } catch (final Exception e) {
      LOG.error("Error closing market-data Aeron client", e);
    }
    try {
      clusterClient.close();
    } catch (final Exception e) {
      LOG.error("Error closing WebSocketClusterClient", e);
    }
    LOG.info("WebSocket server components shut down");
  }
}
