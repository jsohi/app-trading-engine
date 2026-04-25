package com.trading.engine.launcher;

import com.trading.engine.websocket.AeronEgressThread;
import com.trading.engine.websocket.EgressEntry;
import com.trading.engine.websocket.WebSocketClusterClient;
import com.trading.engine.websocket.WebSocketEgressListener;
import com.trading.engine.websocket.WebSocketMetrics;
import com.trading.engine.websocket.WebSocketServerConfig;
import com.trading.engine.websocket.WebSocketServerMain;
import com.trading.engine.websocket.WebSocketSessionManager;
import java.nio.file.Path;
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

    // 1. Config
    final var config = WebSocketServerConfig.fromYaml(configPath);

    // 2. Metrics (uses WebSocketMetrics.createWithPrometheus() which owns the registry)
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

    // 9. Netty WebSocket server (binds port)
    final var server =
        new WebSocketServerMain(config, egressQueue, egressListener, sessionManager, metrics);
    server.start();

    LOG.info(
        "WebSocket server launched: port={} queueCapacity={}",
        config.port(),
        config.egressQueueCapacity());

    return new WebSocketComponents(server, egressThread, clusterClient);
  }
}
