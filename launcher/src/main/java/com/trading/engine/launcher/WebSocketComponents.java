package com.trading.engine.launcher;

import com.trading.engine.websocket.AeronEgressThread;
import com.trading.engine.websocket.WebSocketClusterClient;
import com.trading.engine.websocket.WebSocketServerMain;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Lifecycle holder for WebSocket server components. Groups the cluster client, egress thread, and
 * Netty server for coordinated startup and shutdown.
 *
 * <p><b>Shutdown order.</b> Graceful drain: close Netty server (stops accepting new connections) →
 * stop AeronEgressThread (stops polling cluster egress) → close WebSocketClusterClient (closes
 * cluster session). This order ensures all in-flight messages are drained before the cluster
 * connection is severed.
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

  /**
   * Create a lifecycle holder for the WebSocket server components.
   *
   * @param server the Netty WebSocket server
   * @param egressThread the Aeron egress polling thread
   * @param clusterClient the Aeron cluster client
   */
  public WebSocketComponents(
      final WebSocketServerMain server,
      final AeronEgressThread egressThread,
      final WebSocketClusterClient clusterClient) {
    this.server = server;
    this.egressThread = egressThread;
    this.clusterClient = clusterClient;
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
   * Shut down all WebSocket components in order: server → egress thread → cluster client.
   *
   * <p>This is called from the shutdown hook thread. Each close is wrapped in try-catch to ensure
   * subsequent components are still closed even if one fails.
   */
  @Override
  public void close() {
    LOG.info("Shutting down WebSocket server components...");
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
    try {
      clusterClient.close();
    } catch (final Exception e) {
      LOG.error("Error closing WebSocketClusterClient", e);
    }
    LOG.info("WebSocket server components shut down");
  }
}
