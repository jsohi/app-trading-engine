package com.trading.engine.launcher;

import com.trading.engine.gateway.ClusterClient;
import io.aeron.Aeron;
import java.util.Objects;
import org.agrona.CloseHelper;
import org.agrona.concurrent.AgentRunner;

/**
 * Holds the runtime resources for the FIX gateway process: the {@link AgentRunner} (wrapping {@link
 * com.trading.engine.gateway.FixGateway}) and the {@link ClusterClient}.
 *
 * <p><b>Lifecycle.</b> {@link #close()} stops the agent runner first (which invokes {@link
 * com.trading.engine.gateway.FixGateway#onClose()} — drains in-flight commands, sends FIX Logouts),
 * then closes the cluster client (which closes its owned Aeron client when {@code
 * ownsAeronClient=true}).
 *
 * <p><b>Threading.</b> Not thread-safe — call {@link #close()} from the shutdown hook or the thread
 * that created the components.
 */
public final class GatewayComponents implements AutoCloseable {

  private final AgentRunner agentRunner;
  private final ClusterClient clusterClient;
  private final Aeron ipcAeron;

  /**
   * @param agentRunner wraps the {@link com.trading.engine.gateway.FixGateway} duty cycle
   * @param clusterClient cluster connection; closed after the agent runner to allow graceful drain
   * @param ipcAeron Aeron client for orchestrator IPC; closed after agent runner and cluster client
   */
  public GatewayComponents(
      final AgentRunner agentRunner, final ClusterClient clusterClient, final Aeron ipcAeron) {
    this.agentRunner = Objects.requireNonNull(agentRunner, "agentRunner");
    this.clusterClient = Objects.requireNonNull(clusterClient, "clusterClient");
    this.ipcAeron = ipcAeron; // nullable — null when orchestrator is not wired
  }

  /**
   * Returns the agent runner wrapping the FIX gateway duty cycle.
   *
   * @return the agent runner; never null
   */
  public AgentRunner agentRunner() {
    return agentRunner;
  }

  /**
   * Returns the cluster client used for cluster communication.
   *
   * @return the cluster client; never null
   */
  public ClusterClient clusterClient() {
    return clusterClient;
  }

  /**
   * Shuts down the gateway in order: agent runner (triggers FIX drain + Logout), then cluster
   * client. Uses {@link CloseHelper#closeAll} so a failure in one close does not mask the other.
   */
  @Override
  public void close() {
    CloseHelper.closeAll(agentRunner, clusterClient, ipcAeron);
  }
}
