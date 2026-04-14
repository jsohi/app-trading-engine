package com.trading.engine.orchestrator;

import io.aeron.Aeron;
import java.util.Objects;
import org.agrona.CloseHelper;
import org.agrona.concurrent.AgentRunner;

/**
 * Holds the runtime resources for the orchestrator process: the {@link AgentRunner} (wrapping the
 * {@link OrchestratorService} agent) and the optional owned {@link Aeron} client.
 *
 * <p><b>Lifecycle.</b> {@link #close()} stops the agent runner first (which invokes the
 * orchestrator service's {@code onClose()} — drains in-flight RFQs, publishes expiry notifications,
 * closes subscriptions and publications), then closes the Aeron client if this instance owns it.
 *
 * <p><b>Ownership model.</b> The Aeron client may be shared with other components in the same
 * process. When the orchestrator is launched standalone, it creates and owns its own Aeron client
 * ({@code ownsAeron=true}). When embedded in the top-level launcher, the launcher owns the Aeron
 * client and passes {@code ownsAeron=false}.
 *
 * <p><b>Threading.</b> Not thread-safe — call {@link #close()} from the shutdown hook or the thread
 * that created the components.
 *
 * @see OrchestratorLauncher
 * @see OrchestratorService
 */
public final class OrchestratorComponents implements AutoCloseable {

  private final AgentRunner agentRunner;
  private final Aeron aeron;
  private final boolean ownsAeron;

  /**
   * Constructs an orchestrator components holder.
   *
   * @param agentRunner wraps the orchestrator service agent duty cycle; must not be null
   * @param aeron Aeron client connected to the shared media driver; may be null
   * @param ownsAeron {@code true} if this holder should close the Aeron client on {@link #close()}
   */
  public OrchestratorComponents(
      final AgentRunner agentRunner, final Aeron aeron, final boolean ownsAeron) {
    this.agentRunner = Objects.requireNonNull(agentRunner, "agentRunner");
    this.aeron = aeron;
    this.ownsAeron = ownsAeron;
  }

  /**
   * Returns the agent runner wrapping the orchestrator service duty cycle.
   *
   * @return the orchestrator agent runner, never null
   */
  public AgentRunner agentRunner() {
    return agentRunner;
  }

  /**
   * Returns the Aeron client, or {@code null} if this holder does not track it.
   *
   * @return the Aeron client, or null if not owned
   */
  public Aeron aeron() {
    return aeron;
  }

  /**
   * Shuts down the orchestrator in order: agent runner (triggers orchestrator's onClose()), then
   * optionally the owned Aeron client. Uses {@link CloseHelper#closeAll} so a failure in one close
   * does not mask the other.
   */
  @Override
  public void close() {
    if (ownsAeron) {
      CloseHelper.closeAll(agentRunner, aeron);
    } else {
      CloseHelper.closeAll(agentRunner);
    }
  }
}
