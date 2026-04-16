package com.trading.engine.orchestrator;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import java.util.Objects;
import org.agrona.CloseHelper;
import org.agrona.concurrent.AgentRunner;

/**
 * Holds the runtime resources for the orchestrator process: the {@link AgentRunner} (wrapping the
 * {@link OrchestratorService} agent), the two outbound {@link ExclusivePublication}s (gateway and
 * pricing), and the optional owned {@link Aeron} client.
 *
 * <p><b>Lifecycle.</b> {@link #close()} stops the agent runner first (which invokes the
 * orchestrator service's {@code onClose()} — drains in-flight RFQs, publishes expiry notifications,
 * closes subscriptions), then closes the outbound publications, then closes the Aeron client if
 * this instance owns it.
 *
 * <p><b>Why the components hold publications.</b> The orchestrator service receives publications as
 * {@link Publisher} SAMs ({@code gatewayPublication::offer}) for testability — Aeron's {@code
 * ExclusivePublication} is {@code final} and cannot be subclassed. Lifecycle responsibility for the
 * underlying {@link ExclusivePublication} therefore moves up to the launcher's component holder,
 * which is why this class owns and closes them.
 *
 * <p><b>Caveat.</b> If {@link #close()} is never called (e.g., {@code kill -9}, JVM crash, or
 * uncaught exception in caller code), the {@code ExclusivePublication} instances leak. Aeron's
 * media driver eventually reclaims the underlying IPC resources when all clients disconnect, but
 * for graceful shutdown and resource monitoring callers MUST invoke {@link #close()} before process
 * termination.
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
 * @see Publisher
 */
public final class OrchestratorComponents implements AutoCloseable {

  private final AgentRunner agentRunner;
  private final ExclusivePublication gatewayPublication;
  private final ExclusivePublication pricingPublication;
  private final Aeron aeron;
  private final boolean ownsAeron;

  /**
   * Constructs an orchestrator components holder.
   *
   * @param agentRunner wraps the orchestrator service agent duty cycle; must not be null
   * @param gatewayPublication outbound publication to gateway; must not be null
   * @param pricingPublication outbound publication to pricing; must not be null
   * @param aeron Aeron client connected to the shared media driver; may be null
   * @param ownsAeron {@code true} if this holder should close the Aeron client on {@link #close()}
   */
  public OrchestratorComponents(
      final AgentRunner agentRunner,
      final ExclusivePublication gatewayPublication,
      final ExclusivePublication pricingPublication,
      final Aeron aeron,
      final boolean ownsAeron) {
    this.agentRunner = Objects.requireNonNull(agentRunner, "agentRunner");
    this.gatewayPublication = Objects.requireNonNull(gatewayPublication, "gatewayPublication");
    this.pricingPublication = Objects.requireNonNull(pricingPublication, "pricingPublication");
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
   * Shuts down the orchestrator in order:
   *
   * <ol>
   *   <li>{@code agentRunner} — triggers orchestrator's {@code onClose()} → drains active RFQs and
   *       closes subscriptions (no more inbound traffic).
   *   <li>{@code gatewayPublication}, {@code pricingPublication} — stop outbound traffic.
   *   <li>{@code aeron} (if owned) — close the Aeron client.
   * </ol>
   *
   * <p>Uses {@link CloseHelper#closeAll} so a failure in one close attempts the remainder.
   */
  @Override
  public void close() {
    // Order: agentRunner (drains RFQs + closes subscriptions) → publications (no more outbound
    // traffic possible) → aeron client (if owned). CloseHelper.closeAll suppresses per-resource
    // exceptions and continues with the remainder, so a throwing agentRunner.close() does not
    // skip publication cleanup.
    if (ownsAeron) {
      CloseHelper.closeAll(agentRunner, gatewayPublication, pricingPublication, aeron);
    } else {
      CloseHelper.closeAll(agentRunner, gatewayPublication, pricingPublication);
    }
  }
}
