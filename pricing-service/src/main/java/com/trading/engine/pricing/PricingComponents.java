package com.trading.engine.pricing;

import io.aeron.Aeron;
import java.util.Objects;
import org.agrona.CloseHelper;
import org.agrona.concurrent.AgentRunner;

/**
 * Holds the runtime resources for the pricing service process: the {@link AgentRunner} (wrapping
 * the {@code PricingService} agent) and the optional owned {@link Aeron} client.
 *
 * <p><b>Lifecycle.</b> {@link #close()} stops the agent runner first (which invokes the pricing
 * service agent's {@code onClose()} -- drains in-flight quote responses, releases SBE flyweights),
 * then closes the Aeron client if this instance owns it. The two-phase ordering mirrors the
 * convention in the gateway module ({@link com.trading.engine.launcher.GatewayComponents}) and
 * ensures the agent's final duty-cycle iteration completes before the underlying transport is torn
 * down.
 *
 * <p><b>Ownership model.</b> The Aeron client may be shared with other components in the same
 * process (e.g., a co-located gateway or event logger). When the pricing service is launched
 * standalone, it creates and owns its own Aeron client ({@code ownsAeron=true}). When embedded in
 * the top-level {@code TradingEngineLauncher}, the launcher owns the Aeron client and passes {@code
 * ownsAeron=false}.
 *
 * <p><b>Threading.</b> Not thread-safe -- call {@link #close()} from the shutdown hook or the
 * thread that created the components.
 */
public final class PricingComponents implements AutoCloseable {

  /** Agent runner wrapping the pricing service duty cycle. */
  private final AgentRunner agentRunner;

  /**
   * Aeron client connected to the shared media driver. May be {@code null} if the caller elected
   * not to track the Aeron instance (e.g., the client is managed elsewhere).
   */
  private final Aeron aeron;

  /**
   * Whether this holder owns the {@link #aeron} client and should close it in {@link #close()}.
   * When {@code false}, the Aeron client's lifecycle is managed externally.
   */
  private final boolean ownsAeron;

  /**
   * Constructs a pricing components holder.
   *
   * @param agentRunner wraps the pricing service agent duty cycle; must not be null
   * @param aeron Aeron client connected to the shared media driver; may be null if the caller does
   *     not want this holder to track or close the client
   * @param ownsAeron {@code true} if this holder should close the Aeron client on {@link #close()};
   *     ignored when {@code aeron} is null
   */
  public PricingComponents(
      final AgentRunner agentRunner, final Aeron aeron, final boolean ownsAeron) {
    this.agentRunner = Objects.requireNonNull(agentRunner, "agentRunner");
    this.aeron = aeron;
    this.ownsAeron = ownsAeron;
  }

  /**
   * Returns the agent runner wrapping the pricing service duty cycle.
   *
   * @return the agent runner; never null
   */
  public AgentRunner agentRunner() {
    return agentRunner;
  }

  /**
   * Returns the Aeron client, or {@code null} if this holder does not track it.
   *
   * @return the Aeron client, or null
   */
  public Aeron aeron() {
    return aeron;
  }

  /**
   * Shuts down the pricing service in order: agent runner (triggers pricing agent's onClose()),
   * then optionally the owned Aeron client. Uses {@link CloseHelper#closeAll} so a failure in one
   * close does not mask the other.
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
