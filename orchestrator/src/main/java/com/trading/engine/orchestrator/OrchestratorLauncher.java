package com.trading.engine.orchestrator;

import static com.trading.engine.orchestrator.OrchestratorConstants.DEFAULT_MAX_ACTIVE_RFQS;
import static com.trading.engine.orchestrator.OrchestratorConstants.DEFAULT_PENDING_PRICE_TIMEOUT_NANOS;
import static com.trading.engine.orchestrator.OrchestratorConstants.DEFAULT_PENDING_VALIDATION_TIMEOUT_NANOS;
import static com.trading.engine.orchestrator.OrchestratorConstants.DEFAULT_QUOTED_TIMEOUT_NANOS;
import static com.trading.engine.orchestrator.OrchestratorConstants.GATEWAY_REQUEST_STREAM_ID;
import static com.trading.engine.orchestrator.OrchestratorConstants.GATEWAY_RESPONSE_STREAM_ID;
import static com.trading.engine.orchestrator.OrchestratorConstants.IPC_CHANNEL;
import static com.trading.engine.orchestrator.OrchestratorConstants.PRICING_REQUEST_STREAM_ID;
import static com.trading.engine.orchestrator.OrchestratorConstants.PRICING_RESPONSE_STREAM_ID;
import static com.trading.engine.orchestrator.OrchestratorConstants.SWEEP_INTERVAL_NANOS;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.trading.engine.messages.clock.TradingClocks;
import com.trading.engine.orchestrator.codec.OrchestratorMessageEncoder;
import io.aeron.Aeron;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.IdleStrategy;

/**
 * Static factory that wires and starts the orchestrator service agent. Does NOT start a MediaDriver
 * — expects one running at {@code aeronDir}.
 *
 * <p><b>Construction order.</b> All hot-path objects are pre-allocated during {@link #launch}:
 * Aeron connect → gateway sub/pub → pricing pub/sub → OrchestratorIdGenerator → RfqStateMachine →
 * OrchestratorMessageEncoder → OrchestratorService → AgentRunner → start thread.
 *
 * <p><b>Threading.</b> Creates one orchestrator duty-cycle thread named "orchestrator" (from the
 * agent's {@code roleName()}).
 *
 * <p><b>Idle strategy.</b> Configurable via the {@code idleStrategy} parameter. {@link
 * org.agrona.concurrent.BackoffIdleStrategy} is reasonable for dev; production should use {@link
 * org.agrona.concurrent.YieldingIdleStrategy} or {@link
 * org.agrona.concurrent.BusySpinIdleStrategy}.
 *
 * <p><b>Allocation.</b> All hot-path objects pre-allocated during launch(). No heap allocation on
 * the orchestrator duty-cycle hot path after launch completes.
 *
 * @see OrchestratorComponents
 * @see OrchestratorService
 */
public final class OrchestratorLauncher {

  private static final Log LOG = LogFactory.getLog(OrchestratorLauncher.class);

  private OrchestratorLauncher() {} // static factory only

  /**
   * Wires and starts the orchestrator service with all components connected.
   *
   * <p>The method performs the full 14-step construction sequence:
   *
   * <ol>
   *   <li>Validate inputs
   *   <li>Connect to the shared Media Driver at {@code aeronDir}
   *   <li>Create gateway inbound Subscription (IPC, stream 100)
   *   <li>Create gateway outbound ExclusivePublication (IPC, stream 101)
   *   <li>Create pricing outbound ExclusivePublication (IPC, stream 200)
   *   <li>Create pricing inbound Subscription (IPC, stream 201)
   *   <li>Construct {@link OrchestratorIdGenerator} with prefix "QTE"
   *   <li>Construct {@link RfqStateMachine} with per-state timeouts
   *   <li>Construct {@link OrchestratorMessageEncoder}
   *   <li>Get clocks from {@link TradingClocks}
   *   <li>Construct {@link OrchestratorService} with all dependencies
   *   <li>Create {@link AgentRunner}
   *   <li>Start agent thread
   *   <li>Return {@link OrchestratorComponents}
   * </ol>
   *
   * @param aeronDir Aeron CnC directory for the external Media Driver; must not be blank
   * @param idleStrategy idle strategy for the orchestrator agent runner duty cycle; must not be
   *     null
   * @return an {@link OrchestratorComponents} handle that owns the runner thread and Aeron client
   * @throws NullPointerException if {@code idleStrategy} is null
   * @throws IllegalArgumentException if {@code aeronDir} is blank
   */
  public static OrchestratorComponents launch(
      final String aeronDir, final IdleStrategy idleStrategy) {

    // --- Step 1: Validate inputs ---
    requireNonBlank(aeronDir, "aeronDir");
    if (idleStrategy == null) {
      throw new NullPointerException("idleStrategy must not be null");
    }

    LOG.info().append("Launching orchestrator: aeronDir=").append(aeronDir).commit();

    // --- Step 2: Connect to shared Media Driver ---
    final var aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));

    try {
      return launchWithAeron(aeron, idleStrategy, true);
    } catch (final RuntimeException e) {
      CloseHelper.closeAll(aeron);
      throw e;
    }
  }

  /**
   * Internal wiring method given an established Aeron client.
   *
   * @param aeron connected Aeron client
   * @param idleStrategy idle strategy for the agent runner
   * @param ownsAeron whether the returned components should close the Aeron client
   * @return a fully wired OrchestratorComponents
   */
  private static OrchestratorComponents launchWithAeron(
      final Aeron aeron, final IdleStrategy idleStrategy, final boolean ownsAeron) {

    // --- Step 3: Create gateway inbound subscription ---
    final var gatewaySubscription = aeron.addSubscription(IPC_CHANNEL, GATEWAY_REQUEST_STREAM_ID);

    // --- Step 4: Create gateway outbound publication ---
    final var gatewayPublication =
        aeron.addExclusivePublication(IPC_CHANNEL, GATEWAY_RESPONSE_STREAM_ID);

    // --- Step 5: Create pricing outbound publication ---
    final var pricingPublication =
        aeron.addExclusivePublication(IPC_CHANNEL, PRICING_REQUEST_STREAM_ID);

    // --- Step 6: Create pricing inbound subscription ---
    final var pricingSubscription = aeron.addSubscription(IPC_CHANNEL, PRICING_RESPONSE_STREAM_ID);

    // --- Step 7: Construct OrchestratorIdGenerator ---
    final var quoteIdGenerator = new OrchestratorIdGenerator("QTE");

    // --- Step 8: Construct RfqStateMachine ---
    final var stateMachine =
        new RfqStateMachine(
            DEFAULT_MAX_ACTIVE_RFQS,
            DEFAULT_PENDING_PRICE_TIMEOUT_NANOS,
            DEFAULT_QUOTED_TIMEOUT_NANOS,
            DEFAULT_PENDING_VALIDATION_TIMEOUT_NANOS);

    // --- Step 9: Construct OrchestratorMessageEncoder ---
    final var encoder = new OrchestratorMessageEncoder();

    // --- Step 10: Get clocks ---
    final var epochClock = TradingClocks.epochNanoClock();
    final var nanoClock = TradingClocks.nanoClock();

    // --- Step 11: Construct OrchestratorService ---
    final var orchestratorService =
        new OrchestratorService(
            gatewaySubscription,
            gatewayPublication,
            pricingSubscription,
            pricingPublication,
            stateMachine,
            quoteIdGenerator,
            encoder,
            nanoClock,
            epochClock,
            SWEEP_INTERVAL_NANOS);

    // --- Step 12-13: Create and start AgentRunner ---
    final ErrorHandler errorHandler =
        throwable ->
            LOG.error()
                .append("Orchestrator error: ")
                .append(throwable.getClass().getName())
                .append(" - ")
                .append(throwable.getMessage())
                .commit();

    final var agentRunner = new AgentRunner(idleStrategy, errorHandler, null, orchestratorService);

    try {
      AgentRunner.startOnThread(agentRunner);
    } catch (final RuntimeException e) {
      CloseHelper.closeAll(agentRunner);
      throw e;
    }

    LOG.info()
        .append("Orchestrator launched: pool=")
        .append(DEFAULT_MAX_ACTIVE_RFQS)
        .append(" quoteIdPrefix=QTE")
        .commit();

    // --- Step 14: Return components ---
    return new OrchestratorComponents(agentRunner, aeron, ownsAeron);
  }

  private static void requireNonBlank(final String value, final String name) {
    if (value == null) {
      throw new NullPointerException(name + " must not be null");
    }
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
  }
}
