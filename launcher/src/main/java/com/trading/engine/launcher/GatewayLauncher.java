package com.trading.engine.launcher;

import com.trading.engine.gateway.ClusterClient;
import com.trading.engine.gateway.ClusterEgressListener;
import com.trading.engine.gateway.FixGateway;
import com.trading.engine.gateway.FixToSbeTranslator;
import com.trading.engine.gateway.InFlightTracker;
import com.trading.engine.gateway.RejectEmitter;
import com.trading.engine.gateway.SbeToFixTranslator;
import com.trading.engine.gateway.SessionRegistry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SystemNanoClock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Static factory that wires and starts the FIX gateway + cluster client. Does NOT start a
 * MediaDriver — expects one running at {@code aeronDir}.
 *
 * <p><b>Construction order.</b> All hot-path objects are pre-allocated during {@link #launch}:
 * InFlightTracker → SessionRegistry → translators → RejectEmitter → FixGateway → EgressListener →
 * ClusterClient → init → AgentRunner → start thread.
 *
 * <p><b>Threading.</b> Creates one gateway duty-cycle thread named "fix-gateway" (from {@link
 * FixGateway#roleName()}).
 *
 * <p><b>Idle strategy.</b> Configurable via the {@code idleStrategy} parameter. {@link
 * org.agrona.concurrent.BackoffIdleStrategy} is reasonable for dev; production should use {@link
 * org.agrona.concurrent.YieldingIdleStrategy} (bounded ~10us, no park) or {@link
 * org.agrona.concurrent.BusySpinIdleStrategy} (dedicated core, lowest latency).
 *
 * <p><b>Allocation.</b> All hot-path objects pre-allocated during launch().
 */
public final class GatewayLauncher {

  private static final Logger LOG = LogManager.getLogger(GatewayLauncher.class);

  // Default capacities — sized for a mid-tier FIX gateway
  private static final int MAX_SESSIONS = 64;
  private static final int MAX_SESSIONS_PER_COMP_ID = 4;
  private static final int CORRELATION_CAPACITY = 1024;
  private static final int IN_FLIGHT_CAPACITY = 4096;
  private static final long IN_FLIGHT_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(30);

  private GatewayLauncher() {} // static factory only

  /**
   * Wires and starts the FIX gateway with a connected cluster client.
   *
   * @param fixHost TCP bind address for FIX connections (e.g., "localhost")
   * @param fixPort TCP port for FIX connections; must be in [1, 65535]
   * @param aeronDir Aeron CnC directory for the gateway's external Media Driver
   * @param ingressEndpoints comma-separated cluster ingress endpoints
   * @param idleStrategy idle strategy for the gateway agent runner duty cycle
   * @return a {@link GatewayComponents} handle that owns the runner thread and cluster client
   * @throws NullPointerException if any string argument is null
   * @throws IllegalArgumentException if fixHost is blank, aeronDir is blank, ingressEndpoints is
   *     blank, or fixPort is out of range
   */
  public static GatewayComponents launch(
      final String fixHost,
      final int fixPort,
      final String aeronDir,
      final String ingressEndpoints,
      final IdleStrategy idleStrategy) {
    // --- Validate inputs ---
    requireNonBlank(fixHost, "fixHost");
    requireNonBlank(aeronDir, "aeronDir");
    requireNonBlank(ingressEndpoints, "ingressEndpoints");
    if (fixPort < 1 || fixPort > 65_535) {
      throw new IllegalArgumentException("fixPort must be in [1, 65535], got: " + fixPort);
    }
    if (idleStrategy == null) {
      throw new NullPointerException("idleStrategy must not be null");
    }

    // --- Wire components ---
    final ErrorHandler errorHandler = throwable -> LOG.error("Gateway error", throwable);

    final var inFlightTracker = new InFlightTracker(IN_FLIGHT_CAPACITY, IN_FLIGHT_TIMEOUT_NS);

    final var registry =
        new SessionRegistry(MAX_SESSIONS, MAX_SESSIONS_PER_COMP_ID, CORRELATION_CAPACITY);

    final var fixToSbeTranslator = new FixToSbeTranslator();
    final var sbeToFixTranslator = new SbeToFixTranslator();
    final var rejectEmitter = new RejectEmitter();

    // TODO(APP-157): externalize allowed SenderCompIDs to configuration
    final var fixGateway =
        new FixGateway(
            fixHost,
            fixPort,
            "aeron:ipc",
            "fix-logs", // TODO(APP-205): resolve against configurable logDir
            "TRADING",
            Set.of("CLIENT1", "CLIENT2", "FIX_BRIDGE"),
            registry,
            fixToSbeTranslator,
            rejectEmitter,
            inFlightTracker,
            SystemNanoClock.INSTANCE);

    final var egressListener =
        new ClusterEgressListener(
            sbeToFixTranslator, registry, inFlightTracker, fixGateway::onEgressMessage);

    final var clusterClient =
        ClusterClient.builder()
            .aeronDirectoryName(aeronDir)
            .ingressEndpoints(ingressEndpoints)
            .egressChannel("aeron:udp?endpoint=localhost:0")
            .egressListener(egressListener)
            .errorHandler(errorHandler)
            .inFlightTracker(inFlightTracker)
            .ownsAeronClient(true)
            .build();

    fixGateway.init(clusterClient, egressListener);

    // AgentRunner.startOnThread() calls fixGateway.onStart(), which:
    //   1. Launches the Artio FIX engine + library
    //   2. Delegates to clusterClient.onStart() (via guard flag) → connects to cluster
    final var agentRunner = new AgentRunner(idleStrategy, errorHandler, null, fixGateway);
    try {
      AgentRunner.startOnThread(agentRunner);
    } catch (final RuntimeException e) {
      CloseHelper.closeAll(agentRunner, clusterClient);
      throw e;
    }

    LOG.info(
        "Gateway launched: {}:{} aeronDir={} ingressEndpoints={}",
        fixHost,
        fixPort,
        aeronDir,
        ingressEndpoints);

    return new GatewayComponents(agentRunner, clusterClient);
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
