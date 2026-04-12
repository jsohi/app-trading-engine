package com.trading.engine.launcher;

import com.trading.refdata.ClusterCommandSender;
import com.trading.refdata.ReferenceDataLoadException;
import com.trading.refdata.ReferenceDataOrchestrator;
import com.trading.refdata.ResponseCollector;
import com.trading.refdata.account.AccountCommandEncoder;
import com.trading.refdata.account.YamlAccountLoader;
import io.aeron.Aeron;
import io.aeron.cluster.client.AeronCluster;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.agrona.CloseHelper;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.SystemNanoClock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Top-level entry point that boots the full trading engine: media drivers, 3-node Aeron Cluster,
 * reference data loading, and the FIX gateway.
 *
 * <p><b>Startup invariant.</b> MediaDriver → Cluster → ReferenceData (await ALL acks) → Gateway.
 * The FIX acceptor must NOT bind until all reference data is confirmed loaded.
 *
 * <p><b>Startup sequence (12 steps):</b>
 *
 * <ol>
 *   <li>Parse + validate config via {@link LauncherConfig#fromSystemProperties()}
 *   <li>Create log directory for per-process media driver output
 *   <li>Register shutdown hook EARLY (before any resource creation)
 *   <li>Spawn media driver processes (one per cluster node + one for gateway)
 *   <li>Validate driver liveness via {@link Aeron#connect} with timeout
 *   <li>Build cluster member + ingress endpoint strings
 *   <li>Launch cluster nodes via {@link ClusterNodeLauncher#launch}
 *   <li>Load reference data (subsumes leader election via {@link AeronCluster#connect})
 *   <li>Launch gateway via {@link GatewayLauncher#launch}
 *   <li>Wait for gateway cluster client to reach CONNECTED state (30 s timeout)
 *   <li>Log SYSTEM_READY event with total startup time
 *   <li>Block on {@link ShutdownSignalBarrier#await()}
 * </ol>
 *
 * <p><b>Partial failure.</b> The shutdown hook is registered before any resources are created (Step
 * 3). On any failure, the exception propagates out of {@code main()}, the JVM exits, and the
 * shutdown hook fires for orderly cleanup. No {@code System.exit()} call is needed.
 *
 * <p><b>Media driver crash detection.</b> Each spawned driver's {@link Process#onExit()} callback
 * signals the {@link ShutdownSignalBarrier}, unblocking the main thread and triggering orderly
 * shutdown.
 *
 * <p><b>Threading.</b> Single main thread for startup orchestration. Gateway duty cycle runs on its
 * own named thread ("fix-gateway"). Shutdown hook runs on "trading-engine-shutdown".
 */
public final class TradingEngineLauncher {

  private static final Logger LOG = LogManager.getLogger(TradingEngineLauncher.class);

  /** JVM args required for Aeron's unsafe memory access (Agrona, Archive, Cluster). */
  private static final List<String> AERON_JVM_ARGS =
      List.of(
          "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
          "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED");

  private static final long DRIVER_CONNECT_TIMEOUT_MS = 15_000;
  private static final long REF_DATA_MESSAGE_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);

  private TradingEngineLauncher() {}

  /**
   * Main entry point. Boots the full trading engine.
   *
   * @param args ignored — all configuration via system properties
   */
  public static void main(final String[] args) throws Exception {
    final long launchStartNs = System.nanoTime();

    // ===== Step 1: Parse + validate config =====
    final var config = LauncherConfig.fromSystemProperties();
    LOG.info(
        "Configuration: fixHost={} fixPort={} nodeCount={} baseDir={} logDir={}",
        config.fixHost(),
        config.fixPort(),
        config.nodeCount(),
        config.baseDir(),
        config.logDir());

    // ===== Step 2: Create log directory =====
    final var logDir = Path.of(config.logDir());
    Files.createDirectories(logDir);
    final var pidDir = logDir.resolve("pids");
    Files.createDirectories(pidDir);

    // ===== Step 3: Register shutdown hook EARLY (P0-4) =====
    // AtomicReference/AtomicReferenceArray provide happens-before visibility between the
    // main thread (which writes) and the shutdown hook thread (which reads). Plain array
    // element writes have no cross-thread visibility guarantee — arrays are not volatile.
    final var mediaDriverProcesses = new AtomicReferenceArray<Process>(config.nodeCount() + 1);
    final var clusterNodes = new AtomicReferenceArray<ClusterComponents>(config.nodeCount());
    final var gatewayRef = new AtomicReference<GatewayComponents>();

    final var barrier = new ShutdownSignalBarrier();

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  LOG.info("Shutdown hook triggered — cleaning up in reverse order");
                  CloseHelper.quietClose(gatewayRef.get());
                  for (int i = clusterNodes.length() - 1; i >= 0; i--) {
                    CloseHelper.quietClose(clusterNodes.get(i));
                  }
                  destroyAllMediaDrivers(
                      mediaDriverProcesses, config.driverShutdownTimeoutSeconds());
                  cleanupPidFiles(pidDir, mediaDriverProcesses.length());
                },
                "trading-engine-shutdown"));

    try {
      // ===== Step 4: Spawn media driver processes =====
      long stepStart = System.nanoTime();
      final var aeronDirs = new String[config.nodeCount() + 1];
      for (int i = 0; i < config.nodeCount(); i++) {
        aeronDirs[i] = "/tmp/aeron-node-" + i;
        final var driverProc = spawnMediaDriver(aeronDirs[i], logDir, pidDir, i);
        mediaDriverProcesses.set(i, driverProc);
        registerCrashHandler(driverProc, i, barrier);
      }
      // Gateway media driver
      final int gwIndex = config.nodeCount();
      aeronDirs[gwIndex] = "/tmp/aeron-gateway";
      final var gwDriverProc = spawnMediaDriver(aeronDirs[gwIndex], logDir, pidDir, gwIndex);
      mediaDriverProcesses.set(gwIndex, gwDriverProc);
      registerCrashHandler(gwDriverProc, gwIndex, barrier);
      LOG.info(
          "Step 4 complete: {} media drivers spawned in {}ms",
          mediaDriverProcesses.length(),
          elapsedMs(stepStart));

      // ===== Step 5: Validate driver liveness (P1-6) =====
      stepStart = System.nanoTime();
      for (int i = 0; i < aeronDirs.length; i++) {
        final long driverStart = System.nanoTime();
        try (var ignored =
            Aeron.connect(
                new Aeron.Context()
                    .aeronDirectoryName(aeronDirs[i])
                    .driverTimeoutMs(DRIVER_CONNECT_TIMEOUT_MS))) {
          LOG.info("Media driver {} ready: {}ms", i, elapsedMs(driverStart));
        }
      }
      LOG.info("Step 5 complete: all drivers validated in {}ms", elapsedMs(stepStart));

      // ===== Step 6: Build cluster member strings =====
      final var clusterMembers = ClusterConfig.buildClusterMembers(config.nodeCount());
      final var ingressEndpoints = ClusterConfig.ingressEndpoints(config.nodeCount());
      LOG.info("Cluster members: {}", clusterMembers);

      // ===== Step 7: Launch cluster nodes =====
      stepStart = System.nanoTime();
      for (int i = 0; i < config.nodeCount(); i++) {
        clusterNodes.set(
            i, ClusterNodeLauncher.launch(i, config.baseDir(), aeronDirs[i], clusterMembers));
        LOG.info("Cluster node {} launched", i);
      }
      LOG.info(
          "Step 7 complete: {} cluster nodes launched in {}ms",
          config.nodeCount(),
          elapsedMs(stepStart));

      // ===== Step 8: Load reference data =====
      stepStart = System.nanoTime();
      loadReferenceData(aeronDirs[gwIndex], ingressEndpoints, config.accountsFile());
      LOG.info("Step 8 complete: reference data loaded in {}ms", elapsedMs(stepStart));

      // ===== Step 9: Launch gateway =====
      stepStart = System.nanoTime();
      // TODO(APP-185): make idle strategy configurable via LauncherConfig
      gatewayRef.set(
          GatewayLauncher.launch(
              config.fixHost(),
              config.fixPort(),
              aeronDirs[gwIndex],
              ingressEndpoints,
              new BackoffIdleStrategy()));
      LOG.info("Step 9 complete: gateway thread started in {}ms", elapsedMs(stepStart));

      // ===== Step 10: Wait for gateway readiness =====
      // AgentRunner.startOnThread() only spawns the thread — FixGateway.onStart() runs
      // asynchronously (launches Artio engine, connects ClusterClient). Poll until the
      // cluster client is connected before declaring SYSTEM_READY.
      stepStart = System.nanoTime();
      final long readinessDeadlineNs = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
      while (!gatewayRef.get().clusterClient().isConnected()) {
        if (System.nanoTime() > readinessDeadlineNs) {
          throw new IllegalStateException("Gateway failed to connect to cluster within 30 seconds");
        }
        Thread.sleep(100);
      }
      LOG.info("Step 10 complete: gateway connected to cluster in {}ms", elapsedMs(stepStart));

    } catch (final Exception e) {
      LOG.error("Startup failed", e);
      throw e; // JVM exits main() → shutdown hook fires → orderly cleanup
    }

    // ===== Step 11: SYSTEM_READY (P1-7) =====
    LOG.info(
        "SYSTEM_READY: trading engine fully operational, total startup={}ms",
        elapsedMs(launchStartNs));

    // ===== Step 12: Block until shutdown signal =====
    barrier.await();
    LOG.info("Shutdown signal received — exiting main()");
  }

  // ===========================================================================
  // Reference data loading
  // ===========================================================================

  /**
   * Creates a temporary AeronCluster connection, loads reference data, then closes the connection.
   * The AeronCluster.connect() call inherently waits for leader election.
   */
  private static void loadReferenceData(
      final String gatewayAeronDir, final String ingressEndpoints, final String accountsFile)
      throws ReferenceDataLoadException {

    final var collector = new ResponseCollector();
    final var egressBridge = new RefDataEgressBridge(collector);

    // Temporary cluster connection for ref-data loading only. ownsAeronClient=true ensures
    // the internally-created Aeron client is closed when this connection closes.
    final var ctx =
        new AeronCluster.Context()
            .aeronDirectoryName(gatewayAeronDir)
            .ingressEndpoints(ingressEndpoints)
            .egressChannel("aeron:udp?endpoint=localhost:0")
            .controlledEgressListener(egressBridge)
            .messageTimeoutNs(REF_DATA_MESSAGE_TIMEOUT_NS)
            .ownsAeronClient(true);

    LOG.info("Connecting to cluster for reference data loading...");
    try (var cluster = AeronCluster.connect(ctx)) {
      LOG.info(
          "Connected to cluster: sessionId={} leader={}",
          cluster.clusterSessionId(),
          cluster.leaderMemberId());

      final ClusterCommandSender sender = cluster::offer;
      final Runnable pollEgress = () -> cluster.controlledPollEgress();
      final var orchestrator = new ReferenceDataOrchestrator(SystemNanoClock.INSTANCE);

      // Load accounts
      orchestrator.load(
          new YamlAccountLoader(Path.of(accountsFile)),
          new AccountCommandEncoder(),
          sender,
          pollEgress,
          collector);
      LOG.info("Account reference data loaded successfully");

      // TODO(APP-196): load currencies and risk limits when loaders are implemented
    }
  }

  // ===========================================================================
  // Media driver process management
  // ===========================================================================

  /**
   * Spawns a media driver process via ProcessBuilder. Output is redirected to per-process log files
   * (P1-2). PID is written to a file for orphan detection (P1-5).
   */
  private static Process spawnMediaDriver(
      final String aeronDir, final Path logDir, final Path pidDir, final int index)
      throws IOException {

    final var javaHome = System.getProperty("java.home");
    final var classpath = System.getProperty("java.class.path");

    final var command = new ArrayList<String>();
    command.add(javaHome + "/bin/java");
    command.addAll(AERON_JVM_ARGS);
    command.add("-cp");
    command.add(classpath);
    command.add("com.trading.engine.media.driver.MediaDriverLauncher");
    command.add("--aeron-dir=" + aeronDir);
    command.add("--dir-delete-on-start=true");

    final var pb = new ProcessBuilder(command);
    pb.redirectOutput(new File(logDir.toFile(), "media-driver-" + index + ".stdout.log"));
    pb.redirectErrorStream(true); // merge stderr into stdout for single log file per driver

    final var process = pb.start();

    // Write PID file for orphan detection on next startup
    Files.writeString(
        pidDir.resolve("media-driver-" + index + ".pid"), String.valueOf(process.pid()));
    LOG.info("Media driver {} spawned: pid={} aeronDir={}", index, process.pid(), aeronDir);

    return process;
  }

  /**
   * Registers a crash handler for a media driver process. On unexpected exit (non-zero,
   * non-SIGTERM), signals the barrier to trigger orderly shutdown (P0-3). Normal exits (code 0) and
   * SIGTERM (code 143) are ignored — they occur during orderly shutdown when {@link
   * #destroyAllMediaDrivers} sends SIGTERM.
   */
  private static void registerCrashHandler(
      final Process process, final int index, final ShutdownSignalBarrier barrier) {
    process
        .onExit()
        .thenAccept(
            proc -> {
              final int exitCode = proc.exitValue();
              // 0 = normal exit, 143 = SIGTERM (128 + 15) during orderly shutdown
              if (exitCode != 0 && exitCode != 143) {
                LOG.error("Media driver {} crashed: exit code={}", index, exitCode);
                barrier.signal();
              }
            });
  }

  /**
   * Sends SIGTERM to all media driver processes, waits up to the configured timeout, then escalates
   * to SIGKILL if necessary.
   */
  private static void destroyAllMediaDrivers(
      final AtomicReferenceArray<Process> processes, final long timeoutSeconds) {
    for (int i = processes.length() - 1; i >= 0; i--) {
      final var p = processes.get(i);
      if (p != null && p.isAlive()) {
        p.destroy(); // SIGTERM
        try {
          if (!p.waitFor(timeoutSeconds, TimeUnit.SECONDS)) {
            LOG.warn(
                "Media driver {} did not terminate within {}s — sending SIGKILL",
                i,
                timeoutSeconds);
            p.destroyForcibly();
          }
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          p.destroyForcibly();
        }
      }
    }
  }

  /** Cleans up PID files after orderly shutdown. */
  private static void cleanupPidFiles(final Path pidDir, final int count) {
    for (int i = 0; i < count; i++) {
      try {
        Files.deleteIfExists(pidDir.resolve("media-driver-" + i + ".pid"));
      } catch (final IOException e) {
        LOG.warn("Failed to delete PID file for media driver {}", i, e);
      }
    }
  }

  private static long elapsedMs(final long startNs) {
    return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);
  }
}
