package com.trading.engine.launcher;

import com.trading.engine.cluster.TradingClusteredService;
import com.trading.engine.cluster.TradingClusteredServiceFactory;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.NanosecondClusterClock;
import io.aeron.cluster.service.ClusteredServiceContainer;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import org.agrona.CloseHelper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Library-style static factory that brings up a single Aeron Cluster node against an
 * <b>external</b> Media Driver. The Media Driver is expected to already be running at {@code
 * aeronDir}; this launcher verifies the presence of {@code cnc.dat} and fails fast if it is
 * missing.
 *
 * <p>The returned {@link ClusterComponents} owns the three launched resources ({@link Archive},
 * {@link ConsensusModule}, {@link ClusteredServiceContainer}) and must be closed by the caller to
 * shut the node down.
 *
 * <p>Out of scope (deferred to APP-15): properties-file loading, {@code main(String[])}
 * composition, and multi-process orchestration.
 */
public final class ClusterNodeLauncher {

  private static final Logger LOG = LogManager.getLogger(ClusterNodeLauncher.class);

  private static final String CNC_FILENAME = "cnc.dat";
  private static final String INGRESS_CHANNEL = "aeron:udp?term-length=64k";
  private static final String LOG_CHANNEL = "aeron:udp?term-length=256k";

  /**
   * Snapshot channel configured with a 128 MB term buffer, yielding a {@code maxMessageLength} of
   * 16 MB ({@code termBufferLength / 8}). This allows the entire cluster snapshot to be published
   * as a single atomic {@code ExclusivePublication.offer()} call — either all bytes commit or none
   * do — preventing the truncated-snapshot problem that would occur with per-fragment offers.
   *
   * <p>Sizing rationale: the {@code OrderBookSnapshot} buffer starts at 8 MB and each order record
   * is ~103 bytes, so 16 MB supports ~163 K concurrent orders — well above the pool's {@code
   * MAX_CAPACITY} of 65 534. If future state growth approaches the limit, double the term-length to
   * 256 MB (32 MB max message) and restart all nodes.
   *
   * <p>This channel applies to {@link ClusteredServiceContainer.Context#snapshotChannel} only. The
   * {@link ConsensusModule.Context} does not expose a {@code snapshotChannel} setter — it records
   * the service's snapshot publication using the service container's channel configuration.
   */
  // Aeron ChannelUri accepts both '|' and '&' as parameter separators; '|' is the Aeron convention.
  static final String SNAPSHOT_CHANNEL = "aeron:ipc?alias=snapshot|term-length=134217728";

  // Archive stream IDs — base values; the launch path adds {@code nodeId} to each so that even
  // if multiple cluster nodes ever share a Media Driver (they currently don't — each node has
  // its own aeron.dir), their Archive IPC streams will not collide.
  private static final int ARCHIVE_CONTROL_STREAM_ID = 100;
  private static final int ARCHIVE_LOCAL_CONTROL_STREAM_ID = 110;
  private static final int ARCHIVE_CONTROL_RESPONSE_STREAM_ID = 120;

  private ClusterNodeLauncher() {}

  /**
   * Launch one cluster node. The caller is responsible for:
   *
   * <ul>
   *   <li>starting the Media Driver process at {@code aeronDir} beforehand;
   *   <li>choosing a unique {@code baseDir} per node (archive + cluster state lives underneath);
   *   <li>closing the returned {@link ClusterComponents} on shutdown.
   * </ul>
   *
   * <p>The local bind host for the replication and archive-control-response channels is extracted
   * from {@code clusterMembers} — specifically, the ingress hostname at entry {@code nodeId}. This
   * means a single-host dev cluster (built via {@link ClusterConfig#buildClusterMembers(int)})
   * binds to {@code localhost}, while a multi-host cluster (built via {@link
   * ClusterConfig#buildClusterMembers(int, String...)}) binds to the routable hostname the caller
   * supplied.
   *
   * @param nodeId cluster member id (0-based; must be in {@code [0, ClusterConfig.MAX_NODES)})
   * @param baseDir parent directory for per-node archive and cluster dirs (non-blank)
   * @param aeronDir external Media Driver's {@code aeron.dir} (non-blank)
   * @param clusterMembers member string in the format produced by {@link
   *     ClusterConfig#buildClusterMembers(int)} or {@link ClusterConfig#buildClusterMembers(int,
   *     String...)} (non-blank; must contain an entry for {@code nodeId})
   * @return handle owning the launched Archive, ConsensusModule, and ClusteredServiceContainer
   * @throws NullPointerException if any string argument is {@code null}
   * @throws IllegalArgumentException if {@code nodeId} is out of range or any string argument is
   *     blank
   * @throws IllegalStateException if the external Media Driver is not running ({@code cnc.dat}
   *     missing at {@code aeronDir}) or per-node directories cannot be created
   */
  public static ClusterComponents launch(
      final int nodeId, final String baseDir, final String aeronDir, final String clusterMembers) {
    // Validate all inputs BEFORE taking any filesystem side effects so that a bad nodeId /
    // blank arg does not leave stray archive-<n> / cluster-<n> directories behind.
    ClusterConfig.checkNodeId(nodeId);
    requireNonBlank(baseDir, "baseDir");
    requireNonBlank(aeronDir, "aeronDir");
    requireNonBlank(clusterMembers, "clusterMembers");
    requireRunningMediaDriver(aeronDir);

    // Extract this node's advertised ingress hostname from the member string. Used for:
    //   1. The Archive's external UDP controlChannel (so ClusterTool / backup nodes can reach it)
    //   2. The ConsensusModule's replication channel (peer-to-peer snapshot transfer)
    // The AeronArchive client context uses IPC instead since it runs in-process. Using
    // ephemeral port 0 on UDP endpoints lets the OS pick a free port.
    final String localHost = ClusterConfig.hostForMember(clusterMembers, nodeId);
    final String replicationChannel = udpEndpoint(localHost, 0);

    final File archiveDir;
    final File clusterDir;
    try {
      archiveDir = Files.createDirectories(Paths.get(baseDir, "archive-" + nodeId)).toFile();
      clusterDir = Files.createDirectories(Paths.get(baseDir, "cluster-" + nodeId)).toFile();
    } catch (final IOException e) {
      throw new IllegalStateException(
          "failed to create cluster dirs under baseDir=" + baseDir + " for nodeId=" + nodeId, e);
    }

    LOG.info(
        "Launching cluster node {} (aeronDir={}, archiveDir={}, clusterDir={})",
        nodeId,
        aeronDir,
        archiveDir,
        clusterDir);

    Archive archive = null;
    ConsensusModule consensusModule = null;
    ClusteredServiceContainer serviceContainer = null;
    try {
      // 1. Archive — per-node embedded archive that records the cluster log and snapshots.
      final Archive.Context archiveCtx =
          new Archive.Context()
              .aeronDirectoryName(aeronDir)
              .archiveDir(archiveDir)
              // UDP controlChannel — used by external operators (ClusterTool, backup nodes).
              .controlChannel(udpEndpoint(localHost, ClusterConfig.archivePort(nodeId)))
              .controlStreamId(ARCHIVE_CONTROL_STREAM_ID + nodeId)
              // IPC localControlChannel — used by the in-process ConsensusModule and
              // ClusteredServiceContainer. Intra-process comms don't need UDP overhead.
              .localControlChannel("aeron:ipc?term-length=64k")
              .localControlStreamId(ARCHIVE_LOCAL_CONTROL_STREAM_ID + nodeId)
              // Recording events surface recording position / progress on a dedicated stream —
              // required for ConsensusModule and external monitoring tools to observe archive
              // health. Aeron 1.50+ requires explicit channel when enabled.
              .recordingEventsEnabled(true)
              .recordingEventsChannel("aeron:ipc?term-length=64k")
              // DEDICATED gives the archive its own recorder + replayer threads, which is the
              // recommended mode for real clusters. SHARED is fine for tests but causes the
              // conductor and replayer to contend on a single thread under load.
              .threadingMode(ArchiveThreadingMode.DEDICATED)
              // replicationChannel — required by Aeron Archive 1.50+ for log replication
              // between archive instances during cluster catchup and snapshot transfer.
              .replicationChannel(replicationChannel);
      archive = Archive.launch(archiveCtx);

      // 2. AeronArchive client context reused (cloned) by the ConsensusModule and the
      //    ClusteredServiceContainer to talk to the embedded Archive above. Both components
      //    live in this same JVM, so we use IPC (localControlChannel) instead of UDP — faster
      //    and bypasses the kernel network stack entirely.
      final AeronArchive.Context aeronArchiveCtx =
          new AeronArchive.Context()
              .aeronDirectoryName(aeronDir)
              .controlRequestChannel(archiveCtx.localControlChannel())
              .controlRequestStreamId(archiveCtx.localControlStreamId())
              .controlResponseChannel("aeron:ipc")
              .controlResponseStreamId(ARCHIVE_CONTROL_RESPONSE_STREAM_ID + nodeId);

      // 3. ConsensusModule — the Raft state machine + log replication.
      final ConsensusModule.Context consensusCtx =
          new ConsensusModule.Context()
              .aeronDirectoryName(aeronDir)
              .archiveContext(aeronArchiveCtx.clone())
              .clusterMemberId(nodeId)
              .clusterMembers(clusterMembers)
              .clusterDir(clusterDir)
              .ingressChannel(INGRESS_CHANNEL)
              .logChannel(LOG_CHANNEL)
              .replicationChannel(replicationChannel)
              .clusterClock(new NanosecondClusterClock())
              .errorHandler(throwable -> LOG.error("ConsensusModule error", throwable));
      consensusModule = ConsensusModule.launch(consensusCtx);

      // 4. ClusteredServiceContainer — hosts our TradingClusteredService, wired via the shared
      //    factory so test + production object graphs cannot drift. The service itself holds no
      //    AutoCloseable state (pre-allocated buffers + stores only), so constructing it before
      //    container launch and orphaning it on a subsequent failure is benign.
      final TradingClusteredService service = TradingClusteredServiceFactory.create();
      final ClusteredServiceContainer.Context serviceCtx =
          new ClusteredServiceContainer.Context()
              .aeronDirectoryName(aeronDir)
              .archiveContext(aeronArchiveCtx.clone())
              .clusteredService(service)
              .clusterDir(clusterDir)
              .snapshotChannel(SNAPSHOT_CHANNEL)
              .errorHandler(throwable -> LOG.error("ClusteredServiceContainer error", throwable));
      serviceContainer = ClusteredServiceContainer.launch(serviceCtx);

      LOG.info("Cluster node {} launched", nodeId);
      return new ClusterComponents(archive, consensusModule, serviceContainer);
    } catch (final RuntimeException e) {
      // Roll back already-constructed components in reverse order. We deliberately do NOT catch
      // Error — OutOfMemoryError, StackOverflowError, and LinkageError subclasses should not be
      // swallowed; letting them propagate lets the JVM crash cleanly rather than allocating
      // more memory during a half-broken rollback.
      CloseHelper.quietCloseAll(serviceContainer, consensusModule, archive);
      throw e;
    }
  }

  private static void requireRunningMediaDriver(final String aeronDir) {
    final Path cnc = Paths.get(aeronDir, CNC_FILENAME);
    if (!Files.isReadable(cnc)) {
      throw new IllegalStateException(
          "media driver not running at " + aeronDir + ": cnc.dat missing");
    }
  }

  private static void requireNonBlank(final String value, final String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
  }

  /**
   * Build an {@code aeron:udp?endpoint=host:port} URI, bracketing the host automatically if it
   * contains a {@code :} (i.e. an IPv6 literal). Aeron's endpoint URI grammar requires bracketed
   * IPv6 literals — emitting {@code aeron:udp?endpoint=2001:db8::1:8010} would be parsed as {@code
   * host=2001} + garbage port.
   */
  private static String udpEndpoint(final String host, final int port) {
    final String hostPart = host.indexOf(':') >= 0 ? "[" + host + "]" : host;
    return "aeron:udp?endpoint=" + hostPart + ":" + port;
  }
}
