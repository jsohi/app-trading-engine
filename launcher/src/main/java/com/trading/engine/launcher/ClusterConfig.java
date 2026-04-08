package com.trading.engine.launcher;

/**
 * Static helpers for the 3-node Aeron Cluster port scheme used by {@link ClusterNodeLauncher}.
 *
 * <p>Port table (per Linear APP-14):
 *
 * <pre>
 * Node | Ingress | Consensus | Log   | Catchup | Archive
 * 0    | 20110   | 20220     | 20330 | 20440   | 8010
 * 1    | 21110   | 21220     | 21330 | 21440   | 8011
 * 2    | 22110   | 22220     | 22330 | 22440   | 8012
 * </pre>
 *
 * <p>The {@link #buildClusterMembers(int)} format is the exact string that {@code
 * ConsensusModule.Context#clusterMembers(String)} expects: members separated by {@code |}, fields
 * within a member separated by {@code ,}, in the order {@code memberId,ingress,consensus,log,
 * catchup,archive}.
 */
public final class ClusterConfig {

  public static final int MAX_NODES = 3;

  // Ingress: 20110, 21110, 22110
  private static final int INGRESS_BASE = 20110;
  // Consensus: 20220, 21220, 22220
  private static final int CONSENSUS_BASE = 20220;
  // Log: 20330, 21330, 22330
  private static final int LOG_BASE = 20330;
  // Catchup: 20440, 21440, 22440
  private static final int CATCHUP_BASE = 20440;
  // Per-node offset applied to ingress/consensus/log/catchup bases
  private static final int NODE_PORT_OFFSET = 1_000;

  // Archive: 8010, 8011, 8012
  private static final int ARCHIVE_BASE = 8010;

  private ClusterConfig() {}

  public static int ingressPort(final int nodeId) {
    checkNodeId(nodeId);
    return INGRESS_BASE + nodeId * NODE_PORT_OFFSET;
  }

  public static int consensusPort(final int nodeId) {
    checkNodeId(nodeId);
    return CONSENSUS_BASE + nodeId * NODE_PORT_OFFSET;
  }

  public static int logPort(final int nodeId) {
    checkNodeId(nodeId);
    return LOG_BASE + nodeId * NODE_PORT_OFFSET;
  }

  public static int catchupPort(final int nodeId) {
    checkNodeId(nodeId);
    return CATCHUP_BASE + nodeId * NODE_PORT_OFFSET;
  }

  public static int archivePort(final int nodeId) {
    checkNodeId(nodeId);
    return ARCHIVE_BASE + nodeId;
  }

  /**
   * Build the {@code clusterMembers} string passed to {@code
   * ConsensusModule.Context#clusterMembers(String)}.
   *
   * <p>Example for {@code nodeCount=3}: {@code
   * "0,localhost:20110,localhost:20220,localhost:20330,localhost:20440,localhost:8010|1,localhost:21110,localhost:21220,localhost:21330,localhost:21440,localhost:8011|2,localhost:22110,localhost:22220,localhost:22330,localhost:22440,localhost:8012"}
   */
  public static String buildClusterMembers(final int nodeCount) {
    checkNodeCount(nodeCount);
    final StringBuilder sb = new StringBuilder();
    for (int nodeId = 0; nodeId < nodeCount; nodeId++) {
      if (nodeId > 0) {
        sb.append('|');
      }
      sb.append(nodeId)
          .append(",localhost:")
          .append(ingressPort(nodeId))
          .append(",localhost:")
          .append(consensusPort(nodeId))
          .append(",localhost:")
          .append(logPort(nodeId))
          .append(",localhost:")
          .append(catchupPort(nodeId))
          .append(",localhost:")
          .append(archivePort(nodeId));
    }
    return sb.toString();
  }

  /**
   * Build the {@code ingressEndpoints} string used by {@code AeronCluster.Context} clients to reach
   * any member. Format: {@code "0=host:port,1=host:port,2=host:port"}.
   */
  public static String ingressEndpoints(final int nodeCount) {
    checkNodeCount(nodeCount);
    final StringBuilder sb = new StringBuilder();
    for (int nodeId = 0; nodeId < nodeCount; nodeId++) {
      if (nodeId > 0) {
        sb.append(',');
      }
      sb.append(nodeId).append("=localhost:").append(ingressPort(nodeId));
    }
    return sb.toString();
  }

  /**
   * Validate that {@code nodeId} is in the supported range. Exposed package-private so {@link
   * ClusterNodeLauncher} can bounds-check a caller-supplied node id before taking any side effects
   * (e.g., creating per-node directories on disk).
   */
  static void checkNodeId(final int nodeId) {
    if (nodeId < 0 || nodeId >= MAX_NODES) {
      throw new IllegalArgumentException("nodeId must be in [0, " + MAX_NODES + "); got " + nodeId);
    }
  }

  private static void checkNodeCount(final int nodeCount) {
    if (nodeCount < 1 || nodeCount > MAX_NODES) {
      throw new IllegalArgumentException(
          "nodeCount must be in [1, " + MAX_NODES + "]; got " + nodeCount);
    }
  }
}
