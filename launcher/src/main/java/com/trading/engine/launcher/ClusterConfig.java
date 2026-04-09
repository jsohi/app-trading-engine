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
 * <p>The {@link #buildClusterMembers(int, String...)} format is the exact string that {@code
 * ConsensusModule.Context#clusterMembers(String)} expects: members separated by {@code |}, fields
 * within a member separated by {@code ,}, in the order {@code memberId,ingress,consensus,log,
 * catchup,archive}.
 *
 * <p>The no-host overloads ({@link #buildClusterMembers(int)}, {@link #ingressEndpoints(int)})
 * default every member to {@code localhost} and are intended for single-host dev and hermetic
 * tests. Multi-host deployments use the {@code String...} overloads to pass a routable hostname (or
 * IP) per member; APP-15's {@code TradingEngineLauncher} + {@code cluster.properties} config loader
 * will wire those overloads from file.
 */
public final class ClusterConfig {

  public static final int MAX_NODES = 3;

  /** Number of comma-separated fields in a valid cluster-member entry. */
  private static final int EXPECTED_MEMBER_FIELDS = 6;

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
   * ConsensusModule.Context#clusterMembers(String)}, defaulting every member host to {@code
   * localhost}. Shorthand for single-host dev / hermetic tests.
   */
  public static String buildClusterMembers(final int nodeCount) {
    checkNodeCount(nodeCount);
    return buildClusterMembers(nodeCount, defaultHosts(nodeCount));
  }

  /**
   * Build the {@code clusterMembers} string with an explicit hostname per member. Use this overload
   * for multi-host deployments where each node lives on a different machine.
   *
   * <p>Example for {@code nodeCount=3, hosts=["host-a","host-b","host-c"]}: {@code
   * "0,host-a:20110,host-a:20220,host-a:20330,host-a:20440,host-a:8010|1,host-b:21110,...|2,host-c:22110,..."}
   *
   * @param nodeCount number of members (1..{@link #MAX_NODES})
   * @param hosts hostname or IP per member; length must equal {@code nodeCount}; no element may be
   *     {@code null} or blank
   */
  public static String buildClusterMembers(final int nodeCount, final String... hosts) {
    checkNodeCount(nodeCount);
    checkHosts(nodeCount, hosts);
    final StringBuilder sb = new StringBuilder();
    for (int nodeId = 0; nodeId < nodeCount; nodeId++) {
      if (nodeId > 0) {
        sb.append('|');
      }
      final String host = hosts[nodeId];
      sb.append(nodeId)
          .append(',')
          .append(host)
          .append(':')
          .append(ingressPort(nodeId))
          .append(',')
          .append(host)
          .append(':')
          .append(consensusPort(nodeId))
          .append(',')
          .append(host)
          .append(':')
          .append(logPort(nodeId))
          .append(',')
          .append(host)
          .append(':')
          .append(catchupPort(nodeId))
          .append(',')
          .append(host)
          .append(':')
          .append(archivePort(nodeId));
    }
    return sb.toString();
  }

  /**
   * Build the {@code ingressEndpoints} string used by {@code AeronCluster.Context} clients to reach
   * any member, defaulting every host to {@code localhost}. Format: {@code
   * "0=host:port,1=host:port,2=host:port"}.
   */
  public static String ingressEndpoints(final int nodeCount) {
    checkNodeCount(nodeCount);
    return ingressEndpoints(nodeCount, defaultHosts(nodeCount));
  }

  /**
   * Build the {@code ingressEndpoints} string with an explicit hostname per member.
   *
   * @param nodeCount number of members (1..{@link #MAX_NODES})
   * @param hosts hostname or IP per member; length must equal {@code nodeCount}; no element may be
   *     {@code null} or blank
   */
  public static String ingressEndpoints(final int nodeCount, final String... hosts) {
    checkNodeCount(nodeCount);
    checkHosts(nodeCount, hosts);
    final StringBuilder sb = new StringBuilder();
    for (int nodeId = 0; nodeId < nodeCount; nodeId++) {
      if (nodeId > 0) {
        sb.append(',');
      }
      sb.append(nodeId).append('=').append(hosts[nodeId]).append(':').append(ingressPort(nodeId));
    }
    return sb.toString();
  }

  /**
   * Extract the hostname of a given cluster member from a {@code clusterMembers} string previously
   * built by {@link #buildClusterMembers(int, String...)}. Used by {@link ClusterNodeLauncher} to
   * recover its local bind host at launch time without requiring a separate constructor argument.
   *
   * @param clusterMembers the full member string
   * @param nodeId the member whose host to return
   * @throws IllegalArgumentException if the member string is malformed or does not contain {@code
   *     nodeId}
   */
  public static String hostForMember(final String clusterMembers, final int nodeId) {
    if (clusterMembers == null || clusterMembers.isBlank()) {
      throw new IllegalArgumentException("clusterMembers must not be blank");
    }
    for (final String member : clusterMembers.split("\\|")) {
      final String[] fields = member.split(",");
      // A valid member entry has exactly 6 fields: memberId, ingress, consensus, log, catchup,
      // archive. Being stricter than "length < 2" catches partially-formed strings that would
      // otherwise silently pass and later blow up inside ConsensusModule.
      if (fields.length != EXPECTED_MEMBER_FIELDS) {
        throw new IllegalArgumentException(
            "malformed member entry, expected " + EXPECTED_MEMBER_FIELDS + " fields: " + member);
      }
      final int id;
      try {
        id = Integer.parseInt(fields[0]);
      } catch (final NumberFormatException e) {
        throw new IllegalArgumentException("malformed member id in entry: " + member, e);
      }
      if (id == nodeId) {
        final String ingress = fields[1]; // format "host:port"
        final int colon = ingress.lastIndexOf(':');
        if (colon <= 0) {
          throw new IllegalArgumentException("malformed ingress endpoint in entry: " + member);
        }
        return ingress.substring(0, colon);
      }
    }
    throw new IllegalArgumentException(
        "clusterMembers does not contain nodeId " + nodeId + ": " + clusterMembers);
  }

  private static String[] defaultHosts(final int nodeCount) {
    final String[] hosts = new String[nodeCount];
    for (int i = 0; i < nodeCount; i++) {
      hosts[i] = "localhost";
    }
    return hosts;
  }

  private static void checkHosts(final int nodeCount, final String[] hosts) {
    if (hosts == null) {
      throw new IllegalArgumentException("hosts must not be null");
    }
    if (hosts.length != nodeCount) {
      throw new IllegalArgumentException(
          "hosts length (" + hosts.length + ") must equal nodeCount (" + nodeCount + ")");
    }
    for (int i = 0; i < hosts.length; i++) {
      if (hosts[i] == null || hosts[i].isBlank()) {
        throw new IllegalArgumentException("hosts[" + i + "] must not be blank");
      }
    }
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
