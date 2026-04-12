package com.trading.engine.launcher;

/**
 * Immutable configuration for {@link TradingEngineLauncher}, parsed from system properties.
 *
 * <p><b>Thread safety.</b> Immutable record — safe to share between the main thread and the
 * shutdown hook thread without synchronization.
 *
 * <p><b>Defaults.</b> fix.host=localhost, fix.port=9880, cluster.nodeCount=3,
 * cluster.baseDir=cluster-data, log.dir=logs, driver.shutdown.timeout.seconds=10,
 * accounts.file=accounts.yaml.
 *
 * @param fixHost TCP bind address for FIX connections
 * @param fixPort TCP port for FIX connections; must be in [1, 65535]
 * @param nodeCount number of cluster nodes; must be in [1, {@link ClusterConfig#MAX_NODES}]
 * @param baseDir base directory for cluster data files
 * @param logDir directory for per-process media driver log files
 * @param driverShutdownTimeoutSeconds seconds to wait for media driver shutdown before SIGKILL
 * @param accountsFile path to the accounts YAML file for reference data loading
 */
public record LauncherConfig(
    String fixHost,
    int fixPort,
    int nodeCount,
    String baseDir,
    String logDir,
    long driverShutdownTimeoutSeconds,
    String accountsFile) {

  /**
   * Compact constructor — validates all fields at construction time.
   *
   * @throws IllegalArgumentException if any field is out of range or blank
   */
  public LauncherConfig {
    if (fixHost == null || fixHost.isBlank()) {
      throw new IllegalArgumentException("fix.host must not be blank");
    }
    if (fixPort < 1 || fixPort > 65_535) {
      throw new IllegalArgumentException("fix.port must be in [1, 65535], got: " + fixPort);
    }
    if (nodeCount < 1 || nodeCount > ClusterConfig.MAX_NODES) {
      throw new IllegalArgumentException(
          "cluster.nodeCount must be in [1, " + ClusterConfig.MAX_NODES + "], got: " + nodeCount);
    }
    if (baseDir == null || baseDir.isBlank()) {
      throw new IllegalArgumentException("cluster.baseDir must not be blank");
    }
    if (logDir == null || logDir.isBlank()) {
      throw new IllegalArgumentException("log.dir must not be blank");
    }
    if (driverShutdownTimeoutSeconds <= 0) {
      throw new IllegalArgumentException(
          "driver.shutdown.timeout.seconds must be > 0, got: " + driverShutdownTimeoutSeconds);
    }
    if (accountsFile == null || accountsFile.isBlank()) {
      throw new IllegalArgumentException("accounts.file must not be blank");
    }
  }

  /**
   * Parse configuration from system properties with sensible defaults.
   *
   * @return a validated {@link LauncherConfig}
   * @throws NumberFormatException if a numeric property contains a non-numeric value
   * @throws IllegalArgumentException if any parsed value is out of range
   */
  public static LauncherConfig fromSystemProperties() {
    return new LauncherConfig(
        System.getProperty("fix.host", "localhost"),
        Integer.parseInt(System.getProperty("fix.port", "9880")),
        Integer.parseInt(System.getProperty("cluster.nodeCount", "3")),
        System.getProperty("cluster.baseDir", "cluster-data"),
        System.getProperty("log.dir", "logs"),
        Long.parseLong(System.getProperty("driver.shutdown.timeout.seconds", "10")),
        System.getProperty("accounts.file", "accounts.yaml"));
  }
}
