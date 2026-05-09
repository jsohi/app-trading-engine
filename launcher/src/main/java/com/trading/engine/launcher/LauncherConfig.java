package com.trading.engine.launcher;

/**
 * Immutable configuration for {@link TradingEngineLauncher}, parsed from system properties.
 *
 * <p><b>Thread safety.</b> Immutable record — safe to share between the main thread and the
 * shutdown hook thread without synchronization.
 *
 * <p><b>Defaults.</b> fix.host=localhost, fix.port=9880, cluster.nodeCount=3,
 * cluster.baseDir=cluster-data, log.dir=logs, driver.shutdown.timeout.seconds=10,
 * accounts.file=accounts.yaml, currencies.file=currencies.yaml, risk-limits.file=risk-limits.yaml,
 * aeron.dir.prefix="" (empty), rfq.poolCapacity=8192, rfq.defaultTtlNanos=30_000_000_000L,
 * rfq.rateLimitPerSession=100, rfq.rateLimitWindowNanos=1_000_000_000L,
 * rfq.requestTimeoutNanos=5_000_000_000L, rfq.acceptPriceToleranceBps=0,
 * rfq.acceptQtyToleranceBps=0.
 *
 * @param fixHost TCP bind address for FIX connections
 * @param fixPort TCP port for FIX connections; must be in [1, 65535]
 * @param nodeCount number of cluster nodes; must be in [1, {@link ClusterConfig#MAX_NODES}]
 * @param baseDir base directory for cluster data files
 * @param logDir directory for per-process media driver log files
 * @param driverShutdownTimeoutSeconds seconds to wait for media driver shutdown before SIGKILL
 * @param accountsFile path to the accounts YAML file for reference data loading
 * @param currenciesFile path to the currencies YAML file for reference data loading
 * @param riskLimitsFile path to the risk-limits YAML file for reference data loading
 * @param aeronDirPrefix prefix for Aeron directory names; empty string means production defaults
 *     ({@code /tmp/aeron-node-{i}}), non-empty (e.g. "e2e") produces {@code
 *     /tmp/aeron-e2e-node-{i}} for process isolation
 * @param rfqPoolCapacity RFQ slot pool capacity (power-of-two; range [256, 65536]); APP-232 §7.5
 * @param rfqDefaultTtlNanos RFQ TTL fallback in nanos when productType unknown; range [1s, 5min] =
 *     [1_000_000_000L, 300_000_000_000L]
 * @param rfqRateLimitPerSession RFQ token-bucket capacity per cluster session; range [1, 10_000]
 * @param rfqRateLimitWindowNanos RFQ token-bucket refill window in nanos; range [100ms, 60s] =
 *     [100_000_000L, 60_000_000_000L]
 * @param rfqRequestTimeoutNanos REQUESTED slot timeout in nanos (PriceResponse must arrive within
 *     this); range [100ms, 60s]
 * @param rfqAcceptPriceToleranceBps NOS-with-quoteId price-mismatch tolerance in bps; range [0,
 *     100]; 0 = exact match
 * @param rfqAcceptQtyToleranceBps NOS-with-quoteId qty-mismatch tolerance in bps; range [0, 1000];
 *     0 = exact match
 */
public record LauncherConfig(
    String fixHost,
    int fixPort,
    int nodeCount,
    String baseDir,
    String logDir,
    long driverShutdownTimeoutSeconds,
    String accountsFile,
    String currenciesFile,
    String riskLimitsFile,
    String aeronDirPrefix,
    int rfqPoolCapacity,
    long rfqDefaultTtlNanos,
    long rfqRateLimitPerSession,
    long rfqRateLimitWindowNanos,
    long rfqRequestTimeoutNanos,
    int rfqAcceptPriceToleranceBps,
    int rfqAcceptQtyToleranceBps) {

  /** Default RFQ slot pool capacity (power-of-two; covers 30s TTL × ~273 RFQs/sec peak). */
  public static final int DEFAULT_RFQ_POOL_CAPACITY = 8192;

  /** Default RFQ TTL fallback (30 s) when productType lookup misses. */
  public static final long DEFAULT_RFQ_DEFAULT_TTL_NANOS = 30_000_000_000L;

  /** Default per-session rate limit (100 RFQs/sec). */
  public static final long DEFAULT_RFQ_RATE_LIMIT_PER_SESSION = 100L;

  /** Default rate-limit refill window (1 s). */
  public static final long DEFAULT_RFQ_RATE_LIMIT_WINDOW_NANOS = 1_000_000_000L;

  /** Default REQUESTED-slot request-timeout (5 s) — PriceResponse must arrive within this. */
  public static final long DEFAULT_RFQ_REQUEST_TIMEOUT_NANOS = 5_000_000_000L;

  /** Default NOS-with-quoteId price tolerance (0 bps = exact match required). */
  public static final int DEFAULT_RFQ_ACCEPT_PRICE_TOLERANCE_BPS = 0;

  /** Default NOS-with-quoteId qty tolerance (0 bps = exact match required). */
  public static final int DEFAULT_RFQ_ACCEPT_QTY_TOLERANCE_BPS = 0;

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
    if (currenciesFile == null || currenciesFile.isBlank()) {
      throw new IllegalArgumentException("currencies.file must not be blank");
    }
    if (riskLimitsFile == null || riskLimitsFile.isBlank()) {
      throw new IllegalArgumentException("risk-limits.file must not be blank");
    }
    if (aeronDirPrefix == null) {
      throw new IllegalArgumentException("aeron.dir.prefix must not be null");
    }
    // Validate prefix contains only safe characters — it is interpolated into /tmp/aeron-<prefix>-
    // paths. Reject path separators, traversal sequences, and control characters.
    if (!aeronDirPrefix.isEmpty() && !aeronDirPrefix.matches("[A-Za-z0-9][A-Za-z0-9\\-]*")) {
      throw new IllegalArgumentException(
          "aeron.dir.prefix must be alphanumeric/hyphen (no path separators), got: '"
              + aeronDirPrefix
              + "'");
    }
    // ---- APP-232 RFQ config validation ----
    if (rfqPoolCapacity < 256
        || rfqPoolCapacity > 65_536
        || Integer.bitCount(rfqPoolCapacity) != 1) {
      throw new IllegalArgumentException(
          "rfq.poolCapacity must be a power-of-two in [256, 65536], got: " + rfqPoolCapacity);
    }
    if (rfqDefaultTtlNanos < 1_000_000_000L || rfqDefaultTtlNanos > 300_000_000_000L) {
      throw new IllegalArgumentException(
          "rfq.defaultTtlNanos must be in [1s, 5min], got: " + rfqDefaultTtlNanos);
    }
    if (rfqRateLimitPerSession < 1L || rfqRateLimitPerSession > 10_000L) {
      throw new IllegalArgumentException(
          "rfq.rateLimitPerSession must be in [1, 10000], got: " + rfqRateLimitPerSession);
    }
    if (rfqRateLimitWindowNanos < 100_000_000L || rfqRateLimitWindowNanos > 60_000_000_000L) {
      throw new IllegalArgumentException(
          "rfq.rateLimitWindowNanos must be in [100ms, 60s], got: " + rfqRateLimitWindowNanos);
    }
    if (rfqRequestTimeoutNanos < 100_000_000L || rfqRequestTimeoutNanos > 60_000_000_000L) {
      throw new IllegalArgumentException(
          "rfq.requestTimeoutNanos must be in [100ms, 60s], got: " + rfqRequestTimeoutNanos);
    }
    if (rfqAcceptPriceToleranceBps < 0 || rfqAcceptPriceToleranceBps > 100) {
      throw new IllegalArgumentException(
          "rfq.acceptPriceToleranceBps must be in [0, 100], got: " + rfqAcceptPriceToleranceBps);
    }
    if (rfqAcceptQtyToleranceBps < 0 || rfqAcceptQtyToleranceBps > 1000) {
      throw new IllegalArgumentException(
          "rfq.acceptQtyToleranceBps must be in [0, 1000], got: " + rfqAcceptQtyToleranceBps);
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
        System.getProperty("accounts.file", "accounts.yaml"),
        System.getProperty("currencies.file", "currencies.yaml"),
        System.getProperty("risk-limits.file", "risk-limits.yaml"),
        System.getProperty("aeron.dir.prefix", ""),
        Integer.parseInt(
            System.getProperty("rfq.poolCapacity", Integer.toString(DEFAULT_RFQ_POOL_CAPACITY))),
        Long.parseLong(
            System.getProperty(
                "rfq.defaultTtlNanos", Long.toString(DEFAULT_RFQ_DEFAULT_TTL_NANOS))),
        Long.parseLong(
            System.getProperty(
                "rfq.rateLimitPerSession", Long.toString(DEFAULT_RFQ_RATE_LIMIT_PER_SESSION))),
        Long.parseLong(
            System.getProperty(
                "rfq.rateLimitWindowNanos", Long.toString(DEFAULT_RFQ_RATE_LIMIT_WINDOW_NANOS))),
        Long.parseLong(
            System.getProperty(
                "rfq.requestTimeoutNanos", Long.toString(DEFAULT_RFQ_REQUEST_TIMEOUT_NANOS))),
        Integer.parseInt(
            System.getProperty(
                "rfq.acceptPriceToleranceBps",
                Integer.toString(DEFAULT_RFQ_ACCEPT_PRICE_TOLERANCE_BPS))),
        Integer.parseInt(
            System.getProperty(
                "rfq.acceptQtyToleranceBps",
                Integer.toString(DEFAULT_RFQ_ACCEPT_QTY_TOLERANCE_BPS))));
  }

  /**
   * Returns a {@link LauncherConfig} populated entirely from {@code DEFAULT_*} constants. Used by
   * tests that don't go through {@link #fromSystemProperties()} and by the test-friendly {@code
   * ClusterNodeLauncher.launch} overload that omits a config argument.
   *
   * @return a default-valued {@link LauncherConfig}
   */
  public static LauncherConfig defaults() {
    return new LauncherConfig(
        "localhost",
        9880,
        3,
        "cluster-data",
        "logs",
        10L,
        "accounts.yaml",
        "currencies.yaml",
        "risk-limits.yaml",
        "",
        DEFAULT_RFQ_POOL_CAPACITY,
        DEFAULT_RFQ_DEFAULT_TTL_NANOS,
        DEFAULT_RFQ_RATE_LIMIT_PER_SESSION,
        DEFAULT_RFQ_RATE_LIMIT_WINDOW_NANOS,
        DEFAULT_RFQ_REQUEST_TIMEOUT_NANOS,
        DEFAULT_RFQ_ACCEPT_PRICE_TOLERANCE_BPS,
        DEFAULT_RFQ_ACCEPT_QTY_TOLERANCE_BPS);
  }
}
