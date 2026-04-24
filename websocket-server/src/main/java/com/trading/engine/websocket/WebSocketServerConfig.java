package com.trading.engine.websocket;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.yaml.snakeyaml.Yaml;

/**
 * Immutable configuration for the Netty WebSocket server, loaded from YAML.
 *
 * <p>All tunables from {@code docs/websocket-architecture.md} Section 6 are externalized here.
 * Defaults match the architecture doc specification. Validation is performed eagerly at
 * construction time — a malformed config file fails the server startup immediately.
 *
 * <p><b>Thread safety.</b> All fields are final. Safe to share between the Netty bootstrap thread,
 * the AeronEgressThread, and the shutdown hook without synchronization.
 *
 * <p><b>Loading.</b> Production: {@link #fromYaml(Path)}. Tests: {@link Builder}.
 *
 * @see <a href="docs/websocket-architecture.md">WebSocket Architecture</a>
 */
public final class WebSocketServerConfig {

  // --- Server ---
  private final int port;
  private final int maxConcurrentSessions;
  private final int maxSubscriptionsPerClient;

  // --- Connections ---
  private final int maxConnectionsPerIp;
  private final int maxConnectionsPerUser;
  private final int perIpNewConnectionsPerSec;
  private final int globalNewConnectionsPerSec;

  // --- Session ---
  private final long sessionGracePeriodMs;
  private final long clientTimeoutMs;

  // --- Replay ---
  private final int replayBufferFrames;
  private final int replayBufferFrameSize;

  // --- Rate Limits ---
  private final int commandsPerSecSustained;
  private final int commandsBurst;
  private final int subscriptionsPerSec;

  // --- Heartbeat ---
  private final long heartbeatIntervalMs;

  // --- Snapshot ---
  private final int snapshotFragmentSizeBytes;

  // --- TLS ---
  private final String tlsCertPath;
  private final String tlsKeyPath;
  private final List<String> cipherSuites;

  // --- Security ---
  private final List<String> originsWhitelist;
  private final int maxRevokedJtis;
  private final int revocationTtlMinutes;

  // --- Backpressure ---
  private final int writeBufferLowWaterMark;
  private final int writeBufferHighWaterMark;
  private final int egressQueueCapacity;

  // --- Issuer Registry ---
  private final Map<String, String> issuerRegistry;

  private WebSocketServerConfig(final Builder b) {
    this.port = b.port;
    this.maxConcurrentSessions = b.maxConcurrentSessions;
    this.maxSubscriptionsPerClient = b.maxSubscriptionsPerClient;
    this.maxConnectionsPerIp = b.maxConnectionsPerIp;
    this.maxConnectionsPerUser = b.maxConnectionsPerUser;
    this.perIpNewConnectionsPerSec = b.perIpNewConnectionsPerSec;
    this.globalNewConnectionsPerSec = b.globalNewConnectionsPerSec;
    this.sessionGracePeriodMs = b.sessionGracePeriodMs;
    this.clientTimeoutMs = b.clientTimeoutMs;
    this.replayBufferFrames = b.replayBufferFrames;
    this.replayBufferFrameSize = b.replayBufferFrameSize;
    this.commandsPerSecSustained = b.commandsPerSecSustained;
    this.commandsBurst = b.commandsBurst;
    this.subscriptionsPerSec = b.subscriptionsPerSec;
    this.heartbeatIntervalMs = b.heartbeatIntervalMs;
    this.snapshotFragmentSizeBytes = b.snapshotFragmentSizeBytes;
    this.tlsCertPath = b.tlsCertPath;
    this.tlsKeyPath = b.tlsKeyPath;
    this.cipherSuites = List.copyOf(b.cipherSuites);
    this.originsWhitelist = List.copyOf(b.originsWhitelist);
    this.maxRevokedJtis = b.maxRevokedJtis;
    this.revocationTtlMinutes = b.revocationTtlMinutes;
    this.writeBufferLowWaterMark = b.writeBufferLowWaterMark;
    this.writeBufferHighWaterMark = b.writeBufferHighWaterMark;
    this.egressQueueCapacity = b.egressQueueCapacity;
    this.issuerRegistry = Map.copyOf(b.issuerRegistry);
    validate();
  }

  private void validate() {
    require(port >= 1 && port <= 65_535, "port must be in [1, 65535], got: " + port);
    require(maxConcurrentSessions >= 1, "maxConcurrentSessions must be >= 1");
    require(maxSubscriptionsPerClient >= 1, "maxSubscriptionsPerClient must be >= 1");
    require(maxConnectionsPerIp >= 1, "maxConnectionsPerIp must be >= 1");
    require(maxConnectionsPerUser >= 1, "maxConnectionsPerUser must be >= 1");
    require(perIpNewConnectionsPerSec >= 1, "perIpNewConnectionsPerSec must be >= 1");
    require(globalNewConnectionsPerSec >= 1, "globalNewConnectionsPerSec must be >= 1");
    require(sessionGracePeriodMs > 0, "sessionGracePeriodMs must be > 0");
    require(clientTimeoutMs > 0, "clientTimeoutMs must be > 0");
    require(
        clientTimeoutMs > heartbeatIntervalMs,
        "clientTimeoutMs ("
            + clientTimeoutMs
            + ") must be > heartbeatIntervalMs ("
            + heartbeatIntervalMs
            + ")");
    require(
        replayBufferFrames > 0 && Integer.bitCount(replayBufferFrames) == 1,
        "replayBufferFrames must be a power of 2, got: " + replayBufferFrames);
    require(
        replayBufferFrameSize > 0 && replayBufferFrameSize <= 65_536,
        "replayBufferFrameSize must be in [1, 65536], got: " + replayBufferFrameSize);
    require(commandsPerSecSustained > 0, "commandsPerSecSustained must be > 0");
    require(
        commandsBurst >= commandsPerSecSustained,
        "commandsBurst ("
            + commandsBurst
            + ") must be >= commandsPerSecSustained ("
            + commandsPerSecSustained
            + ")");
    require(subscriptionsPerSec > 0, "subscriptionsPerSec must be > 0");
    require(heartbeatIntervalMs > 0, "heartbeatIntervalMs must be > 0");
    require(
        snapshotFragmentSizeBytes > 0 && snapshotFragmentSizeBytes <= 65_536,
        "snapshotFragmentSizeBytes must be in [1, 65536], got: " + snapshotFragmentSizeBytes);
    require(maxRevokedJtis > 0, "maxRevokedJtis must be > 0");
    require(revocationTtlMinutes > 0, "revocationTtlMinutes must be > 0");
    require(writeBufferLowWaterMark > 0, "writeBufferLowWaterMark must be > 0");
    require(
        writeBufferHighWaterMark > writeBufferLowWaterMark,
        "writeBufferHighWaterMark ("
            + writeBufferHighWaterMark
            + ") must be > writeBufferLowWaterMark ("
            + writeBufferLowWaterMark
            + ")");
    require(
        egressQueueCapacity > 0 && Integer.bitCount(egressQueueCapacity) == 1,
        "egressQueueCapacity must be a power of 2, got: " + egressQueueCapacity);
  }

  private static void require(final boolean condition, final String message) {
    if (!condition) {
      throw new IllegalArgumentException(message);
    }
  }

  // --- Factory Methods ---

  /**
   * Load configuration from a YAML file.
   *
   * <p>Missing keys use architecture-doc defaults. SnakeYAML Yaml instance is not thread-safe —
   * created and discarded per load.
   *
   * @param filePath path to the YAML configuration file
   * @return a validated {@link WebSocketServerConfig}
   * @throws IOException if the file cannot be read
   * @throws IllegalArgumentException if any value is out of range
   */
  public static WebSocketServerConfig fromYaml(final Path filePath) throws IOException {
    final Object raw;
    try (InputStream in = Files.newInputStream(filePath)) {
      raw = new Yaml().load(in);
    }
    if (raw == null) {
      return builder().build();
    }
    if (!(raw instanceof Map<?, ?>)) {
      throw new IllegalArgumentException(
          "YAML root must be a mapping, got: " + raw.getClass().getSimpleName());
    }
    @SuppressWarnings("unchecked")
    final Map<String, Object> root = (Map<String, Object>) raw;

    final var b = builder();
    ifPresent(root, "port", Integer.class, b::port);
    ifPresent(root, "maxConcurrentSessions", Integer.class, b::maxConcurrentSessions);
    ifPresent(root, "maxSubscriptionsPerClient", Integer.class, b::maxSubscriptionsPerClient);
    ifPresent(root, "maxConnectionsPerIp", Integer.class, b::maxConnectionsPerIp);
    ifPresent(root, "maxConnectionsPerUser", Integer.class, b::maxConnectionsPerUser);
    ifPresent(root, "perIpNewConnectionsPerSec", Integer.class, b::perIpNewConnectionsPerSec);
    ifPresent(root, "globalNewConnectionsPerSec", Integer.class, b::globalNewConnectionsPerSec);
    ifPresent(root, "sessionGracePeriodMs", Integer.class, v -> b.sessionGracePeriodMs(v));
    ifPresent(root, "clientTimeoutMs", Integer.class, v -> b.clientTimeoutMs(v));
    ifPresent(root, "replayBufferFrames", Integer.class, b::replayBufferFrames);
    ifPresent(root, "replayBufferFrameSize", Integer.class, b::replayBufferFrameSize);
    ifPresent(root, "commandsPerSecSustained", Integer.class, b::commandsPerSecSustained);
    ifPresent(root, "commandsBurst", Integer.class, b::commandsBurst);
    ifPresent(root, "subscriptionsPerSec", Integer.class, b::subscriptionsPerSec);
    ifPresent(root, "heartbeatIntervalMs", Integer.class, v -> b.heartbeatIntervalMs(v));
    ifPresent(root, "snapshotFragmentSizeBytes", Integer.class, b::snapshotFragmentSizeBytes);
    ifPresent(root, "tlsCertPath", String.class, b::tlsCertPath);
    ifPresent(root, "tlsKeyPath", String.class, b::tlsKeyPath);
    ifPresent(root, "maxRevokedJtis", Integer.class, b::maxRevokedJtis);
    ifPresent(root, "revocationTtlMinutes", Integer.class, b::revocationTtlMinutes);
    ifPresent(root, "writeBufferLowWaterMark", Integer.class, b::writeBufferLowWaterMark);
    ifPresent(root, "writeBufferHighWaterMark", Integer.class, b::writeBufferHighWaterMark);
    ifPresent(root, "egressQueueCapacity", Integer.class, b::egressQueueCapacity);

    final Object ciphers = root.get("cipherSuites");
    if (ciphers instanceof List<?> list) {
      b.cipherSuites(list.stream().map(String::valueOf).toList());
    }

    final Object origins = root.get("originsWhitelist");
    if (origins instanceof List<?> list) {
      b.originsWhitelist(list.stream().map(String::valueOf).toList());
    }

    final Object issuers = root.get("issuerRegistry");
    if (issuers instanceof Map<?, ?> map) {
      final var registry = new HashMap<String, String>();
      map.forEach(
          (k, v) -> {
            if (v instanceof Map<?, ?> inner) {
              final Object uri = inner.get("jwksUri");
              if (uri != null) {
                registry.put(String.valueOf(k), String.valueOf(uri));
              }
            }
          });
      b.issuerRegistry(registry);
    }

    return b.build();
  }

  private static <T> void ifPresent(
      final Map<String, Object> map,
      final String key,
      final Class<T> type,
      final Consumer<T> setter) {
    final Object value = map.get(key);
    if (value != null) {
      if (type == Integer.class && value instanceof Number n) {
        setter.accept(type.cast(n.intValue()));
      } else if (type.isInstance(value)) {
        setter.accept(type.cast(value));
      } else {
        throw new IllegalArgumentException(
            "Config key '"
                + key
                + "' expected "
                + type.getSimpleName()
                + " but got "
                + value.getClass().getSimpleName()
                + ": "
                + value);
      }
    }
  }

  // --- Accessors ---

  public int port() {
    return port;
  }

  public int maxConcurrentSessions() {
    return maxConcurrentSessions;
  }

  public int maxSubscriptionsPerClient() {
    return maxSubscriptionsPerClient;
  }

  public int maxConnectionsPerIp() {
    return maxConnectionsPerIp;
  }

  public int maxConnectionsPerUser() {
    return maxConnectionsPerUser;
  }

  public int perIpNewConnectionsPerSec() {
    return perIpNewConnectionsPerSec;
  }

  public int globalNewConnectionsPerSec() {
    return globalNewConnectionsPerSec;
  }

  public long sessionGracePeriodMs() {
    return sessionGracePeriodMs;
  }

  public long clientTimeoutMs() {
    return clientTimeoutMs;
  }

  public int replayBufferFrames() {
    return replayBufferFrames;
  }

  public int replayBufferFrameSize() {
    return replayBufferFrameSize;
  }

  public int commandsPerSecSustained() {
    return commandsPerSecSustained;
  }

  public int commandsBurst() {
    return commandsBurst;
  }

  public int subscriptionsPerSec() {
    return subscriptionsPerSec;
  }

  public long heartbeatIntervalMs() {
    return heartbeatIntervalMs;
  }

  public int snapshotFragmentSizeBytes() {
    return snapshotFragmentSizeBytes;
  }

  public String tlsCertPath() {
    return tlsCertPath;
  }

  public String tlsKeyPath() {
    return tlsKeyPath;
  }

  public List<String> cipherSuites() {
    return cipherSuites;
  }

  public List<String> originsWhitelist() {
    return originsWhitelist;
  }

  public int maxRevokedJtis() {
    return maxRevokedJtis;
  }

  public int revocationTtlMinutes() {
    return revocationTtlMinutes;
  }

  public int writeBufferLowWaterMark() {
    return writeBufferLowWaterMark;
  }

  public int writeBufferHighWaterMark() {
    return writeBufferHighWaterMark;
  }

  public int egressQueueCapacity() {
    return egressQueueCapacity;
  }

  public Map<String, String> issuerRegistry() {
    return issuerRegistry;
  }

  // --- Builder ---

  /**
   * Creates a builder pre-populated with architecture-doc defaults.
   *
   * @return a new {@link Builder}
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Fluent builder for {@link WebSocketServerConfig}. All fields default to the values specified in
   * {@code docs/websocket-architecture.md} Section 6.
   */
  public static final class Builder {

    private int port = 8443;
    private int maxConcurrentSessions = 256;
    private int maxSubscriptionsPerClient = 100;
    private int maxConnectionsPerIp = 10;
    private int maxConnectionsPerUser = 4;
    private int perIpNewConnectionsPerSec = 10;
    private int globalNewConnectionsPerSec = 256;
    private long sessionGracePeriodMs = 30_000;
    private long clientTimeoutMs = 20_000;
    private int replayBufferFrames = 4096;
    private int replayBufferFrameSize = 1024;
    private int commandsPerSecSustained = 50;
    private int commandsBurst = 100;
    private int subscriptionsPerSec = 5;
    private long heartbeatIntervalMs = 5000;
    private int snapshotFragmentSizeBytes = 16_384;
    private String tlsCertPath = "";
    private String tlsKeyPath = "";
    private List<String> cipherSuites =
        List.of("TLS_AES_256_GCM_SHA384", "TLS_CHACHA20_POLY1305_SHA256", "TLS_AES_128_GCM_SHA256");
    private List<String> originsWhitelist = List.of();
    private int maxRevokedJtis = 10_000;
    private int revocationTtlMinutes = 15;
    private int writeBufferLowWaterMark = 131_072;
    private int writeBufferHighWaterMark = 262_144;
    private int egressQueueCapacity = 8192;
    private Map<String, String> issuerRegistry = Map.of();

    private Builder() {}

    public Builder port(final int port) {
      this.port = port;
      return this;
    }

    public Builder maxConcurrentSessions(final int maxConcurrentSessions) {
      this.maxConcurrentSessions = maxConcurrentSessions;
      return this;
    }

    public Builder maxSubscriptionsPerClient(final int maxSubscriptionsPerClient) {
      this.maxSubscriptionsPerClient = maxSubscriptionsPerClient;
      return this;
    }

    public Builder maxConnectionsPerIp(final int maxConnectionsPerIp) {
      this.maxConnectionsPerIp = maxConnectionsPerIp;
      return this;
    }

    public Builder maxConnectionsPerUser(final int maxConnectionsPerUser) {
      this.maxConnectionsPerUser = maxConnectionsPerUser;
      return this;
    }

    public Builder perIpNewConnectionsPerSec(final int perIpNewConnectionsPerSec) {
      this.perIpNewConnectionsPerSec = perIpNewConnectionsPerSec;
      return this;
    }

    public Builder globalNewConnectionsPerSec(final int globalNewConnectionsPerSec) {
      this.globalNewConnectionsPerSec = globalNewConnectionsPerSec;
      return this;
    }

    public Builder sessionGracePeriodMs(final long sessionGracePeriodMs) {
      this.sessionGracePeriodMs = sessionGracePeriodMs;
      return this;
    }

    public Builder clientTimeoutMs(final long clientTimeoutMs) {
      this.clientTimeoutMs = clientTimeoutMs;
      return this;
    }

    public Builder replayBufferFrames(final int replayBufferFrames) {
      this.replayBufferFrames = replayBufferFrames;
      return this;
    }

    public Builder replayBufferFrameSize(final int replayBufferFrameSize) {
      this.replayBufferFrameSize = replayBufferFrameSize;
      return this;
    }

    public Builder commandsPerSecSustained(final int commandsPerSecSustained) {
      this.commandsPerSecSustained = commandsPerSecSustained;
      return this;
    }

    public Builder commandsBurst(final int commandsBurst) {
      this.commandsBurst = commandsBurst;
      return this;
    }

    public Builder subscriptionsPerSec(final int subscriptionsPerSec) {
      this.subscriptionsPerSec = subscriptionsPerSec;
      return this;
    }

    public Builder heartbeatIntervalMs(final long heartbeatIntervalMs) {
      this.heartbeatIntervalMs = heartbeatIntervalMs;
      return this;
    }

    public Builder snapshotFragmentSizeBytes(final int snapshotFragmentSizeBytes) {
      this.snapshotFragmentSizeBytes = snapshotFragmentSizeBytes;
      return this;
    }

    public Builder tlsCertPath(final String tlsCertPath) {
      this.tlsCertPath = tlsCertPath;
      return this;
    }

    public Builder tlsKeyPath(final String tlsKeyPath) {
      this.tlsKeyPath = tlsKeyPath;
      return this;
    }

    public Builder cipherSuites(final List<String> cipherSuites) {
      this.cipherSuites = cipherSuites;
      return this;
    }

    public Builder originsWhitelist(final List<String> originsWhitelist) {
      this.originsWhitelist = originsWhitelist;
      return this;
    }

    public Builder maxRevokedJtis(final int maxRevokedJtis) {
      this.maxRevokedJtis = maxRevokedJtis;
      return this;
    }

    public Builder revocationTtlMinutes(final int revocationTtlMinutes) {
      this.revocationTtlMinutes = revocationTtlMinutes;
      return this;
    }

    public Builder writeBufferLowWaterMark(final int writeBufferLowWaterMark) {
      this.writeBufferLowWaterMark = writeBufferLowWaterMark;
      return this;
    }

    public Builder writeBufferHighWaterMark(final int writeBufferHighWaterMark) {
      this.writeBufferHighWaterMark = writeBufferHighWaterMark;
      return this;
    }

    public Builder egressQueueCapacity(final int egressQueueCapacity) {
      this.egressQueueCapacity = egressQueueCapacity;
      return this;
    }

    public Builder issuerRegistry(final Map<String, String> issuerRegistry) {
      this.issuerRegistry = issuerRegistry;
      return this;
    }

    /**
     * Build and validate the configuration.
     *
     * @return a validated {@link WebSocketServerConfig}
     * @throws IllegalArgumentException if any value is out of range
     */
    public WebSocketServerConfig build() {
      return new WebSocketServerConfig(this);
    }
  }
}
