package com.trading.engine.websocket;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

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

  // --- Authentication ---
  private final String jwtAudience;
  private final int maxTokenSizeBytes;
  private final int maxPendingAuth;
  private final int authFailureLockoutThreshold;
  private final int authFailureLockoutSeconds;

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
    this.tlsCertPath = Objects.requireNonNull(b.tlsCertPath, "tlsCertPath");
    this.tlsKeyPath = Objects.requireNonNull(b.tlsKeyPath, "tlsKeyPath");
    this.cipherSuites = List.copyOf(b.cipherSuites);
    this.originsWhitelist = List.copyOf(b.originsWhitelist);
    this.maxRevokedJtis = b.maxRevokedJtis;
    this.revocationTtlMinutes = b.revocationTtlMinutes;
    this.writeBufferLowWaterMark = b.writeBufferLowWaterMark;
    this.writeBufferHighWaterMark = b.writeBufferHighWaterMark;
    this.egressQueueCapacity = b.egressQueueCapacity;
    this.jwtAudience = Objects.requireNonNull(b.jwtAudience, "jwtAudience");
    this.maxTokenSizeBytes = b.maxTokenSizeBytes;
    this.maxPendingAuth = b.maxPendingAuth;
    this.authFailureLockoutThreshold = b.authFailureLockoutThreshold;
    this.authFailureLockoutSeconds = b.authFailureLockoutSeconds;
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
    require(
        perIpNewConnectionsPerSec <= globalNewConnectionsPerSec,
        "perIpNewConnectionsPerSec ("
            + perIpNewConnectionsPerSec
            + ") must be <= globalNewConnectionsPerSec ("
            + globalNewConnectionsPerSec
            + ")");
    require(
        tlsCertPath.isEmpty() == tlsKeyPath.isEmpty(),
        "tlsCertPath and tlsKeyPath must both be set or both be empty");

    // --- Authentication validation ---
    require(
        maxTokenSizeBytes >= 256 && maxTokenSizeBytes <= 65_536,
        "maxTokenSizeBytes must be in [256, 65536], got: " + maxTokenSizeBytes);
    require(
        maxPendingAuth >= 1 && maxPendingAuth <= maxConcurrentSessions,
        "maxPendingAuth must be in [1, maxConcurrentSessions("
            + maxConcurrentSessions
            + ")], got: "
            + maxPendingAuth);
    require(
        authFailureLockoutThreshold > 0,
        "authFailureLockoutThreshold must be > 0, got: " + authFailureLockoutThreshold);
    require(
        authFailureLockoutSeconds > 0,
        "authFailureLockoutSeconds must be > 0, got: " + authFailureLockoutSeconds);
    // jwtAudience must be non-empty when issuerRegistry is configured (auth is enabled)
    require(
        issuerRegistry.isEmpty() || !jwtAudience.isBlank(),
        "jwtAudience must be non-blank when issuerRegistry is configured");
    // All JWKS URIs must use HTTPS to prevent SSRF and MITM attacks
    for (final var entry : issuerRegistry.entrySet()) {
      require(
          entry.getValue().toLowerCase(Locale.ROOT).startsWith("https://"),
          "issuerRegistry entry '"
              + entry.getKey()
              + "' jwksUri must use https:// scheme, got: "
              + entry.getValue());
    }
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
    try (final var in = Files.newInputStream(filePath)) {
      raw = new Yaml(new SafeConstructor(new LoaderOptions())).load(in);
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
    ifPresent(root, "sessionGracePeriodMs", Long.class, b::sessionGracePeriodMs);
    ifPresent(root, "clientTimeoutMs", Long.class, b::clientTimeoutMs);
    ifPresent(root, "replayBufferFrames", Integer.class, b::replayBufferFrames);
    ifPresent(root, "replayBufferFrameSize", Integer.class, b::replayBufferFrameSize);
    ifPresent(root, "commandsPerSecSustained", Integer.class, b::commandsPerSecSustained);
    ifPresent(root, "commandsBurst", Integer.class, b::commandsBurst);
    ifPresent(root, "subscriptionsPerSec", Integer.class, b::subscriptionsPerSec);
    ifPresent(root, "heartbeatIntervalMs", Long.class, b::heartbeatIntervalMs);
    ifPresent(root, "snapshotFragmentSizeBytes", Integer.class, b::snapshotFragmentSizeBytes);
    ifPresent(root, "tlsCertPath", String.class, b::tlsCertPath);
    ifPresent(root, "tlsKeyPath", String.class, b::tlsKeyPath);
    ifPresent(root, "maxRevokedJtis", Integer.class, b::maxRevokedJtis);
    ifPresent(root, "revocationTtlMinutes", Integer.class, b::revocationTtlMinutes);
    ifPresent(root, "writeBufferLowWaterMark", Integer.class, b::writeBufferLowWaterMark);
    ifPresent(root, "writeBufferHighWaterMark", Integer.class, b::writeBufferHighWaterMark);
    ifPresent(root, "egressQueueCapacity", Integer.class, b::egressQueueCapacity);
    ifPresent(root, "jwtAudience", String.class, b::jwtAudience);
    ifPresent(root, "maxTokenSizeBytes", Integer.class, b::maxTokenSizeBytes);
    ifPresent(root, "maxPendingAuth", Integer.class, b::maxPendingAuth);
    ifPresent(root, "authFailureLockoutThreshold", Integer.class, b::authFailureLockoutThreshold);
    ifPresent(root, "authFailureLockoutSeconds", Integer.class, b::authFailureLockoutSeconds);

    final var ciphers = root.get("cipherSuites");
    if (ciphers != null) {
      if (!(ciphers instanceof List<?>)) {
        throw new IllegalArgumentException(
            "Config key 'cipherSuites' must be a list, got: " + ciphers.getClass().getSimpleName());
      }
      b.cipherSuites(requireStringList((List<?>) ciphers, "cipherSuites"));
    }

    final var origins = root.get("originsWhitelist");
    if (origins != null) {
      if (!(origins instanceof List<?>)) {
        throw new IllegalArgumentException(
            "Config key 'originsWhitelist' must be a list, got: "
                + origins.getClass().getSimpleName());
      }
      b.originsWhitelist(requireStringList((List<?>) origins, "originsWhitelist"));
    }

    final var issuers = root.get("issuerRegistry");
    if (issuers != null) {
      if (!(issuers instanceof Map<?, ?>)) {
        throw new IllegalArgumentException(
            "Config key 'issuerRegistry' must be a mapping, got: "
                + issuers.getClass().getSimpleName());
      }
      b.issuerRegistry(requireIssuerRegistry((Map<?, ?>) issuers));
    }

    return b.build();
  }

  private static <T> void ifPresent(
      final Map<String, Object> map,
      final String key,
      final Class<T> type,
      final Consumer<T> setter) {
    final var value = map.get(key);
    if (value != null) {
      if (value instanceof Number n) {
        // Reject floating-point and BigInteger values — config only accepts int/long
        if (n instanceof Double || n instanceof Float) {
          throw new IllegalArgumentException(
              "Config key '" + key + "' must be an integer, got floating-point: " + value);
        }
        if (n instanceof BigInteger) {
          throw new IllegalArgumentException(
              "Config key '" + key + "' exceeds long range: " + value);
        }
        if (type == Integer.class) {
          final long longVal = n.longValue();
          if (longVal < Integer.MIN_VALUE || longVal > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                "Config key '" + key + "' overflows int range: " + longVal);
          }
          setter.accept(type.cast(n.intValue()));
        } else if (type == Long.class) {
          setter.accept(type.cast(n.longValue()));
        } else {
          throw new IllegalArgumentException(
              "Config key '"
                  + key
                  + "' expected "
                  + type.getSimpleName()
                  + " but got Number: "
                  + value);
        }
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

  private static List<String> requireStringList(final List<?> list, final String key) {
    for (int i = 0; i < list.size(); i++) {
      final var element = list.get(i);
      if (element == null) {
        throw new IllegalArgumentException(
            "Config key '" + key + "' element [" + i + "] must not be null");
      }
      if (!(element instanceof String)) {
        throw new IllegalArgumentException(
            "Config key '"
                + key
                + "' element ["
                + i
                + "] must be a string, got: "
                + element.getClass().getSimpleName());
      }
    }
    @SuppressWarnings("unchecked")
    final var stringList = (List<String>) list;
    return List.copyOf(stringList);
  }

  private static Map<String, String> requireIssuerRegistry(final Map<?, ?> map) {
    final var registry = new HashMap<String, String>();
    map.forEach(
        (k, v) -> {
          if (!(k instanceof String)) {
            throw new IllegalArgumentException(
                "issuerRegistry key must be a string, got: " + k.getClass().getSimpleName());
          }
          if (!(v instanceof Map<?, ?> inner)) {
            throw new IllegalArgumentException(
                "issuerRegistry entry '"
                    + k
                    + "' must be a mapping with 'jwksUri', got: "
                    + v.getClass().getSimpleName());
          }
          final var uri = inner.get("jwksUri");
          if (uri == null) {
            throw new IllegalArgumentException(
                "issuerRegistry entry '" + k + "' is missing required 'jwksUri' field");
          }
          if (!(uri instanceof String)) {
            throw new IllegalArgumentException(
                "issuerRegistry entry '"
                    + k
                    + "' jwksUri must be a string, got: "
                    + uri.getClass().getSimpleName());
          }
          registry.put((String) k, (String) uri);
        });
    return registry;
  }

  // --- Accessors ---

  /**
   * @return the WebSocket server port (default 8443)
   */
  public int port() {
    return port;
  }

  /**
   * @return the maximum number of concurrent WebSocket sessions (default 256)
   */
  public int maxConcurrentSessions() {
    return maxConcurrentSessions;
  }

  /**
   * @return the maximum subscriptions allowed per client session (default 100)
   */
  public int maxSubscriptionsPerClient() {
    return maxSubscriptionsPerClient;
  }

  /**
   * @return the maximum concurrent connections allowed from a single IP address (default 10)
   */
  public int maxConnectionsPerIp() {
    return maxConnectionsPerIp;
  }

  /**
   * @return the maximum concurrent connections allowed per authenticated user (default 4)
   */
  public int maxConnectionsPerUser() {
    return maxConnectionsPerUser;
  }

  /**
   * @return the per-IP new connection rate limit in connections per second (default 10)
   */
  public int perIpNewConnectionsPerSec() {
    return perIpNewConnectionsPerSec;
  }

  /**
   * @return the global new connection rate limit in connections per second (default 256)
   */
  public int globalNewConnectionsPerSec() {
    return globalNewConnectionsPerSec;
  }

  /**
   * @return the session grace period in milliseconds for reconnection (default 30000)
   */
  public long sessionGracePeriodMs() {
    return sessionGracePeriodMs;
  }

  /**
   * @return the client inactivity timeout in milliseconds (default 20000)
   */
  public long clientTimeoutMs() {
    return clientTimeoutMs;
  }

  /**
   * @return the number of frames in the per-session replay ring buffer; must be a power of 2
   *     (default 4096)
   */
  public int replayBufferFrames() {
    return replayBufferFrames;
  }

  /**
   * @return the maximum size in bytes of a single replay buffer frame (default 1024)
   */
  public int replayBufferFrameSize() {
    return replayBufferFrameSize;
  }

  /**
   * @return the sustained command rate limit in commands per second (default 50)
   */
  public int commandsPerSecSustained() {
    return commandsPerSecSustained;
  }

  /**
   * @return the burst command rate limit; must be >= {@link #commandsPerSecSustained()} (default
   *     100)
   */
  public int commandsBurst() {
    return commandsBurst;
  }

  /**
   * @return the subscription request rate limit in subscriptions per second (default 5)
   */
  public int subscriptionsPerSec() {
    return subscriptionsPerSec;
  }

  /**
   * @return the server-to-client heartbeat interval in milliseconds (default 5000)
   */
  public long heartbeatIntervalMs() {
    return heartbeatIntervalMs;
  }

  /**
   * @return the maximum fragment size in bytes for snapshot streaming (default 16384)
   */
  public int snapshotFragmentSizeBytes() {
    return snapshotFragmentSizeBytes;
  }

  /**
   * @return the file path to the TLS certificate, or empty string if TLS is disabled
   */
  public String tlsCertPath() {
    return tlsCertPath;
  }

  /**
   * @return the file path to the TLS private key, or empty string if TLS is disabled
   */
  public String tlsKeyPath() {
    return tlsKeyPath;
  }

  /**
   * @return an unmodifiable list of allowed TLS 1.3 cipher suites
   */
  public List<String> cipherSuites() {
    return cipherSuites;
  }

  /**
   * @return an unmodifiable list of allowed WebSocket origins; empty list permits all origins
   */
  public List<String> originsWhitelist() {
    return originsWhitelist;
  }

  /**
   * @return the maximum number of revoked JTI entries tracked for replay prevention (default 10000)
   */
  public int maxRevokedJtis() {
    return maxRevokedJtis;
  }

  /**
   * @return the TTL in minutes for revoked JTI entries (default 15)
   */
  public int revocationTtlMinutes() {
    return revocationTtlMinutes;
  }

  /**
   * @return the Netty write buffer low water mark in bytes (default 131072)
   */
  public int writeBufferLowWaterMark() {
    return writeBufferLowWaterMark;
  }

  /**
   * @return the Netty write buffer high water mark in bytes; must be > low water mark (default
   *     262144)
   */
  public int writeBufferHighWaterMark() {
    return writeBufferHighWaterMark;
  }

  /**
   * @return the egress queue capacity; must be a power of 2 (default 8192)
   */
  public int egressQueueCapacity() {
    return egressQueueCapacity;
  }

  /**
   * @return an unmodifiable map of JWT issuer identifiers to their JWKS endpoint URIs
   */
  public Map<String, String> issuerRegistry() {
    return issuerRegistry;
  }

  /**
   * @return the expected JWT audience claim; empty string when auth is disabled (no issuerRegistry
   *     configured)
   */
  public String jwtAudience() {
    return jwtAudience;
  }

  /**
   * @return the maximum JWT token size in bytes before parsing; rejects oversized tokens early to
   *     prevent parser DoS (default 8192)
   */
  public int maxTokenSizeBytes() {
    return maxTokenSizeBytes;
  }

  /**
   * @return the maximum number of unauthenticated connections allowed concurrently; prevents FD
   *     exhaustion from connection floods (default 64)
   */
  public int maxPendingAuth() {
    return maxPendingAuth;
  }

  /**
   * @return the number of auth failures within the lockout window before an IP is blocked (default
   *     5)
   */
  public int authFailureLockoutThreshold() {
    return authFailureLockoutThreshold;
  }

  /**
   * @return the lockout duration in seconds after exceeding the failure threshold (default 60)
   */
  public int authFailureLockoutSeconds() {
    return authFailureLockoutSeconds;
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
    private String jwtAudience = "";
    private int maxTokenSizeBytes = 8192;
    private int maxPendingAuth = 64;
    private int authFailureLockoutThreshold = 5;
    private int authFailureLockoutSeconds = 60;

    private Builder() {}

    /**
     * @param port the WebSocket server port; must be in [1, 65535]
     * @return this builder
     */
    public Builder port(final int port) {
      this.port = port;
      return this;
    }

    /**
     * @param maxConcurrentSessions the maximum number of concurrent WebSocket sessions; must be >=
     *     1
     * @return this builder
     */
    public Builder maxConcurrentSessions(final int maxConcurrentSessions) {
      this.maxConcurrentSessions = maxConcurrentSessions;
      return this;
    }

    /**
     * @param maxSubscriptionsPerClient the maximum subscriptions per client session; must be >= 1
     * @return this builder
     */
    public Builder maxSubscriptionsPerClient(final int maxSubscriptionsPerClient) {
      this.maxSubscriptionsPerClient = maxSubscriptionsPerClient;
      return this;
    }

    /**
     * @param maxConnectionsPerIp the maximum concurrent connections from a single IP; must be >= 1
     * @return this builder
     */
    public Builder maxConnectionsPerIp(final int maxConnectionsPerIp) {
      this.maxConnectionsPerIp = maxConnectionsPerIp;
      return this;
    }

    /**
     * @param maxConnectionsPerUser the maximum concurrent connections per authenticated user; must
     *     be >= 1
     * @return this builder
     */
    public Builder maxConnectionsPerUser(final int maxConnectionsPerUser) {
      this.maxConnectionsPerUser = maxConnectionsPerUser;
      return this;
    }

    /**
     * @param perIpNewConnectionsPerSec the per-IP new connection rate limit; must be >= 1 and <=
     *     globalNewConnectionsPerSec
     * @return this builder
     */
    public Builder perIpNewConnectionsPerSec(final int perIpNewConnectionsPerSec) {
      this.perIpNewConnectionsPerSec = perIpNewConnectionsPerSec;
      return this;
    }

    /**
     * @param globalNewConnectionsPerSec the global new connection rate limit; must be >= 1
     * @return this builder
     */
    public Builder globalNewConnectionsPerSec(final int globalNewConnectionsPerSec) {
      this.globalNewConnectionsPerSec = globalNewConnectionsPerSec;
      return this;
    }

    /**
     * @param sessionGracePeriodMs the session grace period in milliseconds for reconnection; must
     *     be > 0
     * @return this builder
     */
    public Builder sessionGracePeriodMs(final long sessionGracePeriodMs) {
      this.sessionGracePeriodMs = sessionGracePeriodMs;
      return this;
    }

    /**
     * @param clientTimeoutMs the client inactivity timeout in milliseconds; must be >
     *     heartbeatIntervalMs
     * @return this builder
     */
    public Builder clientTimeoutMs(final long clientTimeoutMs) {
      this.clientTimeoutMs = clientTimeoutMs;
      return this;
    }

    /**
     * @param replayBufferFrames the number of frames in the per-session replay ring buffer; must be
     *     a power of 2
     * @return this builder
     */
    public Builder replayBufferFrames(final int replayBufferFrames) {
      this.replayBufferFrames = replayBufferFrames;
      return this;
    }

    /**
     * @param replayBufferFrameSize the maximum size in bytes of a single replay buffer frame; must
     *     be in [1, 65536]
     * @return this builder
     */
    public Builder replayBufferFrameSize(final int replayBufferFrameSize) {
      this.replayBufferFrameSize = replayBufferFrameSize;
      return this;
    }

    /**
     * @param commandsPerSecSustained the sustained command rate limit per second; must be > 0
     * @return this builder
     */
    public Builder commandsPerSecSustained(final int commandsPerSecSustained) {
      this.commandsPerSecSustained = commandsPerSecSustained;
      return this;
    }

    /**
     * @param commandsBurst the burst command rate limit; must be >= commandsPerSecSustained
     * @return this builder
     */
    public Builder commandsBurst(final int commandsBurst) {
      this.commandsBurst = commandsBurst;
      return this;
    }

    /**
     * @param subscriptionsPerSec the subscription request rate limit per second; must be > 0
     * @return this builder
     */
    public Builder subscriptionsPerSec(final int subscriptionsPerSec) {
      this.subscriptionsPerSec = subscriptionsPerSec;
      return this;
    }

    /**
     * @param heartbeatIntervalMs the server-to-client heartbeat interval in milliseconds; must be >
     *     0
     * @return this builder
     */
    public Builder heartbeatIntervalMs(final long heartbeatIntervalMs) {
      this.heartbeatIntervalMs = heartbeatIntervalMs;
      return this;
    }

    /**
     * @param snapshotFragmentSizeBytes the maximum fragment size in bytes for snapshot streaming;
     *     must be in [1, 65536]
     * @return this builder
     */
    public Builder snapshotFragmentSizeBytes(final int snapshotFragmentSizeBytes) {
      this.snapshotFragmentSizeBytes = snapshotFragmentSizeBytes;
      return this;
    }

    /**
     * @param tlsCertPath the file path to the TLS certificate; empty string disables TLS
     * @return this builder
     */
    public Builder tlsCertPath(final String tlsCertPath) {
      this.tlsCertPath = tlsCertPath;
      return this;
    }

    /**
     * @param tlsKeyPath the file path to the TLS private key; empty string disables TLS
     * @return this builder
     */
    public Builder tlsKeyPath(final String tlsKeyPath) {
      this.tlsKeyPath = tlsKeyPath;
      return this;
    }

    /**
     * @param cipherSuites the list of allowed TLS 1.3 cipher suite names; must not be null
     * @return this builder
     */
    public Builder cipherSuites(final List<String> cipherSuites) {
      this.cipherSuites = Objects.requireNonNull(cipherSuites, "cipherSuites");
      return this;
    }

    /**
     * @param originsWhitelist the list of allowed WebSocket origins; empty list permits all
     *     origins; must not be null
     * @return this builder
     */
    public Builder originsWhitelist(final List<String> originsWhitelist) {
      this.originsWhitelist = Objects.requireNonNull(originsWhitelist, "originsWhitelist");
      return this;
    }

    /**
     * @param maxRevokedJtis the maximum number of revoked JTI entries for replay prevention; must
     *     be > 0
     * @return this builder
     */
    public Builder maxRevokedJtis(final int maxRevokedJtis) {
      this.maxRevokedJtis = maxRevokedJtis;
      return this;
    }

    /**
     * @param revocationTtlMinutes the TTL in minutes for revoked JTI entries; must be > 0
     * @return this builder
     */
    public Builder revocationTtlMinutes(final int revocationTtlMinutes) {
      this.revocationTtlMinutes = revocationTtlMinutes;
      return this;
    }

    /**
     * @param writeBufferLowWaterMark the Netty write buffer low water mark in bytes; must be > 0
     * @return this builder
     */
    public Builder writeBufferLowWaterMark(final int writeBufferLowWaterMark) {
      this.writeBufferLowWaterMark = writeBufferLowWaterMark;
      return this;
    }

    /**
     * @param writeBufferHighWaterMark the Netty write buffer high water mark in bytes; must be >
     *     writeBufferLowWaterMark
     * @return this builder
     */
    public Builder writeBufferHighWaterMark(final int writeBufferHighWaterMark) {
      this.writeBufferHighWaterMark = writeBufferHighWaterMark;
      return this;
    }

    /**
     * @param egressQueueCapacity the egress queue capacity; must be a power of 2
     * @return this builder
     */
    public Builder egressQueueCapacity(final int egressQueueCapacity) {
      this.egressQueueCapacity = egressQueueCapacity;
      return this;
    }

    /**
     * @param issuerRegistry a map of JWT issuer identifiers to JWKS endpoint URIs; must not be null
     * @return this builder
     */
    public Builder issuerRegistry(final Map<String, String> issuerRegistry) {
      this.issuerRegistry = Objects.requireNonNull(issuerRegistry, "issuerRegistry");
      return this;
    }

    /**
     * @param jwtAudience the expected JWT audience claim; must be non-empty when issuerRegistry is
     *     configured
     * @return this builder
     */
    public Builder jwtAudience(final String jwtAudience) {
      this.jwtAudience = Objects.requireNonNull(jwtAudience, "jwtAudience");
      return this;
    }

    /**
     * @param maxTokenSizeBytes the maximum JWT token size in bytes; must be in [256, 65536]
     * @return this builder
     */
    public Builder maxTokenSizeBytes(final int maxTokenSizeBytes) {
      this.maxTokenSizeBytes = maxTokenSizeBytes;
      return this;
    }

    /**
     * @param maxPendingAuth the maximum unauthenticated connections allowed concurrently; must be
     *     >= 1
     * @return this builder
     */
    public Builder maxPendingAuth(final int maxPendingAuth) {
      this.maxPendingAuth = maxPendingAuth;
      return this;
    }

    /**
     * @param authFailureLockoutThreshold failures before IP lockout; must be > 0
     * @return this builder
     */
    public Builder authFailureLockoutThreshold(final int authFailureLockoutThreshold) {
      this.authFailureLockoutThreshold = authFailureLockoutThreshold;
      return this;
    }

    /**
     * @param authFailureLockoutSeconds lockout duration in seconds; must be > 0
     * @return this builder
     */
    public Builder authFailureLockoutSeconds(final int authFailureLockoutSeconds) {
      this.authFailureLockoutSeconds = authFailureLockoutSeconds;
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
