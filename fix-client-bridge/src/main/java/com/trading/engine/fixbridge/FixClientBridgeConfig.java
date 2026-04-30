package com.trading.engine.fixbridge;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

/**
 * Immutable configuration record for the {@code fix-client-bridge} module.
 *
 * <p><b>Purpose.</b> Holds every operator-tunable knob for the FIX Client Bridge: WebSocket
 * listener address, Artio FIX initiator target, capacity limits, idle / handshake / auth timing,
 * JWT issuer registry, debug flag, and reconnect / heartbeat parameters. Loaded from a YAML file
 * via {@link #fromYaml(Path)} at bridge startup; overrides via {@code -Dbridge.*} system properties
 * are applied by {@link #fromYamlWithSystemProperties(Path)}.
 *
 * <p><b>Threading.</b> Immutable record — safe to share across threads. Constructor performs eager
 * validation; downstream components read fields directly.
 *
 * <p><b>Allocation.</b> Constructed once at startup. The {@code jwtIssuerRegistry} map is wrapped
 * in {@link Collections#unmodifiableMap} so consumers cannot mutate the registry post-load.
 *
 * <p><b>Lifecycle.</b> Loaded once during bridge bootstrap; lives for the JVM's lifetime.
 *
 * <p><b>Dependencies.</b> Only SnakeYAML (parser) and JDK collections. Intentionally has no Aeron /
 * Netty / Artio imports so it can be unit-tested in isolation.
 *
 * @param port WebSocket listen port (default 8444)
 * @param bindAddress WebSocket bind address (default 127.0.0.1 for dev loopback per APP-236)
 * @param gatewayHost FIX acceptor host (Artio gateway target)
 * @param gatewayPort FIX acceptor port
 * @param targetCompId FIX TargetCompID — the gateway's CompID
 * @param senderCompId FIX SenderCompID — this bridge's CompID
 * @param sessionsPath directory for Artio session-state persistence (locked §16)
 * @param forceSequenceReset send Logon with ResetSeqNumFlag=Y on next connect (operator recovery)
 * @param maxConcurrentBridgeSessions max simultaneous browser sessions (default 256)
 * @param maxJsonBytes per-frame JSON cap (default 64 KiB; mapped to {@code JsonObjectDecoder})
 * @param quoteCacheCapacityPerSession quote-cache size for AcceptQuote two-phase commit
 * @param outboundQueueCapacityPerSession bounded retain-deque per session for backpressure
 * @param idleReaderSeconds Netty {@code IdleStateHandler} reader-idle deadline
 * @param idleWriterSeconds Netty {@code IdleStateHandler} writer-idle ping interval
 * @param handshakeTimeoutMillis WebSocket handshake deadline (locked §15)
 * @param authTimeoutSeconds first-Auth deadline before the bridge closes the channel
 * @param jwtIssuerRegistry map of JWT {@code iss} claim → JWKS endpoint URL (HTTPS required)
 * @param expectedAudience JWT {@code aud} claim value the bridge enforces
 * @param bridgeDebug gate for APP-40 sim-failure injection (must default false for safety)
 * @param reconnectBackoffSecondsCap max reconnect backoff in seconds
 * @param fatalAfterFailures consecutive failures before a {@code BridgeStatus(fatal=true)}
 * @param fatalAfterSeconds time-window since first failure before declaring fatal
 * @param heartbeatSeconds {@code BridgeStatus} heartbeat interval
 */
public record FixClientBridgeConfig(
    int port,
    String bindAddress,
    String gatewayHost,
    int gatewayPort,
    String targetCompId,
    String senderCompId,
    String sessionsPath,
    boolean forceSequenceReset,
    int maxConcurrentBridgeSessions,
    int maxJsonBytes,
    int quoteCacheCapacityPerSession,
    int outboundQueueCapacityPerSession,
    int idleReaderSeconds,
    int idleWriterSeconds,
    long handshakeTimeoutMillis,
    int authTimeoutSeconds,
    Map<String, String> jwtIssuerRegistry,
    String expectedAudience,
    boolean bridgeDebug,
    int reconnectBackoffSecondsCap,
    int fatalAfterFailures,
    int fatalAfterSeconds,
    int heartbeatSeconds) {

  /** Default identifier used when {@code -Dbridge.jwksUri} is set without an explicit registry. */
  public static final String DEV_ISSUER_KEY = "trading-engine-dev-issuer";

  /** System-property prefix accepted by {@link #fromYamlWithSystemProperties(Path)}. */
  public static final String SYSPROP_PREFIX = "bridge.";

  /**
   * Compact constructor — eager validation. All numeric fields must be positive (zero or negative
   * values are operator misconfigurations and would silently break the runtime).
   *
   * @throws NullPointerException if any reference field is null
   * @throws IllegalArgumentException if any numeric field is non-positive or {@code
   *     expectedAudience} is empty
   */
  public FixClientBridgeConfig {
    Objects.requireNonNull(bindAddress, "bindAddress");
    Objects.requireNonNull(gatewayHost, "gatewayHost");
    Objects.requireNonNull(targetCompId, "targetCompId");
    Objects.requireNonNull(senderCompId, "senderCompId");
    Objects.requireNonNull(sessionsPath, "sessionsPath");
    Objects.requireNonNull(jwtIssuerRegistry, "jwtIssuerRegistry");
    Objects.requireNonNull(expectedAudience, "expectedAudience");

    if (port <= 0 || port > 65535) {
      throw new IllegalArgumentException("port out of range 1-65535: " + port);
    }
    if (gatewayPort <= 0 || gatewayPort > 65535) {
      throw new IllegalArgumentException("gatewayPort out of range 1-65535: " + gatewayPort);
    }
    if (maxConcurrentBridgeSessions <= 0) {
      throw new IllegalArgumentException(
          "maxConcurrentBridgeSessions must be > 0: " + maxConcurrentBridgeSessions);
    }
    if (maxJsonBytes <= 0) {
      throw new IllegalArgumentException("maxJsonBytes must be > 0: " + maxJsonBytes);
    }
    if (quoteCacheCapacityPerSession <= 0) {
      throw new IllegalArgumentException(
          "quoteCacheCapacityPerSession must be > 0: " + quoteCacheCapacityPerSession);
    }
    if (outboundQueueCapacityPerSession <= 0) {
      throw new IllegalArgumentException(
          "outboundQueueCapacityPerSession must be > 0: " + outboundQueueCapacityPerSession);
    }
    if (idleReaderSeconds <= 0) {
      throw new IllegalArgumentException("idleReaderSeconds must be > 0: " + idleReaderSeconds);
    }
    if (idleWriterSeconds <= 0) {
      throw new IllegalArgumentException("idleWriterSeconds must be > 0: " + idleWriterSeconds);
    }
    if (handshakeTimeoutMillis <= 0) {
      throw new IllegalArgumentException(
          "handshakeTimeoutMillis must be > 0: " + handshakeTimeoutMillis);
    }
    if (authTimeoutSeconds <= 0) {
      throw new IllegalArgumentException("authTimeoutSeconds must be > 0: " + authTimeoutSeconds);
    }
    if (expectedAudience.isEmpty()) {
      throw new IllegalArgumentException("expectedAudience must not be empty");
    }
    if (reconnectBackoffSecondsCap <= 0) {
      throw new IllegalArgumentException(
          "reconnectBackoffSecondsCap must be > 0: " + reconnectBackoffSecondsCap);
    }
    if (fatalAfterFailures <= 0) {
      throw new IllegalArgumentException("fatalAfterFailures must be > 0: " + fatalAfterFailures);
    }
    if (fatalAfterSeconds <= 0) {
      throw new IllegalArgumentException("fatalAfterSeconds must be > 0: " + fatalAfterSeconds);
    }
    if (heartbeatSeconds <= 0) {
      throw new IllegalArgumentException("heartbeatSeconds must be > 0: " + heartbeatSeconds);
    }

    // Defensive copy + immutable wrapper — protects the runtime from operator code mutating the
    // registry map after construction.
    jwtIssuerRegistry = Collections.unmodifiableMap(new LinkedHashMap<>(jwtIssuerRegistry));

    // Each JWKS URL must be HTTPS. JwtValidator enforces this at processor-build time, but the
    // bridge re-asserts here so misconfiguration surfaces during startup config parsing rather
    // than later inside Nimbus exception messages.
    for (final var entry : jwtIssuerRegistry.entrySet()) {
      final var url = entry.getValue();
      if (url == null || !url.startsWith("https://")) {
        throw new IllegalArgumentException(
            "JWKS URL for issuer '" + entry.getKey() + "' must use https://, got: " + url);
      }
    }
  }

  /**
   * Parse a YAML configuration file. Missing fields fall back to the project defaults documented on
   * each constructor parameter.
   *
   * @param yaml path to a YAML file readable by SnakeYAML's {@link SafeConstructor}
   * @return an immutable, validated configuration
   * @throws IOException on file read failure
   * @throws IllegalArgumentException on validation failure (see compact constructor)
   */
  public static FixClientBridgeConfig fromYaml(final Path yaml) throws IOException {
    Objects.requireNonNull(yaml, "yaml");
    final var parser = new Yaml(new SafeConstructor(new LoaderOptions()));
    final Map<String, Object> raw;
    try (var reader = Files.newBufferedReader(yaml)) {
      // SafeConstructor returns Map<String,Object> for top-level mappings.
      @SuppressWarnings("unchecked")
      final var parsed = (Map<String, Object>) parser.load(reader);
      raw = parsed == null ? Map.of() : parsed;
    }
    return fromMap(raw);
  }

  /**
   * Same as {@link #fromYaml(Path)} but layers {@code -Dbridge.<field>} system-property overrides
   * on top of the parsed YAML. Used by the launcher path.
   *
   * <p>Single-issuer overrides via {@code -Dbridge.jwksUri} are folded into {@code
   * jwtIssuerRegistry} as a single-entry map keyed by {@link #DEV_ISSUER_KEY}, matching the {@code
   * scripts/e2e.sh} contract.
   *
   * @param yaml YAML config file
   * @return final configuration
   * @throws IOException on file read failure
   */
  public static FixClientBridgeConfig fromYamlWithSystemProperties(final Path yaml)
      throws IOException {
    final var base = fromYaml(yaml);
    return base.withSystemPropertyOverrides(System.getProperties());
  }

  /**
   * Apply a property bag (typically {@link System#getProperties()}) on top of this configuration.
   * Only keys with the {@link #SYSPROP_PREFIX} prefix are honoured. Returns a fresh validated
   * record — does not mutate the receiver.
   *
   * @param properties property source (must not be null)
   * @return overlaid configuration
   */
  public FixClientBridgeConfig withSystemPropertyOverrides(final Properties properties) {
    Objects.requireNonNull(properties, "properties");
    final var registry = new LinkedHashMap<>(jwtIssuerRegistry);
    final var singleJwks = properties.getProperty(SYSPROP_PREFIX + "jwksUri");
    if (singleJwks != null && !singleJwks.isEmpty()) {
      registry.clear();
      registry.put(DEV_ISSUER_KEY, singleJwks);
    }

    return new FixClientBridgeConfig(
        intProp(properties, "port", port),
        strProp(properties, "bindAddress", bindAddress),
        strProp(properties, "gatewayHost", gatewayHost),
        intProp(properties, "gatewayPort", gatewayPort),
        strProp(properties, "targetCompId", targetCompId),
        strProp(properties, "senderCompId", senderCompId),
        strProp(properties, "sessionsPath", sessionsPath),
        boolProp(properties, "forceSequenceReset", forceSequenceReset),
        intProp(properties, "maxConcurrentBridgeSessions", maxConcurrentBridgeSessions),
        intProp(properties, "maxJsonBytes", maxJsonBytes),
        intProp(properties, "quoteCacheCapacityPerSession", quoteCacheCapacityPerSession),
        intProp(properties, "outboundQueueCapacityPerSession", outboundQueueCapacityPerSession),
        intProp(properties, "idleReaderSeconds", idleReaderSeconds),
        intProp(properties, "idleWriterSeconds", idleWriterSeconds),
        longProp(properties, "handshakeTimeoutMillis", handshakeTimeoutMillis),
        intProp(properties, "authTimeoutSeconds", authTimeoutSeconds),
        registry,
        strProp(properties, "expectedAudience", expectedAudience),
        boolProp(properties, "bridgeDebug", bridgeDebug),
        intProp(properties, "reconnectBackoffSecondsCap", reconnectBackoffSecondsCap),
        intProp(properties, "fatalAfterFailures", fatalAfterFailures),
        intProp(properties, "fatalAfterSeconds", fatalAfterSeconds),
        intProp(properties, "heartbeatSeconds", heartbeatSeconds));
  }

  // ===========================================================================
  // YAML mapping
  // ===========================================================================

  /**
   * Convert a raw parsed YAML map into a validated config. Keys missing from the map fall back to
   * the project defaults; unrecognised keys are ignored (forward-compatibility).
   *
   * @param raw parsed YAML map
   * @return validated config
   */
  static FixClientBridgeConfig fromMap(final Map<String, Object> raw) {
    Objects.requireNonNull(raw, "raw");

    final var registry = parseIssuerRegistry(raw);

    // sessionsPath default expands ${LOG_DIR} when set; falls back to a relative path so a missing
    // env var still resolves predictably against the JVM CWD (matches the plan's behaviour rule).
    final String sessionsDefault;
    final var logDir = System.getenv("LOG_DIR");
    if (logDir != null && !logDir.isEmpty()) {
      sessionsDefault = logDir + "/bridge-sessions";
    } else {
      sessionsDefault = "logs/bridge-sessions";
    }

    return new FixClientBridgeConfig(
        intOr(raw, "port", 8444),
        strOr(raw, "bindAddress", "127.0.0.1"),
        strOr(raw, "gatewayHost", "localhost"),
        intOr(raw, "gatewayPort", 19880),
        strOr(raw, "targetCompId", "EXCH"),
        strOr(raw, "senderCompId", "BRIDGE"),
        strOr(raw, "sessionsPath", sessionsDefault),
        boolOr(raw, "forceSequenceReset", false),
        intOr(raw, "maxConcurrentBridgeSessions", 256),
        intOr(raw, "maxJsonBytes", 65536),
        intOr(raw, "quoteCacheCapacityPerSession", 64),
        intOr(raw, "outboundQueueCapacityPerSession", 64),
        intOr(raw, "idleReaderSeconds", 30),
        intOr(raw, "idleWriterSeconds", 15),
        longOr(raw, "handshakeTimeoutMillis", 5000L),
        intOr(raw, "authTimeoutSeconds", 5),
        registry,
        // No default for expectedAudience — operator misconfig must fail-fast at the compact ctor's
        // requireNonNull rather than silently boot with the dev value (locked: prod-ready policy).
        strOrNull(raw, "expectedAudience"),
        boolOr(raw, "bridgeDebug", false),
        intOr(raw, "reconnectBackoffSecondsCap", 32),
        intOr(raw, "fatalAfterFailures", 10),
        intOr(raw, "fatalAfterSeconds", 600),
        intOr(raw, "heartbeatSeconds", 10));
  }

  /**
   * Parse and validate the {@code jwtIssuerRegistry} entry from a raw YAML map. SnakeYAML decodes
   * the value as a {@code Map<String, Object>}; this helper enforces that every value is a {@code
   * String} and surfaces a clear {@link IllegalArgumentException} (rather than a {@link
   * ClassCastException} at first iteration) when the YAML is malformed.
   *
   * @param raw parsed YAML map
   * @return immutable {@code Map<String, String>} of issuer → JWKS URL
   * @throws IllegalArgumentException if {@code jwtIssuerRegistry} is present but is not a map of
   *     strings, or any value is not a {@code String}
   */
  private static Map<String, String> parseIssuerRegistry(final Map<String, Object> raw) {
    final var rawRegistry = raw.get("jwtIssuerRegistry");
    if (rawRegistry == null) {
      return Map.of();
    }
    if (!(rawRegistry instanceof Map<?, ?> rawMap)) {
      throw new IllegalArgumentException(
          "jwtIssuerRegistry must be a YAML map, got: " + rawRegistry.getClass().getName());
    }
    final var typed = new LinkedHashMap<String, String>();
    for (final var entry : rawMap.entrySet()) {
      final var k = entry.getKey();
      final var v = entry.getValue();
      if (!(k instanceof String key)) {
        throw new IllegalArgumentException(
            "jwtIssuerRegistry key must be a string, got: "
                + (k == null ? "null" : k.getClass().getName()));
      }
      if (!(v instanceof String value)) {
        throw new IllegalArgumentException(
            "jwtIssuerRegistry value for '"
                + key
                + "' must be a string, got: "
                + (v == null ? "null" : v.getClass().getName()));
      }
      typed.put(key, value);
    }
    return typed;
  }

  /**
   * Read a string value from the YAML map without applying a default. Returns {@code null} when the
   * key is absent so the compact ctor's {@code requireNonNull} can surface the misconfiguration as
   * a fail-fast error.
   */
  private static String strOrNull(final Map<String, Object> raw, final String key) {
    final var v = raw.get(key);
    return v == null ? null : v.toString();
  }

  // ===========================================================================
  // Primitive coercion helpers (private)
  // ===========================================================================

  private static String strOr(final Map<String, Object> raw, final String key, final String def) {
    final var v = raw.get(key);
    return v == null ? def : v.toString();
  }

  private static int intOr(final Map<String, Object> raw, final String key, final int def) {
    final var v = raw.get(key);
    if (v == null) {
      return def;
    }
    if (v instanceof Number n) {
      return n.intValue();
    }
    return Integer.parseInt(v.toString());
  }

  private static long longOr(final Map<String, Object> raw, final String key, final long def) {
    final var v = raw.get(key);
    if (v == null) {
      return def;
    }
    if (v instanceof Number n) {
      return n.longValue();
    }
    return Long.parseLong(v.toString());
  }

  private static boolean boolOr(
      final Map<String, Object> raw, final String key, final boolean def) {
    final var v = raw.get(key);
    if (v == null) {
      return def;
    }
    if (v instanceof Boolean b) {
      return b;
    }
    return Boolean.parseBoolean(v.toString());
  }

  private static String strProp(
      final Properties properties, final String suffix, final String def) {
    final var v = properties.getProperty(SYSPROP_PREFIX + suffix);
    return v == null ? def : v;
  }

  private static int intProp(final Properties properties, final String suffix, final int def) {
    final var v = properties.getProperty(SYSPROP_PREFIX + suffix);
    return v == null ? def : Integer.parseInt(v);
  }

  private static long longProp(final Properties properties, final String suffix, final long def) {
    final var v = properties.getProperty(SYSPROP_PREFIX + suffix);
    return v == null ? def : Long.parseLong(v);
  }

  private static boolean boolProp(
      final Properties properties, final String suffix, final boolean def) {
    final var v = properties.getProperty(SYSPROP_PREFIX + suffix);
    return v == null ? def : Boolean.parseBoolean(v);
  }
}
