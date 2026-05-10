package com.trading.engine.fixbridge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Phase 1 green-check for {@link FixClientBridgeConfig}: round-trips a YAML file, validates that
 * defaults match the documented configuration surface, exercises the system-property override path,
 * and asserts that misconfiguration triggers fail-fast {@link IllegalArgumentException}s.
 *
 * <p>Pattern borrowed from {@code WebSocketServerConfigTest}.
 */
final class BridgeBootstrapTest {

  // --- Defaults ---

  @Test
  void fromYaml_defaultsOnly_returnsExpectedFields(@TempDir final Path tmp) throws IOException {
    final var yaml = tmp.resolve("bridge.yaml");
    Files.writeString(
        yaml,
        """
        # Minimal config — relies on defaults
        expectedAudience: "trading-engine-dev"
        """);

    final var cfg = FixClientBridgeConfig.fromYaml(yaml);

    assertEquals(8444, cfg.port());
    assertEquals("127.0.0.1", cfg.bindAddress());
    assertEquals("localhost", cfg.gatewayHost());
    assertEquals(19880, cfg.gatewayPort());
    assertEquals("EXCH", cfg.targetCompId());
    assertEquals("BRIDGE", cfg.senderCompId());
    assertEquals(256, cfg.maxConcurrentBridgeSessions());
    assertEquals(65536, cfg.maxJsonBytes());
    assertEquals(64, cfg.quoteCacheCapacityPerSession());
    assertEquals(64, cfg.outboundQueueCapacityPerSession());
    assertEquals(30, cfg.idleReaderSeconds());
    assertEquals(15, cfg.idleWriterSeconds());
    assertEquals(5000L, cfg.handshakeTimeoutMillis());
    assertEquals(5, cfg.authTimeoutSeconds());
    assertFalse(cfg.bridgeDebug(), "bridgeDebug must default false");
    assertFalse(cfg.forceSequenceReset(), "forceSequenceReset must default false");
    assertEquals(32, cfg.reconnectBackoffSecondsCap());
    assertEquals(10, cfg.fatalAfterFailures());
    assertEquals(600, cfg.fatalAfterSeconds());
    assertEquals(10, cfg.heartbeatSeconds());
    assertEquals("trading-engine-dev", cfg.expectedAudience());
    assertTrue(cfg.jwtIssuerRegistry().isEmpty(), "issuer registry must default empty");
    assertTrue(cfg.allowedOrigins().isEmpty(), "allowedOrigins must default empty (fail-safe)");
    assertEquals("audit_view", cfg.auditViewRole());
    assertEquals(5, cfg.authFailureLockoutThreshold());
    assertEquals(60, cfg.authFailureLockoutSeconds());
  }

  // --- YAML round-trip ---

  @Test
  void fromYaml_explicitFields_overrideDefaults(@TempDir final Path tmp) throws IOException {
    final var yaml = tmp.resolve("bridge.yaml");
    Files.writeString(
        yaml,
        """
        port: 19999
        bindAddress: "0.0.0.0"
        gatewayHost: "fix-acceptor.internal"
        gatewayPort: 5555
        targetCompId: "ACCEPTOR"
        senderCompId: "BRIDGE-PROD"
        sessionsPath: "/var/lib/bridge/sessions"
        forceSequenceReset: true
        maxConcurrentBridgeSessions: 1024
        maxJsonBytes: 131072
        quoteCacheCapacityPerSession: 128
        outboundQueueCapacityPerSession: 128
        idleReaderSeconds: 60
        idleWriterSeconds: 30
        handshakeTimeoutMillis: 10000
        authTimeoutSeconds: 10
        jwtIssuerRegistry:
          "https://idp.example.com": "https://idp.example.com/.well-known/jwks.json"
        expectedAudience: "wss://trading.prod/ws"
        bridgeDebug: false
        reconnectBackoffSecondsCap: 60
        fatalAfterFailures: 25
        fatalAfterSeconds: 1800
        heartbeatSeconds: 5
        """);

    final var cfg = FixClientBridgeConfig.fromYaml(yaml);

    assertEquals(19999, cfg.port());
    assertEquals("0.0.0.0", cfg.bindAddress());
    assertEquals("fix-acceptor.internal", cfg.gatewayHost());
    assertEquals(5555, cfg.gatewayPort());
    assertEquals("ACCEPTOR", cfg.targetCompId());
    assertEquals("BRIDGE-PROD", cfg.senderCompId());
    assertEquals("/var/lib/bridge/sessions", cfg.sessionsPath());
    assertTrue(cfg.forceSequenceReset());
    assertEquals(1024, cfg.maxConcurrentBridgeSessions());
    assertEquals(131072, cfg.maxJsonBytes());
    assertEquals(128, cfg.quoteCacheCapacityPerSession());
    assertEquals(60, cfg.idleReaderSeconds());
    assertEquals(10000L, cfg.handshakeTimeoutMillis());
    assertEquals(
        "https://idp.example.com/.well-known/jwks.json",
        cfg.jwtIssuerRegistry().get("https://idp.example.com"));
    assertEquals("wss://trading.prod/ws", cfg.expectedAudience());
    assertEquals(60, cfg.reconnectBackoffSecondsCap());
    assertEquals(25, cfg.fatalAfterFailures());
    assertEquals(1800, cfg.fatalAfterSeconds());
    assertEquals(5, cfg.heartbeatSeconds());
  }

  // --- System-property override path ---

  @Test
  void withSystemPropertyOverrides_jwksUriOverride_registrySingleEntry(@TempDir final Path tmp)
      throws IOException {
    final var yaml = tmp.resolve("bridge.yaml");
    Files.writeString(yaml, "expectedAudience: \"trading-engine-dev\"\n");

    final var props = new Properties();
    props.setProperty("bridge.port", "9001");
    props.setProperty("bridge.gatewayHost", "test-gateway.local");
    props.setProperty("bridge.gatewayPort", "29999");
    props.setProperty("bridge.jwksUri", "https://localhost:7100/jwks.json");
    props.setProperty("bridge.expectedAudience", "wss://trading.test/ws");
    props.setProperty("bridge.bridgeDebug", "true");

    final var base = FixClientBridgeConfig.fromYaml(yaml);
    final var cfg = base.withSystemPropertyOverrides(props);

    assertEquals(9001, cfg.port());
    assertEquals("test-gateway.local", cfg.gatewayHost());
    assertEquals(29999, cfg.gatewayPort());
    assertEquals(
        "https://localhost:7100/jwks.json",
        cfg.jwtIssuerRegistry().get(FixClientBridgeConfig.DEV_ISSUER_KEY));
    assertEquals(1, cfg.jwtIssuerRegistry().size(), "single-issuer override should clear registry");
    assertEquals("wss://trading.test/ws", cfg.expectedAudience());
    assertTrue(cfg.bridgeDebug(), "bridge.bridgeDebug=true should toggle bridgeDebug");
  }

  // --- Validation ---

  @Test
  void compactCtor_emptyAudience_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new FixClientBridgeConfig(
                8444,
                "127.0.0.1",
                "localhost",
                19880,
                "EXCH",
                "BRIDGE",
                "logs/sess",
                false,
                256,
                65536,
                64,
                64,
                30,
                15,
                5000L,
                5,
                Map.of(),
                "", // empty audience
                false,
                32,
                10,
                600,
                10,
                java.util.List.of(),
                "audit_view",
                5,
                60));
  }

  @Test
  void compactCtor_nonHttpsJwksUrl_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new FixClientBridgeConfig(
                8444,
                "127.0.0.1",
                "localhost",
                19880,
                "EXCH",
                "BRIDGE",
                "logs/sess",
                false,
                256,
                65536,
                64,
                64,
                30,
                15,
                5000L,
                5,
                Map.of("issuer", "http://insecure.example/jwks.json"),
                "trading-engine-dev",
                false,
                32,
                10,
                600,
                10,
                java.util.List.of(),
                "audit_view",
                5,
                60));
  }

  @Test
  void compactCtor_negativePort_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new FixClientBridgeConfig(
                -1,
                "127.0.0.1",
                "localhost",
                19880,
                "EXCH",
                "BRIDGE",
                "logs/sess",
                false,
                256,
                65536,
                64,
                64,
                30,
                15,
                5000L,
                5,
                Map.of(),
                "trading-engine-dev",
                false,
                32,
                10,
                600,
                10,
                java.util.List.of(),
                "audit_view",
                5,
                60));
  }

  // --- allowedOrigins YAML parsing ---

  @Test
  void fromYaml_allowedOriginsList_parsesEachEntry(@TempDir final Path tmp) throws IOException {
    final var yaml = tmp.resolve("bridge.yaml");
    Files.writeString(
        yaml,
        """
        expectedAudience: "trading-engine-dev"
        allowedOrigins:
          - "https://a.test"
          - "https://b.test"
        """);

    final var cfg = FixClientBridgeConfig.fromYaml(yaml);

    assertEquals(2, cfg.allowedOrigins().size(), "Expected 2 allowed origins");
    assertEquals("https://a.test", cfg.allowedOrigins().get(0));
    assertEquals("https://b.test", cfg.allowedOrigins().get(1));
  }

  @Test
  void fromYaml_allowedOriginsNonString_throws(@TempDir final Path tmp) throws IOException {
    final var yaml = tmp.resolve("bridge.yaml");
    Files.writeString(
        yaml,
        """
        expectedAudience: "trading-engine-dev"
        allowedOrigins:
          - 42
        """);

    assertThrows(IllegalArgumentException.class, () -> FixClientBridgeConfig.fromYaml(yaml));
  }

  @Test
  void withSystemPropertyOverrides_allowedOriginsCsv_replacesList(@TempDir final Path tmp)
      throws IOException {
    final var yaml = tmp.resolve("bridge.yaml");
    Files.writeString(yaml, "expectedAudience: \"trading-engine-dev\"\n");

    final var props = new Properties();
    props.setProperty("bridge.allowedOrigins", "https://x.test, https://y.test");

    final var base = FixClientBridgeConfig.fromYaml(yaml);
    final var cfg = base.withSystemPropertyOverrides(props);

    assertEquals(2, cfg.allowedOrigins().size(), "CSV override should produce 2 entries");
    assertEquals("https://x.test", cfg.allowedOrigins().get(0));
    assertEquals("https://y.test", cfg.allowedOrigins().get(1));
  }
}
