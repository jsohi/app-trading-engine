package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests YAML loading, builder defaults, and validation for {@link WebSocketServerConfig}.
 *
 * <p>Covers: default values match architecture doc, YAML override, validation rejects out-of-range,
 * missing file throws, empty YAML produces defaults.
 */
final class WebSocketServerConfigTest {

  @Test
  void builderDefaults_matchArchitectureDocSection6() {
    final var config = WebSocketServerConfig.builder().build();

    assertEquals(8443, config.port());
    assertEquals(256, config.maxConcurrentSessions());
    assertEquals(100, config.maxSubscriptionsPerClient());
    assertEquals(10, config.maxConnectionsPerIp());
    assertEquals(4, config.maxConnectionsPerUser());
    assertEquals(10, config.perIpNewConnectionsPerSec());
    assertEquals(256, config.globalNewConnectionsPerSec());
    assertEquals(30_000, config.sessionGracePeriodMs());
    assertEquals(20_000, config.clientTimeoutMs());
    assertEquals(4096, config.replayBufferFrames());
    assertEquals(1024, config.replayBufferFrameSize());
    assertEquals(50, config.commandsPerSecSustained());
    assertEquals(100, config.commandsBurst());
    assertEquals(5, config.subscriptionsPerSec());
    assertEquals(5000, config.heartbeatIntervalMs());
    assertEquals(16_384, config.snapshotFragmentSizeBytes());
    assertEquals(10_000, config.maxRevokedJtis());
    assertEquals(15, config.revocationTtlMinutes());
    assertEquals(131_072, config.writeBufferLowWaterMark());
    assertEquals(262_144, config.writeBufferHighWaterMark());
    assertEquals(8192, config.egressQueueCapacity());
    assertEquals(3, config.cipherSuites().size());
    assertTrue(config.originsWhitelist().isEmpty());
    assertTrue(config.issuerRegistry().isEmpty());
  }

  @Test
  void fromYaml_overridesDefaults(@TempDir final Path tempDir) throws IOException {
    final Path yaml = tempDir.resolve("test-config.yaml");
    Files.writeString(
        yaml,
        """
        port: 9443
        maxConcurrentSessions: 128
        maxSubscriptionsPerClient: 50
        commandsPerSecSustained: 25
        commandsBurst: 50
        heartbeatIntervalMs: 3000
        clientTimeoutMs: 10000
        originsWhitelist:
          - https://app.example.com
          - https://staging.example.com
        issuerRegistry:
          my-issuer:
            jwksUri: https://auth.example.com/.well-known/jwks.json
        """);

    final var config = WebSocketServerConfig.fromYaml(yaml);

    assertEquals(9443, config.port());
    assertEquals(128, config.maxConcurrentSessions());
    assertEquals(50, config.maxSubscriptionsPerClient());
    assertEquals(25, config.commandsPerSecSustained());
    assertEquals(50, config.commandsBurst());
    assertEquals(3000, config.heartbeatIntervalMs());
    assertEquals(10_000, config.clientTimeoutMs());
    assertEquals(2, config.originsWhitelist().size());
    assertEquals("https://app.example.com", config.originsWhitelist().get(0));
    assertEquals(
        "https://auth.example.com/.well-known/jwks.json", config.issuerRegistry().get("my-issuer"));
  }

  @Test
  void fromYaml_emptyFile_producesDefaults(@TempDir final Path tempDir) throws IOException {
    final Path yaml = tempDir.resolve("empty.yaml");
    Files.writeString(yaml, "");

    final var config = WebSocketServerConfig.fromYaml(yaml);

    assertEquals(8443, config.port());
    assertEquals(256, config.maxConcurrentSessions());
  }

  @Test
  void fromYaml_missingFile_throwsIOException() {
    assertThrows(IOException.class, () -> WebSocketServerConfig.fromYaml(Path.of("/nonexistent")));
  }

  @Test
  void fromYaml_wrongTypeForKey_throwsIllegalArgument(@TempDir final Path tempDir)
      throws IOException {
    final Path yaml = tempDir.resolve("bad-type.yaml");
    Files.writeString(yaml, "port: \"not-a-number\"\n");

    assertThrows(IllegalArgumentException.class, () -> WebSocketServerConfig.fromYaml(yaml));
  }

  @Test
  void fromYaml_rootIsList_throwsIllegalArgument(@TempDir final Path tempDir) throws IOException {
    final Path yaml = tempDir.resolve("list-root.yaml");
    Files.writeString(yaml, "- item1\n- item2\n");

    assertThrows(IllegalArgumentException.class, () -> WebSocketServerConfig.fromYaml(yaml));
  }

  @Test
  void validate_portOutOfRange_throws() {
    assertThrows(
        IllegalArgumentException.class, () -> WebSocketServerConfig.builder().port(0).build());
    assertThrows(
        IllegalArgumentException.class, () -> WebSocketServerConfig.builder().port(65_536).build());
  }

  @Test
  void validate_replayBufferFramesNotPowerOf2_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> WebSocketServerConfig.builder().replayBufferFrames(1000).build());
  }

  @Test
  void validate_replayBufferFramesPowerOf2_succeeds() {
    assertDoesNotThrow(() -> WebSocketServerConfig.builder().replayBufferFrames(2048).build());
  }

  @Test
  void validate_clientTimeoutMustExceedHeartbeatInterval_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            WebSocketServerConfig.builder()
                .heartbeatIntervalMs(10_000)
                .clientTimeoutMs(5000)
                .build());
  }

  @Test
  void validate_commandsBurstMustExceedSustained_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            WebSocketServerConfig.builder().commandsPerSecSustained(100).commandsBurst(50).build());
  }

  @Test
  void validate_writeBufferHighWaterMarkMustExceedLow_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            WebSocketServerConfig.builder()
                .writeBufferLowWaterMark(300_000)
                .writeBufferHighWaterMark(200_000)
                .build());
  }

  @Test
  void validate_egressQueueCapacityNotPowerOf2_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> WebSocketServerConfig.builder().egressQueueCapacity(5000).build());
  }

  @Test
  void cipherSuites_areImmutable() {
    final var config = WebSocketServerConfig.builder().build();

    assertThrows(UnsupportedOperationException.class, () -> config.cipherSuites().add("TLS_FAKE"));
  }

  @Test
  void originsWhitelist_areImmutable() {
    final var config = WebSocketServerConfig.builder().build();

    assertThrows(
        UnsupportedOperationException.class,
        () -> config.originsWhitelist().add("https://evil.com"));
  }

  @Test
  void issuerRegistry_isImmutable() {
    final var config = WebSocketServerConfig.builder().build();

    assertThrows(
        UnsupportedOperationException.class,
        () -> config.issuerRegistry().put("rogue", "https://evil.com/jwks"));
  }
}
