package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
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
  void build_defaultValues_matchArchitectureDocSection6() {
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
    // APP-242: bumped from 262_144 to 2_097_152 so SlowConsumerHandler observes level-4 entry
    // before Netty's own isWritable flips false.
    assertEquals(2_097_152, config.writeBufferHighWaterMark());
    assertEquals(8192, config.egressQueueCapacity());
    // APP-242 slow-consumer ladder defaults
    assertEquals(102_400, config.slowConsumerLevel1Bytes());
    assertEquals(524_288, config.slowConsumerLevel2Bytes());
    assertEquals(1_048_576, config.slowConsumerLevel3Bytes());
    assertEquals(2_097_152, config.slowConsumerLevel4Bytes());
    assertEquals(5_000L, config.slowConsumerDisconnectMs());
    // APP-242 command dispatcher defaults
    assertEquals(4096, config.commandQueueCapacity());
    assertEquals(1024, config.commandAckQueueCapacity());
    assertEquals(10_000, config.clOrdIdDedupCapacity());
    assertEquals(600_000L, config.clOrdIdDedupTtlMs());
    assertEquals(100_000, config.clOrdIdDedupMaxUsers());
    assertEquals(50L, config.dedupTryLockMicros());
    assertEquals(3, config.cipherSuites().size());
    assertTrue(config.originsWhitelist().isEmpty());
    assertTrue(config.issuerRegistry().isEmpty());
    assertEquals("", config.jwtAudience());
    assertEquals(8192, config.maxTokenSizeBytes());
    assertEquals(64, config.maxPendingAuth());
    assertEquals(5, config.authFailureLockoutThreshold());
    assertEquals(60, config.authFailureLockoutSeconds());
  }

  @Test
  void fromYaml_overridesDefaults(@TempDir final Path tempDir) throws IOException {
    final var yaml = tempDir.resolve("test-config.yaml");
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
        jwtAudience: wss://trading.example.com/ws
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
    assertEquals("wss://trading.example.com/ws", config.jwtAudience());
  }

  @Test
  void fromYaml_emptyFile_producesDefaults(@TempDir final Path tempDir) throws IOException {
    final var yaml = tempDir.resolve("empty.yaml");
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
    final var yaml = tempDir.resolve("bad-type.yaml");
    Files.writeString(yaml, "port: \"not-a-number\"\n");

    assertThrows(IllegalArgumentException.class, () -> WebSocketServerConfig.fromYaml(yaml));
  }

  @Test
  void fromYaml_rootIsList_throwsIllegalArgument(@TempDir final Path tempDir) throws IOException {
    final var yaml = tempDir.resolve("list-root.yaml");
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
  void cipherSuites_defaultConfig_isImmutableList() {
    final var config = WebSocketServerConfig.builder().build();

    assertThrows(UnsupportedOperationException.class, () -> config.cipherSuites().add("TLS_FAKE"));
  }

  @Test
  void originsWhitelist_defaultConfig_isImmutableList() {
    final var config = WebSocketServerConfig.builder().build();

    assertThrows(
        UnsupportedOperationException.class,
        () -> config.originsWhitelist().add("https://evil.com"));
  }

  @Test
  void issuerRegistry_defaultConfig_isImmutableMap() {
    final var config = WebSocketServerConfig.builder().build();

    assertThrows(
        UnsupportedOperationException.class,
        () -> config.issuerRegistry().put("rogue", "https://evil.com/jwks"));
  }

  @Test
  void validate_clientTimeoutEqualsHeartbeatInterval_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            WebSocketServerConfig.builder()
                .heartbeatIntervalMs(10_000)
                .clientTimeoutMs(10_000)
                .build());
  }

  @Test
  void validate_snapshotFragmentSizeBytesZero_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> WebSocketServerConfig.builder().snapshotFragmentSizeBytes(0).build());
  }

  @Test
  void validate_perIpExceedsGlobal_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            WebSocketServerConfig.builder()
                .perIpNewConnectionsPerSec(100)
                .globalNewConnectionsPerSec(10)
                .build());
  }

  @Test
  void validate_tlsCertWithoutKey_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> WebSocketServerConfig.builder().tlsCertPath("/path/to/cert.pem").build());
  }

  @Test
  void fromYaml_floatingPointForIntegerField_throwsIllegalArgument(@TempDir final Path tempDir)
      throws IOException {
    final var yaml = tempDir.resolve("float.yaml");
    Files.writeString(yaml, "port: 8443.5\n");

    assertThrows(IllegalArgumentException.class, () -> WebSocketServerConfig.fromYaml(yaml));
  }

  @Test
  void fromYaml_issuerRegistryMissingJwksUri_throwsIllegalArgument(@TempDir final Path tempDir)
      throws IOException {
    final var yaml = tempDir.resolve("bad-issuer.yaml");
    Files.writeString(yaml, "issuerRegistry:\n  my-issuer:\n    notJwksUri: https://example.com\n");

    assertThrows(IllegalArgumentException.class, () -> WebSocketServerConfig.fromYaml(yaml));
  }

  // --- Authentication field validation tests (PR 3 Steps 13-18) ---

  @Test
  void validate_jwtAudienceRequiredWhenIssuerRegistryPresent_throws() {
    // jwtAudience defaults to "" (empty), issuerRegistry is non-empty → must throw
    assertThrows(
        IllegalArgumentException.class,
        () ->
            WebSocketServerConfig.builder()
                .issuerRegistry(Map.of("issuer", "https://auth.example.com/.well-known/jwks.json"))
                .build());
  }

  @Test
  void validate_jwtAudienceWithoutIssuerRegistry_throws() {
    // jwtAudience set but no issuers → misconfiguration (can never authenticate)
    assertThrows(
        IllegalArgumentException.class,
        () ->
            WebSocketServerConfig.builder()
                .jwtAudience("wss://trading.example.com/ws")
                .issuerRegistry(Map.of())
                .build());
  }

  @Test
  void validate_jwtAudienceEmptyWithEmptyIssuerRegistry_succeeds() {
    // Both empty → auth disabled, should succeed
    assertDoesNotThrow(
        () -> WebSocketServerConfig.builder().jwtAudience("").issuerRegistry(Map.of()).build());
  }

  @Test
  void validate_maxTokenSizeBytesTooSmall_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> WebSocketServerConfig.builder().maxTokenSizeBytes(255).build());
  }

  @Test
  void validate_maxTokenSizeBytesTooLarge_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> WebSocketServerConfig.builder().maxTokenSizeBytes(65_537).build());
  }

  @Test
  void validate_maxTokenSizeBytesAtBoundaries_succeeds() {
    assertDoesNotThrow(() -> WebSocketServerConfig.builder().maxTokenSizeBytes(256).build());
    assertDoesNotThrow(() -> WebSocketServerConfig.builder().maxTokenSizeBytes(65_536).build());
  }

  @Test
  void validate_maxPendingAuthZero_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> WebSocketServerConfig.builder().maxPendingAuth(0).build());
  }

  @Test
  void validate_issuerRegistryHttpUrl_throws() {
    // JWKS URLs must use https:// — http:// is rejected to prevent SSRF and MITM
    assertThrows(
        IllegalArgumentException.class,
        () ->
            WebSocketServerConfig.builder()
                .jwtAudience("wss://trading.example.com/ws")
                .issuerRegistry(Map.of("issuer", "http://auth.example.com/.well-known/jwks.json"))
                .build());
  }

  @Test
  void validate_authFailureLockoutFieldsValidation() {
    // Zero values
    assertThrows(
        IllegalArgumentException.class,
        () -> WebSocketServerConfig.builder().authFailureLockoutThreshold(0).build());
    assertThrows(
        IllegalArgumentException.class,
        () -> WebSocketServerConfig.builder().authFailureLockoutSeconds(0).build());
    // Negative values
    assertThrows(
        IllegalArgumentException.class,
        () -> WebSocketServerConfig.builder().authFailureLockoutThreshold(-1).build());
    assertThrows(
        IllegalArgumentException.class,
        () -> WebSocketServerConfig.builder().authFailureLockoutSeconds(-1).build());
  }

  @Test
  void validate_maxPendingAuthNegative_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> WebSocketServerConfig.builder().maxPendingAuth(-1).build());
  }

  @Test
  void validate_maxPendingAuthEqualsMaxSessions_succeeds() {
    assertDoesNotThrow(
        () ->
            WebSocketServerConfig.builder().maxConcurrentSessions(128).maxPendingAuth(128).build());
  }

  @Test
  void validate_maxPendingAuthExceedsMaxSessions_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            WebSocketServerConfig.builder().maxConcurrentSessions(128).maxPendingAuth(129).build());
  }

  @Test
  void builder_jwtAudienceNull_throwsNullPointerException() {
    assertThrows(
        NullPointerException.class, () -> WebSocketServerConfig.builder().jwtAudience(null));
  }

  @Test
  void fromYaml_authFieldsOverridden(@TempDir final Path tempDir) throws IOException {
    final var yaml = tempDir.resolve("auth-config.yaml");
    Files.writeString(
        yaml,
        """
        jwtAudience: wss://trading.example.com/ws
        maxTokenSizeBytes: 4096
        maxPendingAuth: 32
        authFailureLockoutThreshold: 3
        authFailureLockoutSeconds: 120
        issuerRegistry:
          my-issuer:
            jwksUri: https://auth.example.com/.well-known/jwks.json
        """);

    final var config = WebSocketServerConfig.fromYaml(yaml);

    assertEquals("wss://trading.example.com/ws", config.jwtAudience());
    assertEquals(4096, config.maxTokenSizeBytes());
    assertEquals(32, config.maxPendingAuth());
    assertEquals(3, config.authFailureLockoutThreshold());
    assertEquals(120, config.authFailureLockoutSeconds());
  }
}
