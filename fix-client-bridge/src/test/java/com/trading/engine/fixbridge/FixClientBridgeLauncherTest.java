package com.trading.engine.fixbridge;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.fixbridge.transport.FixSessionAdapter;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Smoke test for {@link FixClientBridgeLauncher}'s composition graph.
 *
 * <p>Does NOT call {@link FixClientBridgeLauncher#start()} because that would bind a real Netty
 * server to a real TCP port (which the unit-test sandbox forbids). Instead verifies that:
 *
 * <ul>
 *   <li>The default constructor wires every singleton without throwing.
 *   <li>The 3-arg constructor with explicit Artio + cluster bindings also wires cleanly.
 *   <li>Null arguments are caught at construction.
 *   <li>The Micrometer registry is exposed via the public accessor.
 * </ul>
 *
 * <p>End-to-end behaviour (real Netty bind + auth flow + dispatch) is covered by the bridge's
 * integration test suite (under {@code src/integrationTest}).
 */
final class FixClientBridgeLauncherTest {

  @Test
  void defaultCtor_wiresWithoutThrowing_andExposesMeterRegistry() throws Exception {
    final var config = devConfig();
    try (final var launcher = new FixClientBridgeLauncher(config)) {
      assertNotNull(launcher.meterRegistry(), "meterRegistry must be exposed");
    }
  }

  @Test
  void explicitCtor_withCustomBindings_wiresWithoutThrowing() throws Exception {
    final var config = devConfig();
    final FixClientBridgeLauncher.ArtioSessionConnector artioStub =
        session -> FixSessionAdapter.NOOP;
    try (final var launcher = new FixClientBridgeLauncher(config, artioStub, account -> null)) {
      assertNotNull(launcher.meterRegistry());
    }
  }

  @Test
  void ctor_nullConfig_throwsNPE() {
    assertThrows(NullPointerException.class, () -> new FixClientBridgeLauncher(null));
  }

  @Test
  void ctor_nullArtioConnector_throwsNPE() {
    assertThrows(
        NullPointerException.class,
        () -> new FixClientBridgeLauncher(devConfig(), null, account -> null));
  }

  @Test
  void ctor_nullAccountLimitsLookup_throwsNPE() {
    assertThrows(
        NullPointerException.class,
        () ->
            new FixClientBridgeLauncher(
                devConfig(), FixClientBridgeLauncher.ArtioSessionConnector.NOOP, null));
  }

  @Test
  void meterRegistry_initiallyEmpty_butReady() throws Exception {
    try (final var launcher = new FixClientBridgeLauncher(devConfig())) {
      // The registry is freshly constructed — no metrics yet, but it must be a working
      // PrometheusMeterRegistry (or at least a MeterRegistry impl that supports counter
      // registration). Smoke-check by registering and reading a counter.
      final var probe = launcher.meterRegistry().counter("test_probe_total");
      probe.increment();
      assertTrue(probe.count() >= 1.0, "registry must accept counter operations");
    }
  }

  /**
   * Build a minimal valid {@link FixClientBridgeConfig} suitable for the launcher's composition
   * smoke test. Uses an empty issuer registry (no preflight HTTP fetch) and the dev self-signed TLS
   * path so construction does not require operator-supplied cert files.
   */
  private static FixClientBridgeConfig devConfig() {
    return new FixClientBridgeConfig(
        /* port */ 18444, // never bound — launcher.start() is NOT called in these tests
        /* bindAddress */ "127.0.0.1",
        /* gatewayHost */ "localhost",
        /* gatewayPort */ 19880,
        /* targetCompId */ "EXCH",
        /* senderCompId */ "BRIDGE",
        /* sessionsPath */ "logs/sess",
        /* bridgeDebug */ false,
        /* maxConcurrentSessions */ 256,
        /* maxJsonBytes */ 65536,
        /* outboundQueueCapacity */ 64,
        /* outboundQueueCapacityPerSession */ 64,
        /* idleReaderSeconds */ 30,
        /* idleWriterSeconds */ 15,
        /* handshakeTimeoutMillis */ 5000L,
        /* authTimeoutSeconds */ 5,
        /* jwtIssuerRegistry */ Map.of(),
        /* expectedAudience */ "trading-engine",
        /* requireClientCert */ false,
        /* maxCommandsPerSecond */ 32,
        /* initialWindowMaxCommands */ 10,
        /* initialWindowSeconds */ 600,
        /* burstSize */ 10,
        /* allowedOrigins */ List.<String>of(),
        /* auditViewRole */ "audit_view",
        /* authFailureLockoutThreshold */ 5,
        /* authFailureLockoutSeconds */ 60,
        /* tlsCertPath */ null,
        /* tlsKeyPath */ null,
        /* allowSelfSignedCert */ true);
  }
}
