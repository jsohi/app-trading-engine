package com.trading.engine.launcher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.junit.jupiter.api.Test;

/**
 * Tests fail-fast input validation in {@link GatewayLauncher#launch}. These tests do NOT start real
 * Aeron or Artio components — they verify that invalid arguments are rejected before any resource
 * creation.
 */
class GatewayLauncherValidationTest {

  private static final IdleStrategy IDLE = new BackoffIdleStrategy();

  // ===== fixHost =====

  @Test
  void nullFixHost_throwsNpe() {
    final var ex =
        assertThrows(
            NullPointerException.class,
            () -> GatewayLauncher.launch(null, 9880, "/tmp/aeron", "0=localhost:20110", IDLE));
    assertEquals("fixHost must not be null", ex.getMessage());
  }

  @Test
  void blankFixHost_throwsIae() {
    final var ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> GatewayLauncher.launch("  ", 9880, "/tmp/aeron", "0=localhost:20110", IDLE));
    assertEquals("fixHost must not be blank", ex.getMessage());
  }

  // ===== fixPort =====

  @Test
  void portZero_throwsIae() {
    final var ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> GatewayLauncher.launch("localhost", 0, "/tmp/aeron", "0=localhost:20110", IDLE));
    assertEquals("fixPort must be in [1, 65535], got: 0", ex.getMessage());
  }

  @Test
  void portNegative_throwsIae() {
    assertThrows(
        IllegalArgumentException.class,
        () -> GatewayLauncher.launch("localhost", -1, "/tmp/aeron", "0=localhost:20110", IDLE));
  }

  @Test
  void portAboveMax_throwsIae() {
    final var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                GatewayLauncher.launch(
                    "localhost", 65536, "/tmp/aeron", "0=localhost:20110", IDLE));
    assertEquals("fixPort must be in [1, 65535], got: 65536", ex.getMessage());
  }

  // ===== aeronDir =====

  @Test
  void nullAeronDir_throwsNpe() {
    final var ex =
        assertThrows(
            NullPointerException.class,
            () -> GatewayLauncher.launch("localhost", 9880, null, "0=localhost:20110", IDLE));
    assertEquals("aeronDir must not be null", ex.getMessage());
  }

  @Test
  void blankAeronDir_throwsIae() {
    final var ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> GatewayLauncher.launch("localhost", 9880, "", "0=localhost:20110", IDLE));
    assertEquals("aeronDir must not be blank", ex.getMessage());
  }

  // ===== ingressEndpoints =====

  @Test
  void nullIngressEndpoints_throwsNpe() {
    final var ex =
        assertThrows(
            NullPointerException.class,
            () -> GatewayLauncher.launch("localhost", 9880, "/tmp/aeron", null, IDLE));
    assertEquals("ingressEndpoints must not be null", ex.getMessage());
  }

  @Test
  void blankIngressEndpoints_throwsIae() {
    final var ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> GatewayLauncher.launch("localhost", 9880, "/tmp/aeron", "   ", IDLE));
    assertEquals("ingressEndpoints must not be blank", ex.getMessage());
  }

  // ===== idleStrategy =====

  @Test
  void nullIdleStrategy_throwsNpe() {
    final var ex =
        assertThrows(
            NullPointerException.class,
            () ->
                GatewayLauncher.launch("localhost", 9880, "/tmp/aeron", "0=localhost:20110", null));
    assertEquals("idleStrategy must not be null", ex.getMessage());
  }
}
