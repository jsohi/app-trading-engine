package com.trading.engine.fixbridge.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * Trivial registry assertion for {@link BridgeCloseCodes}.
 *
 * <p>Guards against close-code drift — if any constant is renamed or renumbered the browser
 * protocol breaks silently (close codes are compared numerically in the browser worker). This test
 * pins the values to the §3.3 registry so a refactor cannot silently change a constant.
 *
 * <p><b>Threading.</b> Stateless — safe to run in parallel.
 *
 * <p><b>Allocation.</b> None.
 */
final class BridgeCloseCodesTest {

  @Test
  void authExpired_codeIs4001() {
    assertEquals(4001, BridgeCloseCodes.AUTH_EXPIRED);
  }

  @Test
  void sessionTerminated_codeIs4002() {
    assertEquals(4002, BridgeCloseCodes.SESSION_TERMINATED);
  }

  @Test
  void policyViolation_codeIs4008() {
    assertEquals(4008, BridgeCloseCodes.POLICY_VIOLATION);
  }
}
