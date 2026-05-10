package com.trading.engine.fixbridge.audit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link AuditAction}. Covers uniqueness of wire values and the locked set of 17
 * action constants (§3.7 taxonomy).
 */
class AuditActionTest {

  // ===========================================================================
  // Wire-value uniqueness
  // ===========================================================================

  @Test
  void wireValues_allUnique_setSizeMatchesValuesLength() {
    final var values = AuditAction.values();
    final var wireValues = new HashSet<String>();
    for (final var a : values) {
      wireValues.add(a.wireValue());
    }
    assertEquals(
        values.length, wireValues.size(), "every AuditAction must have a unique wireValue()");
  }

  // ===========================================================================
  // Locked taxonomy — exactly 17 constants (§3.7)
  // ===========================================================================

  @Test
  void values_exactlySeventeenConstants() {
    assertEquals(
        17,
        AuditAction.values().length,
        "AuditAction must have exactly 17 constants per §3.7 locked taxonomy");
  }

  @Test
  void values_containsAuthSuccess() {
    assertTrue(containsWire("auth_success"), "must contain auth_success");
  }

  @Test
  void values_containsAuthFail() {
    assertTrue(containsWire("auth_fail"), "must contain auth_fail");
  }

  @Test
  void values_containsAuthTimeout() {
    assertTrue(containsWire("auth_timeout"), "must contain auth_timeout");
  }

  @Test
  void values_containsQuoteRequestReceived() {
    assertTrue(containsWire("quote_request_received"), "must contain quote_request_received");
  }

  @Test
  void values_containsQuoteEmittedToSession() {
    assertTrue(containsWire("quote_emitted_to_session"), "must contain quote_emitted_to_session");
  }

  @Test
  void values_containsAcceptQuoteReceived() {
    assertTrue(containsWire("accept_quote_received"), "must contain accept_quote_received");
  }

  @Test
  void values_containsRejectQuoteReceived() {
    assertTrue(containsWire("reject_quote_received"), "must contain reject_quote_received");
  }

  @Test
  void values_containsNewOrderReceived() {
    assertTrue(containsWire("new_order_received"), "must contain new_order_received");
  }

  @Test
  void values_containsCancelOrderReceived() {
    assertTrue(containsWire("cancel_order_received"), "must contain cancel_order_received");
  }

  @Test
  void values_containsOrderStatusRequest() {
    assertTrue(containsWire("order_status_request"), "must contain order_status_request");
  }

  @Test
  void values_containsOrderReconciled() {
    assertTrue(containsWire("order_reconciled"), "must contain order_reconciled");
  }

  @Test
  void values_containsKillSwitchPress() {
    assertTrue(containsWire("kill_switch_press"), "must contain kill_switch_press");
  }

  @Test
  void values_containsBridgeDebugToggle() {
    assertTrue(containsWire("bridge_debug_toggle"), "must contain bridge_debug_toggle");
  }

  @Test
  void values_containsRateLimitHit() {
    assertTrue(containsWire("rate_limit_hit"), "must contain rate_limit_hit");
  }

  @Test
  void values_containsQuoteOrphaned() {
    assertTrue(containsWire("quote_orphaned"), "must contain quote_orphaned");
  }

  @Test
  void values_containsSessionTerminated() {
    assertTrue(containsWire("session_terminated"), "must contain session_terminated");
  }

  @Test
  void values_containsFrameOversizedDrop() {
    assertTrue(containsWire("frame_oversized_drop"), "must contain frame_oversized_drop");
  }

  // ===========================================================================
  // Locked complete set — single assertion for regression
  // ===========================================================================

  @Test
  void wireValues_lockedSet_exactlyMatches() {
    final var expected = new HashSet<String>();
    expected.add("auth_success");
    expected.add("auth_fail");
    expected.add("auth_timeout");
    expected.add("quote_request_received");
    expected.add("quote_emitted_to_session");
    expected.add("accept_quote_received");
    expected.add("reject_quote_received");
    expected.add("new_order_received");
    expected.add("cancel_order_received");
    expected.add("order_status_request");
    expected.add("order_reconciled");
    expected.add("kill_switch_press");
    expected.add("bridge_debug_toggle");
    expected.add("rate_limit_hit");
    expected.add("quote_orphaned");
    expected.add("session_terminated");
    expected.add("frame_oversized_drop");

    final var actual = new HashSet<String>();
    for (final var a : AuditAction.values()) {
      actual.add(a.wireValue());
    }

    assertEquals(
        expected, actual, "AuditAction wire-value set must exactly match the locked §3.7 taxonomy");
  }

  // ===========================================================================
  // Helpers
  // ===========================================================================

  private static boolean containsWire(final String wire) {
    for (final var a : AuditAction.values()) {
      if (a.wireValue().equals(wire)) {
        return true;
      }
    }
    return false;
  }
}
