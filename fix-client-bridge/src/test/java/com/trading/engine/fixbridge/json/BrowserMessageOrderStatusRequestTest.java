package com.trading.engine.fixbridge.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for the {@link BrowserMessage.OrderStatusRequest} record added in APP-40a phase-6.
 * Covers field accessor, record equality / hashCode contract, and sealed-type membership in {@link
 * BrowserMessage}.
 */
class BrowserMessageOrderStatusRequestTest {

  // ===========================================================================
  // Field accessor
  // ===========================================================================

  @Test
  void orderStatusRequest_clOrdId_returnsConstructorArg() {
    final var msg = new BrowserMessage.OrderStatusRequest("C-1");
    assertEquals("C-1", msg.clOrdId(), "clOrdId() must return the string supplied at construction");
  }

  @Test
  void orderStatusRequest_differentClOrdId_storedVerbatim() {
    final var msg = new BrowserMessage.OrderStatusRequest("ORDER-999");
    assertEquals("ORDER-999", msg.clOrdId());
  }

  // ===========================================================================
  // Sealed-type membership
  // ===========================================================================

  @Test
  void orderStatusRequest_isBrowserMessage() {
    final BrowserMessage msg = new BrowserMessage.OrderStatusRequest("C-2");
    assertTrue(msg instanceof BrowserMessage, "OrderStatusRequest must be a BrowserMessage");
    assertTrue(
        msg instanceof BrowserMessage.OrderStatusRequest,
        "object must be an instance of the concrete record type");
  }

  // ===========================================================================
  // Record equality
  // ===========================================================================

  @Test
  void orderStatusRequest_equality_sameClOrdId_areEqual() {
    final var a = new BrowserMessage.OrderStatusRequest("C-1");
    final var b = new BrowserMessage.OrderStatusRequest("C-1");
    assertEquals(
        a,
        b,
        "two OrderStatusRequest records with the same clOrdId must be equal (record equality)");
  }

  @Test
  void orderStatusRequest_equality_differentClOrdId_notEqual() {
    final var a = new BrowserMessage.OrderStatusRequest("C-1");
    final var b = new BrowserMessage.OrderStatusRequest("C-2");
    assertNotEquals(a, b, "records with different clOrdId values must not be equal");
  }

  @Test
  void orderStatusRequest_equality_reflexive() {
    final var a = new BrowserMessage.OrderStatusRequest("C-1");
    assertEquals(a, a, "a record must equal itself");
  }

  // ===========================================================================
  // Record hashCode contract
  // ===========================================================================

  @Test
  void orderStatusRequest_hashCode_equalObjectsHaveSameHash() {
    final var a = new BrowserMessage.OrderStatusRequest("C-1");
    final var b = new BrowserMessage.OrderStatusRequest("C-1");
    assertEquals(a.hashCode(), b.hashCode(), "equal records must have the same hashCode");
  }
}
