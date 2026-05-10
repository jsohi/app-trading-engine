package com.trading.engine.fixbridge.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for the new {@link BrowserEvent} record variants added in APP-40a phase-6: {@link
 * BrowserEvent.AccountLimits}, {@link BrowserEvent.SessionTerminated}, {@link
 * BrowserEvent.OrderReconciled}, {@link BrowserEvent.OrderStatusReply}, and the extensions to
 * {@link BrowserEvent.BridgeStatus} (3-arg and 7-arg ctors) and {@link BrowserEvent.OrderReject}
 * (typed convenience ctor). Pre-existing record variants are covered in their own test files.
 */
class BrowserEventNewRecordsTest {

  // ===========================================================================
  // AccountLimits
  // ===========================================================================

  @Test
  void accountLimits_fieldsRoundTrip() {
    final var evt =
        new BrowserEvent.AccountLimits(
            "ACCT-1",
            500_000_000L, // maxQtyInt64
            10_000_000_000L, // maxNotionalInt64
            50, // priceDeviationBps
            100); // maxOrdersPerSecond

    assertEquals("ACCT-1", evt.account());
    assertEquals(500_000_000L, evt.maxQtyInt64());
    assertEquals(10_000_000_000L, evt.maxNotionalInt64());
    assertEquals(50, evt.priceDeviationBps());
    assertEquals(100, evt.maxOrdersPerSecond());
    assertTrue(evt instanceof BrowserEvent, "AccountLimits must implement BrowserEvent");
  }

  @Test
  void accountLimits_zeroValues_accepted() {
    final var evt = new BrowserEvent.AccountLimits("ACCT-2", 0L, 0L, 0, 0);
    assertEquals(0L, evt.maxQtyInt64());
    assertEquals(0L, evt.maxNotionalInt64());
    assertEquals(0, evt.priceDeviationBps());
    assertEquals(0, evt.maxOrdersPerSecond());
  }

  // ===========================================================================
  // SessionTerminated singleton
  // ===========================================================================

  @Test
  void sessionTerminated_instance_isSingleton() {
    assertSame(
        BrowserEvent.SessionTerminated.INSTANCE,
        BrowserEvent.SessionTerminated.INSTANCE,
        "INSTANCE must return the same object reference every time");
  }

  @Test
  void sessionTerminated_newInstance_equalsINSTANCE() {
    // Records use value equality — a fresh record with the same components must equal INSTANCE.
    final var fresh = new BrowserEvent.SessionTerminated();
    assertEquals(
        BrowserEvent.SessionTerminated.INSTANCE,
        fresh,
        "new SessionTerminated() must equal INSTANCE via record equality");
  }

  @Test
  void sessionTerminated_isBrowserEvent() {
    assertTrue(
        BrowserEvent.SessionTerminated.INSTANCE instanceof BrowserEvent,
        "SessionTerminated must implement BrowserEvent");
  }

  // ===========================================================================
  // OrderReconciled — valid status strings
  // ===========================================================================

  @Test
  void orderReconciled_filledStatus_storesFields() {
    final var evt =
        new BrowserEvent.OrderReconciled("ORD-1", "Filled", 100_000_000L, 0L, 150_000_000L);
    assertEquals("ORD-1", evt.clOrdId());
    assertEquals("Filled", evt.status());
    assertEquals(100_000_000L, evt.cumQtyInt64());
    assertEquals(0L, evt.leavesQtyInt64());
    assertEquals(150_000_000L, evt.avgPxInt64());
  }

  @Test
  void orderReconciled_partiallyFilledStatus_storesString() {
    final var evt =
        new BrowserEvent.OrderReconciled(
            "ORD-2", "PartiallyFilled", 50_000_000L, 50_000_000L, 149_000_000L);
    assertEquals("PartiallyFilled", evt.status());
  }

  @Test
  void orderReconciled_cancelledStatus_storesString() {
    final var evt = new BrowserEvent.OrderReconciled("ORD-3", "Cancelled", 0L, 0L, 0L);
    assertEquals("Cancelled", evt.status());
  }

  @Test
  void orderReconciled_rejectedStatus_storesString() {
    final var evt = new BrowserEvent.OrderReconciled("ORD-4", "Rejected", 0L, 0L, 0L);
    assertEquals("Rejected", evt.status());
  }

  @Test
  void orderReconciled_workingStatus_storesString() {
    final var evt = new BrowserEvent.OrderReconciled("ORD-5", "Working", 0L, 100_000_000L, 0L);
    assertEquals("Working", evt.status());
  }

  @Test
  void orderReconciled_unknownStatus_storesString() {
    final var evt = new BrowserEvent.OrderReconciled("ORD-6", "Unknown", 0L, 0L, 0L);
    assertEquals("Unknown", evt.status());
  }

  // ===========================================================================
  // OrderStatusReply
  // ===========================================================================

  @Test
  void orderStatusReply_lastExecIdNull_accepted() {
    final var evt =
        new BrowserEvent.OrderStatusReply("ORD-7", "Working", 0L, 100_000_000L, 0L, null);
    assertEquals("ORD-7", evt.clOrdId());
    assertEquals("Working", evt.status());
    assertEquals(0L, evt.cumQtyInt64());
    assertEquals(100_000_000L, evt.leavesQtyInt64());
    assertEquals(0L, evt.avgPxInt64());
    assertNull(evt.lastExecId(), "lastExecId must be null when no executions exist");
  }

  @Test
  void orderStatusReply_lastExecIdPresent_storesField() {
    final var evt =
        new BrowserEvent.OrderStatusReply(
            "ORD-8", "Filled", 200_000_000L, 0L, 155_000_000L, "EXEC-99");
    assertEquals("EXEC-99", evt.lastExecId());
  }

  @Test
  void orderStatusReply_isBrowserEvent() {
    final var evt = new BrowserEvent.OrderStatusReply("x", "Unknown", 0L, 0L, 0L, null);
    assertTrue(evt instanceof BrowserEvent);
  }

  // ===========================================================================
  // BridgeStatus — 3-arg backward-compat ctor
  // ===========================================================================

  @Test
  void bridgeStatus_threeArgCtor_defaultsNewOrdersTrue() {
    final var evt = new BrowserEvent.BridgeStatus(true, false, "ok");
    assertTrue(evt.newOrders(), "3-arg ctor must default newOrders=true");
  }

  @Test
  void bridgeStatus_threeArgCtor_defaultsNewQuotesTrue() {
    final var evt = new BrowserEvent.BridgeStatus(false, false, "shutdown");
    assertTrue(evt.newQuotes(), "3-arg ctor must default newQuotes=true");
  }

  @Test
  void bridgeStatus_threeArgCtor_defaultsProtocolVersionOne() {
    final var evt = new BrowserEvent.BridgeStatus(true, true, "test");
    assertEquals(1, evt.protocolVersion(), "3-arg ctor must default protocolVersion=1");
  }

  @Test
  void bridgeStatus_threeArgCtor_defaultsServerOrderTimeoutMsZero() {
    final var evt = new BrowserEvent.BridgeStatus(true, false, "reason");
    assertEquals(0L, evt.serverOrderTimeoutMs(), "3-arg ctor must default serverOrderTimeoutMs=0L");
  }

  @Test
  void bridgeStatus_threeArgCtor_preservesExplicitFields() {
    final var evt = new BrowserEvent.BridgeStatus(true, true, "detail");
    assertTrue(evt.fixSessionUp());
    assertTrue(evt.fatal());
    assertEquals("detail", evt.reason());
  }

  // ===========================================================================
  // BridgeStatus — 7-arg full ctor
  // ===========================================================================

  @Test
  void bridgeStatus_sevenArgCtor_preservesAllFields() {
    final var evt = new BrowserEvent.BridgeStatus(true, false, "running", false, false, 3, 30_000L);
    assertTrue(evt.fixSessionUp());
    assertTrue(!evt.fatal());
    assertEquals("running", evt.reason());
    assertTrue(!evt.newOrders(), "newOrders must be false");
    assertTrue(!evt.newQuotes(), "newQuotes must be false");
    assertEquals(3, evt.protocolVersion());
    assertEquals(30_000L, evt.serverOrderTimeoutMs());
  }

  @Test
  void bridgeStatus_sevenArgCtor_isBrowserEvent() {
    final var evt = new BrowserEvent.BridgeStatus(true, false, "ok", true, true, 1, 0L);
    assertTrue(evt instanceof BrowserEvent);
  }

  // ===========================================================================
  // OrderReject — typed convenience ctor
  // ===========================================================================

  @Test
  void orderReject_typedCtor_reasonStringEqualsWireValue() {
    final var reason = OrderRejectReason.QUOTE_EXPIRED;
    final var evt = new BrowserEvent.OrderReject("C-1", reason);
    assertEquals(
        reason.wireValue(),
        evt.reason(),
        "typed ctor must store reason.wireValue() as the reason String");
  }

  @Test
  void orderReject_typedCtor_clOrdIdPreserved() {
    final var evt = new BrowserEvent.OrderReject("ORDER-42", OrderRejectReason.BACKPRESSURE);
    assertEquals("ORDER-42", evt.clOrdId());
  }

  @Test
  void orderReject_typedCtor_everyReason_doesNotThrow() {
    // All 15 enum constants should wire through the typed ctor without exception.
    for (final var reason : OrderRejectReason.values()) {
      final var evt = new BrowserEvent.OrderReject("X", reason);
      assertEquals(reason.wireValue(), evt.reason());
    }
  }

  @Test
  void orderReject_typedCtor_isBrowserEvent() {
    final var evt = new BrowserEvent.OrderReject("C-2", OrderRejectReason.INTERNAL);
    assertTrue(evt instanceof BrowserEvent);
  }
}
