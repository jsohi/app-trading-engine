package com.trading.engine.fixbridge.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/**
 * Verifies the pool-style mutability rules of {@link RetainedEvent}: bind once, read fields, then
 * release before re-binding. Exercises every {@code setX} variant + {@code release} round-trip.
 */
final class RetainedEventTest {

  @Test
  void newInstance_isFree() {
    final var r = new RetainedEvent();
    assertEquals(RetainedEvent.KIND_FREE, r.kind());
  }

  @Test
  void setOrderReject_thenRelease_returnsToFreeState() {
    final var r = new RetainedEvent();
    r.setOrderReject("C-1", "bridge-down");

    assertEquals(RetainedEvent.KIND_ORDER_REJECT, r.kind());
    assertEquals("C-1", r.clOrdId());
    assertEquals("bridge-down", r.reason());

    r.release();
    assertEquals(RetainedEvent.KIND_FREE, r.kind());
    assertNull(r.clOrdId());
    assertNull(r.reason());
  }

  @Test
  void setExecutionReport_storesAllFields() {
    final var r = new RetainedEvent();
    r.setExecutionReport("C-1", "EX-1", 'F', '2', "EURUSD", "Buy", 100L, 200L, 110_000_000L);

    assertEquals(RetainedEvent.KIND_EXEC_REPORT, r.kind());
    assertEquals("C-1", r.clOrdId());
    assertEquals("EX-1", r.execId());
    assertEquals('F', r.execType());
    assertEquals('2', r.ordStatus());
    assertEquals("EURUSD", r.symbol());
    assertEquals("Buy", r.side());
    assertEquals(100L, r.cumQtyInt64());
    assertEquals(200L, r.leavesQtyInt64());
    assertEquals(110_000_000L, r.avgPxInt64());
  }

  @Test
  void setAuthExpired_marksKind() {
    final var r = new RetainedEvent();
    r.setAuthExpired();
    assertEquals(RetainedEvent.KIND_AUTH_EXPIRED, r.kind());
  }

  @Test
  void setOrderRejectWhenAlreadyBound_throwsIllegalState() {
    final var r = new RetainedEvent();
    r.setOrderReject("C", "x");
    assertThrows(IllegalStateException.class, () -> r.setOrderReject("C2", "y"));
  }

  @Test
  void setExecutionReportWhenAlreadyBound_throwsIllegalState() {
    final var r = new RetainedEvent();
    r.setOrderReject("C", "x");
    assertThrows(
        IllegalStateException.class,
        () -> r.setExecutionReport("C", "X", 'F', '2', "S", "Buy", 0L, 0L, 0L));
  }

  @Test
  void setAuthExpiredWhenAlreadyBound_throwsIllegalState() {
    final var r = new RetainedEvent();
    r.setOrderReject("C", "x");
    assertThrows(IllegalStateException.class, r::setAuthExpired);
  }

  @Test
  void release_thenRebind_succeeds() {
    final var r = new RetainedEvent();
    r.setOrderReject("C", "x");
    r.release();
    r.setExecutionReport("C2", "EX", 'F', '0', "S", "Sell", 1L, 2L, 3L);
    assertEquals(RetainedEvent.KIND_EXEC_REPORT, r.kind());
  }
}
