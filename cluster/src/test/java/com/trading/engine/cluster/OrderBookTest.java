package com.trading.engine.cluster;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.OrdStatusEnum;
import com.trading.engine.messages.sbe.OrdTypeEnum;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.messages.sbe.TimeInForceEnum;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

class OrderBookTest {

  private static final long PRICE_SCALE = 100_000_000L;

  /**
   * Populate a pooled {@link OrderState} with a textual order id {@code "ORD-NNNNNNNNNNN"} matching
   * the supplied counter. Used by the snapshot round-trip tests to exercise the counter-suffix
   * parse path without standing up an IdGenerator.
   */
  private static OrderState populate(
      final OrderState state,
      final long counter,
      final String clOrdId,
      final String symbol,
      final long pxWhole,
      final long qtyWhole,
      final long accountId) {
    final byte[] orderIdBytes = new byte[OrderState.ORDER_ID_LENGTH];
    final String orderId = "ORD-" + String.format("%011d", counter);
    final byte[] src = orderId.getBytes(java.nio.charset.StandardCharsets.US_ASCII);
    System.arraycopy(src, 0, orderIdBytes, 0, src.length);
    state.setOrderIdBytes(orderIdBytes, 0);

    final byte[] clOrdIdBytes = new byte[OrderState.CL_ORD_ID_LENGTH];
    final byte[] clOrdIdSrc = clOrdId.getBytes(java.nio.charset.StandardCharsets.US_ASCII);
    System.arraycopy(clOrdIdSrc, 0, clOrdIdBytes, 0, clOrdIdSrc.length);
    state.setClOrdIdBytes(clOrdIdBytes, 0);

    final byte[] symbolBytes = new byte[OrderState.SYMBOL_LENGTH];
    final byte[] symbolSrc = symbol.getBytes(java.nio.charset.StandardCharsets.US_ASCII);
    System.arraycopy(symbolSrc, 0, symbolBytes, 0, symbolSrc.length);
    state.setSymbolBytes(symbolBytes, 0);

    state.setSide(SideEnum.Buy);
    state.setOrdType(OrdTypeEnum.Limit);
    state.setTimeInForce(TimeInForceEnum.Day);
    state.setPrice(pxWhole * PRICE_SCALE);
    state.setOrderQty(qtyWhole * PRICE_SCALE);
    state.setLeavesQty(qtyWhole * PRICE_SCALE);
    state.setCumQty(0L);
    state.setOrdStatus(OrdStatusEnum.New);
    state.setAccountId(accountId);
    state.setTransactTime(1_700_000_000_000_000_000L + counter);
    return state;
  }

  // ---------------------------------------------------------------------------
  // Pool + lookup
  // ---------------------------------------------------------------------------

  @Test
  void acquireAndGet() {
    final OrderBook book = new OrderBook(8);
    final OrderState acquired = book.acquire(1L);
    populate(acquired, 1L, "CL-1", "EURUSD", 1L, 1_000_000L, 42L);

    assertEquals(1, book.size());
    assertEquals(1L, acquired.orderKey());
    assertTrue(book.contains(1L));

    final OrderState fetched = book.get(1L);
    assertNotNull(fetched);
    assertEquals(acquired, fetched);
    assertEquals(42L, fetched.accountId());
  }

  @Test
  void getReturnsNullForUnknownKey() {
    final OrderBook book = new OrderBook(8);
    assertNull(book.get(999L));
    assertFalse(book.contains(999L));
  }

  @Test
  void multipleOrdersTrackedIndependently() {
    final OrderBook book = new OrderBook(8);
    populate(book.acquire(1L), 1L, "CL-1", "EURUSD", 1L, 1_000_000L, 10L);
    populate(book.acquire(2L), 2L, "CL-2", "GBPUSD", 1L, 2_000_000L, 20L);
    populate(book.acquire(3L), 3L, "CL-3", "USDJPY", 150L, 500_000L, 30L);

    assertEquals(3, book.size());
    assertEquals(10L, book.get(1L).accountId());
    assertEquals(20L, book.get(2L).accountId());
    assertEquals(30L, book.get(3L).accountId());
  }

  @Test
  void acquireDuplicateKeyRejected() {
    final OrderBook book = new OrderBook(8);
    book.acquire(1L);
    assertThrows(IllegalStateException.class, () -> book.acquire(1L));
  }

  @Test
  void acquireExhaustsPoolReturnsNull() {
    final OrderBook book = new OrderBook(3);
    assertNotNull(book.acquire(1L));
    assertNotNull(book.acquire(2L));
    assertNotNull(book.acquire(3L));
    assertNull(book.acquire(4L), "pool should be exhausted");
    assertEquals(3, book.size());
  }

  @Test
  void releaseReturnsSlotToPool() {
    final OrderBook book = new OrderBook(2);
    final OrderState a = book.acquire(1L);
    final OrderState b = book.acquire(2L);
    assertNull(book.acquire(3L)); // pool exhausted

    // Release slot held by orderKey=1 — we should now be able to acquire a new key.
    book.release(1L);
    assertEquals(1, book.size());
    assertFalse(book.contains(1L));
    final OrderState c = book.acquire(3L);
    assertNotNull(c);
    assertEquals(2, book.size());

    // b was never released and must still be retrievable.
    assertEquals(b, book.get(2L));
    // a is now unused; after re-acquire the pool must have handed us a fresh instance.
    assertEquals(0L, book.get(3L).accountId()); // reset() zeroed the fields
    populate(c, 3L, "CL-3", "USDJPY", 150L, 500_000L, 99L);
  }

  @Test
  void releaseUnknownKeyIsNoOp() {
    final OrderBook book = new OrderBook(4);
    book.acquire(1L);
    book.release(999L); // not present
    assertEquals(1, book.size());
    assertNotNull(book.get(1L));
  }

  @Test
  void clearEmptiesAndRestoresFullPool() {
    final OrderBook book = new OrderBook(3);
    book.acquire(1L);
    book.acquire(2L);
    book.acquire(3L);
    assertNull(book.acquire(4L));

    book.clear();
    assertEquals(0, book.size());
    assertNull(book.get(1L));

    // All three slots are back in the free-list.
    assertNotNull(book.acquire(10L));
    assertNotNull(book.acquire(11L));
    assertNotNull(book.acquire(12L));
    assertNull(book.acquire(13L));
  }

  @Test
  void acquireResetsPreviouslyUsedSlotFields() {
    final OrderBook book = new OrderBook(2);
    final OrderState first = book.acquire(1L);
    populate(first, 1L, "CL-1", "EURUSD", 1L, 1_000_000L, 42L);
    book.release(1L);

    final OrderState reused = book.acquire(2L);
    assertEquals(2L, reused.orderKey());
    assertEquals(0L, reused.accountId());
    assertEquals(0L, reused.price());
    assertEquals(0L, reused.orderQty());
    assertEquals(0L, reused.transactTime());
    assertEquals(SideEnum.NULL_VAL, reused.side());
    assertEquals(OrdTypeEnum.NULL_VAL, reused.ordType());
    // orderId buffer was zeroed.
    for (int i = 0; i < OrderState.ORDER_ID_LENGTH; i++) {
      assertEquals((byte) 0, reused.orderIdByte(i));
    }
  }

  @Test
  void capacityGetter() {
    assertEquals(128, new OrderBook(128).capacity());
  }

  @Test
  void rejectsNonPositiveCapacity() {
    assertThrows(IllegalArgumentException.class, () -> new OrderBook(0));
    assertThrows(IllegalArgumentException.class, () -> new OrderBook(-5));
  }

  @Test
  void rejectsCapacityAboveSbeGroupLimit() {
    // The SBE OrderBookSnapshot.noOrders repeating group encodes numInGroup as uint16 and the
    // generated encoder throws for count > MAX_CAPACITY (65_534). A book whose pool is larger
    // than that could not be snapshotted at full capacity.
    assertThrows(IllegalArgumentException.class, () -> new OrderBook(OrderBook.MAX_CAPACITY + 1));
    assertThrows(IllegalArgumentException.class, () -> new OrderBook(Integer.MAX_VALUE));
    // The boundary value itself is accepted.
    assertEquals(OrderBook.MAX_CAPACITY, new OrderBook(OrderBook.MAX_CAPACITY).capacity());
  }

  // ---------------------------------------------------------------------------
  // Snapshot round-trip
  // ---------------------------------------------------------------------------

  @Test
  void snapshotRoundTripPopulated() {
    final OrderBook src = new OrderBook(16);
    populate(src.acquire(1L), 1L, "CL-1", "EURUSD", 1L, 1_000_000L, 100L);
    populate(src.acquire(2L), 2L, "CL-2", "GBPUSD", 1L, 2_000_000L, 200L);
    populate(src.acquire(3L), 3L, "CL-3", "USDJPY", 150L, 500_000L, 300L);

    final MutableDirectBuffer buf = new ExpandableArrayBuffer(4096);
    final int written = src.snapshotTo(buf, 0);
    assertTrue(written > 0);

    final OrderBook restored = new OrderBook(16);
    final int read = restored.restoreFrom(buf, 0);
    assertEquals(written, read);
    assertEquals(3, restored.size());

    final OrderState r1 = restored.get(1L);
    assertNotNull(r1);
    assertEquals(100L, r1.accountId());
    assertEquals(1L * PRICE_SCALE, r1.price());
    assertEquals(1_000_000L * PRICE_SCALE, r1.orderQty());
    assertEquals(1_000_000L * PRICE_SCALE, r1.leavesQty());
    assertEquals(0L, r1.cumQty());
    assertEquals(SideEnum.Buy, r1.side());
    assertEquals(OrdTypeEnum.Limit, r1.ordType());
    assertEquals(TimeInForceEnum.Day, r1.timeInForce());

    // OrderId bytes round-trip: "ORD-00000000001" padded with zeros to 20.
    final byte[] expected = new byte[OrderState.ORDER_ID_LENGTH];
    final byte[] src20 = "ORD-00000000001".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
    System.arraycopy(src20, 0, expected, 0, src20.length);
    final byte[] actual = new byte[OrderState.ORDER_ID_LENGTH];
    r1.copyOrderIdTo(actual, 0);
    assertArrayEquals(expected, actual);
  }

  @Test
  void snapshotRoundTripEmpty() {
    final OrderBook src = new OrderBook(8);
    final MutableDirectBuffer buf = new ExpandableArrayBuffer(128);
    final int written = src.snapshotTo(buf, 0);
    final OrderBook restored = new OrderBook(8);
    final int read = restored.restoreFrom(buf, 0);
    assertEquals(written, read);
    assertEquals(0, restored.size());
  }

  @Test
  void restoreDropsPrePopulatedEntries() {
    final OrderBook src = new OrderBook(8);
    populate(src.acquire(1L), 1L, "CL-1", "EURUSD", 1L, 1_000_000L, 100L);
    final MutableDirectBuffer buf = new ExpandableArrayBuffer(1024);
    src.snapshotTo(buf, 0);

    final OrderBook dst = new OrderBook(8);
    populate(dst.acquire(1L), 1L, "CL-1", "EURUSD", 1L, 1_000_000L, 100L);
    populate(dst.acquire(99L), 99L, "CL-99", "USDJPY", 150L, 1L, 999L); // orphan
    assertEquals(2, dst.size());

    dst.restoreFrom(buf, 0);
    assertEquals(1, dst.size());
    assertNull(dst.get(99L));
    assertNotNull(dst.get(1L));
  }

  @Test
  void snapshotIsDeterministicAcrossInsertOrder() {
    final OrderBook a = new OrderBook(16);
    populate(a.acquire(1L), 1L, "CL-1", "EURUSD", 1L, 1_000_000L, 10L);
    populate(a.acquire(2L), 2L, "CL-2", "GBPUSD", 1L, 2_000_000L, 20L);
    populate(a.acquire(3L), 3L, "CL-3", "USDJPY", 150L, 500_000L, 30L);

    final OrderBook b = new OrderBook(16);
    populate(b.acquire(3L), 3L, "CL-3", "USDJPY", 150L, 500_000L, 30L);
    populate(b.acquire(1L), 1L, "CL-1", "EURUSD", 1L, 1_000_000L, 10L);
    populate(b.acquire(2L), 2L, "CL-2", "GBPUSD", 1L, 2_000_000L, 20L);

    final MutableDirectBuffer bufA = new ExpandableArrayBuffer(4096);
    final MutableDirectBuffer bufB = new ExpandableArrayBuffer(4096);
    final int writtenA = a.snapshotTo(bufA, 0);
    final int writtenB = b.snapshotTo(bufB, 0);
    assertEquals(writtenA, writtenB);

    final byte[] bytesA = new byte[writtenA];
    final byte[] bytesB = new byte[writtenB];
    bufA.getBytes(0, bytesA);
    bufB.getBytes(0, bytesB);
    assertArrayEquals(
        bytesA, bytesB, "snapshot bytes must be deterministic regardless of insert order");
  }

  @Test
  void snapshotRoundTripToleratesHyphenInPrefix() {
    // parseOrderKey must find the LAST hyphen, not the first — otherwise an IdGenerator prefix
    // containing its own hyphen (e.g. "FX-ORD") would round-trip incorrectly. Simulate an
    // "FX-ORD-00000000042" textual id (3+1+3+1+11=19 bytes, padded to 20) by populating the
    // state manually and verifying the round-trip key lands on 42.
    final OrderBook src = new OrderBook(4);
    final OrderState state = src.acquire(42L);
    final byte[] orderIdBytes = new byte[OrderState.ORDER_ID_LENGTH];
    final byte[] prefix = "FX-ORD-00000000042".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
    System.arraycopy(prefix, 0, orderIdBytes, 0, prefix.length);
    state.setOrderIdBytes(orderIdBytes, 0);
    state.setSide(SideEnum.Buy);
    state.setOrdType(OrdTypeEnum.Limit);
    state.setTimeInForce(TimeInForceEnum.Day);

    final MutableDirectBuffer buf = new ExpandableArrayBuffer(1024);
    final int written = src.snapshotTo(buf, 0);

    final OrderBook restored = new OrderBook(4);
    restored.restoreFrom(buf, 0);
    assertNotNull(restored.get(42L), "round-trip must locate the key parsed from the LAST hyphen");
    assertEquals(1, restored.size());
  }

  @Test
  void snapshotAtNonZeroOffset() {
    // Write the snapshot at a non-zero offset to verify the encoder/decoder honour it.
    final OrderBook src = new OrderBook(4);
    populate(src.acquire(42L), 42L, "CL-42", "EURUSD", 1L, 1_000L, 1L);
    final UnsafeBuffer buf = new UnsafeBuffer(new byte[2048]);
    final int baseOffset = 128;
    final int written = src.snapshotTo(buf, baseOffset);
    assertTrue(written > 0);

    final OrderBook restored = new OrderBook(4);
    final int read = restored.restoreFrom(buf, baseOffset);
    assertEquals(written, read);
    assertEquals(1, restored.size());
    assertNotNull(restored.get(42L));
  }
}
