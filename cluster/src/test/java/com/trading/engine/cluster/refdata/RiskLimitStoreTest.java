package com.trading.engine.cluster.refdata;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.AccountStatusEnum;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.Test;

class RiskLimitStoreTest {

  static RiskLimitState makeState(long accountId, long maxOrderSize, long maxDailyVolume) {
    final RiskLimitState s = new RiskLimitState();
    s.setAccountId(accountId);
    s.setMaxOrderSize(maxOrderSize);
    s.setMaxOrderNotional(0L);
    s.setMaxDailyVolume(maxDailyVolume);
    s.setStatus(AccountStatusEnum.Active);
    s.setTransactTime(0L);
    return s;
  }

  @Test
  void putGet() {
    final RiskLimitStore store = new RiskLimitStore();
    store.put(makeState(1L, 1_000_00000000L, 10_000_00000000L));
    assertEquals(1, store.size());
    final RiskLimitState found = store.get(1L);
    assertNotNull(found);
    assertEquals(1L, found.accountId());
    assertEquals(1_000_00000000L, found.maxOrderSize());
  }

  @Test
  void getReturnsNullForUnknown() {
    final RiskLimitStore store = new RiskLimitStore();
    assertNull(store.get(99L));
    assertFalse(store.contains(99L));
  }

  @Test
  void upsertOverwrites() {
    final RiskLimitStore store = new RiskLimitStore();
    store.put(makeState(1L, 100L, 1000L));
    store.put(makeState(1L, 500L, 5000L));
    assertEquals(1, store.size());
    assertEquals(500L, store.get(1L).maxOrderSize());
    assertEquals(5000L, store.get(1L).maxDailyVolume());
  }

  @Test
  void clearEmpties() {
    final RiskLimitStore store = new RiskLimitStore();
    store.put(makeState(1L, 100L, 1000L));
    store.put(makeState(2L, 200L, 2000L));
    store.clear();
    assertEquals(0, store.size());
  }

  @Test
  void snapshotRoundTrip() {
    final RiskLimitStore src = new RiskLimitStore();
    src.put(makeState(1L, 100_00000000L, 1000_00000000L));
    src.put(makeState(2L, 200_00000000L, 2000_00000000L));
    src.put(makeState(3L, 0L, 0L)); // unlimited

    final MutableDirectBuffer buf = new ExpandableArrayBuffer(2048);
    int written = src.snapshotTo(buf, 0);
    assertTrue(written > 0);

    final RiskLimitStore restored = new RiskLimitStore();
    int read = restored.restoreFrom(buf, 0);
    assertEquals(written, read);
    assertEquals(3, restored.size());

    assertEquals(100_00000000L, restored.get(1L).maxOrderSize());
    assertEquals(0L, restored.get(3L).maxOrderSize());
  }

  @Test
  void restoreOverPrePopulatedStoreDropsOrphans() {
    // The defensive clear() at the start of restoreFrom must drop pre-existing entries.
    final RiskLimitStore src = new RiskLimitStore();
    src.put(makeState(1L, 100L, 1000L));
    final MutableDirectBuffer buf = new ExpandableArrayBuffer(1024);
    src.snapshotTo(buf, 0);

    final RiskLimitStore dst = new RiskLimitStore();
    dst.put(makeState(1L, 100L, 1000L));
    dst.put(makeState(99L, 200L, 2000L)); // orphan
    assertEquals(2, dst.size());

    dst.restoreFrom(buf, 0);
    assertEquals(1, dst.size());
    assertNull(dst.get(99L));
    assertNotNull(dst.get(1L));
  }

  @Test
  void snapshotIsDeterministic() {
    final RiskLimitStore a = new RiskLimitStore();
    a.put(makeState(1L, 1L, 1L));
    a.put(makeState(2L, 2L, 2L));
    a.put(makeState(3L, 3L, 3L));

    final RiskLimitStore b = new RiskLimitStore();
    b.put(makeState(3L, 3L, 3L));
    b.put(makeState(1L, 1L, 1L));
    b.put(makeState(2L, 2L, 2L));

    final MutableDirectBuffer bufA = new ExpandableArrayBuffer(1024);
    final MutableDirectBuffer bufB = new ExpandableArrayBuffer(1024);
    int wA = a.snapshotTo(bufA, 0);
    int wB = b.snapshotTo(bufB, 0);
    assertEquals(wA, wB);

    byte[] bA = new byte[wA];
    byte[] bB = new byte[wB];
    bufA.getBytes(0, bA);
    bufB.getBytes(0, bB);
    assertArrayEquals(bA, bB);
  }
}
