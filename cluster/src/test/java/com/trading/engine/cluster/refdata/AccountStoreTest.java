package com.trading.engine.cluster.refdata;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.AccountTypeEnum;
import com.trading.engine.messages.sbe.AcctIDSourceEnum;
import com.trading.engine.messages.sbe.ComplianceStatusEnum;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

class AccountStoreTest {

  static AccountState makeState(
      final long accountId, final String code, final String name, final String baseCcy) {
    final AccountState s = new AccountState();
    s.setAccountId(accountId);
    s.setParentAccountId(0L);
    final byte[] codeBytes = code.getBytes();
    s.setAccountCode(codeBytes, 0, codeBytes.length);
    s.setAcctIdSource(AcctIDSourceEnum.Internal);
    final byte[] nameBytes = name.getBytes();
    s.setAccountName(nameBytes, 0, nameBytes.length);
    s.setAccountType(AccountTypeEnum.Client);
    final byte[] ccy = baseCcy.getBytes();
    s.setBaseCurrency(ccy[0], ccy[1], ccy[2]);
    s.setStatus(AccountStatusEnum.Active);
    s.setComplianceStatus(ComplianceStatusEnum.OK);
    s.setCapabilities(AccountState.Capabilities.CAN_TRADE | AccountState.Capabilities.CAN_RFQ);
    s.setTransactTime(0L);
    return s;
  }

  @Test
  void putGetByPrimaryKey() {
    final AccountStore store = new AccountStore();
    store.put(makeState(1L, "ACME", "Acme Inc", "USD"));
    assertEquals(1, store.size());

    final AccountState found = store.get(1L);
    assertNotNull(found);
    assertEquals(1L, found.accountId());
    assertEquals(4, found.accountCodeLength());
    assertTrue(found.canTrade());
    assertTrue(found.canRequestQuotes());
  }

  @Test
  void getByCodeFromDirectBuffer() {
    final AccountStore store = new AccountStore();
    store.put(makeState(7L, "BIGCO", "Big Corp", "EUR"));
    final UnsafeBuffer probe = new UnsafeBuffer(new byte[] {'B', 'I', 'G', 'C', 'O'});
    final AccountState found = store.getByCode(probe, 0, 5);
    assertNotNull(found);
    assertEquals(7L, found.accountId());
  }

  @Test
  void getByCodeBytes() {
    final AccountStore store = new AccountStore();
    store.put(makeState(7L, "BIGCO", "Big Corp", "EUR"));
    final byte[] probe = {'B', 'I', 'G', 'C', 'O'};
    final AccountState found = store.getByCodeBytes(probe, 0, 5);
    assertNotNull(found);
    assertEquals(7L, found.accountId());
  }

  @Test
  void getByCodeReturnsNullForUnknown() {
    final AccountStore store = new AccountStore();
    store.put(makeState(1L, "ACME", "Acme", "USD"));
    final UnsafeBuffer probe = new UnsafeBuffer(new byte[] {'X', 'Y', 'Z'});
    assertNull(store.getByCode(probe, 0, 3));
    assertFalse(store.containsCode(probe, 0, 3));
  }

  @Test
  void upsertSameIdSameCodeOverwrites() {
    final AccountStore store = new AccountStore();
    store.put(makeState(1L, "ACME", "Acme v1", "USD"));
    store.put(makeState(1L, "ACME", "Acme v2", "USD"));
    assertEquals(1, store.size());
    final byte[] expectedName = "Acme v2".getBytes();
    final byte[] actual = new byte[expectedName.length];
    store.get(1L).copyAccountNameTo(actual, 0);
    assertArrayEquals(expectedName, actual);
  }

  @Test
  void upsertSameIdDifferentCodeRebuildsSecondaryIndex() {
    final AccountStore store = new AccountStore();
    store.put(makeState(1L, "ACME", "Acme", "USD"));

    // Verify ACME is findable.
    final UnsafeBuffer acme = new UnsafeBuffer(new byte[] {'A', 'C', 'M', 'E'});
    assertNotNull(store.getByCode(acme, 0, 4));

    // Re-load same accountId with a different code.
    store.put(makeState(1L, "NEWCODE", "Acme Renamed", "USD"));
    assertEquals(1, store.size());

    // Old code is gone, new code resolves.
    assertNull(store.getByCode(acme, 0, 4));
    final UnsafeBuffer newCode = new UnsafeBuffer(new byte[] {'N', 'E', 'W', 'C', 'O', 'D', 'E'});
    assertNotNull(store.getByCode(newCode, 0, 7));
  }

  @Test
  void clearEmptiesBothIndexes() {
    final AccountStore store = new AccountStore();
    store.put(makeState(1L, "ACME", "Acme", "USD"));
    store.put(makeState(2L, "BIGCO", "Big", "EUR"));
    store.clear();
    assertEquals(0, store.size());
    final UnsafeBuffer acme = new UnsafeBuffer(new byte[] {'A', 'C', 'M', 'E'});
    assertNull(store.getByCode(acme, 0, 4));
    assertNull(store.get(1L));
  }

  // ---------------------------------------------------------------------------
  // Snapshot round-trip including secondary index rebuild
  // ---------------------------------------------------------------------------

  @Test
  void snapshotRoundTripPopulated() {
    final AccountStore src = new AccountStore();
    src.put(makeState(1L, "ACME", "Acme", "USD"));
    src.put(makeState(2L, "BIGCO", "Big Corp", "EUR"));
    src.put(makeState(3L, "JPN", "Japan Trading", "JPY"));

    final MutableDirectBuffer buf = new ExpandableArrayBuffer(4096);
    final int written = src.snapshotTo(buf, 0);
    assertTrue(written > 0);

    final AccountStore restored = new AccountStore();
    final int read = restored.restoreFrom(buf, 0);
    assertEquals(written, read);
    assertEquals(3, restored.size());

    // Primary index intact.
    assertNotNull(restored.get(1L));
    assertNotNull(restored.get(2L));
    assertNotNull(restored.get(3L));

    // Secondary index rebuilt.
    final UnsafeBuffer acme = new UnsafeBuffer(new byte[] {'A', 'C', 'M', 'E'});
    final AccountState foundAcme = restored.getByCode(acme, 0, 4);
    assertNotNull(foundAcme);
    assertEquals(1L, foundAcme.accountId());

    final UnsafeBuffer bigco = new UnsafeBuffer(new byte[] {'B', 'I', 'G', 'C', 'O'});
    assertEquals(2L, restored.getByCode(bigco, 0, 5).accountId());

    final UnsafeBuffer jpn = new UnsafeBuffer(new byte[] {'J', 'P', 'N'});
    assertEquals(3L, restored.getByCode(jpn, 0, 3).accountId());

    // Capabilities round-trip.
    assertTrue(foundAcme.canTrade());
    assertTrue(foundAcme.canRequestQuotes());
  }

  @Test
  void snapshotRoundTripEmpty() {
    final AccountStore src = new AccountStore();
    final MutableDirectBuffer buf = new ExpandableArrayBuffer(128);
    final int written = src.snapshotTo(buf, 0);
    final AccountStore restored = new AccountStore();
    final int read = restored.restoreFrom(buf, 0);
    assertEquals(written, read);
    assertEquals(0, restored.size());
  }

  @Test
  void snapshotIsDeterministicAcrossInsertOrder() {
    final AccountStore a = new AccountStore();
    a.put(makeState(1L, "A1", "One", "USD"));
    a.put(makeState(2L, "A2", "Two", "EUR"));
    a.put(makeState(3L, "A3", "Three", "JPY"));

    final AccountStore b = new AccountStore();
    b.put(makeState(3L, "A3", "Three", "JPY"));
    b.put(makeState(1L, "A1", "One", "USD"));
    b.put(makeState(2L, "A2", "Two", "EUR"));

    final MutableDirectBuffer bufA = new ExpandableArrayBuffer(4096);
    final MutableDirectBuffer bufB = new ExpandableArrayBuffer(4096);
    final int wA = a.snapshotTo(bufA, 0);
    final int wB = b.snapshotTo(bufB, 0);
    assertEquals(wA, wB);

    final byte[] bytesA = new byte[wA];
    final byte[] bytesB = new byte[wB];
    bufA.getBytes(0, bytesA);
    bufB.getBytes(0, bytesB);
    assertArrayEquals(bytesA, bytesB);
  }
}
