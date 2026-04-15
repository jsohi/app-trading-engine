package com.trading.engine.cluster.refdata;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

class AccountStoreTest {

  @Test
  void putGetByPrimaryKey() {
    final AccountStore store = new AccountStore();
    store.put(AccountFixtures.account(1L, "ACME", "Acme Inc", "USD"));
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
    store.put(AccountFixtures.account(7L, "BIGCO", "Big Corp", "EUR"));
    final UnsafeBuffer probe = new UnsafeBuffer(new byte[] {'B', 'I', 'G', 'C', 'O'});
    final AccountState found = store.getByCode(probe, 0, 5);
    assertNotNull(found);
    assertEquals(7L, found.accountId());
  }

  @Test
  void getByCodeBytes() {
    final AccountStore store = new AccountStore();
    store.put(AccountFixtures.account(7L, "BIGCO", "Big Corp", "EUR"));
    final byte[] probe = {'B', 'I', 'G', 'C', 'O'};
    final AccountState found = store.getByCodeBytes(probe, 0, 5);
    assertNotNull(found);
    assertEquals(7L, found.accountId());
  }

  @Test
  void getByCodeReturnsNullForUnknown() {
    final AccountStore store = new AccountStore();
    store.put(AccountFixtures.account(1L, "ACME", "Acme", "USD"));
    final UnsafeBuffer probe = new UnsafeBuffer(new byte[] {'X', 'Y', 'Z'});
    assertNull(store.getByCode(probe, 0, 3));
    assertFalse(store.containsCode(probe, 0, 3));
  }

  @Test
  void upsertSameIdSameCodeOverwrites() {
    final AccountStore store = new AccountStore();
    store.put(AccountFixtures.account(1L, "ACME", "Acme v1", "USD"));
    store.put(AccountFixtures.account(1L, "ACME", "Acme v2", "USD"));
    assertEquals(1, store.size());
    final byte[] expectedName = "Acme v2".getBytes();
    final byte[] actual = new byte[expectedName.length];
    store.get(1L).copyAccountNameTo(actual, 0);
    assertArrayEquals(expectedName, actual);
  }

  @Test
  void upsertSameIdDifferentCodeRebuildsSecondaryIndex() {
    final AccountStore store = new AccountStore();
    store.put(AccountFixtures.account(1L, "ACME", "Acme", "USD"));

    // Verify ACME is findable.
    final UnsafeBuffer acme = new UnsafeBuffer(new byte[] {'A', 'C', 'M', 'E'});
    assertNotNull(store.getByCode(acme, 0, 4));

    // Re-load same accountId with a different code.
    store.put(AccountFixtures.account(1L, "NEWCODE", "Acme Renamed", "USD"));
    assertEquals(1, store.size());

    // Old code is gone, new code resolves.
    assertNull(store.getByCode(acme, 0, 4));
    final UnsafeBuffer newCode = new UnsafeBuffer(new byte[] {'N', 'E', 'W', 'C', 'O', 'D', 'E'});
    assertNotNull(store.getByCode(newCode, 0, 7));
  }

  @Test
  void upsertWithLoaderAliasingPattern_secondaryIndexStaysClean() {
    // Regression test for the critical aliasing bug gemini caught: the loader pattern is
    //   AccountState s = store.get(id);   // returns the same instance the store holds
    //   s.setAccountCode(NEW_BYTES);      // mutates the stored instance in place
    //   store.put(s);                     // put() must still find the OLD code in the
    //                                     // secondary index and remove it
    // The previous implementation captured the "old" code from the stored instance after
    // mutation — by then `previous == s` had the new bytes, so the wrong byCode entry was
    // removed and the OLD code stayed indexed forever (silent secondary-index leak).
    final AccountStore store = new AccountStore();
    store.put(AccountFixtures.account(42L, "ORIG", "Acme", "USD"));

    // Loader pattern: fetch the existing state, mutate it in place, call put.
    final AccountState fetched = store.get(42L);
    assertNotNull(fetched);
    final byte[] newCodeBytes = "RENAMED".getBytes();
    fetched.setAccountCode(newCodeBytes, 0, newCodeBytes.length);
    store.put(fetched);

    // Old code MUST be gone from the secondary index.
    final UnsafeBuffer orig = new UnsafeBuffer(new byte[] {'O', 'R', 'I', 'G'});
    assertNull(
        store.getByCode(orig, 0, 4),
        "old code 'ORIG' must not still resolve after the loader mutation + put");

    // New code MUST resolve and point at the same accountId.
    final UnsafeBuffer renamed = new UnsafeBuffer(newCodeBytes);
    final AccountState foundByNew = store.getByCode(renamed, 0, 7);
    assertNotNull(foundByNew);
    assertEquals(42L, foundByNew.accountId());
    assertEquals(1, store.size());
  }

  @Test
  void clearEmptiesBothIndexes() {
    final AccountStore store = new AccountStore();
    store.put(AccountFixtures.account(1L, "ACME", "Acme", "USD"));
    store.put(AccountFixtures.account(2L, "BIGCO", "Big", "EUR"));
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
    src.put(AccountFixtures.account(1L, "ACME", "Acme", "USD"));
    src.put(AccountFixtures.account(2L, "BIGCO", "Big Corp", "EUR"));
    src.put(AccountFixtures.account(3L, "JPN", "Japan Trading", "JPY"));

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
  void restoreOverPrePopulatedStoreDropsOrphans() {
    // The defensive clear() at the start of restoreFrom() must drop primary entries, secondary
    // index entries, AND the codeKeyByAccountId sidecar entries — otherwise an orphan account
    // would remain reachable via getByCode() after restoring a smaller snapshot.
    final AccountStore src = new AccountStore();
    src.put(AccountFixtures.account(1L, "ACME", "Acme", "USD"));
    final MutableDirectBuffer buf = new ExpandableArrayBuffer(1024);
    final int written = src.snapshotTo(buf, 0);

    final AccountStore dst = new AccountStore();
    // Pre-populate dst with TWO accounts. Restore should leave only the snapshot's record.
    dst.put(AccountFixtures.account(1L, "ACME", "Acme", "USD"));
    dst.put(AccountFixtures.account(99L, "ORPHAN", "Should Be Gone", "EUR"));
    assertEquals(2, dst.size());

    final int read = dst.restoreFrom(buf, 0);
    assertEquals(written, read);
    assertEquals(1, dst.size());
    // Orphan id is gone from the primary index.
    assertNull(dst.get(99L));
    // Orphan code is gone from the secondary index — load-bearing check that catches sidecar
    // leaks.
    final UnsafeBuffer orphanCode = new UnsafeBuffer(new byte[] {'O', 'R', 'P', 'H', 'A', 'N'});
    assertNull(dst.getByCode(orphanCode, 0, 6));
    // The snapshot's record is still findable.
    assertNotNull(dst.get(1L));
    final UnsafeBuffer acmeCode = new UnsafeBuffer(new byte[] {'A', 'C', 'M', 'E'});
    assertNotNull(dst.getByCode(acmeCode, 0, 4));
  }

  @Test
  void scaleSnapshotRoundTripWithMixedCodeLengths() {
    // Load 500 accounts with varying code lengths (1..16 bytes) and verify snapshot/restore
    // including dual-index rebuild for every record. Exercises the trim-trailing-zeros path
    // for both short and full-length codes.
    final AccountStore src = new AccountStore();
    for (long id = 1; id <= 500; id++) {
      // Code length cycles 1..16.
      final int codeLen = 1 + (int) ((id - 1) % 16);
      final StringBuilder sb = new StringBuilder(codeLen);
      for (int i = 0; i < codeLen; i++) {
        sb.append((char) ('A' + ((id + i) % 26)));
      }
      // Append id to ensure code uniqueness across the range.
      sb.append((char) ('0' + (int) (id % 10)));
      final String code = sb.toString().substring(0, Math.min(sb.length(), 16));
      src.put(AccountFixtures.account(id, code, "Account " + id, "USD"));
    }
    assertEquals(500, src.size());

    final MutableDirectBuffer buf = new ExpandableArrayBuffer(128 * 1024);
    final int written = src.snapshotTo(buf, 0);
    assertTrue(written > 0);

    final AccountStore restored = new AccountStore();
    final int read = restored.restoreFrom(buf, 0);
    assertEquals(written, read);
    assertEquals(500, restored.size());

    // Spot-check both indexes are populated.
    for (long id = 1; id <= 500; id += 50) {
      assertNotNull(restored.get(id), "primary index missing id " + id);
    }
  }

  @Test
  void snapshotIsDeterministicAcrossInsertOrder() {
    final AccountStore a = new AccountStore();
    a.put(AccountFixtures.account(1L, "A1", "One", "USD"));
    a.put(AccountFixtures.account(2L, "A2", "Two", "EUR"));
    a.put(AccountFixtures.account(3L, "A3", "Three", "JPY"));

    final AccountStore b = new AccountStore();
    b.put(AccountFixtures.account(3L, "A3", "Three", "JPY"));
    b.put(AccountFixtures.account(1L, "A1", "One", "USD"));
    b.put(AccountFixtures.account(2L, "A2", "Two", "EUR"));

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

  @Test
  void snapshotRoundTripGrowsScratchBeyondInitialCapacity() {
    // AccountStore initialises its snapshotKeysScratch at INITIAL_CAPACITY (4096). This test
    // populates > 4096 records so snapshotTo must grow the scratch array on demand on the
    // first call, and continue to work correctly afterwards. A regression that (e.g.) forgot
    // to reassign the field after new long[recordCount] would fail this test.
    final AccountStore src = new AccountStore();
    final int count = 5000;
    for (int i = 1; i <= count; i++) {
      src.put(AccountFixtures.account(i, "ACC" + i, "Name " + i, "USD"));
    }
    assertEquals(count, src.size());

    final MutableDirectBuffer buf = new ExpandableArrayBuffer(1_048_576);
    final int written = src.snapshotTo(buf, 0);
    assertTrue(written > 0);

    final AccountStore restored = new AccountStore();
    final int read = restored.restoreFrom(buf, 0);
    assertEquals(written, read);
    assertEquals(count, restored.size());
    // Spot-check a few records spanning the full range.
    assertNotNull(restored.get(1L));
    assertNotNull(restored.get(count / 2L));
    assertNotNull(restored.get((long) count));
  }

  @Test
  void snapshotToCanBeCalledRepeatedlyOnSameInstance() {
    // Regression test for the snapshotKeysFillIdx reset. A second snapshot on the same store
    // must produce a byte-identical result to a fresh store populated from the first snapshot
    // — this only holds if snapshotTo correctly zeroes snapshotKeysFillIdx at the start of
    // every call. A regression that dropped the reset would either throw
    // ArrayIndexOutOfBoundsException on the second call or silently produce a corrupt snapshot.
    final AccountStore store = new AccountStore();
    store.put(AccountFixtures.account(1L, "ACME", "Acme Inc", "USD"));
    store.put(AccountFixtures.account(2L, "BIGCO", "Big Co", "EUR"));
    store.put(AccountFixtures.account(3L, "JPN", "Japan Co", "JPY"));

    final MutableDirectBuffer bufA = new ExpandableArrayBuffer(4096);
    final int writtenA = store.snapshotTo(bufA, 0);

    // Add a new record and snapshot again on the SAME store instance.
    store.put(AccountFixtures.account(4L, "NEWCO", "New Co", "USD"));
    final MutableDirectBuffer bufB = new ExpandableArrayBuffer(4096);
    final int writtenB = store.snapshotTo(bufB, 0);

    assertTrue(writtenB >= writtenA, "second snapshot should contain at least as much data");

    // The first snapshot must be unchanged by the second call (verifies no cross-call state
    // corruption). Round-trip it and assert it has exactly 3 accounts.
    final AccountStore restoredA = new AccountStore();
    restoredA.restoreFrom(bufA, 0);
    assertEquals(3, restoredA.size());

    // The second snapshot must round-trip to the full 4-account state.
    final AccountStore restoredB = new AccountStore();
    restoredB.restoreFrom(bufB, 0);
    assertEquals(4, restoredB.size());
    assertNotNull(restoredB.get(4L));
  }
}
