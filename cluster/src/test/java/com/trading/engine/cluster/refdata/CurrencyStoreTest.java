package com.trading.engine.cluster.refdata;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.CurrencyClassEnum;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

class CurrencyStoreTest {

  // ---------------------------------------------------------------------------
  // packCode
  // ---------------------------------------------------------------------------

  @Test
  void packCodeProducesUniqueIntPerCode() {
    final int usd = CurrencyStore.packCode((byte) 'U', (byte) 'S', (byte) 'D');
    final int eur = CurrencyStore.packCode((byte) 'E', (byte) 'U', (byte) 'R');
    final int jpy = CurrencyStore.packCode((byte) 'J', (byte) 'P', (byte) 'Y');
    assertEquals(0x555344, usd);
    assertEquals(0x455552, eur);
    assertEquals(0x4A5059, jpy);
  }

  @Test
  void packCodeFromDirectBuffer() {
    final UnsafeBuffer buf = new UnsafeBuffer(new byte[] {'U', 'S', 'D'});
    assertEquals(0x555344, CurrencyStore.packCode(buf, 0));
  }

  @Test
  void packCodeRejectsLowercase() {
    assertThrows(
        IllegalArgumentException.class,
        () -> CurrencyStore.packCode((byte) 'u', (byte) 's', (byte) 'd'));
  }

  @Test
  void packCodeRejectsDigits() {
    assertThrows(
        IllegalArgumentException.class,
        () -> CurrencyStore.packCode((byte) 'U', (byte) 'S', (byte) '1'));
  }

  @Test
  void packCodeRejectsControlBytes() {
    assertThrows(
        IllegalArgumentException.class,
        () -> CurrencyStore.packCode((byte) 0, (byte) 'S', (byte) 'D'));
  }

  // ---------------------------------------------------------------------------
  // CRUD
  // ---------------------------------------------------------------------------

  private static CurrencyState makeState(final String code, final int isoNum, final int decimals) {
    final CurrencyState s = new CurrencyState();
    final byte[] bytes = code.getBytes();
    s.setCcyCode(bytes, 0);
    s.setIsoNumeric(isoNum);
    final byte[] name = ("Name " + code).getBytes();
    s.setName(name, 0, name.length);
    s.setDecimals(decimals);
    s.setCurrencyClass(CurrencyClassEnum.Fiat);
    s.setStatus(AccountStatusEnum.Active);
    s.setTransactTime(0L);
    return s;
  }

  @Test
  void putGet() {
    final CurrencyStore store = new CurrencyStore();
    assertEquals(0, store.size());
    store.put(CurrencyStore.packCode((byte) 'U', (byte) 'S', (byte) 'D'), makeState("USD", 840, 2));
    assertEquals(1, store.size());

    final CurrencyState found =
        store.get(CurrencyStore.packCode((byte) 'U', (byte) 'S', (byte) 'D'));
    assertNotNull(found);
    assertEquals(840, found.isoNumeric());
    assertEquals(2, found.decimals());
  }

  @Test
  void getByCodeFromDirectBuffer() {
    final CurrencyStore store = new CurrencyStore();
    store.put(CurrencyStore.packCode((byte) 'E', (byte) 'U', (byte) 'R'), makeState("EUR", 978, 2));
    final UnsafeBuffer probe = new UnsafeBuffer(new byte[] {'E', 'U', 'R'});
    assertNotNull(store.getByCode(probe, 0));
    assertEquals(978, store.getByCode(probe, 0).isoNumeric());
  }

  @Test
  void getReturnsNullForUnknown() {
    final CurrencyStore store = new CurrencyStore();
    assertNull(store.get(CurrencyStore.packCode((byte) 'X', (byte) 'X', (byte) 'X')));
    assertFalse(store.contains(CurrencyStore.packCode((byte) 'X', (byte) 'X', (byte) 'X')));
  }

  @Test
  void upsertOverwrites() {
    final CurrencyStore store = new CurrencyStore();
    final int key = CurrencyStore.packCode((byte) 'J', (byte) 'P', (byte) 'Y');
    store.put(key, makeState("JPY", 392, 0));
    store.put(key, makeState("JPY", 392, 2)); // re-load with different decimals
    assertEquals(1, store.size());
    assertEquals(2, store.get(key).decimals());
  }

  @Test
  void clearEmpties() {
    final CurrencyStore store = new CurrencyStore();
    store.put(CurrencyStore.packCode((byte) 'U', (byte) 'S', (byte) 'D'), makeState("USD", 840, 2));
    store.put(CurrencyStore.packCode((byte) 'E', (byte) 'U', (byte) 'R'), makeState("EUR", 978, 2));
    store.clear();
    assertEquals(0, store.size());
  }

  // ---------------------------------------------------------------------------
  // Snapshot round-trip
  // ---------------------------------------------------------------------------

  @Test
  void snapshotRoundTripPopulated() {
    final CurrencyStore src = new CurrencyStore();
    src.put(CurrencyStore.packCode((byte) 'U', (byte) 'S', (byte) 'D'), makeState("USD", 840, 2));
    src.put(CurrencyStore.packCode((byte) 'E', (byte) 'U', (byte) 'R'), makeState("EUR", 978, 2));
    src.put(CurrencyStore.packCode((byte) 'J', (byte) 'P', (byte) 'Y'), makeState("JPY", 392, 0));

    final MutableDirectBuffer buf = new ExpandableArrayBuffer(4096);
    final int written = src.snapshotTo(buf, 0);
    assertTrue(written > 0);

    final CurrencyStore restored = new CurrencyStore();
    final int read = restored.restoreFrom(buf, 0);
    assertEquals(written, read);
    assertEquals(3, restored.size());

    final CurrencyState usd =
        restored.get(CurrencyStore.packCode((byte) 'U', (byte) 'S', (byte) 'D'));
    assertNotNull(usd);
    assertEquals(840, usd.isoNumeric());
    assertEquals(2, usd.decimals());
    assertEquals(CurrencyClassEnum.Fiat, usd.currencyClass());
    assertEquals(AccountStatusEnum.Active, usd.status());

    // The name field round-trips with trailing-zero trimming.
    final byte[] nameBytes = new byte[64];
    usd.copyNameTo(nameBytes, 0);
    final byte[] expected = "Name USD".getBytes();
    final byte[] actual = new byte[expected.length];
    System.arraycopy(nameBytes, 0, actual, 0, expected.length);
    assertArrayEquals(expected, actual);
  }

  @Test
  void snapshotRoundTripEmpty() {
    final CurrencyStore src = new CurrencyStore();
    final MutableDirectBuffer buf = new ExpandableArrayBuffer(128);
    final int written = src.snapshotTo(buf, 0);
    final CurrencyStore restored = new CurrencyStore();
    final int read = restored.restoreFrom(buf, 0);
    assertEquals(written, read);
    assertEquals(0, restored.size());
  }

  @Test
  void snapshotIsDeterministicAcrossInsertOrder() {
    // Insert in different orders, snapshot, compare bytes — must be byte-identical.
    final CurrencyStore a = new CurrencyStore();
    a.put(CurrencyStore.packCode((byte) 'U', (byte) 'S', (byte) 'D'), makeState("USD", 840, 2));
    a.put(CurrencyStore.packCode((byte) 'E', (byte) 'U', (byte) 'R'), makeState("EUR", 978, 2));
    a.put(CurrencyStore.packCode((byte) 'J', (byte) 'P', (byte) 'Y'), makeState("JPY", 392, 0));

    final CurrencyStore b = new CurrencyStore();
    b.put(CurrencyStore.packCode((byte) 'J', (byte) 'P', (byte) 'Y'), makeState("JPY", 392, 0));
    b.put(CurrencyStore.packCode((byte) 'U', (byte) 'S', (byte) 'D'), makeState("USD", 840, 2));
    b.put(CurrencyStore.packCode((byte) 'E', (byte) 'U', (byte) 'R'), makeState("EUR", 978, 2));

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
}
