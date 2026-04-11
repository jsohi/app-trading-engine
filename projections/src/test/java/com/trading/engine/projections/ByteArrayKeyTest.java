package com.trading.engine.projections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.agrona.collections.Object2ObjectHashMap;
import org.junit.jupiter.api.Test;

class ByteArrayKeyTest {

  @Test
  void equalKeysHaveSameHashCode() {
    final byte[] data = "EURUSD".getBytes();
    final ByteArrayKey a = ByteArrayKey.copyOf(data, 0, data.length);
    final ByteArrayKey b = ByteArrayKey.copyOf(data, 0, data.length);
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
  }

  @Test
  void differentKeysNotEqual() {
    final ByteArrayKey a = ByteArrayKey.copyOf("EURUSD".getBytes(), 0, 6);
    final ByteArrayKey b = ByteArrayKey.copyOf("USDJPY".getBytes(), 0, 6);
    assertNotEquals(a, b);
  }

  @Test
  void copyOfCreatesIndependentCopy() {
    final byte[] src = "EURUSD".getBytes();
    final ByteArrayKey key = ByteArrayKey.copyOf(src, 0, src.length);
    // Modify source — key should be unaffected
    src[0] = 'X';
    assertEquals("EURUSD", key.toString());
  }

  @Test
  void probeKeyLookupMatchesCopiedKey() {
    final Object2ObjectHashMap<ByteArrayKey, String> map = new Object2ObjectHashMap<>();

    // Insert with copyOf
    final byte[] data = "ORD-001".getBytes();
    final ByteArrayKey insertKey = ByteArrayKey.copyOf(data, 0, data.length);
    map.put(insertKey, "found");

    // Lookup with probe
    final ByteArrayKey probe = ByteArrayKey.emptyForLookup(20);
    probe.set(data, 0, data.length);
    assertEquals("found", map.get(probe));
  }

  @Test
  void emptyForLookupDoesNotAllocateOnSet() {
    final ByteArrayKey probe = ByteArrayKey.emptyForLookup(20);
    final ByteArrayKey ref = probe;

    probe.set("HELLO".getBytes(), 0, 5);
    assertSame(ref, probe); // Same object identity — no allocation
    assertEquals("HELLO", probe.toString());

    probe.set("WORLD".getBytes(), 0, 5);
    assertSame(ref, probe);
    assertEquals("WORLD", probe.toString());
  }

  @Test
  void compositeKeyPacksTwoArrays() {
    final byte[] account = "ACME".getBytes();
    final byte[] settlDate = "20260412".getBytes();

    final ByteArrayKey probe = ByteArrayKey.emptyForLookup(24);
    probe.setComposite(account, 0, account.length, settlDate, 0, settlDate.length);

    assertEquals(account.length + settlDate.length, probe.length());
    assertEquals("ACME20260412", probe.toString());

    // Verify bytes
    final byte[] dst = new byte[12];
    probe.getBytes(dst, 0, 0, 12);
    assertArrayEquals("ACME20260412".getBytes(), dst);
  }

  @Test
  void compositeKeyEquality() {
    final byte[] account = "ACME".getBytes();
    final byte[] settlDate = "20260412".getBytes();

    final ByteArrayKey a = ByteArrayKey.emptyForLookup(24);
    a.setComposite(account, 0, account.length, settlDate, 0, settlDate.length);

    final ByteArrayKey b = ByteArrayKey.emptyForLookup(24);
    b.setComposite(account, 0, account.length, settlDate, 0, settlDate.length);

    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());

    // copyOf should also be equal
    final ByteArrayKey copy = a.copyOf();
    assertNotSame(a, copy);
    assertEquals(a, copy);
  }

  @Test
  void prefixEqualsMatchesPrefix() {
    final byte[] account = "ACME".getBytes();
    final byte[] settlDate = "20260412".getBytes();

    final ByteArrayKey key = ByteArrayKey.emptyForLookup(24);
    key.setComposite(account, 0, account.length, settlDate, 0, settlDate.length);

    assertTrue(key.prefixEquals(account, 0, account.length));
    assertFalse(key.prefixEquals("BETA".getBytes(), 0, 4));
    // Prefix longer than key
    assertFalse(key.prefixEquals("ACME20260412EXTRA".getBytes(), 0, 17));
  }

  @Test
  void getBytesExtractsSubrange() {
    final ByteArrayKey key = ByteArrayKey.copyOf("HELLO_WORLD".getBytes(), 0, 11);
    final byte[] dst = new byte[5];
    key.getBytes(dst, 0, 6, 5);
    assertArrayEquals("WORLD".getBytes(), dst);
  }

  @Test
  void immutableKeyThrowsOnSet() {
    final ByteArrayKey key = ByteArrayKey.copyOf("EURUSD".getBytes(), 0, 6);
    org.junit.jupiter.api.Assertions.assertThrows(
        UnsupportedOperationException.class, () -> key.set("USDJPY".getBytes(), 0, 6));
  }

  @Test
  void immutableKeyThrowsOnSetComposite() {
    final ByteArrayKey key = ByteArrayKey.copyOf("ACME".getBytes(), 0, 4);
    org.junit.jupiter.api.Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> key.setComposite("ACME".getBytes(), 0, 4, "20260412".getBytes(), 0, 8));
  }
}
