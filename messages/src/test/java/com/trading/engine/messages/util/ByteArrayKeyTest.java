package com.trading.engine.messages.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import org.agrona.collections.Object2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/**
 * Consolidated unit tests for the shared {@link ByteArrayKey} utility. Covers both owned and probe
 * factory semantics, content-based equality and FNV-1a hashing, in-place mutation ({@code set},
 * {@code wrapForProbe}, {@code overwrite}, {@code setComposite}), Agrona map integration, accessor
 * methods ({@code backingArray}, {@code offset}, {@code getBytes}, {@code prefixEquals}), and error
 * paths.
 *
 * <p>This test class is the single source of truth for ByteArrayKey validation, consolidating tests
 * previously duplicated across the cluster, orchestrator, and projections modules (APP-161).
 *
 * <p>Test naming follows the {@code methodUnderTest_scenario_expectedBehavior} convention.
 */
class ByteArrayKeyTest {

  private static final byte[] ABC = "ABC".getBytes(StandardCharsets.US_ASCII);
  private static final byte[] DEF = "DEF".getBytes(StandardCharsets.US_ASCII);
  private static final byte[] ABCD = "ABCD".getBytes(StandardCharsets.US_ASCII);
  private static final byte[] ACME = {'A', 'C', 'M', 'E'};
  private static final byte[] BIGCO = {'B', 'I', 'G', 'C', 'O'};

  // ===========================================================================
  // Factory methods — owned
  // ===========================================================================

  @Test
  void owned_fromByteArray_createsDefensiveCopy() {
    final byte[] src = "XYZ".getBytes(StandardCharsets.US_ASCII);
    final ByteArrayKey key = ByteArrayKey.owned(src, 0, src.length);

    // Mutate source — key should be unaffected
    src[0] = (byte) 'A';
    assertEquals("XYZ", key.toString());
  }

  @Test
  void owned_fromByteArraySlice_createsCorrectKey() {
    final byte[] src = {'X', 'A', 'C', 'M', 'E', 'Y'};
    final ByteArrayKey key = ByteArrayKey.owned(src, 1, 4);
    assertEquals(4, key.length());
    assertEquals("ACME", key.toString());
  }

  @Test
  void copyOf_byteArray_createsIndependentCopy() {
    final byte[] src = "FOO".getBytes(StandardCharsets.US_ASCII);
    final ByteArrayKey key = ByteArrayKey.copyOf(src, 0, src.length);

    src[0] = (byte) 'B';
    assertEquals("FOO", key.toString()); // independent of source
  }

  @Test
  void copyOf_byteArraySlice_matchesOwned() {
    final byte[] src = {'X', 'A', 'C', 'M', 'E', 'Y'};
    final ByteArrayKey fromCopyOf = ByteArrayKey.copyOf(src, 1, 4);
    final ByteArrayKey fromOwned = ByteArrayKey.owned(ACME, 0, ACME.length);
    assertEquals(fromOwned, fromCopyOf);
  }

  @Test
  void copyOf_directBuffer_createsIndependentCopy() {
    final UnsafeBuffer buf = new UnsafeBuffer("BAR".getBytes(StandardCharsets.US_ASCII));
    final ByteArrayKey key = ByteArrayKey.copyOf(buf, 0, 3);

    buf.putByte(0, (byte) 'C');
    assertEquals("BAR", key.toString()); // independent of buffer
  }

  @Test
  void copyOf_directBufferSlice_matchesOwned() {
    final byte[] backing = {'X', 'A', 'C', 'M', 'E', 'Y'};
    final UnsafeBuffer buf = new UnsafeBuffer(backing);
    final ByteArrayKey key = ByteArrayKey.copyOf(buf, 1, 4);
    final ByteArrayKey direct = ByteArrayKey.owned(ACME, 0, ACME.length);
    assertEquals(direct, key);
  }

  @Test
  void copyOfWithCapacity_largerBacking_createsKeyWithExtraCapacity() {
    final ByteArrayKey key = ByteArrayKey.copyOfWithCapacity(ABC, 0, ABC.length, 16);
    assertEquals(3, key.length());
    assertEquals("ABC", key.toString());
  }

  @Test
  void copyOfWithCapacity_capacityLessThanLength_throwsIae() {
    assertThrows(
        IllegalArgumentException.class, () -> ByteArrayKey.copyOfWithCapacity(ABC, 0, 3, 2));
  }

  // ===========================================================================
  // Factory methods — probe
  // ===========================================================================

  @Test
  void emptyForLookup_createsZeroLengthKey() {
    final ByteArrayKey key = ByteArrayKey.emptyForLookup(20);
    assertEquals(0, key.length());
  }

  @Test
  void probe_fromByteArray_sharesBackingArray() {
    final byte[] src = "HELLO".getBytes(StandardCharsets.US_ASCII);
    final ByteArrayKey key = ByteArrayKey.probe(src, 0, src.length);
    assertEquals("HELLO", key.toString());

    // Mutate source — key IS affected (shared backing)
    src[0] = (byte) 'J';
    assertEquals("JELLO", key.toString());
  }

  // ===========================================================================
  // Equality
  // ===========================================================================

  @Test
  void equals_sameContent_returnsTrue() {
    final ByteArrayKey a = ByteArrayKey.owned(ACME, 0, ACME.length);
    final ByteArrayKey b = ByteArrayKey.owned(ACME, 0, ACME.length);
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
  }

  @Test
  void equals_differentContent_returnsFalse() {
    final ByteArrayKey a = ByteArrayKey.owned(ABC, 0, ABC.length);
    final ByteArrayKey b = ByteArrayKey.owned(DEF, 0, DEF.length);
    assertNotEquals(a, b);
  }

  @Test
  void equals_differentLength_returnsFalse() {
    final ByteArrayKey a = ByteArrayKey.owned(ABC, 0, ABC.length);
    final ByteArrayKey b = ByteArrayKey.owned(ABCD, 0, ABCD.length);
    assertNotEquals(a, b);
  }

  @Test
  void equals_null_returnsFalse() {
    final ByteArrayKey key = ByteArrayKey.owned(ABC, 0, ABC.length);
    assertFalse(key.equals(null));
  }

  @Test
  void equals_wrongType_returnsFalse() {
    final ByteArrayKey key = ByteArrayKey.owned(ABC, 0, ABC.length);
    assertFalse(key.equals("ABC"));
  }

  @Test
  void equals_reflexive_returnsTrue() {
    final ByteArrayKey key = ByteArrayKey.owned(ABC, 0, ABC.length);
    assertEquals(key, key);
  }

  @Test
  void equals_emptyKeys_returnsTrue() {
    final ByteArrayKey a = ByteArrayKey.owned(new byte[0], 0, 0);
    final ByteArrayKey b = ByteArrayKey.owned(new byte[0], 0, 0);
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertEquals(0, a.length());
  }

  @Test
  void equals_nullPaddedContent_matchesIdenticalPadding() {
    final byte[] padded1 = new byte[] {'A', 'B', 0, 0, 0};
    final byte[] padded2 = new byte[] {'A', 'B', 0, 0, 0};
    final ByteArrayKey a = ByteArrayKey.owned(padded1, 0, 5);
    final ByteArrayKey b = ByteArrayKey.owned(padded2, 0, 5);
    assertEquals(a, b);
  }

  // ===========================================================================
  // Hashing
  // ===========================================================================

  @Test
  void hashCode_sameContent_returnsSameHash() {
    final ByteArrayKey a = ByteArrayKey.owned(ACME, 0, ACME.length);
    final ByteArrayKey b = ByteArrayKey.owned(ACME, 0, ACME.length);
    assertEquals(a.hashCode(), b.hashCode());
  }

  @Test
  void hashCode_differentContent_returnsDifferentHash() {
    final ByteArrayKey a = ByteArrayKey.owned(ACME, 0, ACME.length);
    final ByteArrayKey b = ByteArrayKey.owned(BIGCO, 0, BIGCO.length);
    // Verified: FNV-1a produces distinct hashes for these specific inputs (deterministic, no
    // flake).
    assertNotEquals(a.hashCode(), b.hashCode());
  }

  // ===========================================================================
  // toString
  // ===========================================================================

  @Test
  void toString_returnsAsciiRepresentation() {
    final ByteArrayKey key = ByteArrayKey.owned("EURUSD".getBytes(StandardCharsets.US_ASCII), 0, 6);
    assertEquals("EURUSD", key.toString());
  }

  // ===========================================================================
  // length accessor
  // ===========================================================================

  @Test
  void length_returnsKeyLength() {
    assertEquals(3, ByteArrayKey.owned(ABC, 0, ABC.length).length());
    assertEquals(4, ByteArrayKey.owned(ABCD, 0, ABCD.length).length());
    assertEquals(0, ByteArrayKey.emptyForLookup(10).length());
  }

  // ===========================================================================
  // set — DirectBuffer and byte[]
  // ===========================================================================

  @Test
  void set_directBuffer_populatesProbe() {
    final ByteArrayKey probe = ByteArrayKey.emptyForLookup(16);
    assertEquals(0, probe.length());

    final UnsafeBuffer src1 = new UnsafeBuffer(ACME);
    probe.set(src1, 0, 4);
    assertEquals(4, probe.length());

    final ByteArrayKey acme = ByteArrayKey.owned(ACME, 0, ACME.length);
    assertEquals(acme, probe);
    assertEquals(acme.hashCode(), probe.hashCode());

    // Reuse probe for a different key
    final UnsafeBuffer src2 = new UnsafeBuffer(BIGCO);
    probe.set(src2, 0, 5);
    assertEquals(5, probe.length());

    final ByteArrayKey bigco = ByteArrayKey.owned(BIGCO, 0, BIGCO.length);
    assertEquals(bigco, probe);
    assertNotEquals(acme, probe);
  }

  @Test
  void set_directBuffer_exceedsCapacity_throwsIoobe() {
    final ByteArrayKey probe = ByteArrayKey.emptyForLookup(4);
    final UnsafeBuffer src = new UnsafeBuffer(new byte[] {'A', 'B', 'C', 'D', 'E'});
    assertThrows(IndexOutOfBoundsException.class, () -> probe.set(src, 0, 5));
  }

  @Test
  void set_byteArray_matchesDirectBufferSet() {
    final ByteArrayKey probe1 = ByteArrayKey.emptyForLookup(16);
    probe1.set(ACME, 0, ACME.length);

    final ByteArrayKey probe2 = ByteArrayKey.emptyForLookup(16);
    final UnsafeBuffer buf = new UnsafeBuffer(ACME);
    probe2.set(buf, 0, ACME.length);

    assertEquals(probe1, probe2);
    assertEquals(probe1.hashCode(), probe2.hashCode());
  }

  @Test
  void set_byteArray_reusesProbeObjectIdentity() {
    final ByteArrayKey probe = ByteArrayKey.emptyForLookup(20);
    final ByteArrayKey ref = probe;

    probe.set("HELLO".getBytes(StandardCharsets.US_ASCII), 0, 5);
    assertSame(ref, probe); // Same object identity -- no allocation
    assertEquals("HELLO", probe.toString());

    probe.set("WORLD".getBytes(StandardCharsets.US_ASCII), 0, 5);
    assertSame(ref, probe);
    assertEquals("WORLD", probe.toString());
  }

  // ===========================================================================
  // wrapForProbe — byte[] and DirectBuffer
  // ===========================================================================

  @Test
  void wrapForProbe_byteArray_updatesWithoutCopy() {
    final ByteArrayKey key = ByteArrayKey.emptyForLookup(10);
    assertEquals(0, key.length());

    key.wrapForProbe(ABC, 0, ABC.length);

    assertEquals("ABC", key.toString());
    assertEquals(3, key.length());
  }

  @Test
  void wrapForProbe_directBuffer_updatesWithCopy() {
    final ByteArrayKey key = ByteArrayKey.emptyForLookup(10);
    final UnsafeBuffer buf = new UnsafeBuffer("HELLO".getBytes(StandardCharsets.US_ASCII));

    key.wrapForProbe(buf, 0, 5);

    assertEquals("HELLO", key.toString());
    assertEquals(5, key.length());
  }

  @Test
  void wrapForProbe_directBuffer_exceedsCapacity_throwsIoobe() {
    final ByteArrayKey key = ByteArrayKey.emptyForLookup(3);
    final UnsafeBuffer buf = new UnsafeBuffer("TOOLONG".getBytes(StandardCharsets.US_ASCII));

    assertThrows(IndexOutOfBoundsException.class, () -> key.wrapForProbe(buf, 0, 7));
  }

  // ===========================================================================
  // overwrite — DirectBuffer and byte[]
  // ===========================================================================

  @Test
  void overwrite_directBuffer_updatesContentAndHash() {
    final ByteArrayKey key = ByteArrayKey.owned(ABC, 0, ABC.length);
    final int oldHash = key.hashCode();

    final UnsafeBuffer newBuf = new UnsafeBuffer(DEF);
    key.overwrite(newBuf, 0, DEF.length);

    assertEquals("DEF", key.toString());
    assertNotEquals(oldHash, key.hashCode());
  }

  @Test
  void overwrite_byteArray_copiesBytesAndRecomputesHash() {
    final ByteArrayKey key = ByteArrayKey.owned(ABC, 0, ABC.length);
    final int oldHash = key.hashCode();

    key.overwrite(ACME, 0, ACME.length);

    assertEquals("ACME", key.toString());
    assertEquals(4, key.length());
    assertNotEquals(oldHash, key.hashCode());
    // Verify consistency with a freshly constructed key
    assertEquals(ByteArrayKey.owned(ACME, 0, ACME.length), key);
  }

  @Test
  void overwrite_byteArray_shorterThanBacking_updatesLength() {
    // Start with a 5-byte key backed by a capacity-16 array
    final ByteArrayKey key = ByteArrayKey.copyOfWithCapacity(BIGCO, 0, BIGCO.length, 16);
    assertEquals(5, key.length());

    // Overwrite with a shorter value
    key.overwrite(ABC, 0, ABC.length);
    assertEquals(3, key.length());
    assertEquals("ABC", key.toString());

    // Key must still be usable as an equal match
    final ByteArrayKey expected = ByteArrayKey.owned(ABC, 0, ABC.length);
    assertEquals(expected, key);
    assertEquals(expected.hashCode(), key.hashCode());
  }

  // ===========================================================================
  // setComposite
  // ===========================================================================

  @Test
  void setComposite_packsTwoArrays() {
    final byte[] account = "ACME".getBytes(StandardCharsets.US_ASCII);
    final byte[] settlDate = "20260412".getBytes(StandardCharsets.US_ASCII);

    final ByteArrayKey probe = ByteArrayKey.emptyForLookup(24);
    probe.setComposite(account, 0, account.length, settlDate, 0, settlDate.length);

    assertEquals(account.length + settlDate.length, probe.length());
    assertEquals("ACME20260412", probe.toString());

    // Verify bytes
    final byte[] dst = new byte[12];
    probe.getBytes(dst, 0, 0, 12);
    assertArrayEquals("ACME20260412".getBytes(StandardCharsets.US_ASCII), dst);
  }

  @Test
  void setComposite_equality() {
    final byte[] account = "ACME".getBytes(StandardCharsets.US_ASCII);
    final byte[] settlDate = "20260412".getBytes(StandardCharsets.US_ASCII);

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

  // ===========================================================================
  // prefixEquals
  // ===========================================================================

  @Test
  void prefixEquals_matchesPrefix() {
    final byte[] account = "ACME".getBytes(StandardCharsets.US_ASCII);
    final byte[] settlDate = "20260412".getBytes(StandardCharsets.US_ASCII);

    final ByteArrayKey key = ByteArrayKey.emptyForLookup(24);
    key.setComposite(account, 0, account.length, settlDate, 0, settlDate.length);

    assertTrue(key.prefixEquals(account, 0, account.length));
    assertFalse(key.prefixEquals("BETA".getBytes(StandardCharsets.US_ASCII), 0, 4));
    // Prefix longer than key
    assertFalse(key.prefixEquals("ACME20260412EXTRA".getBytes(StandardCharsets.US_ASCII), 0, 17));
  }

  // ===========================================================================
  // getBytes
  // ===========================================================================

  @Test
  void getBytes_extractsSubrange() {
    final ByteArrayKey key =
        ByteArrayKey.copyOf("HELLO_WORLD".getBytes(StandardCharsets.US_ASCII), 0, 11);
    final byte[] dst = new byte[5];
    key.getBytes(dst, 0, 6, 5);
    assertArrayEquals("WORLD".getBytes(StandardCharsets.US_ASCII), dst);
  }

  // ===========================================================================
  // backingArray and offset accessors
  // ===========================================================================

  @Test
  void backingArray_returnsInternalData() {
    final ByteArrayKey key = ByteArrayKey.owned(ACME, 0, ACME.length);

    final byte[] backing = key.backingArray();

    // Backing should contain the key bytes at offset 0 with key.length() bytes
    assertEquals(ACME.length, key.length());
    for (int i = 0; i < ACME.length; i++) {
      assertEquals(ACME[i], backing[key.offset() + i]);
    }
  }

  @Test
  void offset_ownedKey_alwaysZero() {
    // owned, copyOf, and copyOfWithCapacity all produce owned keys with offset = 0
    assertEquals(0, ByteArrayKey.owned(ACME, 0, ACME.length).offset());
    assertEquals(0, ByteArrayKey.copyOf(ABC, 0, ABC.length).offset());
    assertEquals(0, ByteArrayKey.copyOfWithCapacity(DEF, 0, DEF.length, 16).offset());
  }

  @Test
  void offset_probeKey_reflectsWrappedRange() {
    // probe wraps an external array by reference, preserving the provided offset
    final byte[] src = {'_', '_', 'A', 'B', 'C', '_'};
    final ByteArrayKey probeKey = ByteArrayKey.probe(src, 2, 3);

    assertEquals(2, probeKey.offset());
    assertEquals(3, probeKey.length());
    assertEquals("ABC", probeKey.toString());

    // After wrapForProbe(byte[]) with a different offset, offset updates
    final byte[] src2 = {'Z', 'Z', 'Z', 'D', 'E', 'F'};
    probeKey.wrapForProbe(src2, 3, 3);
    assertEquals(3, probeKey.offset());
    assertEquals("DEF", probeKey.toString());
  }

  // ===========================================================================
  // copyOf instance method (defensive copy)
  // ===========================================================================

  @Test
  void copyOf_instanceMethod_createsIndependentCopy() {
    final ByteArrayKey original =
        ByteArrayKey.owned("EURUSD".getBytes(StandardCharsets.US_ASCII), 0, 6);
    final ByteArrayKey copy = original.copyOf();

    assertNotSame(original, copy);
    assertEquals(original, copy);
    assertEquals(original.hashCode(), copy.hashCode());
  }

  // ===========================================================================
  // Agrona map integration
  // ===========================================================================

  @Test
  void ownedKey_insertableIntoAgronaMap() {
    final Object2ObjectHashMap<ByteArrayKey, String> map = new Object2ObjectHashMap<>(4, 0.65f);
    final ByteArrayKey key = ByteArrayKey.owned(ABC, 0, ABC.length);

    map.put(key, "value");

    assertEquals("value", map.get(key));
    // Also findable via a probe with the same content
    final ByteArrayKey probe = ByteArrayKey.probe(ABC, 0, ABC.length);
    assertEquals("value", map.get(probe));
  }

  @Test
  void probeKey_looksUpCorrectValues() {
    final Object2ObjectHashMap<ByteArrayKey, String> map = new Object2ObjectHashMap<>();
    map.put(ByteArrayKey.owned(ACME, 0, ACME.length), "one");
    map.put(ByteArrayKey.owned(BIGCO, 0, BIGCO.length), "two");

    // Reusable probe should look up the right values
    final ByteArrayKey probe = ByteArrayKey.emptyForLookup(16);
    final UnsafeBuffer acmeBuf = new UnsafeBuffer(ACME);
    probe.set(acmeBuf, 0, 4);
    assertEquals("one", map.get(probe));

    final UnsafeBuffer bigcoBuf = new UnsafeBuffer(BIGCO);
    probe.set(bigcoBuf, 0, 5);
    assertEquals("two", map.get(probe));

    // A code that isn't there returns null
    final UnsafeBuffer xyz = new UnsafeBuffer(new byte[] {'X', 'Y', 'Z'});
    probe.set(xyz, 0, 3);
    assertNull(map.get(probe));
  }

  @Test
  void probeKey_lookupWithByteArraySet_matchesCopiedKey() {
    final Object2ObjectHashMap<ByteArrayKey, String> map = new Object2ObjectHashMap<>();

    // Insert with copyOf
    final byte[] data = "ORD-001".getBytes(StandardCharsets.US_ASCII);
    final ByteArrayKey insertKey = ByteArrayKey.copyOf(data, 0, data.length);
    map.put(insertKey, "found");

    // Lookup with probe set from byte[]
    final ByteArrayKey probe = ByteArrayKey.emptyForLookup(20);
    probe.set(data, 0, data.length);
    assertEquals("found", map.get(probe));
  }

  /**
   * Demonstrates why {@link ByteArrayKey} is preferred over {@link UnsafeBuffer} for secondary
   * indices: ByteArrayKey takes a defensive copy on insert, decoupling the map's keys from the SBE
   * message buffer lifecycle. An UnsafeBuffer key would be silently corrupted when the source
   * buffer is reused for the next command.
   */
  @Test
  void ownedKey_decouplesMapFromSourceBufferLifecycle() {
    // Simulate a reusable SBE message buffer
    final byte[] reusableSource = new byte[] {'A', 'C', 'M', 'E'};

    // Insertion path: defensive copy. The map key is independent of the source.
    final Object2ObjectHashMap<ByteArrayKey, String> map = new Object2ObjectHashMap<>();
    map.put(ByteArrayKey.copyOf(reusableSource, 0, 4), "acme");

    // Reuse the source buffer for a different account code
    reusableSource[0] = 'B';
    reusableSource[1] = 'I';
    reusableSource[2] = 'G';
    reusableSource[3] = 'X';

    // The original map entry is still findable -- the inserted key was a copy, not a view
    assertEquals("acme", map.get(ByteArrayKey.owned(ACME, 0, ACME.length)));
    // And the new bytes don't accidentally find the old entry
    assertNull(map.get(ByteArrayKey.owned(reusableSource, 0, reusableSource.length)));
  }

  // ===========================================================================
  // Additional edge cases (review round 1 findings)
  // ===========================================================================

  @Test
  void set_byteArray_exceedsCapacity_throwsIoobe() {
    final ByteArrayKey probe = ByteArrayKey.emptyForLookup(4);
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> probe.set(new byte[] {'A', 'B', 'C', 'D', 'E'}, 0, 5));
  }

  @Test
  void overwrite_byteArray_longerThanBacking_allocatesAndCopies() {
    final ByteArrayKey key = ByteArrayKey.owned(ABC, 0, ABC.length); // byte[3]
    final byte[] longer = "LONGER_VALUE".getBytes(StandardCharsets.US_ASCII);
    key.overwrite(longer, 0, longer.length);
    assertEquals("LONGER_VALUE", key.toString());
    assertEquals(12, key.length());
  }

  @Test
  void copyOf_instanceMethod_probeWithOffset_createsCorrectCopy() {
    final byte[] src = {'_', '_', 'A', 'B', 'C', '_'};
    final ByteArrayKey probe = ByteArrayKey.probe(src, 2, 3);
    final ByteArrayKey copy = probe.copyOf();

    assertNotSame(probe, copy);
    assertEquals(probe, copy);
    assertEquals(0, copy.offset());
    assertEquals("ABC", copy.toString());
  }

  @Test
  void prefixEquals_zeroLengthPrefix_returnsTrue() {
    final ByteArrayKey key = ByteArrayKey.owned(ABC, 0, ABC.length);
    assertTrue(key.prefixEquals(new byte[0], 0, 0));
  }

  @Test
  void prefixEquals_exactMatch_returnsTrue() {
    final ByteArrayKey key = ByteArrayKey.owned(ABC, 0, ABC.length);
    assertTrue(key.prefixEquals(ABC, 0, ABC.length));
  }

  @Test
  void hashCode_emptyKey_returnsFnvOffsetBasis() {
    final ByteArrayKey empty = ByteArrayKey.owned(new byte[0], 0, 0);
    assertEquals(0x811C9DC5, empty.hashCode());
  }

  @Test
  void setComposite_exceedsCapacity_throwsIoobe() {
    final ByteArrayKey probe = ByteArrayKey.emptyForLookup(4);
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> probe.setComposite(ABC, 0, ABC.length, ABC, 0, ABC.length)); // 6 > 4
  }
}
