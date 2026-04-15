package com.trading.engine.orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import org.agrona.collections.Object2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ByteArrayKey}. Covers owned/probe/emptyForLookup factory semantics,
 * content-based equality and FNV-1a hashing, in-place mutation (overwrite, wrapForProbe, set),
 * Agrona map insertion, and error paths. ~8 tests mirror the cluster's ByteArrayKeyTest for shared
 * logic; the remaining tests cover orchestrator-specific API additions (overwrite, wrapForProbe
 * with DirectBuffer, probe factory, set delegation).
 */
class ByteArrayKeyTest {

  private static final byte[] ABC = "ABC".getBytes(StandardCharsets.US_ASCII);
  private static final byte[] DEF = "DEF".getBytes(StandardCharsets.US_ASCII);
  private static final byte[] ABCD = "ABCD".getBytes(StandardCharsets.US_ASCII);

  // ===========================================================================
  // Factory methods
  // ===========================================================================

  @Test
  void owned_fromByteArray_createsDefensiveCopy() {
    final byte[] src = "XYZ".getBytes(StandardCharsets.US_ASCII);
    final var key = ByteArrayKey.owned(src, 0, src.length);

    // Mutate source — key should be unaffected
    src[0] = (byte) 'A';
    assertEquals("XYZ", key.toString());
  }

  @Test
  void emptyForLookup_createsZeroedBackingArray() {
    final var key = ByteArrayKey.emptyForLookup(20);
    assertEquals(0, key.length());
  }

  @Test
  void probe_fromByteArray_sharesBackingArray() {
    final byte[] src = "HELLO".getBytes(StandardCharsets.US_ASCII);
    final var key = ByteArrayKey.probe(src, 0, src.length);
    assertEquals("HELLO", key.toString());

    // Mutate source — key IS affected (shared backing)
    src[0] = (byte) 'J';
    assertEquals("JELLO", key.toString());
  }

  @Test
  void copyOf_byteArray_createsIndependentCopy() {
    final byte[] src = "FOO".getBytes(StandardCharsets.US_ASCII);
    final var key = ByteArrayKey.copyOf(src, 0, src.length);

    src[0] = (byte) 'B';
    assertEquals("FOO", key.toString()); // independent of source
  }

  @Test
  void copyOf_directBuffer_createsIndependentCopy() {
    final var buf = new UnsafeBuffer("BAR".getBytes(StandardCharsets.US_ASCII));
    final var key = ByteArrayKey.copyOf(buf, 0, 3);

    buf.putByte(0, (byte) 'C');
    assertEquals("BAR", key.toString()); // independent of buffer
  }

  // ===========================================================================
  // Equality
  // ===========================================================================

  @Test
  void equals_sameContent_returnsTrue() {
    final var key1 = ByteArrayKey.owned(ABC, 0, ABC.length);
    final var key2 = ByteArrayKey.owned(ABC, 0, ABC.length);
    assertEquals(key1, key2);
  }

  @Test
  void equals_differentContent_returnsFalse() {
    final var key1 = ByteArrayKey.owned(ABC, 0, ABC.length);
    final var key2 = ByteArrayKey.owned(DEF, 0, DEF.length);
    assertNotEquals(key1, key2);
  }

  @Test
  void equals_differentLength_returnsFalse() {
    final var key1 = ByteArrayKey.owned(ABC, 0, ABC.length);
    final var key2 = ByteArrayKey.owned(ABCD, 0, ABCD.length);
    assertNotEquals(key1, key2);
  }

  @Test
  void equals_null_returnsFalse() {
    final var key = ByteArrayKey.owned(ABC, 0, ABC.length);
    assertFalse(key.equals(null));
  }

  @Test
  void equals_wrongType_returnsFalse() {
    final var key = ByteArrayKey.owned(ABC, 0, ABC.length);
    assertFalse(key.equals("ABC"));
  }

  @Test
  void equals_reflexive_returnsTrue() {
    final var key = ByteArrayKey.owned(ABC, 0, ABC.length);
    assertEquals(key, key);
  }

  @Test
  void equals_nullPaddedContent_matchesIdenticalPadding() {
    final byte[] padded1 = new byte[] {'A', 'B', 0, 0, 0};
    final byte[] padded2 = new byte[] {'A', 'B', 0, 0, 0};
    final var key1 = ByteArrayKey.owned(padded1, 0, 5);
    final var key2 = ByteArrayKey.owned(padded2, 0, 5);
    assertEquals(key1, key2);
  }

  // ===========================================================================
  // Hashing
  // ===========================================================================

  @Test
  void hashCode_sameContent_returnsSameHash() {
    final var key1 = ByteArrayKey.owned(ABC, 0, ABC.length);
    final var key2 = ByteArrayKey.owned(ABC, 0, ABC.length);
    assertEquals(key1.hashCode(), key2.hashCode());
  }

  @Test
  void hashCode_differentContent_returnsDifferentHash() {
    final var key1 = ByteArrayKey.owned(ABC, 0, ABC.length);
    final var key2 = ByteArrayKey.owned(DEF, 0, DEF.length);
    // Hash collision is theoretically possible but astronomically unlikely for 3-byte ASCII keys
    assertNotEquals(key1.hashCode(), key2.hashCode());
  }

  // ===========================================================================
  // In-place mutation (orchestrator-specific)
  // ===========================================================================

  @Test
  void overwrite_updatesContentAndHash() {
    final var buf = new UnsafeBuffer(ABC);
    final var key = ByteArrayKey.owned(ABC, 0, ABC.length);
    final int oldHash = key.hashCode();

    final var newBuf = new UnsafeBuffer(DEF);
    key.overwrite(newBuf, 0, DEF.length);

    assertEquals("DEF", key.toString());
    assertNotEquals(oldHash, key.hashCode());
  }

  @Test
  void wrapForProbe_byteArray_updatesWithoutCopy() {
    final var key = ByteArrayKey.emptyForLookup(10);
    assertEquals(0, key.length());

    key.wrapForProbe(ABC, 0, ABC.length);

    assertEquals("ABC", key.toString());
    assertEquals(3, key.length());
  }

  @Test
  void wrapForProbe_directBuffer_updatesWithoutCopy() {
    final var key = ByteArrayKey.emptyForLookup(10);
    final var buf = new UnsafeBuffer("HELLO".getBytes(StandardCharsets.US_ASCII));

    key.wrapForProbe(buf, 0, 5);

    assertEquals("HELLO", key.toString());
    assertEquals(5, key.length());
  }

  @Test
  void wrapForProbe_directBuffer_exceedsCapacity_throwsIoobe() {
    final var key = ByteArrayKey.emptyForLookup(3);
    final var buf = new UnsafeBuffer("TOOLONG".getBytes(StandardCharsets.US_ASCII));

    assertThrows(IndexOutOfBoundsException.class, () -> key.wrapForProbe(buf, 0, 7));
  }

  @Test
  void set_directBuffer_delegatesToWrapForProbe() {
    final var key = ByteArrayKey.emptyForLookup(10);
    final var buf = new UnsafeBuffer("TEST".getBytes(StandardCharsets.US_ASCII));

    key.set(buf, 0, 4);

    assertEquals("TEST", key.toString());
    assertEquals(4, key.length());
  }

  // ===========================================================================
  // Agrona map integration
  // ===========================================================================

  @Test
  void owned_insertableIntoAgronaMap() {
    final var map = new Object2ObjectHashMap<ByteArrayKey, String>(4, 0.65f);
    final var key = ByteArrayKey.owned(ABC, 0, ABC.length);

    map.put(key, "value");

    assertEquals("value", map.get(key));
    // Also findable via a probe with the same content
    final var probe = ByteArrayKey.probe(ABC, 0, ABC.length);
    assertEquals("value", map.get(probe));
  }

  // ===========================================================================
  // Accessors
  // ===========================================================================

  @Test
  void length_returnsKeyLength() {
    assertEquals(3, ByteArrayKey.owned(ABC, 0, ABC.length).length());
    assertEquals(4, ByteArrayKey.owned(ABCD, 0, ABCD.length).length());
    assertEquals(0, ByteArrayKey.emptyForLookup(10).length());
  }

  @Test
  void toString_returnsAsciiRepresentation() {
    final var key = ByteArrayKey.owned("EURUSD".getBytes(StandardCharsets.US_ASCII), 0, 6);
    assertEquals("EURUSD", key.toString());
  }
}
