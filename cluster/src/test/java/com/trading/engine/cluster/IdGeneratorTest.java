package com.trading.engine.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.Test;

class IdGeneratorTest {

  @Test
  void producesSequentialZeroPaddedIds() {
    IdGenerator gen = new IdGenerator("ORD");
    assertEquals("ORD-00000000001", gen.next());
    assertEquals("ORD-00000000002", gen.next());
    assertEquals("ORD-00000000003", gen.next());
  }

  @Test
  void honoursPrefix() {
    assertEquals("EXE-00000000001", new IdGenerator("EXE").next());
    assertEquals("QTE-00000000001", new IdGenerator("QTE").next());
  }

  @Test
  void deterministicAcrossInstances() {
    IdGenerator a = new IdGenerator("ORD");
    IdGenerator b = new IdGenerator("ORD");
    for (int i = 0; i < 1000; i++) {
      assertEquals(a.next(), b.next());
    }
  }

  @Test
  void currentCounterReflectsCallCount() {
    IdGenerator gen = new IdGenerator("ORD");
    assertEquals(0L, gen.currentCounter());
    gen.next();
    gen.next();
    gen.next();
    assertEquals(3L, gen.currentCounter());
  }

  @Test
  void snapshotRoundTripContinuesSequence() {
    IdGenerator src = new IdGenerator("ORD");
    for (int i = 0; i < 42; i++) {
      src.next();
    }
    assertEquals(42L, src.currentCounter());

    ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(IdGenerator.SNAPSHOT_LENGTH);
    src.saveTo(buffer, 0);

    IdGenerator restored = new IdGenerator("ORD");
    restored.loadFrom(buffer, 0);
    assertEquals(42L, restored.currentCounter());
    assertEquals("ORD-00000000043", restored.next());
  }

  @Test
  void snapshotAtOffset() {
    IdGenerator src = new IdGenerator("ORD");
    for (int i = 0; i < 7; i++) {
      src.next();
    }
    ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64);
    src.saveTo(buffer, 16);

    IdGenerator restored = new IdGenerator("ORD");
    restored.loadFrom(buffer, 16);
    assertEquals("ORD-00000000008", restored.next());
  }

  @Test
  void rejectsEmptyPrefix() {
    assertThrows(IllegalArgumentException.class, () -> new IdGenerator(""));
    assertThrows(IllegalArgumentException.class, () -> new IdGenerator(null));
  }

  @Test
  void rejectsOverlongPrefix() {
    // 8 chars is the boundary — the IdPrefix SBE type is char[8] in IdGeneratorSnapshot (205);
    // a longer prefix would silently truncate on snapshot save and break recovery determinism.
    String eightChars = "ABCDEFGH";
    assertEquals(IdGenerator.MAX_PREFIX_LENGTH, eightChars.length());
    new IdGenerator(eightChars); // must not throw
    assertThrows(IllegalArgumentException.class, () -> new IdGenerator(eightChars + "I"));
  }

  @Test
  void nextIntoWritesAsciiBytesAtOffset() {
    IdGenerator gen = new IdGenerator("ORD");
    ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64);

    int len1 = gen.nextInto(buffer, 4); // start at offset 4 to verify offset honored
    assertEquals(15, len1); // "ORD-00000000001" = 15 bytes
    assertEquals(15, gen.idByteLength());
    assertEquals("ORD-00000000001", readAscii(buffer, 4, len1));

    int len2 = gen.nextInto(buffer, 24);
    assertEquals(15, len2);
    assertEquals("ORD-00000000002", readAscii(buffer, 24, len2));

    // The earlier write at offset 4 must NOT have been clobbered by the second call.
    assertEquals("ORD-00000000001", readAscii(buffer, 4, len1));
  }

  @Test
  void nextIntoAndNextShareCounter() {
    // Both APIs advance the same counter — interleaved use stays sequential.
    IdGenerator gen = new IdGenerator("ORD");
    ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64);
    assertEquals("ORD-00000000001", gen.next());
    gen.nextInto(buffer, 0);
    assertEquals("ORD-00000000002", readAscii(buffer, 0, gen.idByteLength()));
    assertEquals("ORD-00000000003", gen.next());
  }

  @Test
  void nextIntoRejectsCounterExhaustion() {
    IdGenerator gen = new IdGenerator("ORD");
    ExpandableArrayBuffer snapshot = new ExpandableArrayBuffer(IdGenerator.SNAPSHOT_LENGTH);
    snapshot.putLong(0, IdGenerator.MAX_COUNTER, ByteOrder.LITTLE_ENDIAN);
    gen.loadFrom(snapshot, 0);

    ExpandableArrayBuffer dst = new ExpandableArrayBuffer(64);
    assertThrows(IllegalStateException.class, () -> gen.nextInto(dst, 0));
    assertEquals(IdGenerator.MAX_COUNTER, gen.currentCounter()); // counter unchanged on failure
  }

  @Test
  void idByteLengthMatchesPrefix() {
    assertEquals(15, new IdGenerator("ORD").idByteLength()); // 3 + 1 + 11
    assertEquals(13, new IdGenerator("E").idByteLength()); // 1 + 1 + 11
    assertEquals(20, new IdGenerator("ABCDEFGH").idByteLength()); // 8 + 1 + 11 — exact SBE fit
  }

  private static String readAscii(ExpandableArrayBuffer buffer, int offset, int length) {
    byte[] dst = new byte[length];
    buffer.getBytes(offset, dst);
    return new String(dst, StandardCharsets.US_ASCII);
  }

  @Test
  void rejectsNonAsciiPrefix() {
    // SBE char[8] is 8 BYTES; a non-ASCII char would inflate byte length even when
    // String.length() is within the cap, so the prefix must be ASCII-only.
    assertThrows(IllegalArgumentException.class, () -> new IdGenerator("ORDé"));
    assertThrows(IllegalArgumentException.class, () -> new IdGenerator("注文")); // 2 CJK chars
    assertThrows(IllegalArgumentException.class, () -> new IdGenerator("A\u0080")); // boundary
    new IdGenerator("A\u007F"); // 0x7F is the highest valid ASCII — must not throw
  }

  @Test
  void rejectsCounterExhaustion() {
    IdGenerator gen = new IdGenerator("ORD");
    ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(IdGenerator.SNAPSHOT_LENGTH);
    buffer.putLong(0, IdGenerator.MAX_COUNTER, ByteOrder.LITTLE_ENDIAN);
    gen.loadFrom(buffer, 0);

    assertThrows(IllegalStateException.class, gen::next);
    // Counter must NOT advance on a failed call — pins check-before-increment ordering
    assertEquals(IdGenerator.MAX_COUNTER, gen.currentCounter());
    // Subsequent calls keep throwing rather than wrapping
    assertThrows(IllegalStateException.class, gen::next);
    assertEquals(IdGenerator.MAX_COUNTER, gen.currentCounter());
  }

  @Test
  void rendersFullElevenDigitRange() {
    // Pin the zero-pad renderer at the edges — 1, 10, 100, 1000, 10^9 (where the loop crosses
    // int range), and the final two values before MAX_COUNTER. Catches any future off-by-one
    // in DIGITS or the modulo loop.
    ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(IdGenerator.SNAPSHOT_LENGTH);

    assertGeneratesNextId(buffer, 0L, "ORD-00000000001");
    assertGeneratesNextId(buffer, 9L, "ORD-00000000010");
    assertGeneratesNextId(buffer, 99L, "ORD-00000000100");
    assertGeneratesNextId(buffer, 999L, "ORD-00000001000");
    assertGeneratesNextId(buffer, 999_999_999L, "ORD-01000000000"); // crosses 10-digit boundary
    assertGeneratesNextId(buffer, 99_999_999_998L, "ORD-99999999999");
  }

  private static void assertGeneratesNextId(
      ExpandableArrayBuffer buffer, long seedCounter, String expected) {
    buffer.putLong(0, seedCounter, ByteOrder.LITTLE_ENDIAN);
    IdGenerator gen = new IdGenerator("ORD");
    gen.loadFrom(buffer, 0);
    assertEquals(expected, gen.next());
  }

  @Test
  void rejectsOutOfRangeSnapshotCounter() {
    IdGenerator gen = new IdGenerator("ORD");
    ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(IdGenerator.SNAPSHOT_LENGTH);
    buffer.putLong(0, IdGenerator.MAX_COUNTER + 1, ByteOrder.LITTLE_ENDIAN);
    assertThrows(IllegalStateException.class, () -> gen.loadFrom(buffer, 0));

    buffer.putLong(0, -1L, ByteOrder.LITTLE_ENDIAN);
    assertThrows(IllegalStateException.class, () -> gen.loadFrom(buffer, 0));
  }

  @Test
  void differentPrefixesProduceDistinctIds() {
    IdGenerator orders = new IdGenerator("ORD");
    IdGenerator execs = new IdGenerator("EXE");
    assertNotEquals(orders.next(), execs.next());
  }
}
