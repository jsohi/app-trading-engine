package com.trading.engine.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.Test;

class IdGeneratorTest {

  @Test
  void producesSequentialZeroPaddedIds() {
    IdGenerator gen = new IdGenerator("ORD");
    assertEquals("ORD-000000001", gen.next());
    assertEquals("ORD-000000002", gen.next());
    assertEquals("ORD-000000003", gen.next());
  }

  @Test
  void honoursPrefix() {
    assertEquals("EXE-000000001", new IdGenerator("EXE").next());
    assertEquals("QTE-000000001", new IdGenerator("QTE").next());
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
    assertEquals("ORD-000000043", restored.next());
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
    assertEquals("ORD-000000008", restored.next());
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
  void rejectsCounterExhaustion() {
    IdGenerator gen = new IdGenerator("ORD");
    ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(IdGenerator.SNAPSHOT_LENGTH);
    buffer.putLong(0, IdGenerator.MAX_COUNTER);
    gen.loadFrom(buffer, 0);

    assertThrows(IllegalStateException.class, gen::next);
    // Counter must NOT advance on a failed call — pins check-before-increment ordering
    assertEquals(IdGenerator.MAX_COUNTER, gen.currentCounter());
    // Subsequent calls keep throwing rather than wrapping
    assertThrows(IllegalStateException.class, gen::next);
    assertEquals(IdGenerator.MAX_COUNTER, gen.currentCounter());
  }

  @Test
  void rendersFullNineDigitRange() {
    // Pin the zero-pad renderer at the edges (1, 10, 99, 100, 999_999_998, 999_999_999)
    // by seeding via snapshot. Catches any future off-by-one in DIGITS or the modulo loop.
    ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(IdGenerator.SNAPSHOT_LENGTH);

    assertGeneratesNextId(buffer, 0L, "ORD-000000001");
    assertGeneratesNextId(buffer, 9L, "ORD-000000010");
    assertGeneratesNextId(buffer, 98L, "ORD-000000099");
    assertGeneratesNextId(buffer, 99L, "ORD-000000100");
    assertGeneratesNextId(buffer, 999_999_997L, "ORD-999999998");
    assertGeneratesNextId(buffer, 999_999_998L, "ORD-999999999");
  }

  private static void assertGeneratesNextId(
      ExpandableArrayBuffer buffer, long seedCounter, String expected) {
    buffer.putLong(0, seedCounter);
    IdGenerator gen = new IdGenerator("ORD");
    gen.loadFrom(buffer, 0);
    assertEquals(expected, gen.next());
  }

  @Test
  void rejectsOutOfRangeSnapshotCounter() {
    IdGenerator gen = new IdGenerator("ORD");
    ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(IdGenerator.SNAPSHOT_LENGTH);
    buffer.putLong(0, IdGenerator.MAX_COUNTER + 1);
    assertThrows(IllegalStateException.class, () -> gen.loadFrom(buffer, 0));

    buffer.putLong(0, -1L);
    assertThrows(IllegalStateException.class, () -> gen.loadFrom(buffer, 0));
  }

  @Test
  void differentPrefixesProduceDistinctIds() {
    IdGenerator orders = new IdGenerator("ORD");
    IdGenerator execs = new IdGenerator("EXE");
    assertNotEquals(orders.next(), execs.next());
  }
}
