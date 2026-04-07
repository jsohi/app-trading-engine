package com.trading.engine.cluster.sequencer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteOrder;
import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.Test;

class EventSequencerTest {

  @Test
  void producesMonotonicGaplessSequence() {
    EventSequencer seq = new EventSequencer();
    for (long expected = 1L; expected <= 1000L; expected++) {
      assertEquals(expected, seq.nextSequence());
    }
  }

  @Test
  void currentSequenceBeforeFirstCall() {
    EventSequencer seq = new EventSequencer();
    assertEquals(0L, seq.currentSequence());
  }

  @Test
  void currentSequenceReflectsCallCount() {
    EventSequencer seq = new EventSequencer();
    for (int i = 0; i < 42; i++) {
      seq.nextSequence();
    }
    assertEquals(42L, seq.currentSequence());
  }

  @Test
  void snapshotRoundTripContinuesSequence() {
    EventSequencer src = new EventSequencer();
    for (int i = 0; i < 42; i++) {
      src.nextSequence();
    }
    assertEquals(42L, src.currentSequence());

    ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(EventSequencer.SNAPSHOT_LENGTH);
    int written = src.snapshotTo(buffer, 0);
    assertEquals(EventSequencer.SNAPSHOT_LENGTH, written);

    EventSequencer restored = new EventSequencer();
    restored.restoreFrom(buffer, 0);
    assertEquals(42L, restored.currentSequence());
    assertEquals(43L, restored.nextSequence());
  }

  @Test
  void snapshotAtOffset() {
    EventSequencer src = new EventSequencer();
    for (int i = 0; i < 7; i++) {
      src.nextSequence();
    }
    ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64);
    src.snapshotTo(buffer, 16);

    EventSequencer restored = new EventSequencer();
    restored.restoreFrom(buffer, 16);
    assertEquals(7L, restored.currentSequence());
    assertEquals(8L, restored.nextSequence());
  }

  @Test
  void deterministicAcrossInstances() {
    EventSequencer a = new EventSequencer();
    EventSequencer b = new EventSequencer();
    for (int i = 0; i < 1000; i++) {
      assertEquals(a.nextSequence(), b.nextSequence());
    }
  }

  @Test
  void snapshotLengthIsEightBytes() {
    assertEquals(Long.BYTES, EventSequencer.SNAPSHOT_LENGTH);
  }

  @Test
  void rejectsNegativeSnapshotCounter() {
    EventSequencer seq = new EventSequencer();
    ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(EventSequencer.SNAPSHOT_LENGTH);
    buffer.putLong(0, -1L, ByteOrder.LITTLE_ENDIAN);
    assertThrows(IllegalStateException.class, () -> seq.restoreFrom(buffer, 0));

    buffer.putLong(0, Long.MIN_VALUE, ByteOrder.LITTLE_ENDIAN);
    assertThrows(IllegalStateException.class, () -> seq.restoreFrom(buffer, 0));
  }

  @Test
  void roundTripAtZeroFreshInstance() {
    // Fresh sequencer (counter=0) round-trips and stays fresh: first nextSequence() still
    // returns 1. Pins the "unassigned" sentinel across snapshot.
    EventSequencer src = new EventSequencer();
    ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(EventSequencer.SNAPSHOT_LENGTH);
    src.snapshotTo(buffer, 0);

    EventSequencer restored = new EventSequencer();
    restored.restoreFrom(buffer, 0);
    assertEquals(0L, restored.currentSequence());
    assertEquals(1L, restored.nextSequence());
  }
}
