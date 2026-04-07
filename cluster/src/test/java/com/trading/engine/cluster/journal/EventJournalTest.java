package com.trading.engine.cluster.journal;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

class EventJournalTest {

  /**
   * Small helper to append an event carrying an ASCII payload. Returns the payload bytes so the
   * test can assert round-trip integrity.
   */
  private static byte[] appendAscii(EventJournal journal, long seqNo, int eventType, String text) {
    byte[] bytes = text.getBytes(StandardCharsets.US_ASCII);
    UnsafeBuffer src = new UnsafeBuffer(bytes);
    journal.append(seqNo, eventType, src, 0, bytes.length);
    return bytes;
  }

  // ---------------------------------------------------------------------------
  // Constructor validation
  // ---------------------------------------------------------------------------

  @Test
  void constructorRejectsZero() {
    assertThrows(IllegalArgumentException.class, () -> new EventJournal(0));
  }

  @Test
  void constructorRejectsNegative() {
    assertThrows(IllegalArgumentException.class, () -> new EventJournal(-1));
    assertThrows(IllegalArgumentException.class, () -> new EventJournal(-8));
  }

  @Test
  void constructorRejectsNonPowerOfTwo() {
    assertThrows(IllegalArgumentException.class, () -> new EventJournal(3));
    assertThrows(IllegalArgumentException.class, () -> new EventJournal(7));
    assertThrows(IllegalArgumentException.class, () -> new EventJournal(100));
    assertThrows(IllegalArgumentException.class, () -> new EventJournal(1000));
  }

  @Test
  void constructorRejectsCapacityOne() {
    // capacity=1 is degenerate: the eviction math reads the "next slot" to derive the new
    // lowestSequence, and with a single slot "next" wraps back to the same slot we're evicting.
    assertThrows(IllegalArgumentException.class, () -> new EventJournal(1));
  }

  @Test
  void constructorAcceptsPowerOfTwoCapacities() {
    for (int cap : new int[] {2, 4, 8, 16, 64, 1024, 65_536}) {
      EventJournal j = new EventJournal(cap);
      assertEquals(cap, j.capacity());
      assertEquals(0, j.size());
      assertTrue(j.isEmpty());
    }
  }

  // ---------------------------------------------------------------------------
  // Append validation
  // ---------------------------------------------------------------------------

  @Test
  void appendRejectsZeroSeqNo() {
    EventJournal j = new EventJournal(16);
    UnsafeBuffer src = new UnsafeBuffer(new byte[] {1, 2, 3});
    assertThrows(IllegalArgumentException.class, () -> j.append(0L, 100, src, 0, 3));
  }

  @Test
  void appendRejectsNegativeSeqNo() {
    EventJournal j = new EventJournal(16);
    UnsafeBuffer src = new UnsafeBuffer(new byte[] {1, 2, 3});
    assertThrows(IllegalArgumentException.class, () -> j.append(-1L, 100, src, 0, 3));
  }

  @Test
  void appendRejectsNegativeLength() {
    EventJournal j = new EventJournal(16);
    UnsafeBuffer src = new UnsafeBuffer(new byte[] {1, 2, 3});
    assertThrows(IllegalArgumentException.class, () -> j.append(1L, 100, src, 0, -1));
  }

  @Test
  void appendAcceptsZeroLengthPayload() {
    EventJournal j = new EventJournal(16);
    UnsafeBuffer src = new UnsafeBuffer(new byte[0]);
    j.append(1L, 100, src, 0, 0);
    EventEntry entry = j.get(1L);
    assertNotNull(entry);
    assertEquals(0, entry.payloadLength());
  }

  // ---------------------------------------------------------------------------
  // Eviction boundary
  // ---------------------------------------------------------------------------

  @Test
  void firstEvictionBoundary() {
    // Append exactly capacity events, then one more — verify the eviction transition.
    EventJournal j = new EventJournal(4);
    for (long seqNo = 1L; seqNo <= 4L; seqNo++) {
      appendAscii(j, seqNo, 100, "e" + seqNo);
    }
    assertEquals(4, j.size());
    assertEquals(1L, j.lowestSequence());
    assertEquals(4L, j.highestSequence());

    // The (capacity+1)-th append triggers the very first eviction.
    appendAscii(j, 5L, 100, "e5");
    assertEquals(4, j.size());
    assertEquals(2L, j.lowestSequence());
    assertEquals(5L, j.highestSequence());
    assertNull(j.get(1L));
    assertNotNull(j.get(2L));
    assertNotNull(j.get(5L));
  }

  @Test
  void replayFromExactlyHighestReturnsOne() {
    EventJournal j = new EventJournal(16);
    for (long seqNo = 1L; seqNo <= 5L; seqNo++) {
      appendAscii(j, seqNo, 100, "e" + seqNo);
    }
    int[] counter = new int[] {0};
    long[] seen = new long[] {-1L};
    int delivered =
        j.replayFrom(
            5L,
            (seqNo, eventType, buf, offset, length) -> {
              seen[0] = seqNo;
              counter[0]++;
            });
    assertEquals(1, delivered);
    assertEquals(5L, seen[0]);
  }

  @Test
  void replayFromExactlyLowestReturnsAll() {
    EventJournal j = new EventJournal(4);
    for (long seqNo = 1L; seqNo <= 6L; seqNo++) {
      appendAscii(j, seqNo, 100, "e" + seqNo);
    }
    // lowest=3, highest=6
    int[] counter = new int[] {0};
    int delivered = j.replayFrom(3L, (seqNo, eventType, buf, offset, length) -> counter[0]++);
    assertEquals(4, delivered);
  }

  @Test
  void defaultConstructorUses65536() {
    EventJournal j = new EventJournal();
    assertEquals(EventJournal.DEFAULT_CAPACITY, j.capacity());
    assertEquals(65_536, j.capacity());
  }

  // ---------------------------------------------------------------------------
  // Append + diagnostics
  // ---------------------------------------------------------------------------

  @Test
  void emptyJournalDiagnostics() {
    EventJournal j = new EventJournal(16);
    assertEquals(0, j.size());
    assertTrue(j.isEmpty());
    assertEquals(0L, j.lowestSequence());
    assertEquals(0L, j.highestSequence());
  }

  @Test
  void monotonicAppendTracksWindow() {
    EventJournal j = new EventJournal(16);
    for (long seqNo = 1L; seqNo <= 10L; seqNo++) {
      appendAscii(j, seqNo, 100, "event-" + seqNo);
    }
    assertEquals(10, j.size());
    assertFalse(j.isEmpty());
    assertEquals(1L, j.lowestSequence());
    assertEquals(10L, j.highestSequence());
  }

  @Test
  void evictionOnOverflow() {
    EventJournal j = new EventJournal(4);
    for (long seqNo = 1L; seqNo <= 6L; seqNo++) {
      appendAscii(j, seqNo, 100, "e" + seqNo);
    }
    // Ring saturated at 4, oldest two (1 and 2) evicted.
    assertEquals(4, j.size());
    assertEquals(3L, j.lowestSequence());
    assertEquals(6L, j.highestSequence());
    assertNull(j.get(1L));
    assertNull(j.get(2L));
    for (long seqNo = 3L; seqNo <= 6L; seqNo++) {
      assertNotNull(j.get(seqNo), "seqNo " + seqNo + " should still be retained");
    }
  }

  // ---------------------------------------------------------------------------
  // Replay
  // ---------------------------------------------------------------------------

  @Test
  void replayFromMidWindowReturnsTail() {
    EventJournal j = new EventJournal(16);
    for (long seqNo = 1L; seqNo <= 10L; seqNo++) {
      appendAscii(j, seqNo, 100, "e" + seqNo);
    }

    long[] seenSeqNos = new long[10];
    int[] counter = new int[] {0};
    int delivered =
        j.replayFrom(
            5L,
            (seqNo, eventType, buf, offset, length) -> {
              seenSeqNos[counter[0]++] = seqNo;
            });

    assertEquals(6, delivered);
    assertEquals(6, counter[0]);
    for (int i = 0; i < 6; i++) {
      assertEquals(5L + i, seenSeqNos[i]);
    }
  }

  @Test
  void replayFromSequenceAboveHighestReturnsZero() {
    EventJournal j = new EventJournal(16);
    for (long seqNo = 1L; seqNo <= 5L; seqNo++) {
      appendAscii(j, seqNo, 100, "e" + seqNo);
    }
    int[] counter = new int[] {0};
    int delivered = j.replayFrom(999L, (seqNo, eventType, buf, offset, length) -> counter[0]++);
    assertEquals(0, delivered);
    assertEquals(0, counter[0]);
  }

  @Test
  void replayFromClampsToLowestAfterEviction() {
    EventJournal j = new EventJournal(4);
    for (long seqNo = 1L; seqNo <= 6L; seqNo++) {
      appendAscii(j, seqNo, 100, "e" + seqNo);
    }
    // lowest is 3 now (1 and 2 evicted). replayFrom(1) should deliver 3..6 = 4 events.
    int[] counter = new int[] {0};
    long[] first = new long[] {-1};
    int delivered =
        j.replayFrom(
            1L,
            (seqNo, eventType, buf, offset, length) -> {
              if (counter[0] == 0) {
                first[0] = seqNo;
              }
              counter[0]++;
            });
    assertEquals(4, delivered);
    assertEquals(3L, first[0]);
  }

  @Test
  void replayOnEmptyJournalReturnsZero() {
    EventJournal j = new EventJournal(16);
    int[] counter = new int[] {0};
    int delivered = j.replayFrom(1L, (seqNo, eventType, buf, offset, length) -> counter[0]++);
    assertEquals(0, delivered);
  }

  // ---------------------------------------------------------------------------
  // Payload integrity
  // ---------------------------------------------------------------------------

  @Test
  void getReturnsIntactPayload() {
    EventJournal j = new EventJournal(16);
    byte[] expected = appendAscii(j, 42L, 102, "OrderFilled:ORD-42:cumQty=100");
    EventEntry entry = j.get(42L);
    assertNotNull(entry);
    assertEquals(42L, entry.sequenceNumber());
    assertEquals(102, entry.eventType());
    assertEquals(expected.length, entry.payloadLength());
    byte[] actual = new byte[entry.payloadLength()];
    entry.payload().getBytes(0, actual);
    assertArrayEquals(expected, actual);
  }

  @Test
  void getReturnsNullForUnknownSeqNo() {
    EventJournal j = new EventJournal(16);
    appendAscii(j, 1L, 100, "e1");
    assertNull(j.get(999L));
    assertNull(j.get(0L));
    assertNull(j.get(-5L));
  }

  @Test
  void payloadBufferGrowsForLargeEvent() {
    // Small event (8 bytes) then a large one (8 KB) to the same slot after wraparound.
    EventJournal j = new EventJournal(2);
    appendAscii(j, 1L, 100, "tinyevt");
    appendAscii(j, 2L, 100, "another");

    byte[] big = new byte[8 * 1024];
    for (int i = 0; i < big.length; i++) {
      big[i] = (byte) (i & 0xff);
    }
    UnsafeBuffer src = new UnsafeBuffer(big);
    j.append(3L, 100, src, 0, big.length);

    EventEntry entry = j.get(3L);
    assertNotNull(entry);
    assertEquals(big.length, entry.payloadLength());
    byte[] actual = new byte[entry.payloadLength()];
    entry.payload().getBytes(0, actual);
    assertArrayEquals(big, actual);
  }

  // ---------------------------------------------------------------------------
  // Snapshot round-trip
  // ---------------------------------------------------------------------------

  @Test
  void snapshotRoundTripPopulatedJournal() {
    EventJournal src = new EventJournal(16);
    for (long seqNo = 1L; seqNo <= 10L; seqNo++) {
      appendAscii(src, seqNo, 100 + (int) seqNo, "event-" + seqNo);
    }

    MutableDirectBuffer buf = new ExpandableArrayBuffer(2048);
    int bytesWritten = src.snapshotTo(buf, 0);
    assertTrue(bytesWritten > EventJournal.SNAPSHOT_HEADER_LENGTH);

    EventJournal restored = new EventJournal(16);
    int bytesRead = restored.restoreFrom(buf, 0);
    assertEquals(bytesWritten, bytesRead);

    assertEquals(10, restored.size());
    assertEquals(1L, restored.lowestSequence());
    assertEquals(10L, restored.highestSequence());

    // Every seqNo retrievable with the right eventType and payload.
    for (long seqNo = 1L; seqNo <= 10L; seqNo++) {
      EventEntry entry = restored.get(seqNo);
      assertNotNull(entry, "restored journal missing seqNo " + seqNo);
      assertEquals(100 + (int) seqNo, entry.eventType());
      byte[] expected = ("event-" + seqNo).getBytes(StandardCharsets.US_ASCII);
      assertEquals(expected.length, entry.payloadLength());
      byte[] actual = new byte[entry.payloadLength()];
      entry.payload().getBytes(0, actual);
      assertArrayEquals(expected, actual);
    }
  }

  @Test
  void snapshotRoundTripEmptyJournal() {
    EventJournal src = new EventJournal(16);
    MutableDirectBuffer buf = new ExpandableArrayBuffer(128);
    int bytesWritten = src.snapshotTo(buf, 0);
    assertEquals(EventJournal.SNAPSHOT_HEADER_LENGTH, bytesWritten);

    EventJournal restored = new EventJournal(16);
    int bytesRead = restored.restoreFrom(buf, 0);
    assertEquals(bytesWritten, bytesRead);
    assertTrue(restored.isEmpty());
    assertEquals(0, restored.size());
    assertEquals(0L, restored.lowestSequence());
    assertEquals(0L, restored.highestSequence());
  }

  @Test
  void snapshotRoundTripExactlyFullRing() {
    // size == capacity, writeIdx wrapped to 0 — the most load-bearing case for the writeIdx
    // restore math.
    EventJournal src = new EventJournal(4);
    for (long seqNo = 1L; seqNo <= 4L; seqNo++) {
      appendAscii(src, seqNo, 100, "e" + seqNo);
    }
    assertEquals(4, src.size());

    MutableDirectBuffer buf = new ExpandableArrayBuffer(1024);
    src.snapshotTo(buf, 0);

    EventJournal restored = new EventJournal(4);
    restored.restoreFrom(buf, 0);
    assertEquals(4, restored.size());
    assertEquals(1L, restored.lowestSequence());
    assertEquals(4L, restored.highestSequence());

    // Next append must evict the oldest (seqNo 1) and bump lowest to 2.
    appendAscii(restored, 5L, 100, "e5");
    assertEquals(4, restored.size());
    assertEquals(2L, restored.lowestSequence());
    assertEquals(5L, restored.highestSequence());
    assertNull(restored.get(1L));
    assertNotNull(restored.get(2L));
    assertNotNull(restored.get(5L));
  }

  @Test
  void postEvictionRestoreThenMoreEvictions() {
    // Pre-evict, snapshot, restore, then append enough to trigger more evictions on the
    // restored journal — locks down that the writeIdx restore math is consistent with the
    // eviction loop.
    EventJournal src = new EventJournal(4);
    for (long seqNo = 1L; seqNo <= 6L; seqNo++) {
      appendAscii(src, seqNo, 100, "e" + seqNo);
    }
    // src: lowest=3, highest=6, size=4
    MutableDirectBuffer buf = new ExpandableArrayBuffer(1024);
    src.snapshotTo(buf, 0);

    EventJournal restored = new EventJournal(4);
    restored.restoreFrom(buf, 0);

    // Append two more — should evict 3 and 4, leaving 5..8.
    appendAscii(restored, 7L, 100, "e7");
    appendAscii(restored, 8L, 100, "e8");
    assertEquals(4, restored.size());
    assertEquals(5L, restored.lowestSequence());
    assertEquals(8L, restored.highestSequence());
    assertNull(restored.get(3L));
    assertNull(restored.get(4L));
    for (long seqNo = 5L; seqNo <= 8L; seqNo++) {
      assertNotNull(restored.get(seqNo), "seqNo " + seqNo + " should be retained");
    }
  }

  @Test
  void snapshotRoundTripAfterEviction() {
    // Build a journal that has wrapped around so the oldest live slot is NOT slot 0.
    EventJournal src = new EventJournal(4);
    for (long seqNo = 1L; seqNo <= 7L; seqNo++) {
      appendAscii(src, seqNo, 100, "e" + seqNo);
    }
    // lowest=4, highest=7, size=4

    MutableDirectBuffer buf = new ExpandableArrayBuffer(1024);
    src.snapshotTo(buf, 0);

    EventJournal restored = new EventJournal(4);
    restored.restoreFrom(buf, 0);
    assertEquals(4, restored.size());
    assertEquals(4L, restored.lowestSequence());
    assertEquals(7L, restored.highestSequence());
    for (long seqNo = 4L; seqNo <= 7L; seqNo++) {
      assertNotNull(restored.get(seqNo));
    }
    assertNull(restored.get(3L));
    assertNull(restored.get(8L));

    // replayFrom on the restored journal should deliver all 4 in ascending order.
    int[] counter = new int[] {0};
    long[] seqNos = new long[4];
    restored.replayFrom(
        0L,
        (seqNo, eventType, bufPayload, offset, length) -> {
          seqNos[counter[0]++] = seqNo;
        });
    assertEquals(4, counter[0]);
    for (int i = 0; i < 4; i++) {
      assertEquals(4L + i, seqNos[i]);
    }
  }

  @Test
  void snapshotAtNonZeroOffset() {
    EventJournal src = new EventJournal(16);
    for (long seqNo = 1L; seqNo <= 5L; seqNo++) {
      appendAscii(src, seqNo, 100, "e" + seqNo);
    }
    MutableDirectBuffer buf = new ExpandableArrayBuffer(2048);
    int written = src.snapshotTo(buf, 256);

    EventJournal restored = new EventJournal(16);
    int read = restored.restoreFrom(buf, 256);
    assertEquals(written, read);
    assertEquals(5, restored.size());
    assertEquals(1L, restored.lowestSequence());
    assertEquals(5L, restored.highestSequence());
  }

  @Test
  void restoreFromRejectsCorruptHeaderEmptySnapshotWithSeqs() {
    // size=0 but lowest/highest non-zero — corrupt header, must be rejected.
    MutableDirectBuffer buf = new ExpandableArrayBuffer(128);
    buf.putInt(0, 0); // size
    buf.putLong(Integer.BYTES, 5L); // lowestSequence
    buf.putLong(Integer.BYTES + Long.BYTES, 5L); // highestSequence

    EventJournal restored = new EventJournal(16);
    assertThrows(IllegalStateException.class, () -> restored.restoreFrom(buf, 0));
  }

  @Test
  void restoreFromRejectsHighestBelowLowest() {
    MutableDirectBuffer buf = new ExpandableArrayBuffer(128);
    buf.putInt(0, 1); // size
    buf.putLong(Integer.BYTES, 10L); // lowestSequence
    buf.putLong(Integer.BYTES + Long.BYTES, 5L); // highestSequence < lowest

    EventJournal restored = new EventJournal(16);
    assertThrows(IllegalStateException.class, () -> restored.restoreFrom(buf, 0));
  }

  @Test
  void restoreFromRejectsSizeExceedingCapacity() {
    MutableDirectBuffer buf = new ExpandableArrayBuffer(128);
    buf.putInt(0, 999); // size > capacity
    buf.putLong(Integer.BYTES, 1L);
    buf.putLong(Integer.BYTES + Long.BYTES, 999L);

    EventJournal restored = new EventJournal(16);
    assertThrows(IllegalStateException.class, () -> restored.restoreFrom(buf, 0));
  }

  @Test
  void appendRejectsPayloadExceedingMaxLength() {
    EventJournal j = new EventJournal(16);
    UnsafeBuffer src = new UnsafeBuffer(new byte[EventEntry.MAX_PAYLOAD_LENGTH + 1]);
    assertThrows(
        IllegalArgumentException.class,
        () -> j.append(1L, 100, src, 0, EventEntry.MAX_PAYLOAD_LENGTH + 1));
  }

  @Test
  void appendContinuesAfterRestore() {
    EventJournal src = new EventJournal(16);
    for (long seqNo = 1L; seqNo <= 5L; seqNo++) {
      appendAscii(src, seqNo, 100, "e" + seqNo);
    }
    MutableDirectBuffer buf = new ExpandableArrayBuffer(2048);
    src.snapshotTo(buf, 0);

    EventJournal restored = new EventJournal(16);
    restored.restoreFrom(buf, 0);

    // Continue appending after restore; the journal should accept new events and track them.
    appendAscii(restored, 6L, 100, "e6");
    appendAscii(restored, 7L, 100, "e7");
    assertEquals(7, restored.size());
    assertEquals(1L, restored.lowestSequence());
    assertEquals(7L, restored.highestSequence());
    assertNotNull(restored.get(6L));
    assertNotNull(restored.get(7L));
  }
}
