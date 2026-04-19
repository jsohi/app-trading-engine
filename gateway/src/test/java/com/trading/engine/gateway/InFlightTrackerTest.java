package com.trading.engine.gateway;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.util.ByteArrayKey;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class InFlightTrackerTest {

  private static final long TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);
  private static final int MAX_CLORDID_LEN = 20;

  /**
   * Pre-computed FNV-1a 32-bit collision pair. Both strings produce hash -542748011. Found via
   * brute-force search over "ORD" + 7-digit sequences. Used by {@link
   * #onCommandSent_hashCollision_bothTracked} to verify content-based keying distinguishes entries
   * that would collide on a hash-only map.
   */
  private static final byte[] COLLISION_A = id("ORD0306246");

  private static final byte[] COLLISION_B = id("ORD1047780");

  private final InFlightTracker tracker = new InFlightTracker(16, TIMEOUT_NS, MAX_CLORDID_LEN);

  private static byte[] id(final String s) {
    return s.getBytes(StandardCharsets.US_ASCII);
  }

  @Test
  void onCommandSent_tracksEntry() {
    final byte[] clOrdId = id("ORD-00000000001");
    tracker.onCommandSent(clOrdId, 0, clOrdId.length, 1_000_000L);
    assertEquals(1, tracker.size());
  }

  @Test
  void onCommandSent_multipleEntries() {
    tracker.onCommandSent(id("ORD-001"), 0, 7, 1_000L);
    tracker.onCommandSent(id("ORD-002"), 0, 7, 2_000L);
    tracker.onCommandSent(id("ORD-003"), 0, 7, 3_000L);
    assertEquals(3, tracker.size());
  }

  @Test
  void onResponseReceived_removesTrackedEntry() {
    final byte[] clOrdId = id("ORD-00000000001");
    tracker.onCommandSent(clOrdId, 0, clOrdId.length, 1_000_000L);
    assertTrue(tracker.onResponseReceived(clOrdId, 0, clOrdId.length));
    assertEquals(0, tracker.size());
  }

  @Test
  void onResponseReceived_unknownId_returnsFalse() {
    final byte[] clOrdId = id("UNKNOWN");
    assertFalse(tracker.onResponseReceived(clOrdId, 0, clOrdId.length));
  }

  @Test
  void onResponseReceived_alreadyRemoved_returnsFalse() {
    final byte[] clOrdId = id("ORD-001");
    tracker.onCommandSent(clOrdId, 0, clOrdId.length, 1_000L);
    assertTrue(tracker.onResponseReceived(clOrdId, 0, clOrdId.length));
    assertFalse(tracker.onResponseReceived(clOrdId, 0, clOrdId.length));
  }

  @Test
  void checkTimeouts_expiredEntries_invokesCallback() {
    final byte[] clOrdId = id("ORD-001");
    tracker.onCommandSent(clOrdId, 0, clOrdId.length, 1_000L);

    final AtomicInteger expired = new AtomicInteger();
    final long nowNs = 1_000L + TIMEOUT_NS; // exactly at timeout boundary
    final int count =
        tracker.checkTimeouts(
            nowNs, (clOrdId2, offset, length, sentNs) -> expired.incrementAndGet());

    assertEquals(1, count);
    assertEquals(1, expired.get());
    assertEquals(0, tracker.size());
  }

  @Test
  void checkTimeouts_unexpiredEntries_notInvoked() {
    final byte[] clOrdId = id("ORD-001");
    tracker.onCommandSent(clOrdId, 0, clOrdId.length, 1_000L);

    final AtomicInteger expired = new AtomicInteger();
    final long nowNs = 1_000L + TIMEOUT_NS - 1; // just before timeout
    final int count =
        tracker.checkTimeouts(
            nowNs, (clOrdId2, offset, length, sentNs) -> expired.incrementAndGet());

    assertEquals(0, count);
    assertEquals(0, expired.get());
    assertEquals(1, tracker.size());
  }

  @Test
  void checkTimeouts_mixedEntries_onlyExpiresOld() {
    tracker.onCommandSent(id("OLD"), 0, 3, 1_000L);
    tracker.onCommandSent(id("NEW"), 0, 3, TIMEOUT_NS + 500L);

    final AtomicInteger expired = new AtomicInteger();
    final long nowNs = 1_000L + TIMEOUT_NS;
    final int count =
        tracker.checkTimeouts(
            nowNs, (clOrdId2, offset, length, sentNs) -> expired.incrementAndGet());

    assertEquals(1, count);
    assertEquals(1, tracker.size());
  }

  @Test
  void reset_clearsAllEntries() {
    tracker.onCommandSent(id("ORD-001"), 0, 7, 1_000L);
    tracker.onCommandSent(id("ORD-002"), 0, 7, 2_000L);
    tracker.reset();
    assertEquals(0, tracker.size());
  }

  @Test
  void constructor_negativeTimeout_throws() {
    assertThrows(IllegalArgumentException.class, () -> new InFlightTracker(16, -1, 20));
  }

  @Test
  void constructor_zeroTimeout_throws() {
    assertThrows(IllegalArgumentException.class, () -> new InFlightTracker(16, 0, 20));
  }

  @Test
  void constructor_zeroCapacity_throws() {
    assertThrows(IllegalArgumentException.class, () -> new InFlightTracker(0, TIMEOUT_NS, 20));
  }

  @Test
  void constructor_negativeCapacity_throws() {
    assertThrows(IllegalArgumentException.class, () -> new InFlightTracker(-1, TIMEOUT_NS, 20));
  }

  @Test
  void checkTimeouts_callbackThrows_stillRemovesEntry() {
    final byte[] clOrdId = id("ORD-001");
    tracker.onCommandSent(clOrdId, 0, clOrdId.length, 1_000L);
    assertEquals(1, tracker.size());

    final long nowNs = 1_000L + TIMEOUT_NS;
    assertThrows(
        RuntimeException.class,
        () ->
            tracker.checkTimeouts(
                nowNs,
                (clOrdId2, offset, length, sentNs) -> {
                  throw new RuntimeException("boom");
                }));
    assertEquals(0, tracker.size());
  }

  // ===========================================================================
  // Correctness — content-based keying
  // ===========================================================================

  @Test
  void onCommandSent_twoDistinctClOrdIds_trackedSeparately() {
    final byte[] a = id("ORD-AAA");
    final byte[] b = id("ORD-BBB");
    tracker.onCommandSent(a, 0, a.length, 1_000L);
    tracker.onCommandSent(b, 0, b.length, 2_000L);
    assertEquals(2, tracker.size());

    assertTrue(tracker.onResponseReceived(a, 0, a.length));
    assertEquals(1, tracker.size());
    assertTrue(tracker.onResponseReceived(b, 0, b.length));
    assertEquals(0, tracker.size());
  }

  @Test
  void onCommandSent_duplicateClOrdId_updatesTimestamp() {
    final byte[] clOrdId = id("ORD-DUP");
    tracker.onCommandSent(clOrdId, 0, clOrdId.length, 1_000L);
    tracker.onCommandSent(clOrdId, 0, clOrdId.length, 2_000L);
    assertEquals(1, tracker.size());

    // Timeout should fire based on the updated timestamp (2000), not the original (1000)
    final long tooEarlyNs = 1_000L + TIMEOUT_NS;
    assertEquals(0, tracker.checkTimeouts(tooEarlyNs, (c, o, l, s) -> {}));
    assertEquals(1, tracker.size());

    final long correctNs = 2_000L + TIMEOUT_NS;
    assertEquals(1, tracker.checkTimeouts(correctNs, (c, o, l, s) -> {}));
    assertEquals(0, tracker.size());
  }

  @Test
  void checkTimeouts_callbackReceivesActualClOrdIdBytes() {
    final byte[] original = id("ORD-001");
    tracker.onCommandSent(original, 0, original.length, 1_000L);

    final AtomicReference<byte[]> captured = new AtomicReference<>();
    final AtomicInteger capturedLength = new AtomicInteger();

    tracker.checkTimeouts(
        1_000L + TIMEOUT_NS,
        (clOrdId, offset, length, sentNs) -> {
          captured.set(Arrays.copyOfRange(clOrdId, offset, offset + length));
          capturedLength.set(length);
        });

    assertArrayEquals(original, captured.get());
    assertEquals(original.length, capturedLength.get());
  }

  @Test
  void onCommandSent_hashCollision_bothTracked() {
    // Precondition: verify both strings produce the same FNV-1a 32-bit hash
    assertEquals(
        ByteArrayKey.owned(COLLISION_A, 0, COLLISION_A.length).hashCode(),
        ByteArrayKey.owned(COLLISION_B, 0, COLLISION_B.length).hashCode(),
        "Test precondition failed: COLLISION_A and COLLISION_B must have same FNV-1a hash");

    // Both should be tracked separately (would FAIL on old hash-only Long2LongHashMap)
    tracker.onCommandSent(COLLISION_A, 0, COLLISION_A.length, 1_000L);
    tracker.onCommandSent(COLLISION_B, 0, COLLISION_B.length, 2_000L);
    assertEquals(2, tracker.size());

    // Independent acknowledgement
    assertTrue(tracker.onResponseReceived(COLLISION_A, 0, COLLISION_A.length));
    assertEquals(1, tracker.size());
    assertTrue(tracker.onResponseReceived(COLLISION_B, 0, COLLISION_B.length));
    assertEquals(0, tracker.size());
  }

  // ===========================================================================
  // Pool management
  // ===========================================================================

  @Test
  void onCommandSent_poolExhausted_skipsTracking() {
    final InFlightTracker small = new InFlightTracker(2, TIMEOUT_NS, MAX_CLORDID_LEN);
    small.onCommandSent(id("ORD-001"), 0, 7, 1_000L);
    small.onCommandSent(id("ORD-002"), 0, 7, 2_000L);
    small.onCommandSent(id("ORD-003"), 0, 7, 3_000L); // pool exhausted — skipped
    assertEquals(2, small.size());

    assertTrue(small.onResponseReceived(id("ORD-001"), 0, 7));
    assertTrue(small.onResponseReceived(id("ORD-002"), 0, 7));
    assertEquals(0, small.size());
  }

  @Test
  void onResponseReceived_returnsEntryToPool_reusedByNextCommand() {
    final InFlightTracker small = new InFlightTracker(2, TIMEOUT_NS, MAX_CLORDID_LEN);
    small.onCommandSent(id("ORD-001"), 0, 7, 1_000L);
    small.onCommandSent(id("ORD-002"), 0, 7, 2_000L);

    assertTrue(small.onResponseReceived(id("ORD-001"), 0, 7));

    // Third command should succeed (reuses freed pool slot)
    small.onCommandSent(id("ORD-003"), 0, 7, 3_000L);
    assertEquals(2, small.size());
  }

  @Test
  void onResponseReceived_forPoolSkippedOrder_returnsFalse() {
    final InFlightTracker small = new InFlightTracker(2, TIMEOUT_NS, MAX_CLORDID_LEN);
    small.onCommandSent(id("ORD-001"), 0, 7, 1_000L);
    small.onCommandSent(id("ORD-002"), 0, 7, 2_000L);
    small.onCommandSent(id("ORD-003"), 0, 7, 3_000L); // skipped

    assertFalse(small.onResponseReceived(id("ORD-003"), 0, 7));
    assertEquals(2, small.size());
  }

  @Test
  void reset_followedByNewCommands_tracksCorrectly() {
    tracker.onCommandSent(id("ORD-001"), 0, 7, 1_000L);
    tracker.onCommandSent(id("ORD-002"), 0, 7, 2_000L);
    tracker.reset();
    assertEquals(0, tracker.size());

    tracker.onCommandSent(id("ORD-NEW1"), 0, 8, 3_000L);
    tracker.onCommandSent(id("ORD-NEW2"), 0, 8, 4_000L);
    assertEquals(2, tracker.size());

    assertTrue(tracker.onResponseReceived(id("ORD-NEW1"), 0, 8));
    assertTrue(tracker.onResponseReceived(id("ORD-NEW2"), 0, 8));
    assertEquals(0, tracker.size());
  }

  @Test
  void checkTimeouts_callbackThrows_stillRecyclesEntry() {
    tracker.onCommandSent(id("ORD-001"), 0, 7, 1_000L);

    assertThrows(
        RuntimeException.class,
        () ->
            tracker.checkTimeouts(
                1_000L + TIMEOUT_NS,
                (c, o, l, s) -> {
                  throw new RuntimeException("boom");
                }));
    assertEquals(0, tracker.size());

    // Pool entry was recycled — can track a new command
    tracker.onCommandSent(id("ORD-002"), 0, 7, 5_000L);
    assertEquals(1, tracker.size());
  }

  // ===========================================================================
  // Boundary tests
  // ===========================================================================

  @Test
  void onCommandSent_clOrdIdExactlyMaxLength_tracksSuccessfully() {
    final byte[] max = id("12345678901234567890"); // exactly 20 bytes
    assertEquals(MAX_CLORDID_LEN, max.length);
    tracker.onCommandSent(max, 0, max.length, 1_000L);
    assertEquals(1, tracker.size());
    assertTrue(tracker.onResponseReceived(max, 0, max.length));
  }

  @Test
  void onCommandSent_clOrdIdSingleByte_tracksSuccessfully() {
    final byte[] single = id("X");
    tracker.onCommandSent(single, 0, 1, 1_000L);
    assertEquals(1, tracker.size());
    assertTrue(tracker.onResponseReceived(single, 0, 1));
  }

  @Test
  void onCommandSent_clOrdIdExceedsMaxLength_skips() {
    final byte[] tooLong = id("123456789012345678901"); // 21 bytes
    tracker.onCommandSent(tooLong, 0, tooLong.length, 1_000L);
    assertEquals(0, tracker.size());
  }

  @Test
  void onCommandSent_zeroLengthClOrdId_skips() {
    tracker.onCommandSent(id("X"), 0, 0, 1_000L);
    assertEquals(0, tracker.size());
  }

  @Test
  void constructor_zeroMaxClOrdIdLength_throws() {
    assertThrows(IllegalArgumentException.class, () -> new InFlightTracker(16, TIMEOUT_NS, 0));
  }

  @Test
  void constructor_negativeMaxClOrdIdLength_throws() {
    assertThrows(IllegalArgumentException.class, () -> new InFlightTracker(16, TIMEOUT_NS, -1));
  }

  // ===========================================================================
  // Multi-timeout — deterministic drain across scans
  // ===========================================================================

  @Test
  void checkTimeouts_multipleExpiredEntries_allDrainedAcrossScans() {
    tracker.onCommandSent(id("ORD-001"), 0, 7, 1_000L);
    tracker.onCommandSent(id("ORD-002"), 0, 7, 1_000L);
    tracker.onCommandSent(id("ORD-003"), 0, 7, 1_000L);

    final long nowNs = 1_000L + TIMEOUT_NS;
    final AtomicInteger total = new AtomicInteger();

    // Agrona's compaction may skip entries — drain across multiple scans (max 3)
    for (int scan = 0; scan < 3 && tracker.size() > 0; scan++) {
      tracker.checkTimeouts(nowNs, (c, o, l, s) -> total.incrementAndGet());
    }

    assertEquals(3, total.get());
    assertEquals(0, tracker.size());
  }
}
