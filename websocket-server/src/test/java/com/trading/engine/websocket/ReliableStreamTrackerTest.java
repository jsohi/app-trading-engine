package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/** Unit tests for {@link ReliableStreamTracker}. */
final class ReliableStreamTrackerTest {

  private static ReliableStreamTracker newTracker() {
    return new ReliableStreamTracker(8, 64, WebSocketMetrics.createWithDefaults());
  }

  @Test
  void capture_thenLookup_returnsPayload() {
    final var tracker = newTracker();
    final var payload = new byte[] {1, 2, 3, 4, 5};
    tracker.capture(1L, 100, payload, 0, payload.length);

    assertEquals(payload.length, tracker.lookupLength(1L));
    assertEquals(100, tracker.lookupTemplateId(1L));
    final var dst = new byte[payload.length];
    final int copied = tracker.copyPayload(1L, dst, 0);
    assertEquals(payload.length, copied);
    assertArrayEquals(payload, dst);
  }

  @Test
  void capture_seqNoOverwrite_oldestEvicted() {
    // Capacity 8 → seqNo 1 maps to slot 1; seqNo 9 also maps to slot 1, evicting seqNo 1.
    final var tracker = newTracker();
    final var p1 = new byte[] {1};
    final var p9 = new byte[] {9};
    tracker.capture(1L, 100, p1, 0, 1);
    assertEquals(1, tracker.lookupLength(1L));
    tracker.capture(9L, 100, p9, 0, 1);
    assertEquals(-1, tracker.lookupLength(1L));
    assertEquals(1, tracker.lookupLength(9L));
  }

  @Test
  void evict_clearsSlot() {
    final var tracker = newTracker();
    final var payload = new byte[] {1, 2, 3};
    tracker.capture(1L, 100, payload, 0, payload.length);
    tracker.evict(1L);
    assertEquals(-1, tracker.lookupLength(1L));
  }

  @Test
  void evict_idempotentOnMissingSlot() {
    final var tracker = newTracker();
    tracker.evict(99L); // never captured — must not throw
    assertEquals(-1, tracker.lookupLength(99L));
  }

  @Test
  void capture_oversizedPayload_recordsEvictionAndSkipsCapture() {
    // payloadCapacity = frameSize - SLOT_HEADER_SIZE = 64 - 16 = 48
    final var tracker = newTracker();
    final var oversized = new byte[64]; // > 48
    tracker.capture(1L, 100, oversized, 0, oversized.length);
    assertEquals(-1, tracker.lookupLength(1L), "oversized payload not captured");
    assertEquals(1L, tracker.highestSeqNo(), "highSeqNo still updated to preserve replay bounds");
  }

  @Test
  void capture_zeroOrNegativeSeqNo_throws() {
    final var tracker = newTracker();
    final var payload = new byte[] {1};
    assertThrows(
        IllegalArgumentException.class, () -> tracker.capture(0L, 100, payload, 0, payload.length));
  }

  @Test
  void oldestSeqNo_beforeRollover_isOne() {
    final var tracker = newTracker();
    final var payload = new byte[] {1};
    for (int i = 1; i <= 4; i++) {
      tracker.capture(i, 100, payload, 0, 1);
    }
    assertEquals(1L, tracker.oldestSeqNo());
  }

  @Test
  void oldestSeqNo_afterRollover_tracksRingFront() {
    // Capacity 8 → after seqNo 12, oldest is 12 - 8 + 1 = 5.
    final var tracker = newTracker();
    final var payload = new byte[] {1};
    for (int i = 1; i <= 12; i++) {
      tracker.capture(i, 100, payload, 0, 1);
    }
    assertEquals(5L, tracker.oldestSeqNo());
  }

  @Test
  void constructor_invalidCapacity_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new ReliableStreamTracker(7, 64, WebSocketMetrics.createWithDefaults()));
  }

  @Test
  void constructor_frameSizeTooSmall_throws() {
    // frameSize must be > SLOT_HEADER_SIZE (16)
    assertThrows(
        IllegalArgumentException.class,
        () -> new ReliableStreamTracker(8, 16, WebSocketMetrics.createWithDefaults()));
  }
}
