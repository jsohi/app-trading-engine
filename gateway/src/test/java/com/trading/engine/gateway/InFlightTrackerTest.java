package com.trading.engine.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class InFlightTrackerTest {

  private static final long TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);

  private final InFlightTracker tracker = new InFlightTracker(16, TIMEOUT_NS);

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
    final int count = tracker.checkTimeouts(nowNs, (hash, sentNs) -> expired.incrementAndGet());

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
    final int count = tracker.checkTimeouts(nowNs, (hash, sentNs) -> expired.incrementAndGet());

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
    final int count = tracker.checkTimeouts(nowNs, (hash, sentNs) -> expired.incrementAndGet());

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
    assertThrows(IllegalArgumentException.class, () -> new InFlightTracker(16, -1));
  }

  @Test
  void constructor_zeroTimeout_throws() {
    assertThrows(IllegalArgumentException.class, () -> new InFlightTracker(16, 0));
  }

  @Test
  void constructor_zeroCapacity_throws() {
    assertThrows(IllegalArgumentException.class, () -> new InFlightTracker(0, TIMEOUT_NS));
  }

  @Test
  void constructor_negativeCapacity_throws() {
    assertThrows(IllegalArgumentException.class, () -> new InFlightTracker(-1, TIMEOUT_NS));
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
                (hash, sentNs) -> {
                  throw new RuntimeException("boom");
                }));
    assertEquals(0, tracker.size());
  }

  @Test
  void fnv1aHash_deterministic() {
    final byte[] a = id("ORD-00000000001");
    final byte[] b = id("ORD-00000000001");
    assertEquals(
        InFlightTracker.fnv1aHash(a, 0, a.length), InFlightTracker.fnv1aHash(b, 0, b.length));
  }

  @Test
  void fnv1aHash_differentInputs_differentHashes() {
    final byte[] a = id("ORD-001");
    final byte[] b = id("ORD-002");
    assertTrue(
        InFlightTracker.fnv1aHash(a, 0, a.length) != InFlightTracker.fnv1aHash(b, 0, b.length));
  }

  @Test
  void fnv1aHash_respectsOffsetAndLength() {
    final byte[] buf = id("xxORD-001yy");
    final byte[] exact = id("ORD-001");
    assertEquals(
        InFlightTracker.fnv1aHash(exact, 0, exact.length), InFlightTracker.fnv1aHash(buf, 2, 7));
  }
}
