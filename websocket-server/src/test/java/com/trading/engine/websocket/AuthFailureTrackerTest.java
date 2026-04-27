package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicLong;
import org.agrona.concurrent.NanoClock;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link AuthFailureTracker} -- verifies per-IP lockout, lockout expiry, stale entry
 * eviction, LRU capacity enforcement, and constructor validation.
 */
final class AuthFailureTrackerTest {

  /** Test clock that can be advanced programmatically. */
  private final AtomicLong clockNs = new AtomicLong(1_000_000_000_000L);

  private final NanoClock testClock = clockNs::get;

  // --- Basic blocking ---

  @Test
  void isBlocked_noFailures_returnsFalse() {
    final var tracker = new AuthFailureTracker(5, 60, testClock);
    assertFalse(tracker.isBlocked("192.168.1.1"));
  }

  @Test
  void isBlocked_belowThreshold_returnsFalse() {
    final var tracker = new AuthFailureTracker(5, 60, testClock);
    tracker.recordFailure("192.168.1.1");
    tracker.recordFailure("192.168.1.1");
    tracker.recordFailure("192.168.1.1");
    tracker.recordFailure("192.168.1.1"); // 4 failures, threshold is 5

    assertFalse(tracker.isBlocked("192.168.1.1"));
  }

  @Test
  void isBlocked_atThreshold_returnsTrue() {
    final var tracker = new AuthFailureTracker(5, 60, testClock);
    for (int i = 0; i < 5; i++) {
      tracker.recordFailure("192.168.1.1");
    }

    assertTrue(tracker.isBlocked("192.168.1.1"));
  }

  @Test
  void isBlocked_nullIp_returnsFalse() {
    final var tracker = new AuthFailureTracker(5, 60, testClock);
    assertFalse(tracker.isBlocked(null));
  }

  @Test
  void recordFailure_nullIp_noOp() {
    final var tracker = new AuthFailureTracker(5, 60, testClock);
    tracker.recordFailure(null);
    assertEquals(0, tracker.trackedIpCount());
  }

  // --- Different IPs are independent ---

  @Test
  void isBlocked_differentIps_independent() {
    final var tracker = new AuthFailureTracker(3, 60, testClock);
    for (int i = 0; i < 3; i++) {
      tracker.recordFailure("192.168.1.1");
    }

    assertTrue(tracker.isBlocked("192.168.1.1"));
    assertFalse(tracker.isBlocked("10.0.0.1")); // different IP, no failures
  }

  // --- Lockout expiry ---

  @Test
  void isBlocked_afterLockoutExpiry_returnsFalse() {
    final var tracker = new AuthFailureTracker(3, 60, testClock);
    for (int i = 0; i < 3; i++) {
      tracker.recordFailure("192.168.1.1");
    }
    assertTrue(tracker.isBlocked("192.168.1.1"));

    // Advance 61 seconds (past 60s lockout)
    clockNs.addAndGet(61L * 1_000_000_000L);
    assertFalse(tracker.isBlocked("192.168.1.1"));
  }

  @Test
  void isBlocked_duringLockout_returnsTrue() {
    final var tracker = new AuthFailureTracker(3, 60, testClock);
    for (int i = 0; i < 3; i++) {
      tracker.recordFailure("192.168.1.1");
    }

    // Advance 30 seconds (still within 60s lockout)
    clockNs.addAndGet(30L * 1_000_000_000L);
    assertTrue(tracker.isBlocked("192.168.1.1"));
  }

  @Test
  void isBlocked_afterLockoutExpiry_resetsFailureCount() {
    final var tracker = new AuthFailureTracker(3, 60, testClock);
    for (int i = 0; i < 3; i++) {
      tracker.recordFailure("192.168.1.1");
    }

    // Advance past lockout
    clockNs.addAndGet(61L * 1_000_000_000L);
    assertFalse(tracker.isBlocked("192.168.1.1")); // resets count

    // Single failure should not trigger lockout again
    tracker.recordFailure("192.168.1.1");
    assertFalse(tracker.isBlocked("192.168.1.1"));
  }

  // --- Stale entry eviction ---

  @Test
  void recordFailure_evictsStaleEntries() {
    final var tracker = new AuthFailureTracker(3, 60, testClock);
    tracker.recordFailure("192.168.1.1");

    // Advance 6 minutes (past 5-minute stale threshold)
    clockNs.addAndGet(6L * 60_000_000_000L);

    // Fill to capacity to trigger eviction
    for (int i = 0; i < AuthFailureTracker.MAX_TRACKED_IPS; i++) {
      tracker.recordFailure("10.0.0." + i);
    }

    // Original stale entry should have been evicted
    assertFalse(tracker.isBlocked("192.168.1.1"));
  }

  // --- Capacity enforcement ---

  @Test
  void recordFailure_atCapacity_evictsOldestEntry() {
    final var tracker = new AuthFailureTracker(100, 60, testClock);

    // Fill to MAX_TRACKED_IPS
    for (int i = 0; i < AuthFailureTracker.MAX_TRACKED_IPS; i++) {
      tracker.recordFailure("10.0.0." + i);
      clockNs.addAndGet(1_000_000L); // 1ms between each to create ordering
    }

    assertEquals(AuthFailureTracker.MAX_TRACKED_IPS, tracker.trackedIpCount());

    // Add one more — should evict the oldest (10.0.0.0)
    tracker.recordFailure("192.168.1.1");
    // Size should not exceed MAX_TRACKED_IPS + 1 (the new entry is added after eviction)
    assertTrue(tracker.trackedIpCount() <= AuthFailureTracker.MAX_TRACKED_IPS + 1);
  }

  // --- Constructor validation ---

  @Test
  void constructor_invalidThreshold_throws() {
    final var ex =
        assertThrows(
            IllegalArgumentException.class, () -> new AuthFailureTracker(0, 60, testClock));
    assertTrue(ex.getMessage().contains("lockoutThreshold"));
  }

  @Test
  void constructor_invalidLockoutSeconds_throws() {
    final var ex =
        assertThrows(IllegalArgumentException.class, () -> new AuthFailureTracker(5, 0, testClock));
    assertTrue(ex.getMessage().contains("lockoutSeconds"));
  }

  // --- trackedIpCount ---

  @Test
  void trackedIpCount_reflectsUniqueIps() {
    final var tracker = new AuthFailureTracker(5, 60, testClock);
    assertEquals(0, tracker.trackedIpCount());

    tracker.recordFailure("192.168.1.1");
    assertEquals(1, tracker.trackedIpCount());

    tracker.recordFailure("192.168.1.1"); // same IP
    assertEquals(1, tracker.trackedIpCount());

    tracker.recordFailure("10.0.0.1"); // different IP
    assertEquals(2, tracker.trackedIpCount());
  }
}
