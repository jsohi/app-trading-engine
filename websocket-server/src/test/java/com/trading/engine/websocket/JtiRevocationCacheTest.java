package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicLong;
import org.agrona.concurrent.NanoClock;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link JtiRevocationCache} -- verifies revocation, TTL expiry, capacity overflow,
 * fail-safe behavior, and full JTI string keying (no hash collisions).
 */
final class JtiRevocationCacheTest {

  /** Test clock that can be advanced programmatically. */
  private final AtomicLong clockNs = new AtomicLong(1_000_000_000_000L);

  private final NanoClock testClock = clockNs::get;

  // --- Basic revocation ---

  @Test
  void isRevoked_newCache_returnsFalse() {
    final var cache = new JtiRevocationCache(1000, 15, testClock);
    assertFalse(cache.isRevoked("jti-001"));
  }

  @Test
  void revoke_thenIsRevoked_returnsTrue() {
    final var cache = new JtiRevocationCache(1000, 15, testClock);
    cache.revoke("jti-001");

    assertTrue(cache.isRevoked("jti-001"));
  }

  @Test
  void isRevoked_differentJti_returnsFalse() {
    final var cache = new JtiRevocationCache(1000, 15, testClock);
    cache.revoke("jti-001");

    assertFalse(cache.isRevoked("jti-002"));
  }

  @Test
  void isRevoked_nullJti_returnsFalse() {
    final var cache = new JtiRevocationCache(1000, 15, testClock);
    assertFalse(cache.isRevoked(null));
  }

  @Test
  void revoke_nullJti_noOp() {
    final var cache = new JtiRevocationCache(1000, 15, testClock);
    cache.revoke(null);
    assertEquals(0, cache.size());
  }

  // --- TTL expiry (17 minutes = 15 + 2 clock skew extension) ---

  @Test
  void isRevoked_beforeTtlExpiry_returnsTrue() {
    final var cache = new JtiRevocationCache(1000, 15, testClock);
    cache.revoke("jti-001");

    // Advance 16 minutes (still within 17-minute TTL)
    clockNs.addAndGet(16L * 60_000_000_000L);
    assertTrue(cache.isRevoked("jti-001"));
  }

  @Test
  void isRevoked_afterTtlExpiry_returnsFalse() {
    final var cache = new JtiRevocationCache(1000, 15, testClock);
    cache.revoke("jti-001");

    // Advance 18 minutes (past 17-minute TTL)
    clockNs.addAndGet(18L * 60_000_000_000L);
    assertFalse(cache.isRevoked("jti-001"));
  }

  @Test
  void isRevoked_exactlyAtTtlBoundary_returnsTrue() {
    final var cache = new JtiRevocationCache(1000, 15, testClock);
    cache.revoke("jti-001");

    // Advance exactly 17 minutes (at boundary, not past)
    clockNs.addAndGet(17L * 60_000_000_000L);
    assertTrue(cache.isRevoked("jti-001"));
  }

  // --- Capacity and fail-safe ---

  @Test
  void revoke_atCapacity_evictsExpiredEntries() {
    final var cache = new JtiRevocationCache(3, 15, testClock);
    cache.revoke("jti-001");
    cache.revoke("jti-002");
    cache.revoke("jti-003");
    assertEquals(3, cache.size());

    // Advance past TTL so all entries expire
    clockNs.addAndGet(18L * 60_000_000_000L);

    // New revocation should evict expired entries
    cache.revoke("jti-004");
    assertEquals(1, cache.size()); // only jti-004 remains
    assertTrue(cache.isRevoked("jti-004"));
    assertFalse(cache.isRevoked("jti-001")); // expired and evicted
  }

  @Test
  void revoke_atCapacityAllFresh_activatesFailSafe() {
    final var cache = new JtiRevocationCache(3, 15, testClock);
    cache.revoke("jti-001");
    cache.revoke("jti-002");
    cache.revoke("jti-003");

    assertFalse(cache.isFailSafeActive());

    // Attempt to revoke 4th without any expiry — fail-safe activates
    cache.revoke("jti-004");
    assertTrue(cache.isFailSafeActive());

    // All isRevoked calls return true during fail-safe
    assertTrue(cache.isRevoked("jti-never-revoked"));
    assertTrue(cache.isRevoked("any-jti"));
  }

  @Test
  void revoke_failSafeDeactivates_afterEviction() {
    final var cache = new JtiRevocationCache(3, 15, testClock);
    cache.revoke("jti-001");
    cache.revoke("jti-002");
    cache.revoke("jti-003");
    cache.revoke("jti-004"); // activates fail-safe
    assertTrue(cache.isFailSafeActive());

    // Advance past TTL
    clockNs.addAndGet(18L * 60_000_000_000L);

    // New revocation should evict expired entries and clear fail-safe
    cache.revoke("jti-005");
    assertFalse(cache.isFailSafeActive());
    assertTrue(cache.isRevoked("jti-005"));
  }

  @Test
  void isRevoked_failSafeActive_autoRecoversAfterTtlExpiry() {
    final var cache = new JtiRevocationCache(3, 15, testClock);
    cache.revoke("jti-001");
    cache.revoke("jti-002");
    cache.revoke("jti-003");
    cache.revoke("jti-004"); // activates fail-safe
    assertTrue(cache.isFailSafeActive());

    // Advance past TTL — all entries should be expired
    clockNs.addAndGet(18L * 60_000_000_000L);

    // isRevoked alone (without calling revoke) should auto-recover from fail-safe
    assertFalse(cache.isRevoked("new-jti"));
    assertFalse(cache.isFailSafeActive());
  }

  @Test
  void isRevoked_failSafeCooldown_skipsEvictionWithin1Second() {
    final var cache = new JtiRevocationCache(3, 15, testClock);
    cache.revoke("jti-001");
    cache.revoke("jti-002");
    cache.revoke("jti-003");
    cache.revoke("jti-004"); // activates fail-safe
    assertTrue(cache.isFailSafeActive());

    // Advance past TTL
    clockNs.addAndGet(18L * 60_000_000_000L);

    // First isRevoked triggers recovery scan — should clear fail-safe
    assertFalse(cache.isRevoked("check-1"));
    assertFalse(cache.isFailSafeActive());

    // Re-fill to trigger fail-safe again
    cache.revoke("jti-A");
    cache.revoke("jti-B");
    cache.revoke("jti-C");
    cache.revoke("jti-D"); // fail-safe again
    assertTrue(cache.isFailSafeActive());

    // Advance only 500ms (within 1s cooldown)
    clockNs.addAndGet(500_000_000L);

    // isRevoked should return true without running eviction (cooldown active)
    assertTrue(cache.isRevoked("check-2"));
    assertTrue(cache.isFailSafeActive()); // still active — cooldown prevented scan

    // Advance past cooldown (1.5s total)
    clockNs.addAndGet(18L * 60_000_000_000L); // also past TTL for entries

    // Now isRevoked should trigger recovery
    assertFalse(cache.isRevoked("check-3"));
    assertFalse(cache.isFailSafeActive());
  }

  @Test
  void revoke_duplicateJti_updatesTimestamp() {
    final var cache = new JtiRevocationCache(1000, 15, testClock);
    cache.revoke("jti-001");

    // Advance 10 minutes
    clockNs.addAndGet(10L * 60_000_000_000L);
    cache.revoke("jti-001"); // re-revoke with new timestamp

    // Advance 10 more minutes (20 total from first, 10 from second)
    clockNs.addAndGet(10L * 60_000_000_000L);

    // Should still be revoked (second revocation refreshed the timestamp)
    assertTrue(cache.isRevoked("jti-001"));
  }

  // --- Size tracking ---

  @Test
  void size_afterRevokeAndExpiry_reflectsActiveEntries() {
    final var cache = new JtiRevocationCache(1000, 15, testClock);
    assertEquals(0, cache.size());

    cache.revoke("jti-001");
    cache.revoke("jti-002");
    assertEquals(2, cache.size());

    // Note: size includes expired entries until lazy eviction
    clockNs.addAndGet(18L * 60_000_000_000L);
    // Entries still in map until isRevoked or revoke triggers lazy eviction
    assertEquals(2, cache.size());

    // isRevoked triggers lazy removal
    assertFalse(cache.isRevoked("jti-001"));
    assertEquals(1, cache.size()); // only jti-001 removed (lazy per-entry)
  }

  // --- Constructor validation ---

  @Test
  void constructor_invalidCapacity_throws() {
    final var ex =
        assertThrows(
            IllegalArgumentException.class, () -> new JtiRevocationCache(0, 15, testClock));
    assertTrue(ex.getMessage().contains("maxCapacity"));
  }

  @Test
  void constructor_invalidTtl_throws() {
    final var ex =
        assertThrows(
            IllegalArgumentException.class, () -> new JtiRevocationCache(1000, 0, testClock));
    assertTrue(ex.getMessage().contains("revocationTtlMinutes"));
  }
}
