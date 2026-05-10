package com.trading.engine.fixbridge.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link JtiRevocationCache}.
 *
 * <p><b>Coverage.</b> Construction validation, revoke semantics (fresh insertion, idempotent
 * refresh with expiry update, NPE/IAE guards), FIFO eviction at capacity, and lazy-expiry
 * behaviour in {@link JtiRevocationCache#isRevoked(String, long)}.
 *
 * <p><b>Threading.</b> Single-threaded; {@link JtiRevocationCache} is not thread-safe per its
 * contract.
 *
 * <p><b>Design note.</b> The lazy-eviction test ({@code
 * isRevoked_lazyEvictedDoesNotConsumeBudget}) verifies that after a lazy-eviction clears the
 * underlying map entry, the {@code insertionRing} slot remains occupied with a stale reference.
 * The ring slot is overwritten only on the next FIFO-eviction cycle, so a fresh revoke at
 * capacity still triggers FIFO eviction even though the live map size is below {@code maxEntries}.
 * Tests assert what the code actually does, not what would be intuitive from the public contract
 * summary alone.
 */
final class JtiRevocationCacheTest {

  // ---------------------------------------------------------------------------
  // Construction
  // ---------------------------------------------------------------------------

  @Test
  void ctor_default_usesDefaultMaxEntries() {
    final var cache = new JtiRevocationCache();
    assertEquals(0, cache.size(), "fresh cache must be empty");
  }

  @Test
  void ctor_zeroMaxEntries_throwsIAE() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new JtiRevocationCache(0),
        "maxEntries=0 must throw IAE");
  }

  @Test
  void ctor_negativeMaxEntries_throwsIAE() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new JtiRevocationCache(-1),
        "maxEntries=-1 must throw IAE");
  }

  // ---------------------------------------------------------------------------
  // revoke — basic semantics
  // ---------------------------------------------------------------------------

  @Test
  void revoke_freshJti_isRevokedAtAnyTimeBeforeExp() {
    final var cache = new JtiRevocationCache();
    final long expNs = 1_000_000L;
    cache.revoke("jti-A", expNs);

    assertTrue(cache.isRevoked("jti-A", 0L), "should be revoked at time 0");
    assertTrue(cache.isRevoked("jti-A", expNs - 1), "should be revoked one ns before exp");
  }

  @Test
  void revoke_nullJti_throwsNPE() {
    final var cache = new JtiRevocationCache();
    assertThrows(NullPointerException.class, () -> cache.revoke(null, 1_000L));
  }

  @Test
  void revoke_emptyJti_throwsIAE() {
    final var cache = new JtiRevocationCache();
    assertThrows(IllegalArgumentException.class, () -> cache.revoke("", 1_000L));
  }

  @Test
  void revoke_sameJtiTwiceLaterExp_keepsLaterExpiry() {
    // Revoke jti1 with exp=1000, then again with exp=5000 (later).
    // isRevoked at now=4500 must return true — the later expiry was retained.
    final var cache = new JtiRevocationCache();
    cache.revoke("jti1", 1_000L);
    cache.revoke("jti1", 5_000L);

    assertTrue(
        cache.isRevoked("jti1", 4_500L),
        "later expiry (5000) should be kept; entry still valid at 4500");
    assertEquals(1, cache.size(), "refresh must not increase size");
  }

  @Test
  void revoke_sameJtiTwiceEarlierExp_keepsExistingExpiry() {
    // Revoke jti1 with exp=5000, then again with exp=1000 (earlier).
    // isRevoked at now=4500 must still return true — the existing later expiry was kept.
    final var cache = new JtiRevocationCache();
    cache.revoke("jti1", 5_000L);
    cache.revoke("jti1", 1_000L);

    assertTrue(
        cache.isRevoked("jti1", 4_500L),
        "existing expiry (5000) should be kept; entry still valid at 4500");
    assertEquals(1, cache.size(), "refresh must not increase size");
  }

  @Test
  void revoke_idempotent_doesNotConsumeBudget() {
    // Four revokes of the same JTI count as one insertion.
    final var cache = new JtiRevocationCache();
    cache.revoke("jti1", 1_000_000L);
    cache.revoke("jti1", 1_000_000L);
    cache.revoke("jti1", 1_000_000L);
    cache.revoke("jti1", 1_000_000L);

    assertEquals(1, cache.size(), "idempotent revoking same JTI must not grow size beyond 1");
  }

  // ---------------------------------------------------------------------------
  // FIFO eviction at capacity
  // ---------------------------------------------------------------------------

  @Test
  void revoke_atCapacityFreshJti_evictsOldest() {
    // capacity=3: revoke jti1, jti2, jti3 (full), then jti4 (triggers FIFO evict of jti1).
    final var cache = new JtiRevocationCache(3);
    final long farFuture = Long.MAX_VALUE;
    cache.revoke("jti1", farFuture);
    cache.revoke("jti2", farFuture);
    cache.revoke("jti3", farFuture);
    cache.revoke("jti4", farFuture);

    assertFalse(cache.isRevoked("jti1", 0L), "jti1 must have been evicted (oldest)");
    assertTrue(cache.isRevoked("jti2", 0L), "jti2 must still be revoked");
    assertTrue(cache.isRevoked("jti3", 0L), "jti3 must still be revoked");
    assertTrue(cache.isRevoked("jti4", 0L), "jti4 must have been inserted");
  }

  @Test
  void revoke_evictedReturnsFalseFromIsRevoked() {
    // Verify the evicted entry reads as absent, not stale.
    final var cache = new JtiRevocationCache(2);
    cache.revoke("jti1", Long.MAX_VALUE);
    cache.revoke("jti2", Long.MAX_VALUE);
    cache.revoke("jti3", Long.MAX_VALUE); // jti1 evicted

    assertFalse(cache.isRevoked("jti1", 0L), "evicted jti1 must return false from isRevoked");
  }

  @Test
  void revoke_atCapacityOverwriteSameJti_doesNotEvict() {
    // capacity=3; revoke jti1, jti2, jti3, then refresh jti1 (same key — not a fresh insert).
    // All three entries must survive; no FIFO eviction occurs.
    final var cache = new JtiRevocationCache(3);
    final long farFuture = Long.MAX_VALUE;
    cache.revoke("jti1", farFuture);
    cache.revoke("jti2", farFuture);
    cache.revoke("jti3", farFuture);
    cache.revoke("jti1", farFuture); // refresh — must NOT evict anyone

    assertTrue(cache.isRevoked("jti1", 0L), "jti1 must still be revoked after refresh");
    assertTrue(cache.isRevoked("jti2", 0L), "jti2 must still be revoked");
    assertTrue(cache.isRevoked("jti3", 0L), "jti3 must still be revoked");
    assertEquals(3, cache.size(), "refresh must not change size");
  }

  // ---------------------------------------------------------------------------
  // isRevoked + lazy expiry
  // ---------------------------------------------------------------------------

  @Test
  void isRevoked_nullJti_returnsFalse() {
    final var cache = new JtiRevocationCache();
    assertFalse(cache.isRevoked(null, 0L));
  }

  @Test
  void isRevoked_unknownJti_returnsFalse() {
    final var cache = new JtiRevocationCache();
    assertFalse(cache.isRevoked("never-revoked", 0L));
  }

  @Test
  void isRevoked_atExactExp_returnsFalseAndEvicts() {
    // The check is nowEpochNs >= expNs — at exact exp it is NOT considered revoked.
    final var cache = new JtiRevocationCache();
    cache.revoke("jti1", 1_000L);

    assertFalse(cache.isRevoked("jti1", 1_000L), "at exact exp the entry must NOT be revoked");
    assertEquals(0, cache.size(), "lazy eviction at exact exp must remove the entry from the map");
  }

  @Test
  void isRevoked_pastExp_returnsFalseAndEvicts() {
    // nowEpochNs > expNs — entry is expired; lazy eviction fires and size drops.
    final var cache = new JtiRevocationCache();
    cache.revoke("jti1", 1_000L);
    cache.revoke("jti2", 1_000_000L);

    assertEquals(2, cache.size(), "before expiry check");
    assertFalse(cache.isRevoked("jti1", 2_000L), "past exp must return false");
    assertEquals(1, cache.size(), "expired entry must be removed from map");
  }

  @Test
  void isRevoked_beforeExp_returnsTrue() {
    final var cache = new JtiRevocationCache();
    cache.revoke("jti1", 1_000L);

    assertTrue(cache.isRevoked("jti1", 999L), "one ns before exp must return true");
  }

  @Test
  void isRevoked_lazyEvictedDoesNotConsumeBudget() {
    // capacity=2.
    // Step 1: revoke jti1@exp=100, jti2@exp=200 — fills capacity.
    // Step 2: isRevoked(jti1, now=150) → false (expired) + lazy-evicts jti1 from map.
    //         Map size drops to 1, but insertionRing slot for jti1 is NOT compacted.
    // Step 3: revoke jti3 — ring is still "full" from the ring's perspective (ringSize==2),
    //         so FIFO eviction fires and overwrites the ring slot currently holding "jti1"
    //         (which is already gone from the map — jtiToExpNs.remove is a no-op).
    //         jti3 is inserted; map size becomes 2 (jti2 + jti3).
    // Result: size() == 2, jti2 and jti3 are both revoked.
    final var cache = new JtiRevocationCache(2);
    cache.revoke("jti1", 100L);
    cache.revoke("jti2", 200L);

    // Lazy-evict jti1.
    assertFalse(cache.isRevoked("jti1", 150L), "jti1 must be expired at now=150");
    // Map has 1 entry (jti2) but ring still has ringSize==2 occupying both slots.

    // Insert jti3 — FIFO eviction of the oldest ring slot (jti1 stale ref) fires.
    cache.revoke("jti3", 300L);

    // After insertion: map holds jti2 and jti3.
    assertEquals(2, cache.size(), "map should contain jti2 and jti3");
    assertTrue(cache.isRevoked("jti2", 0L), "jti2 must still be revoked");
    assertTrue(cache.isRevoked("jti3", 0L), "jti3 must be revoked");
  }
}
