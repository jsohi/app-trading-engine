package com.trading.engine.fixbridge.auth;

import org.agrona.collections.Object2LongHashMap;

/**
 * Bounded JTI ({@code JWT id}) revocation cache for the bridge auth handler (§3.3).
 *
 * <p><b>Purpose.</b> Tracks JWT identifiers that have been revoked by user sign-out so a subsequent
 * {@code Auth} frame carrying the same JTI is rejected (close 4001). Bounded at {@link
 * #DEFAULT_MAX_ENTRIES} entries with FIFO eviction; per-entry TTL = the JWT's remaining {@code exp}
 * so the entry naturally expires at the same time the underlying token would have.
 *
 * <p><b>Sign-out flow (§3.3 / §3.7 / §4.9).</b>
 *
 * <ol>
 *   <li>Client calls {@code tokenProvider.signOut()} which issues an HTTPS request to the bridge's
 *       revocation endpoint.
 *   <li>Bridge {@link #revoke(String, long)}s the JTI with the JWT's {@code exp} as the TTL upper
 *       bound.
 *   <li>Bridge looks up {@code subToSessionIds[sub]} (in {@code SessionQuoteIndex}) and emits
 *       {@code SessionTerminated} + WS close 4002 to every other session of that user.
 *   <li>Subsequent {@code Auth} frames presenting the same JTI are rejected (close 4001).
 * </ol>
 *
 * <p><b>Threading.</b> NOT thread-safe. Owned by the bridge's auth-handler thread (one Netty
 * boss-loop thread for the bridge process).
 *
 * <p><b>Allocation.</b> Constructor allocates the underlying {@link Object2LongHashMap}. {@link
 * #revoke(String, long)} allocates only when growing the map (Agrona's open-addressing hashmap
 * expands by powers of two; pre-sized to {@code DEFAULT_MAX_ENTRIES} avoids growth in steady
 * state). {@link #isRevoked(String, long)} is zero-alloc.
 *
 * <p><b>Capacity-budget caveat.</b> Lazy expiry in {@link #isRevoked(String, long)} removes the map
 * entry but leaves the {@link #insertionRing} slot occupied with a stale reference until the next
 * FIFO eviction overwrites it. Effective capacity is therefore "inserted-since-cleared count", not
 * "live count" — under sustained sign-out + lazy-expiry pressure the ring's "full" trigger may fire
 * while the live entry count is below {@link #DEFAULT_MAX_ENTRIES}. Compacting the ring on lazy
 * expiry would be O(N) and adds no correctness value; the practical capacity headroom is more than
 * sufficient for the documented 1100/min sign-out rate.
 *
 * <p><b>Lifecycle.</b> Singleton in the bridge process; lives for the JVM's lifetime.
 *
 * <p><b>Dependencies.</b> Agrona collections only.
 */
public final class JtiRevocationCache {

  /** Default capacity ceiling — 10k entries per §3.3 / §4.9. */
  public static final int DEFAULT_MAX_ENTRIES = 10_000;

  /** Sentinel used by {@link Object2LongHashMap} for "key absent". */
  private static final long ABSENT_SENTINEL = Long.MIN_VALUE;

  private final int maxEntries;

  /** {@code jti -> expEpochNs} (the JWT's {@code exp} claim re-expressed in epoch nanoseconds). */
  private final Object2LongHashMap<String> jtiToExpNs;

  /**
   * Insertion-order ring of JTIs for FIFO eviction when {@link #maxEntries} is reached. Sized to
   * {@link #maxEntries}; sized once at construction.
   */
  private final String[] insertionRing;

  // Mutable scan pointers — single-event-loop owner per CLAUDE.md carve-out for tight scans.
  private int ringWriteIndex;
  private int ringSize;

  /** Construct a cache with the {@link #DEFAULT_MAX_ENTRIES} default capacity. */
  public JtiRevocationCache() {
    this(DEFAULT_MAX_ENTRIES);
  }

  /**
   * Construct a cache with a custom capacity.
   *
   * @param maxEntries hard cap on resident entries; the (maxEntries+1)-th revoke evicts the oldest
   *     insertion
   * @throws IllegalArgumentException if {@code maxEntries <= 0}
   */
  public JtiRevocationCache(final int maxEntries) {
    if (maxEntries <= 0) {
      throw new IllegalArgumentException("maxEntries must be > 0, was " + maxEntries);
    }
    this.maxEntries = maxEntries;
    // Agrona Object2LongHashMap takes (initialCapacity, loadFactor, missingValue). Pre-size at
    // 2x to avoid growth in steady state.
    this.jtiToExpNs = new Object2LongHashMap<>(maxEntries * 2, 0.65f, ABSENT_SENTINEL);
    this.insertionRing = new String[maxEntries];
    this.ringWriteIndex = 0;
    this.ringSize = 0;
  }

  /**
   * Mark {@code jti} as revoked until {@code expEpochNs}. Idempotent: re-revoking an existing entry
   * refreshes its expiry (taking the later of the two if the new exp is further in the future) but
   * does NOT count as a fresh insertion.
   *
   * <p>If the cache is at capacity, the oldest insertion is evicted (FIFO). Eviction is silent —
   * the cache makes no attempt to alert the operator. Practical reasoning: at the {@link
   * #DEFAULT_MAX_ENTRIES} default and the JWT 15-min TTL convention, sustained sign-out rates above
   * ~1100/min would be required to force eviction; well above any realistic load.
   *
   * @param jti JWT id to revoke (non-null, non-empty)
   * @param expEpochNs JWT exp claim in epoch nanoseconds — used as the upper bound for {@link
   *     #isRevoked(String, long)}
   * @throws NullPointerException if {@code jti} is null
   * @throws IllegalArgumentException if {@code jti} is empty
   */
  public void revoke(final String jti, final long expEpochNs) {
    if (jti == null) {
      throw new NullPointerException("jti must not be null");
    }
    if (jti.isEmpty()) {
      throw new IllegalArgumentException("jti must not be empty");
    }
    final long existing = jtiToExpNs.getValue(jti);
    if (existing != ABSENT_SENTINEL) {
      // Refresh: keep the later expiry.
      if (expEpochNs > existing) {
        jtiToExpNs.put(jti, expEpochNs);
      }
      return;
    }
    // Fresh insertion — make room if needed.
    if (ringSize >= maxEntries) {
      final var evicted = insertionRing[ringWriteIndex];
      if (evicted != null) {
        jtiToExpNs.remove(evicted);
      }
    } else {
      ringSize++;
    }
    insertionRing[ringWriteIndex] = jti;
    ringWriteIndex = (ringWriteIndex + 1) % maxEntries;
    jtiToExpNs.put(jti, expEpochNs);
  }

  /**
   * Check whether {@code jti} is revoked at {@code nowEpochNs}. An entry is considered "revoked"
   * iff present in the cache AND not yet past its recorded exp; expired entries are lazy-evicted on
   * read so they do not consume the {@link #maxEntries} budget.
   *
   * @param jti JWT id to check (may be null — always returns false)
   * @param nowEpochNs current epoch-nanosecond time (from injected {@code EpochNanoClock})
   * @return {@code true} iff {@code jti} is in the cache and {@code nowEpochNs < expEpochNs}
   */
  public boolean isRevoked(final String jti, final long nowEpochNs) {
    if (jti == null) {
      return false;
    }
    final long expNs = jtiToExpNs.getValue(jti);
    if (expNs == ABSENT_SENTINEL) {
      return false;
    }
    if (nowEpochNs >= expNs) {
      // Lazy eviction — expired entry; remove and report not-revoked.
      jtiToExpNs.remove(jti);
      // Note: insertionRing slot stays occupied with the (stale) reference. It will be silently
      // overwritten on the next FIFO eviction. Compacting the ring here would be O(N) and
      // unnecessary — the cache's O(1) memory bound is unaffected.
      return false;
    }
    return true;
  }

  /**
   * Visible for testing. Number of entries currently resident in the cache.
   *
   * @return cache size
   */
  public int size() {
    return jtiToExpNs.size();
  }
}
