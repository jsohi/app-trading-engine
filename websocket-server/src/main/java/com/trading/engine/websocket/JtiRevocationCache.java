package com.trading.engine.websocket;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.agrona.concurrent.NanoClock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Tracks revoked JWT {@code jti} (JWT ID) claims to prevent token replay attacks within the
 * revocation window.
 *
 * <p>When a client authenticates or re-authenticates, the server revokes the old JTI so it cannot
 * be reused. The cache stores full JTI strings (not hashes — 32-bit {@code String.hashCode()} has
 * ~1.2% collision probability at 10K entries, which is exploitable for revocation bypass).
 *
 * <p><b>TTL.</b> Entries expire after {@code revocationTtlMinutes + 2} minutes (17 minutes for a
 * 15-minute token lifetime). The 2-minute extension covers clock skew between the IdP and the
 * WebSocket server.
 *
 * <p><b>Fail-safe.</b> If the cache reaches {@code maxCapacity} after evicting expired entries, all
 * subsequent {@link #isRevoked(String)} calls return {@code true} (reject all tokens). This
 * prevents a token flood from bypassing revocation.
 *
 * <p><b>Threading.</b> Thread-safe via {@link ConcurrentHashMap}. Called from Netty event loop
 * threads (one per channel for authentication). The fail-safe flag is volatile.
 *
 * <p><b>Allocation.</b> One {@link String} key + one {@link Long} boxed value per revocation. This
 * is acceptable because revocation happens only on auth/re-auth (cold path, not per-message).
 *
 * @see <a href="docs/websocket-architecture.md">WebSocket Architecture — Section 4</a>
 */
public final class JtiRevocationCache {

  private static final Logger LOG = LogManager.getLogger(JtiRevocationCache.class);

  /** Number of nanoseconds per minute. */
  private static final long NANOS_PER_MINUTE = 60_000_000_000L;

  /**
   * Extra TTL minutes beyond the configured revocationTtlMinutes to cover clock skew between the
   * IdP and the WebSocket server (requires PTP/chrony &lt;1s drift).
   */
  private static final int CLOCK_SKEW_EXTENSION_MINUTES = 2;

  // ConcurrentHashMap<String, Long> boxes long→Long on every put(). This is acceptable because
  // revoke() is cold-path (once per auth, not per-message). Agrona Object2LongHashMap is not
  // thread-safe, so ConcurrentHashMap is required here for multi-event-loop access.
  private final ConcurrentHashMap<String, Long> entries;
  private final int maxCapacity;
  private final long ttlNanos;
  private final NanoClock nanoClock;

  /** Fail-safe flag — when true, {@link #isRevoked(String)} returns true for ALL tokens. */
  private volatile boolean failSafe;

  /**
   * Cooldown for fail-safe recovery attempts. Rate-limits the O(N) evictExpired scan in isRevoked
   * to at most once per second, preventing CPU exhaustion under auth floods.
   */
  private volatile long lastFailSafeRecoveryNs;

  /**
   * Create a new JTI revocation cache.
   *
   * @param maxCapacity maximum number of revoked JTI entries (from config.maxRevokedJtis())
   * @param revocationTtlMinutes TTL in minutes for revoked entries (from config); internally
   *     extended by {@value CLOCK_SKEW_EXTENSION_MINUTES} minutes
   * @param nanoClock monotonic clock for TTL tracking (injectable for testing)
   */
  public JtiRevocationCache(
      final int maxCapacity, final int revocationTtlMinutes, final NanoClock nanoClock) {
    if (maxCapacity <= 0) {
      throw new IllegalArgumentException("maxCapacity must be > 0, got: " + maxCapacity);
    }
    if (revocationTtlMinutes <= 0) {
      throw new IllegalArgumentException(
          "revocationTtlMinutes must be > 0, got: " + revocationTtlMinutes);
    }
    if (revocationTtlMinutes > 525_600) { // 1 year in minutes — sanity cap
      throw new IllegalArgumentException(
          "revocationTtlMinutes unreasonably large: " + revocationTtlMinutes);
    }
    this.maxCapacity = maxCapacity;
    // Cast to long before multiplication to avoid int overflow on extreme values
    this.ttlNanos = ((long) revocationTtlMinutes + CLOCK_SKEW_EXTENSION_MINUTES) * NANOS_PER_MINUTE;
    this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
    this.entries = new ConcurrentHashMap<>();
  }

  /**
   * Check whether a JTI has been revoked and has not yet expired.
   *
   * <p>If the fail-safe is active (cache at capacity with no expired entries), returns {@code true}
   * for ALL tokens — rejecting new authentications until entries expire.
   *
   * @param jti the JWT ID claim (full string, not a hash)
   * @return true if the JTI is revoked (or fail-safe is active)
   */
  public boolean isRevoked(final String jti) {
    if (jti == null) {
      return false;
    }

    // Fail-safe recovery: when active, attempt to evict expired entries. If capacity is freed,
    // clear fail-safe and resume normal operation. Without this, fail-safe would be permanently
    // sticky because isRevoked() blocks auth, which blocks revoke(), which is the only other
    // place that clears fail-safe.
    //
    // Thread-safety note: concurrent isRevoked() calls may both enter this block, both evict
    // (idempotent via ConcurrentHashMap iterator), and both set failSafe = false. This is benign
    // — the worst case is a redundant eviction pass, not a correctness issue.
    if (failSafe) {
      final long nowNs = nanoClock.nanoTime();
      // Rate-limit recovery scans to at most once per second to prevent O(N) CPU exhaustion
      // under auth floods when fail-safe is active.
      if (nowNs - lastFailSafeRecoveryNs < 1_000_000_000L) {
        return true; // cooldown active — reject without scanning
      }
      lastFailSafeRecoveryNs = nowNs;
      evictExpired(nowNs);
      if (entries.size() < maxCapacity) {
        failSafe = false;
        LOG.info("JTI revocation cache fail-safe cleared after expired entry eviction");
      } else {
        return true; // still at capacity — reject all
      }
    }

    final var insertionNs = entries.get(jti);
    if (insertionNs == null) {
      return false;
    }

    // Check TTL — expired entries are lazily removed
    final long nowNs = nanoClock.nanoTime();
    if (nowNs - insertionNs > ttlNanos) {
      entries.remove(jti, insertionNs); // CAS remove to avoid ABA
      return false;
    }
    return true;
  }

  /**
   * Revoke a JTI. The entry expires after the configured TTL + clock skew extension.
   *
   * <p>If the cache is at capacity, expired entries are evicted first. If still at capacity after
   * eviction, the fail-safe is activated (all future {@link #isRevoked} calls return true).
   *
   * @param jti the JWT ID claim to revoke (full string)
   */
  public void revoke(final String jti) {
    if (jti == null) {
      return;
    }

    final long nowNs = nanoClock.nanoTime();

    // Evict expired entries if at or near capacity
    if (entries.size() >= maxCapacity) {
      evictExpired(nowNs);

      // Fail-safe: if still at capacity after eviction, activate fail-safe but STILL add the JTI.
      // Without adding, if fail-safe later clears (entries expire), this JTI would be missing from
      // the cache — a revocation bypass (Gemini review finding G10).
      if (entries.size() >= maxCapacity) {
        failSafe = true;
        LOG.warn(
            "JTI revocation cache at capacity ({}) after eviction — fail-safe activated, "
                + "rejecting all new tokens until entries expire",
            maxCapacity);
        // Still add — the map may briefly exceed maxCapacity by 1, which is acceptable.
        // The alternative (not adding) creates a revocation bypass.
        entries.put(jti, nowNs);
        return;
      }
      failSafe = false; // clear fail-safe if eviction freed space
    }

    entries.put(jti, nowNs);
  }

  /**
   * @return the current number of entries in the cache (including potentially expired entries that
   *     have not yet been lazily evicted)
   */
  public int size() {
    return entries.size();
  }

  /**
   * @return true if the fail-safe is currently active (rejecting all tokens)
   */
  public boolean isFailSafeActive() {
    return failSafe;
  }

  /**
   * Evict all expired entries from the cache. Used by both {@link #revoke} (capacity management)
   * and {@link #isRevoked} (fail-safe recovery).
   *
   * @param nowNs current monotonic time in nanoseconds
   */
  private void evictExpired(final long nowNs) {
    int evicted = 0;
    final var it = entries.entrySet().iterator();
    while (it.hasNext()) {
      final var entry = it.next();
      if (nowNs - entry.getValue() > ttlNanos) {
        it.remove();
        evicted++;
      }
    }
    if (evicted > 0) {
      LOG.debug("JTI revocation cache evicted {} expired entries", evicted);
    }
  }
}
