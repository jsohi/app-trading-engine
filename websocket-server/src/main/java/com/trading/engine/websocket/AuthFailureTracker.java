package com.trading.engine.websocket;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.agrona.concurrent.NanoClock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Per-IP authentication failure rate limiter for DDoS mitigation against brute-force JWT attacks.
 *
 * <p>Tracks authentication failures per remote IP address and applies a fixed-duration lockout when
 * the failure threshold is exceeded. This prevents an attacker from consuming RSA verification CPU
 * and JWKS fetch bandwidth by sending a high rate of invalid JWTs.
 *
 * <p><b>Lockout policy.</b> After {@code lockoutThreshold} failures within the tracking window, the
 * IP is locked out for {@code lockoutSeconds} seconds. During lockout, {@link #isBlocked(String)}
 * returns {@code true} and the auth handler rejects the connection without performing JWT
 * validation.
 *
 * <p><b>Capacity.</b> Maximum {@value MAX_TRACKED_IPS} tracked IPs. When at capacity, the oldest
 * entry (by last failure time) is evicted to prevent unbounded memory growth from botnets with
 * millions of unique IPs.
 *
 * <p><b>Threading.</b> Thread-safe via {@link ConcurrentHashMap} and {@link AtomicInteger}. Called
 * from multiple Netty event loop threads (one per channel).
 *
 * <p><b>Allocation.</b> One {@link FailureRecord} per unique IP with failures. Acceptable — auth
 * failures are infrequent in normal operation.
 *
 * @see <a href="docs/websocket-architecture.md">WebSocket Architecture — Section 4</a>
 */
public final class AuthFailureTracker {

  private static final Logger LOG = LogManager.getLogger(AuthFailureTracker.class);

  /**
   * Maximum number of tracked IPs. Prevents unbounded memory growth from distributed attacks. 16384
   * entries * ~100 bytes per entry = ~1.6MB worst case.
   */
  static final int MAX_TRACKED_IPS = 16_384;

  /** Stale entry threshold: entries older than 5 minutes with no new failures are evicted. */
  private static final long STALE_THRESHOLD_NS = 5L * 60_000_000_000L;

  private final ConcurrentHashMap<String, FailureRecord> entries;
  private final int lockoutThreshold;
  private final long lockoutNanos;
  private final NanoClock nanoClock;

  /**
   * Create a new auth failure tracker.
   *
   * @param lockoutThreshold number of failures before lockout (from config)
   * @param lockoutSeconds duration of lockout in seconds (from config)
   * @param nanoClock monotonic clock (injectable for testing)
   */
  public AuthFailureTracker(
      final int lockoutThreshold, final int lockoutSeconds, final NanoClock nanoClock) {
    if (lockoutThreshold <= 0) {
      throw new IllegalArgumentException("lockoutThreshold must be > 0, got: " + lockoutThreshold);
    }
    if (lockoutSeconds <= 0) {
      throw new IllegalArgumentException("lockoutSeconds must be > 0, got: " + lockoutSeconds);
    }
    this.lockoutThreshold = lockoutThreshold;
    this.lockoutNanos = lockoutSeconds * 1_000_000_000L;
    this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
    this.entries = new ConcurrentHashMap<>();
  }

  /**
   * Check whether an IP is currently blocked due to excessive auth failures.
   *
   * @param ip the remote IP address
   * @return true if the IP is in lockout period
   */
  public boolean isBlocked(final String ip) {
    if (ip == null) {
      return false;
    }
    final var record = entries.get(ip);
    if (record == null) {
      return false;
    }

    final long nowNs = nanoClock.nanoTime();
    if (record.lockoutUntilNs > 0) {
      if (nowNs < record.lockoutUntilNs) {
        return true; // still in lockout period
      }
      // Lockout expired — reset count for fresh tracking.
      // Thread-safety note: concurrent isBlocked()+recordFailure() on the same FailureRecord is a
      // benign race. Worst case: one extra failure before re-lockout. This is acceptable for a
      // rate limiter (secondary defense behind Netty connection rate limiting). Full atomicity
      // would require a lock or AtomicReference<State>, which is overkill for this cold path.
      record.failureCount.set(0);
      record.lockoutUntilNs = 0;
    }
    return false;
  }

  /**
   * Record an authentication failure for an IP address. If the failure count exceeds the threshold,
   * the IP is locked out.
   *
   * @param ip the remote IP address
   */
  public void recordFailure(final String ip) {
    if (ip == null) {
      return;
    }

    final long nowNs = nanoClock.nanoTime();

    // Evict stale entries and enforce hard capacity before adding.
    // Three-level eviction: (1) stale entries, (2) oldest non-locked entry, (3) oldest any entry.
    // Level 3 prevents unbounded memory growth when all entries are locked out (DDoS scenario).
    if (entries.size() >= MAX_TRACKED_IPS) {
      evictStaleEntries(nowNs);
      if (entries.size() >= MAX_TRACKED_IPS) {
        if (!evictOldestUnlockedEntry(nowNs)) {
          evictOldestEntry(); // last resort: evict oldest even if locked out
        }
      }
    }

    final var record = entries.computeIfAbsent(ip, k -> new FailureRecord());
    record.lastFailureNs = nowNs;
    final int count = record.failureCount.incrementAndGet();

    // Non-atomic check-then-act: two threads may both see lockoutUntilNs == 0 and both write.
    // Benign: both write nearly identical values (nowNs + lockoutNanos), so the second write
    // merely extends the lockout by a few nanoseconds. Not worth a lock for this cold path.
    if (count >= lockoutThreshold && record.lockoutUntilNs == 0) {
      record.lockoutUntilNs = nowNs + lockoutNanos;
      LOG.warn("Auth failure lockout activated for IP {} ({} failures)", ip, count);
    }
  }

  /**
   * @return the current number of tracked IPs
   */
  public int trackedIpCount() {
    return entries.size();
  }

  /** Remove entries that have not had a failure in the last 5 minutes. */
  private void evictStaleEntries(final long nowNs) {
    final var it = entries.entrySet().iterator();
    while (it.hasNext()) {
      final var entry = it.next();
      if (nowNs - entry.getValue().lastFailureNs > STALE_THRESHOLD_NS) {
        it.remove();
      }
    }
  }

  /**
   * Evict the oldest non-locked-out entry (preferred eviction). Entries currently in active lockout
   * are skipped to preserve DDoS protection for known-bad IPs.
   *
   * @param nowNs current monotonic time
   * @return true if an entry was evicted, false if all entries are locked out
   */
  private boolean evictOldestUnlockedEntry(final long nowNs) {
    String oldestIp = null;
    long oldestNs = Long.MAX_VALUE;

    for (final var entry : entries.entrySet()) {
      final var rec = entry.getValue();
      if (rec.lockoutUntilNs > 0 && nowNs < rec.lockoutUntilNs) {
        continue; // skip locked-out IPs
      }
      if (rec.lastFailureNs < oldestNs) {
        oldestNs = rec.lastFailureNs;
        oldestIp = entry.getKey();
      }
    }

    if (oldestIp != null) {
      entries.remove(oldestIp);
      return true;
    }
    return false;
  }

  /**
   * Last-resort eviction: remove the oldest entry regardless of lockout status. Called only when
   * all entries are locked out and the map would otherwise grow unboundedly. This sacrifices one
   * locked- out IP's tracking to prevent memory exhaustion under sustained distributed DDoS.
   */
  private void evictOldestEntry() {
    String oldestIp = null;
    long oldestNs = Long.MAX_VALUE;

    for (final var entry : entries.entrySet()) {
      if (entry.getValue().lastFailureNs < oldestNs) {
        oldestNs = entry.getValue().lastFailureNs;
        oldestIp = entry.getKey();
      }
    }

    if (oldestIp != null) {
      entries.remove(oldestIp);
    }
  }

  /** Mutable tracking record for a single IP address. */
  static final class FailureRecord {
    final AtomicInteger failureCount = new AtomicInteger(0);
    volatile long lastFailureNs;
    volatile long lockoutUntilNs;
  }
}
