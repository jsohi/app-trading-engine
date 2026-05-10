package com.trading.engine.fixbridge.auth;

import java.util.HashMap;
import java.util.Map;

/**
 * Test-only in-memory implementation of {@link JtiReplayCache} backed by a {@link HashMap}.
 *
 * <p><b>Purpose.</b> Used by {@link NimbusDpopValidatorTest} to exercise the replay-detection
 * branch without standing up the production cache (which the launcher composes).
 *
 * <p><b>Expiry semantics.</b> Entries store the {@code expireAtNs} deadline supplied by the caller;
 * the cache evicts an entry when a subsequent {@link #checkAndAdd} for the SAME jti arrives after
 * that deadline (relative to the configurable {@link #clearExpired(long)} clock — tests call this
 * explicitly to control eviction without depending on wall time). This keeps tests deterministic
 * regardless of which time source the validator uses for the deadline.
 *
 * <p><b>Threading.</b> Synchronised on the cache instance — the SAM contract requires atomic
 * check-and-insert.
 *
 * <p><b>Allocation.</b> Cold path; allocates per call.
 */
public final class InMemoryJtiReplayCache implements JtiReplayCache {

  private final Map<String, Long> entries = new HashMap<>();

  @Override
  public synchronized boolean checkAndAdd(final String jti, final long expireAtNs) {
    final var existing = entries.get(jti);
    if (existing != null) {
      return false;
    }
    entries.put(jti, expireAtNs);
    return true;
  }

  /**
   * Manually evict every entry whose recorded deadline is at or before {@code nowNs}. Test-only —
   * production caches lazy-evict on read.
   *
   * @param nowNs current time in the same units the validator used when calling {@link
   *     #checkAndAdd}
   */
  public synchronized void clearExpired(final long nowNs) {
    entries.entrySet().removeIf(e -> e.getValue() <= nowNs);
  }

  /** Visible for tests — current resident entry count. */
  public synchronized int size() {
    return entries.size();
  }
}
