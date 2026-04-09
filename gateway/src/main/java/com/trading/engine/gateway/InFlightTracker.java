package com.trading.engine.gateway;

import org.agrona.collections.Long2LongHashMap;

/**
 * Tracks in-flight commands sent to the cluster that have not yet received a response. Each entry
 * maps a hash of the ClOrdID to the nanosecond timestamp at which the command was offered.
 * Periodically, {@link #checkTimeouts} is called from the gateway duty cycle to detect and evict
 * stale entries whose age exceeds the configured timeout.
 *
 * <p><b>Hashing.</b> ClOrdID bytes are hashed with FNV-1a 64-bit to produce a deterministic {@code
 * long} key. Collisions are possible but extremely unlikely for 20-byte ASCII identifiers; the
 * practical effect of a collision is a missed timeout or a premature ack — not data corruption.
 *
 * <p><b>Allocation.</b> Zero allocation after construction. Uses Agrona {@link Long2LongHashMap}
 * (no boxing). The timeout scan iterates the map's {@code entrySet()} which is a reusable flyweight
 * iterator.
 *
 * <p><b>Threading.</b> Not thread-safe — single-threaded gateway duty-cycle thread only.
 */
public final class InFlightTracker {

  /** Sentinel for missing entries in the underlying map. */
  private static final long MISSING_VALUE = Long.MIN_VALUE;

  /** FNV-1a 64-bit offset basis. */
  private static final long FNV_OFFSET_BASIS = 0xcbf29ce484222325L;

  /** FNV-1a 64-bit prime. */
  private static final long FNV_PRIME = 0x100000001b3L;

  private final Long2LongHashMap pending;
  private final long timeoutNs;

  /**
   * @param initialCapacity expected number of concurrent in-flight commands
   * @param timeoutNs timeout in nanoseconds; entries older than this are expired by {@link
   *     #checkTimeouts}
   */
  public InFlightTracker(final int initialCapacity, final long timeoutNs) {
    if (timeoutNs <= 0) {
      throw new IllegalArgumentException("timeoutNs must be positive: " + timeoutNs);
    }
    this.pending = new Long2LongHashMap(initialCapacity, 0.65f, MISSING_VALUE);
    this.timeoutNs = timeoutNs;
  }

  /**
   * Record that a command with the given ClOrdID was sent to the cluster.
   *
   * @param clOrdId ClOrdID bytes (null-padding already trimmed)
   * @param offset start offset within {@code clOrdId}
   * @param length number of significant bytes
   * @param timestampNs monotonic nanosecond timestamp at which the command was offered
   */
  public void onCommandSent(
      final byte[] clOrdId, final int offset, final int length, final long timestampNs) {
    final long hash = fnv1aHash(clOrdId, offset, length);
    pending.put(hash, timestampNs);
  }

  /**
   * Acknowledge that a response was received for the given ClOrdID. Removes the entry from the
   * in-flight map.
   *
   * @return {@code true} if the entry was tracked (and removed), {@code false} if it was already
   *     timed out or never tracked
   */
  public boolean onResponseReceived(final byte[] clOrdId, final int offset, final int length) {
    final long hash = fnv1aHash(clOrdId, offset, length);
    return pending.remove(hash) != MISSING_VALUE;
  }

  /**
   * Scan all in-flight entries and invoke {@code callback} for each one whose age exceeds {@link
   * #timeoutNs}. Expired entries are removed from the map during the scan.
   *
   * <p>Called from the gateway duty cycle on each {@code doWork()} iteration.
   *
   * @param nowNs current monotonic nanosecond timestamp
   * @param callback invoked for each expired entry
   * @return number of entries that timed out
   */
  public int checkTimeouts(final long nowNs, final TimeoutCallback callback) {
    int expired = 0;
    final Long2LongHashMap.EntryIterator it = pending.entrySet().iterator();
    while (it.hasNext()) {
      it.next();
      final long sentNs = it.getLongValue();
      if (nowNs - sentNs >= timeoutNs) {
        callback.onTimeout(it.getLongKey(), sentNs);
        it.remove();
        expired++;
      }
    }
    return expired;
  }

  /** Number of commands currently in-flight. */
  public int size() {
    return pending.size();
  }

  /** Clear all tracked entries. Called on cluster reconnection (all pending requests are stale). */
  public void reset() {
    pending.clear();
  }

  /** Callback invoked by {@link #checkTimeouts} for each expired in-flight entry. */
  @FunctionalInterface
  public interface TimeoutCallback {

    /**
     * @param clOrdIdHash FNV-1a hash of the timed-out ClOrdID
     * @param sentTimestampNs nanosecond timestamp at which the command was originally offered
     */
    void onTimeout(long clOrdIdHash, long sentTimestampNs);
  }

  // ---------------------------------------------------------------------------
  // FNV-1a 64-bit hash
  // ---------------------------------------------------------------------------

  static long fnv1aHash(final byte[] data, final int offset, final int length) {
    long hash = FNV_OFFSET_BASIS;
    for (int i = offset, end = offset + length; i < end; i++) {
      hash ^= (data[i] & 0xFFL);
      hash *= FNV_PRIME;
    }
    return hash;
  }
}
