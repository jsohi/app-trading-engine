package com.trading.engine.gateway;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.trading.engine.messages.util.ByteArrayKey;
import org.agrona.collections.Object2ObjectHashMap;

/**
 * Tracks in-flight commands sent to the cluster that have not yet received a response. Each entry
 * maps a ClOrdID (by direct byte comparison) to the nanosecond timestamp at which the command was
 * offered. Periodically, {@link #checkTimeouts} is called from the gateway duty cycle to detect and
 * evict stale entries whose age exceeds the configured timeout.
 *
 * <p><b>Keying.</b> Uses content-based {@link ByteArrayKey} equality via Agrona's {@link
 * Object2ObjectHashMap}. Unlike the previous FNV-1a 64-bit hash approach, this guarantees
 * collision-free tracking — two distinct ClOrdIDs are never confused, regardless of hash
 * distribution. ClOrdID is 20 bytes (FIX tag 11) — it does not fit cleanly into primitive long
 * keys, and Agrona has no multi-key primitive map. {@code ByteArrayKey} + {@code
 * Object2ObjectHashMap} provides collision-free content-based keying at sub-microsecond lookup
 * cost.
 *
 * <p><b>Pool pattern.</b> Pre-allocated LIFO free-list stack of {@link InFlightEntry} objects,
 * matching the {@code OrderBook} pool pattern used in the cluster module. Each entry owns a {@link
 * ByteArrayKey} with a backing array sized to {@code maxClOrdIdLength}. On pool exhaustion, the
 * command is not tracked (timeout monitoring lost, not correctness) and a warning is logged.
 *
 * <p><b>Allocation.</b> Zero allocation after construction. The map's internal capacity is
 * pre-sized to prevent {@code rehash()} allocation on the hot path. The {@link
 * Object2ObjectHashMap} is constructed with {@code shouldAvoidAllocation = true} (the 2-arg
 * constructor default), so {@code values().iterator()} returns a reusable flyweight.
 *
 * <p><b>Iterator compaction.</b> Agrona's {@code Object2ObjectHashMap} uses open-addressing with
 * compaction on remove, which may cause the iterator to skip entries in a single pass. Any missed
 * entries will be caught on the next scan. This is acceptable given the throttled scan interval
 * (typically 100ms).
 *
 * <p><b>Threading.</b> Not thread-safe — single-threaded gateway duty-cycle thread only.
 *
 * @see SessionRegistry
 * @see ClusterClient
 * @see ClusterEgressListener
 */
public final class InFlightTracker {

  private static final Log LOG = LogFactory.getLog(InFlightTracker.class);

  /** Load factor for the Object2ObjectHashMap. Matches QuoteManager/RfqStateMachine convention. */
  private static final float LOAD_FACTOR = 0.55f;

  private final Object2ObjectHashMap<ByteArrayKey, InFlightEntry> pending;
  private final long timeoutNs;
  private final int maxClOrdIdLength;

  /** All pre-allocated entries — permanent reference for {@link #reset()} pool rebuilds. */
  private final InFlightEntry[] allEntries;

  /** LIFO free-list stack. {@code freeStack[0..freeCount)} contains available entries. */
  private final InFlightEntry[] freeStack;

  /** Number of entries currently available in {@link #freeStack} (top of stack index). */
  private int freeCount;

  /** Reusable probe key for zero-allocation map lookups. */
  private final ByteArrayKey probeKey;

  /**
   * @param initialCapacity expected number of concurrent in-flight commands; determines pool size
   *     and map capacity
   * @param timeoutNs timeout in nanoseconds; entries older than this are expired by {@link
   *     #checkTimeouts}
   * @param maxClOrdIdLength maximum byte length of a ClOrdID (typically 20 from the SBE schema);
   *     determines the pre-allocated backing array size for each pool entry
   */
  public InFlightTracker(
      final int initialCapacity, final long timeoutNs, final int maxClOrdIdLength) {
    if (initialCapacity <= 0) {
      throw new IllegalArgumentException("initialCapacity must be positive: " + initialCapacity);
    }
    if (timeoutNs <= 0) {
      throw new IllegalArgumentException("timeoutNs must be positive: " + timeoutNs);
    }
    if (maxClOrdIdLength <= 0) {
      throw new IllegalArgumentException("maxClOrdIdLength must be positive: " + maxClOrdIdLength);
    }

    this.timeoutNs = timeoutNs;
    this.maxClOrdIdLength = maxClOrdIdLength;

    // Size the map so that initialCapacity entries never trigger rehash().
    // Agrona rounds to next power-of-2 internally; with 0.55 load factor and 4096 capacity,
    // internal capacity = 8192, threshold = floor(8192 * 0.55) = 4505 > 4096.
    final int mapCapacity = (int) Math.ceil(initialCapacity / (double) LOAD_FACTOR);
    this.pending = new Object2ObjectHashMap<>(mapCapacity, LOAD_FACTOR);

    // Force lazy ValueCollection allocation at construction (cold path), not on first
    // hot-path timeout scan.
    pending.values();

    // Pre-allocate pool entries with full-size backing arrays.
    this.allEntries = new InFlightEntry[initialCapacity];
    this.freeStack = new InFlightEntry[initialCapacity];
    for (int i = 0; i < initialCapacity; i++) {
      final var key = ByteArrayKey.owned(new byte[maxClOrdIdLength], 0, maxClOrdIdLength);
      allEntries[i] = new InFlightEntry(key);
      freeStack[i] = allEntries[i];
    }
    this.freeCount = initialCapacity;

    this.probeKey = ByteArrayKey.emptyForLookup(maxClOrdIdLength);
  }

  /**
   * Record that a command with the given ClOrdID was sent to the cluster.
   *
   * <p>If the same ClOrdID is already tracked (duplicate submission), the timestamp is updated to
   * the new value — matching the previous {@code map.put} overwrite semantics.
   *
   * <p>If the pool is exhausted, logs a warning and returns without tracking. The order will still
   * be processed by the cluster and routed via {@link SessionRegistry} — only timeout monitoring is
   * lost.
   *
   * @param clOrdId ClOrdID bytes (null-padding already trimmed)
   * @param offset start offset within {@code clOrdId}
   * @param length number of significant bytes
   * @param timestampNs monotonic nanosecond timestamp at which the command was offered
   */
  public void onCommandSent(
      final byte[] clOrdId, final int offset, final int length, final long timestampNs) {
    if (length <= 0 || length > maxClOrdIdLength) {
      LOG.warn()
          .append("InFlightTracker: rejected ClOrdID with invalid length=")
          .append(length)
          .append(" maxClOrdIdLength=")
          .append(maxClOrdIdLength)
          .commit();
      return;
    }

    probeKey.wrapForProbe(clOrdId, offset, length);

    final var existing = pending.get(probeKey);
    if (existing != null) {
      existing.timestampNs = timestampNs;
      return;
    }

    if (freeCount == 0) {
      LOG.warn().append("InFlightTracker pool exhausted, size=").append(pending.size()).commit();
      return;
    }

    final var entry = freeStack[--freeCount];
    entry.key.overwrite(clOrdId, offset, length);
    entry.timestampNs = timestampNs;
    pending.put(entry.key, entry);
  }

  /**
   * Acknowledge that a response was received for the given ClOrdID. Removes the entry from the
   * in-flight map and returns it to the pool.
   *
   * @param clOrdId ClOrdID bytes
   * @param offset start offset within {@code clOrdId}
   * @param length number of significant bytes
   * @return {@code true} if the entry was tracked (and removed), {@code false} if it was already
   *     timed out, never tracked, or skipped due to pool exhaustion
   */
  public boolean onResponseReceived(final byte[] clOrdId, final int offset, final int length) {
    if (length <= 0 || length > maxClOrdIdLength) {
      return false;
    }
    probeKey.wrapForProbe(clOrdId, offset, length);
    final var entry = pending.remove(probeKey);
    if (entry != null && freeCount < freeStack.length) {
      freeStack[freeCount++] = entry;
    }
    return entry != null;
  }

  /**
   * Scan all in-flight entries and invoke {@code callback} for each one whose age exceeds {@link
   * #timeoutNs}. Expired entries are removed from the map and returned to the pool during the scan.
   *
   * <p>Called from the gateway duty cycle at a throttled interval (not every {@code doWork()}).
   *
   * <p><b>Iterator compaction.</b> Agrona's {@code Object2ObjectHashMap} uses open-addressing with
   * compaction on remove, which may cause the iterator to skip entries in a single pass. Any missed
   * entries will be caught on the next scan. This is acceptable given the throttled scan interval
   * (typically 100ms).
   *
   * <p><b>Exception propagation.</b> If the callback throws, the expired entry is still removed and
   * returned to the pool (via the {@code finally} block), but the scan terminates early. Remaining
   * expired entries will be caught on the next scan.
   *
   * @param nowNs current monotonic nanosecond timestamp
   * @param callback invoked for each expired entry; see {@link TimeoutCallback} for lifetime and
   *     re-entrancy constraints
   * @return number of entries that timed out
   */
  public int checkTimeouts(final long nowNs, final TimeoutCallback callback) {
    int expired = 0;
    final var it = pending.values().iterator();
    while (it.hasNext()) {
      final var entry = it.next();
      if (nowNs - entry.timestampNs >= timeoutNs) {
        try {
          callback.onTimeout(
              entry.key.backingArray(), entry.key.offset(), entry.key.length(), entry.timestampNs);
        } finally {
          it.remove();
          if (freeCount < freeStack.length) {
            freeStack[freeCount++] = entry;
          }
          expired++;
        }
      }
    }
    return expired;
  }

  /** Number of commands currently in-flight. */
  public int size() {
    return pending.size();
  }

  /**
   * Clear all tracked entries and rebuild the pool. Called on cluster reconnection (all pending
   * requests are stale). After this call, the tracker is in the same state as after construction —
   * all pool entries are available for reuse.
   */
  public void reset() {
    pending.clear();
    for (int i = 0; i < allEntries.length; i++) {
      allEntries[i].timestampNs = 0;
      freeStack[i] = allEntries[i];
    }
    freeCount = allEntries.length;
  }

  /**
   * Callback invoked by {@link #checkTimeouts} for each expired in-flight entry.
   *
   * <p><b>Lifetime:</b> the {@code clOrdId} array is owned by the tracker's internal pool.
   * Implementations MUST NOT retain a reference past this method's return — the content becomes
   * undefined after the callback completes.
   *
   * <p><b>Re-entrancy:</b> implementations MUST NOT call any method on {@code InFlightTracker}
   * during this callback. The internal map is being iterated; calling {@code onCommandSent}, {@code
   * onResponseReceived}, or {@code reset} would corrupt the iterator.
   */
  @FunctionalInterface
  public interface TimeoutCallback {

    /**
     * @param clOrdId backing array containing the timed-out ClOrdID bytes
     * @param offset start offset (always 0 for owned keys)
     * @param length significant byte count
     * @param sentTimestampNs nanosecond timestamp at which the command was originally offered
     */
    void onTimeout(byte[] clOrdId, int offset, int length, long sentTimestampNs);
  }

  // ---------------------------------------------------------------------------
  // Pool entry
  // ---------------------------------------------------------------------------

  /**
   * Internal entry pairing a pre-allocated {@link ByteArrayKey} with a sent timestamp. Pooled in
   * {@code allEntries[]} and recycled via the {@code freeStack[]} LIFO stack.
   */
  static final class InFlightEntry {

    /** Owned key with pre-allocated backing array. Used as the map key when this entry is live. */
    final ByteArrayKey key;

    /** Monotonic nanosecond timestamp at which the command was offered to the cluster. */
    long timestampNs;

    InFlightEntry(final ByteArrayKey key) {
      this.key = key;
    }
  }
}
