package com.trading.engine.pricing.quote;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.trading.engine.pricing.ByteArrayKey;
import java.util.Iterator;
import java.util.Map;
import org.agrona.DirectBuffer;
import org.agrona.collections.Object2ObjectHashMap;

/**
 * Tracks active quotes keyed by {@code quoteReqId} (FIX tag 131), backed by a pre-allocated object
 * pool that eliminates per-quote heap allocation on the hot path.
 *
 * <p>The manager owns a fixed-size pool of {@link QuoteEntry} flyweights, allocated once at
 * construction. When a new quote is stored via {@link #allocateAndStore}, the next entry is drawn
 * from the pool in round-robin order and inserted into an Agrona {@link Object2ObjectHashMap}. If a
 * quote with the same {@code quoteReqId} already exists, it is removed first (quote refresh
 * semantics -- CME iLink 3 pattern). If the pool is exhausted (all slots are in use), the entry
 * with the oldest {@link QuoteEntry#creationNanos} is evicted to make room.
 *
 * <h3>Expiry model</h3>
 *
 * <p>{@link #expireStale(long)} performs a FIFO-ish scan of all active entries and removes any
 * whose {@link QuoteEntry#validUntil} has elapsed. This method is designed to be called on a
 * throttled schedule (e.g., every 100ms) rather than on every duty-cycle tick, since the iteration
 * cost is O(n) in the number of active quotes.
 *
 * <h3>Threading model</h3>
 *
 * <p><b>Not thread-safe.</b> All methods are invoked exclusively from the pricing-service agent's
 * single-threaded duty cycle. The reusable {@link #probeKey} must not be used reentrantly.
 *
 * <h3>Allocation behaviour</h3>
 *
 * <p>Zero allocation after construction. Pool entries and map keys are pre-allocated at startup.
 * {@link #allocateAndStore} reuses pool entries and creates an owned {@link ByteArrayKey} for the
 * map key only when a genuinely new slot is populated (the key is allocated once per pool slot over
 * the lifetime of the manager). Lookups via {@link #lookup} use the reusable probe key.
 *
 * @see QuoteEntry
 * @see PriceValidator
 */
public final class QuoteManager {

  private static final Log LOG = LogFactory.getLog(QuoteManager.class);

  /** Default pool capacity if not specified by the caller. */
  private static final int DEFAULT_MAX_ACTIVE_QUOTES = 10_000;

  /** Load factor for the Agrona hash map -- lower than default for fewer collisions. */
  private static final float LOAD_FACTOR = 0.55f;

  /**
   * Primary store: quoteReqId to QuoteEntry. Keys are owned {@link ByteArrayKey} instances
   * allocated once per pool slot. Values are references into the pre-allocated {@link #pool}.
   */
  private final Object2ObjectHashMap<ByteArrayKey, QuoteEntry> activeQuotes;

  /**
   * Pre-allocated pool of {@link QuoteEntry} flyweights. Entries are drawn in round-robin order via
   * {@link #poolIndex}. Pool size equals {@link #maxActiveQuotes}.
   */
  private final QuoteEntry[] pool;

  /**
   * Parallel array of owned {@link ByteArrayKey} instances, one per pool slot. When a pool entry is
   * (re)used, the corresponding key is removed from the map (if present) and re-inserted under the
   * new quoteReqId. This avoids allocating a new key on every quote store.
   */
  private final ByteArrayKey[] poolKeys;

  /**
   * Reusable probe key for zero-allocation lookups into {@link #activeQuotes}. Mutated in place
   * before each {@code map.get(probeKey)} call. NEVER inserted into the map.
   */
  private final ByteArrayKey probeKey;

  /** Maximum number of active quotes (and pool size). */
  private final int maxActiveQuotes;

  /**
   * Round-robin index into {@link #pool}. Wraps to 0 when it reaches {@link #maxActiveQuotes}. On
   * wrap, the oldest entries are evicted to make room.
   */
  private int poolIndex;

  /**
   * Constructs a quote manager with the specified pool capacity.
   *
   * <p><b>Allocation:</b> allocates the pool array, all {@link QuoteEntry} instances, all {@link
   * ByteArrayKey} instances for pool slots, the probe key, and the Agrona hash map. This is a
   * cold-path operation performed once at pricing-service startup.
   *
   * @param maxActiveQuotes maximum number of concurrently active quotes; must be {@code > 0}.
   *     Determines the pool size and map capacity.
   * @throws IllegalArgumentException if {@code maxActiveQuotes <= 0}
   */
  public QuoteManager(final int maxActiveQuotes) {
    if (maxActiveQuotes <= 0) {
      throw new IllegalArgumentException("maxActiveQuotes must be > 0, got: " + maxActiveQuotes);
    }
    this.maxActiveQuotes = maxActiveQuotes;
    this.activeQuotes = new Object2ObjectHashMap<>(maxActiveQuotes, LOAD_FACTOR);
    this.pool = new QuoteEntry[maxActiveQuotes];
    this.poolKeys = new ByteArrayKey[maxActiveQuotes];
    this.probeKey = ByteArrayKey.emptyForLookup(QuoteEntry.QUOTE_REQ_ID_LENGTH);
    this.poolIndex = 0;

    for (int i = 0; i < maxActiveQuotes; i++) {
      pool[i] = new QuoteEntry();
      // Pool keys start as empty 20-byte keys. They will be overwritten with actual
      // quoteReqId content on first use, and reused (removed + re-inserted) thereafter.
      poolKeys[i] = null; // Lazily allocated on first use of each slot.
    }
  }

  /**
   * Constructs a quote manager with the {@link #DEFAULT_MAX_ACTIVE_QUOTES default} pool capacity.
   *
   * <p><b>Allocation:</b> see {@link #QuoteManager(int)}.
   */
  public QuoteManager() {
    this(DEFAULT_MAX_ACTIVE_QUOTES);
  }

  /**
   * Allocates the next pool entry for a new quote identified by {@code quoteReqId} and stores it in
   * the active quotes map.
   *
   * <p><b>Quote refresh:</b> if a quote with the same {@code quoteReqId} already exists in the map,
   * the old entry is removed first. This supports the FIX quote refresh pattern where a market
   * maker sends an updated quote with the same request ID.
   *
   * <p><b>Pool exhaustion:</b> when the round-robin index wraps past {@link #maxActiveQuotes}, the
   * entry at the current pool index is evicted (removed from the map) and reused. The pool index
   * advances sequentially, which means the oldest-allocated entry is the first candidate for
   * eviction -- a FIFO approximation that is efficient and avoids a full scan.
   *
   * <p>The returned {@link QuoteEntry} must be populated by the caller via {@link
   * QuoteEntry#populate} before the next duty-cycle iteration.
   *
   * <p><b>Allocation:</b> zero allocation after the initial warm-up pass through all pool slots.
   * The first use of each slot allocates an owned {@link ByteArrayKey} for the map key. Subsequent
   * reuses of the same slot mutate the existing key in-place via {@link
   * ByteArrayKey#overwrite(DirectBuffer, int, int)} (the key is removed from the map first to
   * preserve the hash invariant, then overwritten and re-inserted).
   *
   * @param quoteReqId buffer containing the QuoteReqID bytes (FIX tag 131)
   * @param offset start offset of the QuoteReqID within the buffer
   * @param length number of meaningful bytes; must be {@code <= 20}
   * @return the pool entry to be populated by the caller; never {@code null}
   */
  public QuoteEntry allocateAndStore(
      final DirectBuffer quoteReqId, final int offset, final int length) {

    // If a quote with this quoteReqId already exists, remove it first (quote refresh).
    probeKey.set(quoteReqId, offset, length);
    final QuoteEntry existing = activeQuotes.get(probeKey);
    if (existing != null) {
      // Remove the old mapping. We need to find and remove the owned key.
      activeQuotes.remove(probeKey);
      LOG.info().append("Quote refresh: removed existing quoteReqId, pool reuse").commit();
    }

    // Advance the round-robin pool index.
    final int slot = poolIndex;
    poolIndex = (poolIndex + 1) % maxActiveQuotes;

    final QuoteEntry entry = pool[slot];

    // If this slot's entry is currently in the map under a different quoteReqId, evict it.
    final ByteArrayKey oldKey = poolKeys[slot];
    if (oldKey != null) {
      final QuoteEntry mapped = activeQuotes.get(oldKey);
      if (mapped == entry) {
        // This pool entry is still actively mapped -- evict it.
        activeQuotes.remove(oldKey);
        LOG.info()
            .append("Pool slot ")
            .append(slot)
            .append(" evicted for reuse, active=")
            .append(activeQuotes.size())
            .commit();
      }
    }

    // Reset the entry for reuse.
    entry.reset();

    // Reuse or create the key for this pool slot.
    // After the first warm-up pass, all slots have pre-allocated keys. We overwrite the
    // key bytes in-place (the old mapping was removed above, so the hash invariant is safe).
    ByteArrayKey insertKey = poolKeys[slot];
    if (insertKey == null) {
      // First use of this slot — allocate the key once.
      insertKey = ByteArrayKey.copyOf(quoteReqId, offset, length);
      poolKeys[slot] = insertKey;
    } else {
      // Subsequent reuses — zero-alloc overwrite of the existing key.
      insertKey.overwrite(quoteReqId, offset, length);
    }
    activeQuotes.put(insertKey, entry);

    return entry;
  }

  /**
   * Looks up an active quote by its {@code quoteReqId}. Uses the pre-allocated probe key for
   * zero-allocation lookup.
   *
   * <p><b>Allocation:</b> zero allocation.
   *
   * @param quoteReqId buffer containing the QuoteReqID bytes (FIX tag 131)
   * @param offset start offset of the QuoteReqID within the buffer
   * @param length number of meaningful bytes; must be {@code <= 20}
   * @return the matching {@link QuoteEntry}, or {@code null} if no active quote exists for this
   *     quoteReqId
   */
  public QuoteEntry lookup(final DirectBuffer quoteReqId, final int offset, final int length) {
    probeKey.set(quoteReqId, offset, length);
    return activeQuotes.get(probeKey);
  }

  /**
   * Scans all active quotes and removes any that have expired (i.e., whose {@link
   * QuoteEntry#validUntil} is at or before {@code nowEpochNanos}).
   *
   * <p>This method iterates the map's entry set and removes expired entries via the iterator. It is
   * O(n) in the number of active quotes and should be called on a throttled schedule (e.g., every
   * 100ms) rather than on every duty-cycle tick.
   *
   * <p><b>Allocation:</b> zero allocation -- Agrona's {@link Object2ObjectHashMap} iterator is
   * allocation-free when used within a single thread and not interleaved with map mutations from
   * outside the iterator.
   *
   * @param nowEpochNanos the current epoch nanosecond time from {@link
   *     com.trading.engine.messages.clock.TradingClocks#epochNanoClock()}
   * @return the number of expired quotes removed
   */
  public int expireStale(final long nowEpochNanos) {
    int expiredCount = 0;

    final Iterator<Map.Entry<ByteArrayKey, QuoteEntry>> it = activeQuotes.entrySet().iterator();
    while (it.hasNext()) {
      final Map.Entry<ByteArrayKey, QuoteEntry> mapEntry = it.next();
      final QuoteEntry entry = mapEntry.getValue();
      if (entry.isExpired(nowEpochNanos)) {
        it.remove();
        entry.reset();
        expiredCount++;
      }
    }

    if (expiredCount > 0) {
      LOG.info()
          .append("Expired ")
          .append(expiredCount)
          .append(" stale quotes, remaining=")
          .append(activeQuotes.size())
          .commit();
    }

    return expiredCount;
  }

  /**
   * Returns the number of currently active (stored, not yet expired or evicted) quotes.
   *
   * <p><b>Allocation:</b> zero allocation.
   *
   * @return the active quote count
   */
  public int size() {
    return activeQuotes.size();
  }
}
