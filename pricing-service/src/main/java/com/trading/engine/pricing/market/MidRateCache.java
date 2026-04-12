package com.trading.engine.pricing.market;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.trading.engine.messages.FixedPointScale;
import com.trading.engine.pricing.ByteArrayKey;
import org.agrona.DirectBuffer;
import org.agrona.collections.Object2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Pre-allocated cache of symbol mid-rates, keyed by the 8-byte SBE {@code Symbol} type.
 *
 * <p>Provides zero-allocation lookups via a reusable probe key (the same pattern as {@code
 * AccountStore.getByCode()} in the cluster module). Insertions allocate a {@link ByteArrayKey} and
 * {@link MidRateEntry} per symbol, but this happens only at startup (via {@link #preload}) or on
 * first encounter of a new symbol during operation — never in the steady-state hot path where the
 * set of traded symbols is fixed.
 *
 * <h3>Threading model</h3>
 *
 * <p><b>Not thread-safe.</b> Single-writer: only the owning {@link MarketDataAdapter} writes
 * entries (via {@link #put}). Reads (via {@link #midRate} and {@link #lastUpdateNanos}) occur from
 * the same agent duty-cycle thread in the pricing service. No concurrent access occurs.
 *
 * <h3>Allocation behaviour</h3>
 *
 * <p>Zero allocation after all symbols have been loaded. {@link #put} on an existing symbol mutates
 * the pre-allocated {@link MidRateEntry} in place. The reusable {@link #probeKey} avoids key
 * allocation on every lookup.
 *
 * @see MidRateEntry
 * @see MarketDataAdapter
 */
public final class MidRateCache {

  private static final Log LOG = LogFactory.getLog(MidRateCache.class);

  /**
   * Maximum byte length of an SBE Symbol field. Matches the schema: {@code <type name="Symbol"
   * primitiveType="char" length="8"/>}.
   */
  static final int SYMBOL_LENGTH = 8;

  /** Default initial capacity for the Agrona hash map. Sized for typical FX pair universe. */
  private static final int INITIAL_CAPACITY = 64;

  /** Load factor for the Agrona hash map — lower than default for fewer collisions. */
  private static final float LOAD_FACTOR = 0.55f;

  /**
   * Symbol-to-entry map. Keys are defensively-copied {@link ByteArrayKey} instances created at
   * insertion time; the value {@link MidRateEntry} is mutated in place on subsequent updates.
   */
  private final Object2ObjectHashMap<ByteArrayKey, MidRateEntry> entries =
      new Object2ObjectHashMap<>(INITIAL_CAPACITY, LOAD_FACTOR);

  /**
   * Reusable lookup probe — mutated in place by {@link #midRate} and {@link #lastUpdateNanos}.
   * NEVER inserted into the map. Same single-threaded, non-reentrant constraint as {@code
   * AccountStore.lookupKey}.
   */
  private final ByteArrayKey probeKey = ByteArrayKey.emptyForLookup(SYMBOL_LENGTH);

  /**
   * Temporary wrapper for converting {@code byte[]} to {@link DirectBuffer} in {@link #preload},
   * avoiding the probe-key aliasing issue that occurs with {@link ByteArrayKey#wrapForProbe(byte[],
   * int, int)} (which sets the probe's backing array to the external reference rather than
   * copying).
   */
  private final UnsafeBuffer preloadScratch = new UnsafeBuffer(new byte[SYMBOL_LENGTH]);

  /**
   * Constructs an empty cache. Symbols are added via {@link #preload} at startup or via {@link
   * #put} during operation.
   */
  public MidRateCache() {
    // Intentionally empty — entries are populated via preload() or put().
  }

  /**
   * Upserts a mid-rate for the given symbol. If the symbol already exists in the cache, the
   * existing {@link MidRateEntry} is mutated in place (zero allocation). If the symbol is new, a
   * new entry and key are allocated (cold path — happens only on first encounter).
   *
   * @param symbol buffer containing the symbol bytes
   * @param offset start offset of the symbol within the buffer
   * @param length number of bytes to read (must be {@code <= SYMBOL_LENGTH})
   * @param midRate the mid-rate in fixed-point {@code 10^-8}
   * @param lastUpdateNanos monotonic nanoseconds of this update
   */
  public void put(
      final DirectBuffer symbol,
      final int offset,
      final int length,
      final long midRate,
      final long lastUpdateNanos) {
    probeKey.set(symbol, offset, length);
    MidRateEntry entry = entries.get(probeKey);
    if (entry != null) {
      entry.update(midRate, lastUpdateNanos);
    } else {
      // First encounter — allocate key and entry (cold path, once per symbol).
      final ByteArrayKey insertKey = ByteArrayKey.copyOf(symbol, offset, length);
      entry = new MidRateEntry();
      entry.update(midRate, lastUpdateNanos);
      entries.put(insertKey, entry);
    }
  }

  /**
   * Returns the current mid-rate for the given symbol, or {@link
   * FixedPointScale#PRICE_NOT_AVAILABLE} if the symbol is not in the cache.
   *
   * <p>Zero-allocation — uses the reusable probe key.
   *
   * @param symbol buffer containing the symbol bytes
   * @param offset start offset of the symbol within the buffer
   * @param length number of bytes to read
   * @return mid-rate in fixed-point {@code 10^-8}, or {@link FixedPointScale#PRICE_NOT_AVAILABLE}
   */
  public long midRate(final DirectBuffer symbol, final int offset, final int length) {
    probeKey.set(symbol, offset, length);
    final MidRateEntry entry = entries.get(probeKey);
    return entry != null ? entry.midRate() : FixedPointScale.PRICE_NOT_AVAILABLE;
  }

  /**
   * Returns the monotonic nanosecond timestamp of the last update for the given symbol, or {@code
   * 0} if the symbol is not in the cache.
   *
   * <p>Zero-allocation — uses the reusable probe key.
   *
   * @param symbol buffer containing the symbol bytes
   * @param offset start offset of the symbol within the buffer
   * @param length number of bytes to read
   * @return monotonic nanoseconds of the last update, or {@code 0} if the symbol is unknown
   */
  public long lastUpdateNanos(final DirectBuffer symbol, final int offset, final int length) {
    probeKey.set(symbol, offset, length);
    final MidRateEntry entry = entries.get(probeKey);
    return entry != null ? entry.lastUpdateNanos() : 0L;
  }

  /**
   * Pre-loads a symbol with a fixed mid-rate and timestamp. Intended for startup initialisation
   * from configuration (e.g., YAML-loaded base rates).
   *
   * <p>This method allocates a {@link ByteArrayKey} and {@link MidRateEntry} per call. It is a
   * cold-path operation invoked only during construction of the adapter — never during steady-state
   * operation.
   *
   * @param symbol the symbol bytes (must be exactly {@link #SYMBOL_LENGTH} bytes, right-padded with
   *     spaces or nulls per SBE convention)
   * @param midRate the initial mid-rate in fixed-point {@code 10^-8}
   * @param lastUpdateNanos monotonic nanoseconds to record as the initial update time
   * @throws IllegalArgumentException if {@code symbol.length} exceeds {@link #SYMBOL_LENGTH}
   */
  public void preload(final byte[] symbol, final long midRate, final long lastUpdateNanos) {
    if (symbol.length > SYMBOL_LENGTH) {
      throw new IllegalArgumentException(
          "symbol length " + symbol.length + " exceeds max " + SYMBOL_LENGTH);
    }
    // Use DirectBuffer overload to avoid aliasing the external byte[] in the probe key.
    // wrapForProbe(byte[]) sets probeKey.data = symbol (external reference), which would
    // corrupt the probe if the caller later mutates the array. The DirectBuffer overload
    // copies bytes into the probe's own backing array.
    preloadScratch.wrap(symbol, 0, symbol.length);
    probeKey.set(preloadScratch, 0, symbol.length);
    final MidRateEntry existing = entries.get(probeKey);
    if (existing != null) {
      LOG.warn().append("preload: duplicate symbol overwritten, midRate=").append(midRate).commit();
      existing.update(midRate, lastUpdateNanos);
      return;
    }
    final ByteArrayKey insertKey = ByteArrayKey.copyOf(symbol, 0, symbol.length);
    final MidRateEntry entry = new MidRateEntry();
    entry.update(midRate, lastUpdateNanos);
    entries.put(insertKey, entry);
  }

  /**
   * Returns the number of symbols currently in the cache.
   *
   * @return entry count
   */
  public int size() {
    return entries.size();
  }
}
