package com.trading.engine.pricing.skew;

import com.trading.engine.messages.util.ByteArrayKey;
import org.agrona.DirectBuffer;
import org.agrona.collections.Object2LongHashMap;

/**
 * Mutable, map-backed implementation of {@link PositionSource} that supports both static
 * configuration and live position updates.
 *
 * <p>Starts at zero position for all symbols. Can be seeded from configuration or test fixtures via
 * {@link #setPosition(byte[], long)}, and updated from a live fill feed via {@link
 * #setPosition(DirectBuffer, int, int, long)} (future). Real position feed via Aeron IPC stream 202
 * to be wired when APP-30 (orchestrator) publishes fills.
 *
 * <p><b>Lookup pattern.</b> Uses the zero-allocation probe-key idiom from {@link ByteArrayKey}: a
 * single {@link ByteArrayKey#emptyForLookup(int) pre-allocated probe key} is mutated in-place
 * before each map probe. The map keys are <em>owned</em> copies ({@link ByteArrayKey#owned(byte[],
 * int, int)}) so the probe can be freely reused. This avoids heap allocation on every {@link
 * #netPosition} call.
 *
 * <p><b>Unknown symbols.</b> Returns {@code 0} for any symbol not present in the map. Zero position
 * means no inventory skew is applied — the safe default for unconfigured instruments.
 *
 * <p><b>Threading model.</b> <b>Not thread-safe.</b> All methods — both the cold-path setters and
 * the hot-path {@link #netPosition} — must be called from the pricing-service agent's
 * single-threaded duty cycle. If position updates arrive from a separate Aeron subscription, they
 * must be polled on the same agent thread (standard Aeron single-writer pattern).
 *
 * <p><b>Allocation behaviour.</b>
 *
 * <ul>
 *   <li>{@link #setPosition(byte[], long)} — cold-path; allocates an owned {@link ByteArrayKey} for
 *       map storage. Called at startup or from a configuration reload — not on the pricing hot
 *       path.
 *   <li>{@link #setPosition(DirectBuffer, int, int, long)} — allocates an owned {@link
 *       ByteArrayKey} for map storage. Intended for live feed updates which are infrequent relative
 *       to quote generation.
 *   <li>{@link #netPosition(DirectBuffer, int, int)} — <b>zero-allocation</b>. Reuses the
 *       pre-allocated probe key for the map lookup. Returns primitive {@code long} directly from
 *       {@link Object2LongHashMap} — no boxing.
 * </ul>
 *
 * @see PositionSource
 * @see InventorySkewModel
 * @see ByteArrayKey#emptyForLookup(int)
 */
public final class ConfigurablePositionSource implements PositionSource {

  /**
   * Maximum symbol length in bytes. Sized for the 8-byte fixed-width SBE {@code Symbol} type with
   * headroom for future variable-length extensions.
   */
  private static final int MAX_SYMBOL_LENGTH = 16;

  /**
   * Per-symbol net position storage. Keys are owned (immutable) {@link ByteArrayKey} instances;
   * values are primitive {@code long} positions in fixed-point {@code 10^-8}.
   *
   * <p>Uses Agrona's {@link Object2LongHashMap} to store positions as unboxed primitives. The
   * missing value is {@code 0L}, meaning unknown symbols return zero position (flat — no skew).
   */
  private final Object2LongHashMap<ByteArrayKey> positions;

  /**
   * Pre-allocated mutable probe key reused for every {@link #netPosition} lookup. Mutated in-place
   * via {@link ByteArrayKey#set(DirectBuffer, int, int)} before each map probe. Never inserted into
   * the map.
   */
  private final ByteArrayKey probeKey;

  /**
   * Constructs a new {@code ConfigurablePositionSource} with an empty position map.
   *
   * <p>All symbols start at zero position (flat). Use {@link #setPosition(byte[], long)} to seed
   * initial positions from configuration or test fixtures.
   *
   * <p><b>Allocation:</b> allocates the backing {@link Object2LongHashMap} and the probe key. No
   * further allocation occurs on the hot-path {@link #netPosition} calls.
   */
  public ConfigurablePositionSource() {
    this.positions = new Object2LongHashMap<>(0L);
    this.probeKey = ByteArrayKey.emptyForLookup(MAX_SYMBOL_LENGTH);
  }

  /**
   * Sets the net position for a symbol from a byte array. This is the cold-path entry point used
   * for static configuration, test fixtures, and initial position seeding at startup.
   *
   * <p>If the symbol already has a position entry, it is overwritten. Setting a position of {@code
   * 0} effectively removes the skew for that symbol (the lookup will still find the entry, but the
   * skew calculation short-circuits on zero position).
   *
   * <p><b>Allocation:</b> allocates an owned {@link ByteArrayKey} (defensive copy). Cold-path only
   * — not called during quote generation.
   *
   * @param symbol symbol bytes in ASCII encoding (e.g., {@code "EUR/USD\0".getBytes()})
   * @param netPosition aggregate net position in fixed-point {@code 10^-8}. Positive = long,
   *     negative = short.
   */
  public void setPosition(final byte[] symbol, final long netPosition) {
    final ByteArrayKey key = ByteArrayKey.owned(symbol, 0, symbol.length);
    positions.put(key, netPosition);
  }

  /**
   * Sets the net position for a symbol from a {@link DirectBuffer}. This entry point is intended
   * for live position updates received from a fill-event Aeron subscription (future — APP-30).
   *
   * <p>If the symbol already has a position entry, it is overwritten.
   *
   * <p><b>Allocation:</b> allocates an owned {@link ByteArrayKey} (defensive copy from buffer).
   * Position updates are infrequent relative to quote generation (one update per fill vs. thousands
   * of quotes per second), so the allocation cost is acceptable.
   *
   * @param symbol buffer containing the symbol bytes
   * @param offset start offset of the symbol within the buffer
   * @param length number of bytes to read
   * @param netPosition aggregate net position in fixed-point {@code 10^-8}. Positive = long,
   *     negative = short.
   */
  public void setPosition(
      final DirectBuffer symbol, final int offset, final int length, final long netPosition) {

    final ByteArrayKey key = ByteArrayKey.copyOf(symbol, offset, length);
    positions.put(key, netPosition);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Performs a zero-allocation lookup using the pre-allocated probe key. If the symbol is not
   * present in the position map, returns {@code 0} (flat — no skew applied).
   *
   * <p><b>Allocation:</b> zero-allocation. The probe key is mutated in-place; no boxing occurs
   * because {@link Object2LongHashMap} stores and returns primitive {@code long} values.
   */
  @Override
  public long netPosition(final DirectBuffer symbol, final int offset, final int length) {
    probeKey.set(symbol, offset, length);
    return positions.getValue(probeKey);
  }
}
