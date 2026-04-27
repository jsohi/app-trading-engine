package com.trading.engine.websocket;

import java.util.Arrays;
import org.agrona.collections.Long2LongHashMap;

/**
 * Per-session event filter that determines whether an egress SBE message should be delivered to a
 * specific WebSocket client based on the client's symbol subscriptions and event type bitmask.
 *
 * <p><b>Design.</b> Each client sends {@code WebSocketSubscribe} (templateId 62) with a list of
 * symbols and a per-symbol event type bitmask. This filter stores those subscriptions and evaluates
 * each egress message against them via {@link #matches(int, byte[], int, int)}.
 *
 * <p><b>Thread safety — copy-on-write with volatile publish.</b> Subscriptions are mutated by the
 * channel's Netty event loop thread (via {@link #addSubscription} / {@link #removeSubscription}).
 * The drain handler reads subscriptions from a different event loop thread via {@link #matches}.
 * Thread safety is achieved without locks: mutations update a mutable {@link Long2LongHashMap},
 * then rebuild and volatile-publish an immutable {@link SubscriptionSnapshot}. The drain thread
 * reads the snapshot via a single volatile read. <b>Critical invariant: published snapshots are
 * never mutated after volatile-publish.</b>
 *
 * <p><b>No-symbol templates.</b> Some event templates (110 AccountLoaded, 111 AccountLoadRejected,
 * 112 OrderCancelRejected, 204 PositionSnapshot) have no extractable symbol field. These are
 * matched against the {@code globalEventBitMask} — the OR of all subscription bitmasks. If ANY
 * subscription includes the event type, the message passes through.
 *
 * <p><b>Allocation.</b> {@link #matches} is zero-allocation (binary search on primitive arrays,
 * single volatile read). {@link #addSubscription} / {@link #removeSubscription} allocate a new
 * snapshot (acceptable — subscription mutations are cold path, not per-message).
 *
 * @see SymbolExtractor
 * @see AccountExtractor
 * @see <a href="docs/websocket-architecture.md">WebSocket Architecture — Section 1</a>
 */
public final class SubscriptionFilter {

  /** Bitmask covering all valid event type bits (bits 0-4). */
  public static final int VALID_EVENT_TYPES_MASK = 0x1F;

  // Event type bit positions (matching SBE schema comment on WebSocketSubscribe.eventTypes)
  private static final int BIT_ORDERS = 0x01; // bit 0: Order lifecycle (100-103, 112)
  private static final int BIT_POSITIONS = 0x02; // bit 1: Positions (204)
  private static final int BIT_PRICES = 0x04; // bit 2: Prices (51)
  private static final int BIT_QUOTES = 0x08; // bit 3: Quotes (104-107)
  private static final int BIT_ACCOUNTS = 0x10; // bit 4: Account events (110, 111)

  /** Empty snapshot — no subscriptions, nothing passes through. */
  private static final SubscriptionSnapshot EMPTY =
      new SubscriptionSnapshot(new long[0], new long[0], 0, 0);

  /**
   * Missing value for {@link Long2LongHashMap}. Used as sentinel for both key and value lookups.
   * Safe because: (1) valid packed symbols are ASCII (MSB byte &lt;= 0x7F), so no packed long has
   * the sign bit set; (2) valid bitmasks are in [0x01, 0x1F]. Neither can equal Long.MIN_VALUE.
   */
  private static final long MISSING_VALUE = Long.MIN_VALUE;

  /** Maximum subscriptions per session. */
  private final int maxSubscriptions;

  /** Mutable state — only accessed from the channel's Netty event loop thread. */
  private final Long2LongHashMap mutable;

  /**
   * Published snapshot — read by the drain thread via volatile read. Never mutated after publish.
   */
  @SuppressWarnings("FieldMayBeFinal") // volatile field cannot be final
  private volatile SubscriptionSnapshot snapshot = EMPTY;

  /**
   * Create a new subscription filter.
   *
   * @param maxSubscriptions the maximum number of symbol subscriptions allowed (from config)
   */
  public SubscriptionFilter(final int maxSubscriptions) {
    this.maxSubscriptions = maxSubscriptions;
    this.mutable = new Long2LongHashMap(MISSING_VALUE);
  }

  /**
   * Check whether an egress message matches this session's subscriptions.
   *
   * <p>Called from the drain handler's event loop thread. Zero allocation — single volatile read,
   * binary search on sorted primitive array.
   *
   * @param templateId the SBE templateId from the message header
   * @param sbePayload the raw SBE message bytes (header + body)
   * @param offset the start offset of the SBE message within the byte array
   * @param length the total length of the SBE message
   * @return true if the message should be delivered to this session
   */
  public boolean matches(
      final int templateId, final byte[] sbePayload, final int offset, final int length) {

    final SubscriptionSnapshot snap = snapshot; // single volatile read
    if (snap.count == 0) {
      return false;
    }

    final int eventBit = templateIdToEventBit(templateId);
    if (eventBit < 0) {
      return false; // unknown or internal template — never delivered
    }

    final long packedSymbol =
        SymbolExtractor.extractPackedSymbol(templateId, sbePayload, offset, length);

    if (packedSymbol == SymbolExtractor.UNKNOWN_SYMBOL) {
      // No-symbol template (110, 111, 112, 204): match if ANY subscription includes this event type
      return (snap.globalEventBitMask & eventBit) != 0;
    }

    // Binary search for the packed symbol in the sorted snapshot array
    final int idx = Arrays.binarySearch(snap.packedSymbols, 0, snap.count, packedSymbol);
    if (idx < 0) {
      return false; // symbol not subscribed
    }

    return (snap.eventBitmasks[idx] & eventBit) != 0;
  }

  /**
   * Add or update a subscription for a symbol with the given event types bitmask.
   *
   * <p>Called from the channel's Netty event loop thread only. Not thread-safe with concurrent
   * calls — single-writer by design.
   *
   * @param packedSymbol the packed symbol (from {@link
   *     com.trading.engine.projections.SymbolPacker})
   * @param eventTypes the event type bitmask (bits 0-4 only; undefined bits must be pre-masked by
   *     the caller)
   * @return true if the subscription was added/updated; false if at capacity
   */
  public boolean addSubscription(final long packedSymbol, final int eventTypes) {
    final int masked = eventTypes & VALID_EVENT_TYPES_MASK;
    if (masked == 0) {
      return false; // no valid event types — reject (prevents wasting subscription slots)
    }

    // Check capacity (only for new symbols, not updates)
    if (!mutable.containsKey(packedSymbol) && mutable.size() >= maxSubscriptions) {
      return false;
    }

    mutable.put(packedSymbol, masked);
    publishSnapshot();
    return true;
  }

  /**
   * Remove a subscription for a symbol.
   *
   * @param packedSymbol the packed symbol to unsubscribe
   */
  public void removeSubscription(final long packedSymbol) {
    if (mutable.containsKey(packedSymbol)) {
      mutable.remove(packedSymbol);
      publishSnapshot();
    }
  }

  /**
   * Remove all subscriptions. Called on disconnect (architecture doc: grace period does NOT hold
   * subscriptions).
   */
  public void clear() {
    mutable.clear();
    snapshot = EMPTY;
  }

  /**
   * @return the current number of subscribed symbols (from the published snapshot)
   */
  public int subscriptionCount() {
    return snapshot.count;
  }

  /**
   * @return true if no subscriptions are active (fast-path for drain handler)
   */
  public boolean isEmpty() {
    return snapshot.count == 0;
  }

  /**
   * Map an SBE templateId to the corresponding event type bit, or -1 if the template is not
   * delivered to WebSocket clients (internal events).
   *
   * @param templateId the SBE templateId
   * @return the event type bit (one of BIT_ORDERS, BIT_POSITIONS, etc.), or -1
   */
  public static int templateIdToEventBit(final int templateId) {
    return switch (templateId) {
      // bit 0: Order lifecycle
      case 100, 101, 102, 103, 112 -> BIT_ORDERS;
      // bit 1: Positions
      case 204 -> BIT_POSITIONS;
      // bit 2: Prices
      case 51 -> BIT_PRICES;
      // bit 3: Quotes
      case 104, 105, 106, 107 -> BIT_QUOTES;
      // bit 4: Account events
      case 110, 111 -> BIT_ACCOUNTS;
      // Internal events (108/109/113-116) — never delivered to WebSocket clients
      default -> -1;
    };
  }

  /**
   * Rebuild and publish an immutable snapshot from the mutable map. The snapshot is a pair of
   * sorted parallel arrays (packed symbols + event bitmasks) plus the global OR of all bitmasks.
   */
  private void publishSnapshot() {
    final int size = mutable.size();
    if (size == 0) {
      snapshot = EMPTY;
      return;
    }

    final long[] symbols = new long[size];
    final long[] bitmasks = new long[size];
    int globalMask = 0;
    int i = 0;

    final Long2LongHashMap.EntryIterator it = mutable.entrySet().iterator();
    while (it.hasNext()) {
      it.next();
      symbols[i] = it.getLongKey();
      bitmasks[i] = it.getLongValue();
      globalMask |= (int) it.getLongValue();
      i++;
    }

    // Sort by packed symbol for binary search in matches()
    sortParallel(symbols, bitmasks, size);
    snapshot = new SubscriptionSnapshot(symbols, bitmasks, globalMask, size);
  }

  /**
   * Sort two parallel arrays by the first array's values (packed symbols). Uses insertion sort —
   * optimal for the small arrays (max ~100 subscriptions per session).
   */
  private static void sortParallel(final long[] keys, final long[] values, final int length) {
    for (int i = 1; i < length; i++) {
      final long key = keys[i];
      final long val = values[i];
      int j = i - 1;
      while (j >= 0 && keys[j] > key) {
        keys[j + 1] = keys[j];
        values[j + 1] = values[j];
        j--;
      }
      keys[j + 1] = key;
      values[j + 1] = val;
    }
  }

  /**
   * Immutable snapshot of the subscription state. Published via volatile write, read by the drain
   * thread. Never mutated after construction.
   *
   * @param packedSymbols sorted array of packed symbols
   * @param eventBitmasks parallel array of event type bitmasks (same index as packedSymbols)
   * @param globalEventBitMask OR of all event bitmasks (for no-symbol template matching)
   * @param count number of active subscriptions
   */
  // SAFETY: arrays must not be mutated after construction. Defensive copy omitted for zero-alloc
  // on the read path. Arrays are freshly allocated in publishSnapshot() and never stored elsewhere.
  record SubscriptionSnapshot(
      long[] packedSymbols, long[] eventBitmasks, int globalEventBitMask, int count) {}
}
