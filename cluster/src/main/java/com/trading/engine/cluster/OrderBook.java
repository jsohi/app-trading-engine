package com.trading.engine.cluster;

import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.OrderBookSnapshotDecoder;
import com.trading.engine.messages.sbe.OrderBookSnapshotEncoder;
import com.trading.engine.messages.sbe.OrderBookSnapshotEncoder.NoOrdersEncoder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;

/**
 * In-memory book of active orders, implemented as a primitive-keyed flyweight pool. Backs the
 * cluster's {@code NewOrderSingle} → {@code ExecutionReport} path and feeds the {@code
 * OrderBookSnapshot} fragment of the cluster snapshot envelope.
 *
 * <p>Design (industry standard, matches exchange-core, LMAX, CME iLink, Aeron/Artio idioms):
 *
 * <ul>
 *   <li>Primary map: {@link Long2ObjectHashMap}{@code <OrderState>} keyed by a primitive {@code
 *       orderKey} — the monotonic counter value from {@link IdGenerator}. No {@code String} keying,
 *       no boxing, no autoboxing on the hot path.
 *   <li>Pre-allocated {@link OrderState} pool sized at construction; the cluster never allocates a
 *       new {@code OrderState} during {@code onSessionMessage}. {@link #acquire(long)} claims the
 *       next free slot and returns a cleared instance; {@link #release(long)} returns it to the
 *       free-list.
 *   <li>Free-list is an {@code int[]} used as a LIFO stack of pool-slot indices. Each {@code
 *       OrderState} carries its slot index so {@code release} is O(1) with no reverse scan.
 *   <li>Pool exhaustion is a first-class outcome: {@link #acquire(long)} returns {@code null} when
 *       the pool is full, and the caller is expected to emit an {@code OrderRejected} event with
 *       reason {@code BookFull}.
 * </ul>
 *
 * <p>Pool capacity is a constructor parameter; the launcher (APP-14) will size it per environment.
 * The default {@link #DEFAULT_CAPACITY} (65,536) is chosen to comfortably exceed the working set of
 * a busy FX RFQ venue while keeping the pool's backing arrays inside L2/L3 cache footprint.
 *
 * <p><b>Threading.</b> Not thread-safe — single-threaded cluster duty cycle only.
 */
public final class OrderBook {

  /**
   * Default capacity of the pool and the primary map. 65,536 is comfortably above a busy FX RFQ
   * working set while keeping the pool's backing arrays inside L2/L3 cache footprint. Re-tune when
   * the matching engine lands and real book depth is measured.
   */
  public static final int DEFAULT_CAPACITY = 65_536;

  private static final float MAP_LOAD_FACTOR = 0.55f;

  private final int capacity;
  private final Long2ObjectHashMap<OrderState> ordersByKey;
  private final OrderState[] pool;
  private final int[] freeList;
  private int freeTop;

  // Pre-allocated SBE flyweights for snapshot encode/decode. Reused across calls.
  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final OrderBookSnapshotEncoder snapshotEncoder = new OrderBookSnapshotEncoder();
  private final OrderBookSnapshotDecoder snapshotDecoder = new OrderBookSnapshotDecoder();

  // Scratch byte buffers for snapshot encode (pad char[] fields to fixed wire length).
  private final byte[] orderIdScratch = new byte[OrderState.ORDER_ID_LENGTH];
  private final byte[] clOrdIdScratch = new byte[OrderState.CL_ORD_ID_LENGTH];
  private final byte[] symbolScratch = new byte[OrderState.SYMBOL_LENGTH];

  /** Construct a book with {@link #DEFAULT_CAPACITY} pool slots. */
  public OrderBook() {
    this(DEFAULT_CAPACITY);
  }

  /**
   * Construct a book with the supplied pool capacity.
   *
   * @param capacity maximum number of concurrently active orders. Must be {@code > 0}.
   * @throws IllegalArgumentException if {@code capacity <= 0}
   */
  public OrderBook(final int capacity) {
    if (capacity <= 0) {
      throw new IllegalArgumentException("OrderBook capacity must be > 0, was " + capacity);
    }
    this.capacity = capacity;
    // Size the underlying map so it never rehashes within the pool's capacity (capacity /
    // load-factor > capacity). Load factor 0.55 leaves plenty of headroom.
    this.ordersByKey =
        new Long2ObjectHashMap<>((int) (capacity / MAP_LOAD_FACTOR) + 1, MAP_LOAD_FACTOR);
    this.pool = new OrderState[capacity];
    this.freeList = new int[capacity];
    for (int i = 0; i < capacity; i++) {
      pool[i] = new OrderState(i);
      // LIFO stack: slot 0 is at the top of the initial free-list so acquire() hands it out first,
      // which produces nicer test output and slightly better cache locality on startup.
      freeList[i] = capacity - 1 - i;
    }
    this.freeTop = capacity;
  }

  // ---------------------------------------------------------------------------
  // Hot-path pool API
  // ---------------------------------------------------------------------------

  /**
   * Acquire a cleared {@link OrderState} from the pool and register it under {@code orderKey}.
   * Returns {@code null} if the pool is exhausted — callers must treat {@code null} as a book-full
   * rejection and emit the appropriate event.
   *
   * <p>The returned instance has {@link OrderState#reset()} already applied; fields are in their
   * neutral state and {@code orderKey()} is pre-set. The caller populates the remaining fields
   * before handing control back to the cluster duty cycle.
   *
   * @throws IllegalStateException if an order is already registered under {@code orderKey}
   */
  public OrderState acquire(final long orderKey) {
    if (freeTop == 0) {
      return null;
    }
    if (ordersByKey.containsKey(orderKey)) {
      throw new IllegalStateException("OrderBook already contains an order with key " + orderKey);
    }
    final int slot = freeList[--freeTop];
    final OrderState state = pool[slot];
    state.reset();
    state.setOrderKey(orderKey);
    ordersByKey.put(orderKey, state);
    return state;
  }

  /** O(1) lookup by primitive key. Zero allocation. */
  public OrderState get(final long orderKey) {
    return ordersByKey.get(orderKey);
  }

  public boolean contains(final long orderKey) {
    return ordersByKey.containsKey(orderKey);
  }

  /**
   * Return an {@link OrderState} to the pool and remove it from the primary map. Idempotent —
   * calling release for an orderKey that is not currently in the book is a no-op.
   */
  public void release(final long orderKey) {
    final OrderState state = ordersByKey.remove(orderKey);
    if (state == null) {
      return;
    }
    freeList[freeTop++] = state.poolSlot();
  }

  public int size() {
    return ordersByKey.size();
  }

  public int capacity() {
    return capacity;
  }

  /**
   * Drop every order from the book and return all pooled slots to the free-list. Called by {@link
   * #restoreFrom} before decoding a snapshot fragment so a smaller snapshot does not leave orphan
   * entries from the previous state.
   */
  public void clear() {
    ordersByKey.clear();
    for (int i = 0; i < capacity; i++) {
      // LIFO re-init: after clear() the free-list is in the same order as construction.
      freeList[i] = capacity - 1 - i;
    }
    freeTop = capacity;
  }

  // ---------------------------------------------------------------------------
  // Snapshot save / restore
  // ---------------------------------------------------------------------------

  /**
   * Serialize every active order to {@code dst} starting at {@code offset} using the {@code
   * OrderBookSnapshot} (templateId 202) SBE message. Records are written in ascending {@code
   * orderKey} order for deterministic byte output across runs and across cluster leaders.
   *
   * @return total bytes written (including the SBE message header)
   */
  public int snapshotTo(final MutableDirectBuffer dst, final int offset) {
    snapshotEncoder.wrapAndApplyHeader(dst, offset, headerEncoder);
    final int recordCount = ordersByKey.size();
    final NoOrdersEncoder group = snapshotEncoder.noOrdersCount(recordCount);

    if (recordCount > 0) {
      // Drain keys into a primitive long[] and sort. Snapshot path — allocation is allowed.
      final long[] sortedKeys = new long[recordCount];
      final Long2ObjectHashMap<OrderState>.KeyIterator keyIt = ordersByKey.keySet().iterator();
      int idx = 0;
      while (keyIt.hasNext()) {
        sortedKeys[idx++] = keyIt.nextLong();
      }
      java.util.Arrays.sort(sortedKeys);

      for (int i = 0; i < recordCount; i++) {
        final long key = sortedKeys[i];
        final OrderState state = ordersByKey.get(key);
        group.next();
        state.copyOrderIdTo(orderIdScratch, 0);
        group.putOrderId(orderIdScratch, 0);
        state.copyClOrdIdTo(clOrdIdScratch, 0);
        group.putClOrdId(clOrdIdScratch, 0);
        state.copySymbolTo(symbolScratch, 0);
        group.putSymbol(symbolScratch, 0);
        group.side(state.side());
        group.ordType(state.ordType());
        group.timeInForce(state.timeInForce());
        group.price(state.price());
        group.orderQty(state.orderQty());
        group.leavesQty(state.leavesQty());
        group.cumQty(state.cumQty());
        group.accountId(state.accountId());
        group.timestamp(state.transactTime());
      }
    }

    return MessageHeaderEncoder.ENCODED_LENGTH + snapshotEncoder.encodedLength();
  }

  /**
   * Restore the book state from an {@code OrderBookSnapshot} fragment. Clears any existing state
   * before decoding so this method is safe to call on a pre-populated book.
   *
   * <p>The {@code orderKey} for each restored record is derived by parsing the 11-digit counter
   * suffix of the textual {@code orderId} (which has the form {@code "PREFIX-NNNNNNNNNNN"} assigned
   * by {@link IdGenerator}). This keeps the wire format identical to what the schema currently
   * ships — the key is metadata of the id and does not need a dedicated field.
   *
   * @return number of bytes consumed (including the SBE message header)
   * @throws IllegalStateException if a record's {@code orderId} does not match the expected id
   *     format, or if the snapshot contains more records than the book's capacity
   */
  public int restoreFrom(final DirectBuffer src, final int offset) {
    headerDecoder.wrap(src, offset);
    snapshotDecoder.wrap(
        src,
        offset + MessageHeaderDecoder.ENCODED_LENGTH,
        headerDecoder.blockLength(),
        headerDecoder.version());
    clear();

    final OrderBookSnapshotDecoder.NoOrdersDecoder group = snapshotDecoder.noOrders();
    final int count = group.count();
    if (count > capacity) {
      throw new IllegalStateException(
          "OrderBook snapshot record count " + count + " exceeds pool capacity " + capacity);
    }
    while (group.hasNext()) {
      group.next();
      group.getOrderId(orderIdScratch, 0);
      final long orderKey = parseOrderKey(orderIdScratch);
      final OrderState state = acquire(orderKey);
      if (state == null) {
        // Should be unreachable due to the count check above.
        throw new IllegalStateException("OrderBook pool exhausted during restoreFrom");
      }
      state.setOrderIdBytes(orderIdScratch, 0);
      group.getClOrdId(clOrdIdScratch, 0);
      state.setClOrdIdBytes(clOrdIdScratch, 0);
      group.getSymbol(symbolScratch, 0);
      state.setSymbolBytes(symbolScratch, 0);
      state.setSide(group.side());
      state.setOrdType(group.ordType());
      state.setTimeInForce(group.timeInForce());
      state.setPrice(group.price());
      state.setOrderQty(group.orderQty());
      state.setLeavesQty(group.leavesQty());
      state.setCumQty(group.cumQty());
      state.setAccountId(group.accountId());
      state.setTransactTime(group.timestamp());
    }

    return MessageHeaderDecoder.ENCODED_LENGTH + snapshotDecoder.encodedLength();
  }

  /**
   * Parse the monotonic counter out of an {@link IdGenerator}-formatted order id. The id has the
   * shape {@code "PREFIX-NNNNNNNNNNN"} padded with trailing zero bytes to 20 bytes total; the
   * counter is the 11 decimal digits after the last {@code '-'}.
   *
   * @throws IllegalStateException if the id does not contain a hyphen or the suffix is not 11 ASCII
   *     digits
   */
  private static long parseOrderKey(final byte[] orderId) {
    // Scan BACKWARDS from the last position that still leaves room for 11 digits after the
    // hyphen. Scanning backwards finds the last '-', which is the one that separates the prefix
    // from the counter digits even when the prefix itself contains a hyphen (IdGenerator allows
    // arbitrary ASCII in the prefix, e.g., "FX-ORD").
    final int maxHyphenInclusive = OrderState.ORDER_ID_LENGTH - ORDER_ID_DIGITS - 1;
    int hyphen = -1;
    for (int i = maxHyphenInclusive; i >= 0; i--) {
      if (orderId[i] == (byte) '-') {
        hyphen = i;
        break;
      }
    }
    if (hyphen < 0) {
      throw new IllegalStateException(
          "OrderBook snapshot orderId does not match PREFIX-NNNNNNNNNNN format");
    }
    final int digitsEnd = hyphen + 1 + ORDER_ID_DIGITS;
    long value = 0L;
    for (int i = hyphen + 1; i < digitsEnd; i++) {
      final byte b = orderId[i];
      if (b < (byte) '0' || b > (byte) '9') {
        throw new IllegalStateException(
            "OrderBook snapshot orderId counter suffix must be 11 ASCII digits");
      }
      value = value * 10L + (b - (byte) '0');
    }
    return value;
  }

  /** Number of digits in the counter suffix (matches {@link IdGenerator}). */
  private static final int ORDER_ID_DIGITS = 11;
}
