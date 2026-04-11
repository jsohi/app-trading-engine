package com.trading.engine.projections.order;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.trading.engine.messages.sbe.ExecTypeEnum;
import com.trading.engine.messages.sbe.OrdStatusEnum;
import com.trading.engine.messages.sbe.OrderCanceledEventDecoder;
import com.trading.engine.messages.sbe.OrderCreatedEventDecoder;
import com.trading.engine.messages.sbe.OrderFilledEventDecoder;
import com.trading.engine.messages.sbe.OrderRejectedEventDecoder;
import com.trading.engine.projections.ByteArrayKey;
import com.trading.engine.projections.Projection;
import com.trading.engine.projections.SymbolPacker;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.StampedLock;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.Object2ObjectHashMap;
import org.agrona.collections.ObjectHashSet;

/**
 * CQRS read-model projection tracking the full order lifecycle. Consumes {@code OrderCreatedEvent}
 * (100), {@code OrderRejectedEvent} (101), {@code OrderFilledEvent} (102), and {@code
 * OrderCanceledEvent} (103) from the cluster event stream.
 *
 * <p><b>Indexes:</b> four indexes are maintained for efficient query access:
 *
 * <ol>
 *   <li>Primary: orderId → {@link OrderView} ({@link Object2ObjectHashMap})
 *   <li>Secondary: clOrdId → {@link OrderView} ({@link Object2ObjectHashMap})
 *   <li>Tertiary: accountCode → {@link ObjectHashSet}{@code <OrderView>} ({@link
 *       Object2ObjectHashMap})
 *   <li>Quaternary: packed symbol (long) → {@link ObjectHashSet}{@code <OrderView>} ({@link
 *       Long2ObjectHashMap})
 * </ol>
 *
 * <p><b>Threading:</b> single-writer / multi-reader via {@link StampedLock}. The event-dispatch
 * thread acquires the write stamp in {@link #onEvent}. Query threads acquire optimistic or
 * pessimistic read stamps in query methods. Query methods return immutable {@link OrderSnapshot}
 * records — internal mutable {@link OrderView} instances are never leaked.
 *
 * <p><b>Allocation:</b> bounded per-entity allocation on the event path (one {@link OrderView} per
 * order, one {@link ByteArrayKey#copyOf()} per map entry). Zero allocation on lookups via
 * pre-allocated probe keys. Query methods allocate snapshots and lists (acceptable — off hot path).
 *
 * <p><b>Error handling:</b> all event processing is wrapped in a try-catch. Decode errors increment
 * {@link #errorCount()} and log via GFLog. The event is skipped (not rethrown) to prevent crashing
 * the {@link com.trading.engine.projections.EventConsumer}. {@link #lastProcessedSequence()} is
 * updated even on error.
 *
 * <p><b>VWAP calculation:</b> average fill price uses 128-bit intermediate arithmetic via {@link
 * Math#multiplyHigh(long, long)} to prevent overflow for large FX notionals.
 *
 * @see OrderView
 * @see OrderSnapshot
 * @see com.trading.engine.projections.EventConsumer
 */
public final class OrderProjection implements Projection {

  private static final Log LOG = LogFactory.getLog(OrderProjection.class);
  private static final long PRICE_SCALE = 100_000_000L;
  private static final float LOAD_FACTOR = 0.65f;

  // --- Primary and secondary indexes ---
  private final Object2ObjectHashMap<ByteArrayKey, OrderView> byOrderId;
  private final Object2ObjectHashMap<ByteArrayKey, OrderView> byClOrdId;
  private final Object2ObjectHashMap<ByteArrayKey, ObjectHashSet<OrderView>> byAccountCode;
  private final Long2ObjectHashMap<ObjectHashSet<OrderView>> bySymbol;

  // --- Pre-allocated SBE flyweight decoders (reused per event) ---
  private final OrderCreatedEventDecoder createdDecoder = new OrderCreatedEventDecoder();
  private final OrderRejectedEventDecoder rejectedDecoder = new OrderRejectedEventDecoder();
  private final OrderFilledEventDecoder filledDecoder = new OrderFilledEventDecoder();
  private final OrderCanceledEventDecoder canceledDecoder = new OrderCanceledEventDecoder();

  // --- Pre-allocated probe keys (event-thread only) ---
  private final ByteArrayKey probeOrderId = ByteArrayKey.emptyForLookup(20);
  private final ByteArrayKey probeClOrdId = ByteArrayKey.emptyForLookup(20);
  private final ByteArrayKey probeAccountCode = ByteArrayKey.emptyForLookup(16);

  // --- Pre-allocated scratch byte arrays for SBE field decoding ---
  private final byte[] scratchOrderId = new byte[20];
  private final byte[] scratchClOrdId = new byte[20];
  private final byte[] scratchSymbol = new byte[8];
  private final byte[] scratchAccountCode = new byte[16];
  private final byte[] scratchExecId = new byte[20];
  private final byte[] scratchSettlDate = new byte[8];
  private final byte[] scratchCurrency = new byte[3];
  private final byte[] scratchSettlCurrency = new byte[3];

  // --- Concurrency ---
  private final StampedLock lock = new StampedLock();

  // --- Counters ---
  private long lastProcessedSeqNo;
  private long eventsProcessed;
  private long errorCount;

  public OrderProjection() {
    this(4096);
  }

  /**
   * Creates an OrderProjection with the specified initial capacity for the primary index.
   *
   * @param initialCapacity expected number of orders (determines initial map sizes)
   */
  public OrderProjection(final int initialCapacity) {
    byOrderId = new Object2ObjectHashMap<>(initialCapacity, LOAD_FACTOR);
    byClOrdId = new Object2ObjectHashMap<>(initialCapacity, LOAD_FACTOR);
    byAccountCode = new Object2ObjectHashMap<>(256, LOAD_FACTOR);
    bySymbol = new Long2ObjectHashMap<>(64, LOAD_FACTOR);
  }

  // ---------------------------------------------------------------------------
  // Projection interface
  // ---------------------------------------------------------------------------

  @Override
  public void onEvent(
      final long seqNo,
      final int eventType,
      final DirectBuffer buffer,
      final int offset,
      final int length) {
    final long stamp = lock.writeLock();
    try {
      switch (eventType) {
        case OrderCreatedEventDecoder.TEMPLATE_ID -> onOrderCreated(seqNo, buffer, offset, length);
        case OrderRejectedEventDecoder.TEMPLATE_ID ->
            onOrderRejected(seqNo, buffer, offset, length);
        case OrderFilledEventDecoder.TEMPLATE_ID -> onOrderFilled(seqNo, buffer, offset, length);
        case OrderCanceledEventDecoder.TEMPLATE_ID ->
            onOrderCanceled(seqNo, buffer, offset, length);
        default -> {
          /* EventConsumer only dispatches registered types */
        }
      }
      eventsProcessed++;
    } catch (final Exception e) {
      errorCount++;
      LOG.error()
          .append("OrderProjection decode error seqNo=")
          .append(seqNo)
          .append(" eventType=")
          .append(eventType)
          .commit();
    } finally {
      lastProcessedSeqNo = seqNo;
      lock.unlockWrite(stamp);
    }
  }

  @Override
  public long lastProcessedSequence() {
    return lastProcessedSeqNo;
  }

  @Override
  public void reset() {
    final long stamp = lock.writeLock();
    try {
      byOrderId.clear();
      byClOrdId.clear();
      byAccountCode.clear();
      bySymbol.clear();
      lastProcessedSeqNo = 0;
      eventsProcessed = 0;
      errorCount = 0;
      LOG.info().append("OrderProjection reset").commit();
    } finally {
      lock.unlockWrite(stamp);
    }
  }

  // ---------------------------------------------------------------------------
  // Event handlers (called under write lock)
  // ---------------------------------------------------------------------------

  private void onOrderCreated(
      final long seqNo, final DirectBuffer buffer, final int offset, final int length) {
    createdDecoder.wrap(
        buffer,
        offset,
        OrderCreatedEventDecoder.BLOCK_LENGTH,
        OrderCreatedEventDecoder.SCHEMA_VERSION);

    final int orderIdLen = sbeStrLen(createdDecoder.getOrderId(scratchOrderId, 0), scratchOrderId);
    final int clOrdIdLen = sbeStrLen(createdDecoder.getClOrdId(scratchClOrdId, 0), scratchClOrdId);
    final int symbolLen = sbeStrLen(createdDecoder.getSymbol(scratchSymbol, 0), scratchSymbol);
    final int accountLen =
        sbeStrLen(createdDecoder.getAccountCode(scratchAccountCode, 0), scratchAccountCode);

    // Check for duplicate orderId — remove old view from secondary indexes before overwriting
    probeOrderId.set(scratchOrderId, 0, orderIdLen);
    final OrderView existing = byOrderId.get(probeOrderId);
    if (existing != null) {
      removeFromSecondaryIndexes(existing);
    }

    final OrderView view = new OrderView();
    view.setOrderId(scratchOrderId, 0, orderIdLen);
    view.setClOrdId(scratchClOrdId, 0, clOrdIdLen);
    view.setSymbol(scratchSymbol, 0, symbolLen);
    view.setAccountCode(scratchAccountCode, 0, accountLen);
    view.setSide(createdDecoder.side());
    view.setOrdType(createdDecoder.ordType());
    view.setOrdStatus(OrdStatusEnum.New);
    view.setExecType(ExecTypeEnum.New);
    view.setProductType(createdDecoder.productType());
    view.setPrice(createdDecoder.price());
    view.setOrderQty(createdDecoder.orderQty());
    view.setLeavesQty(createdDecoder.orderQty());
    view.setCumQty(0);
    view.setAvgPx(0);
    view.setCumNotional(0);
    view.setSettlType(createdDecoder.settlType());
    view.setTenor(createdDecoder.tenor());
    view.setSequenceNumber(seqNo);
    view.setCreatedAt(createdDecoder.timestamp());
    view.setLastUpdatedAt(createdDecoder.timestamp());

    // Decode FX fields
    final int settlDateLen =
        sbeStrLen(createdDecoder.getSettlDate(scratchSettlDate, 0), scratchSettlDate);
    view.setSettlDate(scratchSettlDate, 0, settlDateLen);
    final int currLen = sbeStrLen(createdDecoder.getCurrency(scratchCurrency, 0), scratchCurrency);
    view.setCurrency(scratchCurrency, 0, currLen);
    final int settlCurrLen =
        sbeStrLen(createdDecoder.getSettlCurrency(scratchSettlCurrency, 0), scratchSettlCurrency);
    view.setSettlCurrency(scratchSettlCurrency, 0, settlCurrLen);

    // Index in all 4 maps
    byOrderId.put(probeOrderId.copyOf(), view);

    probeClOrdId.set(scratchClOrdId, 0, clOrdIdLen);
    byClOrdId.put(probeClOrdId.copyOf(), view);

    probeAccountCode.set(scratchAccountCode, 0, accountLen);
    ObjectHashSet<OrderView> accountSet = byAccountCode.get(probeAccountCode);
    if (accountSet == null) {
      accountSet = new ObjectHashSet<>(16, LOAD_FACTOR);
      byAccountCode.put(probeAccountCode.copyOf(), accountSet);
    }
    accountSet.add(view);

    final long symbolPacked = SymbolPacker.pack(scratchSymbol, 0);
    ObjectHashSet<OrderView> symbolSet = bySymbol.get(symbolPacked);
    if (symbolSet == null) {
      symbolSet = new ObjectHashSet<>(64, LOAD_FACTOR);
      bySymbol.put(symbolPacked, symbolSet);
    }
    symbolSet.add(view);
  }

  private void onOrderRejected(
      final long seqNo, final DirectBuffer buffer, final int offset, final int length) {
    rejectedDecoder.wrap(
        buffer,
        offset,
        OrderRejectedEventDecoder.BLOCK_LENGTH,
        OrderRejectedEventDecoder.SCHEMA_VERSION);

    final int clOrdIdLen = sbeStrLen(rejectedDecoder.getClOrdId(scratchClOrdId, 0), scratchClOrdId);
    final int symbolLen = sbeStrLen(rejectedDecoder.getSymbol(scratchSymbol, 0), scratchSymbol);
    final int accountLen =
        sbeStrLen(rejectedDecoder.getAccountCode(scratchAccountCode, 0), scratchAccountCode);

    final OrderView view = new OrderView();
    view.setClOrdId(scratchClOrdId, 0, clOrdIdLen);
    view.setSymbol(scratchSymbol, 0, symbolLen);
    view.setAccountCode(scratchAccountCode, 0, accountLen);
    view.setSide(rejectedDecoder.side());
    view.setOrdStatus(OrdStatusEnum.Rejected);
    view.setExecType(ExecTypeEnum.Rejected);
    view.setRejectReason(rejectedDecoder.rejectReason());
    view.setProductType(rejectedDecoder.productType());
    // FX fields (settlDate, currency, settlCurrency, tenor, settlType) not on template 101
    view.setSequenceNumber(seqNo);
    view.setCreatedAt(rejectedDecoder.timestamp());
    view.setLastUpdatedAt(rejectedDecoder.timestamp());

    // Index by clOrdId and accountCode only (no orderId on rejection)
    probeClOrdId.set(scratchClOrdId, 0, clOrdIdLen);
    byClOrdId.put(probeClOrdId.copyOf(), view);

    probeAccountCode.set(scratchAccountCode, 0, accountLen);
    ObjectHashSet<OrderView> accountSet = byAccountCode.get(probeAccountCode);
    if (accountSet == null) {
      accountSet = new ObjectHashSet<>(16, LOAD_FACTOR);
      byAccountCode.put(probeAccountCode.copyOf(), accountSet);
    }
    accountSet.add(view);
  }

  private void onOrderFilled(
      final long seqNo, final DirectBuffer buffer, final int offset, final int length) {
    filledDecoder.wrap(
        buffer,
        offset,
        OrderFilledEventDecoder.BLOCK_LENGTH,
        OrderFilledEventDecoder.SCHEMA_VERSION);

    final int orderIdLen = sbeStrLen(filledDecoder.getOrderId(scratchOrderId, 0), scratchOrderId);
    probeOrderId.set(scratchOrderId, 0, orderIdLen);
    final OrderView view = byOrderId.get(probeOrderId);
    if (view == null) {
      return; // Unknown orderId — silently skip (projection may have been reset mid-stream)
    }

    final long lastPx = filledDecoder.lastPx();
    final long lastQty = filledDecoder.lastQty();
    final long newLeavesQty = filledDecoder.leavesQty();
    final long newCumQty = filledDecoder.cumQty();

    view.setLeavesQty(newLeavesQty);
    view.setCumQty(newCumQty);

    // VWAP: cumNotional += mulDiv(lastPx, lastQty, PRICE_SCALE)
    final long fillNotional = mulDiv(lastPx, lastQty, PRICE_SCALE);
    final long newCumNotional = view.cumNotional() + fillNotional;
    view.setCumNotional(newCumNotional);
    if (newCumQty > 0) {
      view.setAvgPx(mulDiv(newCumNotional, PRICE_SCALE, newCumQty));
    }

    // ExecId
    final int execIdLen = sbeStrLen(filledDecoder.getExecId(scratchExecId, 0), scratchExecId);
    view.setLastExecId(scratchExecId, 0, execIdLen);

    view.setOrdStatus(newLeavesQty == 0 ? OrdStatusEnum.Filled : OrdStatusEnum.PartiallyFilled);
    view.setExecType(newLeavesQty == 0 ? ExecTypeEnum.Fill : ExecTypeEnum.PartialFill);
    view.setSequenceNumber(seqNo);
    view.setLastUpdatedAt(filledDecoder.timestamp());
  }

  private void onOrderCanceled(
      final long seqNo, final DirectBuffer buffer, final int offset, final int length) {
    canceledDecoder.wrap(
        buffer,
        offset,
        OrderCanceledEventDecoder.BLOCK_LENGTH,
        OrderCanceledEventDecoder.SCHEMA_VERSION);

    final int orderIdLen = sbeStrLen(canceledDecoder.getOrderId(scratchOrderId, 0), scratchOrderId);
    probeOrderId.set(scratchOrderId, 0, orderIdLen);
    final OrderView view = byOrderId.get(probeOrderId);
    if (view == null) {
      return; // Unknown orderId — silently skip
    }

    view.setOrdStatus(OrdStatusEnum.Canceled);
    view.setExecType(ExecTypeEnum.Canceled);
    view.setSequenceNumber(seqNo);
    view.setLastUpdatedAt(canceledDecoder.timestamp());
  }

  // ---------------------------------------------------------------------------
  // Index maintenance
  // ---------------------------------------------------------------------------

  /**
   * Removes an existing OrderView from the clOrdId, account, and symbol secondary indexes. Called
   * before overwriting on duplicate orderId during replay.
   */
  private void removeFromSecondaryIndexes(final OrderView view) {
    // Remove from clOrdId index
    probeClOrdId.set(view.clOrdId(), 0, view.clOrdIdLen());
    byClOrdId.remove(probeClOrdId);

    // Remove from account index
    probeAccountCode.set(view.accountCode(), 0, view.accountCodeLen());
    final ObjectHashSet<OrderView> accountSet = byAccountCode.get(probeAccountCode);
    if (accountSet != null) {
      accountSet.remove(view);
    }

    // Remove from symbol index
    final long symbolPacked = SymbolPacker.pack(view.symbol(), 0);
    final ObjectHashSet<OrderView> symbolSet = bySymbol.get(symbolPacked);
    if (symbolSet != null) {
      symbolSet.remove(view);
    }
  }

  // ---------------------------------------------------------------------------
  // Query methods (acquire read stamp, return immutable snapshots)
  // ---------------------------------------------------------------------------

  /**
   * Looks up an order by exchange order identifier.
   *
   * @param orderId the exchange order ID (FIX tag 37)
   * @return the order snapshot, or {@code null} if not found
   */
  public OrderSnapshot getOrder(final String orderId) {
    final ByteArrayKey key = keyFromString(orderId, 20);
    long stamp = lock.tryOptimisticRead();
    OrderView view = byOrderId.get(key);
    OrderSnapshot result = view != null ? OrderSnapshot.from(view) : null;
    if (!lock.validate(stamp)) {
      stamp = lock.readLock();
      try {
        view = byOrderId.get(key);
        result = view != null ? OrderSnapshot.from(view) : null;
      } finally {
        lock.unlockRead(stamp);
      }
    }
    return result;
  }

  /**
   * Looks up an order by client order identifier.
   *
   * @param clOrdId the client order ID (FIX tag 11)
   * @return the order snapshot, or {@code null} if not found
   */
  public OrderSnapshot getOrderByClOrdId(final String clOrdId) {
    final ByteArrayKey key = keyFromString(clOrdId, 20);
    long stamp = lock.tryOptimisticRead();
    OrderView view = byClOrdId.get(key);
    OrderSnapshot result = view != null ? OrderSnapshot.from(view) : null;
    if (!lock.validate(stamp)) {
      stamp = lock.readLock();
      try {
        view = byClOrdId.get(key);
        result = view != null ? OrderSnapshot.from(view) : null;
      } finally {
        lock.unlockRead(stamp);
      }
    }
    return result;
  }

  /**
   * Returns all orders for the given account.
   *
   * @param accountCode the account code (FIX tag 1)
   * @return list of order snapshots (empty if no orders for this account)
   */
  public List<OrderSnapshot> getOrdersByAccount(final String accountCode) {
    final ByteArrayKey key = keyFromString(accountCode, 16);
    final List<OrderSnapshot> result = new ArrayList<>();
    long stamp = lock.tryOptimisticRead();
    collectAccountOrders(key, result);
    if (!lock.validate(stamp)) {
      result.clear();
      stamp = lock.readLock();
      try {
        collectAccountOrders(key, result);
      } finally {
        lock.unlockRead(stamp);
      }
    }
    return result;
  }

  /**
   * Returns all orders for the given symbol.
   *
   * @param symbol the instrument symbol (FIX tag 55)
   * @return list of order snapshots (empty if no orders for this symbol)
   */
  public List<OrderSnapshot> getOrdersBySymbol(final String symbol) {
    final long symbolPacked = SymbolPacker.pack(symbol);
    final List<OrderSnapshot> result = new ArrayList<>();
    long stamp = lock.tryOptimisticRead();
    collectSymbolOrders(symbolPacked, result);
    if (!lock.validate(stamp)) {
      result.clear();
      stamp = lock.readLock();
      try {
        collectSymbolOrders(symbolPacked, result);
      } finally {
        lock.unlockRead(stamp);
      }
    }
    return result;
  }

  /**
   * Returns all orders in an active (non-terminal) state.
   *
   * @return list of order snapshots with status New or PartiallyFilled
   */
  public List<OrderSnapshot> getActiveOrders() {
    final List<OrderSnapshot> result = new ArrayList<>();
    long stamp = lock.tryOptimisticRead();
    collectActiveOrders(result);
    if (!lock.validate(stamp)) {
      result.clear();
      stamp = lock.readLock();
      try {
        collectActiveOrders(result);
      } finally {
        lock.unlockRead(stamp);
      }
    }
    return result;
  }

  /**
   * Returns the total number of orders tracked (all statuses).
   *
   * @return the order count
   */
  public int size() {
    final long stamp = lock.readLock();
    try {
      return byOrderId.size() + countRejects();
    } finally {
      lock.unlockRead(stamp);
    }
  }

  /**
   * Returns the total number of events that caused a decode or processing error.
   *
   * @return the error count
   */
  public long errorCount() {
    final long stamp = lock.readLock();
    try {
      return errorCount;
    } finally {
      lock.unlockRead(stamp);
    }
  }

  /**
   * Returns the total number of events successfully processed.
   *
   * @return the events processed count
   */
  public long eventsProcessed() {
    final long stamp = lock.readLock();
    try {
      return eventsProcessed;
    } finally {
      lock.unlockRead(stamp);
    }
  }

  // ---------------------------------------------------------------------------
  // Query helpers (called under read stamp — may be invalid for optimistic reads)
  // ---------------------------------------------------------------------------

  private void collectAccountOrders(final ByteArrayKey key, final List<OrderSnapshot> result) {
    final ObjectHashSet<OrderView> set = byAccountCode.get(key);
    if (set != null) {
      set.forEach(v -> result.add(OrderSnapshot.from(v)));
    }
  }

  private void collectSymbolOrders(final long symbolPacked, final List<OrderSnapshot> result) {
    final ObjectHashSet<OrderView> set = bySymbol.get(symbolPacked);
    if (set != null) {
      set.forEach(v -> result.add(OrderSnapshot.from(v)));
    }
  }

  private void collectActiveOrders(final List<OrderSnapshot> result) {
    byOrderId
        .values()
        .forEach(
            v -> {
              if (v.isActive()) {
                result.add(OrderSnapshot.from(v));
              }
            });
  }

  /**
   * Count rejected orders (in clOrdId index but not in orderId index). Rejected orders have no
   * orderId so they only appear in the clOrdId secondary index.
   */
  private int countRejects() {
    int rejectCount = 0;
    for (final OrderView v : byClOrdId.values()) {
      if (v.ordStatus() == OrdStatusEnum.Rejected) {
        rejectCount++;
      }
    }
    return rejectCount;
  }

  // ---------------------------------------------------------------------------
  // Arithmetic — 128-bit safe multiply-divide for VWAP
  // ---------------------------------------------------------------------------

  /**
   * Computes {@code (a * b) / divisor} using 128-bit intermediate to avoid overflow. All values
   * must be non-negative. Divisor must be positive.
   *
   * <p>Fast path: if the product {@code a * b} fits in a signed long, uses direct division. Slow
   * path: uses {@link java.math.BigInteger} for exact 128-bit arithmetic. The slow path allocates,
   * but is only hit for large notionals (> ~92 billion units) and is acceptable on the
   * event-dispatch path since it occurs at most once per fill.
   *
   * @param a non-negative multiplicand
   * @param b non-negative multiplier
   * @param divisor positive divisor
   * @return the quotient, or 0 if any input is non-positive
   */
  static long mulDiv(final long a, final long b, final long divisor) {
    if (a <= 0 || b <= 0 || divisor <= 0) {
      return 0;
    }
    final long hi = Math.multiplyHigh(a, b);
    final long lo = a * b; // lower 64 bits (unsigned wraparound)

    // Fast path: product fits in signed long — no overflow
    if (hi == 0 && lo >= 0) {
      return lo / divisor;
    }

    // Slow path: 128-bit arithmetic via BigInteger.
    // This allocates but is only reached for large FX notionals (500M+ units at typical prices).
    // Acceptable on the projection event path — not on the cluster matching-engine hot path.
    final java.math.BigInteger product =
        java.math.BigInteger.valueOf(a).multiply(java.math.BigInteger.valueOf(b));
    return product.divide(java.math.BigInteger.valueOf(divisor)).longValueExact();
  }

  // ---------------------------------------------------------------------------
  // Utilities
  // ---------------------------------------------------------------------------

  /**
   * Computes the actual string length in an SBE fixed-length char field by scanning for the first
   * NUL byte. SBE pads shorter strings with 0x00.
   */
  private static int sbeStrLen(final int fieldLength, final byte[] data) {
    int end = fieldLength;
    while (end > 0 && data[end - 1] == 0) {
      end--;
    }
    return end;
  }

  /**
   * Creates a ByteArrayKey from a String, NUL-padded to the given maxLength. Used on the query path
   * (allocation acceptable).
   */
  private static ByteArrayKey keyFromString(final String value, final int maxLength) {
    final byte[] padded = new byte[maxLength];
    final byte[] ascii = value.getBytes(StandardCharsets.US_ASCII);
    final int copyLen = Math.min(ascii.length, maxLength);
    System.arraycopy(ascii, 0, padded, 0, copyLen);
    return ByteArrayKey.copyOf(padded, 0, sbeStrLen(maxLength, padded));
  }
}
