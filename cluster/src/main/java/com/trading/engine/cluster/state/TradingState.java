package com.trading.engine.cluster.state;

import com.trading.engine.cluster.IdGenerator;
import com.trading.engine.cluster.OrderBook;
import com.trading.engine.cluster.OrderState;
import com.trading.engine.messages.sbe.OrdStatusEnum;
import com.trading.engine.messages.sbe.OrdTypeEnum;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.messages.sbe.TimeInForceEnum;
import java.util.Objects;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Event-sourced order lifecycle state. Wraps the {@link OrderBook} (pre-allocated order pool) and
 * {@link IdGenerator} instances (deterministic order/exec ID generation).
 *
 * <p><b>Event-sourced contract:</b> state is mutated ONLY via {@code apply*} methods, which are
 * called AFTER the corresponding domain event has been journaled via {@link
 * com.trading.engine.cluster.handler.EventSink#emit}. The event is the source of truth; state is
 * derived from it.
 *
 * <p><b>Two-phase flow for NewOrderSingle:</b>
 *
 * <ol>
 *   <li><b>Phase A (before event encoding):</b> {@link #generateOrderId()} and {@link
 *       #generateExecId()} advance the deterministic ID counters. The generated IDs are written
 *       into the event body by the handler.
 *   <li><b>Phase B (after EventSink.emit):</b> {@link #applyOrderCreated} acquires an {@link
 *       OrderState} from the pool and populates it. This is the actual state mutation.
 * </ol>
 *
 * <p><b>Crash safety:</b> Aeron Cluster's replayed log provides crash safety. If the process
 * crashes between emit and apply, the entire {@code onSessionMessage} re-executes deterministically
 * on restart. ID counters and pool state are captured in the snapshot.
 *
 * <p><b>Threading:</b> single-threaded cluster duty cycle. No synchronization required.
 *
 * <p><b>Allocation:</b> zero allocation after construction. ID scratch buffers are pre-allocated.
 *
 * <p><b>Scope:</b> wraps order lifecycle state only. Reference-data stores (AccountStore,
 * CurrencyStore, RiskLimitStore) remain in {@link
 * com.trading.engine.cluster.refdata.ReferenceDataRegistry}. The {@link
 * com.trading.engine.cluster.sequencer.EventSequencer} remains in {@link
 * com.trading.engine.cluster.handler.EventSink} — sequencing is a transport concern, not domain
 * state.
 *
 * @see com.trading.engine.cluster.handler.CommandHandler
 * @see com.trading.engine.cluster.handler.EventSink
 * @see OrderBook
 * @see IdGenerator
 */
public final class TradingState {

  private final OrderBook orderBook;
  private final IdGenerator orderIdGen;
  private final IdGenerator execIdGen;

  // Pre-allocated scratch buffers for ID generation (handler reads from these after generate*)
  private final byte[] orderIdScratch;
  private final UnsafeBuffer orderIdScratchBuffer;
  private final byte[] execIdScratch;
  private final UnsafeBuffer execIdScratchBuffer;

  /**
   * Creates a TradingState wrapping the given order book and ID generators.
   *
   * @param orderBook the pre-allocated order pool (must not be null)
   * @param orderIdGen the deterministic order ID generator (must not be null)
   * @param execIdGen the deterministic execution ID generator (must not be null)
   */
  public TradingState(
      final OrderBook orderBook, final IdGenerator orderIdGen, final IdGenerator execIdGen) {
    this.orderBook = Objects.requireNonNull(orderBook, "orderBook");
    this.orderIdGen = Objects.requireNonNull(orderIdGen, "orderIdGen");
    this.execIdGen = Objects.requireNonNull(execIdGen, "execIdGen");
    this.orderIdScratch = new byte[orderIdGen.idByteLength()];
    this.orderIdScratchBuffer = new UnsafeBuffer(orderIdScratch);
    this.execIdScratch = new byte[execIdGen.idByteLength()];
    this.execIdScratchBuffer = new UnsafeBuffer(execIdScratch);
  }

  // ---------------------------------------------------------------------------
  // Phase A: ID generation (before event encoding, deterministic)
  // ---------------------------------------------------------------------------

  /**
   * Advances the order ID counter and renders the next ID into {@link #orderIdScratch()}. Called
   * before encoding the OrderCreatedEvent so the handler can write the generated orderId into the
   * event body.
   *
   * @return the order key (counter value) for pool acquisition in {@link #applyOrderCreated}
   */
  public long generateOrderId() {
    orderIdGen.nextInto(orderIdScratchBuffer, 0);
    return orderIdGen.currentCounter();
  }

  /**
   * Advances the exec ID counter and renders the next ID into {@link #execIdScratch()}. Called
   * before encoding the OrderCreatedEvent.
   */
  public void generateExecId() {
    execIdGen.nextInto(execIdScratchBuffer, 0);
  }

  /**
   * Returns the scratch buffer containing the most recently generated order ID. The handler reads
   * from this to encode the orderId field in the OrderCreatedEvent.
   *
   * @return the order ID scratch byte array
   */
  public byte[] orderIdScratch() {
    return orderIdScratch;
  }

  /**
   * Returns the scratch buffer containing the most recently generated exec ID. The handler reads
   * from this to encode the execId field in the OrderCreatedEvent.
   *
   * @return the exec ID scratch byte array
   */
  public byte[] execIdScratch() {
    return execIdScratch;
  }

  // ---------------------------------------------------------------------------
  // Phase B: Apply event (AFTER EventSink.emit — state derived from event)
  // ---------------------------------------------------------------------------

  /**
   * Applies an OrderCreated event to the order book. Called AFTER the event has been journaled via
   * {@link com.trading.engine.cluster.handler.EventSink#emit}. Acquires a pool slot and populates
   * the {@link OrderState} from the event data.
   *
   * <p>If the pool is exhausted despite the pre-validation guard ({@link OrderBook#isFull()}), this
   * indicates an internal consistency failure — the event has already been journaled but the state
   * cannot be applied. The caller must throw {@link IllegalStateException} in this case.
   *
   * <p><b>Explicit data flow:</b> all event-derived data is passed as parameters — this method does
   * not read from shared scratch buffers. The caller is responsible for passing the orderId bytes
   * that were generated by {@link #generateOrderId()} and encoded into the event.
   *
   * @param orderKey the order key from {@link #generateOrderId()}
   * @param clusterTimestamp the cluster timestamp in epoch nanos
   * @param orderIdBytes the generated orderId bytes (from {@link #orderIdScratch()})
   * @param orderIdOffset the offset into orderIdBytes
   * @param side the order side
   * @param ordType the order type
   * @param timeInForce the time-in-force
   * @param price the order price (fixed-point 10^-8)
   * @param orderQty the order quantity (fixed-point 10^-8)
   * @param accountId the resolved account ID
   * @param clOrdId the client order ID bytes
   * @param clOrdIdOffset the offset into clOrdId
   * @param symbol the symbol bytes
   * @param symbolOffset the offset into symbol
   * @return the populated {@link OrderState}, or {@code null} if pool exhausted
   */
  public OrderState applyOrderCreated(
      final long orderKey,
      final long clusterTimestamp,
      final byte[] orderIdBytes,
      final int orderIdOffset,
      final SideEnum side,
      final OrdTypeEnum ordType,
      final TimeInForceEnum timeInForce,
      final long price,
      final long orderQty,
      final long accountId,
      final byte[] clOrdId,
      final int clOrdIdOffset,
      final byte[] symbol,
      final int symbolOffset) {

    final var state = orderBook.acquire(orderKey);
    if (state == null) {
      return null; // Pool exhausted — caller must throw IllegalStateException
    }

    state.setOrderIdBytes(orderIdBytes, orderIdOffset);
    state.setClOrdIdBytes(clOrdId, clOrdIdOffset);
    state.setSymbolBytes(symbol, symbolOffset);
    state.setAccountId(accountId);
    state.setSide(side);
    state.setOrdType(ordType);
    state.setTimeInForce(timeInForce);
    state.setPrice(price);
    state.setOrderQty(orderQty);
    state.setLeavesQty(orderQty);
    state.setCumQty(0);
    state.setOrdStatus(OrdStatusEnum.New);
    state.setTransactTime(clusterTimestamp);

    return state;
  }

  /**
   * Applies an OrderRejected event. No-op — rejected orders have no state in the order book. Exists
   * for structural completeness of the event-sourced pattern. Future rejection tracking (rate
   * limiting, audit counters) belongs in projections, not core state.
   */
  public void applyOrderRejected() {
    // Intentional no-op
  }

  // ---------------------------------------------------------------------------
  // Public query accessors (for handlers in other packages)
  // ---------------------------------------------------------------------------

  /**
   * Returns {@code true} if the order book pool has no free slots. Handlers call this before
   * generating IDs to avoid wasting deterministic ID counter space on an order that cannot be
   * admitted.
   *
   * @return {@code true} if the order book is at capacity
   */
  public boolean isOrderBookFull() {
    return orderBook.isFull();
  }

  // ---------------------------------------------------------------------------
  // Snapshot accessors (public — needed by TradingClusteredService in a different package)
  // ---------------------------------------------------------------------------

  /**
   * Returns the order book for snapshot encode/decode. Not intended for handler use — handlers
   * interact with state exclusively through {@link #generateOrderId()}, {@link #generateExecId()},
   * {@link #applyOrderCreated}, and {@link #isOrderBookFull()}.
   *
   * @return the order book
   */
  public OrderBook orderBook() {
    return orderBook;
  }

  /**
   * Returns the order ID generator for snapshot encode/decode (prefix + counter serialization).
   *
   * @return the order ID generator
   */
  public IdGenerator orderIdGen() {
    return orderIdGen;
  }

  /**
   * Returns the exec ID generator for snapshot encode/decode (prefix + counter serialization).
   *
   * @return the exec ID generator
   */
  public IdGenerator execIdGen() {
    return execIdGen;
  }

  // ---------------------------------------------------------------------------
  // Snapshot delegation
  // ---------------------------------------------------------------------------

  /**
   * Serializes the order book to the destination buffer.
   *
   * @param dst the destination buffer
   * @param offset the start offset
   * @return the number of bytes written
   */
  public int snapshotOrderBookTo(final MutableDirectBuffer dst, final int offset) {
    return orderBook.snapshotTo(dst, offset);
  }

  /**
   * Restores the order book from a snapshot fragment.
   *
   * @param src the source buffer
   * @param offset the start offset
   * @return the number of bytes consumed
   */
  public int restoreOrderBookFrom(final DirectBuffer src, final int offset) {
    return orderBook.restoreFrom(src, offset);
  }

  /**
   * Clears the order book. Called during snapshot restore reset ({@code
   * referenceDataRegistry.resetAll()} handles ref-data stores separately).
   */
  public void clearOrderBook() {
    orderBook.clear();
  }
}
