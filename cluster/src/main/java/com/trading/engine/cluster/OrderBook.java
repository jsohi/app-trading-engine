package com.trading.engine.cluster;

import org.agrona.collections.Hashing;
import org.agrona.collections.Object2ObjectHashMap;

/**
 * In-memory book of active orders keyed by exchange order id.
 *
 * <p>Minimal placeholder for the matching-engine work in later waves: provides only insert and
 * lookup. Backed by Agrona's {@link Object2ObjectHashMap} (no per-entry allocations — unlike {@code
 * java.util.HashMap} which allocates a {@code Map.Entry} per put) per the cluster's no-{@code
 * java.util.*}-collections rule.
 *
 * <p>The map is pre-sized to {@link #INITIAL_CAPACITY} to avoid rehash-induced latency spikes in
 * the cluster duty cycle. Sized for typical active-order working sets — when the matching engine
 * lands this should be re-tuned (or made configurable) against measured book depth.
 *
 * <p>Not thread-safe — single-threaded cluster duty cycle only.
 */
public final class OrderBook {

  /** Initial capacity for the underlying map; sized to avoid rehash in normal operation. */
  public static final int INITIAL_CAPACITY = 1024;

  private final Object2ObjectHashMap<String, OrderState> ordersById =
      new Object2ObjectHashMap<>(INITIAL_CAPACITY, Hashing.DEFAULT_LOAD_FACTOR);

  /**
   * Insert an order. If an order with the same id already exists it is replaced; callers should not
   * rely on this for cancel/replace semantics — that flow goes through dedicated handlers.
   *
   * @param orderId exchange order id assigned by {@link IdGenerator}
   * @param order order state to store
   */
  public void addOrder(String orderId, OrderState order) {
    ordersById.put(orderId, order);
  }

  /** Returns the order with the given id, or {@code null} if no such order exists. */
  public OrderState getOrder(String orderId) {
    return ordersById.get(orderId);
  }

  /** Number of orders currently in the book. */
  public int size() {
    return ordersById.size();
  }
}
