package com.trading.engine.cluster;

import org.agrona.collections.Object2ObjectHashMap;

/**
 * In-memory book of active orders keyed by exchange order id.
 *
 * <p>Minimal placeholder for the matching-engine work in later waves: provides only insert and
 * lookup. Backed by Agrona's {@link Object2ObjectHashMap} (zero-rehash, no boxing) per the
 * cluster's no-{@code java.util.*}-collections rule.
 *
 * <p>Not thread-safe — single-threaded cluster duty cycle only.
 */
public final class OrderBook {

  private final Object2ObjectHashMap<String, OrderState> ordersById = new Object2ObjectHashMap<>();

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
