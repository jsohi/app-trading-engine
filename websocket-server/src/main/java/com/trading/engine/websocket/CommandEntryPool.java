package com.trading.engine.websocket;

import java.util.Objects;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;

/**
 * Dedicated pool of {@link EgressEntry} objects sized for the browser→cluster command path.
 *
 * <p>Distinct from the egress-listener pool to avoid coupling Netty event-loop allocators (multi
 * producer) to the single-threaded egress pool (Aeron-thread only). Backed by a lock-free {@link
 * ManyToOneConcurrentArrayQueue}: multiple Netty worker threads acquire entries concurrently via
 * {@link #tryAcquire()}, and the AeronEgressThread (single-consumer of the commandQueue and
 * producer for ackQueue) returns them via {@link #release(EgressEntry)}.
 *
 * <p><b>Thread safety.</b> Multi-producer ({@code tryAcquire} from any Netty worker), multi-
 * producer for {@code release} (the dispatcher releases on enqueue failure; the AeronEgressThread
 * releases after offer to cluster).
 *
 * <p><b>Allocation.</b> All entries are pre-allocated at construction time. The free queue's
 * capacity matches the pool size; offer/poll are zero-allocation.
 */
public final class CommandEntryPool {

  private final ManyToOneConcurrentArrayQueue<EgressEntry> free;
  private final int capacity;

  /**
   * Create a pool of pre-allocated entries.
   *
   * @param capacity number of entries; must be a positive power of two
   * @param maxMessageSize backing array size for each entry
   */
  public CommandEntryPool(final int capacity, final int maxMessageSize) {
    if (capacity <= 0 || Integer.bitCount(capacity) != 1) {
      throw new IllegalArgumentException(
          "capacity must be a positive power of two, got: " + capacity);
    }
    this.capacity = capacity;
    this.free = new ManyToOneConcurrentArrayQueue<>(capacity);
    for (int i = 0; i < capacity; i++) {
      free.offer(new EgressEntry(maxMessageSize));
    }
  }

  /**
   * Try to acquire a free entry.
   *
   * @return an entry, or {@code null} if the pool is exhausted
   */
  public EgressEntry tryAcquire() {
    return free.poll();
  }

  /**
   * Release an entry back to the pool.
   *
   * @param entry the entry to release; must not be {@code null}
   */
  public void release(final EgressEntry entry) {
    Objects.requireNonNull(entry, "entry");
    entry.resetForPool();
    if (!free.offer(entry)) {
      // Should never happen: free queue capacity equals pool size.
      throw new IllegalStateException("CommandEntryPool overflow on release");
    }
  }

  /**
   * @return the configured pool capacity
   */
  public int capacity() {
    return capacity;
  }

  /**
   * @return the number of entries currently free (approximate under concurrency)
   */
  public int available() {
    return free.size();
  }
}
