package com.trading.engine.fixbridge.transport;

import com.trading.engine.fixbridge.json.BrowserEvent;

/**
 * Bounded per-session outbound event queue with priority-aware drop policy (§3.1).
 *
 * <p><b>Purpose.</b> Decouples the bridge's command-handling thread from the slower browser-side
 * Netty write path. When the WebSocket peer falls behind (a slow consumer or a backpressured
 * channel), this queue buffers up to {@link #DEFAULT_CAPACITY} events; once that bound is hit
 * the priority-aware drop policy preserves order-critical events at the cost of debug-only
 * {@code RawFix} frames.
 *
 * <p><b>Drop policy (§3.1 step 5).</b>
 *
 * <ul>
 *   <li>{@code ExecutionReport}, {@code OrderReject}, {@code BridgeStatus}, {@code AuthExpired},
 *       {@code Error}, {@code AccountLimits}, {@code SessionTerminated},
 *       {@code OrderReconciled}, {@code OrderStatusReply}, {@code Quote} — <b>never dropped</b>.
 *       These carry order/quote semantics or session-state signals; the trader's correctness
 *       depends on them.
 *   <li>{@code RawFix} — dropped first when capacity is exhausted (oldest-first). The drop is
 *       silent at the queue layer; the {@code RawFixTap} caller increments the {@code
 *       fixbridge_rawfix_dropped_total} counter on each rejection.
 *   <li>If the queue is full AND no {@code RawFix} entries can be dropped (worst case — every
 *       slot is critical), {@link #offer(BrowserEvent)} returns {@link OfferResult#TERMINAL},
 *       which the caller MUST escalate to {@code BridgeStatus{fatal:true,reason:"outbound-
 *       overflow"}} and channel close (§3.1 step 5).
 * </ul>
 *
 * <p><b>Threading.</b> NOT thread-safe. Owned by the per-session Netty handler on the channel's
 * single-threaded event loop. Producers and consumers are both the same thread.
 *
 * <p><b>Allocation.</b> Constructor allocates the backing {@code Object[]} ring. {@link
 * #offer(BrowserEvent)} and {@link #poll()} are zero-alloc.
 *
 * <p><b>Lifecycle.</b> Per-session.
 *
 * <p><b>Dependencies.</b> JDK only.
 */
public final class OutboundQueue {

  /** Default per-session capacity — 4096 entries per §3.1. */
  public static final int DEFAULT_CAPACITY = 4096;

  /** Outcome of an {@link #offer(BrowserEvent)} attempt. */
  public enum OfferResult {
    /** Event accepted and enqueued. */
    ACCEPTED,
    /**
     * Event accepted, but the queue was full and an older {@code RawFix} entry was dropped to
     * make room. Caller MUST increment {@code fixbridge_rawfix_dropped_total}.
     */
    ACCEPTED_DROPPED_RAWFIX,
    /**
     * Queue full and no {@code RawFix} entries available to drop. Caller MUST escalate to
     * terminal-overflow {@code BridgeStatus{fatal:true,reason:"outbound-overflow"}} and close
     * the channel.
     */
    TERMINAL
  }

  private final BrowserEvent[] ring;
  private final int capacity;

  private int head;
  private int tail;
  private int size;

  /** Construct with the {@link #DEFAULT_CAPACITY} default. */
  public OutboundQueue() {
    this(DEFAULT_CAPACITY);
  }

  /**
   * Construct with a custom capacity.
   *
   * @param capacity ring size; must be {@code > 0}
   * @throws IllegalArgumentException if {@code capacity <= 0}
   */
  public OutboundQueue(final int capacity) {
    if (capacity <= 0) {
      throw new IllegalArgumentException("capacity must be > 0, was " + capacity);
    }
    this.capacity = capacity;
    this.ring = new BrowserEvent[capacity];
    this.head = 0;
    this.tail = 0;
    this.size = 0;
  }

  /**
   * Try to enqueue {@code e}. Returns one of {@link OfferResult}.
   *
   * @param e event to enqueue (non-null)
   * @return ACCEPTED / ACCEPTED_DROPPED_RAWFIX / TERMINAL — caller MUST honour the contract for
   *     each outcome
   * @throws NullPointerException if {@code e} is null
   */
  public OfferResult offer(final BrowserEvent e) {
    if (e == null) {
      throw new NullPointerException("event must not be null");
    }
    if (size < capacity) {
      ring[tail] = e;
      tail = (tail + 1) % capacity;
      size++;
      return OfferResult.ACCEPTED;
    }
    // Full. Try to drop the oldest RawFix entry.
    if (dropOldestRawFix()) {
      ring[tail] = e;
      tail = (tail + 1) % capacity;
      size++;
      return OfferResult.ACCEPTED_DROPPED_RAWFIX;
    }
    // No RawFix to drop — terminal overflow.
    return OfferResult.TERMINAL;
  }

  /**
   * Remove and return the oldest queued event, or {@code null} if empty.
   *
   * @return oldest event or null
   */
  public BrowserEvent poll() {
    if (size == 0) {
      return null;
    }
    final BrowserEvent e = ring[head];
    ring[head] = null;
    head = (head + 1) % capacity;
    size--;
    return e;
  }

  /** Number of events currently in the queue. */
  public int size() {
    return size;
  }

  /** Configured capacity. */
  public int capacity() {
    return capacity;
  }

  /**
   * Drop the oldest {@code RawFix} entry if any exists. Compacts the ring around the dropped
   * slot. O(N) worst case — fine because RawFix dropouts are rare and N is bounded by capacity.
   *
   * @return {@code true} iff a RawFix entry was found and removed
   */
  private boolean dropOldestRawFix() {
    if (size == 0) {
      return false;
    }
    // Walk from head forward and find the first RawFix.
    // Loop scan pointer mutated across the walk per CLAUDE.md carve-out.
    int idx = head;
    int searched = 0;
    while (searched < size) {
      if (ring[idx] instanceof BrowserEvent.RawFix) {
        // Found one. Compact: shift every later element left by one slot.
        compactOut(idx);
        return true;
      }
      idx = (idx + 1) % capacity;
      searched++;
    }
    return false;
  }

  /**
   * Remove the entry at {@code idx} and shift later entries one slot toward the head so the ring
   * stays contiguous. After removal, {@link #tail} moves back by one.
   */
  private void compactOut(final int idx) {
    // Walk from idx forward, shifting next slot into current.
    // Loop scan pointer mutated across the shift per CLAUDE.md carve-out.
    int cur = idx;
    int next;
    int shifted = 0;
    final int target = size - (countDistance(head, idx) + 1);
    while (shifted < target) {
      next = (cur + 1) % capacity;
      ring[cur] = ring[next];
      cur = next;
      shifted++;
    }
    ring[cur] = null;
    tail = (tail - 1 + capacity) % capacity;
    size--;
  }

  /** Distance (in ring steps) from {@code from} to {@code to}, accounting for wrap-around. */
  private int countDistance(final int from, final int to) {
    if (to >= from) {
      return to - from;
    }
    return (capacity - from) + to;
  }
}
