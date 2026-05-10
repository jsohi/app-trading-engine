package com.trading.engine.fixbridge.transport;

import com.trading.engine.fixbridge.json.BrowserEvent;

/**
 * Bounded per-session outbound event queue with priority-aware drop policy (§3.1).
 *
 * <p><b>Purpose.</b> Decouples the bridge's command-handling thread from the slower browser-side
 * Netty write path. When the WebSocket peer falls behind (a slow consumer or a backpressured
 * channel), this queue buffers up to {@link #DEFAULT_CAPACITY} events; once that bound is hit the
 * priority-aware drop policy preserves order-critical events at the cost of debug-only {@code
 * RawFix} frames.
 *
 * <p><b>Drop policy (§3.1 step 5).</b>
 *
 * <ul>
 *   <li>{@code ExecutionReport}, {@code OrderReject}, {@code BridgeStatus}, {@code AuthExpired},
 *       {@code Error}, {@code AccountLimits}, {@code SessionTerminated}, {@code OrderReconciled},
 *       {@code OrderStatusReply}, {@code Quote} — <b>never dropped</b>. These carry order/quote
 *       semantics or session-state signals; the trader's correctness depends on them.
 *   <li>{@code RawFix} — dropped first when capacity is exhausted (oldest-first). The drop is
 *       silent at the queue layer; the {@code RawFixTap} caller increments the {@code
 *       fixbridge_rawfix_dropped_total} counter on each rejection.
 *   <li>If the queue is full AND no {@code RawFix} entries can be dropped (worst case — every slot
 *       is critical), {@link #offer(BrowserEvent)} returns {@link OfferResult#TERMINAL}, which the
 *       caller MUST escalate to {@code BridgeStatus{fatal:true,reason:"outbound- overflow"}} and
 *       channel close (§3.1 step 5).
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
     * Event accepted, but the queue was full and an older {@code RawFix} entry was dropped to make
     * room. Caller MUST increment {@code fixbridge_rawfix_dropped_total}.
     */
    ACCEPTED_DROPPED_RAWFIX,
    /**
     * Queue full and no {@code RawFix} entries available to drop. Caller MUST escalate to
     * terminal-overflow {@code BridgeStatus{fatal:true,reason:"outbound-overflow"}} and close the
     * channel.
     */
    TERMINAL
  }

  private final BrowserEvent[] ring;
  private final int capacity;

  private int head;
  private int tail;
  private int size;

  /**
   * Count of {@code RawFix}/{@code RawFixSlice} entries currently held in the ring. Maintained
   * incrementally on every {@link #offer}, {@link #poll}, and drop so {@link #dropOldestRawFix} can
   * short-circuit the linear scan when {@code rawFixCount == 0} (the common case under sustained
   * backpressure of mostly-critical events). When non-zero, the scan is still bounded by the
   * position of the oldest RawFix from {@link #head}, but the early-exit alone removes the
   * worst-case all-critical scan that prompted the Gemini O(N) finding on PR #70.
   */
  private int rawFixCount;

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
    this.rawFixCount = 0;
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
      if (isRawFix(e)) {
        rawFixCount++;
      }
      return OfferResult.ACCEPTED;
    }
    // Full. Try to drop the oldest RawFix entry. The rawFixCount short-circuit removes the
    // worst-case all-critical scan flagged by Gemini on PR #70 — when the ring contains only
    // critical events the linear scan is skipped entirely.
    if (rawFixCount > 0 && dropOldestRawFix()) {
      ring[tail] = e;
      tail = (tail + 1) % capacity;
      size++;
      if (isRawFix(e)) {
        rawFixCount++;
      }
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
    final var e = ring[head];
    ring[head] = null;
    head = (head + 1) % capacity;
    size--;
    if (isRawFix(e)) {
      rawFixCount--;
    }
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
   * Drop the oldest {@code RawFix}/{@code RawFixSlice} entry if any exists. Compacts the ring
   * around the dropped slot. The linear scan is bounded by the position of the oldest RawFix from
   * {@link #head}; the {@link #rawFixCount} early-exit at the {@link #offer} call site skips this
   * method entirely when no RawFix entries are present.
   *
   * @return {@code true} iff a RawFix entry was found and removed
   */
  private boolean dropOldestRawFix() {
    if (size == 0 || rawFixCount == 0) {
      return false;
    }
    // Walk from head forward and find the first RawFix.
    // Loop scan pointer mutated across the walk per CLAUDE.md carve-out.
    int idx = head;
    int searched = 0;
    while (searched < size) {
      if (isRawFix(ring[idx])) {
        // Found one. Compact: shift every later element left by one slot.
        compactOut(idx);
        rawFixCount--;
        return true;
      }
      idx = (idx + 1) % capacity;
      searched++;
    }
    return false;
  }

  /**
   * Type-discrimination helper for the RawFix/RawFixSlice branch — extracted to one site so a
   * future addition of another droppable variant only changes one location (and so {@link
   * #rawFixCount} bookkeeping stays consistent across {@link #offer}, {@link #poll}, and {@link
   * #dropOldestRawFix}).
   *
   * @param e candidate event
   * @return {@code true} iff {@code e} is a {@code RawFix} or {@code RawFixSlice}
   */
  private static boolean isRawFix(final BrowserEvent e) {
    return e instanceof BrowserEvent.RawFix || e instanceof BrowserEvent.RawFixSlice;
  }

  /**
   * Remove the entry at {@code idx} and shift later entries one slot toward the head so the ring
   * stays contiguous. After removal, {@link #tail} moves back by one.
   *
   * <p>Uses {@link System#arraycopy} for the shift instead of a manual loop — JVM intrinsifies
   * arraycopy and it's typically 3-10× faster than a Java {@code for} loop on bulk copies. Two
   * arraycopy calls cover the wrap-around case (one for the contiguous range up to the array end,
   * one for the wrapped tail). For non-wrapping ranges only the first arraycopy fires.
   */
  private void compactOut(final int idx) {
    final int target = size - (countDistance(head, idx) + 1);
    if (target > 0) {
      // Fast path — neither source nor destination range wraps the end of the ring. This is
      // the common case (only the rare drop-at-wrap-boundary scenario hits the slow path) and
      // the JVM intrinsifies arraycopy to a memmove (3-10x faster than a Java loop on bulk
      // copies). Per the Gemini review on PR #70.
      // Source range:      [idx+1, idx+1+target) — contiguous iff idx+1+target <= capacity
      // Destination range: [idx, idx+target)     — contiguous iff idx+target   <= capacity
      // Source contiguous implies destination contiguous (idx < idx+1), so the single check on
      // src+target suffices.
      final int srcStart = idx + 1;
      if (srcStart + target <= capacity) {
        System.arraycopy(ring, srcStart, ring, idx, target);
      } else {
        // Slow path — source range wraps. Fall back to the original element-shift loop. This
        // path fires only when a RawFix at the wrap boundary is dropped (very rare under
        // normal operation; the worst case is one full ring traversal pre-wrap).
        // Loop scan pointer mutated across the shift per CLAUDE.md carve-out.
        int cur = idx;
        int next;
        int shifted = 0;
        while (shifted < target) {
          next = (cur + 1) % capacity;
          ring[cur] = ring[next];
          cur = next;
          shifted++;
        }
      }
    }
    // Null out the old tail slot (now duplicated by the shift) so GC can collect the displaced
    // reference. tail moves back by one.
    final int newTail = (tail - 1 + capacity) % capacity;
    ring[newTail] = null;
    tail = newTail;
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
