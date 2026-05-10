package com.trading.engine.fixbridge.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.trading.engine.fixbridge.json.BrowserEvent;
import com.trading.engine.fixbridge.transport.OutboundQueue.OfferResult;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link OutboundQueue}.
 *
 * <p><b>Coverage.</b> Construction validation, basic FIFO ordering, null guard, drop policy
 * (ACCEPTED_DROPPED_RAWFIX vs. TERMINAL), wrap-around correctness, and every "never-drop" event
 * type exercised at capacity to confirm TERMINAL is returned when no {@code RawFix} is present.
 *
 * <p><b>Threading.</b> Single-threaded; {@link OutboundQueue} is not thread-safe per its contract.
 *
 * <p><b>Drop policy summary.</b> When full, {@link OutboundQueue#offer(BrowserEvent)} scans from
 * the head and removes the first {@link BrowserEvent.RawFix} found (ACCEPTED_DROPPED_RAWFIX). If no
 * {@code RawFix} is present, the event is rejected (TERMINAL). All other event subtypes ({@code
 * ExecutionReport}, {@code OrderReject}, {@code BridgeStatus}, {@code AuthExpired}, {@code Quote},
 * etc.) are never dropped.
 */
final class OutboundQueueTest {

  // ---------------------------------------------------------------------------
  // Shared event fixtures — stable references used in alloc-sensitive paths.
  // ---------------------------------------------------------------------------

  private static final BrowserEvent.ExecutionReport EXEC1 =
      new BrowserEvent.ExecutionReport("C1", "E1", '0', '0', "AAPL", "Buy", 0L, 100L, 0L);
  private static final BrowserEvent.ExecutionReport EXEC2 =
      new BrowserEvent.ExecutionReport("C2", "E2", '0', '0', "AAPL", "Buy", 0L, 100L, 0L);
  private static final BrowserEvent.ExecutionReport EXEC3 =
      new BrowserEvent.ExecutionReport("C3", "E3", '0', '0', "AAPL", "Buy", 0L, 100L, 0L);
  private static final BrowserEvent.ExecutionReport EXEC4 =
      new BrowserEvent.ExecutionReport("C4", "E4", '0', '0', "AAPL", "Buy", 0L, 100L, 0L);
  private static final BrowserEvent.RawFix RAW1 = new BrowserEvent.RawFix("in", "8=FIX.4.4|1=A");
  private static final BrowserEvent.RawFix RAW2 = new BrowserEvent.RawFix("in", "8=FIX.4.4|1=B");
  private static final BrowserEvent.RawFix RAW3 = new BrowserEvent.RawFix("in", "8=FIX.4.4|1=C");
  private static final BrowserEvent.RawFix RAW4 = new BrowserEvent.RawFix("in", "8=FIX.4.4|1=D");
  private static final BrowserEvent.Quote QUOTE1 =
      new BrowserEvent.Quote("R1", "Q1", "AAPL", "Buy", 100L, 15000_00000000L, Long.MAX_VALUE);
  private static final BrowserEvent.OrderReject REJECT1 =
      new BrowserEvent.OrderReject("C1", "bridge-down");
  private static final BrowserEvent.BridgeStatus STATUS1 =
      new BrowserEvent.BridgeStatus(false, false, "ok");

  // ---------------------------------------------------------------------------
  // Construction
  // ---------------------------------------------------------------------------

  @Test
  void ctor_default_capacity4096() {
    final var queue = new OutboundQueue();
    assertEquals(4096, queue.capacity(), "default capacity must be 4096");
    assertEquals(0, queue.size(), "fresh queue must be empty");
  }

  @Test
  void ctor_zeroCapacity_throwsIAE() {
    assertThrows(IllegalArgumentException.class, () -> new OutboundQueue(0));
  }

  @Test
  void ctor_negativeCapacity_throwsIAE() {
    assertThrows(IllegalArgumentException.class, () -> new OutboundQueue(-1));
  }

  // ---------------------------------------------------------------------------
  // Basic FIFO
  // ---------------------------------------------------------------------------

  @Test
  void offer_thenPoll_returnsInOrder() {
    final var queue = new OutboundQueue(8);
    final BrowserEvent[] events = {EXEC1, EXEC2, RAW1, QUOTE1, REJECT1};
    for (final var e : events) {
      assertEquals(OfferResult.ACCEPTED, queue.offer(e));
    }
    assertEquals(events.length, queue.size());
    for (final var expected : events) {
      assertSame(expected, queue.poll(), "poll must return events in FIFO order");
    }
    assertEquals(0, queue.size());
  }

  @Test
  void offer_null_throwsNPE() {
    final var queue = new OutboundQueue(4);
    assertThrows(NullPointerException.class, () -> queue.offer(null));
  }

  @Test
  void poll_empty_returnsNull() {
    final var queue = new OutboundQueue(4);
    assertNull(queue.poll(), "poll on empty queue must return null");
  }

  @Test
  void size_tracksOfferAndPoll() {
    final var queue = new OutboundQueue(4);
    assertEquals(0, queue.size());
    queue.offer(EXEC1);
    assertEquals(1, queue.size());
    queue.offer(EXEC2);
    assertEquals(2, queue.size());
    queue.poll();
    assertEquals(1, queue.size());
    queue.poll();
    assertEquals(0, queue.size());
  }

  // ---------------------------------------------------------------------------
  // Capacity full path — RawFix drop
  // ---------------------------------------------------------------------------

  @Test
  void offer_fullWithRawFixesAvailable_dropsOldestRawFixAndAccepts() {
    // capacity=4; fill with 4 RawFix events.
    // Then offer 1 ExecutionReport → ACCEPTED_DROPPED_RAWFIX.
    // The OLDEST RawFix (RAW1) is dropped; RAW2, RAW3, RAW4, EXEC1 remain in that order.
    final var queue = new OutboundQueue(4);
    queue.offer(RAW1);
    queue.offer(RAW2);
    queue.offer(RAW3);
    queue.offer(RAW4);

    final OfferResult result = queue.offer(EXEC1);

    assertEquals(
        OfferResult.ACCEPTED_DROPPED_RAWFIX, result, "offer must return ACCEPTED_DROPPED_RAWFIX");
    assertEquals(4, queue.size(), "size must stay at capacity after drop+insert");

    assertSame(RAW2, queue.poll(), "RAW1 dropped; RAW2 should be first");
    assertSame(RAW3, queue.poll());
    assertSame(RAW4, queue.poll());
    assertSame(EXEC1, queue.poll(), "newly offered ExecutionReport must be last");
  }

  @Test
  void offer_dropsOldestRawFixOnly_notNewest() {
    // capacity=4: offer RAW1, EXEC1, RAW2, EXEC2 (full).
    // Offer EXEC3 as 5th event → drops RAW1 (oldest RawFix), RAW2 survives.
    final var queue = new OutboundQueue(4);
    queue.offer(RAW1);
    queue.offer(EXEC1);
    queue.offer(RAW2);
    queue.offer(EXEC2);

    final OfferResult result = queue.offer(EXEC3);

    assertEquals(OfferResult.ACCEPTED_DROPPED_RAWFIX, result);
    assertEquals(4, queue.size());

    // Expected order after RAW1 dropped: EXEC1, RAW2, EXEC2, EXEC3.
    assertSame(EXEC1, queue.poll(), "EXEC1 at head after RAW1 dropped");
    assertSame(RAW2, queue.poll(), "RAW2 must survive (newer RawFix)");
    assertSame(EXEC2, queue.poll());
    assertSame(EXEC3, queue.poll());
  }

  @Test
  void offer_fullWithMixedTypes_dropsOnlyRawFix() {
    // capacity=4: offer Exec1, RawFix, Exec2, RawFix2 (full).
    // Offer Quote → drops first RawFix (position 1); resulting order: Exec1, Exec2, RawFix2, Quote.
    final var queue = new OutboundQueue(4);
    queue.offer(EXEC1);
    queue.offer(RAW1);
    queue.offer(EXEC2);
    queue.offer(RAW2);

    final OfferResult result = queue.offer(QUOTE1);

    assertEquals(OfferResult.ACCEPTED_DROPPED_RAWFIX, result);
    assertEquals(4, queue.size());

    assertSame(EXEC1, queue.poll());
    assertSame(EXEC2, queue.poll(), "EXEC2 shifts left after RAW1 dropped");
    assertSame(RAW2, queue.poll(), "RAW2 survives (newer)");
    assertSame(QUOTE1, queue.poll());
  }

  // ---------------------------------------------------------------------------
  // Capacity full path — TERMINAL
  // ---------------------------------------------------------------------------

  @Test
  void offer_fullNoRawFix_returnsTerminal() {
    // capacity=3; fill with 3 Quote events (never dropped), offer 4th → TERMINAL.
    final var queue = new OutboundQueue(3);
    queue.offer(QUOTE1);
    queue.offer(QUOTE1);
    queue.offer(QUOTE1);

    final OfferResult result = queue.offer(QUOTE1);

    assertEquals(OfferResult.TERMINAL, result, "must return TERMINAL when no RawFix to drop");
    assertEquals(3, queue.size(), "size must be unchanged after TERMINAL");
  }

  @Test
  void offer_fullAllExecutionReports_terminal() {
    final var queue = new OutboundQueue(3);
    queue.offer(EXEC1);
    queue.offer(EXEC2);
    queue.offer(EXEC3);

    assertEquals(OfferResult.TERMINAL, queue.offer(EXEC4));
    assertEquals(3, queue.size());
  }

  @Test
  void offer_fullAllOrderRejects_terminal() {
    final var queue = new OutboundQueue(2);
    queue.offer(REJECT1);
    queue.offer(REJECT1);

    assertEquals(OfferResult.TERMINAL, queue.offer(REJECT1));
    assertEquals(2, queue.size());
  }

  // ---------------------------------------------------------------------------
  // Wrap-around correctness
  // ---------------------------------------------------------------------------

  @Test
  void offerPollWrapAround_size2() {
    // capacity=2: offer Exec1, Exec2, poll(Exec1), offer Exec3 (wraps), offer Exec4 (full).
    // Offer 5th event → TERMINAL (no RawFix available).
    final var queue = new OutboundQueue(2);
    queue.offer(EXEC1);
    queue.offer(EXEC2);
    assertSame(EXEC1, queue.poll()); // head advances, creating wrap-around opportunity
    queue.offer(EXEC3); // tail wraps to slot 0
    queue.offer(EXEC4); // tail at slot 1 — full again (head=1, tail=1, size=2)

    assertEquals(2, queue.size());
    assertEquals(OfferResult.TERMINAL, queue.offer(EXEC1), "no RawFix, must be TERMINAL");
    assertEquals(2, queue.size());

    // Drain in FIFO order: Exec2, Exec3, Exec4 — only 2 entries (Exec2, Exec3 at slots 1,0).
    // After wrap: head=1 (Exec2), slot 0=Exec3, then Exec4 was at slot 1 but actually:
    // head=1 after poll(Exec1), offer Exec3 → tail=(1+1)%2=0, ring[0]=Exec3,
    // offer Exec4 → tail=(0+1)%2=1, ring[1]=Exec4 (overwrote Exec2's slot? No — Exec2 is at
    // ring[1], head=1. tail=0 after first wrap, then offer Exec3 → ring[0]=Exec3, tail=1.
    // offer Exec4 → ring[1]=Exec4, tail=0. That's wrong — Exec2 is at ring[1] (head=1) but
    // we'd overwrite it. Let's re-trace: initial state after ctor: head=0,tail=0,size=0.
    // offer(Exec1): ring[0]=Exec1, tail=1, size=1.
    // offer(Exec2): ring[1]=Exec2, tail=0, size=2.   ← tail wraps here (cap=2)
    // poll(): ring[0]=null, head=1, size=1. returns Exec1.
    // offer(Exec3): ring[0]=Exec3, tail=1, size=2.
    // offer(Exec4): size==capacity → ACCEPTED? No — size=2 == capacity=2 → goes to full path.
    // Wait: Exec4 offer when size==capacity → we check dropOldestRawFix — none, return TERMINAL.
    // Actually: I computed offer(Exec4) should check size < capacity first. size=2, capacity=2:
    // NOT less than → full path → no RawFix → TERMINAL.
    // So after the 3 offers and 1 poll we have Exec2 (ring[1], head=1) and Exec3 (ring[0]).
    // capacity=2, size=2.
    assertSame(EXEC2, queue.poll());
    assertSame(EXEC3, queue.poll());
    assertEquals(0, queue.size());
  }

  @Test
  void offer_dropRawFixAtWrapBoundary() {
    // capacity=4.
    // Build state: poll 2, offer 2 so head>0, then fill completely with a mix
    // that puts the oldest RawFix at a wrap-around position.
    //
    // Step 1: offer Exec1, Exec2 → ring[0..1], head=0, tail=2, size=2.
    // Step 2: poll Exec1, poll Exec2 → head=2, size=0.
    // Step 3: offer Raw1, Exec3, Exec4, Raw2 → ring[2]=Raw1, ring[3]=Exec3,
    //         ring[0]=Exec4 (wrap), ring[1]=Raw2. head=2, tail=2, size=4 (full).
    // Step 4: offer Quote1 → drops oldest RawFix (Raw1 at ring[2]).
    //         compactOut(2): shifts ring[3]→ring[2], ring[0]→ring[3], ring[1]→ring[0].
    //         tail moves back by 1: tail=(2-1+4)%4=1. size=3.
    //         Then ring[tail]=Quote1 → ring[1]=Quote1? No — after compactOut, tail=1,
    //         then offer sets ring[1]=Quote1, tail=(1+1)%4=2, size=4.
    //
    // Expected poll order after drop: Exec3, Exec4, Raw2, Quote1.
    final var queue = new OutboundQueue(4);
    // Advance head pointer to 2 by offer+poll pairs.
    queue.offer(EXEC1);
    queue.offer(EXEC2);
    queue.poll(); // Exec1
    queue.poll(); // Exec2

    // Fill: head=2, tail=2.
    queue.offer(RAW1); // ring[2], tail=3
    queue.offer(EXEC3); // ring[3], tail=0
    queue.offer(EXEC4); // ring[0], tail=1
    queue.offer(RAW2); // ring[1], tail=2  → full (size=4)

    assertEquals(4, queue.size());

    final OfferResult result = queue.offer(QUOTE1);
    assertEquals(
        OfferResult.ACCEPTED_DROPPED_RAWFIX,
        result,
        "oldest RawFix at wrap boundary must be dropped");
    assertEquals(4, queue.size());

    // RAW1 was oldest and dropped; remaining in order: Exec3, Exec4, Raw2, Quote1.
    assertSame(EXEC3, queue.poll());
    assertSame(EXEC4, queue.poll());
    assertSame(RAW2, queue.poll());
    assertSame(QUOTE1, queue.poll());
    assertEquals(0, queue.size());
  }

  // ---------------------------------------------------------------------------
  // Drop policy completeness — every never-drop type returns TERMINAL when full
  // ---------------------------------------------------------------------------

  @Test
  void offer_fullAllBridgeStatus_terminal() {
    final var queue = new OutboundQueue(2);
    queue.offer(STATUS1);
    queue.offer(STATUS1);

    assertEquals(OfferResult.TERMINAL, queue.offer(STATUS1));
    assertEquals(2, queue.size());
  }

  @Test
  void offer_fullAllAuthExpired_terminal() {
    final var queue = new OutboundQueue(2);
    queue.offer(BrowserEvent.AuthExpired.INSTANCE);
    queue.offer(BrowserEvent.AuthExpired.INSTANCE);

    assertEquals(OfferResult.TERMINAL, queue.offer(BrowserEvent.AuthExpired.INSTANCE));
    assertEquals(2, queue.size());
  }

  @Test
  void offer_fullAllQuote_terminal() {
    final var queue = new OutboundQueue(2);
    queue.offer(QUOTE1);
    queue.offer(QUOTE1);

    assertEquals(OfferResult.TERMINAL, queue.offer(QUOTE1));
    assertEquals(2, queue.size());
  }
}
