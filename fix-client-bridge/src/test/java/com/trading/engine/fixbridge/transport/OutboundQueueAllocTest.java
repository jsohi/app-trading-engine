package com.trading.engine.fixbridge.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.trading.engine.fixbridge.json.BrowserEvent;
import com.trading.engine.fixbridge.transport.OutboundQueue.OfferResult;
import java.lang.management.ManagementFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Allocation regression tripwire for {@link OutboundQueue#offer(BrowserEvent)} and {@link
 * OutboundQueue#poll()}.
 *
 * <p>Asserts {@code GarbageCollectorMXBean.getCollectionCount()} does not advance during {@code
 * 100_000} steady-state iterations of offer+poll on a small ring (capacity=8) with stable,
 * pre-constructed {@link BrowserEvent} references. The production hot path is documented as
 * zero-alloc; this test enforces that invariant after JIT warm-up.
 *
 * <p>The queue is sized at 8 and events are alternately offered and polled so the queue never
 * fills. Stable references are used to avoid allocation in the loop body itself.
 *
 * <p>Gated by {@code -DrunAllocTests=true} so the regular {@code test} task skips it (locked §21,
 * §23).
 *
 * <p><b>Threading.</b> Single-threaded. {@link OutboundQueue} is not thread-safe per its contract;
 * the test owns it exclusively.
 */
@EnabledIfSystemProperty(named = "runAllocTests", matches = "true")
final class OutboundQueueAllocTest {

  private static final int WARMUP_ITERATIONS = 5_000;
  private static final int MEASURED_ITERATIONS = 100_000;

  /** Stable execution-report used for offer+poll without allocating inside the loop. */
  private static final BrowserEvent.ExecutionReport EXEC =
      new BrowserEvent.ExecutionReport("C1", "E1", '0', '0', "AAPL", "Buy", 0L, 100L, 0L);

  @Test
  void offerPoll_repeated_zeroAlloc() {
    final var queue = new OutboundQueue(8);

    // Warmup — let JIT compile the offer+poll hot path before measuring.
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      final OfferResult r = queue.offer(EXEC);
      if (r != OfferResult.ACCEPTED) {
        throw new AssertionError("offer failed during warmup: " + r);
      }
      final BrowserEvent polled = queue.poll();
      if (polled == null) {
        throw new AssertionError("poll returned null during warmup");
      }
    }

    // Measured phase — queue is empty at this point (every offer was immediately polled).
    final long beforeGc = totalGcCount();
    for (int i = 0; i < MEASURED_ITERATIONS; i++) {
      final OfferResult r = queue.offer(EXEC);
      // Prevent dead-code elimination.
      if (r != OfferResult.ACCEPTED) {
        throw new AssertionError("offer failed at iteration " + i + ": " + r);
      }
      final BrowserEvent polled = queue.poll();
      // Prevent dead-code elimination.
      assertNotNull(polled, "poll must return non-null after offer");
    }
    final long afterGc = totalGcCount();

    assertEquals(
        beforeGc,
        afterGc,
        "OutboundQueue offer+poll advanced GC count from " + beforeGc + " to " + afterGc);
  }

  // ---------------------------------------------------------------------------
  // GC count helper — mirrors pattern from other *AllocTest classes.
  // ---------------------------------------------------------------------------

  private static long totalGcCount() {
    long total = 0L;
    final var beans = ManagementFactory.getGarbageCollectorMXBeans();
    for (final var bean : beans) {
      final long c = bean.getCollectionCount();
      if (c >= 0L) {
        total += c;
      }
    }
    return total;
  }
}
