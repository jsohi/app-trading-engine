package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.trading.engine.messages.sbe.FeedStateEnum;
import com.trading.engine.messages.sbe.MarketDataFeedStateChangeDecoder;
import com.trading.engine.messages.sbe.MarketDataFeedStateChangeEncoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.testsupport.clock.ControllableNanoClock;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link AeronEgressThread.FeedStateChangeEmitter} — the inner SAM that encodes
 * {@code MarketDataFeedStateChange} (template 57) and enqueues it onto the reliable egress queue
 * when the {@link MarketDataSubscriptionLivenessTracker} fires a state transition.
 *
 * <p><b>Surfaces covered.</b>
 *
 * <ul>
 *   <li>{@code accept(long)} with a valid state — asserts the entry is enqueued, has the correct
 *       templateId (57) and total encoded length (8-byte SBE header + 9-byte block = 17 bytes), and
 *       the decoded {@link FeedStateEnum} + {@code serverNanos} match expectations.
 *   <li>{@code accept(long)} when the egress pool is exhausted — asserts the drop counter ({@code
 *       websocket.marketdata.dropped}) increments and the queue stays empty.
 *   <li>{@code accept(long)} on a successful transition — asserts the {@code
 *       websocket.marketdata.feed.state.transitions} counter increments.
 * </ul>
 *
 * <p><b>Wiring strategy.</b> Tests construct a real {@link WebSocketEgressListener} with a
 * controlled pool capacity and queue. For the pool-exhaustion test the single entry is borrowed
 * before {@code accept()} is called, leaving the pool empty. The queue capacity matches the pool
 * capacity for simplicity; both are sized to 4 entries (power-of-2 requirement).
 *
 * <p><b>Threading model.</b> All calls are single-threaded on the JUnit runner thread, matching the
 * single-writer contract of the production egress thread.
 *
 * <p><b>Allocation.</b> Test infrastructure (SimpleMeterRegistry, ArrayList, etc.) allocates on
 * heap; no allocation constraints apply to test code.
 */
final class AeronEgressThreadFeedStateChangeEmitterTest {

  /** Expected total encoded length: 8-byte SBE header + 9-byte block. */
  private static final int EXPECTED_ENCODED_LENGTH =
      MessageHeaderEncoder.ENCODED_LENGTH + MarketDataFeedStateChangeDecoder.BLOCK_LENGTH;

  /** Queue and pool capacity for normal tests. */
  private static final int CAPACITY = 4;

  /** Max message size per entry — 64 bytes is more than enough for the 17-byte feed-state frame. */
  private static final int MAX_MESSAGE_SIZE = 64;

  private SimpleMeterRegistry registry;
  private WebSocketMetrics metrics;
  private ManyToOneConcurrentArrayQueue<EgressEntry> egressQueue;
  private ManyToOneConcurrentArrayQueue<EgressEntry> returnQueue;
  private WebSocketEgressListener egressListener;
  private ControllableNanoClock clock;
  private AeronEgressThread.FeedStateChangeEmitter emitter;

  @BeforeEach
  void setUp() {
    registry = new SimpleMeterRegistry();
    metrics = new WebSocketMetrics(registry);
    clock = new ControllableNanoClock(1_000_000_000L);

    egressQueue = new ManyToOneConcurrentArrayQueue<>(CAPACITY);
    returnQueue = new ManyToOneConcurrentArrayQueue<>(CAPACITY);

    egressListener =
        new WebSocketEgressListener(egressQueue, returnQueue, metrics, CAPACITY, MAX_MESSAGE_SIZE);

    emitter =
        new AeronEgressThread.FeedStateChangeEmitter(egressListener, egressQueue, metrics, clock);
  }

  // ── Test 1: valid state enqueues template-57 entry ───────────────────────

  /**
   * A call to {@code accept(FeedStateEnum.Quiet.value())} must enqueue exactly one entry, set its
   * {@code templateId} to {@link MarketDataFeedStateChangeDecoder#TEMPLATE_ID} (57), encode total
   * length {@link #EXPECTED_ENCODED_LENGTH} (17), and the decoded state must equal {@link
   * FeedStateEnum#Quiet} with {@code serverNanos} equal to the clock's current value.
   */
  @Test
  void accept_validState_enqueuesTemplate57Entry() {
    final long clockNanos = 5_000_000_000L;
    clock.setNanos(clockNanos);

    emitter.accept((long) FeedStateEnum.Quiet.value());

    final var entry = egressQueue.poll();
    assertNotNull(entry, "Queue must contain one entry after accept()");
    assertEquals(
        MarketDataFeedStateChangeEncoder.TEMPLATE_ID,
        entry.templateId(),
        "templateId must be 57 (MarketDataFeedStateChange)");
    assertEquals(
        EXPECTED_ENCODED_LENGTH,
        entry.length(),
        "encoded length must be header(8) + block(9) = 17 bytes");

    // Decode the SBE bytes and verify field values.
    final var headerDecoder = new MessageHeaderDecoder();
    final var decoder = new MarketDataFeedStateChangeDecoder();
    final var buf = new UnsafeBuffer(entry.bytes(), 0, entry.length());
    decoder.wrapAndApplyHeader(buf, 0, headerDecoder);

    assertEquals(FeedStateEnum.Quiet, decoder.state(), "decoded state must be Quiet");
    assertEquals(clockNanos, decoder.serverNanos(), "decoded serverNanos must match clock value");

    // Verify no stale-epoch or drop counters were touched.
    assertEquals(
        0.0,
        registry.get("websocket.marketdata.dropped").counter().count(),
        "No drop counter must be incremented on success");
  }

  // ── Test 2: pool exhausted → drop counter increments, queue stays empty ──

  /**
   * When the egress pool is exhausted (all entries borrowed before {@code accept()} is called), the
   * emitter must drop the frame and increment {@code websocket.marketdata.dropped} by 1. The queue
   * must remain empty.
   */
  @Test
  void accept_poolExhausted_dropsAndIncrementsDropCounter() {
    // Borrow all CAPACITY entries so borrowForMarketData() returns null.
    for (int i = 0; i < CAPACITY; i++) {
      final var borrowed = egressListener.borrowForMarketData();
      assertNotNull(borrowed, "Pool must have entry " + i + " available");
    }

    // Pool is now empty — accept() must drop silently.
    emitter.accept((long) FeedStateEnum.Live.value());

    assertNull(egressQueue.poll(), "Queue must be empty when pool is exhausted");
    assertEquals(
        1.0,
        registry.get("websocket.marketdata.dropped").counter().count(),
        "marketdata.dropped counter must be 1 on pool exhaustion");
    assertEquals(
        0.0,
        registry.get("websocket.marketdata.feed.state.transitions").counter().count(),
        "transition counter must NOT increment on pool exhaustion (frame was dropped)");
  }

  // ── Test 3: successful transition increments the transition metric ────────

  /**
   * A successful {@code accept()} — one that actually enqueues an entry — must increment the {@code
   * websocket.marketdata.feed.state.transitions} counter by exactly 1.
   */
  @Test
  void accept_transitionToLive_recordsTransitionMetric() {
    clock.setNanos(2_000_000_000L);

    emitter.accept((long) FeedStateEnum.Live.value());

    assertEquals(
        1.0,
        registry.get("websocket.marketdata.feed.state.transitions").counter().count(),
        "marketdata.feed.state.transitions counter must be 1 after a successful accept()");
    assertEquals(
        0.0,
        registry.get("websocket.marketdata.dropped").counter().count(),
        "marketdata.dropped counter must remain 0 on success");

    // Drain the queue entry to avoid leaving an entry unreturned.
    final var entry = egressQueue.poll();
    assertNotNull(entry, "Entry must be present in queue");
  }

  // ── Test 4: queue full → drop counter increments ─────────────────────────

  /**
   * When the egress queue is already full (CAPACITY entries offered before calling {@code
   * accept()}), the emitter must release the borrowed entry back to the pool via {@link
   * WebSocketEgressListener#releaseDirectly(EgressEntry)} and increment the drop counter.
   */
  @Test
  void accept_queueFull_dropsAndIncrementsDropCounter() {
    // Fill the queue by borrowing + manually offering CAPACITY entries (bypassing the pool so we
    // keep pool entries available for the emitter to borrow).
    for (int i = 0; i < CAPACITY; i++) {
      final var dummy = new EgressEntry(MAX_MESSAGE_SIZE);
      final boolean offered = egressQueue.offer(dummy);
      if (!offered) {
        break; // queue already full enough
      }
    }

    // Queue is full — accept() must fail the queue.offer and drop.
    emitter.accept((long) FeedStateEnum.Stale.value());

    assertEquals(
        1.0,
        registry.get("websocket.marketdata.dropped").counter().count(),
        "marketdata.dropped counter must be 1 when queue is full");
    assertEquals(
        0.0,
        registry.get("websocket.marketdata.feed.state.transitions").counter().count(),
        "transition counter must NOT increment when the queue offer fails");

    // Drain the queue to release resources.
    EgressEntry e;
    while ((e = egressQueue.poll()) != null) {
      // entries were manually created, no pool to return them to
    }
  }
}
