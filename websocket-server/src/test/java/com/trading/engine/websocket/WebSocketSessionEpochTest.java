package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ResourceLeakDetector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Direct unit tests for {@link WebSocketSession#resume()} and {@link
 * WebSocketSession#currentEpoch()} — the session epoch increment / VarHandle fence mechanism
 * introduced in Phase 3 Commit A.
 *
 * <p><b>Surfaces covered.</b>
 *
 * <ul>
 *   <li>{@code currentEpoch()} starts at zero on construction.
 *   <li>{@code resume()} increments the epoch atomically via {@code VarHandle.getAndAdd} and
 *       returns the post-increment value.
 *   <li>{@code resume()} clears the {@link SubscriptionFilter} as a side effect (stale
 *       prior-session subscriptions must not survive into the resumed epoch).
 *   <li>Sequential calls accumulate monotonically.
 *   <li>A concurrent reader thread eventually observes the new epoch via the acquire/release fence
 *       pair — soft-correctness probe for the VarHandle memory ordering (the JMM equivalent of a
 *       JCStress test without the jcstress source set).
 * </ul>
 *
 * <p><b>Threading model.</b> Most tests are single-threaded. {@link
 * #resume_concurrentReadFromDifferentThread_observesNewEpoch} spawns exactly one reader thread; the
 * assertion is bounded by a 1-second timeout to prevent CI hangs.
 *
 * <p><b>Allocation.</b> {@link EmbeddedChannel} is Netty's in-process test fixture. Every test that
 * opens a channel calls {@link EmbeddedChannel#finishAndReleaseAll()} in {@link #tearDown()}.
 */
final class WebSocketSessionEpochTest {

  private EmbeddedChannel channel;
  private WebSocketSession session;

  @BeforeAll
  static void enableLeakDetection() {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
  }

  @BeforeEach
  void setUp() {
    channel = new EmbeddedChannel();
    session = new WebSocketSession(channel, 0L, "127.0.0.1");
  }

  @AfterEach
  void tearDown() {
    channel.finishAndReleaseAll();
  }

  /** A freshly constructed session must report epoch 0 — no resume has occurred yet. */
  @Test
  void currentEpoch_afterConstruction_returnsZero() {
    assertEquals(0L, session.currentEpoch(), "Epoch must be 0 immediately after construction");
  }

  /**
   * A single {@code resume()} must return 1 (post-increment), and any subscriptions added to the
   * filter before the resume must be cleared (the resumed epoch starts with an empty filter).
   */
  @Test
  void resume_firstCall_returnsOne_andClearsSubscriptionFilter() {
    session.initSubscriptionFilter(100);
    // Add 3 distinct subscriptions before the resume
    session.subscriptionFilter().addSubscription(0x4555525553440000L, 0x01); // EURUSD  , orders
    session.subscriptionFilter().addSubscription(0x4742505553440000L, 0x02); // GBPUSD  , positions
    session.subscriptionFilter().addSubscription(0x5553444A50590000L, 0x04); // USDJPY  , prices
    assertEquals(
        3, session.subscriptionFilter().subscriptionCount(), "Three subscriptions before resume");

    final long newEpoch = session.resume();

    assertEquals(1L, newEpoch, "resume() must return 1 on first call");
    assertEquals(1L, session.currentEpoch(), "currentEpoch() must read 1 after first resume");
    assertTrue(
        session.subscriptionFilter().isEmpty(),
        "resume() must clear the subscription filter so stale subscriptions are not inherited");
    assertEquals(
        0, session.subscriptionFilter().subscriptionCount(), "Filter must have 0 subscriptions");
  }

  /**
   * Five sequential {@code resume()} calls must produce a monotonically increasing epoch of 5, and
   * {@code currentEpoch()} must observe 5 after the last call.
   */
  @Test
  void resume_calledFiveTimes_currentEpochReturnsFive() {
    for (int i = 1; i <= 5; i++) {
      final long returned = session.resume();
      assertEquals((long) i, returned, "resume() must return " + i + " on call " + i);
    }
    assertEquals(5L, session.currentEpoch(), "currentEpoch() must be 5 after 5 resume() calls");
  }

  /**
   * A reader thread that spin-reads {@code currentEpoch()} via the {@link
   * java.lang.invoke.VarHandle#acquireFence()} path must observe the post-resume value (≥ 1) within
   * 1 second of the main thread calling {@code resume()}.
   *
   * <p>This is a soft-correctness probe: on any correct JVM with a memory model that honours
   * acquire/release semantics (which includes all production JVMs), the fence pair guarantees the
   * reader eventually sees the writer's update. The 1-second bound is a safety guard for CI; on
   * compliant JVMs the spin exits within microseconds.
   */
  @Test
  void resume_concurrentReadFromDifferentThread_observesNewEpoch() throws InterruptedException {
    // Latch ensures the reader thread is spinning before we call resume().
    final var readerReady = new CountDownLatch(1);
    final var observedEpoch = new AtomicInteger(-1);

    final var reader =
        new Thread(
            () -> {
              // Signal ready, then spin until a non-zero epoch is observed.
              readerReady.countDown();
              long observed;
              do {
                observed = session.currentEpoch(); // acquireFence + volatile read
              } while (observed < 1L);
              observedEpoch.set((int) observed);
            },
            "epoch-reader");
    reader.setDaemon(true);
    reader.start();

    // Block until the reader has started its spin loop.
    final boolean started = readerReady.await(1L, TimeUnit.SECONDS);
    assertTrue(started, "Reader thread must signal ready within 1 second");

    // Resume on the main thread; the reader must observe epoch ≥ 1 within 1 s.
    session.resume();

    reader.join(1_000L /* ms */);
    assertFalse(reader.isAlive(), "Reader thread must exit within 1 second of resume()");
    assertEquals(1, observedEpoch.get(), "Reader must observe epoch 1 after resume()");
  }

  /**
   * {@code resume()} without a subscription filter (null) must not throw — the null guard in
   * production code silently skips the clear step.
   */
  @Test
  void resume_withNullSubscriptionFilter_doesNotThrow() {
    // Filter was never initialised — subscriptionFilter() returns null.
    final long epoch = session.resume();
    assertEquals(1L, epoch, "resume() must return 1 even when subscriptionFilter is null");
    assertEquals(1L, session.currentEpoch(), "currentEpoch() must be 1 after resume()");
  }
}
