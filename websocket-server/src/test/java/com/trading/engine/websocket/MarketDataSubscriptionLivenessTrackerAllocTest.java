package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.sun.management.ThreadMXBean;
import com.trading.engine.messages.MarketDataConstants;
import java.lang.management.ManagementFactory;
import org.agrona.concurrent.NanoClock;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Pre-warm discipline alloc tripwire for {@link MarketDataSubscriptionLivenessTracker}.
 *
 * <p><b>Purpose.</b> Asserts that the hot path — a mixture of {@link
 * MarketDataSubscriptionLivenessTracker#onTickFragment(long)}, {@link
 * MarketDataSubscriptionLivenessTracker#onHeartbeatFragment(long)}, and {@link
 * MarketDataSubscriptionLivenessTracker#tick(long)} calls — allocates ZERO bytes on the Java heap
 * after JIT warm-up. This guards against accidental boxing, lambda capture, or internal collection
 * growth creeping into the hot path.
 *
 * <p><b>Methodology.</b>
 *
 * <ol>
 *   <li>Pre-warm phase: 1 000 cycles of mixed {@code onTickFragment} / {@code onHeartbeatFragment}
 *       / {@code tick} with interleaved state transitions (LIVE→QUIET, QUIET→LIVE, LIVE→STALE,
 *       STALE→LIVE) so all hot paths are JIT-compiled before the measurement window.
 *   <li>Measurement phase: snapshot {@link ThreadMXBean#getThreadAllocatedBytes(long)} before and
 *       after 100 000 cycles; assert delta == 0.
 * </ol>
 *
 * <p><b>JVM guard.</b> {@code com.sun.management.ThreadMXBean} is a Sun/OpenJDK internal API. If
 * the JVM does not provide it (or if the cast fails), the test skips via {@link
 * org.junit.jupiter.api.Assumptions#assumeTrue(boolean)} so CI on non-OpenJDK JVMs does not fail.
 *
 * <p><b>Threading model.</b> Single-threaded — the tracker's single-writer contract requires all
 * method calls to come from the same thread. JUnit's default runner thread satisfies this.
 *
 * <p><b>Allocation intent.</b> Zero allocation after construction per the production Javadoc. The
 * primitive-field state machine has no internal collections. The {@code LongConsumer} callback is a
 * no-op reference bound once at construction (not a per-call lambda).
 */
final class MarketDataSubscriptionLivenessTrackerAllocTest {

  /** Quiet threshold mirrors the production value for realistic transition timing. */
  private static final long QUIET_THRESHOLD_NS =
      (MarketDataConstants.MARKET_DATA_HEARTBEAT_BASE_MS * 1_000_000L * 3L) / 2L;

  /** Stale threshold mirrors the production value. */
  private static final long STALE_THRESHOLD_NS =
      MarketDataConstants.MARKET_DATA_STALE_THRESHOLD_NANOS;

  /** Warm-up cycle count — large enough to trigger JIT tier-2 compilation. */
  private static final int WARMUP_CYCLES = 1_000;

  /** Measurement cycle count. */
  private static final int MEASURE_CYCLES = 100_000;

  private static ThreadMXBean threadMXBean;

  /**
   * Resolve {@link ThreadMXBean} once. If the JVM does not expose {@code
   * com.sun.management.ThreadMXBean}, all measurement tests skip rather than fail.
   */
  @BeforeAll
  static void resolveThreadMXBean() {
    final var base = ManagementFactory.getThreadMXBean();
    if (base instanceof ThreadMXBean cast) {
      threadMXBean = cast;
    }
    // threadMXBean stays null if not available — assumeTrue in each test will skip.
  }

  /** Controllable nanosecond clock for driving time-based transitions. */
  static final class MutableNanoClock implements NanoClock {
    long nanos;

    MutableNanoClock(final long initial) {
      this.nanos = initial;
    }

    @Override
    public long nanoTime() {
      return nanos;
    }
  }

  private MutableNanoClock clock;
  private MarketDataSubscriptionLivenessTracker tracker;

  @BeforeEach
  void setUp() {
    clock = new MutableNanoClock(1_000_000_000L);
    // Bind a no-op LongConsumer — one allocation at construction, not per-call.
    tracker = new MarketDataSubscriptionLivenessTracker(clock, s -> {});
  }

  /**
   * Drives all four state transitions during the warm-up phase so every callback path and every
   * branch inside the tracker is JIT-compiled before the measurement window starts. The measurement
   * phase then drives only steady-state calls (all three entry points) to assert zero heap
   * allocation after warm-up.
   */
  @Test
  void hotPath_afterWarmUp_allocatesZeroBytes() {
    assumeTrue(
        threadMXBean != null,
        "com.sun.management.ThreadMXBean not available on this JVM — skipping alloc test");
    assumeTrue(
        threadMXBean.isThreadAllocatedMemoryEnabled(),
        "Thread allocated-memory tracking is disabled — enable with -XX:+EnableThreadAllocationStats"
            + " or skipping");

    // ── Pre-warm: drive all four transitions ────────────────────────────────
    preWarm();

    // ── Measurement: 100 000 mixed cycles, no state changes ─────────────────
    // Pin the clock so tick() sees no threshold breaches — pure LIVE steady state.
    // onTickFragment and onHeartbeatFragment update lastTickNs/lastFragmentNs but never call
    // transitionTo() when state is already LIVE, so the LongConsumer is never invoked.
    clock.nanos = 1_000_000_000L; // reset to within all thresholds

    final long threadId = Thread.currentThread().threadId();
    final long bytesBefore = threadMXBean.getThreadAllocatedBytes(threadId);

    for (int i = 0; i < MEASURE_CYCLES; i++) {
      final long t = clock.nanos + (long) i;
      tracker.onTickFragment(t);
      tracker.onHeartbeatFragment(t + 1L);
      tracker.tick(t + 2L);
    }

    final long bytesAfter = threadMXBean.getThreadAllocatedBytes(threadId);
    final long delta = bytesAfter - bytesBefore;

    assertEquals(
        0L,
        delta,
        "MarketDataSubscriptionLivenessTracker hot path must allocate zero bytes after JIT warmup; "
            + "allocated "
            + delta
            + " bytes");
  }

  // ── Pre-warm helper ───────────────────────────────────────────────────────

  /**
   * Drives all four transitions (LIVE→QUIET, QUIET→LIVE, LIVE→STALE, STALE→LIVE) repeatedly over
   * {@link #WARMUP_CYCLES} cycles so every branch and the callback path are JIT-compiled before the
   * measurement window.
   */
  private void preWarm() {
    for (int i = 0; i < WARMUP_CYCLES; i++) {
      // Baseline: LIVE at t=0 (tracker was constructed with lastTickNs=clock.nanos=1s)
      final long base = 1_000_000_000L + (long) i * (STALE_THRESHOLD_NS * 2L);

      // 1. LIVE → QUIET: advance past the quiet threshold without ticks
      final long quietTime = base + QUIET_THRESHOLD_NS + 1L;
      clock.nanos = quietTime;
      tracker.tick(quietTime); // transitions LIVE→QUIET (emits callback)

      // 2. QUIET → LIVE: heartbeat clears QUIET
      final long liveTime = quietTime + 1L;
      clock.nanos = liveTime;
      tracker.onHeartbeatFragment(liveTime); // transitions QUIET→LIVE (emits callback)

      // 3. LIVE → STALE: advance past the stale threshold with no fragments
      final long staleTime = liveTime + STALE_THRESHOLD_NS + 1L;
      clock.nanos = staleTime;
      tracker.tick(staleTime); // transitions LIVE→STALE (emits callback)

      // 4. STALE → LIVE: only a tick can clear STALE
      final long recoverTime = staleTime + 1L;
      clock.nanos = recoverTime;
      tracker.onTickFragment(recoverTime); // transitions STALE→LIVE (emits callback)

      // Extra steady-state calls on all three entry points to warm the non-transition paths
      tracker.onTickFragment(recoverTime + 1L);
      tracker.onHeartbeatFragment(recoverTime + 2L);
      tracker.tick(recoverTime + 3L);
    }

    // Reset tracker to a clean LIVE state for the measurement phase.
    // We can't reset internal fields directly, so we call onTickFragment to ensure LIVE.
    clock.nanos = 1_000_000_000L;
    tracker.onTickFragment(clock.nanos);
  }
}
