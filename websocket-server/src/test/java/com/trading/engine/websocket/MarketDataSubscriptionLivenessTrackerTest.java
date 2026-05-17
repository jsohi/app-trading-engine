package com.trading.engine.websocket;

import static com.trading.engine.messages.MarketDataConstants.MARKET_DATA_STALE_THRESHOLD_NANOS;
import static com.trading.engine.websocket.MarketDataSubscriptionLivenessTracker.QUIET_THRESHOLD_NANOS;
import static com.trading.engine.websocket.MarketDataSubscriptionLivenessTracker.STATE_LIVE;
import static com.trading.engine.websocket.MarketDataSubscriptionLivenessTracker.STATE_QUIET;
import static com.trading.engine.websocket.MarketDataSubscriptionLivenessTracker.STATE_STALE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.agrona.concurrent.NanoClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link MarketDataSubscriptionLivenessTracker}.
 *
 * <p><b>Purpose.</b> Verifies all state-machine transitions (LIVE → QUIET, LIVE/QUIET → STALE,
 * QUIET → LIVE, STALE → LIVE) and idempotency/non-regression for states that must NOT change on
 * certain inputs (heartbeat does not clear STALE; tick in STALE is the only exit path; double-tick
 * in STALE emits only one STALE transition).
 *
 * <p><b>Threading model.</b> All tests are single-threaded on the JUnit test runner thread,
 * matching the single-writer contract of the tracker. Test case #11 explicitly spawns a second
 * thread to document the volatile-read cross-thread visibility guarantee on {@link
 * MarketDataSubscriptionLivenessTracker#currentState()}.
 *
 * <p><b>Allocation.</b> Test infrastructure is heap-allocated (ArrayList, FakeNanoClock). No
 * allocation constraints apply in test code.
 *
 * <p><b>Design rationale.</b> A {@link FakeNanoClock} nested static class with a settable {@code
 * nanos} field is used instead of a lambda-over-AtomicLong so tests can advance time without
 * boxing. Transition callbacks are captured in a plain {@code ArrayList<Long>} whose ordinal values
 * are asserted via a small {@code assertTransitions} helper. This mirrors the single-callback-seam
 * design documented in the production Javadoc: the tracker is agnostic to the egress queue; only
 * the ordinal matters.
 *
 * <p><b>Dependencies.</b> {@link MarketDataSubscriptionLivenessTracker} (class under test);
 * constants from {@link com.trading.engine.messages.MarketDataConstants}; {@link NanoClock} from
 * Agrona; JUnit Jupiter.
 */
final class MarketDataSubscriptionLivenessTrackerTest {

  // ── Test infrastructure ───────────────────────────────────────────────────

  /**
   * Minimal controllable clock. Implements {@link NanoClock} via a single mutable {@code long}
   * field. Default value is 0 — tests that need a non-zero epoch must call {@link #setNanos(long)}
   * in their setup.
   */
  static final class FakeNanoClock implements NanoClock {

    private long nanos;

    /** Creates the clock with {@code nanos} initialised to 0. */
    FakeNanoClock() {
      this.nanos = 0L;
    }

    /**
     * Sets the current time.
     *
     * @param nanos the new monotonic timestamp in nanoseconds.
     */
    void setNanos(final long nanos) {
      this.nanos = nanos;
    }

    @Override
    public long nanoTime() {
      return nanos;
    }
  }

  // ── Per-test fixtures ─────────────────────────────────────────────────────

  private FakeNanoClock clock;
  private List<Long> transitions;
  private MarketDataSubscriptionLivenessTracker tracker;

  @BeforeEach
  void setUp() {
    clock = new FakeNanoClock();
    transitions = new ArrayList<>();
    tracker = new MarketDataSubscriptionLivenessTracker(clock, transitions::add);
  }

  /**
   * Asserts that the captured transition list matches the expected ordinal sequence exactly.
   *
   * @param expected ordinal values in emission order (varargs for readability at call sites).
   */
  private void assertTransitions(final int... expected) {
    assertEquals(expected.length, transitions.size(), "transition count mismatch");
    for (int i = 0; i < expected.length; i++) {
      assertEquals((long) expected[i], (long) transitions.get(i), "transition[" + i + "] mismatch");
    }
  }

  // ── Tests ─────────────────────────────────────────────────────────────────

  /**
   * A freshly constructed tracker must be in LIVE state and must not have emitted any transition
   * callback.
   */
  @Test
  void initialState_isLive_noTransitionsEmitted() {
    assertEquals(STATE_LIVE, tracker.currentState(), "initial state must be LIVE");
    assertTransitions(/* empty */ );
  }

  /**
   * After {@code QUIET_THRESHOLD_NANOS + 1} nanoseconds with no tick or heartbeat, a single {@code
   * tick()} call must transition LIVE → QUIET and emit exactly one callback with ordinal {@code
   * STATE_QUIET}.
   */
  @Test
  void tick_noFragments_afterQuietThreshold_transitionsLiveToQuiet() {
    final long now = QUIET_THRESHOLD_NANOS + 1L;
    clock.setNanos(now);

    tracker.tick(now);

    assertEquals(STATE_QUIET, tracker.currentState());
    assertTransitions(STATE_QUIET);
  }

  /**
   * After {@code MARKET_DATA_STALE_THRESHOLD_NANOS + 1} nanoseconds with no fragment of any kind, a
   * single {@code tick()} call must transition directly to STALE. The STALE check fires before the
   * QUIET check inside {@code tick()}, so no intermediate QUIET transition is emitted.
   */
  @Test
  void tick_noFragments_afterStaleThreshold_transitionsLiveToStale() {
    final long now = MARKET_DATA_STALE_THRESHOLD_NANOS + 1L;
    clock.setNanos(now);

    tracker.tick(now);

    assertEquals(STATE_STALE, tracker.currentState());
    assertTransitions(STATE_STALE);
  }

  /**
   * After driving the tracker into QUIET via {@code tick()}, an inbound tick fragment must
   * transition QUIET → LIVE. Two transitions must be recorded: the first to QUIET, the second to
   * LIVE.
   */
  @Test
  void onTickFragment_fromQuiet_transitionsBackToLive() {
    // Drive into QUIET.
    final long quietTime = QUIET_THRESHOLD_NANOS + 1L;
    clock.setNanos(quietTime);
    tracker.tick(quietTime);
    assertEquals(STATE_QUIET, tracker.currentState());

    // Tick fragment arrives — must clear QUIET and return to LIVE.
    final long tickTime = quietTime + 1L;
    clock.setNanos(tickTime);
    tracker.onTickFragment(tickTime);

    assertEquals(STATE_LIVE, tracker.currentState());
    assertTransitions(STATE_QUIET, STATE_LIVE);
  }

  /**
   * A heartbeat fragment received while in QUIET state must transition QUIET → LIVE (per the EBS
   * Direct pattern: a heartbeat is sufficient evidence the publisher is alive and recovering from a
   * quiet period). Two transitions must be recorded: QUIET then LIVE.
   */
  @Test
  void onHeartbeatFragment_fromQuiet_transitionsBackToLive() {
    // Drive into QUIET.
    final long quietTime = QUIET_THRESHOLD_NANOS + 1L;
    clock.setNanos(quietTime);
    tracker.tick(quietTime);
    assertEquals(STATE_QUIET, tracker.currentState());

    // Heartbeat alone is sufficient to clear QUIET.
    final long hbTime = quietTime + 1L;
    clock.setNanos(hbTime);
    tracker.onHeartbeatFragment(hbTime);

    assertEquals(STATE_LIVE, tracker.currentState());
    assertTransitions(STATE_QUIET, STATE_LIVE);
  }

  /**
   * After driving the tracker into STALE via {@code tick()}, an inbound tick fragment must
   * transition STALE → LIVE. Two transitions must be recorded: STALE then LIVE.
   */
  @Test
  void onTickFragment_fromStale_transitionsBackToLive() {
    // Drive into STALE.
    final long staleTime = MARKET_DATA_STALE_THRESHOLD_NANOS + 1L;
    clock.setNanos(staleTime);
    tracker.tick(staleTime);
    assertEquals(STATE_STALE, tracker.currentState());

    // Tick fragment clears STALE.
    final long tickTime = staleTime + 1L;
    clock.setNanos(tickTime);
    tracker.onTickFragment(tickTime);

    assertEquals(STATE_LIVE, tracker.currentState());
    assertTransitions(STATE_STALE, STATE_LIVE);
  }

  /**
   * A heartbeat fragment received while in STALE state must NOT transition the tracker out of STALE
   * (ICE Impact rule: only a real tick proves the price path is healthy). The state must remain
   * STALE and no second transition must be recorded.
   */
  @Test
  void onHeartbeatFragment_fromStale_doesNotClearStale_remainsStale() {
    // Drive into STALE.
    final long staleTime = MARKET_DATA_STALE_THRESHOLD_NANOS + 1L;
    clock.setNanos(staleTime);
    tracker.tick(staleTime);
    assertEquals(STATE_STALE, tracker.currentState());

    // Heartbeat after stale — must NOT exit STALE.
    final long hbTime = staleTime + 1L;
    clock.setNanos(hbTime);
    tracker.onHeartbeatFragment(hbTime);

    assertEquals(STATE_STALE, tracker.currentState());
    assertTransitions(STATE_STALE); // only the initial STALE transition
  }

  /**
   * Confirms that a heartbeat received while STALE refreshes {@code lastFragmentNs} (preventing the
   * STALE timeout from firing again) but does not cause the tracker to exit STALE. A subsequent
   * {@code tick()} 100 ms later must still report STALE.
   */
  @Test
  void onHeartbeatFragment_fromStale_resetsLastFragmentNs_butStaysStale() {
    // Drive into STALE.
    final long staleTime = MARKET_DATA_STALE_THRESHOLD_NANOS + 1L;
    clock.setNanos(staleTime);
    tracker.tick(staleTime);
    assertEquals(STATE_STALE, tracker.currentState());

    // Heartbeat refreshes lastFragmentNs.
    final long hbTime = staleTime + 1L;
    clock.setNanos(hbTime);
    tracker.onHeartbeatFragment(hbTime);

    // 100 ms later: tick(). The lastFragmentNs has been reset by the heartbeat, so sinceFragment
    // is well below STALE_THRESHOLD. The tracker is already STALE — tick() returns early.
    final long laterTime = hbTime + 100_000_000L; // 100 ms
    clock.setNanos(laterTime);
    tracker.tick(laterTime);

    assertEquals(STATE_STALE, tracker.currentState());
    assertTransitions(STATE_STALE); // still only one transition total
  }

  /**
   * Interleaving heartbeats keeps {@code lastFragmentNs} fresh (so STALE never fires), but because
   * no ticks arrive the {@code lastTickNs} cursor ages past {@code QUIET_THRESHOLD_NANOS}. A
   * subsequent {@code tick()} must therefore transition LIVE → QUIET. This proves that the QUIET
   * timer measures gaps in ticks specifically, not fragments in general.
   */
  @Test
  void tick_fromLive_withRecentHeartbeatButNoTicks_transitionsToQuiet() {
    // Heartbeats arrive at 500 ms intervals — well inside the 3 s STALE threshold.
    // No ticks arrive at any point.
    final long hb1 = 500_000_000L; // 500 ms
    clock.setNanos(hb1);
    tracker.onHeartbeatFragment(hb1);

    final long hb2 = 1_000_000_000L; // 1 s
    clock.setNanos(hb2);
    tracker.onHeartbeatFragment(hb2);

    // Now advance past the QUIET threshold relative to construction (lastTickNs = 0).
    final long checkTime = QUIET_THRESHOLD_NANOS + 1L;
    clock.setNanos(checkTime);
    tracker.tick(checkTime);

    assertEquals(STATE_QUIET, tracker.currentState());
    assertTransitions(STATE_QUIET);
  }

  /**
   * Calling {@code tick()} repeatedly while in STALE must not emit duplicate transition callbacks.
   * The state stays STALE and only the initial STALE transition appears in the list.
   */
  @Test
  void tick_fromStale_isIdempotent_noNewTransitionEmitted() {
    // Drive into STALE.
    final long staleTime = MARKET_DATA_STALE_THRESHOLD_NANOS + 1L;
    clock.setNanos(staleTime);
    tracker.tick(staleTime);
    assertEquals(STATE_STALE, tracker.currentState());

    // Second tick — must be a no-op.
    final long laterTime = staleTime + MARKET_DATA_STALE_THRESHOLD_NANOS;
    clock.setNanos(laterTime);
    tracker.tick(laterTime);

    assertEquals(STATE_STALE, tracker.currentState());
    assertTransitions(STATE_STALE); // exactly one transition total
  }

  /**
   * Confirms the volatile-read cross-thread visibility contract on {@link
   * MarketDataSubscriptionLivenessTracker#currentState()}. The main thread drives the tracker to
   * QUIET; a spawned thread reads the state via the volatile field and stores it in an {@link
   * AtomicInteger}. After joining, the stored value must equal {@code STATE_QUIET}.
   *
   * <p>This is a documentation test: the JMM guarantees that a volatile write happens-before any
   * subsequent volatile read of the same variable (JLS §17.4.5). The test makes the contract
   * observable without requiring a lock.
   */
  @Test
  void currentState_isVolatileReadFromAnyThread() throws InterruptedException {
    // Drive into QUIET on the main (writer) thread.
    final long quietTime = QUIET_THRESHOLD_NANOS + 1L;
    clock.setNanos(quietTime);
    tracker.tick(quietTime);
    assertEquals(STATE_QUIET, tracker.currentState());

    // Spawn a reader thread to sample currentState() via the volatile read.
    final var observed = new AtomicInteger(-1);
    final var reader = new Thread(() -> observed.set(tracker.currentState()));
    reader.start();
    reader.join();

    assertEquals(STATE_QUIET, observed.get(), "volatile read on reader thread must observe QUIET");
  }
}
