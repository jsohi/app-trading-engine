package com.trading.engine.websocket;

import static com.trading.engine.messages.MarketDataConstants.MARKET_DATA_HEARTBEAT_BASE_MS;
import static com.trading.engine.messages.MarketDataConstants.MARKET_DATA_STALE_THRESHOLD_NANOS;

import com.trading.engine.messages.sbe.FeedStateEnum;
import com.trading.engine.messages.telemetry.MarketDataFeedStateTransition;
import java.util.Objects;
import java.util.function.LongConsumer;
import org.agrona.concurrent.NanoClock;

/**
 * Server-side LIVE / QUIET / STALE liveness state machine for the market-data broadcast feed.
 *
 * <p>Owned by the {@code AeronEgressThread} agent loop. The egress thread calls {@link
 * #onTickFragment(long)} for every inbound {@code MarketDataTick} (template 54), {@link
 * #onHeartbeatFragment(long)} for every {@code MarketDataHeartbeat} (template 55), and {@link
 * #tick(long)} once per agent cycle (or on a periodic 500 ms timer) to drive timeout-based
 * transitions. On every state change the tracker invokes the {@link #onTransition} callback so the
 * caller can encode and enqueue a {@code MarketDataFeedStateChange} (template 57) onto the
 * per-session reliable egress queue.
 *
 * <p><b>State transitions</b> (EBS Direct / ICE Impact pattern):
 *
 * <ul>
 *   <li>{@code LIVE → QUIET} — no tick (template 54) for {@code HEARTBEAT_BASE_MS × 1.5} but
 *       heartbeats are arriving. The publisher is up but all symbols are idle.
 *   <li>{@code QUIET → LIVE} — next inbound tick OR heartbeat. Either signal indicates the
 *       publisher's price feed is recovering from a quiet period.
 *   <li>{@code LIVE / QUIET → STALE} — no fragment of any kind for {@link
 *       MarketDataConstants#MARKET_DATA_STALE_THRESHOLD_NANOS} (3 s = 3 × heartbeat).
 *   <li>{@code STALE → LIVE} — <b>next inbound tick (template 54) ONLY</b>; heartbeats do NOT clear
 *       STALE. A publisher whose price-feed thread is dead but whose heartbeat thread is alive is
 *       <em>worse</em> than fully dead (the UI shows live transport but stale prices). Only a real
 *       tick proves the price path is healthy.
 * </ul>
 *
 * <p><b>Threading model.</b> Single-writer — all entry points ({@code onTickFragment}, {@code
 * onHeartbeatFragment}, {@code tick}) MUST be called from the {@code AeronEgressThread}'s agent
 * thread. No fences or synchronisation; the JMM happens-before edge on the same thread suffices.
 *
 * <p><b>Allocation.</b> Zero allocation after construction. State transitions invoke the pre-bound
 * {@link LongConsumer} callback (typically a method reference to the caller's {@code
 * encodeAndEnqueueFeedState} method) — no per-transition lambda allocation. Internal state is
 * primitive {@code long}/{@code int} fields.
 *
 * <p><b>Design rationale.</b>
 *
 * <ul>
 *   <li><b>QUIET threshold = HEARTBEAT_BASE_MS × 1.5</b> — tolerates one full heartbeat-jitter
 *       window plus a small grace before declaring no-ticks. Below the 3 × heartbeat STALE
 *       threshold so the UI sees an intermediate state instead of jumping straight from LIVE to
 *       STALE on a single missed cycle.
 *   <li><b>STALE clear requires a tick</b> — see ICE Impact pattern above; heartbeats arriving
 *       while STALE update {@code lastFragmentNs} for the QUIET threshold but cannot transition out
 *       of STALE.
 *   <li><b>Single callback seam</b> — the tracker doesn't know about Aeron or the egress queue
 *       layout; the caller binds a {@code LongConsumer} that encodes the SBE frame and enqueues it.
 *       Lets the unit test drive transitions and assert on captured callback arguments without
 *       spinning up the websocket-server full stack.
 * </ul>
 *
 * <p><b>Dependencies.</b> {@link NanoClock} for monotonic time; cross-module constants from {@link
 * com.trading.engine.messages.MarketDataConstants}.
 */
public final class MarketDataSubscriptionLivenessTracker {

  /**
   * QUIET threshold in nanoseconds — {@code HEARTBEAT_BASE_MS × 1_500_000 ns/ms × 1.5}. After this
   * duration without a tick the tracker transitions LIVE → QUIET.
   */
  static final long QUIET_THRESHOLD_NANOS = (MARKET_DATA_HEARTBEAT_BASE_MS * 1_000_000L * 3L) / 2L;

  /** LIVE state ordinal — matches {@link FeedStateEnum#Live} on the wire. */
  public static final int STATE_LIVE = (int) FeedStateEnum.Live.value();

  /** QUIET state ordinal — matches {@link FeedStateEnum#Quiet}. */
  public static final int STATE_QUIET = (int) FeedStateEnum.Quiet.value();

  /** STALE state ordinal — matches {@link FeedStateEnum#Stale}. */
  public static final int STATE_STALE = (int) FeedStateEnum.Stale.value();

  /**
   * Pre-interned state name strings for use in {@link MarketDataFeedStateTransition} JFR events.
   * Stored as constants to avoid per-transition {@code String} allocation; the JFR event commit is
   * guarded by {@code shouldCommit()} so the reference assignment is skipped when JFR is not
   * recording.
   */
  private static final String STATE_NAME_LIVE = "Live";

  private static final String STATE_NAME_QUIET = "Quiet";

  private static final String STATE_NAME_STALE = "Stale";

  private final NanoClock nanoClock;
  private final LongConsumer onTransition;

  /**
   * Current liveness state — the FeedStateEnum byte value. Volatile so external metrics gauges can
   * sample it from a different thread (the {@code marketdata.feed.state} Micrometer gauge reads
   * this in Phase 3 Commit 9 instrumentation).
   */
  private volatile int state;

  /**
   * Monotonic ns of the most recent inbound tick (template 54). Updated by {@link
   * #onTickFragment(long)}; read by {@link #tick(long)} for the QUIET transition.
   */
  private long lastTickNs;

  /**
   * Monotonic ns of the most recent inbound fragment (tick OR heartbeat). Updated by both {@code
   * onTickFragment} and {@code onHeartbeatFragment}; read by {@link #tick(long)} for the STALE
   * transition.
   */
  private long lastFragmentNs;

  /**
   * Constructs the tracker in LIVE state with {@code lastTickNs} and {@code lastFragmentNs} seeded
   * to {@code nanoClock.nanoTime()}.
   *
   * @param nanoClock monotonic clock source (typically {@code TradingClocks.nanoClock()}).
   * @param onTransition invoked on every state transition with the new {@link FeedStateEnum}
   *     ordinal as a {@code long} argument. Bind a method reference at construction (NOT a per-call
   *     lambda) so the SAM is allocated once. May be a no-op {@code (s) -> {}} in tests that only
   *     assert on {@link #currentState()}.
   */
  public MarketDataSubscriptionLivenessTracker(
      final NanoClock nanoClock, final LongConsumer onTransition) {
    this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
    this.onTransition = Objects.requireNonNull(onTransition, "onTransition");
    final long now = nanoClock.nanoTime();
    this.state = STATE_LIVE;
    this.lastTickNs = now;
    this.lastFragmentNs = now;
  }

  /**
   * Records an inbound {@code MarketDataTick} (template 54). Updates both the {@code lastTickNs}
   * and {@code lastFragmentNs} cursors. Transitions QUIET → LIVE or STALE → LIVE if currently in
   * either of those states (the only paths out of STALE per the EBS Direct / ICE Impact rule).
   *
   * @param nowNanos monotonic ns of the fragment arrival.
   */
  public void onTickFragment(final long nowNanos) {
    lastTickNs = nowNanos;
    lastFragmentNs = nowNanos;
    if (state != STATE_LIVE) {
      transitionTo(STATE_LIVE);
    }
  }

  /**
   * Records an inbound {@code MarketDataHeartbeat} (template 55). Updates the {@code
   * lastFragmentNs} cursor only (heartbeats do NOT update {@code lastTickNs} — the QUIET threshold
   * measures gaps in ticks specifically).
   *
   * <p>Transitions QUIET → LIVE (a heartbeat is sufficient evidence the publisher is alive, which
   * is the QUIET-exit signal). Does NOT transition STALE → LIVE — a stale publisher whose heartbeat
   * thread is alive but price-feed thread is dead is exactly the failure mode the STALE state
   * guards against; only a real tick clears it.
   *
   * @param nowNanos monotonic ns of the fragment arrival.
   */
  public void onHeartbeatFragment(final long nowNanos) {
    lastFragmentNs = nowNanos;
    if (state == STATE_QUIET) {
      transitionTo(STATE_LIVE);
    }
  }

  /**
   * Periodic check — called every agent-thread cycle (or every 500 ms via a scheduled timer).
   * Evaluates the timeout-based transitions:
   *
   * <ul>
   *   <li>LIVE → QUIET if no tick for {@link #QUIET_THRESHOLD_NANOS}.
   *   <li>LIVE or QUIET → STALE if no fragment of any kind for {@link
   *       MarketDataConstants#MARKET_DATA_STALE_THRESHOLD_NANOS}.
   * </ul>
   *
   * @param nowNanos monotonic ns of the current cycle.
   */
  public void tick(final long nowNanos) {
    if (state == STATE_STALE) {
      // Already STALE; only a tick (handled in onTickFragment) can transition out. The
      // periodic tick has nothing to do.
      return;
    }
    final long sinceFragment = nowNanos - lastFragmentNs;
    if (sinceFragment >= MARKET_DATA_STALE_THRESHOLD_NANOS) {
      transitionTo(STATE_STALE);
      return;
    }
    if (state == STATE_LIVE && (nowNanos - lastTickNs) >= QUIET_THRESHOLD_NANOS) {
      transitionTo(STATE_QUIET);
    }
  }

  /**
   * Returns the current state as the {@link FeedStateEnum} ordinal byte value. Safe to call from
   * any thread (volatile read).
   *
   * @return the current state ordinal.
   */
  public int currentState() {
    return state;
  }

  /**
   * Convenience accessor for the injected clock — used by callers that want to drive {@code
   * tick(nanoClock.nanoTime())} without holding their own clock reference.
   *
   * @return the injected {@link NanoClock}.
   */
  public NanoClock nanoClock() {
    return nanoClock;
  }

  // ────────────────────────────────────────────────────────────────────────

  private void transitionTo(final int newState) {
    final int oldState = this.state;
    this.state = newState;
    // Emit JFR event BEFORE the wire emission so the record is captured even if the
    // encode/enqueue path throws (EBS Direct / ICE Impact audit-trail ordering invariant).
    // Gated by the cheap pre-construction EventType.isEnabled() volatile read on the cached TYPE
    // field — when no recording has subscribed to this event the Event object is NEVER allocated.
    // The post-construction Event.shouldCommit() pattern was abandoned because HotSpot escape
    // analysis cannot scalar-replace the new-Event() call (shouldCommit() dispatches through a
    // native method whose purity EA cannot prove). The state-name String constants are pre-interned
    // class-level fields — no per-transition allocation on the recording path either.
    if (MarketDataFeedStateTransition.TYPE.isEnabled()) {
      final var jfrTransition = new MarketDataFeedStateTransition();
      jfrTransition.from = stateOrdinalToName(oldState);
      jfrTransition.to = stateOrdinalToName(newState);
      jfrTransition.lastFragmentNs = lastFragmentNs;
      jfrTransition.commit();
    }
    onTransition.accept((long) newState);
  }

  /**
   * Maps a state ordinal to its human-readable name for JFR events. Returns the pre-interned
   * constant string to avoid allocation. Falls back to {@code "Unknown"} for unexpected values
   * (defensive; the state machine should never produce an unrecognised ordinal).
   *
   * @param stateOrdinal one of {@link #STATE_LIVE}, {@link #STATE_QUIET}, {@link #STATE_STALE}.
   * @return the human-readable state name.
   */
  private static String stateOrdinalToName(final int stateOrdinal) {
    if (stateOrdinal == STATE_LIVE) return STATE_NAME_LIVE;
    if (stateOrdinal == STATE_QUIET) return STATE_NAME_QUIET;
    if (stateOrdinal == STATE_STALE) return STATE_NAME_STALE;
    return "Unknown";
  }
}
