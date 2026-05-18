package com.trading.engine.messages.telemetry;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

/**
 * JFR custom event emitted by {@link
 * com.trading.engine.websocket.MarketDataSubscriptionLivenessTracker} immediately before the {@link
 * com.trading.engine.messages.sbe.FeedStateEnum} state change is forwarded to the websocket egress
 * queue.
 *
 * <p><b>Sampling model.</b> No {@code @Period} and no {@code @Threshold} annotations — every state
 * transition emits unconditionally. State transitions ({@code LIVE → QUIET}, {@code LIVE/QUIET →
 * STALE}, {@code STALE/QUIET → LIVE}) are rare lifecycle events (at most one per several heartbeat
 * intervals, i.e. once every 1–3 seconds in pathological conditions). Missing a transition event in
 * JFR would make post-incident triage impossible; the volume is negligibly small (a few events per
 * hour in normal operation).
 *
 * <p><b>Ordering invariant.</b> The event is committed BEFORE the {@code onTransition} callback
 * encodes and enqueues the {@code MarketDataFeedStateChange} SBE frame. This ordering ensures the
 * JFR event is captured even if the wire-encode path throws (e.g. buffer overflow) — the JFR record
 * always reflects that the transition <em>was decided</em>, not that it <em>was delivered</em>.
 *
 * <p><b>Allocation model.</b> The caller guards the field-set + commit with {@code shouldCommit()}.
 * When JFR is not recording, {@code shouldCommit()} returns {@code false} in nanoseconds and the
 * guard short-circuits before any field write. Fields are two {@code String} references (state
 * names, typically interned constants) and one {@code long} (monotonic ns). The caller must wrap
 * the emit block as follows:
 *
 * <pre>{@code
 * final var e = new MarketDataFeedStateTransition();
 * if (e.shouldCommit()) {
 *     e.from = fromStateName;
 *     e.to = toStateName;
 *     e.lastFragmentNs = lastFragmentNs;
 *     e.commit();
 * }
 * // fire the wire emission AFTER commit
 * onTransition.accept((long) newState);
 * }</pre>
 *
 * <p><b>Threading model.</b> Instances are always created and committed on the websocket-server
 * {@code AeronEgressThread}'s agent thread — the single-writer thread of {@link
 * com.trading.engine.websocket.MarketDataSubscriptionLivenessTracker}. Never shared across threads.
 *
 * <p><b>State name mapping.</b> The {@link #from} and {@link #to} fields carry the {@link
 * com.trading.engine.messages.sbe.FeedStateEnum} name string (e.g. {@code "Live"}, {@code "Quiet"},
 * {@code "Stale"}) rather than an ordinal. State transitions are rare enough that the {@code
 * String} allocation on the recording path is inconsequential, and human-readable names
 * dramatically improve Mission Control usability. The string constants are stored at class level in
 * the caller to avoid per-transition allocation.
 *
 * <p><b>Design rationale.</b> The EBS Direct / ICE Impact pattern requires a deterministic record
 * of every feed-health state change for post-trade audit. JFR is the lowest-overhead mechanism
 * available in the JDK standard library — no external agent, no allocation on the cold path when
 * JFR is not recording, and queryable via standard JMC or {@code jcmd}.
 *
 * @see MarketDataTickPublished
 * @see MarketDataTickRejected
 */
@Name("trading.MarketDataFeedStateTransition")
@Label("Market Data Feed State Transition")
@Description(
    "Emitted on every LIVE/QUIET/STALE state transition in the market-data subscription liveness tracker.")
@Category({"Trading Engine", "Market Data"})
@StackTrace(false)
public final class MarketDataFeedStateTransition extends Event {

  /**
   * Human-readable name of the state being transitioned FROM. One of {@code "Live"}, {@code
   * "Quiet"}, or {@code "Stale"}, corresponding to {@link
   * com.trading.engine.messages.sbe.FeedStateEnum} values.
   */
  @Label("From State")
  public String from;

  /**
   * Human-readable name of the state being transitioned TO. One of {@code "Live"}, {@code "Quiet"},
   * or {@code "Stale"}.
   */
  @Label("To State")
  public String to;

  /**
   * Monotonic nanoseconds ({@link org.agrona.concurrent.NanoClock#nanoTime()}) of the most recent
   * inbound Aeron fragment (tick or heartbeat) at the time the transition was decided. This value
   * lets post-incident analysis reconstruct the exact silence duration that triggered the
   * transition: {@code transitionDecisionNs - lastFragmentNs}.
   */
  @Label("Last Fragment Nanos")
  public long lastFragmentNs;
}
