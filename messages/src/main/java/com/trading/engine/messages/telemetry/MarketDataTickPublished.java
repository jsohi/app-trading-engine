package com.trading.engine.messages.telemetry;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.EventType;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.Period;
import jdk.jfr.StackTrace;

/**
 * JFR custom event emitted by {@link com.trading.engine.pricing.market.MarketDataPublisher} each
 * time a conflated market-data tick is successfully offered to the Aeron publication on stream 204.
 *
 * <p><b>Sampling model.</b> Annotated with {@code @Period("100 ms")} — the JFR runtime samples at
 * 100 ms intervals (10 Hz) rather than recording every publication. At a 5 ms drain cadence and 4
 * active symbols, the publisher offers up to 800 ticks/second; recording each one would dominate
 * the JFR buffer. A 100 ms period preserves the publish-latency histogram shape (CME MDP 3.0
 * pattern) while reducing event volume by ~50×.
 *
 * <p><b>Threshold note.</b> {@code @Threshold} is intentionally absent. {@code @Threshold(value =
 * "0 ms")} gates on the event's <em>duration</em>, which is undefined for a point event like a
 * single publish call (duration ≈ 0 µs → every event would be below any non-zero threshold and
 * silently dropped). {@code @Period} is the correct annotation for a point-in-time measurement
 * whose volume must be controlled.
 *
 * <p><b>Allocation model.</b> The event object allocation itself is gated by a cheap
 * pre-construction {@link EventType#isEnabled()} check on the cached {@link #TYPE} field — NOT by
 * the post-construction {@code shouldCommit()} call. HotSpot escape analysis cannot prove {@code
 * Event.shouldCommit()} is pure (it dispatches through a native method) and therefore cannot
 * scalar-replace the {@code new Event()} allocation; under JFR-on the unguarded {@code new ...();
 * if (e.shouldCommit())} pattern leaks ~96 B/instance onto the heap on every emit. {@link
 * EventType#isEnabled()} is a cheap volatile read that returns {@code false} when no recording has
 * subscribed to this event type — the canonical fast-path "is anyone listening" check recommended
 * by the OpenJDK JFR team. When disabled (the steady-state production case AND the JFR-off case),
 * no {@code Event} subclass is allocated at all. When enabled, the per-sample allocation is paid in
 * exchange for the JFR record. Callers MUST use this emit shape:
 *
 * <pre>{@code
 * if (MarketDataTickPublished.TYPE.isEnabled()) {
 *     final var e = new MarketDataTickPublished();
 *     e.symbol = symbol;
 *     e.symbolSeq = seq;
 *     e.publishLatencyNanos = lat;
 *     e.commit();
 * }
 * }</pre>
 *
 * <p><b>Threading model.</b> Instances are always created and committed on the pricing-service
 * agent thread (the single-writer thread of {@link
 * com.trading.engine.pricing.market.MarketDataPublisher}). Never shared across threads.
 *
 * <p><b>Design rationale.</b> Mirrors the CME MDP 3.0 / OpenHFT Chronicle instrumentation pattern:
 * a custom JFR event per wire-publish boundary captures symbol-level latency distribution without
 * requiring a separate metrics sink (Prometheus, InfluxDB). The JFR recording is started by the
 * launcher (Phase 1 §1) with {@code settings=default}; no new launcher wiring is needed for this
 * event class.
 *
 * <p><b>Event name.</b> {@code trading.MarketDataTickPublished}. Queryable via JFR streaming API:
 * {@code RecordingFile.readAllEvents()} or Mission Control event browser.
 *
 * @see MarketDataTickRejected
 * @see MarketDataFeedStateTransition
 */
@Name("trading.MarketDataTickPublished")
@Label("Market Data Tick Published")
@Description(
    "Sampled per-symbol market-data tick publish latency on the pricing-service agent thread.")
@Category({"Trading Engine", "Market Data"})
@Period("100 ms")
@StackTrace(false)
public final class MarketDataTickPublished extends Event {

  /**
   * Cached {@link EventType} for the cheap pre-allocation {@link EventType#isEnabled()} gate — see
   * emit sites for the pattern.
   */
  public static final EventType TYPE = EventType.getEventType(MarketDataTickPublished.class);

  /** Symbol name (8-character padded ASCII, e.g. {@code "EURUSD "}). */
  @Label("Symbol")
  public String symbol;

  /**
   * Per-symbol monotonic sequence number assigned at drain time. The websocket-server uses this to
   * detect gaps in the market-data stream (any discontinuity beyond {@code +1} triggers a snapshot
   * request). A value of {@code 0} is the snapshot sentinel (re-sent on demand).
   */
  @Label("Symbol Seq")
  public long symbolSeq;

  /**
   * End-to-end publish latency from the instant the adapter sampled the rate ({@code ingressNanos},
   * FIX tag-60 {@code TransactTime} equivalent) to the instant the SBE frame was offered to the
   * Aeron publication. Measured in nanoseconds using {@link org.agrona.concurrent.EpochNanoClock}.
   * Negative values indicate a clock anomaly (e.g. cross-host clock drift before PTP sync).
   */
  @Label("Publish Latency Nanos")
  public long publishLatencyNanos;
}
