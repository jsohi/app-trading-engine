package com.trading.engine.messages.telemetry;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
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
 * <p><b>Allocation model.</b> The event object is allocated per-call site, but the field-set and
 * {@code commit()} are guarded by {@code shouldCommit()}: when JFR is not recording or outside the
 * 100 ms sampling window, {@code shouldCommit()} returns {@code false} in nanoseconds and the guard
 * short-circuits before any field write. On the recording path, the fields are primitives plus one
 * {@code String} reference (interned symbol name) — no heap allocation beyond the event object
 * itself. The caller is responsible for wrapping the entire emit block in the {@code
 * shouldCommit()} guard to preserve the hot-path zero-alloc invariant:
 *
 * <pre>{@code
 * final var e = new MarketDataTickPublished();
 * if (e.shouldCommit()) {
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
