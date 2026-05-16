package com.trading.engine.messages;

/**
 * Cross-module constants for the Phase 3 market-data broadcast feed (templates 54-57 on Aeron IPC
 * streams 204 publish / 205 snapshot-request).
 *
 * <p><b>Why this lives in {@code messages}.</b> The constants are read by both {@code
 * pricing-service} (publisher) and {@code websocket-server} (subscriber + snapshot-request
 * publisher). The {@code messages} module is the only existing compile-time dep shared by both,
 * making this the natural home. Placing the constants in either of the consumer modules would
 * create a backward Gradle dependency (websocket-server → pricing-service or vice-versa) that does
 * not exist in the runtime architecture.
 *
 * <p><b>Threading model.</b> All fields are {@code public static final} primitives — safe for
 * unrestricted concurrent access across every thread in every process.
 *
 * <p><b>Allocation.</b> Zero allocation — compile-time constants only; class has a private
 * constructor and no instance state.
 *
 * <p><b>Design rationale.</b>
 *
 * <ul>
 *   <li><b>Stream IDs 204 / 205.</b> Aeron IPC stream IDs disjoint from existing cluster /
 *       orchestrator / pricing-service streams. 204 = pricing→browser publish; 205 =
 *       browser→pricing snapshot request. Two distinct streams (not multiplexed) so a slow consumer
 *       on one cannot starve the other.
 *   <li><b>5 ms publish cadence.</b> (a) Browser render ceiling — AG Grid + 60 Hz monitor = ~16 ms
 *       useful frame budget; faster ticks waste bandwidth + cause jank. (b) Conflation requires a
 *       window — drain-per-onTick (sub-µs) means zero conflation, every tick becomes a publish,
 *       slow consumers fall behind. 5 ms gives ~5× wire compression on a 1000 Hz symbol while
 *       always representing the latest top-of-book. (c) Per-publish fixed cost — Aeron offer (~5
 *       µs) + Netty enqueue (~3 µs) + Netty write (~5 µs) ≈ 13 µs/publish. At 5 ms × 4 symbols =
 *       ~52 µs/cycle = ~1% CPU on the agent thread.
 *   <li><b>1 s heartbeat base ± 10% jitter.</b> EBS Direct anti-thundering-herd discipline.
 *       Per-process XorShift64 seed = {@code epochNanoClock.nanoTime() ^ ProcessHandle.current()
 *       .pid()} ensures different processes desynchronise even when started simultaneously.
 *   <li><b>3 s stale threshold = 3 × heartbeat.</b> EBS Direct's 3× rule — tolerates one missed
 *       heartbeat plus normal jitter before declaring the feed stale.
 *   <li><b>2 s snapshot timeout.</b> Inside the 5 s spec-09 STALE budget; ensures a stalled
 *       publisher surfaces as QUIET (not STALE) within a sub-frame budget.
 *   <li><b>10 snapshot req/sec/session token bucket.</b> CME MDP 3.0 §Channel Recovery rate-limit
 *       discipline — protects the publisher from a malicious or buggy client without starving
 *       legitimate recovery flows.
 * </ul>
 *
 * <p><b>Dependencies.</b> None beyond the JDK. The corresponding wire-format definitions live in
 * {@code trading-schema.xml} templates 54-57.
 */
public final class MarketDataConstants {

  /**
   * Aeron IPC stream id for the publish direction: pricing-service →&nbsp;websocket-server. Carries
   * {@code MarketDataTick} (template 54), {@code MarketDataHeartbeat} (template 55), and {@code
   * MarketDataFeedStateChange} (template 57 — emitted by the ws-server liveness tracker, not the
   * publisher; the constant captures the stream id, not the producer).
   */
  public static final int MARKET_DATA_STREAM_ID = 204;

  /**
   * Aeron IPC stream id for the snapshot-request direction: websocket-server
   * →&nbsp;pricing-service. Carries {@code MarketDataSnapshotRequest} (template 56).
   */
  public static final int MARKET_DATA_SNAPSHOT_REQUEST_STREAM_ID = 205;

  /**
   * Publisher drain cadence in microseconds. Aligns with the browser's 30 Hz render ceiling and
   * gives ~5× wire compression on a 1000 Hz symbol. See class-level Javadoc for the three-point
   * tuning rationale (render ceiling / conflation window / per-publish cost).
   */
  public static final long MARKET_DATA_PUBLISH_CADENCE_MICROS = 5_000L;

  /**
   * Heartbeat emission base period in milliseconds. Actual per-emit period is {@code BASE_MS ± 10%}
   * computed via a per-process-seeded {@code XorShift64} so multi-process deployments do not
   * synchronise heartbeats globally.
   */
  public static final long MARKET_DATA_HEARTBEAT_BASE_MS = 1_000L;

  /**
   * Stale-feed threshold in nanoseconds. After no fragment of any kind (tick or heartbeat) for this
   * duration, the {@code MarketDataSubscriptionLivenessTracker} transitions the per-session feed
   * state to {@code STALE}. Aligns with EBS Direct's 3× heartbeat rule.
   */
  public static final long MARKET_DATA_STALE_THRESHOLD_NANOS = 3_000_000_000L;

  /**
   * Snapshot timeout in milliseconds. If the publisher does not deliver the snapshot {@code
   * MarketDataTick} within this budget, the browser's {@code feedState$} transitions to {@code
   * QUIET} (NOT {@code STALE} — the WS transport itself is healthy; only the snapshot path is
   * slow). The rate-limit token is NOT refunded on timeout (the publish to stream 205 already
   * succeeded; the publisher consumed a slot).
   */
  public static final long MARKET_DATA_SNAPSHOT_TIMEOUT_MS = 2_000L;

  /**
   * Per-session snapshot-request rate limit (tokens per second). Bucket capacity equals this value;
   * refill rate is uniform at {@code MARKET_DATA_SNAPSHOT_REQUESTS_PER_SECOND} tokens/second.
   * Enforced by {@code WebSocketFrameDispatcher.handleSubscribe} via the per-session {@code
   * snapshotTokenBucket} field on {@code WebSocketSession}. CME MDP 3.0 §Channel Recovery pattern.
   */
  public static final long MARKET_DATA_SNAPSHOT_REQUESTS_PER_SECOND = 10L;

  /**
   * Aeron IPC channel string for the market-data feed. Pinned with a 16 MiB term-length to absorb
   * GC pauses + bursts on the publisher side without back-pressuring the agent loop; slow consumers
   * drop oldest term frames AFTER the back-pressure signal reaches the publisher via the {@code
   * offer()} return code, never silently. Matches LMAX exchange-core baseline.
   */
  public static final String MARKET_DATA_CHANNEL = "aeron:ipc?term-length=16m";

  private MarketDataConstants() {}
}
