package com.trading.engine.websocket;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Micrometer metric definitions for the WebSocket server.
 *
 * <p>All 6 metrics from {@code docs/websocket-architecture.md} Section 6 plus operational counters.
 * Metrics are registered eagerly at construction time so they appear in Prometheus scrape output
 * even before the first event.
 *
 * <p><b>Thread safety.</b> All Micrometer meters are thread-safe. The gauges wrap atomic holders
 * that are updated from the respective owning threads (AeronEgressThread, Netty event loop,
 * SessionManager).
 *
 * <p><b>Metric names follow the Micrometer naming convention</b> ({@code dot.separated.lowercase}).
 *
 * @see <a href="docs/websocket-architecture.md">WebSocket Architecture — Section 6</a>
 */
public final class WebSocketMetrics {

  // --- Gauge holders (updated by owning threads, read by Prometheus scraper) ---
  private final AtomicInteger activeConnections = new AtomicInteger();
  private final AtomicLong queueDepth = new AtomicLong();
  private final AtomicLong maxClientLag = new AtomicLong();

  // --- Timers ---
  private final Timer aeronPollLatency;

  // --- Counters ---
  private final Counter messagesDropped;
  private final Counter replayEvictions;
  private final Counter authSuccess;
  private final Counter authFailure;
  private final Counter rateLimited;

  /**
   * Register all metrics with the given registry.
   *
   * @param registry the Micrometer meter registry (e.g. {@code PrometheusMeterRegistry}), must not
   *     be null
   * @throws NullPointerException if registry is null
   */
  public WebSocketMetrics(final MeterRegistry registry) {
    Objects.requireNonNull(registry, "registry");
    // Architecture doc Section 6 — 6 required metrics
    this.aeronPollLatency =
        Timer.builder("websocket.aeron.poll.latency")
            .description("Time spent in AeronCluster.pollEgress() per invocation")
            .register(registry);

    registry.gauge("websocket.client.lag", maxClientLag, AtomicLong::doubleValue);

    registry.gauge("websocket.queue.depth", queueDepth, AtomicLong::doubleValue);

    this.messagesDropped =
        Counter.builder("websocket.messages.dropped.backpressure")
            .description("Messages dropped due to queue overflow or slow consumer backpressure")
            .register(registry);

    registry.gauge("websocket.connections.active", activeConnections, AtomicInteger::doubleValue);

    // JVM GC pause — auto-configured via MeterBinder in WebSocketServerMain

    // --- Operational counters ---
    this.replayEvictions =
        Counter.builder("websocket.replay.eviction")
            .description("Replay ring buffer entries evicted due to ring full")
            .register(registry);

    this.authSuccess =
        Counter.builder("websocket.auth.success")
            .description("Successful JWT authentications")
            .register(registry);

    this.authFailure =
        Counter.builder("websocket.auth.failure")
            .description("Failed JWT authentications (invalid, expired, rejected algorithm)")
            .register(registry);

    this.rateLimited =
        Counter.builder("websocket.rate.limited")
            .description("Commands rejected due to rate limiting")
            .register(registry);
  }

  // --- Gauge update methods ---

  /** Increment active connection count. Called from {@code WebSocketSessionManager}. */
  public void connectionOpened() {
    activeConnections.incrementAndGet();
  }

  /** Decrement active connection count. Called from {@code WebSocketSessionManager}. */
  public void connectionClosed() {
    activeConnections.updateAndGet(v -> Math.max(0, v - 1));
  }

  /**
   * Update the current MpscArrayQueue depth. Called from {@code AeronEgressThread} after each poll
   * cycle.
   *
   * @param depth current number of entries in the queue
   */
  public void updateQueueDepth(final long depth) {
    queueDepth.set(depth);
  }

  /**
   * Update the maximum client lag across all sessions. Called from {@code SlowConsumerHandler}
   * after scanning per-session lags.
   *
   * @param lag the current maximum unacked message count across all sessions
   */
  public void updateMaxClientLag(final long lag) {
    maxClientLag.set(lag);
  }

  // --- Timer access ---

  /**
   * Returns the Aeron poll latency timer for recording individual poll durations.
   *
   * @return the poll latency timer
   */
  public Timer aeronPollLatency() {
    return aeronPollLatency;
  }

  // --- Counter access ---

  /** Record a message drop due to backpressure. */
  public void messageDropped() {
    messagesDropped.increment();
  }

  /** Record a replay buffer eviction. */
  public void replayEviction() {
    replayEvictions.increment();
  }

  /** Record a successful JWT authentication. */
  public void authSucceeded() {
    authSuccess.increment();
  }

  /** Record a failed JWT authentication. */
  public void authFailed() {
    authFailure.increment();
  }

  /** Record a rate-limited command rejection. */
  public void commandRateLimited() {
    rateLimited.increment();
  }
}
