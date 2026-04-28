package com.trading.engine.websocket;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
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
  private final Counter filterMatched;
  private final Counter filterFiltered;
  private final Counter authLockout;

  // --- Replay / command / slow-consumer counters (APP-242) ---
  private final Counter gapRequestsReceived;
  private final Counter sessionResumesReceived;
  private final Counter replaysSent;
  private final Counter replayBytesSent;
  private final Counter commandsDispatched;
  private final Counter commandsRejected;
  private final Counter commandsDuplicate;
  private final Counter commandsBackpressured;
  private final Counter commandsAckDropped;
  private final Counter dedupTryLockMisses;
  private final Counter slowConsumerLevel1;
  private final Counter slowConsumerLevel2;
  private final Counter slowConsumerLevel3;
  private final Counter slowConsumerLevel4;
  private final Counter slowConsumerDisconnects;

  // --- Subscription filtering gauges ---
  private final AtomicInteger activeSubscriptions = new AtomicInteger();

  // --- Authentication timers ---
  private final Timer authLatency;

  // --- Drain cycle timer ---
  private final Timer drainCycleLatency;

  /**
   * Create a WebSocketMetrics instance with a simple in-memory registry. Suitable for dev/test or
   * when a Prometheus endpoint is not yet wired. Production callers should use {@link
   * #WebSocketMetrics(MeterRegistry)} with a PrometheusMeterRegistry.
   *
   * @return a new metrics instance with a SimpleMeterRegistry
   */
  public static WebSocketMetrics createWithDefaults() {
    return new WebSocketMetrics(new SimpleMeterRegistry());
  }

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

    // --- Subscription filtering + auth metrics ---
    this.filterMatched =
        Counter.builder("websocket.filter.matched")
            .description("Messages that passed SubscriptionFilter and were delivered")
            .register(registry);

    this.filterFiltered =
        Counter.builder("websocket.filter.filtered")
            .description("Messages filtered out by SubscriptionFilter (not delivered)")
            .register(registry);

    this.authLockout =
        Counter.builder("websocket.auth.lockout")
            .description("Connections rejected due to per-IP auth failure lockout")
            .register(registry);

    registry.gauge(
        "websocket.subscriptions.active", activeSubscriptions, AtomicInteger::doubleValue);

    this.authLatency =
        Timer.builder("websocket.auth.latency")
            .description("JWT validation latency including JWKS fetch on cache miss")
            .register(registry);

    this.drainCycleLatency =
        Timer.builder("websocket.drain.cycle.latency")
            .description("Wall-clock duration of a single drain cycle (queue poll + fan-out)")
            .register(registry);

    // APP-242 counters
    this.gapRequestsReceived =
        Counter.builder("websocket.gap.requests.received")
            .description("WebSocketGapRequest frames received from clients")
            .register(registry);
    this.sessionResumesReceived =
        Counter.builder("websocket.session.resumes.received")
            .description("SessionResume frames received from clients")
            .register(registry);
    this.replaysSent =
        Counter.builder("websocket.replays.sent")
            .description("Replay frames written to clients in response to gap/resume")
            .register(registry);
    this.replayBytesSent =
        Counter.builder("websocket.replay.bytes.sent")
            .description("Total bytes of replay payload (excluding wire envelope)")
            .register(registry);
    this.commandsDispatched =
        Counter.builder("websocket.commands.dispatched")
            .description("Commands accepted and forwarded to the cluster")
            .register(registry);
    this.commandsRejected =
        Counter.builder("websocket.commands.rejected")
            .description("Commands rejected (entitlement/template/format)")
            .register(registry);
    this.commandsDuplicate =
        Counter.builder("websocket.commands.duplicate")
            .description("Commands rejected as ClOrdID duplicates")
            .register(registry);
    this.commandsBackpressured =
        Counter.builder("websocket.commands.backpressured")
            .description("Commands throttled due to cluster BACK_PRESSURED")
            .register(registry);
    this.commandsAckDropped =
        Counter.builder("websocket.commands.ack.dropped")
            .description("CommandAck frames dropped because the ack back-channel was full")
            .register(registry);
    this.dedupTryLockMisses =
        Counter.builder("websocket.dedup.trylock.misses")
            .description("ClOrdID dedup tryLock misses (fail-open accepts)")
            .register(registry);
    this.slowConsumerLevel1 =
        Counter.builder("websocket.slow.consumer.level1")
            .description("Sessions entering slow-consumer level 1 (100KB-500KB pendingBytes)")
            .register(registry);
    this.slowConsumerLevel2 =
        Counter.builder("websocket.slow.consumer.level2")
            .description("Sessions entering slow-consumer level 2 (500KB-1MB pendingBytes)")
            .register(registry);
    this.slowConsumerLevel3 =
        Counter.builder("websocket.slow.consumer.level3")
            .description("Sessions entering slow-consumer level 3 (1MB-2MB pendingBytes)")
            .register(registry);
    this.slowConsumerLevel4 =
        Counter.builder("websocket.slow.consumer.level4")
            .description("Sessions entering slow-consumer level 4 (>2MB sustained)")
            .register(registry);
    this.slowConsumerDisconnects =
        Counter.builder("websocket.slow.consumer.disconnects")
            .description("Slow-consumer sessions disconnected after sustained level 4")
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

  // --- Subscription filter + auth counter/timer access ---

  /** Record a message that passed the SubscriptionFilter. */
  public void filterMatched() {
    filterMatched.increment();
  }

  /** Record a message that was filtered out by the SubscriptionFilter. */
  public void filterFiltered() {
    filterFiltered.increment();
  }

  /** Record a connection rejected due to per-IP auth failure lockout. */
  public void authLockout() {
    authLockout.increment();
  }

  /**
   * Update the total active subscription count across all sessions.
   *
   * @param count the current total subscription count
   */
  public void updateActiveSubscriptions(final int count) {
    activeSubscriptions.set(count);
  }

  /**
   * Returns the auth latency timer for recording JWT validation durations.
   *
   * @return the auth latency timer
   */
  public Timer authLatency() {
    return authLatency;
  }

  /**
   * Record the duration of a single drain cycle in nanoseconds.
   *
   * @param nanos elapsed nanoseconds from injected {@code NanoClock}
   */
  public void recordDrainCycleNanos(final long nanos) {
    drainCycleLatency.record(nanos, TimeUnit.NANOSECONDS);
  }

  // --- APP-242 counter access ---

  /** Record a {@code WebSocketGapRequest} arrival. */
  public void gapRequestReceived() {
    gapRequestsReceived.increment();
  }

  /** Record a {@code SessionResume} arrival. */
  public void sessionResumeReceived() {
    sessionResumesReceived.increment();
  }

  /**
   * Record a single replay frame sent.
   *
   * @param payloadBytes number of payload bytes in this replayed frame (no wire envelope)
   */
  public void replaySent(final long payloadBytes) {
    replaysSent.increment();
    replayBytesSent.increment(payloadBytes);
  }

  /** Record a command successfully forwarded to the cluster. */
  public void commandDispatched() {
    commandsDispatched.increment();
  }

  /** Record a command rejected (entitlement, format, etc.). */
  public void commandRejected() {
    commandsRejected.increment();
  }

  /** Record a command rejected as a ClOrdID duplicate. */
  public void commandDuplicate() {
    commandsDuplicate.increment();
  }

  /** Record a command throttled due to cluster BACK_PRESSURED after retries. */
  public void commandBackpressured() {
    commandsBackpressured.increment();
  }

  /** Record a CommandAck dropped because the ack back-channel was full. */
  public void commandAckDropped() {
    commandsAckDropped.increment();
  }

  /** Record a dedup map tryLock failure (fail-open path). */
  public void dedupTryLockMiss() {
    dedupTryLockMisses.increment();
  }

  /** Record a slow-consumer level-1 entry. */
  public void slowConsumerLevel1() {
    slowConsumerLevel1.increment();
  }

  /** Record a slow-consumer level-2 entry. */
  public void slowConsumerLevel2() {
    slowConsumerLevel2.increment();
  }

  /** Record a slow-consumer level-3 entry. */
  public void slowConsumerLevel3() {
    slowConsumerLevel3.increment();
  }

  /** Record a slow-consumer level-4 entry. */
  public void slowConsumerLevel4() {
    slowConsumerLevel4.increment();
  }

  /** Record a slow-consumer disconnect (post-level-4 sustained). */
  public void slowConsumerDisconnect() {
    slowConsumerDisconnects.increment();
  }
}
