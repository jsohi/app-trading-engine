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

  /** C.1 — count of mid-session JWT expirations that triggered a 4401 close. */
  private final Counter authSessionExpired;

  /**
   * C.1 — count of {@code AuthExpiringSoon} warning frames emitted to clients (one per session per
   * token; the latch prevents intra-window spam).
   */
  private final Counter authExpiringSoonEmitted;

  /**
   * C.2 — JWKS refresh failures for HTTP 5xx responses from the JWKS endpoint.
   *
   * <p>Incremented by {@link JwtValidator} when a JWKS fetch fails with an HTTP server error during
   * the key-refresh path triggered by a signature-verification retry. Tagged with {@code
   * reason="5xx"} to distinguish transient server-side failures from network errors or
   * configuration problems. The tagged counter pattern allows Prometheus to aggregate or filter by
   * failure class without a combinatorial counter explosion.
   */
  private final Counter jwksRefreshFailure5xx;

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

  // --- Phase 3 market-data + egress counters ---
  private final Counter marketDataDropped;
  private final Counter egressDroppedChannelNotWritable;
  private final Counter marketDataFeedStateTransitions;
  private final Counter marketDataSnapshotDeduped;
  private final Counter marketDataSnapshotTimeout;
  private final Counter symbolEntitlementDenied;
  private final Counter dispatcherMalformed;
  private final Counter dispatcherSymbolUnknown;
  private final Counter egressDroppedStaleEpoch;

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

    this.authSessionExpired =
        Counter.builder("websocket.auth.session.expired")
            .description(
                "Mid-session JWT expiries that closed the channel with 4401 AuthExpired "
                    + "(C.1 — JwtExpirySweeper)")
            .register(registry);

    this.authExpiringSoonEmitted =
        Counter.builder("websocket.auth.expiring_soon.emitted")
            .description(
                "Soft-expiry AuthExpiringSoon warning frames emitted to clients ahead of "
                    + "the hard expiry (C.1 — JwtExpirySweeper). One per session per token.")
            .register(registry);

    this.jwksRefreshFailure5xx =
        Counter.builder("jwks.refresh.failure")
            .tag("reason", "5xx")
            .description(
                "JWKS refresh failures caused by HTTP 5xx responses from the JWKS endpoint "
                    + "(C.2 — JwtValidator key-rotation retry path). Tagged reason=\"5xx\".")
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

    // --- Phase 3 market-data + egress counters ---
    this.marketDataDropped =
        Counter.builder("websocket.marketdata.dropped")
            .description(
                "Market-data fragments dropped at ingest (unknown template, pool exhaustion, "
                    + "oversize, or queue full)")
            .register(registry);
    this.egressDroppedChannelNotWritable =
        Counter.builder("websocket.egress.dropped.channel-not-writable")
            .description("Egress frames dropped because the Netty channel reported !isWritable()")
            .register(registry);
    this.marketDataFeedStateTransitions =
        Counter.builder("websocket.marketdata.feed.state.transitions")
            .description(
                "MarketDataFeedStateChange transitions emitted by the liveness tracker "
                    + "(LIVE/QUIET/STALE)")
            .register(registry);
    this.marketDataSnapshotDeduped =
        Counter.builder("websocket.marketdata.snapshot.deduped")
            .description(
                "Snapshot requests deduplicated within the publisher drain cadence window "
                    + "(token refunded)")
            .register(registry);
    this.marketDataSnapshotTimeout =
        Counter.builder("websocket.marketdata.snapshot.timeout")
            .description(
                "Snapshot requests whose publisher response was not observed within the "
                    + "snapshot-timeout budget (token NOT refunded)")
            .register(registry);
    this.symbolEntitlementDenied =
        Counter.builder("websocket.subscription.entitlement.denied")
            .description(
                "Subscription matches denied by the per-account symbol entitlement guard "
                    + "(filter rejected after symbol-bit match)")
            .register(registry);
    this.dispatcherMalformed =
        Counter.builder("websocket.dispatcher.malformed")
            .description(
                "Frames rejected as malformed at the dispatcher admission pipeline (RFC 6455 "
                    + "close 1003 — bad header, bad length, bad symbol encoding)")
            .register(registry);
    this.dispatcherSymbolUnknown =
        Counter.builder("websocket.dispatcher.symbol.unknown")
            .description(
                "Snapshot requests for symbols not present in SymbolEntitlementMap (soft "
                    + "WebSocketError 404 — session preserved)")
            .register(registry);
    this.egressDroppedStaleEpoch =
        Counter.builder("websocket.egress.dropped.stale-epoch")
            .description(
                "Egress entries dropped because their captured sessionEpoch no longer matches "
                    + "the session's current epoch (post-resume race guard)")
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

  /** C.1 — Record a mid-session JWT expiry that triggered a 4401 close. */
  public void authSessionExpired() {
    authSessionExpired.increment();
  }

  /** C.1 — Record an emitted AuthExpiringSoon warning frame. */
  public void authExpiringSoonEmitted() {
    authExpiringSoonEmitted.increment();
  }

  /**
   * C.2 — Record a JWKS refresh failure caused by an HTTP 5xx response from the JWKS endpoint.
   *
   * <p>Called by {@link JwtValidator} when a JWKS fetch during the signature-verification retry
   * path receives an HTTP server error (5xx). Increments the {@code jwks.refresh.failure} counter
   * tagged with {@code reason="5xx"}.
   */
  public void jwksRefreshFailure5xx() {
    jwksRefreshFailure5xx.increment();
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

  // --- Phase 3 market-data + egress counter accessors ---

  /**
   * Record a market-data fragment drop at the ingest path (unknown template, pool exhaustion,
   * oversize fragment, or queue full).
   */
  public void marketDataDropped() {
    marketDataDropped.increment();
  }

  /** Record an egress frame drop because the Netty channel reported {@code !isWritable()}. */
  public void egressDroppedChannelNotWritable() {
    egressDroppedChannelNotWritable.increment();
  }

  /**
   * Record a market-data feed-state transition emission (LIVE/QUIET/STALE). Called by the {@link
   * MarketDataSubscriptionLivenessTracker}'s transition callback after the template-57 frame has
   * been enqueued.
   */
  public void marketDataFeedStateTransition() {
    marketDataFeedStateTransitions.increment();
  }

  /**
   * Record a snapshot request deduplicated within the publisher drain cadence window. The
   * dispatcher refunds the token; the publisher does NOT receive a duplicate stream-205 publish.
   */
  public void marketDataSnapshotDeduped() {
    marketDataSnapshotDeduped.increment();
  }

  /**
   * Record a snapshot request whose publisher response was not observed within the snapshot-timeout
   * budget. Per the dispatcher contract the token is NOT refunded on timeout (the stream-205
   * publish already consumed a publisher slot).
   */
  public void marketDataSnapshotTimeout() {
    marketDataSnapshotTimeout.increment();
  }

  /** Record a subscription match denied by the per-account symbol entitlement guard. */
  public void symbolEntitlementDenied() {
    symbolEntitlementDenied.increment();
  }

  /**
   * Record a frame rejected as malformed by the dispatcher's admission pipeline (RFC 6455 close
   * 1003 — bad SBE header, bad length, or bad symbol encoding).
   */
  public void dispatcherMalformed() {
    dispatcherMalformed.increment();
  }

  /**
   * Record a snapshot request for a symbol not present in the {@link SymbolEntitlementMap}. The
   * dispatcher emits a {@code WebSocketError(code=404 SymbolUnknown)} and preserves the session.
   */
  public void dispatcherSymbolUnknown() {
    dispatcherSymbolUnknown.increment();
  }

  /**
   * Record an egress entry dropped because its captured {@code sessionEpoch} no longer matches the
   * session's current epoch (post-resume race guard).
   */
  public void egressDroppedStaleEpoch() {
    egressDroppedStaleEpoch.increment();
  }
}
