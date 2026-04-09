package com.trading.engine.gateway;

import static io.aeron.Publication.NOT_CONNECTED;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import io.aeron.cluster.client.AeronCluster;
import java.util.concurrent.TimeUnit;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SystemNanoClock;

/**
 * Agent-based Aeron Cluster client for the trading gateway. Provides a single-threaded duty cycle
 * that handles egress polling, keep-alive heartbeats, in-flight timeout detection, and automatic
 * reconnection with exponential backoff.
 *
 * <p><b>Duty cycle.</b> {@link #doWork()} performs the following on each iteration:
 *
 * <ol>
 *   <li>If in {@link State#RECONNECTING}, wait until the backoff deadline, then attempt
 *       reconnection.
 *   <li>If in {@link State#CONNECTED}, poll cluster egress via {@code controlledPollEgress()}.
 *   <li>Send a keep-alive heartbeat if the keep-alive interval has elapsed.
 *   <li>Check in-flight requests for timeouts and invoke the timeout callback.
 *   <li>Check if the egress listener has flagged a reconnection needed (session ERROR/CLOSED).
 * </ol>
 *
 * <p><b>Reconnection.</b> On connection failure or session error/close, the client enters {@link
 * State#RECONNECTING} with exponential backoff (base delay doubled on each attempt, capped at max
 * delay). All in-flight requests are reset as stale. After {@code maxReconnectAttempts}, the client
 * transitions to {@link State#CLOSED} and reports a fatal error via the {@link ErrorHandler}.
 *
 * <p><b>Composition.</b> This client implements {@link Agent} and is designed to be composed into a
 * gateway {@link org.agrona.concurrent.CompositeAgent} alongside other duty-cycle agents (e.g.,
 * Artio library agent).
 *
 * <p><b>Allocation.</b> Zero allocation after construction and initial connection. Reconnection
 * creates a new {@link AeronCluster} context per attempt; all hot-path operations ({@code offer},
 * {@code doWork}) are zero-allocation.
 *
 * <p><b>Threading.</b> Not thread-safe — single-threaded gateway duty-cycle thread only.
 */
public final class ClusterClient implements Agent, AutoCloseable {

  private static final Log LOG = LogFactory.getLog(ClusterClient.class);

  /** Connection state machine: DISCONNECTED → CONNECTED ↔ RECONNECTING → CLOSED. */
  enum State {
    DISCONNECTED,
    CONNECTED,
    RECONNECTING,
    CLOSED
  }

  // --- Configuration (immutable after construction) ---
  private final String aeronDirectoryName;
  private final String ingressEndpoints;
  private final String egressChannel;
  private final ClusterEgressListener egressListener;
  private final long messageTimeoutNs;
  private final long keepAliveIntervalNs;
  private final long timeoutCheckIntervalNs;
  private final long reconnectBaseDelayNs;
  private final long reconnectMaxDelayNs;
  private final int maxReconnectAttempts;
  private final ErrorHandler errorHandler;
  private final NanoClock nanoClock;
  private final InFlightTracker inFlightTracker;

  // --- Pre-allocated callback (zero-alloc on doWork hot path) ---
  private final InFlightTracker.TimeoutCallback timeoutCallback = this::onRequestTimeout;

  // --- Mutable state ---
  private AeronCluster aeronCluster;
  private State state = State.DISCONNECTED;
  private long lastKeepAliveNs;
  private long lastTimeoutCheckNs;
  private long reconnectDeadlineNs;
  private int reconnectAttempts;

  private ClusterClient(final Builder builder) {
    this.aeronDirectoryName = requireNonNull(builder.aeronDirectoryName, "aeronDirectoryName");
    this.ingressEndpoints = requireNonNull(builder.ingressEndpoints, "ingressEndpoints");
    this.egressChannel = requireNonNull(builder.egressChannel, "egressChannel");
    this.egressListener = requireNonNull(builder.egressListener, "egressListener");
    this.messageTimeoutNs = builder.messageTimeoutNs;
    this.keepAliveIntervalNs = builder.keepAliveIntervalNs;
    this.timeoutCheckIntervalNs = builder.timeoutCheckIntervalNs;
    this.reconnectBaseDelayNs = builder.reconnectBaseDelayNs;
    this.reconnectMaxDelayNs = builder.reconnectMaxDelayNs;
    this.maxReconnectAttempts = builder.maxReconnectAttempts;
    this.errorHandler = requireNonNull(builder.errorHandler, "errorHandler");
    this.nanoClock = builder.nanoClock;
    this.inFlightTracker = requireNonNull(builder.inFlightTracker, "inFlightTracker");
  }

  /** Create a new builder for configuring a {@link ClusterClient}. */
  public static Builder builder() {
    return new Builder();
  }

  // ===========================================================================
  // Agent interface
  // ===========================================================================

  @Override
  public String roleName() {
    return "cluster-client";
  }

  @Override
  public void onStart() {
    connect();
  }

  @Override
  public int doWork() {
    if (state == State.CLOSED) {
      return 0;
    }

    // 1. Handle reconnection backoff.
    if (state == State.RECONNECTING) {
      if (reconnectDeadlineNs - nanoClock.nanoTime() > 0) {
        return 0;
      }
      connect();
      if (state != State.CONNECTED) {
        return 0;
      }
    }

    if (state != State.CONNECTED) {
      return 0;
    }

    int workCount = 0;

    // 2. Poll egress (controlled — supports Action.ABORT for backpressure).
    workCount += aeronCluster.controlledPollEgress();

    // 3. Check if egress listener flagged a reconnection needed.
    if (egressListener.isReconnectNeeded()) {
      egressListener.clearReconnectNeeded();
      LOG.info().append("Egress listener signalled reconnect needed").commit();
      initiateReconnect();
      return workCount;
    }

    // 4. Send keep-alive if interval elapsed.
    final long nowNs = nanoClock.nanoTime();
    if (nowNs - lastKeepAliveNs >= keepAliveIntervalNs) {
      if (aeronCluster.sendKeepAlive()) {
        lastKeepAliveNs = nowNs;
        workCount++;
      }
    }

    // 5. Check in-flight request timeouts (throttled to avoid scanning the full map every
    // doWork iteration — the scan is O(n) in the number of in-flight entries).
    if (nowNs - lastTimeoutCheckNs >= timeoutCheckIntervalNs) {
      workCount += inFlightTracker.checkTimeouts(nowNs, timeoutCallback);
      lastTimeoutCheckNs = nowNs;
    }

    return workCount;
  }

  @Override
  public void onClose() {
    close();
  }

  // ===========================================================================
  // Command sending
  // ===========================================================================

  /**
   * Send an SBE-encoded command to the cluster.
   *
   * @param buffer contains the complete SBE message (header + body)
   * @param offset offset of the SBE header within the buffer
   * @param length total message length
   * @return stream position on success ({@code >= 0}), or a negative value on backpressure / error.
   *     Retryable: {@link io.aeron.Publication#BACK_PRESSURED}, {@link
   *     io.aeron.Publication#ADMIN_ACTION}. Non-retryable: {@link
   *     io.aeron.Publication#NOT_CONNECTED}, {@link io.aeron.Publication#CLOSED}, {@link
   *     io.aeron.Publication#MAX_POSITION_EXCEEDED}.
   */
  public long offer(final DirectBuffer buffer, final int offset, final int length) {
    if (state != State.CONNECTED) {
      return NOT_CONNECTED;
    }
    return aeronCluster.offer(buffer, offset, length);
  }

  /**
   * Send a command and track it as in-flight for timeout detection. On a successful offer, the
   * ClOrdID is recorded in the {@link InFlightTracker}; on backpressure or error, nothing is
   * tracked.
   *
   * @param buffer SBE message buffer
   * @param offset SBE header offset
   * @param length total message length
   * @param clOrdId null-padding-trimmed ClOrdID bytes
   * @param clOrdIdOffset start offset within {@code clOrdId}
   * @param clOrdIdLength significant byte count
   * @return same as {@link #offer(DirectBuffer, int, int)}
   */
  public long offerTracked(
      final DirectBuffer buffer,
      final int offset,
      final int length,
      final byte[] clOrdId,
      final int clOrdIdOffset,
      final int clOrdIdLength) {
    final long result = offer(buffer, offset, length);
    if (result >= 0) {
      inFlightTracker.onCommandSent(clOrdId, clOrdIdOffset, clOrdIdLength, nanoClock.nanoTime());
    }
    return result;
  }

  // ===========================================================================
  // Accessors
  // ===========================================================================

  /** Returns the cluster session ID, or {@code -1} if not connected. */
  public long clusterSessionId() {
    return state == State.CONNECTED ? aeronCluster.clusterSessionId() : -1L;
  }

  /** Returns the current cluster leader member ID, or {@code -1} if not connected. */
  public int leaderMemberId() {
    return state == State.CONNECTED ? aeronCluster.leaderMemberId() : -1;
  }

  /** Returns {@code true} if the client is connected to the cluster. */
  public boolean isConnected() {
    return state == State.CONNECTED;
  }

  /** Returns {@code true} if the client has been permanently closed. */
  public boolean isClosed() {
    return state == State.CLOSED;
  }

  /** Returns the current connection state. Package-private for testing. */
  State state() {
    return state;
  }

  // ===========================================================================
  // Lifecycle
  // ===========================================================================

  /** Close the client and release all resources. Idempotent. */
  @Override
  public void close() {
    if (state != State.CLOSED) {
      state = State.CLOSED;
      closeAeronCluster();
      inFlightTracker.reset();
      LOG.info().append("ClusterClient closed").commit();
    }
  }

  // ===========================================================================
  // Connection management (package-private for testing)
  // ===========================================================================

  void connect() {
    if (state == State.CLOSED) {
      return;
    }
    final var ctx =
        new AeronCluster.Context()
            .aeronDirectoryName(aeronDirectoryName)
            .ingressEndpoints(ingressEndpoints)
            .egressChannel(egressChannel)
            .controlledEgressListener(egressListener)
            .messageTimeoutNs(messageTimeoutNs)
            .errorHandler(errorHandler)
            .ownsAeronClient(false);
    try {
      aeronCluster = AeronCluster.connect(ctx);
      state = State.CONNECTED;
      reconnectAttempts = 0;
      lastKeepAliveNs = nanoClock.nanoTime();
      lastTimeoutCheckNs = lastKeepAliveNs;

      LOG.info()
          .append("Connected to cluster: sessionId=")
          .append(aeronCluster.clusterSessionId())
          .append(" leader=")
          .append(aeronCluster.leaderMemberId())
          .commit();
    } catch (final Exception ex) {
      // Close the context to release any partially-constructed resources (subscriptions,
      // publications) that AeronCluster.connect may have allocated before the failure.
      try {
        ctx.close();
      } catch (final Exception closeEx) {
        errorHandler.onError(closeEx);
      }
      LOG.warn().append("Cluster connection failed: ").append(ex.getMessage()).commit();
      scheduleReconnect();
    }
  }

  private void initiateReconnect() {
    closeAeronCluster();
    scheduleReconnect();
  }

  private void scheduleReconnect() {
    if (reconnectAttempts >= maxReconnectAttempts) {
      LOG.info()
          .append("Max reconnect attempts (")
          .append(maxReconnectAttempts)
          .append(") exceeded — closing client")
          .commit();
      state = State.CLOSED;
      inFlightTracker.reset();
      errorHandler.onError(
          new IllegalStateException(
              "ClusterClient: max reconnect attempts exceeded after " + maxReconnectAttempts));
      return;
    }

    // Exponential backoff: base * 2^attempt, capped at max. The shift count is limited to the
    // number of leading zeros in the base value so the result never overflows (stays positive).
    // For any shift beyond that limit, the uncapped value exceeds Long.MAX_VALUE, so we use
    // reconnectMaxDelayNs directly.
    final int safeShift =
        Math.min(reconnectAttempts, Long.numberOfLeadingZeros(reconnectBaseDelayNs) - 1);
    final long delayNs =
        safeShift < 0
            ? reconnectMaxDelayNs
            : Math.min(reconnectBaseDelayNs << safeShift, reconnectMaxDelayNs);
    reconnectDeadlineNs = nanoClock.nanoTime() + delayNs;
    reconnectAttempts++;
    state = State.RECONNECTING;
    inFlightTracker.reset();

    LOG.info()
        .append("Scheduling reconnect attempt ")
        .append(reconnectAttempts)
        .append("/")
        .append(maxReconnectAttempts)
        .append(" in ")
        .append(TimeUnit.NANOSECONDS.toMillis(delayNs))
        .append("ms")
        .commit();
  }

  private void closeAeronCluster() {
    if (aeronCluster != null) {
      try {
        aeronCluster.close();
      } catch (final Exception ex) {
        errorHandler.onError(ex);
      } finally {
        aeronCluster = null;
      }
    }
  }

  private void onRequestTimeout(final long clOrdIdHash, final long sentTimestampNs) {
    final long ageMs = TimeUnit.NANOSECONDS.toMillis(nanoClock.nanoTime() - sentTimestampNs);
    LOG.info()
        .append("In-flight request timed out: hash=")
        .append(clOrdIdHash)
        .append(" ageMs=")
        .append(ageMs)
        .commit();
  }

  // ===========================================================================
  // Internal
  // ===========================================================================

  private static <T> T requireNonNull(final T value, final String name) {
    if (value == null) {
      throw new NullPointerException(name + " must not be null");
    }
    return value;
  }

  // ===========================================================================
  // Builder
  // ===========================================================================

  /**
   * Fluent builder for {@link ClusterClient}. All required fields must be set before calling {@link
   * #build()}.
   */
  public static final class Builder {

    String aeronDirectoryName;
    String ingressEndpoints;
    String egressChannel = "aeron:udp";
    ClusterEgressListener egressListener;
    long messageTimeoutNs = TimeUnit.SECONDS.toNanos(5);
    long keepAliveIntervalNs = TimeUnit.SECONDS.toNanos(1);
    long timeoutCheckIntervalNs = TimeUnit.MILLISECONDS.toNanos(100);
    long reconnectBaseDelayNs = TimeUnit.MILLISECONDS.toNanos(100);
    long reconnectMaxDelayNs = TimeUnit.SECONDS.toNanos(10);
    int maxReconnectAttempts = 10;
    ErrorHandler errorHandler;
    NanoClock nanoClock = SystemNanoClock.INSTANCE;
    InFlightTracker inFlightTracker;

    Builder() {}

    /** Aeron CnC directory for the shared external Media Driver. Required. */
    public Builder aeronDirectoryName(final String aeronDirectoryName) {
      this.aeronDirectoryName = aeronDirectoryName;
      return this;
    }

    /** Comma-separated cluster ingress endpoints (e.g., "0=localhost:20110,..."). Required. */
    public Builder ingressEndpoints(final String ingressEndpoints) {
      this.ingressEndpoints = ingressEndpoints;
      return this;
    }

    /** Aeron channel for egress (default: "aeron:udp"). */
    public Builder egressChannel(final String egressChannel) {
      this.egressChannel = egressChannel;
      return this;
    }

    /** Controlled egress listener for cluster responses. Required. */
    public Builder egressListener(final ClusterEgressListener egressListener) {
      this.egressListener = egressListener;
      return this;
    }

    /** Cluster session message timeout in nanoseconds (default: 5 seconds). */
    public Builder messageTimeoutNs(final long messageTimeoutNs) {
      this.messageTimeoutNs = messageTimeoutNs;
      return this;
    }

    /** Interval between keep-alive heartbeats in nanoseconds (default: 1 second). */
    public Builder keepAliveIntervalNs(final long keepAliveIntervalNs) {
      this.keepAliveIntervalNs = keepAliveIntervalNs;
      return this;
    }

    /** Interval between in-flight timeout scans in nanoseconds (default: 100ms). */
    public Builder timeoutCheckIntervalNs(final long timeoutCheckIntervalNs) {
      this.timeoutCheckIntervalNs = timeoutCheckIntervalNs;
      return this;
    }

    /** Base delay for reconnection backoff in nanoseconds (default: 100ms). */
    public Builder reconnectBaseDelayNs(final long reconnectBaseDelayNs) {
      this.reconnectBaseDelayNs = reconnectBaseDelayNs;
      return this;
    }

    /** Maximum delay for reconnection backoff in nanoseconds (default: 10 seconds). */
    public Builder reconnectMaxDelayNs(final long reconnectMaxDelayNs) {
      this.reconnectMaxDelayNs = reconnectMaxDelayNs;
      return this;
    }

    /** Maximum reconnection attempts before fatal close (default: 10). */
    public Builder maxReconnectAttempts(final int maxReconnectAttempts) {
      this.maxReconnectAttempts = maxReconnectAttempts;
      return this;
    }

    /** Error handler for non-recoverable errors. Required. */
    public Builder errorHandler(final ErrorHandler errorHandler) {
      this.errorHandler = errorHandler;
      return this;
    }

    /** Monotonic nanosecond clock for testability (default: {@link SystemNanoClock}). */
    public Builder nanoClock(final NanoClock nanoClock) {
      this.nanoClock = nanoClock;
      return this;
    }

    /** In-flight request tracker (shared with the egress listener). Required. */
    public Builder inFlightTracker(final InFlightTracker inFlightTracker) {
      this.inFlightTracker = inFlightTracker;
      return this;
    }

    /** Build the {@link ClusterClient}. Validates that all required fields are set. */
    public ClusterClient build() {
      return new ClusterClient(this);
    }
  }
}
