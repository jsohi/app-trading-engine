package com.trading.engine.websocket;

import static io.aeron.Publication.NOT_CONNECTED;

import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SystemNanoClock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Agent-based Aeron Cluster client for the WebSocket server. Provides a single-threaded duty cycle
 * that handles egress polling, keep-alive heartbeats, and automatic reconnection with exponential
 * backoff.
 *
 * <p>Mirrors the gateway's {@code ClusterClient} with key differences:
 *
 * <ul>
 *   <li>Uses {@link EgressListener} (not {@code ControlledEgressListener}) — never ABORTs. Per-
 *       client backpressure is handled by Netty, not Aeron.
 *   <li>Calls {@link AeronCluster#pollEgress()} (not {@code controlledPollEgress()}).
 *   <li>Unlimited reconnection attempts (architecture doc Section 5 — WebSocket server must always
 *       attempt to reconnect).
 *   <li>No {@code InFlightTracker} — WebSocket commands use {@code CommandAck} instead of FIX
 *       correlation.
 *   <li>Uses Log4j2 (infra module, not GFLog).
 * </ul>
 *
 * <p><b>Duty cycle.</b> {@link #doWork()} performs:
 *
 * <ol>
 *   <li>If reconnecting, wait until backoff deadline, then attempt reconnection.
 *   <li>Poll cluster egress via {@code pollEgress()}.
 *   <li>Check if egress listener flagged reconnection needed (session ERROR/CLOSED).
 *   <li>Send keep-alive heartbeat if interval has elapsed.
 * </ol>
 *
 * <p><b>Allocation.</b> Zero allocation after construction and initial connection. Reconnection
 * creates a new {@link AeronCluster} context per attempt.
 *
 * <p><b>Threading.</b> Not thread-safe — single-threaded AeronEgressThread only. The {@link
 * #isConnected()} method is safe for cross-thread readiness polling (volatile state).
 *
 * @see <a href="docs/websocket-architecture.md">WebSocket Architecture — Section 1</a>
 */
public final class WebSocketClusterClient implements Agent, AutoCloseable {

  private static final Logger LOG = LogManager.getLogger(WebSocketClusterClient.class);

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
  private final EgressListener egressListener;
  private final long messageTimeoutNs;
  private final long keepAliveIntervalNs;
  private final long reconnectBaseDelayNs;
  private final long reconnectMaxDelayNs;
  private final ErrorHandler errorHandler;
  private final NanoClock nanoClock;

  // --- Mutable state ---
  private AeronCluster aeronCluster;
  private volatile State state = State.DISCONNECTED;
  private long lastKeepAliveNs;
  private long reconnectDeadlineNs;
  private int reconnectAttempts;

  private WebSocketClusterClient(final Builder builder) {
    this.aeronDirectoryName =
        Objects.requireNonNull(builder.aeronDirectoryName, "aeronDirectoryName");
    this.ingressEndpoints = Objects.requireNonNull(builder.ingressEndpoints, "ingressEndpoints");
    this.egressChannel = Objects.requireNonNull(builder.egressChannel, "egressChannel");
    this.egressListener = Objects.requireNonNull(builder.egressListener, "egressListener");
    this.messageTimeoutNs = builder.messageTimeoutNs;
    this.keepAliveIntervalNs = builder.keepAliveIntervalNs;
    this.reconnectBaseDelayNs = builder.reconnectBaseDelayNs;
    this.reconnectMaxDelayNs = builder.reconnectMaxDelayNs;
    this.errorHandler = Objects.requireNonNull(builder.errorHandler, "errorHandler");
    this.nanoClock = builder.nanoClock;
  }

  /**
   * Create a new builder for configuring a {@link WebSocketClusterClient}.
   *
   * @return a new builder
   */
  public static Builder builder() {
    return new Builder();
  }

  // ===========================================================================
  // Agent interface
  // ===========================================================================

  /**
   * @return the agent role name for thread naming
   */
  @Override
  public String roleName() {
    return "ws-cluster-client";
  }

  /** Connect to the cluster on agent start. */
  @Override
  public void onStart() {
    connect();
  }

  /**
   * Single-threaded duty cycle: reconnect backoff → poll egress → reconnect check → keep-alive.
   *
   * @return the amount of work done (for idle strategy tuning)
   */
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

    // 2. Poll egress (non-controlled — never ABORTs; backpressure via Netty, not Aeron).
    workCount += aeronCluster.pollEgress();

    // 3. Send keep-alive if interval elapsed.
    final long nowNs = nanoClock.nanoTime();
    if (nowNs - lastKeepAliveNs >= keepAliveIntervalNs) {
      if (aeronCluster.sendKeepAlive()) {
        lastKeepAliveNs = nowNs;
        workCount++;
      }
    }

    return workCount;
  }

  /** Close the client on agent shutdown. */
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
   * @return stream position on success, or a negative error code
   */
  public long offer(final DirectBuffer buffer, final int offset, final int length) {
    if (state != State.CONNECTED) {
      return NOT_CONNECTED;
    }
    return aeronCluster.offer(buffer, offset, length);
  }

  // ===========================================================================
  // Accessors
  // ===========================================================================

  /**
   * Check if connected to the cluster. Safe for cross-thread readiness polling.
   *
   * @return true if connected
   */
  public boolean isConnected() {
    return state == State.CONNECTED;
  }

  /**
   * Check if the client has been permanently closed.
   *
   * @return true if closed
   */
  public boolean isClosed() {
    return state == State.CLOSED;
  }

  /**
   * Returns the current connection state. Package-private for testing.
   *
   * @return the current state
   */
  State state() {
    return state;
  }

  /**
   * Signal that the egress listener detected a session error requiring reconnection. Called from
   * the egress listener callback during {@code pollEgress()} — same thread as {@code doWork()}.
   */
  public void signalReconnectNeeded() {
    if (state == State.CONNECTED) {
      LOG.info("Egress listener signalled reconnect needed");
      initiateReconnect();
    }
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
      LOG.info("WebSocketClusterClient closed");
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
            .ingressChannel("aeron:udp")
            .ingressEndpoints(ingressEndpoints)
            .egressChannel(egressChannel)
            .egressListener(egressListener) // NOT controlledEgressListener
            .messageTimeoutNs(messageTimeoutNs)
            .errorHandler(errorHandler)
            .ownsAeronClient(true);
    try {
      aeronCluster = AeronCluster.connect(ctx);
      state = State.CONNECTED;
      reconnectAttempts = 0;
      lastKeepAliveNs = nanoClock.nanoTime();

      LOG.info(
          "Connected to cluster: sessionId={} leader={}",
          aeronCluster.clusterSessionId(),
          aeronCluster.leaderMemberId());
    } catch (final Exception ex) {
      try {
        ctx.close();
      } catch (final Exception closeEx) {
        errorHandler.onError(closeEx);
      }
      LOG.warn("Cluster connection failed: {}", ex.getMessage());
      scheduleReconnect();
    }
  }

  private void initiateReconnect() {
    closeAeronCluster();
    scheduleReconnect();
  }

  private void scheduleReconnect() {
    // Unlimited reconnection (architecture doc Section 5 — WebSocket server must always reconnect).
    // Exponential backoff: base * 2^attempt, capped at max. Safe shift to avoid overflow.
    final int safeShift =
        Math.min(reconnectAttempts, Long.numberOfLeadingZeros(reconnectBaseDelayNs) - 1);
    final long delayNs =
        safeShift < 0
            ? reconnectMaxDelayNs
            : Math.min(reconnectBaseDelayNs << safeShift, reconnectMaxDelayNs);
    reconnectDeadlineNs = nanoClock.nanoTime() + delayNs;
    reconnectAttempts++;
    state = State.RECONNECTING;

    LOG.info(
        "Scheduling reconnect attempt {} in {}ms",
        reconnectAttempts,
        TimeUnit.NANOSECONDS.toMillis(delayNs));
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

  // ===========================================================================
  // Builder
  // ===========================================================================

  /**
   * Fluent builder for {@link WebSocketClusterClient}. All required fields must be set before
   * calling {@link #build()}.
   */
  public static final class Builder {

    String aeronDirectoryName;
    String ingressEndpoints;
    String egressChannel = "aeron:udp";
    EgressListener egressListener;
    long messageTimeoutNs = TimeUnit.SECONDS.toNanos(5);
    long keepAliveIntervalNs = TimeUnit.SECONDS.toNanos(1);
    long reconnectBaseDelayNs = TimeUnit.MILLISECONDS.toNanos(100);
    long reconnectMaxDelayNs = TimeUnit.SECONDS.toNanos(10);
    ErrorHandler errorHandler;
    NanoClock nanoClock = SystemNanoClock.INSTANCE;

    Builder() {}

    /**
     * @param aeronDirectoryName Aeron CnC directory for the shared Media Driver; required
     * @return this builder
     */
    public Builder aeronDirectoryName(final String aeronDirectoryName) {
      this.aeronDirectoryName = aeronDirectoryName;
      return this;
    }

    /**
     * @param ingressEndpoints comma-separated cluster ingress endpoints; required
     * @return this builder
     */
    public Builder ingressEndpoints(final String ingressEndpoints) {
      this.ingressEndpoints = ingressEndpoints;
      return this;
    }

    /**
     * @param egressChannel Aeron channel for egress (default: "aeron:udp")
     * @return this builder
     */
    public Builder egressChannel(final String egressChannel) {
      this.egressChannel = egressChannel;
      return this;
    }

    /**
     * @param egressListener egress listener for cluster responses; required
     * @return this builder
     */
    public Builder egressListener(final EgressListener egressListener) {
      this.egressListener = egressListener;
      return this;
    }

    /**
     * @param messageTimeoutNs cluster session message timeout in nanoseconds (default: 5s)
     * @return this builder
     */
    public Builder messageTimeoutNs(final long messageTimeoutNs) {
      this.messageTimeoutNs = messageTimeoutNs;
      return this;
    }

    /**
     * @param keepAliveIntervalNs interval between keep-alive heartbeats in nanoseconds (default:
     *     1s)
     * @return this builder
     */
    public Builder keepAliveIntervalNs(final long keepAliveIntervalNs) {
      this.keepAliveIntervalNs = keepAliveIntervalNs;
      return this;
    }

    /**
     * @param reconnectBaseDelayNs base delay for exponential backoff in nanoseconds (default:
     *     100ms)
     * @return this builder
     */
    public Builder reconnectBaseDelayNs(final long reconnectBaseDelayNs) {
      this.reconnectBaseDelayNs = reconnectBaseDelayNs;
      return this;
    }

    /**
     * @param reconnectMaxDelayNs maximum delay for exponential backoff in nanoseconds (default:
     *     10s)
     * @return this builder
     */
    public Builder reconnectMaxDelayNs(final long reconnectMaxDelayNs) {
      this.reconnectMaxDelayNs = reconnectMaxDelayNs;
      return this;
    }

    /**
     * @param errorHandler handler for non-recoverable errors; required
     * @return this builder
     */
    public Builder errorHandler(final ErrorHandler errorHandler) {
      this.errorHandler = errorHandler;
      return this;
    }

    /**
     * @param nanoClock monotonic nanosecond clock for testability (default: SystemNanoClock)
     * @return this builder
     */
    public Builder nanoClock(final NanoClock nanoClock) {
      this.nanoClock = nanoClock;
      return this;
    }

    /**
     * Build the {@link WebSocketClusterClient}. Validates required fields and timing parameters.
     *
     * @return a configured cluster client
     * @throws NullPointerException if any required field is null
     * @throws IllegalArgumentException if timing parameters are invalid
     */
    public WebSocketClusterClient build() {
      if (reconnectBaseDelayNs <= 0) {
        throw new IllegalArgumentException(
            "reconnectBaseDelayNs must be > 0, got: " + reconnectBaseDelayNs);
      }
      if (reconnectMaxDelayNs < reconnectBaseDelayNs) {
        throw new IllegalArgumentException(
            "reconnectMaxDelayNs ("
                + reconnectMaxDelayNs
                + ") must be >= reconnectBaseDelayNs ("
                + reconnectBaseDelayNs
                + ")");
      }
      return new WebSocketClusterClient(this);
    }
  }
}
