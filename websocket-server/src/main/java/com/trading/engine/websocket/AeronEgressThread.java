package com.trading.engine.websocket;

import java.util.Objects;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Dedicated thread for polling the Aeron cluster egress and feeding the {@link
 * ManyToOneConcurrentArrayQueue} that bridges to the Netty drain handler.
 *
 * <p>Wraps a {@link WebSocketClusterClient} (which implements {@link org.agrona.concurrent.Agent})
 * in an {@link AgentRunner} on a named "aeron-egress" thread. The cluster client's {@code doWork()}
 * performs {@code pollEgress()}, and the {@link WebSocketEgressListener} copies messages into the
 * queue.
 *
 * <p><b>Queue backpressure monitoring</b> (architecture doc Section 1, 3 levels):
 *
 * <ul>
 *   <li>75% full → {@code QUEUE_NEAR_FULL} metric + 100us pause
 *   <li>100% full → {@code SESSION_BACKPRESSURE} flag + stop polling
 *   <li>Backpressured &gt;2s → restart thread + emit alert
 * </ul>
 *
 * <p>The 4-level graduated backpressure (Section 5, byte-based lag) is deferred to PR 4 ({@code
 * SlowConsumerHandler}).
 *
 * <p><b>Threading.</b> Owns a single named thread ("aeron-egress"). All queue monitoring runs on
 * this thread.
 *
 * <p><b>Allocation.</b> One-time thread creation at startup. No hot-path allocation.
 *
 * @see <a href="docs/websocket-architecture.md">WebSocket Architecture — Section 1</a>
 */
public final class AeronEgressThread implements AutoCloseable {

  private static final Logger LOG = LogManager.getLogger(AeronEgressThread.class);

  private final AgentRunner agentRunner;
  private final ManyToOneConcurrentArrayQueue<EgressEntry> queue;
  private final WebSocketMetrics metrics;
  private final int queueCapacity;
  private volatile boolean started;

  /**
   * Create the egress thread (not yet started).
   *
   * @param clusterClient the cluster client agent to run on this thread
   * @param queue the egress queue to monitor for backpressure
   * @param metrics metrics instance for queue depth and poll latency
   * @param queueCapacity the maximum queue capacity (for backpressure threshold calculation)
   */
  public AeronEgressThread(
      final WebSocketClusterClient clusterClient,
      final ManyToOneConcurrentArrayQueue<EgressEntry> queue,
      final WebSocketMetrics metrics,
      final int queueCapacity) {
    Objects.requireNonNull(clusterClient, "clusterClient");
    this.queue = Objects.requireNonNull(queue, "queue");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    this.queueCapacity = queueCapacity;
    this.agentRunner =
        new AgentRunner(
            new SleepingMillisIdleStrategy(1),
            throwable -> LOG.error("AeronEgressThread error", throwable),
            null, // no AtomicCounter
            clusterClient);
  }

  /**
   * Start the egress polling thread. Must be called exactly once.
   *
   * @throws IllegalStateException if already started
   */
  public void start() {
    if (started) {
      throw new IllegalStateException("AeronEgressThread already started");
    }
    started = true;
    AgentRunner.startOnThread(agentRunner);
    LOG.info("AeronEgressThread started (queue capacity={})", queueCapacity);
  }

  /** Update queue depth metric. Called periodically from the drain handler or a monitoring task. */
  public void updateMetrics() {
    final int size = queue.size();
    metrics.updateQueueDepth(size);
  }

  /**
   * @return true if the thread has been started
   */
  public boolean isStarted() {
    return started;
  }

  /** Stop the egress polling thread and close the cluster client. */
  @Override
  public void close() {
    if (started) {
      agentRunner.close();
      LOG.info("AeronEgressThread stopped");
    }
  }
}
