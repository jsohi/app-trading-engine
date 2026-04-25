package com.trading.engine.websocket;

import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.logbuffer.Header;
import java.util.Objects;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Aeron cluster egress listener that copies incoming SBE messages into a lock-free queue for
 * consumption by the Netty drain handler.
 *
 * <p>This listener runs on the AeronEgressThread and writes to the {@link
 * ManyToOneConcurrentArrayQueue}. The Netty event loop thread reads from the queue. The queue is
 * the only shared data structure between the two threads.
 *
 * <p><b>Design decisions:</b>
 *
 * <ul>
 *   <li>Implements {@link EgressListener} (not {@code ControlledEgressListener}) — never returns
 *       ABORT. Per-client backpressure is handled by Netty's WriteBufferWaterMark, not Aeron.
 *   <li>Uses a LIFO free-list pool of {@link EgressEntry} objects to avoid allocation on the hot
 *       path. When the pool is exhausted, messages are dropped with a metric increment.
 *   <li>The raw SBE bytes (header + body) are copied into the entry's backing array via {@code
 *       buffer.getBytes()}. The Aeron DirectBuffer is only valid for the duration of this callback.
 * </ul>
 *
 * <p><b>Threading.</b> Single-threaded — called only from the AeronEgressThread's {@code
 * pollEgress()} loop. Not thread-safe.
 *
 * <p><b>Allocation.</b> Zero allocation after construction. All entries are pre-allocated.
 *
 * @see <a href="docs/websocket-architecture.md">WebSocket Architecture — Section 1</a>
 */
public final class WebSocketEgressListener implements EgressListener {

  private static final Logger LOG = LogManager.getLogger(WebSocketEgressListener.class);

  private final ManyToOneConcurrentArrayQueue<EgressEntry> queue;
  private final ManyToOneConcurrentArrayQueue<EgressEntry> returnQueue;
  private final WebSocketMetrics metrics;

  // Deferred init — set via init() after cluster client is constructed (breaks circular dep).
  // Same pattern as FixGateway.init(clusterClient, egressListener).
  private WebSocketClusterClient clusterClient;

  // --- LIFO free-list pool (Aeron thread only — not shared with Netty) ---
  private final EgressEntry[] pool;
  private int poolCount;

  /**
   * Create a new egress listener with a pre-allocated entry pool.
   *
   * @param queue the lock-free queue bridging AeronEgressThread → Netty event loop
   * @param returnQueue the lock-free return queue for recycling entries from Netty → Aeron thread
   * @param metrics metrics for dropped messages and queue depth tracking
   * @param poolCapacity number of pre-allocated pool entries (typically matches queue capacity)
   * @param maxMessageSize maximum SBE message size per entry in bytes
   */
  public WebSocketEgressListener(
      final ManyToOneConcurrentArrayQueue<EgressEntry> queue,
      final ManyToOneConcurrentArrayQueue<EgressEntry> returnQueue,
      final WebSocketMetrics metrics,
      final int poolCapacity,
      final int maxMessageSize) {
    this.queue = Objects.requireNonNull(queue, "queue");
    this.returnQueue = Objects.requireNonNull(returnQueue, "returnQueue");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    this.pool = new EgressEntry[poolCapacity];
    this.poolCount = poolCapacity;
    for (int i = 0; i < poolCapacity; i++) {
      pool[i] = new EgressEntry(maxMessageSize);
    }
  }

  /**
   * Deferred initialization — sets the cluster client reference after construction. Must be called
   * before the egress thread starts polling. Breaks the circular dependency between egress listener
   * and cluster client (same pattern as {@code FixGateway.init(clusterClient, egressListener)}).
   *
   * @param clusterClient the cluster client for reconnect signaling
   */
  public void init(final WebSocketClusterClient clusterClient) {
    this.clusterClient = Objects.requireNonNull(clusterClient, "clusterClient");
  }

  /**
   * Called by Aeron when a message is received from the cluster egress. Copies the raw SBE bytes
   * into a pool entry and enqueues it.
   *
   * @param clusterSessionId the cluster session ID
   * @param timestamp the cluster timestamp (epoch nanoseconds)
   * @param buffer the Aeron DirectBuffer containing the SBE message (valid only during this
   *     callback)
   * @param offset the start offset of the SBE message within the buffer
   * @param length the total length of the SBE message
   * @param header the Aeron log header (unused)
   */
  @Override
  public void onMessage(
      final long clusterSessionId,
      final long timestamp,
      final DirectBuffer buffer,
      final int offset,
      final int length,
      final Header header) {

    // Drain return queue — reclaim entries that Netty has finished processing.
    // This is the only place pool entries are reclaimed, keeping pool access single-threaded.
    EgressEntry returned;
    while ((returned = returnQueue.poll()) != null) {
      if (poolCount < pool.length) {
        pool[poolCount++] = returned;
      }
    }

    // Pop an entry from the pool
    if (poolCount == 0) {
      metrics.messageDropped();
      LOG.warn("Egress pool exhausted — message dropped (length={})", length);
      return;
    }
    final var entry = pool[--poolCount];

    // Bounds check — drop (not truncate) if message exceeds entry capacity. A truncated SBE
    // message is corrupt and would cause decode failures downstream. Better to drop entirely.
    if (length > entry.bytes().length) {
      LOG.warn(
          "Egress message exceeds entry capacity ({} > {}) — dropping",
          length,
          entry.bytes().length);
      pool[poolCount++] = entry;
      metrics.messageDropped();
      return;
    }

    // Copy raw SBE bytes (DirectBuffer only valid during this callback)
    buffer.getBytes(offset, entry.bytes(), 0, length);
    final int templateId = EgressEntry.extractTemplateId(entry.bytes(), 0);
    entry.setMetadata(length, templateId);

    // Enqueue for the Netty drain handler
    if (!queue.offer(entry)) {
      // Queue full — return entry to pool, drop message
      pool[poolCount++] = entry;
      metrics.messageDropped();
      LOG.warn("Egress queue full — message dropped (templateId={})", templateId);
    }
  }

  /**
   * Called by Aeron on cluster session events. Only signals reconnection for error-class events
   * (ERROR, CLOSED, AUTHENTICATION_REJECTED). OK events are logged but do not trigger reconnection.
   *
   * @param correlationId the correlation ID
   * @param clusterSessionId the cluster session ID
   * @param leadershipTermId the leadership term ID
   * @param leaderMemberId the leader member ID
   * @param code the event code
   * @param detail the event detail string
   */
  @Override
  public void onSessionEvent(
      final long correlationId,
      final long clusterSessionId,
      final long leadershipTermId,
      final int leaderMemberId,
      final EventCode code,
      final String detail) {
    LOG.info(
        "Cluster session event: code={} detail={} sessionId={}", code, detail, clusterSessionId);

    // Only signal reconnect for error-class events — OK is expected on successful session open.
    if (code == EventCode.ERROR
        || code == EventCode.CLOSED
        || code == EventCode.AUTHENTICATION_REJECTED) {
      if (clusterClient != null) {
        clusterClient.signalReconnectNeeded();
      }
    }
  }

  /**
   * Called by Aeron when the cluster elects a new leader. Logs the leadership change for
   * operational visibility. No reconnection is needed — the cluster client handles leader failover
   * transparently.
   *
   * @param clusterSessionId the cluster session ID
   * @param leadershipTermId the new leadership term ID
   * @param leaderMemberId the new leader's member ID
   * @param ingressEndpoints the updated ingress endpoints (may be null)
   */
  @Override
  public void onNewLeader(
      final long clusterSessionId,
      final long leadershipTermId,
      final int leaderMemberId,
      final String ingressEndpoints) {
    LOG.info("New cluster leader: memberId={} termId={}", leaderMemberId, leadershipTermId);
  }

  /**
   * Return an entry to the pool after the drain handler has processed it. Called from the Netty
   * event loop thread — enqueues into the return queue which is drained by the Aeron thread.
   *
   * <p><b>Thread safety:</b> Uses the lock-free return queue to transfer entries from the Netty
   * thread back to the Aeron thread. The actual pool array is only accessed by the Aeron thread.
   *
   * @param entry the processed entry to return to the pool
   */
  public void returnToPool(final EgressEntry entry) {
    Objects.requireNonNull(entry, "entry");
    if (!returnQueue.offer(entry)) {
      LOG.warn(
          "Return queue full — pool entry leaked (templateId={}). This indicates the Aeron egress "
              + "thread is not draining the return queue fast enough.",
          entry.templateId());
    }
  }

  /**
   * @return the current number of available entries in the pool
   */
  public int poolAvailable() {
    return poolCount;
  }
}
