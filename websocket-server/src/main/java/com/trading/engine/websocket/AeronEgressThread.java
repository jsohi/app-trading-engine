package com.trading.engine.websocket;

import com.trading.engine.messages.sbe.CommandAckEncoder;
import com.trading.engine.messages.sbe.CommandAckStatus;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import io.aeron.Publication;
import java.util.Objects;
import java.util.UUID;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.agrona.concurrent.UnsafeBuffer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Dedicated thread for polling the Aeron cluster egress and feeding the {@link
 * ManyToOneConcurrentArrayQueue} that bridges to the Netty drain handler. Also drains the
 * browser→cluster command queue produced by {@link CommandDispatcher} on Netty event-loop threads
 * and offers each command to the cluster, with bounded BACK_PRESSURED retries.
 *
 * <p>Wraps a {@link WebSocketClusterClient} (which implements {@link Agent}) plus a small {@code
 * CommandPump} {@link Agent} in a single composite {@code Agent}, then runs the composite on a
 * named "aeron-egress" thread via {@link AgentRunner}. The cluster client's {@code doWork()}
 * performs {@code pollEgress()}; the command pump's {@code doWork()} drains commands and sends
 * BACK_PRESSURED THROTTLED acks via the ack back-channel.
 *
 * <p><b>Deadlock argument.</b> The Aeron egress thread never blocks waiting on the ackQueue —
 * full-queue is dropped (with metric). The Netty drain handler never blocks waiting on the
 * commandQueue (it only reads from egressQueue + ackQueue). No circular wait possible.
 *
 * <p><b>Backpressure retries.</b> When {@code clusterClient.offer()} returns {@link
 * Publication#BACK_PRESSURED}, the same entry is retried up to {@link #MAX_BP_RETRIES} times within
 * the same {@code doWork} invocation. On exhaustion the entry is rejected back to the originator
 * via {@code CommandAck(THROTTLED)} and the entry is returned to the pool.
 *
 * <p><b>Idle strategy.</b> Uses {@link BackoffIdleStrategy} (spin → yield → park) matching the
 * gateway pattern for low-latency egress polling.
 *
 * <p><b>Threading.</b> Owns a single named thread ("aeron-egress"). All cluster-side work happens
 * on this thread.
 *
 * <p><b>Allocation.</b> One-time thread creation at startup. The CommandPump pre-allocates two
 * scratch buffers for ack encoding; no allocation on the per-command hot path beyond what the
 * cluster client itself performs.
 *
 * @see <a href="docs/websocket-architecture.md">WebSocket Architecture — Section 1</a>
 */
public final class AeronEgressThread implements AutoCloseable {

  private static final Logger LOG = LogManager.getLogger(AeronEgressThread.class);

  /** Maximum bounded retries on BACK_PRESSURED before THROTTLED ack. */
  static final int MAX_BP_RETRIES = 8;

  private final AgentRunner agentRunner;
  private final ManyToOneConcurrentArrayQueue<EgressEntry> queue;
  private final WebSocketMetrics metrics;
  private final int queueCapacity;
  private volatile boolean started;

  /**
   * Backwards-compatible constructor — no command/ack wiring. Used by legacy tests that don't
   * exercise the browser→cluster path.
   *
   * @param clusterClient the cluster client agent
   * @param queue the egress queue
   * @param metrics metrics instance
   * @param queueCapacity queue capacity (for logging)
   */
  public AeronEgressThread(
      final WebSocketClusterClient clusterClient,
      final ManyToOneConcurrentArrayQueue<EgressEntry> queue,
      final WebSocketMetrics metrics,
      final int queueCapacity) {
    this(clusterClient, queue, null, null, null, metrics, queueCapacity);
  }

  /**
   * Create the egress thread (not yet started) with full command/ack wiring.
   *
   * @param clusterClient the cluster client agent to run on this thread
   * @param queue the egress queue (cluster→browser) to monitor for backpressure
   * @param commandQueue the browser→cluster command queue to drain (may be null to disable)
   * @param ackQueue the cluster→browser ack back-channel queue (may be null to disable)
   * @param commandEntryPool the pool that owns command entries; releases happen here (may be null
   *     only if commandQueue/ackQueue are null)
   * @param metrics metrics instance for queue depth and poll latency
   * @param queueCapacity the maximum queue capacity (for backpressure threshold calculation)
   */
  public AeronEgressThread(
      final WebSocketClusterClient clusterClient,
      final ManyToOneConcurrentArrayQueue<EgressEntry> queue,
      final ManyToOneConcurrentArrayQueue<EgressEntry> commandQueue,
      final ManyToOneConcurrentArrayQueue<EgressEntry> ackQueue,
      final CommandEntryPool commandEntryPool,
      final WebSocketMetrics metrics,
      final int queueCapacity) {
    Objects.requireNonNull(clusterClient, "clusterClient");
    this.queue = Objects.requireNonNull(queue, "queue");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    this.queueCapacity = queueCapacity;
    final Agent compositeAgent;
    if (commandQueue != null && ackQueue != null && commandEntryPool != null) {
      final var pump =
          new CommandPump(clusterClient, commandQueue, ackQueue, commandEntryPool, metrics);
      compositeAgent = new CompositeAgent(clusterClient, pump);
    } else {
      compositeAgent = clusterClient;
    }
    // BackoffIdleStrategy: spin → yield → park. Matches the gateway pattern for low-latency
    // egress polling. Defaults: 1 spin, 1 yield, 1us min park, 1ms max park.
    this.agentRunner =
        new AgentRunner(
            new BackoffIdleStrategy(),
            throwable -> LOG.error("AeronEgressThread error", throwable),
            null, // no AtomicCounter
            compositeAgent);
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

  // ---------------------------------------------------------------------------
  // CompositeAgent — runs cluster client + command pump on the same thread.
  // ---------------------------------------------------------------------------

  /** Runs two agents serially in a single duty cycle. Both get a chance per loop iteration. */
  static final class CompositeAgent implements Agent {

    private final Agent first;
    private final Agent second;

    CompositeAgent(final Agent first, final Agent second) {
      this.first = first;
      this.second = second;
    }

    @Override
    public void onStart() {
      first.onStart();
      second.onStart();
    }

    @Override
    public int doWork() throws Exception {
      return first.doWork() + second.doWork();
    }

    @Override
    public void onClose() {
      try {
        first.onClose();
      } catch (final RuntimeException e) {
        LOG.warn("First agent onClose failed", e);
      }
      try {
        second.onClose();
      } catch (final RuntimeException e) {
        LOG.warn("Second agent onClose failed", e);
      }
    }

    @Override
    public String roleName() {
      return first.roleName() + "+" + second.roleName();
    }
  }

  // ---------------------------------------------------------------------------
  // CommandPump — drains commandQueue, offers to cluster, fires THROTTLED acks.
  // ---------------------------------------------------------------------------

  /**
   * Drains the commandQueue and forwards each entry to the cluster via {@link
   * WebSocketClusterClient#offer}. On BACK_PRESSURED (after {@link #MAX_BP_RETRIES}) the pump posts
   * a synthetic {@code CommandAck(THROTTLED)} back via the ackQueue so the originating session's
   * drain handler can deliver it to the browser.
   */
  static final class CommandPump implements Agent {

    private final WebSocketClusterClient clusterClient;
    private final ManyToOneConcurrentArrayQueue<EgressEntry> commandQueue;
    private final ManyToOneConcurrentArrayQueue<EgressEntry> ackQueue;
    private final CommandEntryPool commandEntryPool;
    private final WebSocketMetrics metrics;
    // Pre-allocated scratch for ack encoding (this thread only).
    private final ExpandableArrayBuffer ackEncodeBuf = new ExpandableArrayBuffer(64);
    private final UnsafeBuffer offerBuf = new UnsafeBuffer(new byte[0]);

    CommandPump(
        final WebSocketClusterClient clusterClient,
        final ManyToOneConcurrentArrayQueue<EgressEntry> commandQueue,
        final ManyToOneConcurrentArrayQueue<EgressEntry> ackQueue,
        final CommandEntryPool commandEntryPool,
        final WebSocketMetrics metrics) {
      this.clusterClient = clusterClient;
      this.commandQueue = commandQueue;
      this.ackQueue = ackQueue;
      this.commandEntryPool = commandEntryPool;
      this.metrics = metrics;
    }

    @Override
    public int doWork() {
      int work = 0;
      EgressEntry entry;
      while ((entry = commandQueue.poll()) != null) {
        work++;
        boolean repurposedAsAck = false;
        try {
          if (entry.direction() != EgressEntry.Direction.BROWSER_TO_CLUSTER
              || entry.length() <= 0) {
            // Sentinel entry (e.g. degraded path returning to pool) — skip cluster offer.
            continue;
          }
          if (!clusterClient.isConnected()) {
            // Cluster down — synthesize a THROTTLED ack so the client retries later.
            repurposedAsAck = postThrottledAck(entry);
            continue;
          }
          offerBuf.wrap(entry.bytes(), 0, entry.length());
          long result = clusterClient.offer(offerBuf, 0, entry.length());
          int retries = 0;
          while (result == Publication.BACK_PRESSURED && retries < MAX_BP_RETRIES) {
            retries++;
            result = clusterClient.offer(offerBuf, 0, entry.length());
          }
          if (result == Publication.BACK_PRESSURED || result < 0) {
            repurposedAsAck = postThrottledAck(entry);
          }
        } finally {
          if (!repurposedAsAck) {
            commandEntryPool.release(entry);
          }
        }
      }
      return work;
    }

    /**
     * Repurpose the source command entry as a CommandAck(THROTTLED) and offer it on the ack
     * back-channel.
     *
     * @param source the command entry whose offer failed
     * @return {@code true} if the entry was successfully posted to the ack queue (caller must NOT
     *     release it — the drain handler will release it on consume); {@code false} if the post
     *     failed (caller should release to the pool itself)
     */
    private boolean postThrottledAck(final EgressEntry source) {
      final var enc = new CommandAckEncoder();
      final var header = new MessageHeaderEncoder();
      enc.wrapAndApplyHeader(ackEncodeBuf, 0, header);
      enc.clientCmdSeqNo(source.clientCmdSeqNo());
      enc.status(CommandAckStatus.Throttled);
      final int encodedLen = MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
      // Reuse the source entry's backing array — overwrite with the ack bytes.
      final var srcSession = new UUID(source.sessionIdMsb(), source.sessionIdLsb());
      source.fillAckBackChannel(ackEncodeBuf.byteArray(), 0, encodedLen, srcSession);
      if (ackQueue.offer(source)) {
        metrics.commandBackpressured();
        return true;
      }
      metrics.commandAckDropped();
      return false;
    }

    @Override
    public String roleName() {
      return "ws-command-pump";
    }
  }
}
