package com.trading.engine.websocket;

import com.trading.engine.messages.sbe.CommandAckEncoder;
import com.trading.engine.messages.sbe.CommandAckStatus;
import com.trading.engine.messages.sbe.FeedStateEnum;
import com.trading.engine.messages.sbe.MarketDataFeedStateChangeEncoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.logbuffer.FragmentHandler;
import java.util.Objects;
import java.util.UUID;
import java.util.function.LongConsumer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SystemNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Dedicated thread for polling the Aeron cluster egress + the Phase 3 market-data ingest stream and
 * feeding the {@link ManyToOneConcurrentArrayQueue} that bridges to the Netty drain handler. Also
 * drains the browser→cluster command queue produced by {@link CommandDispatcher} on Netty
 * event-loop threads and offers each command to the cluster, with bounded BACK_PRESSURED retries.
 *
 * <p>Wraps three logical agents:
 *
 * <ul>
 *   <li>A {@link WebSocketClusterClient} (which implements {@link Agent}) — performs cluster {@code
 *       pollEgress()} + keep-alive + reconnect.
 *   <li>An optional {@link MarketDataIngressHandler} bound to a {@link Subscription} on the
 *       market-data IPC stream — polled with a separate {@link FragmentAssembler}.
 *   <li>An optional {@link CommandPump} that drains the browser→cluster command queue and posts
 *       BACK_PRESSURED THROTTLED acks via the ack back-channel.
 * </ul>
 *
 * All three are composed under a single named "aeron-egress" thread via {@link AgentRunner}.
 *
 * <p><b>DWRR poll order (RFC 8290 / Linux {@code fq_codel} / LMAX exchange-core pattern).</b> Per
 * cycle the polling composite credits each source by its weight (quantum), then polls in
 * cluster-first order up to {@code min(deficit, FRAGMENT_LIMIT)} fragments. Deficit accumulates
 * across cycles for sources that polled less than their credit; idle sources (poll returned zero)
 * reset their deficit back to ZERO so the next saturated cycle resumes at exactly one quantum — RFC
 * 8290 §4.3, prevents a long-idle source from claiming accumulated burst credit.
 *
 * <ul>
 *   <li>Cluster: quantum = {@link #CLUSTER_QUANTUM} (=2), per-poll cap = {@link
 *       #CLUSTER_FRAGMENT_LIMIT} (=10, matches Aeron's internal {@code AeronCluster.pollEgress()}
 *       fragment limit).
 *   <li>Market-data: quantum = {@link #MARKET_DATA_QUANTUM} (=1), per-poll cap = {@link
 *       #MARKET_DATA_FRAGMENT_LIMIT} (=32 — ticks are conflated upstream so a single drain may
 *       legitimately consume a larger batch when catching up).
 * </ul>
 *
 * In steady saturation the cluster/market-data poll ratio converges to {@code CLUSTER_QUANTUM :
 * MARKET_DATA_QUANTUM = 2 : 1}, matching the cross-source fairness expectation captured in {@code
 * AeronEgressThreadDWRRFairnessTest}.
 *
 * <p><b>Liveness tracker tick.</b> When a {@link MarketDataSubscriptionLivenessTracker} is wired,
 * the polling composite calls {@link MarketDataSubscriptionLivenessTracker#tick(long)} once per
 * cycle so the LIVE → QUIET and LIVE/QUIET → STALE timeout transitions fire even when no fragments
 * are arriving on the market-data stream. The transition callback enqueues a {@code
 * MarketDataFeedStateChange} (template 57) onto the reliable egress queue — encoded with a
 * pre-allocated SBE encoder + scratch {@link UnsafeBuffer} bound at construction.
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
 * gateway pattern for low-latency egress polling. {@code doWork()} returns the SUM of cluster +
 * market-data + command-pump fragments so a non-zero return from any source resets the back-off.
 *
 * <p><b>Threading.</b> Owns a single named thread ("aeron-egress"). All cluster-side work, all
 * market-data ingest work, and all command pump work happen on this thread.
 *
 * <p><b>Allocation.</b> One-time thread creation at startup. The composite agent pre-allocates the
 * market-data {@link FragmentAssembler}, the feed-state-change SBE encoder, a scratch buffer, and a
 * dedicated {@link EgressEntry} pool entry for the transition emission. The {@link CommandPump}
 * pre-allocates two scratch buffers for ack encoding; no allocation on the per-fragment / per-
 * command hot path beyond what the cluster client itself performs.
 *
 * @see <a href="docs/websocket-architecture.md">WebSocket Architecture — Section 1</a>
 */
public final class AeronEgressThread implements AutoCloseable {

  private static final Logger LOG = LogManager.getLogger(AeronEgressThread.class);

  /** Maximum bounded retries on BACK_PRESSURED before THROTTLED ack. */
  static final int MAX_BP_RETRIES = 8;

  /** DWRR quantum (weight) for the cluster source — 2× the market-data weight. */
  static final int CLUSTER_QUANTUM = 2;

  /** DWRR quantum (weight) for the market-data source. */
  static final int MARKET_DATA_QUANTUM = 1;

  /**
   * Per-poll fragment cap for the cluster source. Matches Aeron's internal {@code
   * AeronCluster.pollEgress()} fragment limit of 10 — the cluster path cannot poll more than this
   * in a single call even if our DWRR deficit accumulates higher.
   */
  static final int CLUSTER_FRAGMENT_LIMIT = 10;

  /**
   * Per-poll fragment cap for the market-data source. Higher than the cluster cap because ticks
   * conflate upstream and a single drain may legitimately consume a larger batch.
   */
  static final int MARKET_DATA_FRAGMENT_LIMIT = 32;

  private final AgentRunner agentRunner;
  private final ManyToOneConcurrentArrayQueue<EgressEntry> queue;
  private final WebSocketMetrics metrics;
  private final int queueCapacity;
  private volatile boolean started;

  /**
   * Construct the Phase 3 egress thread. ALL collaborators required — there is no legacy
   * constructor. Tests that don't exercise a particular path must wire a stub for it (e.g. a no-op
   * {@link MarketDataPoller} that returns 0 fragments).
   *
   * @param clusterClient the cluster client agent
   * @param queue the egress queue
   * @param commandQueue the browser→cluster command queue — required
   * @param ackQueue the ack back-channel queue — required
   * @param commandEntryPool command entry pool — required
   * @param metrics metrics instance
   * @param queueCapacity the egress queue capacity
   * @param marketDataPoller SAM seam over the market-data {@code Subscription.poll(...)} (typically
   *     stream 204) — see {@link MarketDataPoller} for the binding idiom; required
   * @param marketDataIngressHandler fragment handler for the market-data subscription; required
   * @param livenessTracker {@link MarketDataSubscriptionLivenessTracker} owning the LIVE/QUIET/
   *     STALE state machine; required. The tracker's transition callback (bound at tracker
   *     construction time, NOT here) must encode + enqueue the resulting {@code
   *     MarketDataFeedStateChange} via the wiring supplied by the launcher
   * @param egressListener the egress listener — used to borrow a pool entry when synthesising
   *     template-57 feed-state-change frames inside this composite; required
   * @param nanoClock monotonic clock used to stamp tracker {@code tick()} and feed-state-change
   *     {@code serverNanos}; typically {@link SystemNanoClock#INSTANCE}
   */
  public AeronEgressThread(
      final WebSocketClusterClient clusterClient,
      final ManyToOneConcurrentArrayQueue<EgressEntry> queue,
      final ManyToOneConcurrentArrayQueue<EgressEntry> commandQueue,
      final ManyToOneConcurrentArrayQueue<EgressEntry> ackQueue,
      final CommandEntryPool commandEntryPool,
      final WebSocketMetrics metrics,
      final int queueCapacity,
      final MarketDataPoller marketDataPoller,
      final MarketDataIngressHandler marketDataIngressHandler,
      final MarketDataSubscriptionLivenessTracker livenessTracker,
      final WebSocketEgressListener egressListener,
      final NanoClock nanoClock) {
    Objects.requireNonNull(clusterClient, "clusterClient");
    this.queue = Objects.requireNonNull(queue, "queue");
    Objects.requireNonNull(commandQueue, "commandQueue");
    Objects.requireNonNull(ackQueue, "ackQueue");
    Objects.requireNonNull(commandEntryPool, "commandEntryPool");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    this.queueCapacity = queueCapacity;
    Objects.requireNonNull(marketDataPoller, "marketDataPoller");
    Objects.requireNonNull(marketDataIngressHandler, "marketDataIngressHandler");
    Objects.requireNonNull(livenessTracker, "livenessTracker");
    Objects.requireNonNull(egressListener, "egressListener");
    Objects.requireNonNull(nanoClock, "nanoClock");

    final Agent pollingAgent =
        new DWRRPollingAgent(
            clusterClient,
            marketDataPoller,
            marketDataIngressHandler,
            livenessTracker,
            metrics,
            nanoClock);

    final Agent compositeAgent;
    {
      final var pump =
          new CommandPump(clusterClient, commandQueue, ackQueue, commandEntryPool, metrics);
      compositeAgent = new CompositeAgent(pollingAgent, pump);
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
  // DWRRPollingAgent — cluster + market-data DWRR poll + tracker.tick().
  // ---------------------------------------------------------------------------

  /**
   * RFC 8290 Deficit Weighted Round Robin polling composite for the cluster + market-data sources.
   * See class-level Javadoc for the cluster:market-data = 2:1 weight rationale. Package-private for
   * direct unit-testing without spinning up the AgentRunner.
   */
  static final class DWRRPollingAgent implements Agent {

    private final WebSocketClusterClient clusterClient;
    private final MarketDataPoller marketDataPoller;
    private final FragmentHandler marketDataHandler;
    private final MarketDataSubscriptionLivenessTracker livenessTracker;
    private final WebSocketMetrics metrics;
    private final NanoClock nanoClock;

    // DWRR deficit counters — accumulated credit per source. Reset to ZERO on idle (poll returned
    // zero fragments) so the next saturated cycle resumes at exactly one quantum. See class
    // Javadoc — RFC 8290 §4.3 anti-burst rule.
    private long clusterDeficit;
    private long marketDataDeficit;

    DWRRPollingAgent(
        final WebSocketClusterClient clusterClient,
        final MarketDataPoller marketDataPoller,
        final FragmentHandler marketDataFragmentHandler,
        final MarketDataSubscriptionLivenessTracker livenessTracker,
        final WebSocketMetrics metrics,
        final NanoClock nanoClock) {
      this.clusterClient = Objects.requireNonNull(clusterClient, "clusterClient");
      this.marketDataPoller = marketDataPoller;
      // Pre-allocated FragmentAssembler — wraps the handler once at construction so per-fragment
      // dispatch is zero-alloc. If no market-data subscription is wired, no assembler is created.
      // Accepts any FragmentHandler (SAM) for testability — production binds a {@link
      // MarketDataIngressHandler}; tests bind scripted lambdas without spinning up the full
      // egress-queue + tracker wiring.
      this.marketDataHandler =
          marketDataPoller == null ? null : new FragmentAssembler(marketDataFragmentHandler);
      this.livenessTracker = livenessTracker;
      this.metrics = Objects.requireNonNull(metrics, "metrics");
      this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
    }

    @Override
    public void onStart() {
      clusterClient.onStart();
    }

    @Override
    public int doWork() throws Exception {
      int total = 0;

      // ── Cluster source (poll first per the cluster-first ordering rule) ──
      clusterDeficit += CLUSTER_QUANTUM;
      // Honour the FRAGMENT_LIMIT cap. The cluster path cannot be told a fragment limit (Aeron's
      // AeronCluster.pollEgress uses its internal limit of 10), so this cap is enforced implicitly
      // by Aeron itself; we cap our deficit math against the same constant for accounting parity.
      //
      // Agent B review F-3: use pollEgressForDwrr() (fragments only) instead of doWork() (which
      // returns fragments + keep-alive's +1). The earlier wiring contaminated the DWRR deficit
      // with keep-alive bookkeeping — every heartbeat decremented the deficit by 1 without a real
      // fragment being consumed, eventually breaking the RFC 8290 anti-burst invariant. Keep-alive
      // still fires on the cluster client's cadence via sendKeepAliveIfDue() below.
      final long clusterBudget = Math.min(clusterDeficit, (long) CLUSTER_FRAGMENT_LIMIT);
      final int clusterFragments = clusterBudget > 0 ? clusterClient.pollEgressForDwrr() : 0;
      if (clusterBudget > 0) {
        // Only mutate deficit when a poll was actually attempted. If the source had negative
        // accumulated debt (over-consumed in a prior cycle, budget <= 0), do NOT zero the deficit
        // — keep the debt so subsequent cycles repay it. Idle reset to zero only when we polled
        // and saw the source genuinely idle (Gemini cloud-review R2 G-4).
        clusterDeficit -= clusterFragments;
        if (clusterFragments == 0) {
          clusterDeficit = 0L; // idle reset — RFC 8290 anti-burst, only after a real poll
        }
      }
      total += clusterFragments;

      // Schedule keep-alive on the cluster client cadence (NOT counted toward the DWRR deficit).
      // Count the +1 toward the cycle's total work so BackoffIdleStrategy doesn't park during
      // heartbeat cycles.
      if (clusterClient.sendKeepAliveIfDue()) {
        total++;
      }

      // ── Market-data source ──
      if (marketDataPoller != null) {
        marketDataDeficit += MARKET_DATA_QUANTUM;
        final int marketDataLimit =
            (int) Math.min(marketDataDeficit, (long) MARKET_DATA_FRAGMENT_LIMIT);
        final int marketDataFragments =
            marketDataLimit > 0 ? marketDataPoller.poll(marketDataHandler, marketDataLimit) : 0;
        if (marketDataLimit > 0) {
          // Same DWRR semantic as the cluster path — only mutate deficit when a poll was
          // attempted; idle reset to zero only when poll returned zero (Gemini cloud-review R2
          // G-5).
          marketDataDeficit -= marketDataFragments;
          if (marketDataFragments == 0) {
            marketDataDeficit = 0L; // idle reset, only after a real poll
          }
        }
        total += marketDataFragments;
      }

      // ── Liveness tracker timer-driven transitions ──
      // Called every cycle (cheap — single comparison branch). Per-cycle granularity is well
      // below the QUIET threshold (1.5 s) and STALE threshold (3 s) so periodic-tick latency is
      // not material. If the cycle stalls (e.g. cluster reconnect parking the agent), the next
      // tick after the stall picks up any overdue transitions.
      if (livenessTracker != null) {
        livenessTracker.tick(nanoClock.nanoTime());
      }

      return total;
    }

    @Override
    public void onClose() {
      try {
        clusterClient.onClose();
      } catch (final RuntimeException e) {
        LOG.warn("Cluster client onClose failed", e);
      }
    }

    @Override
    public String roleName() {
      return clusterClient.roleName() + (marketDataPoller != null ? "+md" : "");
    }

    // Package-private accessors for test introspection.
    long clusterDeficit() {
      return clusterDeficit;
    }

    long marketDataDeficit() {
      return marketDataDeficit;
    }
  }

  // ---------------------------------------------------------------------------
  // CompositeAgent — runs polling composite + command pump on the same thread.
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
  // FeedStateChangeEmitter — encodes + enqueues a template-57 frame on tracker
  // transitions. Pre-allocated SBE encoder + scratch buffer + dedicated pool
  // borrow path so the transition callback is zero-alloc.
  // ---------------------------------------------------------------------------

  /**
   * Bridges the {@link MarketDataSubscriptionLivenessTracker}'s transition callback to the egress
   * queue. Encodes the new state into a {@code MarketDataFeedStateChange} (template 57) frame and
   * enqueues it as a reliable egress entry so the drain handler fans it out to every BIT_PRICES
   * subscriber. Allocation discipline: the SBE encoder, the header encoder, and the scratch {@link
   * ExpandableArrayBuffer} are all final fields bound at construction.
   *
   * <p><b>Threading.</b> Same single-threaded contract as the rest of the egress agent — invoked
   * inside the tracker's {@code transitionTo(...)} on the aeron-egress thread.
   */
  public static final class FeedStateChangeEmitter implements LongConsumer {

    private final WebSocketEgressListener egressListener;
    private final ManyToOneConcurrentArrayQueue<EgressEntry> queue;
    private final WebSocketMetrics metrics;
    private final NanoClock nanoClock;
    private final MarketDataFeedStateChangeEncoder encoder = new MarketDataFeedStateChangeEncoder();
    private final MessageHeaderEncoder header = new MessageHeaderEncoder();
    private final ExpandableArrayBuffer scratch = new ExpandableArrayBuffer(32);

    /**
     * @param egressListener owns the {@link EgressEntry} pool the emitter borrows from
     * @param queue the shared egress queue the encoded frame is enqueued onto
     * @param metrics counter sink for transition + drop accounting
     * @param nanoClock monotonic clock used to stamp the {@code serverNanos} field
     */
    public FeedStateChangeEmitter(
        final WebSocketEgressListener egressListener,
        final ManyToOneConcurrentArrayQueue<EgressEntry> queue,
        final WebSocketMetrics metrics,
        final NanoClock nanoClock) {
      this.egressListener = Objects.requireNonNull(egressListener, "egressListener");
      this.queue = Objects.requireNonNull(queue, "queue");
      this.metrics = Objects.requireNonNull(metrics, "metrics");
      this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
    }

    /**
     * Encodes the transition into a {@code MarketDataFeedStateChange} and enqueues it.
     *
     * @param newState the {@link FeedStateEnum} ordinal as a {@code long} — supplied by the tracker
     */
    @Override
    public void accept(final long newState) {
      encoder.wrapAndApplyHeader(scratch, 0, header);
      encoder.state(FeedStateEnum.get((short) newState));
      encoder.serverNanos(nanoClock.nanoTime());
      final int encodedLen = MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();

      final EgressEntry entry = egressListener.borrowForMarketData();
      if (entry == null) {
        metrics.marketDataDropped();
        LOG.warn(
            "Feed-state-change emission dropped — egress pool exhausted (newState={})", newState);
        return;
      }
      if (encodedLen > entry.bytes().length) {
        egressListener.releaseDirectly(entry);
        metrics.marketDataDropped();
        LOG.warn(
            "Feed-state-change encoded length {} exceeds entry capacity {}",
            encodedLen,
            entry.bytes().length);
        return;
      }
      System.arraycopy(scratch.byteArray(), 0, entry.bytes(), 0, encodedLen);
      entry.setMetadata(encodedLen, MarketDataFeedStateChangeEncoder.TEMPLATE_ID);

      if (!queue.offer(entry)) {
        egressListener.releaseDirectly(entry);
        metrics.marketDataDropped();
        LOG.warn("Feed-state-change emission dropped — egress queue full (newState={})", newState);
        return;
      }
      metrics.marketDataFeedStateTransition();
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
   *
   * <p><b>Allocation.</b> Happy path (command accepted by cluster) is zero-allocation. The
   * THROTTLED ack path repurposes the source entry's backing array and reuses pre-allocated SBE
   * encoders + header — the only per-invocation allocation is a single {@link UUID} for routing
   * (UUID is final/immutable). THROTTLED is cold-path (cluster down or BACK_PRESSURED-exhausted).
   */
  static final class CommandPump implements Agent {

    private final WebSocketClusterClient clusterClient;
    private final ManyToOneConcurrentArrayQueue<EgressEntry> commandQueue;
    private final ManyToOneConcurrentArrayQueue<EgressEntry> ackQueue;
    private final CommandEntryPool commandEntryPool;
    private final WebSocketMetrics metrics;
    // Pre-allocated scratch for ack encoding (this thread only). The encoders + header are also
    // pre-allocated as final fields so a sustained cluster back-pressure burst does not allocate
    // two SBE objects per THROTTLED ack (Agent A review F-5 / Agent B review F-7).
    private final ExpandableArrayBuffer ackEncodeBuf = new ExpandableArrayBuffer(64);
    private final UnsafeBuffer offerBuf = new UnsafeBuffer(new byte[0]);
    private final CommandAckEncoder ackEncoder = new CommandAckEncoder();
    private final MessageHeaderEncoder ackHeaderEncoder = new MessageHeaderEncoder();

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
      ackEncoder.wrapAndApplyHeader(ackEncodeBuf, 0, ackHeaderEncoder);
      ackEncoder.clientCmdSeqNo(source.clientCmdSeqNo());
      ackEncoder.status(CommandAckStatus.Throttled);
      final int encodedLen = MessageHeaderEncoder.ENCODED_LENGTH + ackEncoder.encodedLength();
      // The new UUID() is unavoidable here — fillAckBackChannel needs the UUID for routing, and
      // UUID is final/immutable so it cannot be reused. Cold path (only on cluster down +
      // BACK_PRESSURED-exhausted commands); acceptable per the websocket-server carve-out.
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
