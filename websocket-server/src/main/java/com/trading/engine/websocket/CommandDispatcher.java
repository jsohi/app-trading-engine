package com.trading.engine.websocket;

import com.trading.engine.messages.sbe.CancelOrderRequestDecoder;
import com.trading.engine.messages.sbe.CommandAckEncoder;
import com.trading.engine.messages.sbe.CommandAckStatus;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.NewOrderSingleDecoder;
import com.trading.engine.messages.sbe.QuoteRequestDecoder;
import com.trading.engine.messages.sbe.WebSocketErrorCode;
import com.trading.engine.messages.sbe.WebSocketErrorEncoder;
import com.trading.engine.messages.util.ByteArrayKey;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.Object2ObjectHashMap;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.agrona.concurrent.NanoClock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Browser-to-cluster command dispatch with per-user ClOrdID dedup, lock-free per-user token-bucket
 * rate limiting, account entitlement gating, and synchronous {@code CommandAck} on the enqueue
 * decision (Netty event loop). Cluster-side BACK_PRESSURED throttling is signalled later via the
 * ack back-channel populated by {@link AeronEgressThread}.
 *
 * <p><b>Threading.</b> {@link #dispatch} runs on the channel's Netty event loop. Concurrent calls
 * across different sessions for the same {@code userId} (different worker loops) are coordinated
 * via:
 *
 * <ul>
 *   <li>Per-user dedup state — {@link ConcurrentHashMap} of user → state with a {@link
 *       ReentrantLock} guarding the inner non-thread-safe {@link Object2ObjectHashMap}. {@code
 *       tryLock} with a short timeout; on miss the command is accepted (fail-open) and {@link
 *       WebSocketMetrics#dedupTryLockMiss} is incremented — blocking the event loop is worse than a
 *       missed dedup.
 *   <li>Per-user rate limiter — lock-free CAS on a single {@link AtomicLong} token counter,
 *       mirroring {@link ConnectionRateLimiter.RateLimiterState}.
 * </ul>
 *
 * <p><b>Allocation.</b> Reusable per-instance SBE encoders/scratch buffers. The dedup map's inner
 * {@link ByteArrayKey}s are owned (defensively copied from the SBE payload) and remain alive until
 * evicted by TTL or capacity. Rate-limiter state is pooled across the user's lifetime.
 *
 * <p><b>Cleanup.</b> A single-thread daemon scheduled executor sweeps stale dedup entries every 60
 * seconds (TTL eviction). Stop with {@link #close()} on server shutdown.
 *
 * @see <a href="docs/websocket-architecture.md">WebSocket Architecture — Section 5</a>
 */
public final class CommandDispatcher implements AutoCloseable {

  private static final Logger LOG = LogManager.getLogger(CommandDispatcher.class);

  /** Sweep interval for the dedup TTL/capacity eviction task. */
  private static final long SWEEP_INTERVAL_SECONDS = 60L;

  /** Maximum ClOrdID byte length per the SBE schema. */
  private static final int MAX_CLORDID_LENGTH = 20;

  /** Account code field length (16 bytes). */
  private static final int ACCOUNT_CODE_LENGTH = 16;

  // --- Collaborators ---
  private final WebSocketServerConfig config;
  private final WebSocketMetrics metrics;
  private final NanoClock nanoClock;
  private final ManyToOneConcurrentArrayQueue<EgressEntry> commandQueue;
  private final EgressEntryAllocator entryAllocator;

  // --- Per-user state (cross-thread) ---
  private final ConcurrentHashMap<String, UserDedupState> dedupByUser = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, RateLimiterState> rateByUser = new ConcurrentHashMap<>();

  // Dedicated lock object for top-level eviction coordination on dedupByUser. Held by:
  //   (a) dispatch() while doing the size+containsKey gate + evictOldestUser + computeIfAbsent
  //       (so two Netty workers can't both insert past the cap),
  //   (b) sweep() when removing a top-level user entry (so its iter.remove() doesn't drop a
  //       state object that dispatch() just observed under (a)).
  // Synchronizing on the CHM itself would be misleading — its internal segment locks are not
  // taken by `synchronized(map)`. A dedicated Object makes the intent explicit and verifiable.
  private final Object userMapLock = new Object();

  // --- Sweeper ---
  private final ScheduledExecutorService sweeper;

  // --- Reusable per-instance encoders / scratch (Netty thread access only) ---
  // The dispatcher is held by exactly one WebSocketFrameDispatcher instance (per channel), so
  // these fields are accessed from a single event loop thread.
  private final ExpandableArrayBuffer responseBuf = new ExpandableArrayBuffer(128);
  private final byte[] clOrdIdScratch = new byte[MAX_CLORDID_LENGTH];
  private final byte[] accountScratch = new byte[ACCOUNT_CODE_LENGTH];

  /**
   * Allocate and release pooled {@link EgressEntry} objects. The pool is owned by {@link
   * WebSocketServerMain} (production) or a test fixture; the dispatcher stays oblivious to pool
   * topology by going through this interface.
   */
  public interface EgressEntryAllocator {
    /**
     * @return a free entry from the pool, or {@code null} if the pool is exhausted
     */
    EgressEntry tryAcquire();

    /**
     * Release an entry back to the pool. Called when the dispatcher cannot enqueue the entry (e.g.,
     * commandQueue full, oversized payload).
     *
     * @param entry the entry to release
     */
    void release(EgressEntry entry);
  }

  /**
   * Create a CommandDispatcher and start the periodic sweeper.
   *
   * @param config server configuration (rate-limit, dedup, capacity tunables)
   * @param metrics metrics instance for counters
   * @param nanoClock monotonic clock for rate-limit refill and dedup TTL bookkeeping
   * @param commandQueue browser→cluster queue drained by {@link AeronEgressThread}
   * @param entryAllocator allocator that yields free pool entries; may return {@code null} on
   *     exhaustion (treated as command throttling)
   */
  public CommandDispatcher(
      final WebSocketServerConfig config,
      final WebSocketMetrics metrics,
      final NanoClock nanoClock,
      final ManyToOneConcurrentArrayQueue<EgressEntry> commandQueue,
      final EgressEntryAllocator entryAllocator) {
    this.config = Objects.requireNonNull(config, "config");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
    this.commandQueue = Objects.requireNonNull(commandQueue, "commandQueue");
    this.entryAllocator = Objects.requireNonNull(entryAllocator, "entryAllocator");
    this.sweeper =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              final var t = new Thread(r, "ws-dedup-sweeper");
              t.setDaemon(true);
              return t;
            });
    this.sweeper.scheduleWithFixedDelay(
        this::sweep, SWEEP_INTERVAL_SECONDS, SWEEP_INTERVAL_SECONDS, TimeUnit.SECONDS);
  }

  /**
   * Dispatch a browser command frame. The SBE bytes are read directly from the supplied {@link
   * ByteBuf}; on success they are copied into a pool entry and enqueued. On failure a {@code
   * CommandAck(REJECTED|DUPLICATE|THROTTLED)} or {@code WebSocketError(CommandRejected)} is written
   * synchronously back to the channel as a reliable frame.
   *
   * @param ctx the channel handler context
   * @param session the authenticated session (must have {@code userId} set)
   * @param frameContent the binary-frame content; valid only within this call's scope
   * @param templateId the SBE templateId (1, 4, or 6)
   * @param blockLength the SBE block length from the message header
   * @param version the SBE schema version from the message header
   */
  public void dispatch(
      final ChannelHandlerContext ctx,
      final WebSocketSession session,
      final ByteBuf frameContent,
      final int templateId,
      final int blockLength,
      final int version) {
    Objects.requireNonNull(ctx, "ctx");
    Objects.requireNonNull(session, "session");
    Objects.requireNonNull(frameContent, "frameContent");

    final var userId = session.userId();
    if (userId == null || userId.isEmpty()) {
      // Defensive: dispatcher should only be reached post-auth.
      sendError(ctx, WebSocketErrorCode.CommandRejected);
      metrics.commandRejected();
      return;
    }

    final int totalLength = frameContent.readableBytes();
    final int sbeBodyOffset = MessageHeaderDecoder.ENCODED_LENGTH;

    // 1. Account entitlement check — must extract account from the request body.
    final boolean entitled =
        checkAccountEntitlement(session, frameContent, templateId, sbeBodyOffset, totalLength);
    if (!entitled) {
      sendCommandAck(ctx, session, CommandAckStatus.Rejected);
      metrics.commandRejected();
      return;
    }

    // 2. Per-user rate limit (lock-free CAS).
    if (!tryAcquireRateToken(userId)) {
      metrics.commandRateLimited();
      sendCommandAck(ctx, session, CommandAckStatus.Throttled);
      return;
    }

    // 3. Per-user dedup (NewOrderSingle only — Cancel/Quote each carry their own server-side
    //    identity; cluster enforces global ClOrdID uniqueness per APP-206).
    if (templateId == NewOrderSingleDecoder.TEMPLATE_ID) {
      final var dedupOutcome =
          checkDedup(userId, frameContent, sbeBodyOffset, blockLength, version, totalLength);
      if (dedupOutcome == DedupOutcome.DUPLICATE) {
        metrics.commandDuplicate();
        sendCommandAck(ctx, session, CommandAckStatus.Duplicate);
        return;
      }
      // ACCEPTED or TRYLOCK_MISS → proceed (TRYLOCK_MISS already incremented the counter).
    }

    // 4. Acquire pool entry.
    final var entry = entryAllocator.tryAcquire();
    if (entry == null) {
      // Pool exhausted — treat as throttled.
      sendCommandAck(ctx, session, CommandAckStatus.Throttled);
      metrics.commandBackpressured();
      return;
    }

    // 5. Copy bytes into the entry.
    final var bytes = entry.bytes();
    if (totalLength > bytes.length) {
      // Frame too big for the pool entry — should not happen with sane sizing.
      LOG.warn("Command exceeds entry capacity ({} > {}) — rejecting", totalLength, bytes.length);
      entryAllocator.release(entry);
      sendCommandAck(ctx, session, CommandAckStatus.Rejected);
      metrics.commandRejected();
      return;
    }

    // Copy frame bytes into the entry, then set metadata.
    frameContent.getBytes(0, bytes, 0, totalLength);
    // Embed the client's last-known seqNo for echo-back in the THROTTLED ack path.
    final long ackSeqNo = session.lastClientCmdSeqNo();
    entry.fillCommandMetadata(totalLength, templateId, session.sessionId(), ackSeqNo);

    // 6. Enqueue.
    if (!commandQueue.offer(entry)) {
      LOG.warn("Command queue full — rejecting command (templateId={})", templateId);
      entryAllocator.release(entry);
      sendCommandAck(ctx, session, CommandAckStatus.Throttled);
      metrics.commandBackpressured();
      return;
    }

    metrics.commandDispatched();
    sendCommandAck(ctx, session, CommandAckStatus.Accepted);
  }

  /** Outcome of a dedup check. */
  private enum DedupOutcome {
    ACCEPTED,
    DUPLICATE,
    TRYLOCK_MISS
  }

  // ---------------------------------------------------------------------------
  // Account entitlement
  // ---------------------------------------------------------------------------

  private boolean checkAccountEntitlement(
      final WebSocketSession session,
      final ByteBuf frameContent,
      final int templateId,
      final int sbeBodyOffset,
      final int totalLength) {
    final int accountOffsetInBody = accountOffsetFor(templateId);
    if (accountOffsetInBody < 0) {
      return false; // template not supported for browser commands
    }
    final int absOffset = sbeBodyOffset + accountOffsetInBody;
    if (absOffset + ACCOUNT_CODE_LENGTH > totalLength) {
      return false; // truncated frame
    }
    frameContent.getBytes(absOffset, accountScratch, 0, ACCOUNT_CODE_LENGTH);
    final long high = AccountPacker.packHigh(accountScratch, 0);
    final long low = AccountPacker.packLow(accountScratch, 0);
    return session.isEntitledAccount(high, low);
  }

  private static int accountOffsetFor(final int templateId) {
    return switch (templateId) {
      case NewOrderSingleDecoder.TEMPLATE_ID -> NewOrderSingleDecoder.accountCodeEncodingOffset();
      case CancelOrderRequestDecoder.TEMPLATE_ID ->
          CancelOrderRequestDecoder.accountCodeEncodingOffset();
      case QuoteRequestDecoder.TEMPLATE_ID -> QuoteRequestDecoder.accountCodeEncodingOffset();
      default -> -1;
    };
  }

  // ---------------------------------------------------------------------------
  // Rate limiter (lock-free CAS, per user)
  // ---------------------------------------------------------------------------

  private boolean tryAcquireRateToken(final String userId) {
    final var state =
        rateByUser.computeIfAbsent(
            userId, k -> new RateLimiterState(config.commandsBurst(), nanoClock));
    return state.tryAcquire(config.commandsBurst(), config.commandsPerSecSustained(), nanoClock);
  }

  /**
   * Per-user token-bucket state. Mirrors {@link ConnectionRateLimiter.RateLimiterState} but is
   * keyed by userId rather than IP. CAS-only updates — no lock.
   */
  static final class RateLimiterState {
    private final AtomicLong tokens;
    private final AtomicLong lastRefillNs;

    /** Last time this user issued a successful command (for sweeper LRU). */
    volatile long lastUsedNs;

    RateLimiterState(final int burst, final NanoClock nanoClock) {
      this.tokens = new AtomicLong(burst);
      this.lastRefillNs = new AtomicLong(nanoClock.nanoTime());
      this.lastUsedNs = nanoClock.nanoTime();
    }

    boolean tryAcquire(final int burst, final int sustained, final NanoClock nanoClock) {
      refillIfNeeded(burst, sustained, nanoClock);
      long current;
      do {
        current = tokens.get();
        if (current <= 0) {
          return false;
        }
      } while (!tokens.compareAndSet(current, current - 1));
      lastUsedNs = nanoClock.nanoTime();
      return true;
    }

    private void refillIfNeeded(final int burst, final int sustained, final NanoClock nanoClock) {
      final long nowNs = nanoClock.nanoTime();
      final long lastNs = lastRefillNs.get();
      if (nowNs - lastNs >= 1_000_000_000L) {
        if (lastRefillNs.compareAndSet(lastNs, nowNs)) {
          // Refill: add `sustained` tokens, capped at `burst`. With per-second refill of
          // `sustained` tokens and a cap of `burst`, the steady-state max throughput is
          // `sustained` cmd/sec while permitting bursts up to `burst`.
          final long current = tokens.get();
          final long target = Math.min(current + sustained, burst);
          tokens.set(target);
        }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // ClOrdID dedup (per user, ReentrantLock-guarded inner Object2ObjectHashMap)
  // ---------------------------------------------------------------------------

  private DedupOutcome checkDedup(
      final String userId,
      final ByteBuf frameContent,
      final int sbeBodyOffset,
      final int blockLength,
      final int version,
      final int totalLength) {
    final int clOrdLen = NewOrderSingleDecoder.clOrdIdLength();
    final int clOrdOffsetAbs = sbeBodyOffset + NewOrderSingleDecoder.clOrdIdEncodingOffset();
    if (clOrdOffsetAbs + clOrdLen > totalLength) {
      return DedupOutcome.ACCEPTED; // truncated — let downstream reject
    }
    frameContent.getBytes(clOrdOffsetAbs, clOrdIdScratch, 0, clOrdLen);

    // Outer-map cap check + creation must be atomic against other threads racing the same user.
    // Holding userMapLock (a dedicated Object) across (a) the size+containsKey gate, (b)
    // evictOldestUser's iteration, and (c) the computeIfAbsent insertion ensures two Netty
    // workers cannot both observe size < cap and both insert past it. The sweeper acquires the
    // same lock when removing top-level entries (see sweep()), so the sweeper's iter.remove()
    // never deletes a state object that a dispatch() call just observed under this monitor.
    // Synchronizing on dedupByUser itself would be misleading — `synchronized(map)` does not
    // take CHM's internal segment locks, and the sweeper's CHM operations would still race.
    // The lock is contended only on the cold path (new user OR map-at-cap) and during the
    // once-per-60s sweep — never per-command.
    final UserDedupState state;
    synchronized (userMapLock) {
      if (dedupByUser.size() >= config.clOrdIdDedupMaxUsers() && !dedupByUser.containsKey(userId)) {
        evictOldestUser();
      }
      state =
          dedupByUser.computeIfAbsent(
              userId, k -> new UserDedupState(config.clOrdIdDedupCapacity()));
    }
    final long lockMicros = config.dedupTryLockMicros();
    boolean locked = false;
    try {
      locked = state.lock.tryLock(lockMicros, TimeUnit.MICROSECONDS);
    } catch (final InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
    if (!locked) {
      metrics.dedupTryLockMiss();
      return DedupOutcome.TRYLOCK_MISS;
    }
    try {
      final long nowNs = nanoClock.nanoTime();
      // Fast path: probe with the reusable key.
      state.probe.set(clOrdIdScratch, 0, clOrdLen);
      final var existing = state.entries.get(state.probe);
      if (existing != null) {
        // Check TTL; if expired, treat as new.
        final long ageNs = nowNs - existing.insertNs;
        if (ageNs < TimeUnit.MILLISECONDS.toNanos(config.clOrdIdDedupTtlMs())) {
          return DedupOutcome.DUPLICATE;
        }
        // Expired — fall through to upsert (overwrite insertNs).
        existing.insertNs = nowNs;
        state.lastTouchedNs = nowNs;
        return DedupOutcome.ACCEPTED;
      }
      // Insertion path: enforce inner cap.
      if (state.entries.size() >= state.capacity) {
        state.evictOldest();
      }
      final var ownedKey = ByteArrayKey.copyOf(clOrdIdScratch, 0, clOrdLen);
      state.entries.put(ownedKey, new DedupEntry(nowNs));
      state.lastTouchedNs = nowNs;
      return DedupOutcome.ACCEPTED;
    } finally {
      state.lock.unlock();
    }
  }

  private void evictOldestUser() {
    String oldestUser = null;
    long oldestNs = Long.MAX_VALUE;
    for (final var entry : dedupByUser.entrySet()) {
      final long t = entry.getValue().lastTouchedNs;
      if (t < oldestNs) {
        oldestNs = t;
        oldestUser = entry.getKey();
      }
    }
    if (oldestUser != null) {
      dedupByUser.remove(oldestUser);
    }
  }

  // ---------------------------------------------------------------------------
  // CommandAck / WebSocketError encoding
  // ---------------------------------------------------------------------------

  /**
   * Encode and send a {@code CommandAck} as a reliable frame. The ack carries the session's most
   * recent {@code lastClientCmdSeqNo()} (updated by {@code ClientAck}), which the schema does not
   * provide on the request-side messages — clients correlate command outcomes by ClOrdID for
   * orders/cancels and by quoteReqId for quotes; the seqNo on the ack is the highest reliable seqNo
   * the client had acked at command time, used for diagnostics rather than correlation.
   *
   * @param ctx the channel handler context
   * @param session the session
   * @param status the command-ack status
   */
  void sendCommandAck(
      final ChannelHandlerContext ctx,
      final WebSocketSession session,
      final CommandAckStatus status) {
    final long ackSeqNo = session.lastClientCmdSeqNo();
    final var enc = new CommandAckEncoder();
    final var header = new MessageHeaderEncoder();
    enc.wrapAndApplyHeader(responseBuf, 0, header);
    enc.clientCmdSeqNo(ackSeqNo);
    enc.status(status);
    final int encodedLen = MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
    writeReliableEnvelope(ctx, session, encodedLen);
  }

  private void sendError(final ChannelHandlerContext ctx, final WebSocketErrorCode code) {
    if (!ctx.channel().isActive()) {
      return;
    }
    final var errorText = ErrorTextRegistry.textFor(code);
    final var enc = new WebSocketErrorEncoder();
    final var header = new MessageHeaderEncoder();
    enc.wrapAndApplyHeader(responseBuf, 0, header);
    enc.errorCode(code);
    enc.putErrorText(errorText, 0, errorText.length);
    final int encodedLen = MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
    final var nettyBuf = ctx.alloc().buffer(encodedLen);
    boolean written = false;
    try {
      nettyBuf.writeBytes(responseBuf.byteArray(), 0, encodedLen);
      ctx.writeAndFlush(new BinaryWebSocketFrame(nettyBuf));
      written = true;
    } finally {
      if (!written) {
        nettyBuf.release();
      }
    }
  }

  /**
   * Encode a reliable-stream envelope for the bytes already written into {@link #responseBuf} and
   * write the frame to the channel. Uses {@link FrameParser#encodeReliable} so the ack is captured
   * in the per-session replay ring.
   */
  private void writeReliableEnvelope(
      final ChannelHandlerContext ctx, final WebSocketSession session, final int encodedLen) {
    if (!ctx.channel().isActive()) {
      return;
    }
    final var ch = ctx.channel();
    final long seqNo = session.nextReliableSeqNo();
    final var nettyBuf =
        ch.alloc()
            .buffer(
                FrameParser.RELIABLE_HEADER_SIZE + encodedLen,
                FrameParser.RELIABLE_HEADER_SIZE + encodedLen);
    boolean written = false;
    try {
      FrameParser.encodeReliable(nettyBuf, seqNo, responseBuf.byteArray(), 0, encodedLen);
      // Capture for replay.
      final var tracker = session.reliableStreamTracker();
      if (tracker != null) {
        try {
          final int tid = EgressEntry.extractTemplateId(responseBuf.byteArray(), 0);
          tracker.capture(seqNo, tid, responseBuf.byteArray(), 0, encodedLen);
        } catch (final RuntimeException captureEx) {
          tracker.evict(seqNo);
          throw captureEx;
        }
      }
      ch.writeAndFlush(new BinaryWebSocketFrame(nettyBuf));
      written = true;
    } finally {
      if (!written) {
        // Evict so the ring doesn't replay this seqNo.
        final var tracker = session.reliableStreamTracker();
        if (tracker != null) {
          tracker.evict(seqNo);
        }
        nettyBuf.release();
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Sweeper
  // ---------------------------------------------------------------------------

  /** Periodic dedup map TTL/empty-state sweep. Runs on the {@code ws-dedup-sweeper} thread. */
  private void sweep() {
    try {
      final long nowNs = nanoClock.nanoTime();
      final long ttlNs = TimeUnit.MILLISECONDS.toNanos(config.clOrdIdDedupTtlMs());
      final var iter = dedupByUser.entrySet().iterator();
      while (iter.hasNext()) {
        final Map.Entry<String, UserDedupState> userEntry = iter.next();
        final var state = userEntry.getValue();
        // Skip if another thread holds the user lock — try again next cycle.
        if (!state.lock.tryLock()) {
          continue;
        }
        try {
          // Drop expired entries.
          final Iterator<Map.Entry<ByteArrayKey, DedupEntry>> innerIter =
              state.entries.entrySet().iterator();
          while (innerIter.hasNext()) {
            final var inner = innerIter.next();
            if (nowNs - inner.getValue().insertNs > ttlNs) {
              innerIter.remove();
            }
          }
          // If user state is empty and stale, drop the user entry too. Coordinate with
          // dispatch()'s outer-cap path via userMapLock: prevents the sweeper from removing
          // a state object that a Netty thread just observed and is about to use under the
          // userMapLock-protected size+containsKey+computeIfAbsent gate.
          if (state.entries.isEmpty() && nowNs - state.lastTouchedNs > ttlNs) {
            synchronized (userMapLock) {
              iter.remove();
            }
          }
        } finally {
          state.lock.unlock();
        }
      }
      // Sweep stale rate-limiter state too.
      final var rateIter = rateByUser.entrySet().iterator();
      while (rateIter.hasNext()) {
        final var rEntry = rateIter.next();
        if (nowNs - rEntry.getValue().lastUsedNs > ttlNs) {
          rateIter.remove();
        }
      }
    } catch (final RuntimeException re) {
      LOG.warn("CommandDispatcher sweep failed", re);
    }
  }

  /** Stop the sweeper task. */
  @Override
  public void close() {
    sweeper.shutdownNow();
    try {
      if (!sweeper.awaitTermination(2, TimeUnit.SECONDS)) {
        LOG.warn("ws-dedup-sweeper did not terminate within 2s");
      }
    } catch (final InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
  }

  // ---------------------------------------------------------------------------
  // Test access
  // ---------------------------------------------------------------------------

  /**
   * @return number of users currently tracked in the dedup map (test/metric inspection only)
   */
  int dedupUserCount() {
    return dedupByUser.size();
  }

  /**
   * @return number of users currently tracked in the rate-limiter map (test inspection only)
   */
  int rateUserCount() {
    return rateByUser.size();
  }

  /** Run the periodic sweep synchronously for tests. Safe to call from any thread. */
  void sweepForTest() {
    sweep();
  }

  // ---------------------------------------------------------------------------
  // Inner state types
  // ---------------------------------------------------------------------------

  /** Per-user dedup state: an inner map + a tryLock guard + an LRU touch timestamp. */
  static final class UserDedupState {
    final ReentrantLock lock = new ReentrantLock();
    final Object2ObjectHashMap<ByteArrayKey, DedupEntry> entries;
    final ByteArrayKey probe;
    final int capacity;
    volatile long lastTouchedNs;

    UserDedupState(final int capacity) {
      this.capacity = capacity;
      this.entries =
          new Object2ObjectHashMap<>(Math.max(16, (int) Math.ceil(capacity / 0.55f)), 0.55f);
      this.probe = ByteArrayKey.emptyForLookup(MAX_CLORDID_LENGTH);
    }

    /** Evict the entry with the oldest insertNs (LRU). */
    void evictOldest() {
      ByteArrayKey oldestKey = null;
      long oldestNs = Long.MAX_VALUE;
      for (final var entry : entries.entrySet()) {
        if (entry.getValue().insertNs < oldestNs) {
          oldestNs = entry.getValue().insertNs;
          oldestKey = entry.getKey();
        }
      }
      if (oldestKey != null) {
        entries.remove(oldestKey);
      }
    }
  }

  /** Per-ClOrdID dedup entry — just the insert timestamp for TTL eviction. */
  static final class DedupEntry {
    long insertNs;

    DedupEntry(final long insertNs) {
      this.insertNs = insertNs;
    }
  }
}
