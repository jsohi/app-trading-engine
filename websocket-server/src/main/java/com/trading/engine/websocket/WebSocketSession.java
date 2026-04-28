package com.trading.engine.websocket;

import io.netty.channel.Channel;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-client WebSocket session state. Created on successful authentication, held for the grace
 * period after disconnect, and destroyed on expiry. Stores JWT {@code jti} for revocation tracking,
 * a per-session {@link SubscriptionFilter} for event delivery filtering, and an entitled accounts
 * set from {@link UserEntitlementService} for account-level drain-path filtering.
 *
 * <p><b>Thread safety.</b> Owned by the Netty event loop thread. Not shared across threads. The
 * AeronEgressThread writes to the {@link org.agrona.concurrent.ManyToOneConcurrentArrayQueue} and
 * the drain handler reads session state — but the session object itself is only accessed from the
 * Netty thread. Cross-thread fields ({@link #pendingBytesRef}, {@link #replayInProgress}, {@link
 * #dropBestEffort}) use volatile or atomic semantics where the slow-consumer scan loop (running on
 * a different worker loop) reads them.
 *
 * <p><b>Allocation.</b> One-time allocation per session. UUID generated via {@code
 * UUID.randomUUID()} (acceptable — WebSocket server is not a cluster service).
 *
 * @see <a href="docs/websocket-architecture.md">WebSocket Architecture — Section 3</a>
 */
public final class WebSocketSession {

  private final UUID sessionId;
  private final Channel channel;
  private final String remoteIp;
  private String userId;
  private String jti;

  /**
   * The {@code jti} captured at the FIRST successful auth on this session. Unlike {@link #jti},
   * this field is NOT updated by {@code handleReAuth} — it preserves the original login token
   * across re-auth so {@code SessionResume} can validate that the original login has not been
   * revoked, even if a subsequent re-auth has rotated the current token. Cleared only when the
   * session is removed by {@link WebSocketSessionManager#removeSession}.
   */
  private String originalAuthJti;

  // Volatile: written once by channel event loop at auth time, read by drain handler event loop
  // via matches() call. SubscriptionFilter's internal volatile snapshot handles per-mutation
  // visibility; this volatile ensures the drain handler sees the non-null reference after init.
  private volatile SubscriptionFilter subscriptionFilter;
  // Volatile: written by channel event loop at auth/re-auth time, read by drain handler event loop
  // during account entitlement checks via AccountExtractor.
  private volatile Set<String> entitledAccounts = Set.of();
  // Packed representation of entitledAccounts for zero-allocation drain-path comparison.
  // Format: [high0, low0, high1, low1, ...] — pairs of longs per account code.
  // Volatile: published after entitledAccounts above (two separate volatile stores, not atomic;
  // safe because no single code path reads both fields — Set<String> is used for cold-path
  // String lookups, long[] is used exclusively by isEntitledAccount on the drain hot path).
  private static final long[] EMPTY_PACKED_ACCOUNTS = new long[0];

  private volatile long[] packedEntitledAccounts = EMPTY_PACKED_ACCOUNTS;

  /**
   * Reliable-stream sequence counter. {@link AtomicLong} because writers come from two threads: (a)
   * {@code WebSocketDrainHandler} (worker event loop hosting the drain task) when fanning live
   * cluster events into reliable frames, and (b) {@code CommandDispatcher} (channel's own worker
   * event loop) when emitting {@code CommandAck} as a reliable frame. These workers may differ — a
   * plain {@code long} {@code ++} is not atomic across threads and would silently produce duplicate
   * or skipped sequence numbers (Gemini PR #62 round 2).
   */
  private final AtomicLong reliableSeqCounter = new AtomicLong();

  private long lastClientCmdSeqNo;
  private long lastClientHeartbeatNs;

  /**
   * Volatile so the SlowConsumerHandler scan loop (different worker event loop) can read it without
   * crossing memory barriers.
   */
  private volatile boolean replayInProgress;

  private long gracePeriodStartNs;
  private boolean disconnected;

  /**
   * Reliable-stream replay buffer. Allocated lazily via {@link #initReliableStreamTracker(int, int,
   * WebSocketMetrics)} after auth (same call site as {@link #initSubscriptionFilter}). May be
   * {@code null} if the session is not yet authenticated.
   */
  private ReliableStreamTracker reliableStreamTracker;

  // --- SlowConsumerHandler state (read on the slow-consumer scan loop, written by the channel
  //     event loop). All volatile so the scan loop sees the most recent values without locks.
  /**
   * Reference to the {@link WriteByteCounterHandler}'s {@link AtomicLong} pendingBytes counter.
   * Volatile because the SlowConsumerHandler scan loop runs on the drain worker event loop and
   * reads this from a different Netty worker thread than the one that owns the channel.
   */
  private volatile AtomicLong pendingBytesRef;

  /**
   * Slow-consumer level last entered by this session (0 = clear, 1-4 = level ladder). Volatile
   * because the slow-consumer scan loop reads/writes this and the drain hot path reads it (via
   * {@link #isDropBestEffort()}).
   */
  private volatile int lastLagLevel;

  /** Monotonic nanos when the current lag level was entered (for level-4 disconnect timer). */
  private volatile long levelEnteredNs;

  /**
   * When {@code true}, the drain handler MUST drop best-effort messages for this session even if
   * {@code ch.isWritable()} would otherwise permit them. Set by SlowConsumerHandler on level-2
   * entry; cleared on transition back to level 0/1.
   */
  private volatile boolean dropBestEffort;

  /**
   * When {@code true}, the level-3 SlowConsumer error frame still needs to be sent for the current
   * L3 dwell window. Set by SlowConsumerHandler when entering L3 while replay is in progress (the
   * error is suppressed during replay so the new session is not flagged for the legitimate
   * replay-induced backlog). Cleared once the error has been sent or the level falls below 3
   * (Gemini PR #62 round 2).
   */
  private volatile boolean slowConsumerErrorPending;

  /**
   * Create a new session for an authenticated client.
   *
   * @param channel the Netty channel for this client
   * @param nowNs current monotonic time in nanoseconds (for heartbeat tracking)
   * @param remoteIp the remote IP address captured at registration time; stored so that it remains
   *     available after the channel disconnects (when {@code channel.remoteAddress()} returns null)
   */
  public WebSocketSession(final Channel channel, final long nowNs, final String remoteIp) {
    this.sessionId = UUID.randomUUID();
    this.channel = Objects.requireNonNull(channel, "channel");
    this.remoteIp = Objects.requireNonNull(remoteIp, "remoteIp");
    this.lastClientHeartbeatNs = nowNs;
  }

  /**
   * @return the unique session identifier
   */
  public UUID sessionId() {
    return sessionId;
  }

  /**
   * @return the Netty channel for this client
   */
  public Channel channel() {
    return channel;
  }

  /**
   * @return the remote IP address captured at registration time (remains valid after disconnect)
   */
  public String remoteIp() {
    return remoteIp;
  }

  /**
   * @return the authenticated user identifier (JWT {@code sub} claim)
   */
  public String userId() {
    return userId;
  }

  /**
   * @param userId the authenticated user identifier
   */
  public void userId(final String userId) {
    this.userId = userId;
  }

  /**
   * @return the JWT {@code jti} claim (full string for collision-resistant revocation tracking)
   */
  public String jti() {
    return jti;
  }

  /**
   * @param jti the JWT jti claim (full string, not a hash)
   */
  public void jti(final String jti) {
    this.jti = jti;
  }

  /**
   * @return the {@code jti} captured at the FIRST successful auth, or {@code null} if not yet
   *     captured. Unlike {@link #jti()}, this value does NOT roll on re-auth — it is the binding
   *     used by {@code SessionResume} to verify the original login has not been revoked.
   */
  public String originalAuthJti() {
    return originalAuthJti;
  }

  /**
   * Set the original-auth jti. Idempotent — if already set, the call is a no-op (subsequent
   * re-auths must NOT overwrite this field). Cleared only on {@link
   * WebSocketSessionManager#removeSession}.
   *
   * @param jti the JWT jti from the FIRST successful auth on this session
   */
  public void originalAuthJti(final String jti) {
    if (this.originalAuthJti == null) {
      this.originalAuthJti = jti;
    }
  }

  /**
   * Clear the original-auth jti. Called by the session manager during removal so a new session on a
   * recycled channel starts from a clean state.
   */
  public void clearOriginalAuthJti() {
    this.originalAuthJti = null;
  }

  /**
   * @return the subscription filter for this session, or null if not yet initialized (pre-auth)
   */
  public SubscriptionFilter subscriptionFilter() {
    return subscriptionFilter;
  }

  /**
   * Initialize the subscription filter after successful authentication.
   *
   * @param maxSubscriptions the maximum number of symbol subscriptions allowed per session
   */
  public void initSubscriptionFilter(final int maxSubscriptions) {
    this.subscriptionFilter = new SubscriptionFilter(maxSubscriptions);
  }

  /**
   * Initialize the reliable-stream tracker after successful authentication. Idempotent: if a
   * tracker is already installed (e.g., on session resume), this method is a no-op.
   *
   * @param capacity number of frame slots; must be a positive power of two
   * @param frameSize size of each slot in bytes; must be {@code > 16}
   * @param metrics metrics instance for replay/eviction counters
   */
  public void initReliableStreamTracker(
      final int capacity, final int frameSize, final WebSocketMetrics metrics) {
    if (this.reliableStreamTracker == null) {
      this.reliableStreamTracker = new ReliableStreamTracker(capacity, frameSize, metrics);
    }
  }

  /**
   * @return the reliable-stream tracker, or {@code null} if not initialized (pre-auth)
   */
  public ReliableStreamTracker reliableStreamTracker() {
    return reliableStreamTracker;
  }

  /**
   * @return the set of account codes this session is entitled to access (from JWT accounts claim)
   */
  public Set<String> entitledAccounts() {
    return entitledAccounts;
  }

  /**
   * Set the entitled account codes after validation by {@link UserEntitlementService}. Also builds
   * the packed {@code long[]} representation for zero-allocation drain-path comparison via {@link
   * #isEntitledAccount(long, long)}.
   *
   * @param entitledAccounts unmodifiable set of validated active account codes
   */
  public void entitledAccounts(final Set<String> entitledAccounts) {
    this.entitledAccounts = Objects.requireNonNull(entitledAccounts, "entitledAccounts");

    // Build packed representation: 2 longs per account (high + low)
    final var packed = new long[entitledAccounts.size() * 2];
    final var buf = new long[2];
    int idx = 0;
    for (final var code : entitledAccounts) {
      AccountPacker.pack(code, buf);
      packed[idx++] = buf[0];
      packed[idx++] = buf[1];
    }
    this.packedEntitledAccounts = packed;
  }

  /**
   * Zero-allocation account entitlement check for the drain hot path. Compares the packed account
   * code against all entitled accounts using linear scan over packed {@code long} pairs.
   *
   * <p>Linear scan is optimal for 1-4 accounts (2-8 long comparisons) — faster than hash-based
   * lookup due to no hashing overhead and L1 cache locality.
   *
   * @param high the packed high half (bytes 0-7) from {@link AccountPacker#packHigh}
   * @param low the packed low half (bytes 8-15) from {@link AccountPacker#packLow}
   * @return {@code true} if the account is in the entitled set
   */
  public boolean isEntitledAccount(final long high, final long low) {
    final var packed = this.packedEntitledAccounts; // single volatile read
    for (int i = 0; i < packed.length; i += 2) {
      if (packed[i] == high && packed[i + 1] == low) {
        return true;
      }
    }
    return false;
  }

  /**
   * Assign the next reliable sequence number for outbound messages. Atomic — safe under concurrent
   * calls from the drain thread (live egress fan-out) and the channel thread (CommandAck emission).
   *
   * @return the next sequence number (pre-increment — starts at 1)
   */
  public long nextReliableSeqNo() {
    return reliableSeqCounter.incrementAndGet();
  }

  /**
   * @return the current reliable sequence counter (last assigned). Read with the same memory
   *     ordering guarantees as {@link AtomicLong#get} so cross-thread observers see the latest
   *     committed value (no torn reads of the {@code long}).
   */
  public long reliableSeqCounter() {
    return reliableSeqCounter.get();
  }

  /**
   * @return the last client command sequence number received
   */
  public long lastClientCmdSeqNo() {
    return lastClientCmdSeqNo;
  }

  /**
   * @param seqNo the client command sequence number
   */
  public void lastClientCmdSeqNo(final long seqNo) {
    this.lastClientCmdSeqNo = seqNo;
  }

  /**
   * @return the monotonic timestamp of the last client heartbeat
   */
  public long lastClientHeartbeatNs() {
    return lastClientHeartbeatNs;
  }

  /**
   * @param nowNs the current monotonic time
   */
  public void updateHeartbeat(final long nowNs) {
    this.lastClientHeartbeatNs = nowNs;
  }

  /**
   * @return true if a gap replay is in progress for this session
   */
  public boolean isReplayInProgress() {
    return replayInProgress;
  }

  /**
   * @param replayInProgress true to pause live delivery during gap replay
   */
  public void replayInProgress(final boolean replayInProgress) {
    this.replayInProgress = replayInProgress;
  }

  /**
   * @return true if the client has disconnected (grace period may still be active)
   */
  public boolean isDisconnected() {
    return disconnected;
  }

  /**
   * Mark this session as disconnected and start the grace period.
   *
   * @param nowNs current monotonic time
   */
  public void markDisconnected(final long nowNs) {
    this.disconnected = true;
    this.gracePeriodStartNs = nowNs;
    // Architecture doc: grace period does NOT hold subscriptions or entitlements.
    // Clear to stop receiving events during grace period.
    if (subscriptionFilter != null) {
      subscriptionFilter.clear();
    }
    entitledAccounts = Set.of();
    packedEntitledAccounts = EMPTY_PACKED_ACCOUNTS;
  }

  /**
   * @return the monotonic timestamp when the grace period started
   */
  public long gracePeriodStartNs() {
    return gracePeriodStartNs;
  }

  // --- Slow-consumer state -------------------------------------------------

  /**
   * Install the {@link AtomicLong} byte counter that the {@link WriteByteCounterHandler} updates.
   * Called by {@link WebSocketServerMain} during pipeline assembly.
   *
   * @param ref the live byte-counter reference
   */
  public void pendingBytesRef(final AtomicLong ref) {
    this.pendingBytesRef = ref;
  }

  /**
   * @return the current pending bytes (Netty outbound queue depth), or {@code 0} if no counter is
   *     installed yet
   */
  public long pendingBytes() {
    final var ref = this.pendingBytesRef;
    return ref == null ? 0L : ref.get();
  }

  /**
   * @return the last-classified slow-consumer level (0 = clear, 1-4 = ladder)
   */
  public int lastLagLevel() {
    return lastLagLevel;
  }

  /**
   * @param level the slow-consumer level just entered
   * @param nowNs the monotonic time at which the level was entered
   */
  public void recordLagLevel(final int level, final long nowNs) {
    this.lastLagLevel = level;
    this.levelEnteredNs = nowNs;
  }

  /**
   * @return the monotonic time the current lag level was entered
   */
  public long levelEnteredNs() {
    return levelEnteredNs;
  }

  /**
   * @return true if best-effort messages must be dropped for this session
   */
  public boolean isDropBestEffort() {
    return dropBestEffort;
  }

  /**
   * @param drop true to start dropping best-effort messages, false to resume normal delivery
   */
  public void dropBestEffort(final boolean drop) {
    this.dropBestEffort = drop;
  }

  /**
   * @return true if a level-3 SlowConsumer error frame is queued for delivery once replay finishes.
   *     See {@link #slowConsumerErrorPending}.
   */
  public boolean isSlowConsumerErrorPending() {
    return slowConsumerErrorPending;
  }

  /**
   * @param pending true to mark a deferred level-3 SlowConsumer error; false to clear it (after
   *     send or after dropping below level 3)
   */
  public void slowConsumerErrorPending(final boolean pending) {
    this.slowConsumerErrorPending = pending;
  }
}
