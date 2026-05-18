package com.trading.engine.websocket;

import com.trading.engine.messages.MarketDataConstants;
import io.netty.channel.Channel;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.agrona.collections.LongHashSet;

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
   * Monotonically-increasing session epoch — bumped on every {@link #resume()} so the drain handler
   * can detect and discard egress entries captured before the resume. Cross-thread writes use
   * {@link VarHandle#getAndAdd(Object...)} + {@link VarHandle#releaseFence()} to give the drain
   * thread a guaranteed happens-before edge on the new epoch (plain {@code long++} is NOT atomic on
   * 32-bit JVMs and lacks the cross-thread visibility a compliance reviewer demands). The drain
   * handler reads via {@link VarHandle#acquireFence()} + volatile read and compares against each
   * {@link EgressEntry}'s captured epoch — entries whose epoch no longer matches the session's
   * current epoch increment {@code egress.dropped.stale-epoch} and are dropped.
   *
   * <p>Declared as a primitive {@code long} (not {@code int}) for uniformity with the reliable
   * seq-no discipline; matches the ICE Impact / LMAX exchange-core 64-bit session-epoch convention.
   * Initial value zero; first resume bumps to one.
   */
  @SuppressWarnings("unused") // accessed via VAR_HANDLE_SESSION_EPOCH
  private volatile long sessionEpoch;

  /**
   * JWT {@code exp} claim converted to epoch-nanos at auth time. {@code 0L} until {@link
   * #expEpochNanos(long)} is called. Compared against the injected {@link
   * io.aeron.archive.client.AeronArchive AeronArchive}-grade {@link
   * org.agrona.concurrent.EpochNanoClock} on each {@link JwtExpirySweeper#scan(long)} tick (every
   * ~1 s). Volatile because the sweeper runs on a different worker event loop than the channel that
   * set the value at auth time.
   *
   * <p>Plan §Commit 8 + Gemini iter-4: nano-precision comparison — never truncating integer
   * division to epoch-seconds (would otherwise create a worst-case 999 ms window of accepted
   * expired tokens; CME iLink / EBS Direct fail closed at sub-millisecond precision). Stored as
   * {@code long} (epoch-ns since 1970-01-01); the conversion from the RFC 7519 {@code exp}
   * (epoch-seconds) happens once in {@link JwtAuthHandler#continueAuthOnEventLoop} as {@code expSec
   * * 1_000_000_000L}.
   */
  private volatile long expEpochNanos;

  /**
   * Latch flipped on the first {@code AuthExpiringSoon} warning so subsequent ticks within the
   * warning window do NOT spam the client. Reset path: never (the next reauth replaces this session
   * entirely via {@link JwtAuthHandler#continueAuthOnEventLoop}'s re-registration; this session
   * object is discarded). Volatile for the same cross-loop visibility reason as {@link
   * #expEpochNanos}.
   */
  private volatile boolean expiringWarningSent;

  /**
   * {@link VarHandle} on {@link #sessionEpoch} — bound once per class load. The compile-time check
   * is enforced by {@code MethodHandles.lookup().findVarHandle(...)} which throws on a missing
   * field. Used for {@code getAndAdd}/{@code releaseFence}/{@code acquireFence} discipline around
   * the volatile {@code sessionEpoch} field.
   */
  private static final VarHandle VAR_HANDLE_SESSION_EPOCH;

  static {
    try {
      VAR_HANDLE_SESSION_EPOCH =
          MethodHandles.lookup().findVarHandle(WebSocketSession.class, "sessionEpoch", long.class);
    } catch (final ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  /**
   * Per-session snapshot-request token bucket state — Phase 3 Commit 5 continuation.
   *
   * <p>Two-element primitive array: {@code [0] = tokens (long)}, {@code [1] = lastRefillNanos
   * (long)}. Bucket capacity equals {@link
   * MarketDataConstants#MARKET_DATA_SNAPSHOT_REQUESTS_PER_SECOND} (=10); refill rate is uniform at
   * the same value per wall-second. Enforced by {@code WebSocketFrameDispatcher.handleSubscribe} on
   * every newly-added subscription bit so a buggy or hostile client cannot drown the publisher with
   * snapshot requests. CME MDP 3.0 §Channel Recovery rate-limit discipline.
   *
   * <p>Initialised lazily by {@link #initSnapshotTokenBucket(long)} at session-auth time (same call
   * site as {@link #initSubscriptionFilter}); remains {@code null} on the pre-auth path. Primitive
   * {@code long[]} avoids the {@code AtomicLong} cross-thread-CAS overhead — the channel's own
   * Netty event loop is the sole writer + reader of this field, so no synchronisation is needed.
   */
  private long[] snapshotTokenBucket;

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
   * @return the JWT {@code exp} claim as epoch-nanos, or {@code 0L} if not set (pre-auth).
   */
  public long expEpochNanos() {
    return this.expEpochNanos;
  }

  /**
   * Set the JWT {@code exp} claim as epoch-nanos. Called once per successful auth / re-auth from
   * {@link JwtAuthHandler#continueAuthOnEventLoop} after the claims have been validated. Resets
   * {@link #expiringWarningSent} so a renewed token gets a fresh warning window (matches the prior
   * behavior where re-auth re-arms the sweeper).
   *
   * @param expEpochNanos JWT {@code exp} converted to epoch-nanos via {@code
   *     claims.expiryEpochSec() * 1_000_000_000L}; MUST be {@code > 0}
   */
  public void expEpochNanos(final long expEpochNanos) {
    if (expEpochNanos <= 0L) {
      throw new IllegalArgumentException("expEpochNanos must be > 0; got " + expEpochNanos);
    }
    this.expEpochNanos = expEpochNanos;
    this.expiringWarningSent = false;
  }

  /**
   * @return {@code true} iff the {@code AuthExpiringSoon} warning has already been emitted for the
   *     current token (set by {@link JwtExpirySweeper#scan(long)} on first crossing of the warn
   *     boundary).
   */
  public boolean expiringWarningSent() {
    return this.expiringWarningSent;
  }

  /**
   * Flip the {@link #expiringWarningSent} latch. Called by {@link JwtExpirySweeper} on the first
   * warn-window-crossing tick so subsequent ticks within the window do not spam the client.
   * Idempotent — a second call is a no-op (no observable difference).
   */
  public void markExpiringWarningSent() {
    this.expiringWarningSent = true;
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
   * Initialize the subscription filter after successful authentication. Metrics sink required
   * (non-null) so the entitlement-denied counter increments whenever the per-account guard rejects
   * a symbol not in the published entitlement set.
   *
   * @param maxSubscriptions the maximum number of symbol subscriptions allowed per session
   * @param metrics metrics sink for the entitlement-denied counter; required
   */
  public void initSubscriptionFilter(final int maxSubscriptions, final WebSocketMetrics metrics) {
    this.subscriptionFilter = new SubscriptionFilter(maxSubscriptions, metrics);
  }

  /**
   * Publish a fresh per-account symbol entitlement set on the session's {@link SubscriptionFilter}
   * — Phase 3 Commit A construct-then-publish discipline. Allocates a NEW {@link LongHashSet} sized
   * for the per-tenant cap of 32 symbols (capacity 64 with headroom — guarantees no rehash during
   * populate), unions the entitled-symbols sets for every account code in {@code accountCodes},
   * then volatile-assigns the populated set via {@link
   * SubscriptionFilter#publishEntitledSymbols(LongHashSet)}.
   *
   * <p>Called from the auth / resume path. Idempotent across resume — each invocation publishes a
   * fresh instance; the previous set becomes garbage. Pre-conditions: {@link
   * #initSubscriptionFilter(int, WebSocketMetrics)} has been called.
   *
   * @param entitlementMap source of {@code account → entitled-packed-symbols} mappings, loaded by
   *     the launcher from {@code symbols.yaml}
   * @param accountCodes the validated account codes from the JWT {@code accounts} claim — typically
   *     the same {@link Set} returned by {@link UserEntitlementService#validateAccounts(Set)}
   * @throws IllegalStateException if the subscription filter has not been initialised
   */
  public void publishSymbolEntitlements(
      final SymbolEntitlementMap entitlementMap, final Set<String> accountCodes) {
    if (this.subscriptionFilter == null) {
      throw new IllegalStateException(
          "subscription filter not initialised — call initSubscriptionFilter(...) at auth");
    }
    Objects.requireNonNull(entitlementMap, "entitlementMap");
    Objects.requireNonNull(accountCodes, "accountCodes");
    // Initial capacity 64 — covers the per-tenant max of 32 symbols with headroom so no rehash
    // happens during the populate loop (rehash would allocate; CME MDP §Channel Recovery
    // discipline).
    // Agent A review F-1: SymbolEntitlementMap.entitledSymbolsFor returns a primitive
    // LongHashSet, so the union loop uses the Agrona primitive addAll(LongHashSet) path — zero
    // Long boxing across the auth/resume traversal. Reconnect storms iterate this hot enough
    // that the prior ObjectHashSet<Long> for-each (which allocated an Iterator<Long> + a boxed
    // Long per symbol) was a measurable per-connection cost.
    final var fresh = new LongHashSet(64);
    for (final var accountCode : accountCodes) {
      final var perAccount = entitlementMap.entitledSymbolsFor(accountCode);
      fresh.addAll(perAccount);
    }
    this.subscriptionFilter.publishEntitledSymbols(fresh);
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
   * Initialise the per-session snapshot-request token bucket after successful authentication.
   * Allocates a 2-element primitive {@code long[]} carrying {@code [tokens, lastRefillNanos]}.
   * Idempotent — re-init on session resume keeps the existing bucket state so a session that just
   * exhausted its tokens cannot escape the rate-limit by reconnecting.
   *
   * <p>Phase 3 Commit 5 continuation. Called from the session's authenticated-init path alongside
   * {@link #initSubscriptionFilter(int)} and {@link #initReliableStreamTracker(int, int,
   * WebSocketMetrics)}.
   *
   * @param nowNanos current monotonic-clock nanos at session auth. Seeds the bucket's {@code
   *     lastRefillNanos} so the first refill is timed from this anchor.
   */
  public void initSnapshotTokenBucket(final long nowNanos) {
    if (this.snapshotTokenBucket == null) {
      this.snapshotTokenBucket =
          new long[] {MarketDataConstants.MARKET_DATA_SNAPSHOT_REQUESTS_PER_SECOND, nowNanos};
    }
  }

  /**
   * Attempts to consume one token from the snapshot-request bucket. Refills lazily based on the
   * elapsed time since {@code lastRefillNanos}; if the refilled bucket has at least one token,
   * decrements and returns {@code true}. Otherwise leaves the bucket unchanged (the token was NOT
   * consumed) and returns {@code false}, signalling the dispatcher to reject the snapshot request
   * with {@code WebSocketError code=429 SnapshotThrottled}.
   *
   * <p>Refill math: {@code tokensToAdd = floor(elapsedNs × CAPACITY / SECOND_NS)}; the bucket is
   * capped at CAPACITY so a long idle period does not produce a burst above the steady-state rate
   * (token-bucket leaky-refill contract). The {@code lastRefillNanos} cursor advances by the exact
   * ns budget consumed by the refill — preserves fractional-second precision across many consume
   * calls.
   *
   * <p>Threading: the channel's own Netty event loop is the sole reader/writer; no synchronisation.
   * Allocation: zero (primitive {@code long[]} mutation in place).
   *
   * @param nowNanos current monotonic-clock nanos at consume time.
   * @return {@code true} if a token was consumed, {@code false} if the bucket is empty.
   * @throws IllegalStateException if the bucket has not been initialised via {@link
   *     #initSnapshotTokenBucket(long)}.
   */
  public boolean tryConsumeSnapshotToken(final long nowNanos) {
    if (this.snapshotTokenBucket == null) {
      throw new IllegalStateException(
          "snapshotTokenBucket not initialised — call initSnapshotTokenBucket(nowNanos) at auth");
    }
    final long capacity = MarketDataConstants.MARKET_DATA_SNAPSHOT_REQUESTS_PER_SECOND;
    final long secondNs = 1_000_000_000L;
    final long rawElapsedNs = nowNanos - this.snapshotTokenBucket[1];
    // Agent B review F-9: cap elapsedNs so (elapsedNs * capacity) cannot overflow a signed long.
    // The maximum useful elapsed time for a full-bucket refill is exactly (capacity × secondNs)
    // — anything beyond fills to capacity anyway and the leftover would just be discarded by the
    // cap below. Clamping here also defends against a session that idled for >15 minutes (which
    // would otherwise overflow with capacity=10 since Long.MAX_VALUE / 10 ≈ 922 seconds).
    final long maxUsefulElapsedNs = capacity * secondNs;
    final long elapsedNs = rawElapsedNs > maxUsefulElapsedNs ? maxUsefulElapsedNs : rawElapsedNs;
    if (elapsedNs > 0L) {
      final long tokensToAdd = (elapsedNs * capacity) / secondNs;
      if (tokensToAdd > 0L) {
        // Advance the refill cursor. Two paths (Agent B review F-5: separate the branches so
        // the unclamped `+=` is not dead-computed and then overwritten in the clamped path):
        //   1. Clamped (session idled past the maxUsefulElapsedNs window) — snap the cursor to
        //      nowNanos. The bucket is at capacity already and there is no fractional remainder
        //      worth preserving.
        //   2. Unclamped (steady-state) — advance by exactly the ns consumed by the integer-
        //      token refill so the fractional remainder carries over to the next consume call.
        if (rawElapsedNs > maxUsefulElapsedNs) {
          this.snapshotTokenBucket[1] = nowNanos;
        } else {
          this.snapshotTokenBucket[1] += (tokensToAdd * secondNs) / capacity;
        }
        final long newTokens = this.snapshotTokenBucket[0] + tokensToAdd;
        this.snapshotTokenBucket[0] = newTokens > capacity ? capacity : newTokens;
      }
    }
    if (this.snapshotTokenBucket[0] > 0L) {
      this.snapshotTokenBucket[0]--;
      return true;
    }
    return false;
  }

  /**
   * Refunds one token to the snapshot-request bucket. Called by the dispatcher when a
   * snapshot-request publish on stream 205 fails (e.g. Aeron offer returned BACK_PRESSURED /
   * NOT_CONNECTED / ADMIN_ACTION) — the client's token was consumed at admission but the request
   * never reached the publisher, so refunding lets the client retry without double-penalisation.
   * NEVER called on a successful publish (the publisher consumed a slot — the rate-limit accounting
   * must reflect that work).
   *
   * <p>Caps at the configured bucket capacity so a runaway refund cannot exceed steady-state rate.
   *
   * @throws IllegalStateException if the bucket has not been initialised.
   */
  public void refundSnapshotToken() {
    if (this.snapshotTokenBucket == null) {
      throw new IllegalStateException("snapshotTokenBucket not initialised");
    }
    final long capacity = MarketDataConstants.MARKET_DATA_SNAPSHOT_REQUESTS_PER_SECOND;
    final long refunded = this.snapshotTokenBucket[0] + 1L;
    this.snapshotTokenBucket[0] = refunded > capacity ? capacity : refunded;
  }

  /**
   * Returns the current number of tokens in the snapshot bucket without consuming or refilling.
   * Intended for test assertions and Micrometer gauge sampling only; production code should call
   * {@link #tryConsumeSnapshotToken(long)} which refills lazily.
   *
   * @return current token count; {@code -1L} if the bucket has not been initialised.
   */
  public long snapshotTokensAvailable() {
    return this.snapshotTokenBucket == null ? -1L : this.snapshotTokenBucket[0];
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

  /**
   * Bump the session epoch on resume. Ordering is load-bearing — the side-effects that must be
   * observable to the drain thread happen BEFORE the {@link VarHandle#releaseFence()} so the fence
   * publishes them as a unit:
   *
   * <ol>
   *   <li>Clear the per-session subscription bitmap so stale prior-session subscriptions cannot
   *       survive into the resumed session.
   *   <li>Atomically increment the {@code sessionEpoch} via {@link VarHandle#getAndAdd(Object...)}.
   *   <li>Issue {@link VarHandle#releaseFence()} — this is the publish boundary.
   * </ol>
   *
   * <p>A drain thread that issues {@link VarHandle#acquireFence()} and reads {@link
   * #currentEpoch()} and observes the new epoch is guaranteed (per the JMM happens-before edge
   * established by the matching release/acquire fences) to also observe the cleared subscription
   * filter — the fence cannot reorder writes that precede it. The previous ordering (fence then
   * clear) was racy: the drain thread could observe the new epoch but still see the pre-clear
   * subscription snapshot in the window between {@code getAndAdd} and {@code clear()}.
   *
   * <p>Must be called BEFORE the resume thread re-publishes the session reference to the drain
   * handler — the release fence anchors the cross-thread happens-before edge.
   *
   * @return the post-increment epoch value
   */
  public long resume() {
    // Clear the subscription filter FIRST so the cleared state is part of the data the subsequent
    // release fence publishes (Agent B review F-1).
    final var filter = this.subscriptionFilter;
    if (filter != null) {
      filter.clear();
    }
    final long newEpoch = (long) VAR_HANDLE_SESSION_EPOCH.getAndAdd(this, 1L) + 1L;
    VarHandle.releaseFence();
    return newEpoch;
  }

  /**
   * Returns the current session epoch. Drain handler MUST issue {@link VarHandle#acquireFence()}
   * before reading this value (the acquire fence pairs with the {@link #resume()} release fence to
   * give a guaranteed happens-before edge). The value is a {@code volatile long} read so the read
   * is atomic on all JVMs (including 32-bit) and observes the most recent {@link #resume()} value.
   *
   * @return the current session epoch
   */
  public long currentEpoch() {
    VarHandle.acquireFence();
    return sessionEpoch; // volatile read
  }
}
