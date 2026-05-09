package com.trading.engine.cluster.state;

import com.trading.engine.cluster.handler.EventSink;
import com.trading.engine.cluster.handler.RfqRejectMessages;
import com.trading.engine.cluster.metrics.RfqMetrics;
import com.trading.engine.cluster.refdata.AccountState;
import com.trading.engine.cluster.refdata.AccountStore;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.ProductTypeEnum;
import com.trading.engine.messages.sbe.QuoteExpiredEventEncoder;
import com.trading.engine.messages.sbe.QuoteRejectReasonEnum;
import com.trading.engine.messages.sbe.QuoteRejectedEventEncoder;
import com.trading.engine.messages.sbe.RfqStateEnum;
import com.trading.engine.messages.sbe.RfqStateSnapshotDecoder;
import com.trading.engine.messages.sbe.RfqStateSnapshotEncoder;
import com.trading.engine.messages.sbe.SettlTypeEnum;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.messages.sbe.TenorEnum;
import com.trading.engine.messages.util.ByteArrayKey;
import io.aeron.cluster.service.Cluster;
import java.util.Objects;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.Object2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Cluster-side, deterministic, single-threaded RFQ slot pool. Owns the lifecycle of every in-flight
 * Request-for-Quote, drives TTL via {@link Cluster#scheduleTimer}, snapshots state via SBE template
 * 203 ({@code RfqStateSnapshot}), and emits {@code QuoteExpiredEvent} (107) directly on timer
 * expiry.
 *
 * <p><b>Lifecycle (slot states — see {@link RfqSlotState}):</b>
 *
 * <pre>
 * FREE → REQUESTED   (acquire — QuoteRequest accepted, 104 emitted by QuoteRequestHandler)
 * REQUESTED → QUOTED (PriceResponse accepted, 105 emitted, TTL timer scheduled)
 * REQUESTED → FREE   (PriceResponse declined or request-timeout fired → 106 + release)
 * QUOTED → ACCEPTED  ({@link #commitAccept} — NOS-with-quoteId validated, no event emitted here)
 * QUOTED → FREE      ({@link #onTimerExpiry} TTL fired → 107 + release)
 * ACCEPTED → FREE    (snapshot recovery only; in steady state the transition is atomic)
 * </pre>
 *
 * <p><b>Lookup maps (Agrona, pre-sized, zero-alloc):</b>
 *
 * <ul>
 *   <li>{@code byCorrelationId} — for {@link #onTimerExpiry}. Both TTL and request-timeout
 *       namespace share one map (high-bit on the correlation ID disambiguates).
 *   <li>{@code byQuoteReqId} — for PriceResponse correlation and duplicate detection.
 *   <li>{@code byQuoteId} — for §9.2a NOS-with-quoteId acceptance via {@link #peekByQuoteId} +
 *       {@link #commitAccept}.
 * </ul>
 *
 * <p><b>Determinism:</b> all clock reads are cluster timestamps from {@code onSessionMessage} /
 * {@code onTimerEvent}; all arithmetic is integer; all collections are Agrona (no {@code
 * java.util.*}). The snapshot encoder iterates slots in ascending pool-index order to guarantee
 * byte-identical snapshots across replicas (Raft consistency invariant).
 *
 * <p><b>Threading:</b> not thread-safe — single-threaded cluster duty cycle only.
 *
 * <p><b>Allocation:</b> zero allocation after construction. Slot array, free list, lookup maps,
 * recently-terminal LRU ring, token-bucket pool, and timer-driven 107 encode buffer are all
 * pre-allocated.
 *
 * @see RfqSlot
 * @see RfqSlotState
 * @see TokenBucket
 * @see com.trading.engine.cluster.handler.EventSink
 */
public final class RfqStateMachine {

  // -------------------------------------------------------------------------
  // Constants
  // -------------------------------------------------------------------------

  /** High-bit marker distinguishing the request-timeout correlation ID namespace from TTL. */
  static final long REQUEST_TIMEOUT_NAMESPACE_BIT = 0x8000_0000_0000_0000L;

  /** Generation counter retirement threshold (~1.07 billion reuses). */
  static final int GENERATION_RETIREMENT_THRESHOLD = Integer.MAX_VALUE >> 1;

  /** Egress buffer for the timer-driven 107 emission path. Sized for max event ≈ 280 bytes. */
  static final int EXPIRED_EGRESS_BUFFER_SIZE = 8192;

  /** Egress buffer for the timer-driven 106 (request-timeout) emission path. */
  static final int REJECTED_EGRESS_BUFFER_SIZE = 8192;

  /** Recently-terminal LRU ring capacity. Covers ~10s of peak burst at 100/s. */
  static final int RECENTLY_TERMINAL_CAPACITY = 1024;

  /** {@code recentlyTerminal} reason byte — slot expired via TTL. */
  static final byte TERMINAL_REASON_EXPIRED = 1;

  /** {@code recentlyTerminal} reason byte — slot rejected. */
  static final byte TERMINAL_REASON_REJECTED = 2;

  /** {@code recentlyTerminal} reason byte — slot accepted via NOS-with-quoteId. */
  static final byte TERMINAL_REASON_ACCEPTED = 3;

  // -------------------------------------------------------------------------
  // Configuration (immutable after construction)
  // -------------------------------------------------------------------------

  private final int capacity;
  private final long defaultTtlNanos;
  private final long ttlSpotNanos;
  private final long ttlForwardNanos;
  private final long ttlSwapNanos;
  private final long requestTimeoutNanos;
  private final long rateLimitPerSession;
  private final long rateLimitRefillNanosPerToken;
  private final int acceptPriceToleranceBps;
  private final int acceptQtyToleranceBps;

  // -------------------------------------------------------------------------
  // Slot pool + free list
  // -------------------------------------------------------------------------

  private final RfqSlot[] slots;
  private final int[] freeIndices;
  private int freeCount;

  // -------------------------------------------------------------------------
  // Lookup maps
  // -------------------------------------------------------------------------

  private final Long2ObjectHashMap<RfqSlot> byCorrelationId;
  private final Object2ObjectHashMap<ByteArrayKey, RfqSlot> byQuoteReqId;
  private final Object2ObjectHashMap<ByteArrayKey, RfqSlot> byQuoteId;

  // -------------------------------------------------------------------------
  // Rate-limiter pool
  // -------------------------------------------------------------------------

  private final Long2ObjectHashMap<TokenBucket> rateLimitBuckets;
  private final TokenBucket[] tokenBucketPool;
  private final int[] tokenBucketFreeIndices;
  private int tokenBucketFreeCount;

  // -------------------------------------------------------------------------
  // Recently-terminal LRU ring (1024 entries)
  // -------------------------------------------------------------------------

  private final ByteArrayKey[] recentlyTerminalRing;
  private final byte[] recentlyTerminalReason;
  private int recentlyTerminalRingHead;

  /** Probe key for recently-terminal lookups (zero-alloc). */
  private final ByteArrayKey recentlyTerminalProbe;

  /** Probe key for byQuoteReqId lookups (zero-alloc). */
  private final ByteArrayKey byQuoteReqIdProbe;

  /** Probe key for byQuoteId lookups (zero-alloc). */
  private final ByteArrayKey byQuoteIdProbe;

  // -------------------------------------------------------------------------
  // External dependencies
  // -------------------------------------------------------------------------

  private final AccountStore accountStore;
  private final RfqMetrics metrics;
  private Cluster cluster;

  // -------------------------------------------------------------------------
  // Pre-allocated egress buffers + encoders for timer-driven emissions
  // -------------------------------------------------------------------------

  private final UnsafeBuffer expiredEgressBuffer;
  private final QuoteExpiredEventEncoder quoteExpiredEncoder = new QuoteExpiredEventEncoder();
  private final UnsafeBuffer rejectedEgressBuffer;
  private final QuoteRejectedEventEncoder quoteRejectedEncoder = new QuoteRejectedEventEncoder();
  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();

  /** Pre-allocated snapshot encoder reused across every {@link #encodeInto} call. */
  private final RfqStateSnapshotEncoder rfqStateEncoder = new RfqStateSnapshotEncoder();

  /** Pre-allocated snapshot decoder reused across every {@link #restoreFrom} call. */
  private final RfqStateSnapshotDecoder rfqStateDecoder = new RfqStateSnapshotDecoder();

  // -------------------------------------------------------------------------
  // Constructor
  // -------------------------------------------------------------------------

  /**
   * Constructs an {@link RfqStateMachine} with the given configuration and dependencies.
   *
   * @param capacity slot pool capacity (power-of-two, &gt;= 256)
   * @param defaultTtlNanos default RFQ TTL (used when productType is unknown)
   * @param ttlSpotNanos TTL for {@code Spot} product type
   * @param ttlForwardNanos TTL for {@code Forward} product type
   * @param ttlSwapNanos TTL for {@code Swap} product type
   * @param requestTimeoutNanos bound on a REQUESTED slot waiting for PriceResponse
   * @param rateLimitPerSession token-bucket capacity per session
   * @param rateLimitWindowNanos refill window for the token bucket
   * @param acceptPriceToleranceBps tolerance for NOS-with-quoteId price (bps)
   * @param acceptQtyToleranceBps tolerance for NOS-with-quoteId quantity (bps)
   * @param accountStore reference-data accessor for recovery-time accountCode rehydration
   * @param metrics observability counters
   */
  public RfqStateMachine(
      final int capacity,
      final long defaultTtlNanos,
      final long ttlSpotNanos,
      final long ttlForwardNanos,
      final long ttlSwapNanos,
      final long requestTimeoutNanos,
      final long rateLimitPerSession,
      final long rateLimitWindowNanos,
      final int acceptPriceToleranceBps,
      final int acceptQtyToleranceBps,
      final AccountStore accountStore,
      final RfqMetrics metrics) {
    if (capacity < 256 || capacity > 65536 || Integer.bitCount(capacity) != 1) {
      throw new IllegalArgumentException(
          "capacity must be a power of two in [256, 65536], was " + capacity);
    }
    if (rateLimitPerSession <= 0L || rateLimitWindowNanos <= 0L) {
      throw new IllegalArgumentException("rate limit values must be positive");
    }
    this.capacity = capacity;
    this.defaultTtlNanos = defaultTtlNanos;
    this.ttlSpotNanos = ttlSpotNanos;
    this.ttlForwardNanos = ttlForwardNanos;
    this.ttlSwapNanos = ttlSwapNanos;
    this.requestTimeoutNanos = requestTimeoutNanos;
    this.rateLimitPerSession = rateLimitPerSession;
    this.rateLimitRefillNanosPerToken = rateLimitWindowNanos / rateLimitPerSession;
    this.acceptPriceToleranceBps = acceptPriceToleranceBps;
    this.acceptQtyToleranceBps = acceptQtyToleranceBps;
    this.accountStore = Objects.requireNonNull(accountStore, "accountStore");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    this.metrics.poolCapacity = capacity;

    // Slot pool
    this.slots = new RfqSlot[capacity];
    this.freeIndices = new int[capacity];
    for (int i = 0; i < capacity; i++) {
      slots[i] = new RfqSlot(i);
      freeIndices[i] = capacity - 1 - i; // pop low-index slots first
    }
    this.freeCount = capacity;

    // Lookup maps (load factor 0.55 to avoid rehash; pre-size to next power of two)
    final int mapCapacity = nextPowerOfTwo((int) (capacity * 2));
    this.byCorrelationId = new Long2ObjectHashMap<>(mapCapacity, 0.55f);
    this.byQuoteReqId = new Object2ObjectHashMap<>(mapCapacity, 0.55f);
    this.byQuoteId = new Object2ObjectHashMap<>(mapCapacity, 0.55f);

    // Rate-limiter pool
    this.rateLimitBuckets = new Long2ObjectHashMap<>(mapCapacity, 0.55f);
    this.tokenBucketPool = new TokenBucket[capacity];
    this.tokenBucketFreeIndices = new int[capacity];
    for (int i = 0; i < capacity; i++) {
      tokenBucketPool[i] = new TokenBucket(i);
      tokenBucketFreeIndices[i] = capacity - 1 - i;
    }
    this.tokenBucketFreeCount = capacity;

    // Recently-terminal LRU
    this.recentlyTerminalRing = new ByteArrayKey[RECENTLY_TERMINAL_CAPACITY];
    this.recentlyTerminalReason = new byte[RECENTLY_TERMINAL_CAPACITY];
    for (int i = 0; i < RECENTLY_TERMINAL_CAPACITY; i++) {
      recentlyTerminalRing[i] = ByteArrayKey.emptyForLookup(RfqSlot.QUOTE_REQ_ID_LENGTH);
    }
    this.recentlyTerminalRingHead = 0;
    this.recentlyTerminalProbe = ByteArrayKey.emptyForLookup(RfqSlot.QUOTE_REQ_ID_LENGTH);
    this.byQuoteReqIdProbe = ByteArrayKey.emptyForLookup(RfqSlot.QUOTE_REQ_ID_LENGTH);
    this.byQuoteIdProbe = ByteArrayKey.emptyForLookup(RfqSlot.QUOTE_ID_LENGTH);

    // Egress buffers
    this.expiredEgressBuffer = new UnsafeBuffer(new byte[EXPIRED_EGRESS_BUFFER_SIZE]);
    this.rejectedEgressBuffer = new UnsafeBuffer(new byte[REJECTED_EGRESS_BUFFER_SIZE]);
  }

  /**
   * Sets the cluster reference. Called once from {@code TradingClusteredService.onStart()}.
   *
   * @param cluster the Aeron cluster instance
   */
  public void setCluster(final Cluster cluster) {
    this.cluster = cluster;
  }

  // -------------------------------------------------------------------------
  // Slot pool: acquire / release
  // -------------------------------------------------------------------------

  /**
   * Acquires a fresh slot in REQUESTED state for a new {@code QuoteRequest}. The slot's identity
   * fields (quoteReqId etc.) must be populated by the caller AFTER this returns; this method
   * returns a slot whose {@code state == REQUESTED} but whose byte fields are stale from prior use.
   *
   * @return a fresh slot, or {@code null} if the pool is exhausted
   */
  public RfqSlot acquire() {
    if (freeCount == 0) {
      return null;
    }
    final int slotIndex = freeIndices[--freeCount];
    final RfqSlot slot = slots[slotIndex];
    slot.state = RfqSlotState.REQUESTED;
    metrics.poolOccupancy = capacity - freeCount;
    return slot;
  }

  /**
   * Releases the given slot back to the free list, removes it from all three lookup maps, and
   * advances its generation counter so any subsequently-firing stale timer is silently dropped.
   * Must be called BEFORE any byte mutation on the slot.
   *
   * @param slot the slot to release; must not be null
   */
  public void release(final RfqSlot slot) {
    if (slot.state == RfqSlotState.FREE) {
      return; // idempotent
    }
    // Remove from all maps BEFORE byte mutation. The keys still reflect current bytes here.
    if (slot.timerCorrelationId != 0L) {
      byCorrelationId.remove(slot.timerCorrelationId);
    }
    if (slot.requestTimeoutCorrelationId != 0L) {
      byCorrelationId.remove(slot.requestTimeoutCorrelationId);
    }
    byQuoteReqId.remove(slot.quoteReqIdKey);
    // byQuoteId entry exists for QUOTED slots and persists through the transient ACCEPTED
    // state until release. Both states must trigger removal — without the ACCEPTED arm, every
    // commitAccept (which sets state=ACCEPTED before calling release) silently leaks an entry.
    if (slot.state == RfqSlotState.QUOTED || slot.state == RfqSlotState.ACCEPTED) {
      byQuoteId.remove(slot.quoteIdKey);
    }

    // Generation-overflow retirement: when the slot reaches the threshold, do not return it to
    // the free list. Capacity effectively shrinks by one.
    if (slot.generation >= GENERATION_RETIREMENT_THRESHOLD) {
      slot.state = RfqSlotState.FREE;
      metrics.poolRetiredSlots++;
      metrics.poolOccupancy = capacity - freeCount - (int) metrics.poolRetiredSlots;
      return;
    }

    slot.generation++;
    slot.state = RfqSlotState.FREE;
    slot.timerCorrelationId = 0L;
    slot.requestTimeoutCorrelationId = 0L;
    freeIndices[freeCount++] = slot.poolIndex;
    metrics.poolOccupancy = capacity - freeCount;
  }

  // -------------------------------------------------------------------------
  // byQuoteReqId lookup (used by PriceResponseHandler and QuoteRequest dedup)
  // -------------------------------------------------------------------------

  /**
   * Looks up a slot by quoteReqId without mutating state.
   *
   * @param src buffer containing the quoteReqId bytes
   * @param offset start offset into {@code src}
   * @param length number of bytes (must be {@link RfqSlot#QUOTE_REQ_ID_LENGTH})
   * @return the matching slot, or {@code null} if not found
   */
  public RfqSlot lookupByQuoteReqId(final DirectBuffer src, final int offset, final int length) {
    byQuoteReqIdProbe.wrapForProbe(src, offset, length);
    return byQuoteReqId.get(byQuoteReqIdProbe);
  }

  /**
   * Variant of {@link #lookupByQuoteReqId(DirectBuffer, int, int)} for byte-array sources.
   *
   * @param src source byte array
   * @param offset start offset
   * @param length number of bytes
   * @return the matching slot, or {@code null} if not found
   */
  public RfqSlot lookupByQuoteReqId(final byte[] src, final int offset, final int length) {
    byQuoteReqIdProbe.wrapForProbe(src, offset, length);
    return byQuoteReqId.get(byQuoteReqIdProbe);
  }

  // -------------------------------------------------------------------------
  // §9.2a Two-phase quote acceptance
  // -------------------------------------------------------------------------

  /**
   * Phase A of NOS-with-quoteId acceptance (read-only). Returns the slot if it is in QUOTED state,
   * else null. Does not mutate state.
   *
   * @param src buffer containing the quoteId bytes
   * @param offset start offset
   * @param length number of bytes (must be {@link RfqSlot#QUOTE_ID_LENGTH})
   * @return the QUOTED slot, or {@code null} if not found / not QUOTED
   */
  public RfqSlot peekByQuoteId(final byte[] src, final int offset, final int length) {
    byQuoteIdProbe.wrapForProbe(src, offset, length);
    final RfqSlot slot = byQuoteId.get(byQuoteIdProbe);
    if (slot == null || slot.state != RfqSlotState.QUOTED) {
      return null;
    }
    return slot;
  }

  /**
   * Phase B of NOS-with-quoteId acceptance. Transitions QUOTED→ACCEPTED, records the quoteReqId in
   * the {@code recentlyTerminal} LRU with {@link #TERMINAL_REASON_ACCEPTED}, and releases the slot.
   *
   * <p>Pre-condition: the caller obtained {@code slot} from a same-tick {@link #peekByQuoteId} and
   * made no intervening {@code cluster.*} calls. The single-threaded duty cycle invariant
   * guarantees no snapshot can observe {@code state == ACCEPTED}.
   *
   * @param slot the slot to accept (must be in QUOTED state)
   * @param clusterTs the cluster timestamp in epoch nanos (currently unused — reserved for future
   *     accept-time recording)
   * @param eventSink the event sink (currently unused — no event is emitted by the cluster RFQ path
   *     on accept; the calling NewOrderSingleHandler emits OrderCreatedEvent on its own)
   */
  public void commitAccept(final RfqSlot slot, final long clusterTs, final EventSink eventSink) {
    if (slot.state != RfqSlotState.QUOTED) {
      throw new IllegalStateException(
          "commitAccept called on slot in state " + slot.state + " (expected QUOTED)");
    }
    // Transition then immediately release.
    slot.state = RfqSlotState.ACCEPTED;
    rememberTerminal(slot, TERMINAL_REASON_ACCEPTED);
    release(slot);
    metrics.emitAccepted++;
  }

  // -------------------------------------------------------------------------
  // Recently-terminal LRU
  // -------------------------------------------------------------------------

  /** Records a slot's quoteReqId in the LRU for post-terminal duplicate detection. */
  private void rememberTerminal(final RfqSlot slot, final byte reason) {
    final int idx = recentlyTerminalRingHead;
    recentlyTerminalRing[idx].overwrite(slot.quoteReqIdBytes, 0, RfqSlot.QUOTE_REQ_ID_LENGTH);
    recentlyTerminalReason[idx] = reason;
    recentlyTerminalRingHead = (idx + 1) % RECENTLY_TERMINAL_CAPACITY;
  }

  /**
   * Returns the terminal reason for a quoteReqId in the LRU, or 0 if not found.
   *
   * @param src buffer containing the quoteReqId
   * @param offset start offset
   * @param length number of bytes
   * @return the terminal reason byte, or 0 if not in LRU
   */
  public byte recentlyTerminalReason(final DirectBuffer src, final int offset, final int length) {
    recentlyTerminalProbe.wrapForProbe(src, offset, length);
    for (int i = 0; i < RECENTLY_TERMINAL_CAPACITY; i++) {
      if (recentlyTerminalRing[i].length() == length
          && recentlyTerminalRing[i].equals(recentlyTerminalProbe)) {
        return recentlyTerminalReason[i];
      }
    }
    return (byte) 0;
  }

  // -------------------------------------------------------------------------
  // Rate-limit (token-bucket per session)
  // -------------------------------------------------------------------------

  /**
   * Attempts to consume one rate-limit token for the given session. If no bucket exists yet for
   * this session, a fresh one is allocated from the pool.
   *
   * @param sessionId the cluster session ID
   * @param clusterTs the cluster timestamp
   * @return {@code true} if the request is admitted; {@code false} if rate-limited
   */
  public boolean rateLimitTryConsume(final long sessionId, final long clusterTs) {
    TokenBucket bucket = rateLimitBuckets.get(sessionId);
    if (bucket == null) {
      if (tokenBucketFreeCount == 0) {
        // No bucket available — under-resourced; admit conservatively to avoid false rejects.
        return true;
      }
      final int idx = tokenBucketFreeIndices[--tokenBucketFreeCount];
      bucket = tokenBucketPool[idx];
      bucket.activate(rateLimitPerSession, rateLimitRefillNanosPerToken, clusterTs);
      rateLimitBuckets.put(sessionId, bucket);
    }
    return bucket.tryConsume(clusterTs);
  }

  /**
   * Releases the rate-limit bucket for a session that just closed. Called from {@code
   * TradingClusteredService.onSessionClose}.
   *
   * @param sessionId the closing session ID
   */
  public void releaseRateLimitForSession(final long sessionId) {
    final TokenBucket bucket = rateLimitBuckets.remove(sessionId);
    if (bucket != null) {
      tokenBucketFreeIndices[tokenBucketFreeCount++] = bucket.poolIndex();
    }
  }

  // -------------------------------------------------------------------------
  // Correlation ID computation
  // -------------------------------------------------------------------------

  /**
   * Computes the TTL timer correlation ID for the given slot at its current generation.
   *
   * @param slot the slot
   * @return the TTL correlation ID
   */
  public long ttlCorrelationFor(final RfqSlot slot) {
    return ((long) slot.generation << 31) | slot.poolIndex;
  }

  /**
   * Computes the request-timeout correlation ID for the given slot at its current generation.
   *
   * @param slot the slot
   * @return the request-timeout correlation ID
   */
  public long requestTimeoutCorrelationFor(final RfqSlot slot) {
    return REQUEST_TIMEOUT_NAMESPACE_BIT | ttlCorrelationFor(slot);
  }

  // -------------------------------------------------------------------------
  // TTL lookup by product
  // -------------------------------------------------------------------------

  /**
   * Returns the TTL nanos for the given productType byte. Falls back to the default TTL if the
   * product is unknown.
   *
   * @param productType byte: 1=Spot, 2=Forward, 3=Swap
   * @return TTL nanos
   */
  public long ttlForProduct(final byte productType) {
    if (productType == ProductTypeEnum.Spot.value()) {
      return ttlSpotNanos;
    }
    if (productType == ProductTypeEnum.Forward.value()) {
      return ttlForwardNanos;
    }
    if (productType == ProductTypeEnum.Swap.value()) {
      return ttlSwapNanos;
    }
    return defaultTtlNanos;
  }

  // -------------------------------------------------------------------------
  // Insertion helpers (called by handlers after populating slot bytes)
  // -------------------------------------------------------------------------

  /**
   * Inserts the slot into {@code byQuoteReqId} and {@code byCorrelationId} (request-timeout entry).
   * Caller must have populated {@code quoteReqIdBytes} and called {@link RfqSlot#syncQuoteReqIdKey}
   * before invoking this method.
   *
   * @param slot the slot to register
   */
  public void registerRequested(final RfqSlot slot) {
    byQuoteReqId.put(slot.quoteReqIdKey, slot);
    if (slot.requestTimeoutCorrelationId != 0L) {
      byCorrelationId.put(slot.requestTimeoutCorrelationId, slot);
    }
  }

  /**
   * Inserts the slot into {@code byQuoteId} and replaces the byCorrelationId entry from
   * request-timeout to TTL. Caller must have populated {@code quoteIdBytes} and called {@link
   * RfqSlot#syncQuoteIdKey} before invoking.
   *
   * @param slot the slot transitioning REQUESTED→QUOTED
   */
  public void registerQuoted(final RfqSlot slot) {
    byQuoteId.put(slot.quoteIdKey, slot);
    // Cancel-by-replacement: remove the request-timeout correlation, install the TTL one.
    if (slot.requestTimeoutCorrelationId != 0L) {
      byCorrelationId.remove(slot.requestTimeoutCorrelationId);
    }
    if (slot.timerCorrelationId != 0L) {
      byCorrelationId.put(slot.timerCorrelationId, slot);
    }
  }

  // -------------------------------------------------------------------------
  // Timer expiry — TTL fires → 107, request-timeout fires → 106
  // -------------------------------------------------------------------------

  /**
   * Dispatches a fired timer to its slot. Drops silently on stale-correlation, missing slot, or
   * non-matching state. On a successful TTL fire emits 107; on a successful request-timeout fire
   * emits 106 with text "request timeout".
   *
   * @param correlationId the timer correlation
   * @param timestamp the cluster timestamp at fire time
   * @param eventSink the event sink for emission
   */
  public void onTimerExpiry(
      final long correlationId, final long timestamp, final EventSink eventSink) {
    final RfqSlot slot = byCorrelationId.get(correlationId);
    if (slot == null) {
      metrics.dropStaleTimer++;
      return;
    }
    final boolean isRequestTimeout = (correlationId & REQUEST_TIMEOUT_NAMESPACE_BIT) != 0L;
    if (isRequestTimeout) {
      if (slot.requestTimeoutCorrelationId != correlationId) {
        metrics.dropStaleTimer++;
        return;
      }
      if (slot.state != RfqSlotState.REQUESTED) {
        metrics.dropStaleTimer++;
        return;
      }
      emit106(slot, timestamp, eventSink, RfqRejectMessages.REQUEST_TIMEOUT);
      rememberTerminal(slot, TERMINAL_REASON_REJECTED);
      release(slot);
      metrics.rejectRequestTimeout++;
      metrics.emitRejected++;
    } else {
      if (slot.timerCorrelationId != correlationId) {
        metrics.dropStaleTimer++;
        return;
      }
      if (slot.state != RfqSlotState.QUOTED) {
        metrics.dropStaleTimer++;
        return;
      }
      emit107(slot, timestamp, eventSink);
      rememberTerminal(slot, TERMINAL_REASON_EXPIRED);
      release(slot);
      metrics.emitExpired++;
    }
  }

  /**
   * Emits a {@code QuoteExpiredEvent} (107) for the given slot using the dedicated egress buffer.
   */
  private void emit107(final RfqSlot slot, final long timestamp, final EventSink eventSink) {
    quoteExpiredEncoder.wrapAndApplyHeader(expiredEgressBuffer, 0, headerEncoder);
    quoteExpiredEncoder.sequenceNumber(0L);
    quoteExpiredEncoder.timestamp(0L);
    quoteExpiredEncoder.putQuoteId(slot.quoteIdBytes, 0);
    quoteExpiredEncoder.putQuoteReqId(slot.quoteReqIdBytes, 0);
    quoteExpiredEncoder.putSymbol(slot.symbolBytes, 0);
    quoteExpiredEncoder.side(SideEnum.get(slot.side));
    quoteExpiredEncoder.putAccountCode(slot.accountCodeBytes, 0);
    quoteExpiredEncoder.productType(ProductTypeEnum.get(slot.productType));
    final int len = MessageHeaderEncoder.ENCODED_LENGTH + quoteExpiredEncoder.encodedLength();
    if (len > expiredEgressBuffer.capacity()) {
      throw new IllegalStateException("RFQ 107 encode overflow: " + len);
    }
    eventSink.emit(null, timestamp, expiredEgressBuffer, 0, len);
  }

  /** Emits a {@code QuoteRejectedEvent} (106) for the given slot with the given text. */
  private void emit106(
      final RfqSlot slot, final long timestamp, final EventSink eventSink, final byte[] text) {
    quoteRejectedEncoder.wrapAndApplyHeader(rejectedEgressBuffer, 0, headerEncoder);
    quoteRejectedEncoder.sequenceNumber(0L);
    quoteRejectedEncoder.timestamp(0L);
    quoteRejectedEncoder.putQuoteReqId(slot.quoteReqIdBytes, 0);
    quoteRejectedEncoder.putSymbol(slot.symbolBytes, 0);
    quoteRejectedEncoder.side(SideEnum.get(slot.side));
    quoteRejectedEncoder.putAccountCode(slot.accountCodeBytes, 0);
    quoteRejectedEncoder.quoteRejectReason(QuoteRejectReasonEnum.Other);
    quoteRejectedEncoder.productType(ProductTypeEnum.get(slot.productType));
    quoteRejectedEncoder.putText(text, 0);
    final int len = MessageHeaderEncoder.ENCODED_LENGTH + quoteRejectedEncoder.encodedLength();
    if (len > rejectedEgressBuffer.capacity()) {
      throw new IllegalStateException("RFQ 106 encode overflow: " + len);
    }
    eventSink.emit(null, timestamp, rejectedEgressBuffer, 0, len);
  }

  // -------------------------------------------------------------------------
  // onSessionClose — accelerate timers for slots from this session
  // -------------------------------------------------------------------------

  /**
   * Marks all in-flight slots originating from a closing session for fast-fail. The next timer tick
   * fires their already-scheduled timer harmlessly; we proactively drive a one-shot scan via the
   * cluster's timer at +1 ns to ensure prompt expiry. Both REQUESTED and QUOTED slots are handled.
   *
   * @param sessionId the closing session
   * @param clusterTs the cluster timestamp at close
   */
  public void onSessionClose(final long sessionId, final long clusterTs) {
    if (cluster == null) {
      return;
    }
    for (int i = 0; i < capacity; i++) {
      final RfqSlot slot = slots[i];
      if (slot.state == RfqSlotState.FREE || slot.sessionId != sessionId) {
        continue;
      }
      // Re-arm whichever timer is currently bound at deadline = clusterTs + 1ns. If the timer
      // pool is exhausted, capture the failure into a counter so operators can see that some
      // session-close fast-fails were dropped (the slot will still expire eventually via the
      // original deadline; just not promptly).
      final boolean ok;
      if (slot.state == RfqSlotState.REQUESTED && slot.requestTimeoutCorrelationId != 0L) {
        ok = cluster.scheduleTimer(slot.requestTimeoutCorrelationId, clusterTs + 1L);
      } else if (slot.state == RfqSlotState.QUOTED && slot.timerCorrelationId != 0L) {
        ok = cluster.scheduleTimer(slot.timerCorrelationId, clusterTs + 1L);
      } else {
        ok = true;
      }
      if (!ok) {
        metrics.recoveryTimerRearmFailed++;
      }
    }
    metrics.sessionClosed++;
    releaseRateLimitForSession(sessionId);
  }

  // -------------------------------------------------------------------------
  // Snapshot encode (template 203)
  // -------------------------------------------------------------------------

  /**
   * Returns the active slot count for snapshot pre-flight sizing. Slots are counted in
   * Requested/Quoted/Accepted state (Accepted should be unreachable in steady state but is counted
   * defensively).
   *
   * @return number of active slots
   */
  public int activeSlotCount() {
    int count = 0;
    for (int i = 0; i < capacity; i++) {
      if (slots[i].state != RfqSlotState.FREE) {
        count++;
      }
    }
    return count;
  }

  /**
   * Encodes the active slot pool into the destination buffer using SBE template 203 ({@code
   * RfqStateSnapshot}). Iterates slots in ascending pool-index order to guarantee byte-identical
   * snapshots across replicas.
   *
   * @param dst the destination buffer
   * @param offset the start offset
   * @param hdr the message header encoder (re-used for every fragment)
   * @return the total encoded length (header + body)
   */
  public int encodeInto(
      final MutableDirectBuffer dst, final int offset, final MessageHeaderEncoder hdr) {
    final int active = activeSlotCount();
    rfqStateEncoder.wrapAndApplyHeader(dst, offset, hdr);
    final RfqStateSnapshotEncoder.NoRfqsEncoder grp = rfqStateEncoder.noRfqsCount(active);
    for (int i = 0; i < capacity; i++) {
      final RfqSlot slot = slots[i];
      if (slot.state == RfqSlotState.FREE) {
        continue;
      }
      grp.next();
      grp.putQuoteReqId(slot.quoteReqIdBytes, 0);
      grp.accountId(slot.accountId);
      grp.state(toWireState(slot.state));
      grp.putQuoteId(slot.quoteIdBytes, 0);
      grp.putSymbol(slot.symbolBytes, 0);
      grp.side(SideEnum.get(slot.side));
      grp.orderQty(slot.orderQty);
      grp.bidPx(slot.bidPx);
      grp.offerPx(slot.offerPx);
      grp.bidSize(slot.bidSize);
      grp.offerSize(slot.offerSize);
      grp.lastPx(slot.lastPx);
      grp.swapPoints(slot.swapPoints);
      grp.validUntil(slot.validUntil);
      grp.transactTime(slot.transactTime);
      grp.productType(ProductTypeEnum.get(slot.productType));
      grp.putSettlDate(slot.settlDateBytes, 0);
      grp.settlType(
          slot.settlType == 0 ? SettlTypeEnum.NULL_VAL : SettlTypeEnum.get(slot.settlType));
      grp.putCurrency(slot.currencyBytes, 0);
      grp.putSettlCurrency(slot.settlCurrencyBytes, 0);
      grp.tenor(TenorEnum.get(slot.tenor));
      final RfqStateSnapshotEncoder.NoRfqsEncoder.NoLegsEncoder legGrp =
          grp.noLegsCount(slot.noLegs);
      for (int j = 0; j < slot.noLegs; j++) {
        legGrp.next();
        legGrp.legSide(SideEnum.get(slot.legSide[j]));
        legGrp.putLegSettlDate(slot.legSettlDate[j], 0);
        legGrp.legSettlType(
            slot.legSettlType[j] == 0
                ? SettlTypeEnum.NULL_VAL
                : SettlTypeEnum.get(slot.legSettlType[j]));
        legGrp.putLegCurrency(slot.legCurrency[j], 0);
        legGrp.legTenor(TenorEnum.get(slot.legTenor[j]));
        legGrp.legOrderQty(slot.legOrderQty[j]);
        legGrp.legPrice(slot.legPrice[j]);
        legGrp.legBidPx(slot.legBidPx[j]);
        legGrp.legOfferPx(slot.legOfferPx[j]);
        legGrp.legBidSize(slot.legBidSize[j]);
        legGrp.legOfferSize(slot.legOfferSize[j]);
      }
    }
    return MessageHeaderEncoder.ENCODED_LENGTH + rfqStateEncoder.encodedLength();
  }

  /** Maps internal {@link RfqSlotState} to wire {@link RfqStateEnum}. */
  private static RfqStateEnum toWireState(final RfqSlotState s) {
    switch (s) {
      case REQUESTED:
        return RfqStateEnum.Requested;
      case QUOTED:
        return RfqStateEnum.Quoted;
      case ACCEPTED:
        return RfqStateEnum.Accepted;
      case FREE:
      default:
        throw new IllegalStateException("FREE slot in encodeInto: " + s);
    }
  }

  /** Maps wire {@link RfqStateEnum} to internal {@link RfqSlotState}. */
  private static RfqSlotState fromWireState(final RfqStateEnum w) {
    switch (w) {
      case Requested:
        return RfqSlotState.REQUESTED;
      case Quoted:
        return RfqSlotState.QUOTED;
      case Accepted:
        return RfqSlotState.ACCEPTED;
      default:
        throw new IllegalStateException("Unknown RfqStateEnum: " + w);
    }
  }

  // -------------------------------------------------------------------------
  // Snapshot restore (template 203)
  // -------------------------------------------------------------------------

  /**
   * Restores the slot pool from a snapshot fragment. Resets all slots to FREE first, then
   * repopulates from the wire data. Sets {@code generation = 1} for every restored slot (timer
   * service does not survive restart; correlation IDs are recomputed).
   *
   * @param src the source buffer
   * @param offset the start offset (header already at this offset)
   * @param blockLength the SBE block length from the message header
   * @param schemaVersion the SBE schema version from the message header
   * @return the number of bytes consumed
   * @throws IllegalStateException if the snapshot has more slots than configured capacity
   */
  public int restoreFrom(
      final DirectBuffer src, final int offset, final int blockLength, final int schemaVersion) {
    // Reset state.
    clear();
    rfqStateDecoder.wrap(src, offset, blockLength, schemaVersion);
    final RfqStateSnapshotDecoder.NoRfqsDecoder grp = rfqStateDecoder.noRfqs();
    int restoredCount = 0;
    while (grp.hasNext()) {
      grp.next();
      restoredCount++;
      if (restoredCount > capacity) {
        throw new IllegalStateException(
            "snapshot has more RFQs than rfqPoolCapacity=" + capacity + "; increase capacity");
      }
      // Pop the next free slot for this restore entry.
      final int slotIndex = freeIndices[--freeCount];
      final RfqSlot slot = slots[slotIndex];

      grp.getQuoteReqId(slot.quoteReqIdBytes, 0);
      slot.accountId = grp.accountId();
      slot.state = fromWireState(grp.state());
      grp.getQuoteId(slot.quoteIdBytes, 0);
      grp.getSymbol(slot.symbolBytes, 0);
      slot.side = (byte) grp.side().value();
      slot.orderQty = grp.orderQty();
      slot.bidPx = grp.bidPx();
      slot.offerPx = grp.offerPx();
      slot.bidSize = grp.bidSize();
      slot.offerSize = grp.offerSize();
      slot.lastPx = grp.lastPx();
      slot.swapPoints = grp.swapPoints();
      slot.validUntil = grp.validUntil();
      slot.transactTime = grp.transactTime();
      slot.productType = (byte) grp.productType().value();
      grp.getSettlDate(slot.settlDateBytes, 0);
      final SettlTypeEnum settlType = grp.settlType();
      slot.settlType = settlType == SettlTypeEnum.NULL_VAL ? 0 : (byte) settlType.value();
      grp.getCurrency(slot.currencyBytes, 0);
      grp.getSettlCurrency(slot.settlCurrencyBytes, 0);
      slot.tenor = (byte) grp.tenor().value();

      final RfqStateSnapshotDecoder.NoRfqsDecoder.NoLegsDecoder legGrp = grp.noLegs();
      slot.noLegs = legGrp.count();
      int legIdx = 0;
      while (legGrp.hasNext()) {
        legGrp.next();
        slot.legSide[legIdx] = (byte) legGrp.legSide().value();
        legGrp.getLegSettlDate(slot.legSettlDate[legIdx], 0);
        final SettlTypeEnum legSt = legGrp.legSettlType();
        slot.legSettlType[legIdx] = legSt == SettlTypeEnum.NULL_VAL ? 0 : (byte) legSt.value();
        legGrp.getLegCurrency(slot.legCurrency[legIdx], 0);
        slot.legTenor[legIdx] = (byte) legGrp.legTenor().value();
        slot.legOrderQty[legIdx] = legGrp.legOrderQty();
        slot.legPrice[legIdx] = legGrp.legPrice();
        slot.legBidPx[legIdx] = legGrp.legBidPx();
        slot.legOfferPx[legIdx] = legGrp.legOfferPx();
        slot.legBidSize[legIdx] = legGrp.legBidSize();
        slot.legOfferSize[legIdx] = legGrp.legOfferSize();
        legIdx++;
      }

      // Restore generation = 1 (deterministic across replicas, per plan §10.3a).
      slot.generation = 1;
      slot.timerCorrelationId = ttlCorrelationFor(slot);
      slot.requestTimeoutCorrelationId = requestTimeoutCorrelationFor(slot);
      slot.syncQuoteReqIdKey();
      if (slot.state == RfqSlotState.QUOTED) {
        slot.syncQuoteIdKey();
      }
    }
    metrics.poolOccupancy = capacity - freeCount;
    return MessageHeaderEncoder.ENCODED_LENGTH + rfqStateDecoder.encodedLength();
  }

  /** Resets the pool to all-FREE. Called from restore and tests. */
  public void clear() {
    byCorrelationId.clear();
    byQuoteReqId.clear();
    byQuoteId.clear();
    rateLimitBuckets.clear();
    tokenBucketFreeCount = capacity;
    for (int i = 0; i < capacity; i++) {
      tokenBucketFreeIndices[i] = capacity - 1 - i;
    }
    for (int i = 0; i < capacity; i++) {
      slots[i].state = RfqSlotState.FREE;
      slots[i].timerCorrelationId = 0L;
      slots[i].requestTimeoutCorrelationId = 0L;
      freeIndices[i] = capacity - 1 - i;
    }
    freeCount = capacity;
    metrics.poolOccupancy = 0L;
  }

  // -------------------------------------------------------------------------
  // Recovery sweep (called from TradingClusteredService.onStart after snapshot+replay)
  // -------------------------------------------------------------------------

  /**
   * Re-arms timers and rehydrates {@code accountCode} for every restored non-FREE slot. For {@code
   * REQUESTED} slots: schedules a request-timeout timer at the original deadline; if past, emits
   * 106. For {@code QUOTED} slots: schedules a TTL timer at {@code validUntil}; if past, emits 107.
   *
   * @param currentClusterTs the cluster timestamp at restore time
   * @param eventSink event sink for emissions
   * @param errorHandler error handler for timer-rearm failures
   */
  public void onSnapshotRestored(
      final long currentClusterTs, final EventSink eventSink, final ErrorHandler errorHandler) {
    Objects.requireNonNull(eventSink, "eventSink");
    Objects.requireNonNull(errorHandler, "errorHandler");
    if (cluster == null) {
      throw new IllegalStateException("setCluster must be called before onSnapshotRestored");
    }
    for (int i = 0; i < capacity; i++) {
      final RfqSlot slot = slots[i];
      if (slot.state == RfqSlotState.FREE) {
        continue;
      }
      // Rehydrate accountCode from AccountStore; fail-safe if account was deleted.
      final AccountState account = accountStore.get(slot.accountId);
      if (account == null) {
        metrics.recoveryAccountMissing++;
        if (slot.state == RfqSlotState.REQUESTED) {
          emit106(slot, currentClusterTs, eventSink, RfqRejectMessages.ACCOUNT_MISSING_ON_RECOVERY);
          metrics.emitRejected++;
        } else if (slot.state == RfqSlotState.QUOTED) {
          // Empty accountCode (slot field already restored from snapshot, may be stale).
          emit107(slot, currentClusterTs, eventSink);
          metrics.emitExpired++;
        }
        release(slot);
        continue;
      }
      // Rehydrate accountCode bytes from AccountState.
      // Clear stale accountCode bytes before rehydrate (account code length may be < 16).
      for (int b = 0; b < RfqSlot.ACCOUNT_CODE_LENGTH; b++) {
        slot.accountCodeBytes[b] = 0;
      }
      account.copyAccountCodeTo(slot.accountCodeBytes, 0);

      switch (slot.state) {
        case REQUESTED:
          recoverRequested(slot, currentClusterTs, eventSink, errorHandler);
          break;
        case QUOTED:
          recoverQuoted(slot, currentClusterTs, eventSink, errorHandler);
          break;
        case ACCEPTED:
          // Defensive — should never occur because commit is atomic with release.
          metrics.recoveryAcceptedReleased++;
          release(slot);
          break;
        case FREE:
        default:
          break;
      }
    }
    // Re-populate maps after rehydration.
    for (int i = 0; i < capacity; i++) {
      final RfqSlot slot = slots[i];
      if (slot.state == RfqSlotState.REQUESTED) {
        slot.syncQuoteReqIdKey();
        byQuoteReqId.put(slot.quoteReqIdKey, slot);
      } else if (slot.state == RfqSlotState.QUOTED) {
        slot.syncQuoteReqIdKey();
        slot.syncQuoteIdKey();
        byQuoteReqId.put(slot.quoteReqIdKey, slot);
        byQuoteId.put(slot.quoteIdKey, slot);
      }
    }
  }

  private void recoverRequested(
      final RfqSlot slot,
      final long currentClusterTs,
      final EventSink eventSink,
      final ErrorHandler errorHandler) {
    final long deadline = slot.transactTime + requestTimeoutNanos;
    if (deadline <= currentClusterTs) {
      emit106(slot, currentClusterTs, eventSink, RfqRejectMessages.REQUEST_TIMEOUT_ON_RECOVERY);
      metrics.recoveryRequestTimedOut++;
      metrics.emitRejected++;
      release(slot);
      return;
    }
    final boolean ok = cluster.scheduleTimer(slot.requestTimeoutCorrelationId, deadline);
    if (!ok) {
      errorHandler.onError(new IllegalStateException("recovery: request-timeout rearm failed"));
      emit106(slot, currentClusterTs, eventSink, RfqRejectMessages.RECOVERY_TIMER_REARM_FAILED);
      metrics.recoveryTimerRearmFailed++;
      metrics.emitRejected++;
      release(slot);
      return;
    }
    byCorrelationId.put(slot.requestTimeoutCorrelationId, slot);
    metrics.recoveryRequestRearmed++;
  }

  private void recoverQuoted(
      final RfqSlot slot,
      final long currentClusterTs,
      final EventSink eventSink,
      final ErrorHandler errorHandler) {
    if (slot.validUntil <= currentClusterTs) {
      emit107(slot, currentClusterTs, eventSink);
      metrics.recoveryExpiredOnRestore++;
      metrics.emitExpired++;
      release(slot);
      return;
    }
    final boolean ok = cluster.scheduleTimer(slot.timerCorrelationId, slot.validUntil);
    if (!ok) {
      errorHandler.onError(new IllegalStateException("recovery: TTL rearm failed"));
      emit107(slot, currentClusterTs, eventSink);
      metrics.recoveryTimerRearmFailed++;
      metrics.emitExpired++;
      release(slot);
      return;
    }
    byCorrelationId.put(slot.timerCorrelationId, slot);
    metrics.recoveryQuotedRearmed++;
  }

  // -------------------------------------------------------------------------
  // Test/debug accessors (package-private)
  // -------------------------------------------------------------------------

  /** Returns the configured capacity. Public for tests/diagnostics. */
  public int capacity() {
    return capacity;
  }

  /** Returns the current free slot count. Public for tests. */
  public int freeCount() {
    return freeCount;
  }

  /** Returns the slot at the given pool index. Package-private for tests. */
  RfqSlot slotAt(final int poolIndex) {
    return slots[poolIndex];
  }

  /** Returns the metrics view. Public for IT assertions. */
  public RfqMetrics metrics() {
    return metrics;
  }

  /** Returns acceptPriceToleranceBps. Used by NewOrderSingleHandler. */
  public int acceptPriceToleranceBps() {
    return acceptPriceToleranceBps;
  }

  /** Returns acceptQtyToleranceBps. Used by NewOrderSingleHandler. */
  public int acceptQtyToleranceBps() {
    return acceptQtyToleranceBps;
  }

  /** Returns the request-timeout in nanos. Used by handlers when scheduling timers. */
  public long requestTimeoutNanos() {
    return requestTimeoutNanos;
  }

  // ---- Internal helpers ----

  private static int nextPowerOfTwo(final int value) {
    return Integer.highestOneBit(value - 1) << 1;
  }
}
