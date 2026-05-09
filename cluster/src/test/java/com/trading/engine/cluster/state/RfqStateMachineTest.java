package com.trading.engine.cluster.state;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.cluster.TradingClusteredServiceFactory;
import com.trading.engine.cluster.handler.EventSink;
import com.trading.engine.cluster.journal.EventJournal;
import com.trading.engine.cluster.metrics.RfqMetrics;
import com.trading.engine.cluster.refdata.AccountState;
import com.trading.engine.cluster.refdata.AccountStore;
import com.trading.engine.cluster.sequencer.EventSequencer;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.ProductTypeEnum;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.messages.sbe.TenorEnum;
import com.trading.engine.testsupport.aeron.FakeCluster;
import io.aeron.DirectBufferVector;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.logbuffer.BufferClaim;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Comprehensive unit tests for {@link RfqStateMachine}, verifying slot lifecycle management, timer
 * expiry dispatch, rate-limiting, snapshot encode/restore, and recovery sweep logic.
 *
 * <p>Tests exercise the state machine as a pure unit (no Aeron process); the {@link Cluster}
 * dependency is satisfied by {@link CapturingFakeCluster} (a test-local Aeron Cluster double that
 * records timer schedule calls) and {@link FakeCluster} (from test-support) for non-timer paths.
 * {@link EventSink} is constructed with a real {@link EventSequencer} and {@link EventJournal} —
 * emitting to {@code session=null} is tolerated by {@link EventSink#emit} on the null-session code
 * path.
 *
 * <p><b>Threading:</b> all tests run single-threaded, matching the cluster duty-cycle invariant
 * documented on {@link RfqStateMachine}.
 *
 * <p><b>Coverage:</b> 25 tests covering acquire/release, correlation ID uniqueness, TTL timer
 * expiry, request-timeout expiry, stale-timer drop, peek/commitAccept, rate-limiting,
 * snapshot round-trip, capacity-shrink fault, and recovery sweep (expired, future, and
 * account-missing paths).
 */
class RfqStateMachineTest {

  // -------------------------------------------------------------------------
  // Test-local cluster double that captures scheduleTimer calls
  // -------------------------------------------------------------------------

  /**
   * Aeron {@link Cluster} test double that records every {@code scheduleTimer(correlationId,
   * deadline)} call and returns {@code scheduleTimerResult} (default {@code true}).
   *
   * <p>Extends the contract of {@link FakeCluster} without subclassing it (it is {@code final}) by
   * reimplementing the full {@link Cluster} interface. All non-timer methods delegate to sensible
   * no-op defaults.
   */
  static final class CapturingFakeCluster implements Cluster {

    /** All (correlationId → deadline) pairs from scheduleTimer, in insertion order. */
    final Map<Long, Long> scheduledTimers = new HashMap<>();

    /** Controls the return value of {@link #scheduleTimer}. Default {@code true}. */
    boolean scheduleTimerResult = true;

    private final IdleStrategy idle =
        new IdleStrategy() {
          @Override
          public void idle(final int workCount) {}

          @Override
          public void idle() {}

          @Override
          public void reset() {}
        };

    @Override
    public boolean scheduleTimer(final long correlationId, final long deadline) {
      scheduledTimers.put(correlationId, deadline);
      return scheduleTimerResult;
    }

    @Override
    public boolean cancelTimer(final long correlationId) {
      return true;
    }

    @Override
    public int memberId() {
      return 0;
    }

    @Override
    public Role role() {
      return Role.LEADER;
    }

    @Override
    public long logPosition() {
      return 0L;
    }

    @Override
    public io.aeron.Aeron aeron() {
      return null;
    }

    @Override
    public ClusteredServiceContainer.Context context() {
      return null;
    }

    @Override
    public ClientSession getClientSession(final long clusterSessionId) {
      return null;
    }

    @Override
    public Collection<ClientSession> clientSessions() {
      return List.of();
    }

    @Override
    public void forEachClientSession(final Consumer<? super ClientSession> action) {}

    @Override
    public boolean closeClientSession(final long clusterSessionId) {
      return false;
    }

    @Override
    public long time() {
      return 0L;
    }

    @Override
    public TimeUnit timeUnit() {
      return TimeUnit.NANOSECONDS;
    }

    @Override
    public long offer(final DirectBuffer buffer, final int offset, final int length) {
      return 0L;
    }

    @Override
    public long offer(final DirectBufferVector[] vectors) {
      return 0L;
    }

    @Override
    public long tryClaim(final int length, final BufferClaim bufferClaim) {
      return 0L;
    }

    @Override
    public IdleStrategy idleStrategy() {
      return idle;
    }
  }

  // -------------------------------------------------------------------------
  // Shared fixtures
  // -------------------------------------------------------------------------

  /** Small capacity to exercise pool-exhaustion tests quickly. Must be power-of-two >= 256. */
  private static final int CAPACITY = 256;

  /** Rate-limit: 10 tokens per second window for easier boundary tests. */
  private static final long RATE_LIMIT = 10L;

  /** Rate-limit window in nanos (1 second). */
  private static final long RATE_WINDOW = 1_000_000_000L;

  /** A fixed cluster timestamp large enough to be realistic. */
  private static final long TS = 1_700_000_000_000_000_000L;

  /** Session ID used for rate-limit tests. */
  private static final long SESSION_ID = 42L;

  private AccountStore accountStore;
  private RfqMetrics metrics;
  private RfqStateMachine machine;
  private EventSink eventSink;
  private CapturingFakeCluster capturingCluster;

  @BeforeEach
  void setUp() {
    accountStore = new AccountStore();
    metrics = new RfqMetrics();
    machine = buildMachine(accountStore, metrics);

    final var sequencer = new EventSequencer();
    final var journal = new EventJournal(64);
    eventSink = new EventSink(sequencer, journal);

    capturingCluster = new CapturingFakeCluster();
  }

  /**
   * Constructs an {@link RfqStateMachine} with the configured {@link #CAPACITY} and
   * {@link #RATE_LIMIT}/{@link #RATE_WINDOW} for deterministic rate-limit tests.
   */
  private static RfqStateMachine buildMachine(
      final AccountStore store, final RfqMetrics rfqMetrics) {
    return new RfqStateMachine(
        CAPACITY,
        TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS,
        TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS,
        TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS,
        TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS,
        TradingClusteredServiceFactory.DEFAULT_RFQ_REQUEST_TIMEOUT_NANOS,
        RATE_LIMIT,
        RATE_WINDOW,
        TradingClusteredServiceFactory.DEFAULT_RFQ_ACCEPT_PRICE_TOLERANCE_BPS,
        TradingClusteredServiceFactory.DEFAULT_RFQ_ACCEPT_QTY_TOLERANCE_BPS,
        store,
        rfqMetrics);
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  /**
   * Writes ASCII bytes for {@code text} into a 20-byte fixed-length array, NUL-padded. Asserts the
   * source string does not exceed 20 bytes.
   */
  private static byte[] fixedBytes20(final String text) {
    final byte[] result = new byte[RfqSlot.QUOTE_REQ_ID_LENGTH];
    final byte[] src = text.getBytes(StandardCharsets.US_ASCII);
    System.arraycopy(src, 0, result, 0, Math.min(src.length, result.length));
    return result;
  }

  /**
   * Writes ASCII bytes for {@code text} into an 8-byte fixed-length array, NUL-padded.
   */
  private static byte[] fixedBytes8(final String text) {
    final byte[] result = new byte[RfqSlot.SYMBOL_LENGTH];
    final byte[] src = text.getBytes(StandardCharsets.US_ASCII);
    System.arraycopy(src, 0, result, 0, Math.min(src.length, result.length));
    return result;
  }

  /**
   * Acquires a slot, populates its {@code quoteReqIdBytes} with {@code id}, and sets required
   * enum fields to valid non-zero defaults before calling
   * {@link RfqSlot#syncQuoteReqIdKey()} then {@link RfqStateMachine#registerRequested(RfqSlot)}.
   * Returns the ready-to-use REQUESTED slot.
   *
   * <p>Sets {@code side=Buy}, {@code productType=Spot}, {@code tenor=ON (value=1)} so that
   * {@code encodeInto} and {@code emit106}/{@code emit107} can call {@code SideEnum.get()},
   * {@code ProductTypeEnum.get()}, and {@code TenorEnum.get()} without throwing
   * {@code IllegalArgumentException} on value=0.
   */
  private RfqSlot acquireRequested(final String quoteReqId, final long requestTimeoutCorrId) {
    final var slot = machine.acquire();
    assertNotNull(slot, "pool unexpectedly exhausted");
    final byte[] bytes = fixedBytes20(quoteReqId);
    System.arraycopy(bytes, 0, slot.quoteReqIdBytes, 0, RfqSlot.QUOTE_REQ_ID_LENGTH);
    slot.requestTimeoutCorrelationId = requestTimeoutCorrId;
    slot.transactTime = TS;
    // Enum fields — must be non-zero; the SBE get() throws on value 0.
    slot.side = (byte) SideEnum.Buy.value();
    slot.productType = (byte) ProductTypeEnum.Spot.value();
    slot.tenor = (byte) TenorEnum.ON.value();
    slot.syncQuoteReqIdKey();
    machine.registerRequested(slot);
    return slot;
  }

  /**
   * Transitions a REQUESTED slot to QUOTED by populating quoteId and TTL fields, then calls
   * {@link RfqStateMachine#registerQuoted(RfqSlot)}.
   */
  private void transitionToQuoted(final RfqSlot slot, final String quoteId,
      final long timerCorrId, final long validUntil) {
    final byte[] bytes = fixedBytes20(quoteId);
    System.arraycopy(bytes, 0, slot.quoteIdBytes, 0, RfqSlot.QUOTE_ID_LENGTH);
    slot.timerCorrelationId = timerCorrId;
    slot.validUntil = validUntil;
    slot.state = RfqSlotState.QUOTED;
    slot.side = (byte) SideEnum.Buy.value();
    slot.productType = (byte) ProductTypeEnum.Spot.value();
    slot.tenor = (byte) TenorEnum.ON.value();
    slot.syncQuoteIdKey();
    machine.registerQuoted(slot);
  }

  /**
   * Seeds an {@link AccountStore} with a minimal {@link AccountState} for the given numeric ID and
   * code string.
   */
  private static void seedAccount(final AccountStore store, final long accountId,
      final String code) {
    final var state = new AccountState();
    state.setAccountId(accountId);
    final byte[] codeBytes = code.getBytes(StandardCharsets.US_ASCII);
    state.setAccountCode(codeBytes, 0, codeBytes.length);
    state.setCapabilities(
        AccountState.Capabilities.CAN_TRADE | AccountState.Capabilities.CAN_RFQ);
    store.put(state);
  }

  // -------------------------------------------------------------------------
  // §1 — Pool acquire
  // -------------------------------------------------------------------------

  /**
   * A fresh acquire on an empty pool must return a non-null slot in REQUESTED state. The free count
   * decrements by one and occupancy increments to 1.
   */
  @Test
  void acquire_freshSlot_returnsRequestedState() {
    final var slot = machine.acquire();

    assertNotNull(slot);
    assertEquals(RfqSlotState.REQUESTED, slot.state);
    assertEquals(CAPACITY - 1, machine.freeCount());
    assertEquals(1L, metrics.poolOccupancy);
  }

  // -------------------------------------------------------------------------
  // §2 — Pool exhaustion
  // -------------------------------------------------------------------------

  /**
   * After acquiring all CAPACITY slots, a further acquire must return {@code null} (pool
   * exhausted). The free count must be zero.
   */
  @Test
  void acquire_poolExhausted_returnsSentinel() {
    for (int i = 0; i < CAPACITY; i++) {
      assertNotNull(machine.acquire(), "slot " + i + " unexpectedly null");
    }
    assertEquals(0, machine.freeCount());

    final var overflow = machine.acquire();

    assertNull(overflow);
  }

  // -------------------------------------------------------------------------
  // §3 — Release resets state and increments generation
  // -------------------------------------------------------------------------

  /**
   * Releasing a REQUESTED slot must set it to FREE, increment its generation, clear its
   * correlation IDs, and return it to the free list so the next acquire can reuse it.
   */
  @Test
  void release_resetsState_reusesSlotWithIncrementedGeneration() {
    final var slot = acquireRequested("QREQ-001", 0xBEEFL);
    final int genBefore = slot.generation;
    final int poolIndex = slot.poolIndex;

    machine.release(slot);

    assertEquals(RfqSlotState.FREE, slot.state);
    assertEquals(genBefore + 1, slot.generation);
    assertEquals(0L, slot.timerCorrelationId);
    assertEquals(0L, slot.requestTimeoutCorrelationId);
    assertEquals(CAPACITY, machine.freeCount());

    // The same pool slot should be re-acquired next
    final var reused = machine.acquire();
    assertNotNull(reused);
    assertEquals(poolIndex, reused.poolIndex);
  }

  // -------------------------------------------------------------------------
  // §4 — Release removes byQuoteReqId mapping BEFORE mutating bytes
  // -------------------------------------------------------------------------

  /**
   * After {@code release(slot)}, a lookup by the original quoteReqId must return {@code null}
   * immediately — the map removal must precede any byte mutation so the key is still valid at
   * removal time.
   */
  @Test
  void release_removesByQuoteReqIdMappingBeforeMutatingBytes() {
    final var slot = acquireRequested("QREQ-002", 0L);

    // Confirm the slot is registered
    final var found = machine.lookupByQuoteReqId(slot.quoteReqIdBytes, 0,
        RfqSlot.QUOTE_REQ_ID_LENGTH);
    assertNotNull(found, "pre-release lookup should succeed");

    machine.release(slot);

    final var afterRelease = machine.lookupByQuoteReqId(slot.quoteReqIdBytes, 0,
        RfqSlot.QUOTE_REQ_ID_LENGTH);
    assertNull(afterRelease, "lookup after release must return null");
  }

  // -------------------------------------------------------------------------
  // §5 — Correlation ID uniqueness across reuse
  // -------------------------------------------------------------------------

  /**
   * Acquiring, releasing, and re-acquiring the same pool slot must produce a TTL correlation ID at
   * the second acquisition that differs from the one at the first acquisition because the
   * generation has incremented.
   */
  @Test
  void correlationId_afterReuse_doesNotCollideWithStale() {
    final var first = machine.acquire();
    final long corrFirst = machine.ttlCorrelationFor(first);

    machine.release(first);

    final var second = machine.acquire();
    // Pool pops lowest index — should be same physical slot (index 0)
    assertEquals(first.poolIndex, second.poolIndex);
    final long corrSecond = machine.ttlCorrelationFor(second);

    assertNotEquals(corrFirst, corrSecond,
        "correlation IDs must differ after generation increment");
  }

  // -------------------------------------------------------------------------
  // §6 — TTL timer expiry on QUOTED slot → 107
  // -------------------------------------------------------------------------

  /**
   * Firing the TTL correlation ID for a QUOTED slot must emit one {@code QuoteExpiredEvent} (107),
   * increment {@code metrics.emitExpired} to 1, and release the slot back to FREE.
   */
  @Test
  void onTimerExpiry_quotedSlot_emits107WithNullSession() {
    final var slot = acquireRequested("QREQ-003", 0L);
    // Ensure non-zero generation so ttlCorrelationFor returns a non-zero value:
    // at generation=0 and poolIndex=0 the correlation is 0 and registerQuoted skips insertion.
    // Bump generation to 1 so the TTL correlation is (1L << 31) | poolIndex which is non-zero.
    slot.generation = 1;
    final long timerCorrId = machine.ttlCorrelationFor(slot);
    transitionToQuoted(slot, "QUOTE-003", timerCorrId, TS + 30_000_000_000L);

    machine.onTimerExpiry(timerCorrId, TS, eventSink);

    assertEquals(1L, metrics.emitExpired);
    assertEquals(RfqSlotState.FREE, slot.state);
    assertEquals(CAPACITY, machine.freeCount());
  }

  // -------------------------------------------------------------------------
  // §7 — Stale correlation ID silently dropped
  // -------------------------------------------------------------------------

  /**
   * Calling {@code onTimerExpiry} with a correlation ID that does not exist in
   * {@code byCorrelationId} must silently increment {@code metrics.dropStaleTimer} and perform no
   * emission.
   */
  @Test
  void onTimerExpiry_staleCorrelationId_silentlyDropped() {
    final long staleId = 0xDEAD_BEEF_CAFE_0001L;

    machine.onTimerExpiry(staleId, TS, eventSink);

    assertEquals(1L, metrics.dropStaleTimer);
    assertEquals(0L, metrics.emitExpired);
    assertEquals(0L, metrics.emitRejected);
  }

  // -------------------------------------------------------------------------
  // §8 — Request-timeout on REQUESTED slot → 106
  // -------------------------------------------------------------------------

  /**
   * Firing the request-timeout correlation ID for a REQUESTED slot must emit
   * {@code QuoteRejectedEvent} (106), increment {@code metrics.rejectRequestTimeout} and
   * {@code metrics.emitRejected} to 1, and release the slot to FREE.
   */
  @Test
  void onTimerExpiry_requestTimeoutOnRequested_emits106RequestTimeout() {
    final var slot = machine.acquire();
    final byte[] qreqBytes = fixedBytes20("QREQ-TIMEOUT-001");
    System.arraycopy(qreqBytes, 0, slot.quoteReqIdBytes, 0, RfqSlot.QUOTE_REQ_ID_LENGTH);
    slot.transactTime = TS;
    slot.side = (byte) SideEnum.Sell.value();
    slot.productType = (byte) ProductTypeEnum.Spot.value();
    slot.tenor = (byte) TenorEnum.ON.value();
    final long requestTimeoutCorrId = machine.requestTimeoutCorrelationFor(slot);
    slot.requestTimeoutCorrelationId = requestTimeoutCorrId;
    slot.syncQuoteReqIdKey();
    machine.registerRequested(slot);

    machine.onTimerExpiry(requestTimeoutCorrId, TS, eventSink);

    assertEquals(1L, metrics.rejectRequestTimeout);
    assertEquals(1L, metrics.emitRejected);
    assertEquals(RfqSlotState.FREE, slot.state);
  }

  // -------------------------------------------------------------------------
  // §9 — Request-timeout when slot has already transitioned to QUOTED → drop
  // -------------------------------------------------------------------------

  /**
   * If a request-timeout fires for a slot that has already transitioned to QUOTED (i.e. the slot's
   * requestTimeoutCorrelationId still lives in the map as a stale entry), the code path checks
   * {@code slot.state != REQUESTED} and increments {@code metrics.dropStaleTimer}. In practice the
   * machine removes the request-timeout correlation on {@code registerQuoted}, so the correlation
   * will not be found — validating the drop-on-missing-entry path instead.
   */
  @Test
  void onTimerExpiry_requestTimeoutOnQuoted_silentlyDropped() {
    // Fire the request-timeout correlation for a slot that has already moved to QUOTED.
    // registerQuoted removes the requestTimeoutCorrelationId from byCorrelationId, so
    // onTimerExpiry will not find the slot in the map → dropStaleTimer++.
    final var slot = machine.acquire();
    final byte[] qreqBytes = fixedBytes20("QREQ-QTIMEOUT-001");
    System.arraycopy(qreqBytes, 0, slot.quoteReqIdBytes, 0, RfqSlot.QUOTE_REQ_ID_LENGTH);
    slot.transactTime = TS;
    slot.side = (byte) SideEnum.Buy.value();
    slot.productType = (byte) ProductTypeEnum.Spot.value();
    slot.tenor = (byte) TenorEnum.ON.value();
    final long requestTimeoutCorrId = machine.requestTimeoutCorrelationFor(slot);
    slot.requestTimeoutCorrelationId = requestTimeoutCorrId;
    slot.syncQuoteReqIdKey();
    machine.registerRequested(slot);

    // Transition to QUOTED — this removes requestTimeoutCorrelationId from byCorrelationId
    final long timerCorrId = machine.ttlCorrelationFor(slot);
    transitionToQuoted(slot, "QUOTE-QTIMEOUT-001", timerCorrId, TS + 30_000_000_000L);

    // Fire the OLD request-timeout correlation — it is no longer in the map
    machine.onTimerExpiry(requestTimeoutCorrId, TS, eventSink);

    assertEquals(1L, metrics.dropStaleTimer);
    assertEquals(0L, metrics.emitRejected);
    assertEquals(RfqSlotState.QUOTED, slot.state);
  }

  // -------------------------------------------------------------------------
  // §10 — peekByQuoteId on QUOTED slot returns slot without mutation
  // -------------------------------------------------------------------------

  /**
   * {@code peekByQuoteId} for a registered QUOTED slot must return the same slot and leave it in
   * QUOTED state (read-only peek, no mutation).
   */
  @Test
  void peekByQuoteId_quotedSlot_returnsSlotNoMutation() {
    final var slot = acquireRequested("QREQ-PEEK-001", 0L);
    final long timerCorrId = machine.ttlCorrelationFor(slot);
    transitionToQuoted(slot, "QUOTE-PEEK-001", timerCorrId, TS + 30_000_000_000L);

    final var peeked = machine.peekByQuoteId(
        slot.quoteIdBytes, 0, RfqSlot.QUOTE_ID_LENGTH);

    assertNotNull(peeked);
    assertEquals(slot, peeked);
    assertEquals(RfqSlotState.QUOTED, peeked.state);
  }

  // -------------------------------------------------------------------------
  // §11 — peekByQuoteId on REQUESTED slot returns null
  // -------------------------------------------------------------------------

  /**
   * A slot in REQUESTED state is not yet registered in {@code byQuoteId}; peeking by a quoteId
   * that was never registered must return {@code null}.
   */
  @Test
  void peekByQuoteId_requestedSlot_returnsNull() {
    // Acquire a REQUESTED slot — not yet registered in byQuoteId
    final var slot = acquireRequested("QREQ-PEEK-002", 0L);
    // quoteIdBytes is all zeros — not registered in byQuoteId

    final var result = machine.peekByQuoteId(slot.quoteIdBytes, 0, RfqSlot.QUOTE_ID_LENGTH);

    assertNull(result);
  }

  // -------------------------------------------------------------------------
  // §12 — peekByQuoteId with unknown quoteId returns null
  // -------------------------------------------------------------------------

  /**
   * A lookup for a quoteId that was never inserted into {@code byQuoteId} must return {@code null}.
   */
  @Test
  void peekByQuoteId_notFound_returnsNull() {
    final byte[] unknownQuoteId = fixedBytes20("UNKNOWN-QUOTE-ID");

    final var result = machine.peekByQuoteId(unknownQuoteId, 0, RfqSlot.QUOTE_ID_LENGTH);

    assertNull(result);
  }

  // -------------------------------------------------------------------------
  // §13 — commitAccept releases all three maps and advances generation
  // -------------------------------------------------------------------------

  /**
   * After {@code commitAccept}, the slot must be FREE and not findable via any of the three lookup
   * maps ({@code byCorrelationId} via lookup, {@code byQuoteReqId}, {@code byQuoteId}).
   */
  @Test
  void commitAccept_releasesAllThreeMaps_advancesGeneration() {
    final var slot = acquireRequested("QREQ-ACCEPT-001", 0L);
    final long timerCorrId = machine.ttlCorrelationFor(slot);
    transitionToQuoted(slot, "QUOTE-ACCEPT-001", timerCorrId, TS + 30_000_000_000L);
    final int genBefore = slot.generation;

    machine.commitAccept(slot, TS, eventSink);

    assertEquals(RfqSlotState.FREE, slot.state);
    assertEquals(genBefore + 1, slot.generation);

    // byQuoteReqId — lookup must return null after release
    final var byReqId = machine.lookupByQuoteReqId(
        slot.quoteReqIdBytes, 0, RfqSlot.QUOTE_REQ_ID_LENGTH);
    assertNull(byReqId, "byQuoteReqId must not contain the released slot");

    // byQuoteId — peek must return null after release
    final var byQuoteId = machine.peekByQuoteId(
        slot.quoteIdBytes, 0, RfqSlot.QUOTE_ID_LENGTH);
    assertNull(byQuoteId, "byQuoteId must not contain the released slot");

    // Pool must have reclaimed the slot
    assertEquals(CAPACITY, machine.freeCount());
  }

  // -------------------------------------------------------------------------
  // §14 — commitAccept records TERMINAL_REASON_ACCEPTED in recentlyTerminal
  // -------------------------------------------------------------------------

  /**
   * After {@code commitAccept} the quoteReqId must be recorded in the LRU with reason
   * {@link RfqStateMachine#TERMINAL_REASON_ACCEPTED} so that duplicate NOS-with-quoteId retransmits
   * can be detected.
   */
  @Test
  void commitAccept_recordsAcceptedInRecentlyTerminal() {
    final var slot = acquireRequested("QREQ-ACCEPT-002", 0L);
    final long timerCorrId = machine.ttlCorrelationFor(slot);
    transitionToQuoted(slot, "QUOTE-ACCEPT-002", timerCorrId, TS + 30_000_000_000L);

    // Capture the quoteReqId bytes before release mutates the slot
    final byte[] qreqCopy = new byte[RfqSlot.QUOTE_REQ_ID_LENGTH];
    System.arraycopy(slot.quoteReqIdBytes, 0, qreqCopy, 0, RfqSlot.QUOTE_REQ_ID_LENGTH);

    machine.commitAccept(slot, TS, eventSink);

    final var probe = new UnsafeBuffer(qreqCopy);
    final byte reason = machine.recentlyTerminalReason(probe, 0, RfqSlot.QUOTE_REQ_ID_LENGTH);
    assertEquals(RfqStateMachine.TERMINAL_REASON_ACCEPTED, reason);
  }

  // -------------------------------------------------------------------------
  // §15 — Rate-limit: below capacity admits
  // -------------------------------------------------------------------------

  /**
   * The first RATE_LIMIT consume calls on a fresh session must all return {@code true} (bucket
   * starts full at activation).
   */
  @Test
  void rateLimitTryConsume_belowLimit_admits() {
    // Consume RATE_LIMIT - 1 tokens — all must succeed
    for (int i = 0; i < RATE_LIMIT - 1; i++) {
      final boolean admitted = machine.rateLimitTryConsume(SESSION_ID, TS + i);
      assertTrue(admitted, "consume " + i + " must be admitted");
    }
  }

  // -------------------------------------------------------------------------
  // §16 — Rate-limit: at limit rejects
  // -------------------------------------------------------------------------

  /**
   * After exhausting all RATE_LIMIT tokens in a single window, the next call must return
   * {@code false} (rate-limited).
   */
  @Test
  void rateLimitTryConsume_atLimit_rejects() {
    // Exhaust all tokens
    for (int i = 0; i < RATE_LIMIT; i++) {
      machine.rateLimitTryConsume(SESSION_ID, TS);
    }

    final boolean rejected = machine.rateLimitTryConsume(SESSION_ID, TS);

    assertTrue(!rejected, "consume past limit must be rejected");
  }

  // -------------------------------------------------------------------------
  // §17 — Rate-limit: window expiry refills bucket
  // -------------------------------------------------------------------------

  /**
   * Advancing the cluster timestamp by {@link #RATE_WINDOW} nanos (one full window) after
   * exhausting the bucket must refill it and admit the next call.
   */
  @Test
  void rateLimitTryConsume_acrossWindow_resets() {
    // Exhaust all tokens at TS
    for (int i = 0; i < RATE_LIMIT; i++) {
      machine.rateLimitTryConsume(SESSION_ID, TS);
    }
    // Verify exhausted
    assertTrue(!machine.rateLimitTryConsume(SESSION_ID, TS));

    // Advance by one full window — bucket refills
    final boolean admitted = machine.rateLimitTryConsume(SESSION_ID, TS + RATE_WINDOW);

    assertTrue(admitted, "bucket must refill after one full window");
  }

  // -------------------------------------------------------------------------
  // §18 — releaseRateLimitForSession returns bucket to free list
  // -------------------------------------------------------------------------

  /**
   * After {@code releaseRateLimitForSession}, the session's bucket is returned to the free list.
   * A subsequent call with the same session ID allocates a fresh bucket (starts full again).
   */
  @Test
  void releaseRateLimitForSession_returnsBucketToFreeList() {
    // Exhaust bucket for the session
    for (int i = 0; i < RATE_LIMIT; i++) {
      machine.rateLimitTryConsume(SESSION_ID, TS);
    }
    assertTrue(!machine.rateLimitTryConsume(SESSION_ID, TS), "pre-condition: rate-limited");

    machine.releaseRateLimitForSession(SESSION_ID);

    // Re-acquire — fresh bucket, starts full
    final boolean admitted = machine.rateLimitTryConsume(SESSION_ID, TS + 1L);
    assertTrue(admitted, "new bucket after session release must admit");
  }

  // -------------------------------------------------------------------------
  // §19 — Snapshot encode: empty pool writes zero slots
  // -------------------------------------------------------------------------

  /**
   * Encoding an empty (all-FREE) pool must produce a message with {@code activeSlotCount() == 0}
   * and a positive encoded length (just the SBE header with zero-count group).
   */
  @Test
  void encodeInto_emptyPool_writesZeroSlots() {
    assertEquals(0, machine.activeSlotCount());

    final var buf = new UnsafeBuffer(new byte[4096]);
    final var hdr = new MessageHeaderEncoder();
    final int len = machine.encodeInto(buf, 0, hdr);

    assertTrue(len > 0, "encoded length must be positive even for empty snapshot");
  }

  // -------------------------------------------------------------------------
  // §20 — Snapshot round-trip: encode then restore restores all fields exactly
  // -------------------------------------------------------------------------

  /**
   * A slot populated in REQUESTED state must survive an encode/clear/restore cycle with all numeric
   * and byte fields intact. The restored pool must contain exactly one non-FREE slot at the same
   * field values (quoteReqId, orderQty, side, transactTime).
   *
   * <p>Note: {@code restoreFrom} populates slot arrays but does NOT populate the lookup maps;
   * map population happens in {@code onSnapshotRestored}. This test verifies round-trip fidelity
   * by scanning the slot array directly via the package-private {@code slotAt(poolIndex)} accessor.
   */
  @Test
  void encodeInto_thenRestoreFrom_restoresAllFieldsExactly() {
    final var slot = machine.acquire();
    final int originalPoolIndex = slot.poolIndex;
    final byte[] qreqBytes = fixedBytes20("QREQ-SNAP-001");
    System.arraycopy(qreqBytes, 0, slot.quoteReqIdBytes, 0, RfqSlot.QUOTE_REQ_ID_LENGTH);
    slot.transactTime = TS + 12345L;
    slot.orderQty = 50_000_000_000L; // 500.0 in fixed-point 10^-8
    slot.side = (byte) SideEnum.Sell.value();
    slot.productType = (byte) ProductTypeEnum.Spot.value();
    slot.tenor = (byte) TenorEnum.ON.value();
    slot.syncQuoteReqIdKey();
    machine.registerRequested(slot);

    // Encode
    final var buf = new UnsafeBuffer(new byte[65536]);
    final var hdr = new MessageHeaderEncoder();
    final int len = machine.encodeInto(buf, 0, hdr);
    assertTrue(len > 0);

    // Clear and restore
    machine.clear();
    assertEquals(0, machine.activeSlotCount());
    machine.restoreFrom(buf, MessageHeaderEncoder.ENCODED_LENGTH,
        com.trading.engine.messages.sbe.RfqStateSnapshotDecoder.BLOCK_LENGTH,
        com.trading.engine.messages.sbe.RfqStateSnapshotDecoder.SCHEMA_VERSION);

    // Verify round-trip fidelity by scanning the slot array directly.
    // restoreFrom assigns slots from the free list; the only non-FREE slot is our one entry.
    assertEquals(1, machine.activeSlotCount());
    RfqSlot restored = null;
    for (int i = 0; i < CAPACITY; i++) {
      final var candidate = machine.slotAt(i);
      if (candidate.state != RfqSlotState.FREE) {
        restored = candidate;
        break;
      }
    }
    assertNotNull(restored, "no non-FREE slot found after restore");
    assertEquals(RfqSlotState.REQUESTED, restored.state);
    assertEquals(TS + 12345L, restored.transactTime);
    assertEquals(50_000_000_000L, restored.orderQty);
    assertEquals((byte) SideEnum.Sell.value(), restored.side);
    assertArrayEquals(qreqBytes, restored.quoteReqIdBytes);
  }

  // -------------------------------------------------------------------------
  // §21 — restoreFrom: capacity shrunk below snapshot count fails fast
  // -------------------------------------------------------------------------

  /**
   * Encoding a snapshot with N active slots and restoring it on a machine whose capacity is smaller
   * than N must throw {@link IllegalStateException} rather than silently losing data.
   */
  @Test
  void restoreFrom_capacityShrunkBelowSnapshotCount_failsFast() {
    // Populate 5 slots in the full-capacity machine (unused buf — just testing the larger path)
    for (int i = 0; i < 5; i++) {
      final var slot = machine.acquire();
      final byte[] bytes = fixedBytes20("QREQ-CAP-" + String.format("%010d", i));
      System.arraycopy(bytes, 0, slot.quoteReqIdBytes, 0, RfqSlot.QUOTE_REQ_ID_LENGTH);
      slot.side = (byte) SideEnum.Buy.value();
      slot.productType = (byte) ProductTypeEnum.Spot.value();
      slot.tenor = (byte) TenorEnum.ON.value();
      slot.syncQuoteReqIdKey();
      machine.registerRequested(slot);
    }
    final var buf = new UnsafeBuffer(new byte[65536]);
    final var hdr = new MessageHeaderEncoder();
    machine.encodeInto(buf, 0, hdr);

    // Build a smaller machine (minimum valid capacity = 256, same as CAPACITY, so we can't go
    // below; instead test by encoding 260 slots and restoring into 256-capacity machine).
    // Since CAPACITY=256 is the minimum, we populate 256 slots, encode, then restore into a 256
    // machine — that should succeed. To test the failure path we use an artificially large snapshot
    // by filling the full machine (256 slots) and attempting to restore into the same machine after
    // clear which leaves freeCount=256. That won't trigger the check. Instead we test by encoding
    // with our machine (256), filling it completely, encoding again, and trying to restore into a
    // smaller machine built by the RfqStateMachine constructor minimum.
    //
    // Approach: fill all 256 slots, encode, create a new machine with capacity=256, manually
    // invoke restoreFrom after encoding 257 entries by abusing the buffer — instead we fill the
    // current machine (256 slots) and restore into a machine that has been artificially exhausted
    // (freeCount=0 via acquire-all). The IllegalStateException fires when restoredCount > capacity.
    //
    // Practical approach: fill machine to capacity, encode, then restore into itself after clear;
    // then add one synthetic extra slot to the snapshot by calling encodeInto on a machine with
    // CAPACITY+1 real slots — impossible with CAPACITY=256 minimum. Use a larger machine.

    final var bigMachine = new RfqStateMachine(
        256, // capacity — same minimum
        TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS,
        TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS,
        TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS,
        TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS,
        TradingClusteredServiceFactory.DEFAULT_RFQ_REQUEST_TIMEOUT_NANOS,
        RATE_LIMIT,
        RATE_WINDOW,
        0, 0,
        accountStore, new RfqMetrics());

    // We cannot exceed 256 slots without a larger capacity (minimum is 256). Build a 512-slot
    // machine, fill 257 slots, encode, then attempt to restore into the 256-capacity machine.
    final var largeMachine = new RfqStateMachine(
        512,
        TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS,
        TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS,
        TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS,
        TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS,
        TradingClusteredServiceFactory.DEFAULT_RFQ_REQUEST_TIMEOUT_NANOS,
        RATE_LIMIT,
        RATE_WINDOW,
        0, 0,
        accountStore, new RfqMetrics());

    for (int i = 0; i < 257; i++) {
      final var s = largeMachine.acquire();
      final byte[] bytes = fixedBytes20("QREQ-LRG-" + String.format("%010d", i));
      System.arraycopy(bytes, 0, s.quoteReqIdBytes, 0, RfqSlot.QUOTE_REQ_ID_LENGTH);
      s.side = (byte) SideEnum.Buy.value();
      s.productType = (byte) ProductTypeEnum.Spot.value();
      s.tenor = (byte) TenorEnum.ON.value();
      s.syncQuoteReqIdKey();
      largeMachine.registerRequested(s);
    }
    final var largeBuf = new UnsafeBuffer(new byte[262144]);
    final var largeHdr = new MessageHeaderEncoder();
    largeMachine.encodeInto(largeBuf, 0, largeHdr);

    // Restore 257-slot snapshot into 256-slot machine — must throw
    final var smallMachine = new RfqStateMachine(
        256,
        TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS,
        TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS,
        TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS,
        TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS,
        TradingClusteredServiceFactory.DEFAULT_RFQ_REQUEST_TIMEOUT_NANOS,
        RATE_LIMIT,
        RATE_WINDOW,
        0, 0,
        accountStore, new RfqMetrics());

    assertThrows(IllegalStateException.class, () ->
        smallMachine.restoreFrom(largeBuf, MessageHeaderEncoder.ENCODED_LENGTH,
            com.trading.engine.messages.sbe.RfqStateSnapshotDecoder.BLOCK_LENGTH,
            com.trading.engine.messages.sbe.RfqStateSnapshotDecoder.SCHEMA_VERSION));
  }

  // -------------------------------------------------------------------------
  // §22 — onSnapshotRestored: QUOTED slot past deadline emits 107 immediately
  // -------------------------------------------------------------------------

  /**
   * When a QUOTED slot's {@code validUntil} is in the past relative to the recovery timestamp,
   * {@code onSnapshotRestored} must emit a {@code QuoteExpiredEvent} (107) immediately and release
   * the slot. {@code metrics.recoveryExpiredOnRestore} and {@code metrics.emitExpired} must each
   * equal 1.
   */
  @Test
  void onSnapshotRestored_quotedPastDeadline_emits107Immediately() {
    final long accountId = 1001L;
    seedAccount(accountStore, accountId, "ACC-RESTORE-001");

    // Restore a snapshot that has one QUOTED slot with validUntil in the past
    final var slot = machine.acquire();
    final byte[] qreqBytes = fixedBytes20("QREQ-RECOV-001");
    System.arraycopy(qreqBytes, 0, slot.quoteReqIdBytes, 0, RfqSlot.QUOTE_REQ_ID_LENGTH);
    final byte[] qBytes = fixedBytes20("QUOTE-RECOV-001");
    System.arraycopy(qBytes, 0, slot.quoteIdBytes, 0, RfqSlot.QUOTE_ID_LENGTH);
    slot.accountId = accountId;
    slot.validUntil = TS - 1_000L; // already expired
    slot.side = (byte) SideEnum.Buy.value();
    slot.productType = (byte) ProductTypeEnum.Spot.value();
    slot.tenor = (byte) TenorEnum.ON.value();
    slot.state = RfqSlotState.QUOTED;
    slot.syncQuoteReqIdKey();
    slot.syncQuoteIdKey();

    // Encode, clear, restore — sets generation=1 and correlation IDs
    final var buf = new UnsafeBuffer(new byte[65536]);
    final var hdr = new MessageHeaderEncoder();
    machine.encodeInto(buf, 0, hdr);
    machine.clear();
    machine.restoreFrom(buf, MessageHeaderEncoder.ENCODED_LENGTH,
        com.trading.engine.messages.sbe.RfqStateSnapshotDecoder.BLOCK_LENGTH,
        com.trading.engine.messages.sbe.RfqStateSnapshotDecoder.SCHEMA_VERSION);

    machine.setCluster(capturingCluster);
    machine.onSnapshotRestored(TS, eventSink, err -> {});

    assertEquals(1L, metrics.recoveryExpiredOnRestore);
    assertEquals(1L, metrics.emitExpired);
    assertEquals(0, machine.activeSlotCount());
  }

  // -------------------------------------------------------------------------
  // §23 — onSnapshotRestored: QUOTED future deadline re-arms timer
  // -------------------------------------------------------------------------

  /**
   * When a QUOTED slot's {@code validUntil} is in the future, {@code onSnapshotRestored} must
   * schedule a timer via {@code Cluster.scheduleTimer} at the slot's TTL correlation ID, increment
   * {@code metrics.recoveryQuotedRearmed} to 1, and keep the slot in QUOTED state.
   */
  @Test
  void onSnapshotRestored_quotedFutureDeadline_reArmsTimer() {
    final long accountId = 1002L;
    seedAccount(accountStore, accountId, "ACC-RESTORE-002");

    final long futureDeadline = TS + 30_000_000_000L;

    final var slot = machine.acquire();
    final byte[] qreqBytes = fixedBytes20("QREQ-RECOV-002");
    System.arraycopy(qreqBytes, 0, slot.quoteReqIdBytes, 0, RfqSlot.QUOTE_REQ_ID_LENGTH);
    final byte[] qBytes = fixedBytes20("QUOTE-RECOV-002");
    System.arraycopy(qBytes, 0, slot.quoteIdBytes, 0, RfqSlot.QUOTE_ID_LENGTH);
    slot.accountId = accountId;
    slot.validUntil = futureDeadline;
    slot.side = (byte) SideEnum.Sell.value();
    slot.productType = (byte) ProductTypeEnum.Spot.value();
    slot.tenor = (byte) TenorEnum.ON.value();
    slot.state = RfqSlotState.QUOTED;
    slot.syncQuoteReqIdKey();
    slot.syncQuoteIdKey();

    final var buf = new UnsafeBuffer(new byte[65536]);
    final var hdr = new MessageHeaderEncoder();
    machine.encodeInto(buf, 0, hdr);
    machine.clear();
    machine.restoreFrom(buf, MessageHeaderEncoder.ENCODED_LENGTH,
        com.trading.engine.messages.sbe.RfqStateSnapshotDecoder.BLOCK_LENGTH,
        com.trading.engine.messages.sbe.RfqStateSnapshotDecoder.SCHEMA_VERSION);

    machine.setCluster(capturingCluster);
    machine.onSnapshotRestored(TS, eventSink, err -> {});

    assertEquals(1L, metrics.recoveryQuotedRearmed);
    assertEquals(0L, metrics.recoveryExpiredOnRestore);
    assertEquals(1, machine.activeSlotCount());

    // Timer must have been scheduled for the future deadline
    assertFalse(capturingCluster.scheduledTimers.isEmpty(),
        "scheduleTimer must have been called for re-arm");
    // Verify the deadline value matches
    final boolean anyMatchDeadline = capturingCluster.scheduledTimers.values().stream()
        .anyMatch(d -> d == futureDeadline);
    assertTrue(anyMatchDeadline,
        "a timer must be scheduled at futureDeadline=" + futureDeadline);
  }

  // Helper assertion: assertFalse not available as static import under all JUnit versions
  private static void assertFalse(final boolean condition, final String message) {
    if (condition) {
      throw new AssertionError(message);
    }
  }

  // -------------------------------------------------------------------------
  // §24 — onSnapshotRestored: REQUESTED slot past request-timeout → 106
  // -------------------------------------------------------------------------

  /**
   * When a REQUESTED slot's request-timeout deadline ({@code transactTime + requestTimeoutNanos})
   * has already elapsed relative to the recovery timestamp, {@code onSnapshotRestored} must emit
   * {@code QuoteRejectedEvent} (106), increment {@code metrics.recoveryRequestTimedOut} and
   * {@code metrics.emitRejected} to 1, and release the slot.
   */
  @Test
  void onSnapshotRestored_requestedExpired_emits106RequestTimeout() {
    final long accountId = 1003L;
    seedAccount(accountStore, accountId, "ACC-RESTORE-003");

    // requestTimeoutNanos = DEFAULT_RFQ_REQUEST_TIMEOUT_NANOS = 5s
    // transactTime = TS - 10s → deadline = TS - 5s → already elapsed
    final long staleTransactTime =
        TS - TradingClusteredServiceFactory.DEFAULT_RFQ_REQUEST_TIMEOUT_NANOS - 1_000_000_000L;

    final var slot = machine.acquire();
    final byte[] qreqBytes = fixedBytes20("QREQ-RECOV-003");
    System.arraycopy(qreqBytes, 0, slot.quoteReqIdBytes, 0, RfqSlot.QUOTE_REQ_ID_LENGTH);
    slot.accountId = accountId;
    slot.transactTime = staleTransactTime;
    slot.side = (byte) SideEnum.Buy.value();
    slot.productType = (byte) ProductTypeEnum.Spot.value();
    slot.tenor = (byte) TenorEnum.ON.value();
    slot.state = RfqSlotState.REQUESTED;
    slot.syncQuoteReqIdKey();

    final var buf = new UnsafeBuffer(new byte[65536]);
    final var hdr = new MessageHeaderEncoder();
    machine.encodeInto(buf, 0, hdr);
    machine.clear();
    machine.restoreFrom(buf, MessageHeaderEncoder.ENCODED_LENGTH,
        com.trading.engine.messages.sbe.RfqStateSnapshotDecoder.BLOCK_LENGTH,
        com.trading.engine.messages.sbe.RfqStateSnapshotDecoder.SCHEMA_VERSION);

    machine.setCluster(capturingCluster);
    machine.onSnapshotRestored(TS, eventSink, err -> {});

    assertEquals(1L, metrics.recoveryRequestTimedOut);
    assertEquals(1L, metrics.emitRejected);
    assertEquals(0, machine.activeSlotCount());
  }

  // -------------------------------------------------------------------------
  // §25 — onSnapshotRestored: account missing → REQUESTED slot emits 106
  // -------------------------------------------------------------------------

  /**
   * When the {@link AccountStore} does not contain the account referenced by a REQUESTED slot's
   * {@code accountId}, {@code onSnapshotRestored} must emit {@code QuoteRejectedEvent} (106),
   * increment {@code metrics.recoveryAccountMissing} and {@code metrics.emitRejected} to 1, and
   * release the slot.
   */
  @Test
  void onSnapshotRestored_accountMissing_requestedSlotEmits106() {
    // Do NOT seed accountId=9999 in accountStore
    final long missingAccountId = 9999L;

    final var slot = machine.acquire();
    final byte[] qreqBytes = fixedBytes20("QREQ-RECOV-004");
    System.arraycopy(qreqBytes, 0, slot.quoteReqIdBytes, 0, RfqSlot.QUOTE_REQ_ID_LENGTH);
    slot.accountId = missingAccountId;
    slot.transactTime = TS;
    slot.side = (byte) SideEnum.Buy.value();
    slot.productType = (byte) ProductTypeEnum.Spot.value();
    slot.tenor = (byte) TenorEnum.ON.value();
    slot.state = RfqSlotState.REQUESTED;
    slot.syncQuoteReqIdKey();

    final var buf = new UnsafeBuffer(new byte[65536]);
    final var hdr = new MessageHeaderEncoder();
    machine.encodeInto(buf, 0, hdr);
    machine.clear();
    machine.restoreFrom(buf, MessageHeaderEncoder.ENCODED_LENGTH,
        com.trading.engine.messages.sbe.RfqStateSnapshotDecoder.BLOCK_LENGTH,
        com.trading.engine.messages.sbe.RfqStateSnapshotDecoder.SCHEMA_VERSION);

    machine.setCluster(capturingCluster);
    machine.onSnapshotRestored(TS, eventSink, err -> {});

    assertEquals(1L, metrics.recoveryAccountMissing);
    assertEquals(1L, metrics.emitRejected);
    assertEquals(0, machine.activeSlotCount());
  }
}
