package com.trading.engine.cluster.handler;

import static com.trading.engine.testsupport.sbe.SbeMessageAssertions.assertTemplateId;
import static com.trading.engine.testsupport.sbe.SbeTestDecoder.decodeOrderRejected;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.cluster.IdGenerator;
import com.trading.engine.cluster.OrderBook;
import com.trading.engine.cluster.TradingClusteredServiceFactory;
import com.trading.engine.cluster.journal.EventJournal;
import com.trading.engine.cluster.metrics.RfqMetrics;
import com.trading.engine.cluster.refdata.AccountFixtures;
import com.trading.engine.cluster.refdata.AccountState;
import com.trading.engine.cluster.refdata.AccountStore;
import com.trading.engine.cluster.refdata.CurrencyFixtures;
import com.trading.engine.cluster.refdata.CurrencyStore;
import com.trading.engine.cluster.refdata.ReferenceDataSeeder;
import com.trading.engine.cluster.refdata.RiskLimitFixtures;
import com.trading.engine.cluster.refdata.RiskLimitStore;
import com.trading.engine.cluster.sequencer.EventSequencer;
import com.trading.engine.cluster.state.RfqStateMachine;
import com.trading.engine.cluster.state.TradingState;
import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.NewOrderSingleDecoder;
import com.trading.engine.messages.sbe.OrdTypeEnum;
import com.trading.engine.messages.sbe.OrderCreatedEventDecoder;
import com.trading.engine.messages.sbe.RejectReasonEnum;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.testsupport.aeron.FakeClientSession;
import com.trading.engine.testsupport.aeron.FakeCluster;
import com.trading.engine.testsupport.sbe.SbeTestEncoder;
import java.nio.charset.StandardCharsets;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the APP-206 ClOrdID deduplication logic in {@link NewOrderSingleHandler}.
 *
 * <p>Exercises the {@link Long2LongHashMap}-backed registry keyed by FNV-1a 64-bit hash of {@code
 * (sessionId, clOrdId-bytes)}, the 24-hour window check, lazy eviction at the watermark, and the
 * {@link NewOrderSingleHandler#computeClOrdIdDedupKey} hash primitive directly.
 *
 * <p><b>Threading:</b> single-threaded — matches the cluster duty-cycle invariant.
 */
class NewOrderSingleHandlerClOrdIdDedupTest {

  // -------------------------------------------------------------------------
  // Constants
  // -------------------------------------------------------------------------

  private static final long TS = 1_700_000_000_000_000_000L;
  private static final long ACCOUNT_ID = 1L;
  private static final String ACCOUNT_CODE = "ACME";
  private static final long VALID_QTY = 100_000_000L; // 1 unit in fixed-point 10^-8
  private static final long VALID_PRICE = 100_000_000L; // 1.0 in fixed-point 10^-8

  // -------------------------------------------------------------------------
  // Fixtures
  // -------------------------------------------------------------------------

  private AccountStore accountStore;
  private CurrencyStore currencyStore;
  private RiskLimitStore riskLimitStore;
  private EventSink eventSink;
  private NewOrderSingleHandler handler;
  private FakeClientSession session;
  private FakeCluster fakeCluster;
  private MutableDirectBuffer msgBuf;
  private TradingState tradingState;

  @BeforeEach
  void setUp() {
    accountStore = new AccountStore();
    currencyStore = new CurrencyStore();
    riskLimitStore = new RiskLimitStore();
    seedRefData();

    final var orderBook = new OrderBook(128);
    final var orderIdGen = new IdGenerator("ORD");
    final var execIdGen = new IdGenerator("EXE");
    final var quoteIdGen = new IdGenerator("QTE");
    tradingState = new TradingState(orderBook, orderIdGen, execIdGen, quoteIdGen);

    final var sequencer = new EventSequencer();
    final var journal = new EventJournal(64);
    eventSink = new EventSink(sequencer, journal);
    fakeCluster = new FakeCluster(0L);
    eventSink.setCluster(fakeCluster);

    final var rfqMetrics = new RfqMetrics();
    final var rfqStateMachine = buildRfqStateMachine();

    handler =
        new NewOrderSingleHandler(
            tradingState,
            accountStore,
            currencyStore,
            riskLimitStore,
            ReferenceDataSeeder.permissiveSymbolEligibilityStore());
    handler.wireRfqStateMachine(rfqStateMachine, rfqMetrics);

    session = new FakeClientSession(42L);
    fakeCluster.addClientSession(session);
    msgBuf = new ExpandableArrayBuffer(512);
  }

  // -------------------------------------------------------------------------
  // Seed helpers
  // -------------------------------------------------------------------------

  private void seedRefData() {
    accountStore.put(
        AccountFixtures.account(
            ACCOUNT_ID,
            ACCOUNT_CODE,
            AccountStatusEnum.Active,
            AccountState.Capabilities.CAN_TRADE));
    currencyStore.put(
        CurrencyStore.packCode((byte) 'U', (byte) 'S', (byte) 'D'), CurrencyFixtures.usd());
    currencyStore.put(
        CurrencyStore.packCode((byte) 'E', (byte) 'U', (byte) 'R'), CurrencyFixtures.eur());
    // Permissive risk limit: maxOrderSize = 10 units (scaled)
    riskLimitStore.put(RiskLimitFixtures.permissive(ACCOUNT_ID));
  }

  private RfqStateMachine buildRfqStateMachine() {
    return new RfqStateMachine(
        256,
        TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS,
        TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS,
        TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS,
        TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS,
        TradingClusteredServiceFactory.DEFAULT_RFQ_REQUEST_TIMEOUT_NANOS,
        TradingClusteredServiceFactory.DEFAULT_RFQ_RATE_LIMIT_PER_SESSION,
        TradingClusteredServiceFactory.DEFAULT_RFQ_RATE_LIMIT_WINDOW_NANOS,
        0 /* acceptPriceToleranceBps — exact match for tests */,
        0 /* acceptQtyToleranceBps — exact match for tests */,
        accountStore,
        new RfqMetrics());
  }

  // -------------------------------------------------------------------------
  // Dispatch helpers
  // -------------------------------------------------------------------------

  /**
   * Encodes a valid Limit NOS with the given clOrdId and dispatches it at the given cluster
   * timestamp using the shared {@link #session}.
   *
   * @param clOrdId the ClOrdID (tag 11) for the NOS
   * @param clusterTimestamp the cluster-assigned epoch-nanos timestamp
   * @return the number of messages captured by the session AFTER this dispatch
   */
  private int dispatchNos(final String clOrdId, final long clusterTimestamp) {
    return dispatchNosFor(session, clOrdId, clusterTimestamp);
  }

  /**
   * Encodes a valid Limit NOS with the given clOrdId and dispatches it at the given cluster
   * timestamp using an explicit session (supports multi-session tests). Passing {@code null} for
   * {@code clientSession} is valid — the handler uses {@code sessionId=0} and EventSink tolerates a
   * null session (returns without offering). The return value in the null case is always 0.
   *
   * @param clientSession the session to use, or {@code null} for the anonymous sessionId=0 case
   * @param clOrdId the ClOrdID (tag 11) for the NOS
   * @param clusterTimestamp the cluster-assigned epoch-nanos timestamp
   * @return the total number of messages currently captured by {@code clientSession}, or 0 if null
   */
  private int dispatchNosFor(
      final FakeClientSession clientSession, final String clOrdId, final long clusterTimestamp) {
    final int len =
        SbeTestEncoder.encodeNewOrderSingle(
            msgBuf,
            0,
            clOrdId,
            "EURUSD",
            SideEnum.Buy,
            OrdTypeEnum.Limit,
            VALID_PRICE,
            VALID_QTY,
            ACCOUNT_CODE,
            "USD");
    handler.onCommand(
        clientSession,
        clusterTimestamp,
        msgBuf,
        0,
        len,
        NewOrderSingleDecoder.BLOCK_LENGTH,
        NewOrderSingleDecoder.SCHEMA_VERSION,
        eventSink);
    return clientSession == null ? 0 : clientSession.messages.size();
  }

  // =========================================================================
  // Test 1 — first submission: admitted + registry size grows to 1
  // =========================================================================

  /**
   * First-ever submission of a ClOrdID must be admitted (OrderCreatedEvent emitted) and the
   * clOrdIdRegistry must contain exactly one entry after the call.
   */
  @Test
  void onCommand_firstClOrdIdSubmission_admitsOrderAndRegistersKey() {
    final int msgCount = dispatchNos("CL-DEDUP-001", TS);

    assertEquals(1, msgCount, "exactly one event emitted");
    assertTemplateId(OrderCreatedEventDecoder.TEMPLATE_ID, session.messages.get(0));
    assertEquals(1, handler.clOrdIdRegistry.size(), "registry must hold the newly registered key");
  }

  // =========================================================================
  // Test 2 — duplicate within 24h: rejected with DuplicateClOrdId
  // =========================================================================

  /**
   * A second NOS carrying the same ClOrdID and sessionId within the 24h window must be rejected
   * with {@link RejectReasonEnum#DuplicateClOrdId} and the registry must not grow (no new entry).
   */
  @Test
  void onCommand_duplicateClOrdIdWithinWindow_rejectsWithDuplicateClOrdId() {
    dispatchNos("CL-DEDUP-002", TS);
    assertEquals(1, handler.clOrdIdRegistry.size());

    // Second dispatch: 1 second later — well within the 24h window.
    final long t2 = TS + 1_000_000_000L; // +1s
    dispatchNos("CL-DEDUP-002", t2);

    // Two messages total: first is OrderCreated, second is OrderRejected.
    assertEquals(2, session.messages.size());
    final var rej = decodeOrderRejected(session.messages.get(1));
    assertEquals(RejectReasonEnum.DuplicateClOrdId, rej.rejectReason());
    // Text must contain the expected phrase.
    final var text = rej.text();
    // CodeRabbit PR #81 R2: switch from Java `assert` to JUnit assertTrue — the repo's Gradle
    // JVM args don't enable assertions (no -ea), so `assert` would be a silent no-op.
    assertTrue(
        text.contains("duplicate ClOrdID"),
        () -> "expected 'duplicate ClOrdID' in text, got: " + text);
    // No new entry: registry size stays at 1.
    assertEquals(1, handler.clOrdIdRegistry.size(), "duplicate must not create a second entry");
  }

  // =========================================================================
  // Test 3 — same ClOrdID after window expiry: admitted again
  // =========================================================================

  /**
   * A NOS submitted exactly {@link NewOrderSingleHandler#CLORDID_DEDUP_WINDOW_NS} after the
   * first-seen timestamp must be admitted (the window boundary is exclusive: {@code
   * clusterTimestamp - firstSeen >= WINDOW} is NOT within the window).
   */
  @Test
  void onCommand_sameClOrdIdAfterWindowExpiry_admitsAgain() {
    dispatchNos("CL-DEDUP-003", TS);
    assertEquals(1, session.messages.size());

    // Second dispatch: exactly at the 24h boundary — the window check uses strict-less-than,
    // so this timestamp is no longer within the window and must be admitted.
    final long tExpired = TS + NewOrderSingleHandler.CLORDID_DEDUP_WINDOW_NS;
    dispatchNos("CL-DEDUP-003", tExpired);

    assertEquals(2, session.messages.size(), "second NOS must be admitted after window expiry");
    assertTemplateId(OrderCreatedEventDecoder.TEMPLATE_ID, session.messages.get(1));
    // Registry must hold the refreshed timestamp (still 1 entry, key was updated).
    assertEquals(1, handler.clOrdIdRegistry.size());
  }

  // =========================================================================
  // Test 4 — same ClOrdID, different sessions: both admitted
  // =========================================================================

  /**
   * The dedup key encodes the sessionId, so identical ClOrdIDs from different sessions must each be
   * admitted independently. After both dispatches the registry must have 2 entries — one keyed by
   * (sessionId=42, "ABC123") and one by (sessionId=99, "ABC123").
   *
   * <p>EventSink broadcasts to ALL registered cluster sessions, so both {@code session} (id=42) and
   * {@code session99} (id=99) receive both events. We assert on {@code session.messages} which
   * captures all broadcast events.
   */
  @Test
  void onCommand_sameClOrdIdDifferentSessions_bothAdmitted() {
    // Register a second session so EventSink can broadcast to it.
    final var session99 = new FakeClientSession(99L);
    fakeCluster.addClientSession(session99);

    // First dispatch via session-42.
    dispatchNos("ABC123", TS);
    // Second dispatch via session-99 with the same ClOrdID at the same timestamp.
    dispatchNosFor(session99, "ABC123", TS);

    // EventSink broadcasts every event to ALL sessions; session-42 receives 2 messages total
    // (one OrderCreated from each dispatch). Neither must be a DuplicateClOrdId reject.
    assertEquals(2, session.messages.size(), "session-42 must receive both events via broadcast");
    assertTemplateId(OrderCreatedEventDecoder.TEMPLATE_ID, session.messages.get(0));
    assertTemplateId(OrderCreatedEventDecoder.TEMPLATE_ID, session.messages.get(1));
    // Registry must have 2 entries: distinct keys for the two session-scoped (sessionId, clOrdId).
    assertEquals(2, handler.clOrdIdRegistry.size(), "two distinct session-scoped keys expected");
  }

  // =========================================================================
  // Test 5 — duplicate even after first attempt was rejected
  // =========================================================================

  /**
   * The ClOrdID is consumed (registered) BEFORE other validation runs. If the first NOS has an
   * invalid quantity (qty=0 → InvalidQuantity reject), the ClOrdID is still registered. A second
   * NOS with the same ClOrdID must be rejected with DuplicateClOrdId — not InvalidQuantity — which
   * proves dedup key registration precedes downstream validation.
   *
   * <p>The dedup block in {@code onCommand()} registers the key and continues; validation runs
   * afterwards. So even when validation rejects the first order, the key is already in the map.
   */
  @Test
  void onCommand_duplicateClOrdIdAfterFirstRejected_stillRejects() {
    // First NOS: qty=0 → InvalidQuantity reject (but ClOrdID is registered first).
    final int len1 =
        SbeTestEncoder.encodeNewOrderSingle(
            msgBuf,
            0,
            "CL-DEDUP-005",
            "EURUSD",
            SideEnum.Buy,
            OrdTypeEnum.Limit,
            VALID_PRICE,
            0L /* qty=0 → fails check 2 */,
            ACCOUNT_CODE,
            "USD");
    handler.onCommand(
        session,
        TS,
        msgBuf,
        0,
        len1,
        NewOrderSingleDecoder.BLOCK_LENGTH,
        NewOrderSingleDecoder.SCHEMA_VERSION,
        eventSink);

    // First message: OrderRejected with InvalidQuantity.
    assertEquals(1, session.messages.size());
    final var firstRej = decodeOrderRejected(session.messages.get(0));
    assertEquals(RejectReasonEnum.InvalidQuantity, firstRej.rejectReason());
    // ClOrdID must be in the registry despite the rejection.
    assertEquals(
        1, handler.clOrdIdRegistry.size(), "ClOrdID must be registered even on first-reject");

    // Second NOS: same ClOrdID, this time with valid qty, same session, 1s later.
    final long t2 = TS + 1_000_000_000L;
    dispatchNos("CL-DEDUP-005", t2);

    // Second message: OrderRejected with DuplicateClOrdId.
    assertEquals(2, session.messages.size());
    final var secondRej = decodeOrderRejected(session.messages.get(1));
    assertEquals(
        RejectReasonEnum.DuplicateClOrdId,
        secondRej.rejectReason(),
        "second attempt must be rejected for duplicate ClOrdID regardless of first outcome");
  }

  // =========================================================================
  // Test 6 — watermark reached: eviction runs, registry shrinks before new insert
  // =========================================================================

  /**
   * When the registry reaches {@link NewOrderSingleHandler#CLORDID_DEDUP_MAX_SIZE} entries at t=0,
   * submitting one more NEW ClOrdID at {@code t=CLORDID_DEDUP_WINDOW_NS + 1ns} (all existing
   * entries are expired) triggers lazy eviction. After the call the registry should contain only
   * the newly inserted key — size == 1.
   *
   * <p>Pre-filling uses {@link NewOrderSingleHandler#computeClOrdIdDedupKey} directly (package-
   * private) with synthetic session IDs so we avoid the cost of encoding 100K SBE messages.
   */
  @Test
  void onCommand_registryAtWatermark_evictsExpiredOnNewInsert() {
    // Pre-fill with CLORDID_DEDUP_MAX_SIZE synthetic entries, all first-seen at t=0.
    // Use distinct synthetic keys derived from integer loop counters — each represents a unique
    // (sessionId=i, singleByte=i) combination that produces a distinct FNV-1a hash.
    final long t0 = TS;
    final byte[] syntheticId = new byte[4];
    for (int i = 0; i < NewOrderSingleHandler.CLORDID_DEDUP_MAX_SIZE; i++) {
      syntheticId[0] = (byte) (i & 0xFF);
      syntheticId[1] = (byte) ((i >> 8) & 0xFF);
      syntheticId[2] = (byte) ((i >> 16) & 0xFF);
      syntheticId[3] = (byte) ((i >> 24) & 0xFF);
      final long key = NewOrderSingleHandler.computeClOrdIdDedupKey((long) i, syntheticId, 0, 4);
      handler.clOrdIdRegistry.put(key, t0);
    }
    assertEquals(
        NewOrderSingleHandler.CLORDID_DEDUP_MAX_SIZE,
        handler.clOrdIdRegistry.size(),
        "pre-fill must reach the watermark exactly");

    // Dispatch a NEW ClOrdID at t=WINDOW+1ns — all pre-filled entries are expired (age >= WINDOW).
    final long tEvict = t0 + NewOrderSingleHandler.CLORDID_DEDUP_WINDOW_NS + 1L;
    dispatchNos("CL-DEDUP-EVICT", tEvict);

    // The new order must be admitted (OrderCreatedEvent).
    assertEquals(1, session.messages.size());
    assertTemplateId(OrderCreatedEventDecoder.TEMPLATE_ID, session.messages.get(0));

    // After eviction + insert, registry must contain only the new key.
    assertEquals(
        1,
        handler.clOrdIdRegistry.size(),
        "eviction must remove all expired entries; only the new key remains");
  }

  // =========================================================================
  // Test 7 — computeClOrdIdDedupKey determinism: same inputs → same hash
  // =========================================================================

  /**
   * {@link NewOrderSingleHandler#computeClOrdIdDedupKey} must be deterministic — calling with the
   * same inputs twice must return the same hash. This property is required for Aeron Cluster log
   * replay correctness.
   */
  @Test
  void computeClOrdIdDedupKey_sameInputsTwice_returnsSameHash() {
    final byte[] clOrdId = "MYORDER-42".getBytes(StandardCharsets.US_ASCII);
    final long sessionId = 123_456L;

    final long hash1 =
        NewOrderSingleHandler.computeClOrdIdDedupKey(sessionId, clOrdId, 0, clOrdId.length);
    final long hash2 =
        NewOrderSingleHandler.computeClOrdIdDedupKey(sessionId, clOrdId, 0, clOrdId.length);

    assertEquals(hash1, hash2, "computeClOrdIdDedupKey must be deterministic");
  }

  // =========================================================================
  // Test 8 — computeClOrdIdDedupKey: different sessions → different hashes
  // =========================================================================

  /**
   * When the sessionId differs but the ClOrdID bytes are identical, the resulting hash must differ.
   * This ensures that session-isolation is enforced at the hash level and not just by convention.
   */
  @Test
  void computeClOrdIdDedupKey_differentSessions_returnDifferentHashes() {
    final byte[] clOrdId = "ABC123".getBytes(StandardCharsets.US_ASCII);

    final long hashSession42 =
        NewOrderSingleHandler.computeClOrdIdDedupKey(42L, clOrdId, 0, clOrdId.length);
    final long hashSession99 =
        NewOrderSingleHandler.computeClOrdIdDedupKey(99L, clOrdId, 0, clOrdId.length);

    assertNotEquals(
        hashSession42,
        hashSession99,
        "different sessionIds with same clOrdId must produce different hashes");
  }

  // =========================================================================
  // Test 9 — empty ClOrdID: dedup applies, second attempt rejected
  // =========================================================================

  /**
   * Edge case (raised in R5 review): an empty ClOrdID hashes to a single deterministic per-session
   * key. Submitting an empty ClOrdID twice from the same session must register the first as the
   * canonical entry and reject the second as a duplicate. This proves the dedup path runs BEFORE
   * downstream validation — an empty ClOrdID would otherwise fail symbol/account validation later,
   * but the test confirms that even degenerate input is registered first.
   */
  @Test
  void onCommand_emptyClOrdIdDuplicate_secondAttemptRejectedAsDuplicate() {
    // First submit — empty ClOrdID. Other fields are valid; whether the handler admits or rejects
    // the FIRST attempt is irrelevant — the registry must contain the empty-clOrdId dedup key.
    dispatchNos("", TS);
    assertEquals(
        1, handler.clOrdIdRegistry.size(), "empty ClOrdID must register a single dedup key");
    final int messagesAfterFirst = session.messages.size();

    // Second submit — same empty ClOrdID, same session, 1 second later. MUST be rejected with
    // DuplicateClOrdId regardless of whether the first attempt was admitted or rejected.
    dispatchNos("", TS + 1_000_000_000L);

    final int messagesAfterSecond = session.messages.size();
    assertEquals(
        messagesAfterFirst + 1,
        messagesAfterSecond,
        "second submission must emit exactly one event");
    final var rej = decodeOrderRejected(session.messages.get(messagesAfterSecond - 1));
    assertEquals(
        RejectReasonEnum.DuplicateClOrdId,
        rej.rejectReason(),
        "second empty-ClOrdID submission must reject with DuplicateClOrdId");
    assertEquals(1, handler.clOrdIdRegistry.size(), "duplicate must not grow the registry");
  }

  // =========================================================================
  // Test 10 — hard cap: registry full with no expired entries → BookFull reject
  // =========================================================================

  /**
   * When {@code clOrdIdRegistry} holds exactly {@link NewOrderSingleHandler#CLORDID_DEDUP_MAX_SIZE}
   * live entries (all within the 24-hour window) and a NEW ClOrdID arrives only 1 ns later, the
   * throttled eviction scan finds nothing to remove and the handler must reject the incoming order
   * with {@link RejectReasonEnum#BookFull} and the canonical capacity message.
   *
   * <p>This test covers the "Hard cap (CodeRabbit PR #81 R3)" branch added to {@code onCommand()}.
   * Pre-filling uses {@link NewOrderSingleHandler#computeClOrdIdDedupKey} with synthetic keys (same
   * idiom as Test 6) to avoid encoding 100K SBE messages.
   *
   * <p><b>Order-ID counter invariant:</b> The cap check fires before Phase A ID generation ({@link
   * TradingState#generateOrderId()}), so the order-creation counter must not advance.
   *
   * <p><b>Registry size invariant:</b> No new entry is inserted; size stays at {@link
   * NewOrderSingleHandler#CLORDID_DEDUP_MAX_SIZE}.
   *
   * <p><b>Threading:</b> single-threaded — cluster duty-cycle invariant.
   */
  @Test
  void onCommand_registryAtCapWithNoExpiredEntries_rejectsWithBookFull() {
    // Pre-fill with CLORDID_DEDUP_MAX_SIZE synthetic entries, all first-seen at t0.
    // Every entry is inside the 24h window relative to tEvict (only 1ns later), so
    // the throttled eviction scan will find nothing to remove.
    final long t0 = TS;
    final byte[] syntheticId = new byte[4];
    for (int i = 0; i < NewOrderSingleHandler.CLORDID_DEDUP_MAX_SIZE; i++) {
      syntheticId[0] = (byte) (i & 0xFF);
      syntheticId[1] = (byte) ((i >> 8) & 0xFF);
      syntheticId[2] = (byte) ((i >> 16) & 0xFF);
      syntheticId[3] = (byte) ((i >> 24) & 0xFF);
      final long key = NewOrderSingleHandler.computeClOrdIdDedupKey((long) i, syntheticId, 0, 4);
      handler.clOrdIdRegistry.put(key, t0);
    }
    assertEquals(
        NewOrderSingleHandler.CLORDID_DEDUP_MAX_SIZE,
        handler.clOrdIdRegistry.size(),
        "pre-fill must reach the hard cap exactly");

    // Snapshot the order-ID counter before dispatch; the cap check must not advance it.
    final long counterBefore = tradingState.orderIdGen().currentCounter();

    // Dispatch one more NOS at t0 + 1ns — all pre-filled entries are well inside the 24h window,
    // so evictExpiredClOrdIds removes nothing and the registry stays full.
    final long tEvict = t0 + 1L;
    dispatchNos("CL-DEDUP-CAP", tEvict);

    // Exactly one response message: the BookFull rejection.
    assertEquals(1, session.messages.size(), "exactly one event emitted: the BookFull reject");
    final var rej = decodeOrderRejected(session.messages.get(0));
    assertEquals(
        RejectReasonEnum.BookFull,
        rej.rejectReason(),
        "hard-cap overflow must reject with BookFull");

    // Reject text must contain the canonical capacity phrase.
    final var text = rej.text();
    assertTrue(
        text.contains("ClOrdID dedup registry at capacity"),
        () -> "expected 'ClOrdID dedup registry at capacity' in reject text, got: " + text);

    // Registry must NOT grow beyond the cap (no new entry inserted).
    assertEquals(
        NewOrderSingleHandler.CLORDID_DEDUP_MAX_SIZE,
        handler.clOrdIdRegistry.size(),
        "registry must stay at hard cap — no new entry inserted on rejection");

    // Order-ID counter must NOT advance — the cap is checked before Phase A ID generation.
    assertEquals(
        counterBefore,
        tradingState.orderIdGen().currentCounter(),
        "order-ID counter must not advance when the hard-cap BookFull branch fires");
  }
}
