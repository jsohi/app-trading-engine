package com.trading.engine.cluster.handler;

import static com.trading.engine.testsupport.sbe.SbeTestDecoder.decodeOrderCanceled;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.cluster.IdGenerator;
import com.trading.engine.cluster.OrderBook;
import com.trading.engine.cluster.OrderState;
import com.trading.engine.cluster.TradingClusteredServiceFactory;
import com.trading.engine.cluster.journal.EventJournal;
import com.trading.engine.cluster.metrics.RfqMetrics;
import com.trading.engine.cluster.refdata.AccountFixtures;
import com.trading.engine.cluster.refdata.AccountState;
import com.trading.engine.cluster.refdata.AccountStore;
import com.trading.engine.cluster.refdata.CurrencyFixtures;
import com.trading.engine.cluster.refdata.CurrencyStore;
import com.trading.engine.cluster.refdata.RiskLimitFixtures;
import com.trading.engine.cluster.refdata.RiskLimitStore;
import com.trading.engine.cluster.sequencer.EventSequencer;
import com.trading.engine.cluster.state.RfqStateMachine;
import com.trading.engine.cluster.state.TradingState;
import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.NewOrderSingleDecoder;
import com.trading.engine.messages.sbe.OrdTypeEnum;
import com.trading.engine.messages.sbe.OrderCanceledEventDecoder;
import com.trading.engine.messages.sbe.ProductTypeEnum;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.messages.sbe.TimeInForceEnum;
import com.trading.engine.testsupport.aeron.FakeClientSession;
import com.trading.engine.testsupport.aeron.FakeCluster;
import com.trading.engine.testsupport.sbe.SbeTestEncoder;
import java.nio.charset.StandardCharsets;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.LongHashSet;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the APP-151 phase 1 session-disconnect orphan-cancel feature in {@link
 * NewOrderSingleHandler}.
 *
 * <p>Covers:
 *
 * <ul>
 *   <li>Session lifecycle tracking ({@link NewOrderSingleHandler#onSessionOpen} and {@link
 *       NewOrderSingleHandler#trackSessionOrder}) — idempotency, per-session set allocation, and
 *       multi-order tracking.
 *   <li>{@link NewOrderSingleHandler#onSessionClose} happy paths — zero, one, and N orders; exact
 *       event field matching; pool slot release; session-map eviction.
 *   <li>Edge cases — never-opened session (no-op), empty session (no-op), double-close idempotency,
 *       and stale orderKey (already-released slot skipped silently).
 * </ul>
 *
 * <p><b>Threading:</b> test-only — runs on the JUnit worker thread. All operations are
 * single-threaded, consistent with the cluster duty-cycle invariant.
 *
 * @see NewOrderSingleHandler#onSessionOpen(long)
 * @see NewOrderSingleHandler#onSessionClose(long, long, EventSink)
 * @see NewOrderSingleHandler#trackSessionOrder(long, long)
 */
class NewOrderSingleHandlerSessionCloseTest {

  // -------------------------------------------------------------------------
  // Constants
  // -------------------------------------------------------------------------

  /** Cluster timestamp used for all test dispatches; epoch nanos in 2023. */
  private static final long TS = 1_700_000_000_000_000_000L;

  private static final long ACCOUNT_ID = 1L;
  private static final String ACCOUNT_CODE = "ACME";
  private static final long SESSION_ID = 42L;

  /** 1 unit in fixed-point 10^-8. */
  private static final long VALID_QTY = 100_000_000L;

  /** 1.0 in fixed-point 10^-8. */
  private static final long VALID_PRICE = 100_000_000L;

  // -------------------------------------------------------------------------
  // Fixtures
  // -------------------------------------------------------------------------

  private AccountStore accountStore;
  private CurrencyStore currencyStore;
  private RiskLimitStore riskLimitStore;
  private TradingState tradingState;
  private OrderBook orderBook;
  private EventSink eventSink;
  private NewOrderSingleHandler handler;
  private FakeClientSession session;
  private FakeCluster fakeCluster;
  private MutableDirectBuffer msgBuf;

  /**
   * Shared zero-capacity {@link UnsafeBuffer} re-wrapped onto each captured cancel-event payload
   * for decode assertions. Hoisted to a field to keep the assertion sites allocation-free, matching
   * the class-level "zero-alloc test idiom" claim documented in this file's Javadoc.
   */
  private final UnsafeBuffer decodeBuf = new UnsafeBuffer(0, 0);

  @BeforeEach
  void setUp() {
    accountStore = new AccountStore();
    currencyStore = new CurrencyStore();
    riskLimitStore = new RiskLimitStore();
    seedRefData();

    fakeCluster = new FakeCluster(0L);
    orderBook = new OrderBook(128);
    final var orderIdGen = new IdGenerator("ORD");
    final var execIdGen = new IdGenerator("EXE");
    final var quoteIdGen = new IdGenerator("QTE");
    tradingState = new TradingState(orderBook, orderIdGen, execIdGen, quoteIdGen);

    final var sequencer = new EventSequencer();
    final var journal = new EventJournal(64);
    eventSink = new EventSink(sequencer, journal);
    eventSink.setCluster(fakeCluster);

    final var rfqMetrics = new RfqMetrics();
    final var rfqStateMachine = buildRfqStateMachine(rfqMetrics);

    handler = new NewOrderSingleHandler(tradingState, accountStore, currencyStore, riskLimitStore);
    handler.wireRfqStateMachine(rfqStateMachine, rfqMetrics);

    session = new FakeClientSession(SESSION_ID);
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
    riskLimitStore.put(RiskLimitFixtures.permissive(ACCOUNT_ID));
  }

  private RfqStateMachine buildRfqStateMachine(final RfqMetrics metrics) {
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
        metrics);
  }

  /**
   * Creates an empty {@link LongHashSet} sized to {@link
   * NewOrderSingleHandler#SESSION_ORDERS_PER_SESSION_CAPACITY} — matches what production {@link
   * NewOrderSingleHandler#onSessionOpen} allocates for a new session.
   *
   * @return an empty {@link LongHashSet} with default load factor
   */
  private LongHashSet newSessionSet() {
    return new LongHashSet(NewOrderSingleHandler.SESSION_ORDERS_PER_SESSION_CAPACITY);
  }

  /**
   * Pre-seeds the per-session set into {@code handler.sessionOrders} so the test fixture mirrors a
   * production sequence in which {@code TradingClusteredService.onSessionOpen} has already fired
   * for this session id. Subsequent {@link NewOrderSingleHandler#trackSessionOrder} calls hit the
   * non-null branch (zero-allocation production hot path), exactly as they would in production
   * after the session-open lifecycle event.
   *
   * @param sessionId the session id to register
   * @return the seeded (empty) set for the caller to populate
   */
  private LongHashSet openSession(final long sessionId) {
    final var set = newSessionSet();
    handler.sessionOrders.put(sessionId, set);
    return set;
  }

  // -------------------------------------------------------------------------
  // Dispatch helpers
  // -------------------------------------------------------------------------

  /**
   * Encodes and dispatches a valid Limit Buy NOS for the given {@code clOrdId}. The session must
   * already have its set pre-seeded in {@code sessionOrders} before this call (via {@link
   * #openSession(long)}) so the admit path stays on the zero-allocation production fast track.
   *
   * @param clOrdId the ClOrdID (tag 11) for the NOS; must be unique within the 24h dedup window
   * @return the orderKey (monotonic counter value from {@link TradingState#generateOrderId()})
   */
  private long dispatchNos(final String clOrdId) {
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
        session,
        TS,
        msgBuf,
        0,
        len,
        NewOrderSingleDecoder.BLOCK_LENGTH,
        NewOrderSingleDecoder.SCHEMA_VERSION,
        eventSink);
    return tradingState.orderIdGen().currentCounter();
  }

  /**
   * Seeds an {@link OrderState} directly into the order book via {@link
   * TradingState#applyOrderCreated}, bypassing event emission. Used by tests that need a live
   * OrderState without going through the full NOS dispatch (i.e. when the test calls {@link
   * NewOrderSingleHandler#trackSessionOrder} directly to control what is tracked).
   *
   * @param clOrdId client order ID (max 20 chars, zero-padded to 20 bytes)
   * @param symbol symbol (max 8 chars, zero-padded to 8 bytes)
   * @param side order side
   * @return the orderKey assigned to the seeded state
   */
  private long seedOrderState(final String clOrdId, final String symbol, final SideEnum side) {
    final long orderKey = tradingState.generateOrderId();
    tradingState.generateExecId();
    final byte[] orderIdBytes = tradingState.orderIdScratch();

    final byte[] clOrdIdBytes = new byte[OrderState.CL_ORD_ID_LENGTH];
    final byte[] clOrdIdRaw = clOrdId.getBytes(StandardCharsets.US_ASCII);
    System.arraycopy(
        clOrdIdRaw, 0, clOrdIdBytes, 0, Math.min(clOrdIdRaw.length, OrderState.CL_ORD_ID_LENGTH));

    final byte[] symbolBytes = new byte[OrderState.SYMBOL_LENGTH];
    final byte[] symbolRaw = symbol.getBytes(StandardCharsets.US_ASCII);
    System.arraycopy(
        symbolRaw, 0, symbolBytes, 0, Math.min(symbolRaw.length, OrderState.SYMBOL_LENGTH));

    tradingState.applyOrderCreated(
        orderKey,
        TS,
        orderIdBytes,
        0,
        side,
        OrdTypeEnum.Limit,
        TimeInForceEnum.Day,
        VALID_PRICE,
        VALID_QTY,
        ACCOUNT_ID,
        clOrdIdBytes,
        0,
        symbolBytes,
        0);
    return orderKey;
  }

  /**
   * Re-wraps the shared {@link #decodeBuf} onto the captured cancel-event payload at index {@code
   * i} of {@code session.messages} and returns the shared buffer. Eliminates the {@code new
   * UnsafeBuffer(byte[])} alloc that would otherwise occur on every decode assertion.
   *
   * @param i index into {@code session.messages}
   * @return the shared {@link UnsafeBuffer} wrapped over the captured bytes
   */
  private UnsafeBuffer wrapDecodeBuf(final int i) {
    decodeBuf.wrap(session.messages.get(i));
    return decodeBuf;
  }

  /**
   * Zero-pads an int to 3 ASCII digits without invoking the heavyweight {@code String.format}
   * formatter / reflection machinery. The {@code "00" + i} expression still allocates a single
   * {@link String} via {@code StringBuilder.toString()} — acceptable on the test path; the intent
   * here is to avoid {@code Formatter} overhead, not to be allocation-free.
   *
   * @param i non-negative integer ≤ 999
   * @return 3-character zero-padded ASCII string
   */
  private static String padThree(final int i) {
    if (i < 10) {
      return "00" + i;
    }
    if (i < 100) {
      return "0" + i;
    }
    return Integer.toString(i);
  }

  // =========================================================================
  // Test 1 — onSessionOpen allocates an empty per-session set
  // =========================================================================

  /**
   * Verifies that {@link NewOrderSingleHandler#onSessionOpen} pre-allocates an empty {@link
   * LongHashSet} in {@code sessionOrders} for the given session id.
   */
  @Test
  void onSessionOpen_freshSessionId_allocatesEmptySet() {
    handler.onSessionOpen(SESSION_ID);

    assertTrue(
        handler.sessionOrders.containsKey(SESSION_ID),
        "sessionOrders must contain the opened session id");
    final var set = handler.sessionOrders.get(SESSION_ID);
    assertTrue(set.isEmpty(), "per-session set must be empty immediately after onSessionOpen");
  }

  // =========================================================================
  // Test 2 — onSessionOpen is idempotent: second call preserves the set reference
  // =========================================================================

  /**
   * Verifies that calling {@link NewOrderSingleHandler#onSessionOpen} twice for the same session id
   * is idempotent — the existing {@link LongHashSet} reference is NOT replaced.
   */
  @Test
  void onSessionOpen_alreadyOpen_isIdempotent() {
    handler.onSessionOpen(SESSION_ID);
    final var firstSet = handler.sessionOrders.get(SESSION_ID);

    handler.onSessionOpen(SESSION_ID);
    final var secondSet = handler.sessionOrders.get(SESSION_ID);

    assertSame(firstSet, secondSet, "onSessionOpen called twice must not replace the existing set");
  }

  // =========================================================================
  // Test 3 — trackSessionOrder adds the orderKey to the session's set
  // =========================================================================

  /**
   * Verifies that after pre-seeding the session entry and calling {@link
   * NewOrderSingleHandler#trackSessionOrder}, the order key appears in the session's set. Uses
   * pre-seeds the session via {@link #openSession} so the production zero-alloc fast path is hit.
   */
  @Test
  void trackSessionOrder_afterOpen_addsKey() {
    final long orderKey = 100L;
    openSession(SESSION_ID);
    handler.trackSessionOrder(SESSION_ID, orderKey);

    final var set = handler.sessionOrders.get(SESSION_ID);
    assertTrue(set.contains(orderKey), "set must contain the tracked orderKey");
    assertEquals(1, set.size(), "set must have exactly one entry");
  }

  // =========================================================================
  // Test 4 — trackSessionOrder tracks multiple distinct order keys
  // =========================================================================

  /**
   * Verifies that after tracking N=5 distinct orderKeys for one session, all 5 appear in the
   * session's set.
   */
  @Test
  void trackSessionOrder_multipleOrders_allTracked() {
    final int n = 5;
    openSession(SESSION_ID);
    for (int i = 1; i <= n; i++) {
      handler.trackSessionOrder(SESSION_ID, (long) i * 10L);
    }

    final var set = handler.sessionOrders.get(SESSION_ID);
    assertEquals(n, set.size(), "set must contain exactly N entries after N distinct tracks");
    for (int i = 1; i <= n; i++) {
      assertTrue(set.contains((long) i * 10L), "set must contain orderKey " + (i * 10L));
    }
  }

  // =========================================================================
  // Test 5 — trackSessionOrder is idempotent for duplicate orderKey
  // =========================================================================

  /**
   * Verifies that tracking the same orderKey twice for one session yields a set of size 1 — {@link
   * LongHashSet#add} idempotency holds.
   */
  @Test
  void trackSessionOrder_duplicateOrderKey_isIdempotent() {
    final long orderKey = 77L;
    openSession(SESSION_ID);
    handler.trackSessionOrder(SESSION_ID, orderKey);
    handler.trackSessionOrder(SESSION_ID, orderKey);

    final var set = handler.sessionOrders.get(SESSION_ID);
    assertEquals(1, set.size(), "duplicate track must not grow the set beyond size 1");
    assertTrue(set.contains(orderKey), "set must still contain the orderKey");
  }

  // =========================================================================
  // Test 6 — onSessionClose for a never-opened session is a no-op
  // =========================================================================

  /**
   * Verifies that {@link NewOrderSingleHandler#onSessionClose} for a session that was never opened
   * emits zero events and does not throw.
   */
  @Test
  void onSessionClose_noTrackedOrders_isNoop() {
    final long unknownSessionId = 999L;
    handler.onSessionClose(unknownSessionId, TS, eventSink);

    assertEquals(0, session.messages.size(), "no events must be emitted for an unknown session");
  }

  // =========================================================================
  // Test 7 — onSessionClose for an open-but-empty session is a no-op
  // =========================================================================

  /**
   * Verifies that {@link NewOrderSingleHandler#onSessionClose} for a session that was opened but
   * had no orders tracked emits zero events.
   */
  @Test
  void onSessionClose_emptySessionEntry_isNoop() {
    openSession(SESSION_ID);
    handler.onSessionClose(SESSION_ID, TS, eventSink);

    assertEquals(
        0,
        session.messages.size(),
        "no events must be emitted when the session had no tracked orders");
  }

  // =========================================================================
  // Test 8 — one tracked order emits exactly one OrderCanceledEvent and releases pool slot
  // =========================================================================

  /**
   * Seeds one {@link OrderState} via {@link TradingState#applyOrderCreated}, tracks its orderKey
   * under the session, then calls {@link NewOrderSingleHandler#onSessionClose}. Verifies exactly
   * one {@link OrderCanceledEventDecoder} (template 103) is emitted and the pool slot is released.
   */
  @Test
  void onSessionClose_oneTrackedOrder_emitsOneCanceledEvent() {
    openSession(SESSION_ID);
    final long orderKey = seedOrderState("ORD-CLOSE-001", "EURUSD", SideEnum.Buy);
    handler.trackSessionOrder(SESSION_ID, orderKey);

    assertNotNull(orderBook.get(orderKey), "order must be in the book before session close");

    handler.onSessionClose(SESSION_ID, TS, eventSink);

    assertEquals(1, session.messages.size(), "exactly one OrderCanceledEvent must be emitted");
    // decodeOrderCanceled asserts templateId 103 internally.
    decodeOrderCanceled(wrapDecodeBuf(0), 0);

    assertNull(orderBook.get(orderKey), "order book slot must be released after onSessionClose");
  }

  // =========================================================================
  // Test 9 — N tracked orders emit N OrderCanceledEvents; session map entry removed
  // =========================================================================

  /**
   * Seeds N=3 {@link OrderState} instances, tracks each under the same session, then calls {@link
   * NewOrderSingleHandler#onSessionClose}. Verifies exactly 3 events emitted, all pool slots
   * released, and the session entry removed from {@code sessionOrders}.
   */
  @Test
  void onSessionClose_NTrackedOrders_emitsNCanceledEvents() {
    final int n = 3;
    openSession(SESSION_ID);
    final long[] orderKeys = new long[n];
    for (int i = 0; i < n; i++) {
      orderKeys[i] = seedOrderState("ORD-MULTI-" + padThree(i), "EURUSD", SideEnum.Buy);
      handler.trackSessionOrder(SESSION_ID, orderKeys[i]);
    }

    handler.onSessionClose(SESSION_ID, TS, eventSink);

    assertEquals(n, session.messages.size(), "exactly N OrderCanceledEvents must be emitted");

    // All N events must be template 103.
    for (int i = 0; i < n; i++) {
      decodeOrderCanceled(wrapDecodeBuf(i), 0);
    }

    // All N pool slots must be released.
    for (int i = 0; i < n; i++) {
      assertNull(
          orderBook.get(orderKeys[i]),
          "pool slot for orderKey " + orderKeys[i] + " must be released");
    }

    // Session entry must be removed from the map.
    assertFalse(
        handler.sessionOrders.containsKey(SESSION_ID),
        "sessionOrders must not contain the closed session id after onSessionClose");
  }

  // =========================================================================
  // Test 10 — double-close is idempotent: second call emits zero events
  // =========================================================================

  /**
   * Verifies that calling {@link NewOrderSingleHandler#onSessionClose} twice for the same session
   * only emits events on the first call; the second call is a no-op.
   */
  @Test
  void onSessionClose_doubleClose_isIdempotent() {
    openSession(SESSION_ID);
    final long orderKey = seedOrderState("ORD-DBLCLOSE", "EURUSD", SideEnum.Sell);
    handler.trackSessionOrder(SESSION_ID, orderKey);

    handler.onSessionClose(SESSION_ID, TS, eventSink);
    assertEquals(1, session.messages.size(), "first close must emit exactly one event");

    // Second close — session entry already removed; must be a no-op.
    handler.onSessionClose(SESSION_ID, TS, eventSink);
    assertEquals(
        1, session.messages.size(), "second close must not emit additional events (idempotent)");
  }

  // =========================================================================
  // Test 11 — stale orderKey (already released) is skipped silently
  // =========================================================================

  /**
   * Tracks two orderKeys under the same session; releases one from the order book before calling
   * {@link NewOrderSingleHandler#onSessionClose}. Verifies the stale key is skipped silently (no
   * NPE, no event) while the other key still produces an {@link OrderCanceledEventDecoder}.
   */
  @Test
  void onSessionClose_orderKeyAlreadyReleased_skipsSilently() {
    openSession(SESSION_ID);
    final long staleKey = seedOrderState("ORD-STALE", "EURUSD", SideEnum.Buy);
    final long liveKey = seedOrderState("ORD-LIVE", "EURUSD", SideEnum.Sell);
    handler.trackSessionOrder(SESSION_ID, staleKey);
    handler.trackSessionOrder(SESSION_ID, liveKey);

    // Manually release the stale key from the book before session close.
    tradingState.applyOrderCanceled(staleKey);
    assertNull(
        orderBook.get(staleKey), "stale key must be absent from the book before onSessionClose");

    // Session close — must not throw; must emit exactly one event (for liveKey only).
    handler.onSessionClose(SESSION_ID, TS, eventSink);

    assertEquals(
        1,
        session.messages.size(),
        "exactly one event must be emitted (stale key skipped silently)");
    // The emitted event must be template 103.
    decodeOrderCanceled(wrapDecodeBuf(0), 0);
    // Pool slot for liveKey must be released.
    assertNull(orderBook.get(liveKey), "live order pool slot must be released by onSessionClose");
  }

  // =========================================================================
  // Test 12 — emitted event fields match the seeded OrderState
  // =========================================================================

  /**
   * Seeds an {@link OrderState} with known orderId / clOrdId / symbol / side fields and verifies
   * that the emitted {@link OrderCanceledEventDecoder} round-trips back to the same values: {@code
   * clOrdId == origClOrdId} (server-initiated cancel convention) and {@code productType ==
   * NULL_VAL} (phase 1 — productType not yet retained on OrderState).
   */
  @Test
  void onSessionClose_eventFields_matchOrderState() {
    // Pre-seed session so the dispatch → trackSessionOrder path stays on the zero-alloc fast path.
    openSession(SESSION_ID);

    // Dispatch a full NOS so the orderId is minted by IdGenerator (matching what emitOrderCanceled
    // reads from OrderState). "CLXTEST001" is unique within the dedup window for this test.
    final int msgLen =
        SbeTestEncoder.encodeNewOrderSingle(
            msgBuf,
            0,
            "CLXTEST001",
            "EURUSD",
            SideEnum.Buy,
            OrdTypeEnum.Limit,
            VALID_PRICE,
            VALID_QTY,
            ACCOUNT_CODE,
            "USD");
    handler.onCommand(
        session,
        TS,
        msgBuf,
        0,
        msgLen,
        NewOrderSingleDecoder.BLOCK_LENGTH,
        NewOrderSingleDecoder.SCHEMA_VERSION,
        eventSink);

    // Capture the message count after admit so the cancel-emit assertion below is robust to any
    // future side effects (audit events, etc.) that the admit path might add — we only assert
    // that exactly ONE additional message landed at the close call, regardless of admit emissions.
    final int baselineCount = session.messages.size();

    // orderKey is the counter after the first order (= 1).
    final long orderKey = tradingState.orderIdGen().currentCounter();
    final var state = orderBook.get(orderKey);
    assertNotNull(state, "OrderState must exist in book after NOS dispatch");

    final byte[] expectedOrderId = new byte[OrderState.ORDER_ID_LENGTH];
    state.copyOrderIdTo(expectedOrderId, 0);
    final byte[] expectedClOrdId = new byte[OrderState.CL_ORD_ID_LENGTH];
    state.copyClOrdIdTo(expectedClOrdId, 0);
    final byte[] expectedSymbol = new byte[OrderState.SYMBOL_LENGTH];
    state.copySymbolTo(expectedSymbol, 0);
    final SideEnum expectedSide = state.side();

    handler.onSessionClose(SESSION_ID, TS, eventSink);

    assertEquals(
        baselineCount + 1,
        session.messages.size(),
        "exactly one additional OrderCanceledEvent must be emitted after onSessionClose");
    final var buf = wrapDecodeBuf(baselineCount);
    final var decoded = decodeOrderCanceled(buf, 0);

    // orderId must match.
    final byte[] actualOrderId = new byte[OrderState.ORDER_ID_LENGTH];
    decoded.getOrderId(actualOrderId, 0);
    assertArrayEquals(expectedOrderId, actualOrderId, "orderId must match the seeded OrderState");

    // clOrdId must match.
    final byte[] actualClOrdId = new byte[OrderState.CL_ORD_ID_LENGTH];
    decoded.getClOrdId(actualClOrdId, 0);
    assertArrayEquals(expectedClOrdId, actualClOrdId, "clOrdId must match the seeded OrderState");

    // origClOrdId must equal clOrdId (server-initiated cancel convention: echo the original
    // clOrdId).
    final byte[] actualOrigClOrdId = new byte[OrderState.CL_ORD_ID_LENGTH];
    decoded.getOrigClOrdId(actualOrigClOrdId, 0);
    assertArrayEquals(
        expectedClOrdId, actualOrigClOrdId, "origClOrdId must equal clOrdId for server cancels");

    // symbol must match.
    final byte[] actualSymbol = new byte[OrderState.SYMBOL_LENGTH];
    decoded.getSymbol(actualSymbol, 0);
    assertArrayEquals(expectedSymbol, actualSymbol, "symbol must match the seeded OrderState");

    // side must match.
    assertEquals(expectedSide, decoded.side(), "side must match the seeded OrderState");

    // productType must be NULL_VAL (phase 1 — productType not yet retained on OrderState).
    assertEquals(
        ProductTypeEnum.NULL_VAL, decoded.productType(), "productType must be NULL_VAL in phase 1");
  }
}
