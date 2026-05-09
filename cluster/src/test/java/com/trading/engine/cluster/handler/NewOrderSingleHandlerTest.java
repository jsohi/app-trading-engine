package com.trading.engine.cluster.handler;

import static com.trading.engine.testsupport.FixedPointTestUtil.PRICE_SCALE;
import static com.trading.engine.testsupport.sbe.SbeMessageAssertions.assertTemplateId;
import static com.trading.engine.testsupport.sbe.SbeTestDecoder.decodeOrderRejected;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

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
import com.trading.engine.cluster.refdata.RiskLimitFixtures;
import com.trading.engine.cluster.refdata.RiskLimitStore;
import com.trading.engine.cluster.sequencer.EventSequencer;
import com.trading.engine.cluster.state.RfqSlot;
import com.trading.engine.cluster.state.RfqSlotState;
import com.trading.engine.cluster.state.RfqStateMachine;
import com.trading.engine.cluster.state.TradingState;
import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.OrdTypeEnum;
import com.trading.engine.messages.sbe.OrderCreatedEventDecoder;
import com.trading.engine.messages.sbe.ProductTypeEnum;
import com.trading.engine.messages.sbe.RejectReasonEnum;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.messages.sbe.TenorEnum;
import com.trading.engine.testsupport.aeron.FakeClientSession;
import com.trading.engine.testsupport.sbe.SbeTestEncoder;
import java.nio.charset.StandardCharsets;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link NewOrderSingleHandler} — covering the base validation path and the §9.2a
 * NOS-with-quoteId peek/commit flow introduced in APP-232.
 *
 * <p>The §9.2a tests (§9.2a prefix) exercise the two-phase quote-acceptance integration:
 *
 * <ul>
 *   <li>Peek phase (check 10): read-only lookup of a QUOTED RFQ slot by quoteId; rejects on unknown
 *       quote, side mismatch, price bps breach, and qty bps breach.
 *   <li>Commit phase (step 13): after all validations pass, the QUOTED slot transitions to
 *       ACCEPTED+FREE and {@code emitAccepted} increments.
 * </ul>
 *
 * <p><b>Threading:</b> all tests single-threaded — matches the cluster duty-cycle invariant.
 */
class NewOrderSingleHandlerTest {

  // -------------------------------------------------------------------------
  // Constants
  // -------------------------------------------------------------------------

  private static final long TS = 1_700_000_000_000_000_000L;
  private static final long ACCOUNT_ID = 1L;
  private static final String ACCOUNT_CODE = "ACME";

  // -------------------------------------------------------------------------
  // Fixtures
  // -------------------------------------------------------------------------

  private AccountStore accountStore;
  private CurrencyStore currencyStore;
  private RiskLimitStore riskLimitStore;
  private TradingState tradingState;
  private EventSink eventSink;
  private NewOrderSingleHandler handler;
  private RfqStateMachine rfqStateMachine;
  private RfqMetrics rfqMetrics;
  private FakeClientSession session;
  private MutableDirectBuffer msgBuf;

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

    rfqMetrics = new RfqMetrics();
    rfqStateMachine = buildRfqStateMachine();

    handler = new NewOrderSingleHandler(tradingState, accountStore, currencyStore, riskLimitStore);
    handler.wireRfqStateMachine(rfqStateMachine, rfqMetrics);

    session = new FakeClientSession();
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
        rfqMetrics);
  }

  // -------------------------------------------------------------------------
  // Slot setup helper — builds a QUOTED slot and registers it in the machine
  // -------------------------------------------------------------------------

  /**
   * Acquires a fresh RFQ slot, populates it with the given quoteReqId, quoteId, side, bidPx,
   * offerPx, bidSize, and offerSize, transitions it to QUOTED state, syncs both keys, and registers
   * it via {@link RfqStateMachine#registerQuoted(RfqSlot)}. Returns the QUOTED slot.
   *
   * @param quoteReqId 20-byte ASCII (padded) quoteReqId
   * @param quoteId 20-byte ASCII (padded) quoteId
   * @param side the quoted side (Buy or Sell)
   * @param bidPx bid price in fixed-point 10^-8
   * @param offerPx offer price in fixed-point 10^-8
   * @param bidSize bid size in fixed-point 10^-8
   * @param offerSize offer size in fixed-point 10^-8
   * @return the slot in QUOTED state
   */
  private RfqSlot buildQuotedSlot(
      final String quoteReqId,
      final String quoteId,
      final SideEnum side,
      final long bidPx,
      final long offerPx,
      final long bidSize,
      final long offerSize) {

    final var slot = rfqStateMachine.acquire();
    assertNotNull(slot, "rfqStateMachine pool unexpectedly exhausted");

    writeFixedBytes(quoteReqId, slot.quoteReqIdBytes, RfqSlot.QUOTE_REQ_ID_LENGTH);
    writeFixedBytes(quoteId, slot.quoteIdBytes, RfqSlot.QUOTE_ID_LENGTH);

    slot.side = (byte) side.value();
    slot.bidPx = bidPx;
    slot.offerPx = offerPx;
    slot.bidSize = bidSize;
    slot.offerSize = offerSize;
    slot.productType = (byte) ProductTypeEnum.Spot.value();
    slot.tenor = (byte) TenorEnum.ON.value();
    slot.accountId = ACCOUNT_ID;
    slot.validUntil = TS + TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS;
    slot.state = RfqSlotState.QUOTED;

    slot.syncQuoteReqIdKey();
    slot.syncQuoteIdKey();

    // requestTimeoutCorrelationId is 0 — no timer in the lookup map; registerQuoted only
    // cares about quoteIdKey and timerCorrelationId. Set timerCorrelationId to the computed
    // value so byCorrelationId is populated correctly if we later fire onTimerExpiry.
    slot.timerCorrelationId = rfqStateMachine.ttlCorrelationFor(slot);

    rfqStateMachine.registerQuoted(slot);
    return slot;
  }

  /** Writes ASCII bytes for {@code text} into {@code dst} up to {@code maxLen}, NUL-padded. */
  private static void writeFixedBytes(final String text, final byte[] dst, final int maxLen) {
    final byte[] src = text.getBytes(StandardCharsets.US_ASCII);
    final int copy = Math.min(src.length, maxLen);
    System.arraycopy(src, 0, dst, 0, copy);
    for (int i = copy; i < maxLen; i++) {
      dst[i] = 0;
    }
  }

  /** Dispatches a NOS command buffer through the handler. */
  private void dispatch(final int length) {
    handler.onCommand(
        session,
        TS,
        msgBuf,
        0,
        length,
        com.trading.engine.messages.sbe.NewOrderSingleDecoder.BLOCK_LENGTH,
        com.trading.engine.messages.sbe.NewOrderSingleDecoder.SCHEMA_VERSION,
        eventSink);
  }

  /** Builds and dispatches a PreviouslyQuoted NOS for the given quoteId. Returns encoded length. */
  private int encodePreviouslyQuotedNos(
      final String quoteId, final SideEnum side, final long price, final long qty) {
    return SbeTestEncoder.encodeNewOrderSingle(
        msgBuf,
        0,
        "CL-QA-001",
        "EURUSD",
        side,
        OrdTypeEnum.PreviouslyQuoted,
        price,
        qty,
        ACCOUNT_CODE,
        "USD",
        ProductTypeEnum.Spot,
        "20260101",
        com.trading.engine.messages.sbe.SettlTypeEnum.Regular,
        "USD",
        TenorEnum.SN,
        quoteId,
        com.trading.engine.messages.sbe.TimeInForceEnum.Day,
        0L);
  }

  // =========================================================================
  // §9.2a Test 1 — peek_quotedSlotMatch_cachesSlotNoMutation
  // =========================================================================

  /**
   * When a NOS carries {@code ordType=PreviouslyQuoted} and a valid quoteId whose QUOTED slot
   * matches side, price, and qty exactly, the peek phase must cache the slot without mutating its
   * state — the slot remains in QUOTED state after peek but before the commit (which runs at the
   * end of the happy path). After the full happy path succeeds the slot transitions to FREE and
   * {@code emitAccepted} increments to 1.
   */
  @Test
  void peek_quotedSlotMatch_cachesSlotNoMutation() {
    final long offerPx = 1_10000000L; // 1.1 in 10^-8
    final long offerSize = 5L * PRICE_SCALE;
    final var slot =
        buildQuotedSlot(
            "QREQ-PEEK-MATCH-1",
            "QUOTE-PEEK-MATCH-1",
            SideEnum.Buy, // Buy NOS against offer side
            0L,
            offerPx,
            0L,
            offerSize);

    // NOS: Buy, price = offerPx (exact match, 0 bps tolerance), qty = offerSize
    final int len =
        encodePreviouslyQuotedNos("QUOTE-PEEK-MATCH-1", SideEnum.Buy, offerPx, offerSize);
    dispatch(len);

    // Happy path: OrderCreatedEvent emitted, slot committed (now FREE)
    assertEquals(1, session.messages.size());
    assertTemplateId(OrderCreatedEventDecoder.TEMPLATE_ID, session.messages.get(0));
    assertEquals(1L, rfqMetrics.emitAccepted);
    assertEquals(RfqSlotState.FREE, slot.state);
  }

  // =========================================================================
  // §9.2a Test 2 — peek_unknownQuoteId_emitsRejectQuoteNotFound
  // =========================================================================

  /**
   * When a NOS carries {@code ordType=PreviouslyQuoted} and a quoteId that does not exist in the
   * {@code byQuoteId} map, the peek phase must emit an {@code OrderRejectedEvent} with {@link
   * RejectReasonEnum#QuoteNotFound} and increment {@code rejectUnknownQuote} to 1. The slot pool
   * must be unaffected.
   */
  @Test
  void peek_unknownQuoteId_emitsRejectQuoteNotFound() {
    // No slot registered for "QUOTE-UNKNOWN-1"
    final int len =
        encodePreviouslyQuotedNos("QUOTE-UNKNOWN-1", SideEnum.Buy, 1_10000000L, 5L * PRICE_SCALE);
    dispatch(len);

    assertEquals(1, session.messages.size());
    final var rej = decodeOrderRejected(session.messages.get(0));
    assertEquals(RejectReasonEnum.QuoteNotFound, rej.rejectReason());
    assertEquals(1L, rfqMetrics.rejectUnknownQuote);
    assertEquals(0L, rfqMetrics.emitAccepted);
  }

  // =========================================================================
  // §9.2a Test 3 — peek_quotedSlotSideMismatch_emitsRejectQuoteNotFound
  // =========================================================================

  /**
   * When the NOS side does not match the quoted slot's side (e.g. a Buy NOS against a Sell-only
   * quoted slot), the peek phase must emit {@code QuoteNotFound} and increment {@code
   * rejectQuoteSideMismatch} to 1. The slot must remain QUOTED.
   */
  @Test
  void peek_quotedSlotSideMismatch_emitsRejectQuoteNotFound() {
    // Slot quoted for Sell side only (bidPx/bidSize populated, offerPx=0)
    final long bidPx = 1_10000000L;
    final long bidSize = 5L * PRICE_SCALE;
    final var slot =
        buildQuotedSlot("QREQ-SIDE-MM-1", "QUOTE-SIDE-MM-1", SideEnum.Sell, bidPx, 0L, bidSize, 0L);

    // Send a Buy NOS (side mismatch — slot.side=Sell but NOS.side=Buy)
    final int len = encodePreviouslyQuotedNos("QUOTE-SIDE-MM-1", SideEnum.Buy, bidPx, bidSize);
    dispatch(len);

    assertEquals(1, session.messages.size());
    final var rej = decodeOrderRejected(session.messages.get(0));
    assertEquals(RejectReasonEnum.QuoteNotFound, rej.rejectReason());
    assertEquals(1L, rfqMetrics.rejectQuoteSideMismatch);
    assertEquals(RfqSlotState.QUOTED, slot.state, "slot must remain QUOTED after peek-only reject");
  }

  // =========================================================================
  // §9.2a Test 4 — peek_quotedSlotPriceBeyondTolerance_emitsReject
  // =========================================================================

  /**
   * When the NOS price differs from the quoted price by more than {@code acceptPriceToleranceBps=0}
   * (i.e. any deviation), the peek phase must reject with {@link RejectReasonEnum#QuoteNotFound}
   * and increment {@code rejectQuotePriceMismatch} to 1. The slot must remain QUOTED.
   */
  @Test
  void peek_quotedSlotPriceBeyondTolerance_emitsReject() {
    // Use a price large enough that even a 1-unit delta produces > 0 bps.
    // 1 bps of 100_000_000 = 10_000 units. Use a delta of 1_000_000 (0.01 = 10000 bps).
    // quotedPx = 1.0 scaled = 100_000_000. nosPrice = 1.01 scaled = 101_000_000.
    // pxDelta = 1_000_000; pxDeltaBps = 1_000_000 * 10_000 / 100_000_000 = 100 bps > tolerance=0.
    final long offerPx = 100_000_000L; // 1.0 in 10^-8
    final long offerSize = 5L * PRICE_SCALE;
    final var slot =
        buildQuotedSlot(
            "QREQ-PX-TOL-1", "QUOTE-PX-TOL-1", SideEnum.Buy, 0L, offerPx, 0L, offerSize);

    // NOS price is 100 bps above quoted — exceeds tolerance=0 bps
    final long nosPrice = offerPx + 1_000_000L; // 1.01 scaled = 100 bps deviation
    final int len = encodePreviouslyQuotedNos("QUOTE-PX-TOL-1", SideEnum.Buy, nosPrice, offerSize);
    dispatch(len);

    assertEquals(1, session.messages.size());
    final var rej = decodeOrderRejected(session.messages.get(0));
    assertEquals(RejectReasonEnum.QuoteNotFound, rej.rejectReason());
    assertEquals(1L, rfqMetrics.rejectQuotePriceMismatch);
    assertEquals(RfqSlotState.QUOTED, slot.state, "slot must remain QUOTED after peek-only reject");
  }

  // =========================================================================
  // §9.2a Test 5 — peek_quotedSlotQtyBeyondTolerance_emitsReject
  // =========================================================================

  /**
   * When the NOS qty differs from the quoted size by more than {@code acceptQtyToleranceBps=0}, the
   * peek phase must reject and increment {@code rejectQuoteQtyMismatch} to 1. The slot must remain
   * QUOTED.
   */
  @Test
  void peek_quotedSlotQtyBeyondTolerance_emitsReject() {
    // Use a qty large enough that even a moderate delta produces > 0 bps.
    // quotedSize = 1.0 scaled = 100_000_000. nosQty = 1.01 scaled = 101_000_000.
    // qtyDelta = 1_000_000; qtyDeltaBps = 1_000_000 * 10_000 / 100_000_000 = 100 bps > tolerance=0.
    final long offerPx = 100_000_000L;
    final long offerSize = 100_000_000L; // 1.0 units in 10^-8
    final var slot =
        buildQuotedSlot(
            "QREQ-QTY-TOL-1", "QUOTE-QTY-TOL-1", SideEnum.Buy, 0L, offerPx, 0L, offerSize);

    // NOS qty is 100 bps above quoted — exceeds tolerance=0 bps
    final long nosQty = offerSize + 1_000_000L; // 100 bps deviation
    final int len = encodePreviouslyQuotedNos("QUOTE-QTY-TOL-1", SideEnum.Buy, offerPx, nosQty);
    dispatch(len);

    assertEquals(1, session.messages.size());
    final var rej = decodeOrderRejected(session.messages.get(0));
    assertEquals(RejectReasonEnum.QuoteNotFound, rej.rejectReason());
    assertEquals(1L, rfqMetrics.rejectQuoteQtyMismatch);
    assertEquals(RfqSlotState.QUOTED, slot.state, "slot must remain QUOTED after peek-only reject");
  }

  // =========================================================================
  // §9.2a Test 6 — peek_emptyQuoteId_skipsPeekPhase
  // =========================================================================

  /**
   * When a NOS carries {@code ordType=PreviouslyQuoted} but the quoteId field is all-zero bytes
   * (i.e. effectively empty after trimTrailingZeros), the peek phase must be skipped entirely and
   * the order proceeds through normal validation. {@code emitAccepted} stays at 0 and no reject
   * counter increments.
   */
  @Test
  void peek_emptyQuoteId_skipsPeekPhase() {
    // Send PreviouslyQuoted NOS with an empty quoteId string — SBE pads with zeros
    final int len =
        SbeTestEncoder.encodeNewOrderSingle(
            msgBuf,
            0,
            "CL-EMPTY-QID",
            "EURUSD",
            SideEnum.Buy,
            OrdTypeEnum.PreviouslyQuoted,
            1L * PRICE_SCALE,
            1L * PRICE_SCALE,
            ACCOUNT_CODE,
            "USD",
            ProductTypeEnum.Spot,
            "20260101",
            com.trading.engine.messages.sbe.SettlTypeEnum.Regular,
            "USD",
            TenorEnum.SN,
            "" /* empty quoteId */,
            com.trading.engine.messages.sbe.TimeInForceEnum.Day,
            0L);
    dispatch(len);

    // No reject from §9.2a (peek skipped); order admitted normally
    assertEquals(1, session.messages.size());
    assertTemplateId(OrderCreatedEventDecoder.TEMPLATE_ID, session.messages.get(0));
    assertEquals(0L, rfqMetrics.emitAccepted, "emitAccepted stays 0 when peek was skipped");
    assertEquals(0L, rfqMetrics.rejectUnknownQuote);
  }

  // =========================================================================
  // §9.2a Test 7 — peek_ordTypeNotPreviouslyQuoted_skipsPeekPhase
  // =========================================================================

  /**
   * When a NOS carries {@code ordType=Limit} (not PreviouslyQuoted) even if a quoteId is populated,
   * the peek phase is not entered (the guard condition checks {@code ordType == PreviouslyQuoted}).
   * The order proceeds normally and {@code emitAccepted} stays at 0.
   */
  @Test
  void peek_ordTypeNotPreviouslyQuoted_skipsPeekPhase() {
    // Build a QUOTED slot so there IS a valid quoteId in the map, but send a Limit NOS
    final long offerPx = 1_00000000L;
    final long offerSize = 1L * PRICE_SCALE;
    buildQuotedSlot(
        "QREQ-LIMIT-SKIP", "QUOTE-LIMIT-SKIP", SideEnum.Buy, 0L, offerPx, 0L, offerSize);

    // Limit NOS — peek must be skipped even if quoteId matches
    final int len =
        SbeTestEncoder.encodeNewOrderSingle(
            msgBuf,
            0,
            "CL-LIMIT-SKIP",
            "EURUSD",
            SideEnum.Buy,
            OrdTypeEnum.Limit,
            offerPx,
            offerSize,
            ACCOUNT_CODE,
            "USD",
            ProductTypeEnum.Spot,
            "20260101",
            com.trading.engine.messages.sbe.SettlTypeEnum.Regular,
            "USD",
            TenorEnum.SN,
            "QUOTE-LIMIT-SKIP" /* quoteId present but ignored for Limit */,
            com.trading.engine.messages.sbe.TimeInForceEnum.Day,
            0L);
    dispatch(len);

    // Limit NOS proceeds normally (admitted)
    assertEquals(1, session.messages.size());
    assertTemplateId(OrderCreatedEventDecoder.TEMPLATE_ID, session.messages.get(0));
    assertEquals(0L, rfqMetrics.emitAccepted, "emitAccepted stays 0 when peek was skipped");
  }

  // =========================================================================
  // §9.2a Test 8 — peekThenRiskLimitFails_slotRemainsInQuoted
  // =========================================================================

  /**
   * When the §9.2a peek (check 10) succeeds and caches the slot, but check 11 (risk limit) rejects
   * because the NOS qty exceeds the account's maxOrderSize, the slot must remain in QUOTED state
   * (peek is read-only; the commit step never executes). The {@code emitAccepted} counter stays at
   * 0 and the slot is available for client retry.
   *
   * <p>Setup: risk limit for account 1 is 10 units max order size (from {@code
   * RiskLimitFixtures.permissive}). The NOS qty is 11 units, which exceeds the limit.
   */
  @Test
  void peekThenRiskLimitFails_slotRemainsInQuoted() {
    final long offerPx = 1_00000000L;
    // qty = 11 units in fixed-point — exceeds maxOrderSize of 10 units
    final long oversizedQty = 11L * PRICE_SCALE;

    final var slot =
        buildQuotedSlot(
            "QREQ-RISKFAIL-1", "QUOTE-RISKFAIL-1", SideEnum.Buy, 0L, offerPx, 0L, oversizedQty);

    final int len =
        encodePreviouslyQuotedNos("QUOTE-RISKFAIL-1", SideEnum.Buy, offerPx, oversizedQty);
    dispatch(len);

    // Rejected by check 11 (OrderExceedsMaxSize)
    assertEquals(1, session.messages.size());
    final var rej = decodeOrderRejected(session.messages.get(0));
    assertEquals(RejectReasonEnum.OrderExceedsMaxSize, rej.rejectReason());

    // Slot must still be QUOTED — peek was read-only, commit never executed
    assertEquals(
        RfqSlotState.QUOTED, slot.state, "slot must remain QUOTED when post-peek validation fails");
    assertEquals(0L, rfqMetrics.emitAccepted, "emitAccepted must stay 0 when commit never ran");
    // No §9.2a-specific reject counter should have fired
    assertEquals(0L, rfqMetrics.rejectUnknownQuote);
    assertEquals(0L, rfqMetrics.rejectQuoteSideMismatch);
    assertEquals(0L, rfqMetrics.rejectQuotePriceMismatch);
    assertEquals(0L, rfqMetrics.rejectQuoteQtyMismatch);
  }

  // =========================================================================
  // §9.2a Test 9 — commitPhase_allChecksPassed_slotTransitionsToAcceptedAndReleases
  // =========================================================================

  /**
   * After the full happy path: NOS-with-quoteId with all 12 checks passing, the commit phase (step
   * 13) must:
   *
   * <ul>
   *   <li>Transition the QUOTED slot to FREE (ACCEPTED is transient — committed atomically).
   *   <li>Increment {@code rfqMetrics.emitAccepted} to 1.
   *   <li>Remove the quoteId from {@code byQuoteId} so a subsequent peek returns null.
   * </ul>
   */
  @Test
  void commitPhase_allChecksPassed_slotTransitionsToAcceptedAndReleases() {
    final long offerPx = 1_00000000L;
    final long offerSize = 1L * PRICE_SCALE;
    final var slot =
        buildQuotedSlot(
            "QREQ-COMMIT-OK-1", "QUOTE-COMMIT-OK-1", SideEnum.Buy, 0L, offerPx, 0L, offerSize);

    final int len =
        encodePreviouslyQuotedNos("QUOTE-COMMIT-OK-1", SideEnum.Buy, offerPx, offerSize);
    dispatch(len);

    // Happy path: OrderCreatedEvent emitted
    assertEquals(1, session.messages.size());
    assertTemplateId(OrderCreatedEventDecoder.TEMPLATE_ID, session.messages.get(0));

    // Commit: slot is FREE and emitAccepted incremented
    assertEquals(RfqSlotState.FREE, slot.state, "slot must be FREE after commitAccept");
    assertEquals(1L, rfqMetrics.emitAccepted);

    // byQuoteId must no longer contain the quoteId
    final var peeked = rfqStateMachine.peekByQuoteId(slot.quoteIdBytes, 0, RfqSlot.QUOTE_ID_LENGTH);
    assertNull(peeked, "byQuoteId must not contain the committed quoteId");
  }
}
