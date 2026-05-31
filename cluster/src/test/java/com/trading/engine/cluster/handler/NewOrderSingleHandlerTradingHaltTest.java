package com.trading.engine.cluster.handler;

import static com.trading.engine.testsupport.sbe.SbeMessageAssertions.assertTemplateId;
import static com.trading.engine.testsupport.sbe.SbeTestDecoder.decodeOrderRejected;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the APP-152 trading-halt circuit breaker in {@link NewOrderSingleHandler}.
 *
 * <p>Exercises the halt-gate added as check 0 in {@code validateNewOrder}: when {@link
 * TradingState#isTradingHalted()} returns {@code true}, every incoming NOS must be rejected
 * immediately with {@link RejectReasonEnum#TradingHalted} before any other validation runs. The
 * tests verify admission on normal state, rejection under halt, resume semantics, short- circuit
 * ordering, and the invariant that halted rejections do not consume order-ID counter space.
 *
 * <p><b>Threading:</b> single-threaded — matches the cluster duty-cycle invariant.
 */
class NewOrderSingleHandlerTradingHaltTest {

  // -------------------------------------------------------------------------
  // Constants
  // -------------------------------------------------------------------------

  private static final long TS = 1_700_000_000_000_000_000L;
  private static final long ACCOUNT_ID = 1L;
  private static final String ACCOUNT_CODE = "ACME";

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
  private EventSink eventSink;
  private NewOrderSingleHandler handler;
  private TradingState tradingState;
  private FakeClientSession session;
  private FakeCluster fakeCluster;
  private MutableDirectBuffer msgBuf;

  @BeforeEach
  void setUp() {
    accountStore = new AccountStore();
    currencyStore = new CurrencyStore();
    riskLimitStore = new RiskLimitStore();
    seedRefData();

    fakeCluster = new FakeCluster(0L);
    final var orderBook = new OrderBook(128);
    final var orderIdGen = new IdGenerator("ORD");
    final var execIdGen = new IdGenerator("EXE");
    final var quoteIdGen = new IdGenerator("QTE");
    tradingState = new TradingState(orderBook, orderIdGen, execIdGen, quoteIdGen);

    final var sequencer = new EventSequencer();
    final var journal = new EventJournal(64);
    eventSink = new EventSink(sequencer, journal);
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
  // Dispatch helper
  // -------------------------------------------------------------------------

  /**
   * Encodes a valid Limit NOS with the given clOrdId and dispatches it, returning the session
   * message count after the call.
   *
   * @param clOrdId the ClOrdID (tag 11) for the NOS
   * @return number of messages captured by {@link #session} after dispatch
   */
  private int dispatchNos(final String clOrdId) {
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
    return session.messages.size();
  }

  // =========================================================================
  // Test 1 — default state (halt=false): NOS is admitted
  // =========================================================================

  /**
   * In the default state ({@code tradingHalted=false}) a valid Limit NOS must be admitted. An
   * {@code OrderCreatedEvent} must be emitted with no {@code OrderRejectedEvent}.
   */
  @Test
  void onCommand_tradingNotHalted_admits() {
    // Default state — tradingHalted is false.
    final int msgCount = dispatchNos("HALT-001");

    assertEquals(1, msgCount, "exactly one event must be emitted");
    assertTemplateId(OrderCreatedEventDecoder.TEMPLATE_ID, session.messages.get(0));
  }

  // =========================================================================
  // Test 2 — halt set before dispatch: NOS rejected with TradingHalted
  // =========================================================================

  /**
   * When {@link TradingState#setTradingHalted(boolean)} is called with {@code true} before the NOS
   * is dispatched, the handler must emit an {@code OrderRejectedEvent} with reason {@link
   * RejectReasonEnum#TradingHalted} and rejection text containing {@code "trading halted"}.
   */
  @Test
  void onCommand_tradingHalted_rejectsWithTradingHalted() {
    tradingState.setTradingHalted(true);

    final int msgCount = dispatchNos("HALT-002");

    assertEquals(1, msgCount, "exactly one rejection event must be emitted");
    final var rej = decodeOrderRejected(session.messages.get(0));
    assertEquals(
        RejectReasonEnum.TradingHalted,
        rej.rejectReason(),
        "rejection reason must be TradingHalted");
    final var text = rej.text();
    assertTrue(
        text.contains("trading halted"),
        () -> "rejection text must contain 'trading halted', got: " + text);
  }

  // =========================================================================
  // Test 3 — halt then resume: second NOS admitted after halt lifted
  // =========================================================================

  /**
   * Setting halt to {@code true} and then back to {@code false} must restore normal admission. The
   * first NOS (sent while halted) must produce a {@code TradingHalted} rejection; the second NOS
   * (sent after resume, different clOrdId) must produce an {@code OrderCreatedEvent}.
   */
  @Test
  void onCommand_tradingHaltedThenResumed_admitsAfterResume() {
    tradingState.setTradingHalted(true);
    dispatchNos("HALT-003A");

    // First message: TradingHalted rejection.
    assertEquals(1, session.messages.size(), "halted dispatch must emit exactly one rejection");
    final var rej = decodeOrderRejected(session.messages.get(0));
    assertEquals(RejectReasonEnum.TradingHalted, rej.rejectReason());

    // Resume trading.
    tradingState.setTradingHalted(false);
    dispatchNos("HALT-003B");

    // Second message: OrderCreatedEvent.
    assertEquals(2, session.messages.size(), "resumed dispatch must emit an admit event");
    assertTemplateId(OrderCreatedEventDecoder.TEMPLATE_ID, session.messages.get(1));
  }

  // =========================================================================
  // Test 4 — halt check runs FIRST: TradingHalted beats InvalidQuantity
  // =========================================================================

  /**
   * A NOS that carries {@code qty=0} (would normally fail check 2, {@code InvalidQuantity}) while
   * the cluster is halted must be rejected with {@link RejectReasonEnum#TradingHalted} — not {@link
   * RejectReasonEnum#InvalidQuantity}. This confirms that check 0 (APP-152) short-circuits every
   * subsequent validation gate.
   */
  @Test
  void onCommand_tradingHalted_rejectsBeforeOtherValidation() {
    tradingState.setTradingHalted(true);

    // Encode a NOS with qty=0, which would normally trigger the InvalidQuantity reject (check 2).
    final int len =
        SbeTestEncoder.encodeNewOrderSingle(
            msgBuf,
            0,
            "HALT-004",
            "EURUSD",
            SideEnum.Buy,
            OrdTypeEnum.Limit,
            VALID_PRICE,
            0L /* qty=0 — would fail check 2 if halt check were absent */,
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

    assertEquals(1, session.messages.size(), "exactly one rejection event must be emitted");
    final var rej = decodeOrderRejected(session.messages.get(0));
    assertEquals(
        RejectReasonEnum.TradingHalted,
        rej.rejectReason(),
        "halt check must fire before quantity check — rejection must be TradingHalted, not "
            + "InvalidQuantity");
  }

  // =========================================================================
  // Test 5 — halted reject does not consume order-ID counter space
  // =========================================================================

  /**
   * A rejected NOS on a halted cluster must not advance the deterministic order-ID counter in
   * {@link IdGenerator}. After one halted rejection followed by one admitted order (after resume),
   * the admitted order's counter value must be {@code 1} — not {@code 2} — proving that the halt
   * path skips {@link TradingState#generateOrderId()}.
   *
   * <p>Technique: read {@link TradingState#orderIdGen()} → {@link IdGenerator#currentCounter()}
   * immediately after the admitted order. The counter reflects exactly how many IDs have been
   * generated (each call to {@code generateOrderId()} increments it by 1). If the halted path had
   * called {@code generateOrderId()}, the counter would be 2; it must be 1.
   */
  @Test
  void onCommand_tradingHalted_doesNotConsumeOrderId() {
    // Dispatch while halted — must NOT advance the ID counter.
    tradingState.setTradingHalted(true);
    dispatchNos("HALT-005A");
    assertEquals(1, session.messages.size(), "halted dispatch must emit exactly one rejection");
    // Counter must still be at 0 (no ID generated).
    final long counterAfterHalt = tradingState.orderIdGen().currentCounter();
    assertEquals(0L, counterAfterHalt, "order-ID counter must not advance when trading is halted");

    // Resume and dispatch a valid NOS — counter must advance to exactly 1.
    tradingState.setTradingHalted(false);
    dispatchNos("HALT-005B");
    assertEquals(2, session.messages.size(), "resumed dispatch must emit an admit event");
    assertTemplateId(OrderCreatedEventDecoder.TEMPLATE_ID, session.messages.get(1));

    final long counterAfterAdmit = tradingState.orderIdGen().currentCounter();
    assertEquals(
        1L,
        counterAfterAdmit,
        "order-ID counter must be 1 after exactly one admitted order — halted path must not "
            + "have consumed a counter slot");
  }
}
