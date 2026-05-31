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
import com.trading.engine.cluster.refdata.RiskLimitFixtures;
import com.trading.engine.cluster.refdata.RiskLimitStore;
import com.trading.engine.cluster.sequencer.EventSequencer;
import com.trading.engine.cluster.state.RfqStateMachine;
import com.trading.engine.cluster.state.TradingState;
import com.trading.engine.messages.FixedPointScale;
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
 * Unit tests for the APP-62 first-slice max-order-notional pre-trade check in {@link
 * NewOrderSingleHandler}.
 *
 * <p>Exercises check 11b ({@code orderNotional > riskLimit.maxOrderNotional()}), the boundary
 * conditions (equal notional admitted, zero sentinel treated as unlimited), the Limit-only guard
 * (Market orders bypass the check), and the overflow-saturation path in {@link
 * NewOrderSingleHandler#computeNotionalSaturating(long, long)}.
 *
 * <p><b>Threading:</b> single-threaded — matches the cluster duty-cycle invariant.
 */
class NewOrderSingleHandlerMaxNotionalTest {

  // -------------------------------------------------------------------------
  // Constants
  // -------------------------------------------------------------------------

  private static final long TS = 1_700_000_000_000_000_000L;
  private static final long ACCOUNT_ID = 1L;
  private static final String ACCOUNT_CODE = "ACME";

  /** 1 unit in fixed-point 10^-8 (= PRICE_SCALE). */
  private static final long ONE_UNIT = FixedPointScale.PRICE_SCALE;

  /**
   * Tight notional ceiling used for rejection tests: 5 × PRICE_SCALE == "5 units of base currency".
   * Any Limit order where qty × price / PRICE_SCALE > 5 * PRICE_SCALE must be rejected.
   */
  private static final long TIGHT_NOTIONAL_LIMIT = 5L * FixedPointScale.PRICE_SCALE;

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

  @BeforeEach
  void setUp() {
    accountStore = new AccountStore();
    currencyStore = new CurrencyStore();
    riskLimitStore = new RiskLimitStore();

    fakeCluster = new FakeCluster(0L);
    final var orderBook = new OrderBook(128);
    final var orderIdGen = new IdGenerator("ORD");
    final var execIdGen = new IdGenerator("EXE");
    final var quoteIdGen = new IdGenerator("QTE");
    final var tradingState = new TradingState(orderBook, orderIdGen, execIdGen, quoteIdGen);
    final var sequencer = new EventSequencer();
    final var journal = new EventJournal(64);
    eventSink = new EventSink(sequencer, journal);
    eventSink.setCluster(fakeCluster);

    final var rfqMetrics = new RfqMetrics();
    final var rfqStateMachine = buildRfqStateMachine();

    handler = new NewOrderSingleHandler(tradingState, accountStore, currencyStore, riskLimitStore);
    handler.wireRfqStateMachine(rfqStateMachine, rfqMetrics);

    session = new FakeClientSession(42L);
    fakeCluster.addClientSession(session);
    msgBuf = new ExpandableArrayBuffer(512);
  }

  // -------------------------------------------------------------------------
  // Ref-data seed helpers
  // -------------------------------------------------------------------------

  /**
   * Seeds account and currency ref-data. The risk limit is seeded separately per test so each test
   * can inject its own notional ceiling.
   */
  private void seedAccountAndCurrencies() {
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
  }

  /**
   * Seeds a permissive risk limit with the given {@code maxOrderNotional} ceiling and a large
   * maxOrderSize so the size check (11) never fires before check 11b.
   *
   * @param maxOrderNotional fixed-point notional ceiling; {@code 0} = unlimited
   */
  private void seedRiskLimit(final long maxOrderNotional) {
    // maxOrderSize = 1_000_000 whole units — effectively unlimited for test quantities.
    riskLimitStore.put(
        RiskLimitFixtures.riskLimit(
            ACCOUNT_ID, 1_000_000L * FixedPointScale.PRICE_SCALE, maxOrderNotional, 0L));
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
   * Encodes and dispatches a Limit NOS with the given qty/price and returns the session message
   * count after the call.
   *
   * @param clOrdId unique client order ID to avoid dedup collisions between tests
   * @param ordType order type (Limit or Market)
   * @param qty order quantity in fixed-point 10^-8
   * @param price limit price in fixed-point 10^-8; set to 0 for Market orders
   * @return number of messages captured by {@link #session} after dispatch
   */
  private int dispatchNos(
      final String clOrdId, final OrdTypeEnum ordType, final long qty, final long price) {
    final int len =
        SbeTestEncoder.encodeNewOrderSingle(
            msgBuf, 0, clOrdId, "EURUSD", SideEnum.Buy, ordType, price, qty, ACCOUNT_CODE, "USD");
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
  // Test 1 — order under notional limit → admitted
  // =========================================================================

  /**
   * A Limit order whose notional (qty × price / PRICE_SCALE) is strictly below the account
   * maxOrderNotional ceiling must be admitted — an {@code OrderCreatedEvent} is emitted.
   *
   * <p>Setup: ceiling = 5 × PRICE_SCALE. Order: qty = 3 × PRICE_SCALE, price = 1 × PRICE_SCALE →
   * notional = 3 × PRICE_SCALE, which is &lt; 5 × PRICE_SCALE.
   */
  @Test
  void onCommand_orderUnderMaxNotional_admits() {
    seedAccountAndCurrencies();
    seedRiskLimit(TIGHT_NOTIONAL_LIMIT); // ceiling = 5 units

    final long qty = 3L * ONE_UNIT; // notional = 3 * PRICE_SCALE < 5 * PRICE_SCALE
    final long price = ONE_UNIT; // 1.0
    final int msgCount = dispatchNos("NTL-001", OrdTypeEnum.Limit, qty, price);

    assertEquals(1, msgCount, "exactly one event must be emitted");
    assertTemplateId(OrderCreatedEventDecoder.TEMPLATE_ID, session.messages.get(0));
  }

  // =========================================================================
  // Test 2 — order over notional limit → rejected with OrderExceedsMaxSize
  // =========================================================================

  /**
   * A Limit order whose notional exceeds the account maxOrderNotional ceiling must be rejected with
   * {@link RejectReasonEnum#OrderExceedsMaxSize} and the rejection text must contain the substring
   * {@code "orderNotional exceeds"}.
   *
   * <p>Setup: ceiling = 5 × PRICE_SCALE. Order: qty = 6 × PRICE_SCALE, price = 1 × PRICE_SCALE →
   * notional = 6 × PRICE_SCALE, which is &gt; 5 × PRICE_SCALE.
   */
  @Test
  void onCommand_orderOverMaxNotional_rejectsWithOrderExceedsMaxSize() {
    seedAccountAndCurrencies();
    seedRiskLimit(TIGHT_NOTIONAL_LIMIT); // ceiling = 5 units

    final long qty = 6L * ONE_UNIT; // notional = 6 * PRICE_SCALE > 5 * PRICE_SCALE
    final long price = ONE_UNIT; // 1.0
    final int msgCount = dispatchNos("NTL-002", OrdTypeEnum.Limit, qty, price);

    assertEquals(1, msgCount, "exactly one event (rejection) must be emitted");
    final var rej = decodeOrderRejected(session.messages.get(0));
    assertEquals(
        RejectReasonEnum.OrderExceedsMaxSize,
        rej.rejectReason(),
        "rejection reason must be OrderExceedsMaxSize");
    final var text = rej.text();
    assertTrue(
        text.contains("orderNotional exceeds"),
        () -> "rejection text must contain 'orderNotional exceeds', got: " + text);
  }

  // =========================================================================
  // Test 3 — notional exactly equals limit → admitted (check is strict >)
  // =========================================================================

  /**
   * The notional guard uses a strict {@code >} comparison, so an order whose notional equals the
   * ceiling exactly must be admitted (not rejected).
   *
   * <p>Setup: ceiling = 5 × PRICE_SCALE. Order: qty = 5 × PRICE_SCALE, price = 1 × PRICE_SCALE →
   * notional = 5 × PRICE_SCALE == ceiling → admitted.
   */
  @Test
  void onCommand_orderEqualsMaxNotional_admits() {
    seedAccountAndCurrencies();
    seedRiskLimit(TIGHT_NOTIONAL_LIMIT); // ceiling = 5 units

    final long qty = 5L * ONE_UNIT; // notional = 5 * PRICE_SCALE == ceiling
    final long price = ONE_UNIT; // 1.0
    final int msgCount = dispatchNos("NTL-003", OrdTypeEnum.Limit, qty, price);

    assertEquals(1, msgCount, "boundary order at exactly the limit must be admitted");
    assertTemplateId(OrderCreatedEventDecoder.TEMPLATE_ID, session.messages.get(0));
  }

  // =========================================================================
  // Test 4 — Market order bypasses notional check → admitted
  // =========================================================================

  /**
   * The notional check is conditioned on {@code ordType == OrdTypeEnum.Limit}. A Market order must
   * bypass the check entirely and be admitted regardless of how large its qty is, because the
   * executed notional is unknown at order entry time.
   *
   * <p>Setup: tight notional ceiling. Order: Market, qty = 1_000 × PRICE_SCALE, price = 0 →
   * notional check is skipped → admitted.
   */
  @Test
  void onCommand_marketOrderSkipsNotionalCheck_admits() {
    seedAccountAndCurrencies();
    seedRiskLimit(TIGHT_NOTIONAL_LIMIT); // ceiling = 5 units — would block if check ran

    // Permissive risk limit is also needed to pass the maxOrderSize check (11).
    // Override with a risk limit that also allows the large qty through the size gate.
    riskLimitStore.put(
        RiskLimitFixtures.riskLimit(
            ACCOUNT_ID,
            1_000_000L * FixedPointScale.PRICE_SCALE /* maxOrderSize — very large */,
            TIGHT_NOTIONAL_LIMIT /* maxOrderNotional — tight, but must be bypassed */,
            0L));

    final long qty = 1_000L * ONE_UNIT; // would be 1000 units notional at price=1.0
    final long price = 0L; // Market order — price=0, notional check must be skipped
    final int msgCount = dispatchNos("NTL-004", OrdTypeEnum.Market, qty, price);

    assertEquals(1, msgCount, "Market order must bypass notional check and be admitted");
    assertTemplateId(OrderCreatedEventDecoder.TEMPLATE_ID, session.messages.get(0));
  }

  // =========================================================================
  // Test 5 — maxOrderNotional = 0 (unlimited sentinel) → large notional admitted
  // =========================================================================

  /**
   * A {@code maxOrderNotional} of {@code 0} is the sentinel for "unlimited" — the notional check is
   * skipped when the account has no notional ceiling configured. An order with an otherwise
   * enormous notional must be admitted.
   *
   * <p>Setup: maxOrderNotional = 0. Order: qty = 1_000 × PRICE_SCALE, price = 1_000 × PRICE_SCALE →
   * very large notional → admitted because the gate is disabled.
   */
  @Test
  void onCommand_zeroMaxNotional_treatedAsUnlimited_admits() {
    seedAccountAndCurrencies();
    seedRiskLimit(0L); // 0 = unlimited sentinel — no notional ceiling

    final long qty = 1_000L * ONE_UNIT;
    final long price = 1_000L * ONE_UNIT; // notional = 1_000_000 × PRICE_SCALE
    final int msgCount = dispatchNos("NTL-005", OrdTypeEnum.Limit, qty, price);

    assertEquals(1, msgCount, "unlimited notional (maxOrderNotional=0) must admit any notional");
    assertTemplateId(OrderCreatedEventDecoder.TEMPLATE_ID, session.messages.get(0));
  }

  // =========================================================================
  // Test 6 — qty × price overflows long → saturates to MAX_VALUE → rejected
  // =========================================================================

  /**
   * When the intermediate product {@code orderQty × price} overflows signed-long range, {@link
   * NewOrderSingleHandler#computeNotionalSaturating(long, long)} saturates to {@link
   * Long#MAX_VALUE}. Because {@code Long.MAX_VALUE > any finite notional limit}, the check must
   * reject the order.
   *
   * <p>Setup: tight notional ceiling = 5 × PRICE_SCALE. Order:
   *
   * <ul>
   *   <li>qty = 100_000 × PRICE_SCALE (= 1e13) — fits under maxOrderSize = 1_000_000 × PRICE_SCALE
   *   <li>price = 10_000_000 × PRICE_SCALE (= 1e15) — both positive, product ≈ 1e28 >> 64-bit max
   * </ul>
   *
   * {@code Math.multiplyHigh(1e13, 1e15) != 0} → saturation to {@link Long#MAX_VALUE} → rejected.
   */
  @Test
  void onCommand_qtyPriceOverflowsLong_saturatesToMaxValue_rejects() {
    seedAccountAndCurrencies();
    // maxOrderSize must exceed qty = 100_000 × PRICE_SCALE so check 11 does not fire first.
    // seedRiskLimit sets maxOrderSize = 1_000_000 × PRICE_SCALE, which is 10× larger.
    seedRiskLimit(TIGHT_NOTIONAL_LIMIT); // any finite ceiling will do

    // qty × price = 1e13 × 1e15 = 1e28 — overflows 64-bit signed long (max ≈ 9.2e18).
    // Math.multiplyHigh returns non-zero high word → computeNotionalSaturating returns MAX_VALUE.
    final long qty = 100_000L * FixedPointScale.PRICE_SCALE; // 1e13, below maxOrderSize 1e14
    final long price = 10_000_000L * FixedPointScale.PRICE_SCALE; // 1e15
    final int msgCount = dispatchNos("NTL-006", OrdTypeEnum.Limit, qty, price);

    assertEquals(1, msgCount, "overflow-saturated notional must be rejected");
    final var rej = decodeOrderRejected(session.messages.get(0));
    assertEquals(
        RejectReasonEnum.OrderExceedsMaxSize,
        rej.rejectReason(),
        "overflow must produce OrderExceedsMaxSize rejection");
    final var text = rej.text();
    assertTrue(
        text.contains("orderNotional exceeds"),
        () -> "rejection text must contain 'orderNotional exceeds', got: " + text);
  }

  // =========================================================================
  // Test 7 — computeNotionalSaturating: normal inputs → correct result
  // =========================================================================

  /**
   * Direct unit test of {@link NewOrderSingleHandler#computeNotionalSaturating(long, long)}.
   * Verifies the formula {@code (qty × price) / PRICE_SCALE} for well-behaved inputs:
   *
   * <ul>
   *   <li>qty = 10 × PRICE_SCALE (= 10 whole units)
   *   <li>price = 2 × PRICE_SCALE (= 2.0)
   *   <li>expected notional = 10 × 2 × PRICE_SCALE² / PRICE_SCALE = 20 × PRICE_SCALE
   * </ul>
   */
  @Test
  void computeNotionalSaturating_normalInputs_returnsCorrectNotional() {
    final long qty = 10L * FixedPointScale.PRICE_SCALE;
    final long price = 2L * FixedPointScale.PRICE_SCALE;
    final long expected = 20L * FixedPointScale.PRICE_SCALE;

    final long notional = NewOrderSingleHandler.computeNotionalSaturating(qty, price);

    assertEquals(
        expected, notional, "notional for 10 units at price 2.0 must equal 20 × PRICE_SCALE");
  }

  // =========================================================================
  // Test 8 — computeNotionalSaturating: intermediate overflow → Long.MAX_VALUE
  // =========================================================================

  /**
   * Direct unit test of the overflow-saturation branch in {@link
   * NewOrderSingleHandler#computeNotionalSaturating(long, long)}. When {@code
   * Math.multiplyHigh(qty, price) != 0}, the method must return {@link Long#MAX_VALUE} without
   * performing the divide.
   *
   * <p>Input: qty = {@link Long#MAX_VALUE}, price = 2 → high 64 bits of the 128-bit product are
   * non-zero → saturation to {@code Long.MAX_VALUE}.
   */
  @Test
  void computeNotionalSaturating_intermediateOverflow_returnsLongMaxValue() {
    final long qty = Long.MAX_VALUE;
    final long price = 2L;

    final long notional = NewOrderSingleHandler.computeNotionalSaturating(qty, price);

    assertEquals(
        Long.MAX_VALUE,
        notional,
        "overflowing product (qty=Long.MAX_VALUE, price=2) must saturate to Long.MAX_VALUE");
  }
}
