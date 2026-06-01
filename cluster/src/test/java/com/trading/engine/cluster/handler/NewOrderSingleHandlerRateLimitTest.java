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
import com.trading.engine.cluster.refdata.RiskLimitState;
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
 * Unit tests for the APP-62 slice 2 per-account rate-limit pre-trade check (check 11c) in {@link
 * NewOrderSingleHandler}.
 *
 * <p>Exercises the {@code tryConsumeRateToken} path: the unlimited sentinel ({@code
 * maxOrdersPerSecond=0}), admission up to the limit then rejection, 1-second window transitions,
 * per-account bucket isolation, the invariant that a rejected order does not consume a rate token,
 * and the invariant that a non-rate-limit rejection before check 11c does not consume capacity.
 *
 * <p><b>Threading:</b> single-threaded — matches the cluster duty-cycle invariant.
 */
class NewOrderSingleHandlerRateLimitTest {

  // -------------------------------------------------------------------------
  // Constants
  // -------------------------------------------------------------------------

  /** Epoch-nanos within bucket second 5 (5_000_000_000 / 1e9 = 5). */
  private static final long T0 = 5_000_000_000L;

  /** Epoch-nanos within bucket second 6 — a fresh 1-second window. */
  private static final long T1 = 6_000_000_000L;

  private static final long ACCOUNT_ID_A = 1L;
  private static final long ACCOUNT_ID_B = 2L;
  private static final String ACCOUNT_CODE_A = "ACME";
  private static final String ACCOUNT_CODE_B = "BETA";

  /** 1 unit in fixed-point 10^-8 (= PRICE_SCALE). */
  private static final long VALID_QTY = FixedPointScale.PRICE_SCALE;

  /** 1.0 in fixed-point 10^-8. */
  private static final long VALID_PRICE = FixedPointScale.PRICE_SCALE;

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

  /**
   * Seeds account A and common currencies. Risk limits are seeded separately so each test can
   * choose the {@code maxOrdersPerSecond} ceiling.
   */
  private void seedAccountA() {
    accountStore.put(
        AccountFixtures.account(
            ACCOUNT_ID_A,
            ACCOUNT_CODE_A,
            AccountStatusEnum.Active,
            AccountState.Capabilities.CAN_TRADE));
    currencyStore.put(
        CurrencyStore.packCode((byte) 'U', (byte) 'S', (byte) 'D'), CurrencyFixtures.usd());
    currencyStore.put(
        CurrencyStore.packCode((byte) 'E', (byte) 'U', (byte) 'R'), CurrencyFixtures.eur());
  }

  /**
   * Seeds account B with matching currencies. Assumes {@link #seedAccountA()} has already populated
   * the currency entries (they share the same currency store).
   */
  private void seedAccountB() {
    accountStore.put(
        AccountFixtures.account(
            ACCOUNT_ID_B,
            ACCOUNT_CODE_B,
            AccountStatusEnum.Active,
            AccountState.Capabilities.CAN_TRADE));
  }

  /**
   * Seeds a risk limit for account A with the given per-second order cap and an effectively
   * unlimited maxOrderSize so size check 11 never fires before check 11c.
   *
   * @param maxOrdersPerSecond per-second admission cap; {@code 0} = unlimited
   */
  private void seedRiskLimitA(final long maxOrdersPerSecond) {
    final var limit = riskLimit(ACCOUNT_ID_A, maxOrdersPerSecond);
    riskLimitStore.put(limit);
  }

  /**
   * Seeds a risk limit for account B with the given per-second order cap.
   *
   * @param maxOrdersPerSecond per-second admission cap; {@code 0} = unlimited
   */
  private void seedRiskLimitB(final long maxOrdersPerSecond) {
    final var limit = riskLimit(ACCOUNT_ID_B, maxOrdersPerSecond);
    riskLimitStore.put(limit);
  }

  /**
   * Builds a {@link RiskLimitState} with a large maxOrderSize (effectively unlimited) and the given
   * {@code maxOrdersPerSecond}. All other gates are disabled ({@code 0} = unlimited).
   *
   * @param accountId the owning account's numeric identifier
   * @param maxOrdersPerSecond per-second admission cap; {@code 0} = unlimited
   * @return a fresh {@link RiskLimitState} ready for insertion
   */
  private static RiskLimitState riskLimit(final long accountId, final long maxOrdersPerSecond) {
    // Use 1_000_000 whole units as an effectively unlimited maxOrderSize so size check 11 never
    // fires before check 11c during these rate-limit tests.
    final var limit =
        RiskLimitFixtures.riskLimit(
            accountId,
            1_000_000L * FixedPointScale.PRICE_SCALE /* maxOrderSize */,
            0L /* maxOrderNotional — unlimited */,
            0L /* maxDailyVolume — unlimited */);
    limit.setMaxOrdersPerSecond(maxOrdersPerSecond);
    return limit;
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
   * Encodes and dispatches a Limit NOS for account A at the given cluster timestamp.
   *
   * @param clOrdId unique client order ID — callers must ensure no dedup collision
   * @param clusterTimestamp epoch-nanos assigned by the cluster
   */
  private void dispatchNosA(final String clOrdId, final long clusterTimestamp) {
    dispatchNos(clOrdId, ACCOUNT_CODE_A, clusterTimestamp);
  }

  /**
   * Encodes and dispatches a Limit NOS for account B at the given cluster timestamp.
   *
   * @param clOrdId unique client order ID
   * @param clusterTimestamp epoch-nanos assigned by the cluster
   */
  private void dispatchNosB(final String clOrdId, final long clusterTimestamp) {
    dispatchNos(clOrdId, ACCOUNT_CODE_B, clusterTimestamp);
  }

  /**
   * Encodes and dispatches a Limit NOS with the given account code at the given cluster timestamp.
   * Uses fixed valid qty/price (1.0 × PRICE_SCALE) so all checks except 11c pass.
   *
   * @param clOrdId unique client order ID
   * @param accountCode FIX tag 1 account code
   * @param clusterTimestamp epoch-nanos assigned by the cluster
   */
  private void dispatchNos(
      final String clOrdId, final String accountCode, final long clusterTimestamp) {
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
            accountCode,
            "USD");
    handler.onCommand(
        session,
        clusterTimestamp,
        msgBuf,
        0,
        len,
        NewOrderSingleDecoder.BLOCK_LENGTH,
        NewOrderSingleDecoder.SCHEMA_VERSION,
        eventSink);
  }

  /**
   * Encodes and dispatches a Limit NOS for account A with {@code qty=0} — a quantity that triggers
   * check 2 (InvalidQuantity) before check 11c ever fires. Used to verify that a pre-rate-limit
   * rejection does not consume rate-limit capacity.
   *
   * @param clOrdId unique client order ID
   * @param clusterTimestamp epoch-nanos assigned by the cluster
   */
  private void dispatchNosA_invalidQty(final String clOrdId, final long clusterTimestamp) {
    final int len =
        SbeTestEncoder.encodeNewOrderSingle(
            msgBuf,
            0,
            clOrdId,
            "EURUSD",
            SideEnum.Buy,
            OrdTypeEnum.Limit,
            VALID_PRICE,
            0L /* qty=0 — triggers InvalidQuantity at check 2 */,
            ACCOUNT_CODE_A,
            "USD");
    handler.onCommand(
        session,
        clusterTimestamp,
        msgBuf,
        0,
        len,
        NewOrderSingleDecoder.BLOCK_LENGTH,
        NewOrderSingleDecoder.SCHEMA_VERSION,
        eventSink);
  }

  // =========================================================================
  // Test 1 — maxOrdersPerSecond = 0 (unlimited sentinel) → 100 admissions all pass
  // =========================================================================

  /**
   * A {@code maxOrdersPerSecond} of {@code 0} is the unlimited sentinel: the rate-limit gate is
   * entirely skipped. Submitting 100 orders within the same 1-second bucket must produce 100 {@code
   * OrderCreatedEvent} admissions with no {@code RateLimitExceeded} rejection.
   */
  @Test
  void limitNotConfigured_admits_unboundedSubmissions() {
    seedAccountA();
    seedRiskLimitA(0L /* unlimited */);

    for (int i = 0; i < 100; i++) {
      dispatchNosA("UNBD-" + i, T0);
    }

    assertEquals(100, session.messages.size(), "all 100 orders must be admitted");
    for (int i = 0; i < 100; i++) {
      assertTemplateId(OrderCreatedEventDecoder.TEMPLATE_ID, session.messages.get(i));
    }
  }

  // =========================================================================
  // Test 2 — maxOrdersPerSecond = 3 → first 3 admit, orders 4 and 5 reject
  // =========================================================================

  /**
   * When {@code maxOrdersPerSecond=3} and five orders arrive in the same 1-second bucket, the first
   * three must be admitted (OrderCreatedEvent) and orders 4 and 5 must be rejected with {@link
   * RejectReasonEnum#RateLimitExceeded}.
   */
  @Test
  void limitConfigured_admitsUpToLimit_thenRejects() {
    seedAccountA();
    seedRiskLimitA(3L);

    // Orders 1-3 must admit.
    dispatchNosA("RL-001", T0);
    dispatchNosA("RL-002", T0);
    dispatchNosA("RL-003", T0);

    assertEquals(3, session.messages.size(), "first 3 orders must be admitted");
    assertTemplateId(OrderCreatedEventDecoder.TEMPLATE_ID, session.messages.get(0));
    assertTemplateId(OrderCreatedEventDecoder.TEMPLATE_ID, session.messages.get(1));
    assertTemplateId(OrderCreatedEventDecoder.TEMPLATE_ID, session.messages.get(2));

    // Orders 4 and 5 must be rejected with RateLimitExceeded.
    dispatchNosA("RL-004", T0);
    dispatchNosA("RL-005", T0);

    assertEquals(5, session.messages.size(), "5 total events must be emitted");

    final var rej4 = decodeOrderRejected(session.messages.get(3));
    assertEquals(
        RejectReasonEnum.RateLimitExceeded,
        rej4.rejectReason(),
        "order 4 must be rejected with RateLimitExceeded");
    assertTrue(
        rej4.text().contains("rate limit"),
        () -> "rejection text must mention 'rate limit', got: " + rej4.text());

    final var rej5 = decodeOrderRejected(session.messages.get(4));
    assertEquals(
        RejectReasonEnum.RateLimitExceeded,
        rej5.rejectReason(),
        "order 5 must be rejected with RateLimitExceeded");
  }

  // =========================================================================
  // Test 3 — new 1-second window resets the bucket count
  // =========================================================================

  /**
   * When {@code maxOrdersPerSecond=2} and two orders are submitted at {@link #T0} (bucket=5)
   * followed by two orders at {@link #T1} (bucket=6), all four orders must be admitted. The bucket
   * transition resets the counter from 2 back to 0, allowing the limit to be reached again in the
   * new window.
   */
  @Test
  void tryConsumeRateToken_newWindow_resetsCount() {
    seedAccountA();
    seedRiskLimitA(2L);

    // First window: bucket second 5.
    dispatchNosA("NW-001", T0);
    dispatchNosA("NW-002", T0);

    assertEquals(2, session.messages.size(), "both T0 orders must be admitted");
    assertTemplateId(OrderCreatedEventDecoder.TEMPLATE_ID, session.messages.get(0));
    assertTemplateId(OrderCreatedEventDecoder.TEMPLATE_ID, session.messages.get(1));

    // Second window: bucket second 6 — counter resets.
    dispatchNosA("NW-003", T1);
    dispatchNosA("NW-004", T1);

    assertEquals(4, session.messages.size(), "all 4 orders (across two windows) must be admitted");
    assertTemplateId(OrderCreatedEventDecoder.TEMPLATE_ID, session.messages.get(2));
    assertTemplateId(OrderCreatedEventDecoder.TEMPLATE_ID, session.messages.get(3));
  }

  // =========================================================================
  // Test 4 — two accounts have independent per-account buckets
  // =========================================================================

  /**
   * Rate-limit state is keyed by {@code accountId}, so account A and account B each maintain their
   * own independent bucket. When both accounts have {@code maxOrdersPerSecond=1} and each submits
   * one order at the same cluster timestamp, both must be admitted — account B's admission must not
   * interfere with account A's counter.
   */
  @Test
  void tryConsumeRateToken_differentAccounts_haveIndependentBuckets() {
    seedAccountA();
    seedAccountB();
    seedRiskLimitA(1L);
    seedRiskLimitB(1L);

    dispatchNosA("DA-001", T0);
    dispatchNosB("DB-001", T0);

    assertEquals(2, session.messages.size(), "one admission per account must be emitted");
    assertTemplateId(OrderCreatedEventDecoder.TEMPLATE_ID, session.messages.get(0));
    assertTemplateId(OrderCreatedEventDecoder.TEMPLATE_ID, session.messages.get(1));
  }

  // =========================================================================
  // Test 5 — rejected order does not consume a rate token
  // =========================================================================

  /**
   * When an order is rejected by the rate-limit gate, the {@code accountRateState} counter must NOT
   * be incremented. To verify this: admit 2 orders (exhausting the limit for window T0), let order
   * 3 be rejected (still in T0), then submit order 4 in window T1. Order 4 must be admitted — the
   * limit-exceeded branch must have left the token count at exactly 2 (not 3), allowing the T1
   * bucket to start fresh.
   */
  @Test
  void tryConsumeRateToken_atLimitBoundary_rejectionDoesNotConsumeToken() {
    seedAccountA();
    seedRiskLimitA(2L);

    // Exhaust the limit in T0.
    dispatchNosA("RDT-001", T0);
    dispatchNosA("RDT-002", T0);

    assertEquals(2, session.messages.size());
    assertTemplateId(OrderCreatedEventDecoder.TEMPLATE_ID, session.messages.get(0));
    assertTemplateId(OrderCreatedEventDecoder.TEMPLATE_ID, session.messages.get(1));

    // Order 3 hits the rate limit in T0 — must reject without poisoning the bucket.
    dispatchNosA("RDT-003", T0);
    assertEquals(3, session.messages.size());
    final var rej = decodeOrderRejected(session.messages.get(2));
    assertEquals(RejectReasonEnum.RateLimitExceeded, rej.rejectReason());

    // Order 4 arrives in T1 — fresh window; must be admitted.
    dispatchNosA("RDT-004", T1);
    assertEquals(4, session.messages.size(), "T1 order must be admitted after bucket transition");
    assertTemplateId(OrderCreatedEventDecoder.TEMPLATE_ID, session.messages.get(3));
  }

  // =========================================================================
  // Test 6 — non-rate-limit rejection does not consume a rate token
  // =========================================================================

  /**
   * Check 11c fires only after checks 1–11b all pass. An order that fails at check 2
   * (InvalidQuantity, {@code qty=0}) must not increment the per-account rate counter, because the
   * rate-limit logic is never reached. After that failed order, two valid orders in the same bucket
   * must both be admitted — they would be rejected if the invalid order had consumed a token (when
   * the limit is 2).
   */
  @Test
  void check11c_nonRateLimitRejection_doesNotConsumeRateToken() {
    seedAccountA();
    seedRiskLimitA(2L);

    // Order 1 fails at check 2 (qty=0 → InvalidQuantity) before reaching check 11c.
    dispatchNosA_invalidQty("NRLT-001", T0);
    assertEquals(1, session.messages.size());
    final var rej1 = decodeOrderRejected(session.messages.get(0));
    assertEquals(
        RejectReasonEnum.InvalidQuantity,
        rej1.rejectReason(),
        "order with qty=0 must be rejected with InvalidQuantity, not RateLimitExceeded");

    // Orders 2 and 3 are valid and must both be admitted (limit=2, counter still at 0 after
    // the InvalidQuantity reject since check 11c was never reached).
    dispatchNosA("NRLT-002", T0);
    dispatchNosA("NRLT-003", T0);

    assertEquals(3, session.messages.size(), "both valid orders must be admitted");
    assertTemplateId(OrderCreatedEventDecoder.TEMPLATE_ID, session.messages.get(1));
    assertTemplateId(OrderCreatedEventDecoder.TEMPLATE_ID, session.messages.get(2));
  }

  // =========================================================================
  // Test 7 — APP-62 R10 HIGH (review iter 1): peek-only checks (11e/11f/11g)
  //         run BEFORE the mutating rate-limit (11c) and daily-volume (11d) gates
  // =========================================================================

  /**
   * APP-62 R10 HIGH (review iteration 1) — a symbol-eligibility reject (check 11g) must NOT consume
   * rate-limit capacity. Pre-fix, the original check ordering was 11c → 11d → 11e → 11f → 11g, so a
   * flurry of restricted-symbol orders (11g rejects) drained a legitimate trader's rate budget — a
   * §G DoS vector. Post-fix, the peek-only checks (11e/11f/11g) run first; the mutating gates
   * (11c/11d) run only after the peek-only block passes.
   *
   * <p>Scenario: {@code maxOrdersPerSecond=1}.
   *
   * <ol>
   *   <li>First order targets symbol {@code FORBID} (no eligibility record → 11g fail-closed). Must
   *       reject with {@code RegulatoryRestriction} AND leave the rate counter at zero.
   *   <li>Second order targets the permissive {@code EURUSD}. Must be admitted — would be rejected
   *       with {@code RateLimitExceeded} if the first order had consumed a token.
   *   <li>Third order targets {@code EURUSD} again. Must reject with {@code RateLimitExceeded}
   *       since the second order consumed the only token.
   * </ol>
   */
  @Test
  void check11g_rejectDoesNotConsumeRateToken_thanksToReorder() {
    seedAccountA();
    seedRiskLimitA(1L);

    // Order 1 — symbol FORBID has no eligibility record → §G fail-closed reject.
    final int forbidLen =
        SbeTestEncoder.encodeNewOrderSingle(
            msgBuf,
            0,
            "REORD-001",
            "FORBID",
            SideEnum.Buy,
            OrdTypeEnum.Limit,
            VALID_PRICE,
            VALID_QTY,
            ACCOUNT_CODE_A,
            "USD");
    handler.onCommand(
        session,
        T0,
        msgBuf,
        0,
        forbidLen,
        NewOrderSingleDecoder.BLOCK_LENGTH,
        NewOrderSingleDecoder.SCHEMA_VERSION,
        eventSink);

    assertEquals(1, session.messages.size());
    final var rej1 = decodeOrderRejected(session.messages.get(0));
    assertEquals(
        RejectReasonEnum.RegulatoryRestriction,
        rej1.rejectReason(),
        "FORBID symbol must trigger §G RegulatoryRestriction, not RateLimitExceeded");

    // Order 2 — permissive EURUSD. Must admit; pre-fix this order would have been the rejected
    // one because order 1 would have already consumed the only rate token.
    dispatchNosA("REORD-002", T0);
    assertEquals(
        2,
        session.messages.size(),
        "EURUSD order must be admitted — the §G reject on order 1 must not have consumed a"
            + " rate token");
    assertTemplateId(OrderCreatedEventDecoder.TEMPLATE_ID, session.messages.get(1));

    // Order 3 — permissive EURUSD again. Now the rate-limit counter IS at 1; must reject.
    dispatchNosA("REORD-003", T0);
    assertEquals(3, session.messages.size());
    final var rej3 = decodeOrderRejected(session.messages.get(2));
    assertEquals(
        RejectReasonEnum.RateLimitExceeded,
        rej3.rejectReason(),
        "third order must hit the configured 1-per-second rate limit");
  }
}
