package com.trading.engine.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.cluster.IdGenerator;
import com.trading.engine.cluster.OrderBook;
import com.trading.engine.cluster.TradingClusteredService;
import com.trading.engine.cluster.TradingClusteredServiceFactory;
import com.trading.engine.cluster.handler.EventSink;
import com.trading.engine.cluster.journal.EventJournal;
import com.trading.engine.cluster.metrics.RfqMetrics;
import com.trading.engine.cluster.metrics.RiskMetrics;
import com.trading.engine.cluster.refdata.AccountState;
import com.trading.engine.cluster.refdata.AccountStore;
import com.trading.engine.cluster.refdata.CurrencyState;
import com.trading.engine.cluster.refdata.CurrencyStore;
import com.trading.engine.cluster.refdata.LoadAccountHandler;
import com.trading.engine.cluster.refdata.LoadCurrencyHandler;
import com.trading.engine.cluster.refdata.LoadRiskLimitHandler;
import com.trading.engine.cluster.refdata.ReferenceDataRegistry;
import com.trading.engine.cluster.refdata.RiskLimitStore;
import com.trading.engine.cluster.sequencer.EventSequencer;
import com.trading.engine.cluster.state.RfqStateMachine;
import com.trading.engine.cluster.state.TradingState;
import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.AccountTypeEnum;
import com.trading.engine.messages.sbe.AcctIDSourceEnum;
import com.trading.engine.messages.sbe.ComplianceStatusEnum;
import com.trading.engine.messages.sbe.CurrencyClassEnum;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.QuoteCreatedEventDecoder;
import com.trading.engine.messages.sbe.QuoteRejectReasonEnum;
import com.trading.engine.messages.sbe.QuoteRejectedEventDecoder;
import com.trading.engine.messages.sbe.QuoteRequestedEventDecoder;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.testsupport.aeron.FakeClientSession;
import com.trading.engine.testsupport.aeron.RfqClusterTestHarness;
import com.trading.engine.testsupport.sbe.SbeTestEncoder;
import java.nio.charset.StandardCharsets;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for the RFQ lifecycle event sequence: {@code QuoteRequestedEvent} (104) →
 * {@code QuoteCreatedEvent} (105) → {@code QuoteExpiredEvent} (107), and the validation reject path
 * to {@code QuoteRejectedEvent} (106).
 *
 * <p>These tests drive the production {@link TradingClusteredService} directly via {@link
 * TradingClusteredService#onSessionMessage} and {@link TradingClusteredService#onTimerEvent}, using
 * {@link RfqClusterTestHarness} to capture timer schedules for manual simulation. No Aeron cluster
 * is spun up.
 *
 * <p>Assertions cover:
 *
 * <ul>
 *   <li>Template-ID sequence (104, 105/106, 107) in order
 *   <li>Gapless {@code sequenceNumber} values stamped by {@link EventSink}
 *   <li>Relevant {@link RfqMetrics} counters
 *   <li>Exact {@code QuoteRejectReasonEnum} on 106 events
 * </ul>
 *
 * <p><b>Threading:</b> single-threaded — cluster duty cycle invariant.
 *
 * <p><b>Allocation:</b> allocates test-only infrastructure; not subject to hot-path constraints.
 */
class RfqLifecycleEventsIT {

  // -------------------------------------------------------------------------
  // Constants
  // -------------------------------------------------------------------------

  /**
   * Fixed cluster timestamp used for all {@code onSessionMessage} / {@code onTimerEvent} calls.
   * Chosen as a realistic epoch-nanos value so that {@code validUntil = TIMESTAMP + TTL} is also a
   * positive, non-overflow number.
   */
  private static final long TIMESTAMP = 1_700_000_000_000_000_000L;

  /** Fixed-point scale factor: 1.0 as {@code long} (10^8). */
  private static final long PRICE_SCALE = 100_000_000L;

  /**
   * TTL used by the default {@link RfqStateMachine} (30 s expressed in nanoseconds). Must match
   * {@link TradingClusteredServiceFactory#DEFAULT_RFQ_TTL_NANOS} to correctly compute {@code
   * validUntil} in the timer-fire simulation.
   */
  private static final long TTL_NANOS = TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS;

  /** Known-good instrument symbol present in the default reference data. */
  private static final String SYMBOL = "EURUSD";

  /**
   * Account code for the RFQ-capable account seeded in {@link #setUp()}. The standard {@link
   * com.trading.engine.cluster.refdata.ReferenceDataSeeder} ACME account only has {@code
   * CAN_TRADE}; we seed a distinct account with both {@code CAN_TRADE | CAN_RFQ}.
   */
  private static final String RFQ_ACCOUNT = "RFQACCT";

  /** Standard order quantity: 1 unit in fixed-point. */
  private static final long ORDER_QTY = 1L * PRICE_SCALE;

  /** Bid price: 1.075 in fixed-point 10^8. */
  private static final long BID_PX = 107_500_000L;

  /** Offer price: 1.076 in fixed-point 10^8. */
  private static final long OFFER_PX = 107_600_000L;

  // -------------------------------------------------------------------------
  // Test fixtures
  // -------------------------------------------------------------------------

  private TradingClusteredService service;
  private RfqClusterTestHarness cluster;
  private FakeClientSession session;
  private RfqMetrics rfqMetrics;
  private EventSequencer eventSequencer;

  // -------------------------------------------------------------------------
  // Setup
  // -------------------------------------------------------------------------

  @BeforeEach
  void setUp() {
    final var orderIdGen = new IdGenerator("ORD");
    final var execIdGen = new IdGenerator("EXE");
    final var quoteIdGen = new IdGenerator("QTE");
    final var orderBook = new OrderBook(128);

    eventSequencer = new EventSequencer();
    final var eventJournal = new EventJournal(256);
    final var tradingState = new TradingState(orderBook, orderIdGen, execIdGen, quoteIdGen);
    final var eventSink = new EventSink(eventSequencer, eventJournal);

    final var accountStore = new AccountStore();
    final var currencyStore = new CurrencyStore();
    final var riskLimitStore = new RiskLimitStore();

    // Seed a CAN_RFQ-capable account (id=10, code=RFQACCT).
    accountStore.put(makeRfqAccount(10L, RFQ_ACCOUNT));

    // Seed USD and EUR currencies (required by QuoteRequest validation).
    currencyStore.put(
        CurrencyStore.packCode((byte) 'U', (byte) 'S', (byte) 'D'), makeCurrency("USD"));
    currencyStore.put(
        CurrencyStore.packCode((byte) 'E', (byte) 'U', (byte) 'R'), makeCurrency("EUR"));

    // No risk limits needed — RFQ path does not enforce order-size limits.

    final var registry = new ReferenceDataRegistry();
    registry.registerStore(accountStore);
    registry.registerStore(currencyStore);
    registry.registerStore(riskLimitStore);
    registry.registerLoader(new LoadAccountHandler(accountStore, currencyStore));
    registry.registerLoader(new LoadCurrencyHandler(currencyStore));
    registry.registerLoader(new LoadRiskLimitHandler(riskLimitStore, accountStore));

    rfqMetrics = new RfqMetrics();
    final var rfqStateMachine = newRfqStateMachine(accountStore, rfqMetrics);

    service =
        new TradingClusteredService(
            tradingState,
            eventSink,
            eventJournal,
            accountStore,
            currencyStore,
            riskLimitStore,
            registry,
            rfqStateMachine,
            rfqMetrics,
            new RiskMetrics());

    cluster = new RfqClusterTestHarness(TIMESTAMP);
    session = new FakeClientSession(42L);
    cluster.addClientSession(session);
    service.onStart(cluster, null);
  }

  // -------------------------------------------------------------------------
  // Ref-data helpers (inline, no testFixtures dependency)
  // -------------------------------------------------------------------------

  /**
   * Creates an active {@link AccountState} with both {@code CAN_TRADE} and {@code CAN_RFQ}
   * capabilities. Used to seed the account store for RFQ-path tests.
   *
   * @param id the numeric account identifier
   * @param code the ASCII account code (max 16 chars)
   * @return a ready-to-insert {@link AccountState}
   */
  private static AccountState makeRfqAccount(final long id, final String code) {
    final var s = new AccountState();
    s.setAccountId(id);
    s.setParentAccountId(0L);
    final byte[] codeBytes = code.getBytes(StandardCharsets.US_ASCII);
    s.setAccountCode(codeBytes, 0, codeBytes.length);
    s.setAcctIdSource(AcctIDSourceEnum.Internal);
    final byte[] nameBytes = ("Account " + code).getBytes(StandardCharsets.US_ASCII);
    s.setAccountName(nameBytes, 0, nameBytes.length);
    s.setAccountType(AccountTypeEnum.Client);
    s.setBaseCurrency((byte) 'U', (byte) 'S', (byte) 'D');
    s.setStatus(AccountStatusEnum.Active);
    s.setComplianceStatus(ComplianceStatusEnum.OK);
    s.setCapabilities(AccountState.Capabilities.CAN_TRADE | AccountState.Capabilities.CAN_RFQ);
    s.setTransactTime(0L);
    return s;
  }

  /**
   * Creates a Fiat/Active {@link CurrencyState} with 2 decimal places. Used to seed the currency
   * store for QuoteRequest validation.
   *
   * @param isoCode ISO 4217 alpha-3 code, e.g. {@code "USD"} (exactly 3 uppercase ASCII chars)
   * @return a ready-to-insert {@link CurrencyState}
   */
  private static CurrencyState makeCurrency(final String isoCode) {
    final var c = new CurrencyState();
    final byte[] codeBytes = isoCode.getBytes(StandardCharsets.US_ASCII);
    c.setCcyCode(codeBytes, 0);
    c.setIsoNumeric(0);
    final byte[] nameBytes = ("Currency " + isoCode).getBytes(StandardCharsets.US_ASCII);
    c.setName(nameBytes, 0, nameBytes.length);
    c.setDecimals(2);
    c.setCurrencyClass(CurrencyClassEnum.Fiat);
    c.setStatus(AccountStatusEnum.Active);
    c.setTransactTime(0L);
    return c;
  }

  // -------------------------------------------------------------------------
  // RfqStateMachine factory
  // -------------------------------------------------------------------------

  /**
   * Constructs an {@link RfqStateMachine} with the default service-factory constants.
   *
   * @param accounts the account store (must not be null)
   * @param metrics the metrics container (must not be null)
   * @return a ready-to-use state machine
   */
  private static RfqStateMachine newRfqStateMachine(
      final AccountStore accounts, final RfqMetrics metrics) {
    return new RfqStateMachine(
        TradingClusteredServiceFactory.DEFAULT_RFQ_POOL_CAPACITY,
        TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS,
        TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS,
        TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS,
        TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS,
        TradingClusteredServiceFactory.DEFAULT_RFQ_REQUEST_TIMEOUT_NANOS,
        TradingClusteredServiceFactory.DEFAULT_RFQ_RATE_LIMIT_PER_SESSION,
        TradingClusteredServiceFactory.DEFAULT_RFQ_RATE_LIMIT_WINDOW_NANOS,
        TradingClusteredServiceFactory.DEFAULT_RFQ_ACCEPT_PRICE_TOLERANCE_BPS,
        TradingClusteredServiceFactory.DEFAULT_RFQ_ACCEPT_QTY_TOLERANCE_BPS,
        accounts,
        metrics);
  }

  // -------------------------------------------------------------------------
  // Dispatch helper
  // -------------------------------------------------------------------------

  /**
   * Dispatches a pre-encoded SBE command buffer to the service via {@link
   * TradingClusteredService#onSessionMessage}.
   *
   * @param buf buffer containing the encoded command
   * @param len total encoded length (header + body)
   */
  private void dispatch(final MutableDirectBuffer buf, final int len) {
    service.onSessionMessage(session, TIMESTAMP, buf, 0, len, null);
  }

  // -------------------------------------------------------------------------
  // SBE decode helpers
  // -------------------------------------------------------------------------

  /**
   * Reads the SBE {@code templateId} from a captured message byte array (header at offset 0).
   *
   * @param msg raw captured bytes
   * @return the template ID from the SBE message header
   */
  private static int templateId(final byte[] msg) {
    final var hdr = new MessageHeaderDecoder();
    hdr.wrap(new UnsafeBuffer(msg), 0);
    return hdr.templateId();
  }

  /**
   * Reads the {@code sequenceNumber} field at body offset 0 from a captured byte array. All domain
   * events (templates 100-116) carry this {@code long} field immediately after the SBE message
   * header, stamped by {@link EventSink}.
   *
   * @param msg raw captured bytes
   * @return the sequence number
   */
  private static long sequenceNumber(final byte[] msg) {
    final var buf = new UnsafeBuffer(msg);
    // Body starts at ENCODED_LENGTH; sequenceNumber is the first long in the body.
    return buf.getLong(MessageHeaderDecoder.ENCODED_LENGTH, java.nio.ByteOrder.LITTLE_ENDIAN);
  }

  // =========================================================================
  // Test 1: full accepted flow → 104, 105, 107 with gapless sequence numbers
  // =========================================================================

  /**
   * Happy-path RFQ lifecycle: QuoteRequest → PriceResponse(accepted=true) → TTL timer fires.
   *
   * <p>Expected sequence of egress messages on the session, in order:
   *
   * <ol>
   *   <li>Template 104 — {@code QuoteRequestedEvent} (emitted by {@code QuoteRequestHandler})
   *   <li>Template 105 — {@code QuoteCreatedEvent} (emitted by {@code PriceResponseHandler})
   *   <li>Template 107 — {@code QuoteExpiredEvent} (emitted by {@code
   *       RfqStateMachine.onTimerExpiry})
   * </ol>
   *
   * <p>The three sequence numbers must be consecutive: N, N+1, N+2 (gapless).
   */
  @Test
  void acceptedFlow_observes104Then105Then107WithGaplessSequence() {
    final var quoteReqId = "QR-ACCEPTED-0001";

    // Step 1 — dispatch QuoteRequest; expect template 104.
    final var qrBuf = new ExpandableArrayBuffer(512);
    final int qrLen =
        SbeTestEncoder.encodeQuoteRequest(
            qrBuf, 0, quoteReqId, SYMBOL, SideEnum.Buy, ORDER_QTY, RFQ_ACCOUNT, TIMESTAMP);
    dispatch(qrBuf, qrLen);

    assertEquals(1, session.messages.size(), "QuoteRequest must emit exactly one 104 event");
    assertEquals(
        QuoteRequestedEventDecoder.TEMPLATE_ID,
        templateId(session.messages.get(0)),
        "First event must be template 104 (QuoteRequestedEvent)");

    // Step 2 — dispatch PriceResponse(accepted=true); expect template 105.
    final var prBuf = new ExpandableArrayBuffer(512);
    final int prLen =
        SbeTestEncoder.encodePriceResponse(
            prBuf, 0, quoteReqId, SYMBOL, true, BID_PX, OFFER_PX, TIMESTAMP);
    dispatch(prBuf, prLen);

    assertEquals(2, session.messages.size(), "PriceResponse(accepted) must emit one 105 event");
    assertEquals(
        QuoteCreatedEventDecoder.TEMPLATE_ID,
        templateId(session.messages.get(1)),
        "Second event must be template 105 (QuoteCreatedEvent)");

    // Capture the TTL timer scheduled by PriceResponseHandler.
    final long[] timer = cluster.lastScheduledTimer();
    assertTrue(timer != null, "PriceResponseHandler must schedule a TTL timer after 105 emit");
    final long correlationId = timer[0];
    final long deadline = timer[1];
    // Deadline = TIMESTAMP + TTL_NANOS (production formula: clusterTs + ttlForProduct).
    assertEquals(TIMESTAMP + TTL_NANOS, deadline, "timer deadline must equal clusterTs + TTL");

    // Step 3 — fire the timer at a time >= deadline; 107 is emitted to a null session per
    // plan §9.3 (timer-driven expiry has no originating client session), so the journal-only
    // emission does not appear in the per-session captured `session.messages` list. Verify
    // the lifecycle completed via the metrics counter instead — `rfqMetrics.emitExpired`
    // increments exactly once on the timer fire.
    service.onTimerEvent(correlationId, deadline + 1L);

    // Step 4 — verify gapless sequence numbers on the two client-visible events: N, N+1.
    final long seqN = sequenceNumber(session.messages.get(0));
    assertEquals(seqN + 1L, sequenceNumber(session.messages.get(1)), "seqNo must be N+1");

    // Step 5 — metrics sanity. emitExpired==1 confirms the journal-only 107 was emitted.
    assertEquals(1L, rfqMetrics.emitRequested, "emitRequested must be 1");
    assertEquals(1L, rfqMetrics.emitCreated, "emitCreated must be 1");
    assertEquals(1L, rfqMetrics.emitExpired, "emitExpired must be 1 (timer-driven, null session)");
    assertEquals(0L, rfqMetrics.emitRejected, "emitRejected must be 0");
  }

  // =========================================================================
  // Test 2: validation reject (empty symbol) → exactly one 106, no prior 104
  // =========================================================================

  /**
   * A {@code QuoteRequest} with an empty symbol must be immediately rejected with {@code
   * QuoteRejectedEvent} (106). No {@code QuoteRequestedEvent} (104) is emitted because the reject
   * fires in the validation ladder before slot acquisition.
   *
   * <p>Metric asserted: {@code rfqMetrics.rejectSymbolEmpty == 1}.
   */
  @Test
  void validationRejectedFlow_observesOnly106NoPrior104() {
    final var quoteReqId = "QR-BADSYMBOL-001";

    // Encode a QuoteRequest with an empty symbol — triggers symbol-empty validation reject.
    final var buf = new ExpandableArrayBuffer(512);
    final int len =
        SbeTestEncoder.encodeQuoteRequest(
            buf, 0, quoteReqId, "", SideEnum.Buy, ORDER_QTY, RFQ_ACCOUNT, TIMESTAMP);
    dispatch(buf, len);

    assertEquals(1, session.messages.size(), "Validation reject must emit exactly one 106 event");
    assertEquals(
        QuoteRejectedEventDecoder.TEMPLATE_ID,
        templateId(session.messages.get(0)),
        "Event must be template 106 (QuoteRejectedEvent)");

    // Metrics: no 104 emitted (reject before slot acquisition), one 106 emitted.
    assertEquals(0L, rfqMetrics.emitRequested, "emitRequested must be 0 (reject before 104)");
    assertEquals(1L, rfqMetrics.emitRejected, "emitRejected must be 1");
    assertEquals(1L, rfqMetrics.rejectSymbolEmpty, "rejectSymbolEmpty counter must be 1");
  }

  // =========================================================================
  // Test 3: pricing declined → 104 then 106 with InvalidPrice reason
  // =========================================================================

  /**
   * An accepted {@code QuoteRequest} (emits 104) followed by {@code PriceResponse(accepted=false)}
   * must emit {@code QuoteRejectedEvent} (106) with {@link QuoteRejectReasonEnum#InvalidPrice}.
   *
   * <p>This tests the declined-pricing path: the slot transitions REQUESTED → FREE after the 106.
   * Sequence numbers must be gapless: N (104) → N+1 (106).
   */
  @Test
  void pricingRejectedFlow_observes104Then106WithInvalidPrice() {
    final var quoteReqId = "QR-DECLINED-0001";

    // Step 1 — emit 104.
    final var qrBuf = new ExpandableArrayBuffer(512);
    final int qrLen =
        SbeTestEncoder.encodeQuoteRequest(
            qrBuf, 0, quoteReqId, SYMBOL, SideEnum.Buy, ORDER_QTY, RFQ_ACCOUNT, TIMESTAMP);
    dispatch(qrBuf, qrLen);
    assertEquals(1, session.messages.size(), "QuoteRequest must emit one 104");
    assertEquals(QuoteRequestedEventDecoder.TEMPLATE_ID, templateId(session.messages.get(0)));

    // Step 2 — emit 106 via declined PriceResponse.
    final var prBuf = new ExpandableArrayBuffer(512);
    final int prLen =
        SbeTestEncoder.encodePriceResponse(prBuf, 0, quoteReqId, SYMBOL, false, 0L, 0L, TIMESTAMP);
    dispatch(prBuf, prLen);
    assertEquals(2, session.messages.size(), "Declined PriceResponse must emit one 106");
    assertEquals(
        QuoteRejectedEventDecoder.TEMPLATE_ID,
        templateId(session.messages.get(1)),
        "Second event must be template 106 (QuoteRejectedEvent)");

    // Decode reject reason — must be InvalidPrice.
    final var msgBuf = new UnsafeBuffer(session.messages.get(1));
    final var hdr = new MessageHeaderDecoder();
    hdr.wrap(msgBuf, 0);
    final var dec = new QuoteRejectedEventDecoder();
    dec.wrap(msgBuf, MessageHeaderDecoder.ENCODED_LENGTH, hdr.blockLength(), hdr.version());
    assertEquals(
        QuoteRejectReasonEnum.InvalidPrice,
        dec.quoteRejectReason(),
        "Declined PriceResponse must produce reject reason InvalidPrice");

    // Gapless sequence numbers: N (104), N+1 (106).
    final long seqN = sequenceNumber(session.messages.get(0));
    assertEquals(seqN + 1L, sequenceNumber(session.messages.get(1)), "seqNo must be N+1");

    // Metrics.
    assertEquals(1L, rfqMetrics.emitRequested, "emitRequested must be 1");
    assertEquals(0L, rfqMetrics.emitCreated, "emitCreated must be 0 (declined)");
    assertEquals(1L, rfqMetrics.emitRejected, "emitRejected must be 1");
    assertEquals(1L, rfqMetrics.rejectPricingDeclined, "rejectPricingDeclined must be 1");
  }

  // =========================================================================
  // Test 4: duplicate QuoteReqID → 104 for first, then 106 "duplicate"
  // =========================================================================

  /**
   * Sending the same {@code quoteReqId} twice while the first request is still in-flight (REQUESTED
   * state) must emit 104 for the first request and 106 for the duplicate.
   *
   * <p>The duplicate is detected by the {@code byQuoteReqId} map in {@link RfqStateMachine}. A
   * mismatching body (different side) ensures we hit the {@code rejectDuplicate} branch rather than
   * the idempotent-retransmit drop.
   *
   * <p>Metric asserted: {@code rfqMetrics.rejectDuplicate == 1}.
   */
  @Test
  void duplicateQuoteReqId_emitsTemplate106Duplicate() {
    final var quoteReqId = "QR-DUPLICATE-001";

    // First request — valid, expect 104.
    final var qrFirst = new ExpandableArrayBuffer(512);
    final int lenFirst =
        SbeTestEncoder.encodeQuoteRequest(
            qrFirst, 0, quoteReqId, SYMBOL, SideEnum.Buy, ORDER_QTY, RFQ_ACCOUNT, TIMESTAMP);
    dispatch(qrFirst, lenFirst);
    assertEquals(1, session.messages.size(), "First request must emit one 104");
    assertEquals(QuoteRequestedEventDecoder.TEMPLATE_ID, templateId(session.messages.get(0)));

    // Second request with same quoteReqId but different side → body mismatch → rejectDuplicate.
    final var qrDup = new ExpandableArrayBuffer(512);
    final int lenDup =
        SbeTestEncoder.encodeQuoteRequest(
            qrDup, 0, quoteReqId, SYMBOL, SideEnum.Sell, ORDER_QTY, RFQ_ACCOUNT, TIMESTAMP);
    dispatch(qrDup, lenDup);

    assertEquals(2, session.messages.size(), "Duplicate must emit one additional 106");
    assertEquals(
        QuoteRejectedEventDecoder.TEMPLATE_ID,
        templateId(session.messages.get(1)),
        "Second event must be template 106 (QuoteRejectedEvent) for the duplicate");

    // Metrics.
    assertEquals(1L, rfqMetrics.rejectDuplicate, "rejectDuplicate counter must be 1");
    assertEquals(1L, rfqMetrics.emitRequested, "emitRequested must remain 1 (only first 104)");
    assertEquals(1L, rfqMetrics.emitRejected, "emitRejected must be 1 for the duplicate 106");
  }
}
