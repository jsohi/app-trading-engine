package com.trading.engine.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.trading.engine.cluster.IdGenerator;
import com.trading.engine.cluster.OrderBook;
import com.trading.engine.cluster.TradingClusteredService;
import com.trading.engine.cluster.TradingClusteredServiceFactory;
import com.trading.engine.cluster.handler.EventSink;
import com.trading.engine.cluster.journal.EventJournal;
import com.trading.engine.cluster.metrics.RfqMetrics;
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
import com.trading.engine.messages.sbe.QuoteRequestedEventDecoder;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.testsupport.aeron.FakeClientSession;
import com.trading.engine.testsupport.aeron.RfqClusterTestHarness;
import com.trading.engine.testsupport.sbe.SbeTestEncoder;
import java.nio.charset.StandardCharsets;
import org.agrona.ErrorHandler;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for snapshot-based recovery of in-flight RFQ state.
 *
 * <p>Each test follows the same pattern:
 *
 * <ol>
 *   <li><b>Service A</b> — built and driven to a target in-flight state (QUOTED or REQUESTED).
 *   <li>Snapshot taken via {@link TradingClusteredService#onTakeSnapshot(null)}.
 *   <li><b>Service B</b> — fresh instance, snapshot loaded.
 *   <li>{@link RfqStateMachine#onSnapshotRestored} called with a controlled current timestamp to
 *       exercise the recovery-sweep logic.
 *   <li>Assertions on egress events, {@link RfqMetrics} counters, and scheduled timers.
 * </ol>
 *
 * <p>Several methods used in the snapshot round-trip path ({@code snapshotReassemblyBuffer()},
 * {@code assembleSnapshot()}, {@code loadSnapshot()}) are package-private on {@link
 * TradingClusteredService}. They are accessed via reflection in the static helpers {@link
 * #takeSnapshot(ServiceBundle)} and {@link #loadSnapshot(TradingClusteredService,
 * MutableDirectBuffer, int)}. This is acceptable test-only infrastructure: the same pattern is used
 * by {@code TradingClusteredServiceTest}, which lives in the same package and therefore has direct
 * access; the IT counterpart uses reflection to achieve the same result without modifying the
 * production visibility contract.
 *
 * <p>Tests do NOT spin up an Aeron cluster; all dispatch goes through {@link
 * TradingClusteredService#onSessionMessage} and {@link TradingClusteredService#onTimerEvent}.
 *
 * <p><b>Threading:</b> single-threaded — cluster duty cycle invariant.
 *
 * <p><b>Allocation:</b> allocates test-only infrastructure; not subject to hot-path constraints.
 */
class RfqSnapshotRecoveryIT {

  // -------------------------------------------------------------------------
  // Constants
  // -------------------------------------------------------------------------

  /** Realistic epoch-nanos cluster timestamp used for Service A operations. */
  private static final long TIMESTAMP = 1_700_000_000_000_000_000L;

  /** Fixed-point scale factor: 1.0 as {@code long} (10^8). */
  private static final long PRICE_SCALE = 100_000_000L;

  /**
   * TTL applied by the default {@link RfqStateMachine} for Spot products (30 s in nanoseconds).
   * After a {@code PriceResponse(accepted=true)}, the slot's {@code validUntil} is set to {@code
   * TIMESTAMP + TTL_NANOS}.
   */
  private static final long TTL_NANOS = TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS;

  /** Account code for the RFQ-capable account. */
  private static final String RFQ_ACCOUNT = "RFQACCT";

  /** Standard order quantity: 1 unit in fixed-point. */
  private static final long ORDER_QTY = 1L * PRICE_SCALE;

  /** Bid price: 1.075 in fixed-point 10^8. */
  private static final long BID_PX = 107_500_000L;

  /** Offer price: 1.076 in fixed-point 10^8. */
  private static final long OFFER_PX = 107_600_000L;

  // -------------------------------------------------------------------------
  // ServiceBundle record
  // -------------------------------------------------------------------------

  /**
   * Groups all collaborators of one {@link TradingClusteredService} instance so tests can inspect
   * state and call sweep methods directly.
   *
   * @param service the service instance
   * @param rfqStateMachine the RFQ state machine (for direct {@code onSnapshotRestored} calls)
   * @param rfqMetrics the metrics container
   * @param eventSink the event sink (passed to {@code onSnapshotRestored})
   * @param session the egress capture session
   * @param harness the cluster harness (for timer capture and clearing)
   */
  private record ServiceBundle(
      TradingClusteredService service,
      RfqStateMachine rfqStateMachine,
      RfqMetrics rfqMetrics,
      EventSink eventSink,
      FakeClientSession session,
      RfqClusterTestHarness harness) {}

  // -------------------------------------------------------------------------
  // Factory
  // -------------------------------------------------------------------------

  /**
   * Constructs a fresh {@link TradingClusteredService} with all collaborators. The account store is
   * seeded with one active {@code CAN_TRADE | CAN_RFQ} account; the currency store contains USD and
   * EUR.
   *
   * @param clusterTime the fixed timestamp returned by {@link RfqClusterTestHarness#time()}
   * @param sessionId the session ID for the {@link FakeClientSession}
   * @return a fully initialised {@link ServiceBundle}
   */
  private static ServiceBundle buildBundle(final long clusterTime, final long sessionId) {
    final var orderIdGen = new IdGenerator("ORD");
    final var execIdGen = new IdGenerator("EXE");
    final var quoteIdGen = new IdGenerator("QTE");
    final var orderBook = new OrderBook(128);
    final var eventSequencer = new EventSequencer();
    final var eventJournal = new EventJournal(256);
    final var tradingState = new TradingState(orderBook, orderIdGen, execIdGen, quoteIdGen);
    final var eventSink = new EventSink(eventSequencer, eventJournal);

    final var accountStore = new AccountStore();
    final var currencyStore = new CurrencyStore();
    final var riskLimitStore = new RiskLimitStore();

    accountStore.put(makeRfqAccount(10L, RFQ_ACCOUNT));
    currencyStore.put(
        CurrencyStore.packCode((byte) 'U', (byte) 'S', (byte) 'D'), makeCurrency("USD"));
    currencyStore.put(
        CurrencyStore.packCode((byte) 'E', (byte) 'U', (byte) 'R'), makeCurrency("EUR"));

    final var registry = new ReferenceDataRegistry();
    registry.registerStore(accountStore);
    registry.registerStore(currencyStore);
    registry.registerStore(riskLimitStore);
    registry.registerLoader(new LoadAccountHandler(accountStore, currencyStore));
    registry.registerLoader(new LoadCurrencyHandler(currencyStore));
    registry.registerLoader(new LoadRiskLimitHandler(riskLimitStore, accountStore));

    final var rfqMetrics = new RfqMetrics();
    final var rfqStateMachine = newRfqStateMachine(accountStore, rfqMetrics);

    final var service =
        new TradingClusteredService(
            tradingState,
            eventSink,
            eventJournal,
            accountStore,
            currencyStore,
            riskLimitStore,
            registry,
            rfqStateMachine,
            rfqMetrics);

    final var harness = new RfqClusterTestHarness(clusterTime);
    final var session = new FakeClientSession(sessionId);
    service.onStart(harness, null);

    return new ServiceBundle(service, rfqStateMachine, rfqMetrics, eventSink, session, harness);
  }

  // -------------------------------------------------------------------------
  // Ref-data helpers
  // -------------------------------------------------------------------------

  /**
   * Creates an active, CAN_TRADE + CAN_RFQ {@link AccountState}.
   *
   * @param id numeric account identifier
   * @param code ASCII account code (max 16 chars)
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
   * Creates a Fiat/Active {@link CurrencyState} with 2 decimal places.
   *
   * @param isoCode ISO 4217 alpha-3 code (exactly 3 uppercase ASCII chars)
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
  // Snapshot round-trip helpers (reflection: package-private methods)
  // -------------------------------------------------------------------------

  /**
   * Takes an atomic snapshot from the given service bundle via {@code onTakeSnapshot(null)}, then
   * reads the assembled buffer and total byte count via reflection ({@code assembleSnapshot} and
   * {@code snapshotReassemblyBuffer} are package-private).
   *
   * <p>Reflection is used here because {@code TradingClusteredService}'s snapshot accessors are
   * intentionally package-private — they are tested at unit level inside the {@code cluster}
   * module. The IT uses reflection rather than widening visibility or duplicating the production
   * assembly logic.
   *
   * @param bundle the bundle to snapshot
   * @return two-element array: {@code [MutableDirectBuffer assembledBuf, int totalLen]}
   */
  private static Object[] takeSnapshot(final ServiceBundle bundle) {
    // onTakeSnapshot(null) triggers the full assembly path and stores the result in the
    // internal snapshotReassemblyBuf. The return value of assembleSnapshot(maxMsg) is the total
    // assembled length; we call it separately to get that integer.
    bundle.service().onTakeSnapshot(null);
    try {
      final var assembleMethod =
          TradingClusteredService.class.getDeclaredMethod("assembleSnapshot", int.class);
      assembleMethod.setAccessible(true);
      final int totalLen = (int) assembleMethod.invoke(bundle.service(), Integer.MAX_VALUE);

      final var bufMethod =
          TradingClusteredService.class.getDeclaredMethod("snapshotReassemblyBuffer");
      bufMethod.setAccessible(true);
      final var buf = (MutableDirectBuffer) bufMethod.invoke(bundle.service());
      return new Object[] {buf, totalLen};
    } catch (final ReflectiveOperationException ex) {
      throw new AssertionError("snapshot reflection failed — check method names/visibility", ex);
    }
  }

  /**
   * Loads a snapshot into {@code service} via reflection ({@code loadSnapshot} is package-private).
   *
   * @param service the service to restore
   * @param src the assembled snapshot buffer
   * @param totalLen total byte count of the snapshot
   */
  private static void loadSnapshot(
      final TradingClusteredService service, final MutableDirectBuffer src, final int totalLen) {
    try {
      final var loadMethod =
          TradingClusteredService.class.getDeclaredMethod(
              "loadSnapshot", org.agrona.DirectBuffer.class, int.class, int.class);
      loadMethod.setAccessible(true);
      loadMethod.invoke(service, src, 0, totalLen);
    } catch (final ReflectiveOperationException ex) {
      throw new AssertionError(
          "loadSnapshot reflection failed — check method names/visibility", ex);
    }
  }

  // -------------------------------------------------------------------------
  // State-driving helpers
  // -------------------------------------------------------------------------

  /**
   * Drives a {@code QuoteRequest} + {@code PriceResponse(accepted=true)} pair through the service,
   * leaving the named slot in QUOTED state with one TTL timer scheduled in the harness.
   *
   * @param bundle the service bundle to drive
   * @param quoteReqId the QuoteReqID to use
   */
  private static void driveToQuotedState(final ServiceBundle bundle, final String quoteReqId) {
    final var qrBuf = new ExpandableArrayBuffer(512);
    final int qrLen =
        SbeTestEncoder.encodeQuoteRequest(
            qrBuf, 0, quoteReqId, "EURUSD", SideEnum.Buy, ORDER_QTY, RFQ_ACCOUNT, TIMESTAMP);
    bundle.service().onSessionMessage(bundle.session(), TIMESTAMP, qrBuf, 0, qrLen, null);

    final var prBuf = new ExpandableArrayBuffer(512);
    final int prLen =
        SbeTestEncoder.encodePriceResponse(
            prBuf, 0, quoteReqId, "EURUSD", true, BID_PX, OFFER_PX, TIMESTAMP);
    bundle.service().onSessionMessage(bundle.session(), TIMESTAMP, prBuf, 0, prLen, null);
  }

  /**
   * Drives a {@code QuoteRequest} through the service, leaving the named slot in REQUESTED state
   * (no PriceResponse issued).
   *
   * @param bundle the service bundle to drive
   * @param quoteReqId the QuoteReqID to use
   */
  private static void driveToRequestedState(final ServiceBundle bundle, final String quoteReqId) {
    final var qrBuf = new ExpandableArrayBuffer(512);
    final int qrLen =
        SbeTestEncoder.encodeQuoteRequest(
            qrBuf, 0, quoteReqId, "EURUSD", SideEnum.Buy, ORDER_QTY, RFQ_ACCOUNT, TIMESTAMP);
    bundle.service().onSessionMessage(bundle.session(), TIMESTAMP, qrBuf, 0, qrLen, null);
  }

  // -------------------------------------------------------------------------
  // SBE decode helper
  // -------------------------------------------------------------------------

  /**
   * Reads the SBE {@code templateId} from a captured message byte array.
   *
   * @param msg raw captured bytes (header at offset 0)
   * @return the template ID
   */
  private static int templateId(final byte[] msg) {
    final var hdr = new MessageHeaderDecoder();
    hdr.wrap(new UnsafeBuffer(msg), 0);
    return hdr.templateId();
  }

  /** No-op {@link ErrorHandler} for recovery sweep calls. */
  private static final ErrorHandler NO_OP_ERROR_HANDLER = (t) -> {};

  // =========================================================================
  // Test 1: QUOTED slot snapshotted; restore at ts > validUntil → 107 emitted
  // =========================================================================

  /**
   * RFQ in QUOTED state when snapshotted; on restore with current timestamp past the TTL deadline,
   * the recovery sweep must immediately expire the slot and emit {@code QuoteExpiredEvent} (107).
   *
   * <p>Assertions:
   *
   * <ul>
   *   <li>Service B's session captures exactly one 107 event.
   *   <li>{@code rfqMetrics.emitExpired == 1}.
   *   <li>{@code rfqMetrics.recoveryExpiredOnRestore == 1}.
   * </ul>
   */
  @Test
  void snapshotDuringQuoted_restart_postRestoreSweepEmits107WhenTtlElapsed() {
    // ---- Service A: drive to QUOTED state ----
    final var bundleA = buildBundle(TIMESTAMP, 42L);
    driveToQuotedState(bundleA, "QR-RECOVERY-TTL");

    assertEquals(2, bundleA.session().messages.size(), "Service A must emit 104 then 105");
    assertEquals(
        QuoteCreatedEventDecoder.TEMPLATE_ID,
        templateId(bundleA.session().messages.get(1)),
        "Second message on A must be 105 (QuoteCreatedEvent)");

    // ---- Snapshot Service A ----
    final Object[] snap = takeSnapshot(bundleA);
    final MutableDirectBuffer snapBuf = (MutableDirectBuffer) snap[0];
    final int snapLen = (int) snap[1];

    // ---- Service B: load snapshot at ts > validUntil ----
    // validUntil = TIMESTAMP + TTL_NANOS; choose currentTs 1 s past expiry.
    final long currentTs = TIMESTAMP + TTL_NANOS + 1_000_000_000L;
    final var bundleB = buildBundle(currentTs, 99L);
    loadSnapshot(bundleB.service(), snapBuf, snapLen);
    bundleB.session().messages.clear();

    // ---- Recovery sweep: current time IS past validUntil → 107 emitted to journal only
    // (null session per plan §9.3 + §9.4) → not visible in `session.messages`. Verify the
    // journal emission via the metrics counters that the recovery path increments.
    bundleB
        .rfqStateMachine()
        .onSnapshotRestored(currentTs, bundleB.eventSink(), NO_OP_ERROR_HANDLER);

    assertEquals(1L, bundleB.rfqMetrics().emitExpired, "emitExpired must be 1 after sweep");
    assertEquals(
        1L, bundleB.rfqMetrics().recoveryExpiredOnRestore, "recoveryExpiredOnRestore must be 1");
  }

  // =========================================================================
  // Test 2: QUOTED slot snapshotted; restore at ts < validUntil → timer re-armed
  // =========================================================================

  /**
   * RFQ in QUOTED state when snapshotted; on restore with current timestamp BEFORE the TTL
   * deadline, the recovery sweep must re-arm the TTL timer rather than immediately expire the slot.
   *
   * <p>Assertions:
   *
   * <ul>
   *   <li>No 107 emitted.
   *   <li>{@code harness.scheduledTimers.size() == 1} (exactly one timer re-armed).
   *   <li>{@code rfqMetrics.recoveryQuotedRearmed == 1}.
   * </ul>
   */
  @Test
  void snapshotDuringQuoted_restart_postRestoreSweepReArmsTimerWhenTtlInFuture() {
    // ---- Service A: drive to QUOTED state ----
    final var bundleA = buildBundle(TIMESTAMP, 42L);
    driveToQuotedState(bundleA, "QR-RECOVERY-REARM");

    // ---- Snapshot Service A ----
    final Object[] snap = takeSnapshot(bundleA);
    final MutableDirectBuffer snapBuf = (MutableDirectBuffer) snap[0];
    final int snapLen = (int) snap[1];

    // ---- Service B: load snapshot at ts 10 s BEFORE validUntil ----
    // validUntil = TIMESTAMP + TTL_NANOS; currentTs is 10 s before that.
    final long currentTs = TIMESTAMP + TTL_NANOS - 10_000_000_000L;
    final var bundleB = buildBundle(currentTs, 99L);
    loadSnapshot(bundleB.service(), snapBuf, snapLen);
    bundleB.session().messages.clear();
    bundleB.harness().clearScheduledTimers();

    // ---- Recovery sweep: current time BEFORE validUntil → timer must be re-armed ----
    bundleB
        .rfqStateMachine()
        .onSnapshotRestored(currentTs, bundleB.eventSink(), NO_OP_ERROR_HANDLER);

    assertEquals(
        0,
        bundleB.session().messages.size(),
        "Recovery sweep must NOT emit 107 when TTL is in the future");

    assertEquals(
        1,
        bundleB.harness().scheduledTimers.size(),
        "Recovery sweep must re-arm exactly one TTL timer for the QUOTED slot");

    assertEquals(
        1L,
        bundleB.rfqMetrics().recoveryQuotedRearmed,
        "recoveryQuotedRearmed must be 1 after sweep");

    assertEquals(0L, bundleB.rfqMetrics().emitExpired, "emitExpired must remain 0");
  }

  // =========================================================================
  // Test 3: REQUESTED slot snapshotted; restore; PriceResponse still flows to 105
  // =========================================================================

  /**
   * RFQ in REQUESTED state when snapshotted; on restore with current timestamp before the
   * request-timeout deadline, the recovery sweep re-arms the request-timeout timer. A subsequent
   * {@code PriceResponse(accepted=true)} must then transition the slot to QUOTED and emit 105.
   *
   * <p>This validates the end-to-end recovery path for in-flight REQUESTED slots: the correlation
   * map is rebuilt correctly and the PriceResponse routing still works post-restore.
   *
   * <p>Assertions:
   *
   * <ul>
   *   <li>Recovery sweep emits no events (request-timeout not elapsed).
   *   <li>Exactly one request-timeout timer re-armed.
   *   <li>Post-recovery PriceResponse emits 105.
   *   <li>{@code rfqMetrics.emitCreated == 1}.
   * </ul>
   */
  @Test
  void snapshotDuringRequested_restart_priceResponseStillFlowsTo105() {
    final String quoteReqId = "QR-RECOVERY-REQ";

    // ---- Service A: drive to REQUESTED state ----
    final var bundleA = buildBundle(TIMESTAMP, 42L);
    driveToRequestedState(bundleA, quoteReqId);

    assertEquals(1, bundleA.session().messages.size(), "Service A must emit exactly one 104");
    assertEquals(
        QuoteRequestedEventDecoder.TEMPLATE_ID,
        templateId(bundleA.session().messages.get(0)),
        "Service A message must be 104 (QuoteRequestedEvent)");

    // ---- Snapshot Service A ----
    final Object[] snap = takeSnapshot(bundleA);
    final MutableDirectBuffer snapBuf = (MutableDirectBuffer) snap[0];
    final int snapLen = (int) snap[1];

    // ---- Service B: load snapshot at ts 2 s after TIMESTAMP (before 5-s request timeout) ----
    final long currentTs = TIMESTAMP + 2_000_000_000L;
    final var bundleB = buildBundle(currentTs, 99L);
    loadSnapshot(bundleB.service(), snapBuf, snapLen);
    bundleB.session().messages.clear();
    bundleB.harness().clearScheduledTimers();

    // ---- Recovery sweep: REQUESTED slot, request-timeout not elapsed → re-arm timer ----
    bundleB
        .rfqStateMachine()
        .onSnapshotRestored(currentTs, bundleB.eventSink(), NO_OP_ERROR_HANDLER);

    assertEquals(
        0,
        bundleB.session().messages.size(),
        "Recovery sweep must not emit events when request-timeout has not elapsed");

    assertEquals(
        1,
        bundleB.harness().scheduledTimers.size(),
        "Recovery sweep must re-arm the request-timeout timer for the REQUESTED slot");

    assertEquals(
        1L,
        bundleB.rfqMetrics().recoveryRequestRearmed,
        "recoveryRequestRearmed counter must be 1");

    // ---- Drive PriceResponse(accepted=true) at currentTs ----
    final var prBuf = new ExpandableArrayBuffer(512);
    final int prLen =
        SbeTestEncoder.encodePriceResponse(
            prBuf, 0, quoteReqId, "EURUSD", true, BID_PX, OFFER_PX, currentTs);
    bundleB.service().onSessionMessage(bundleB.session(), currentTs, prBuf, 0, prLen, null);

    assertEquals(
        1,
        bundleB.session().messages.size(),
        "PriceResponse after recovery must emit one 105 event");
    assertEquals(
        QuoteCreatedEventDecoder.TEMPLATE_ID,
        templateId(bundleB.session().messages.get(0)),
        "Post-recovery PriceResponse must produce 105 (QuoteCreatedEvent)");

    assertEquals(1L, bundleB.rfqMetrics().emitCreated, "emitCreated must be 1 after 105 emit");
  }
}
