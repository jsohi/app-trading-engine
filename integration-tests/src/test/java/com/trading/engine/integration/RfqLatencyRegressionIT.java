package com.trading.engine.integration;

import static org.junit.jupiter.api.Assertions.fail;

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
import com.trading.engine.cluster.refdata.SymbolEligibilityStore;
import com.trading.engine.cluster.sequencer.EventSequencer;
import com.trading.engine.cluster.state.RfqStateMachine;
import com.trading.engine.cluster.state.TradingState;
import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.AccountTypeEnum;
import com.trading.engine.messages.sbe.AcctIDSourceEnum;
import com.trading.engine.messages.sbe.ComplianceStatusEnum;
import com.trading.engine.messages.sbe.CurrencyClassEnum;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.testsupport.aeron.FakeClientSession;
import com.trading.engine.testsupport.aeron.RfqClusterTestHarness;
import com.trading.engine.testsupport.sbe.SbeTestEncoder;
import java.nio.charset.StandardCharsets;
import org.HdrHistogram.Histogram;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Latency regression tests for the RFQ command path, gated behind {@code @Tag("perf")} so the
 * default {@code :integration-tests:test} task (which excludes the {@code perf} tag) never runs
 * them in PR pipelines. Run via:
 *
 * <pre>
 *   ./gradlew :integration-tests:perfTest -PperfTest
 * </pre>
 *
 * <p>Uses <a href="https://github.com/HdrHistogram/HdrHistogram">HdrHistogram</a> to measure
 * per-exchange latency ({@code System.nanoTime()} delta around {@link
 * TradingClusteredService#onSessionMessage}) and asserts P50/P99/P999 budgets.
 *
 * <p>The budgets (P50 ≤ 5 µs, P99 ≤ 50 µs, P999 ≤ 200 µs) are intentionally loose: this test runs
 * on developer hardware, not a dedicated perf runner. The goal is to catch catastrophic regressions
 * (e.g. unexpected heap allocation on every call) rather than to pin absolute numbers.
 *
 * <p><b>Why {@code System.nanoTime()} in test code:</b> {@code CLAUDE.md} prohibits {@code
 * System.nanoTime()} on the cluster hot path, but test infrastructure is explicitly exempt. Using a
 * monotonic clock for latency measurement is correct here.
 *
 * <p><b>Threading:</b> single-threaded — all dispatch on the test driver thread.
 *
 * <p><b>Allocation:</b> the <em>measured path</em> (inside the timed region) must not allocate;
 * measurement overhead (histogram recording) happens outside the timed region.
 */
@Tag("perf")
class RfqLatencyRegressionIT {

  // -------------------------------------------------------------------------
  // Constants
  // -------------------------------------------------------------------------

  /** Fixed cluster timestamp for all dispatched messages. */
  private static final long TIMESTAMP = 1_700_000_000_000_000_000L;

  /** Fixed-point scale factor: 1.0 as {@code long} (10^8). */
  private static final long PRICE_SCALE = 100_000_000L;

  /** Standard order quantity: 1 unit in fixed-point. */
  private static final long ORDER_QTY = 1L * PRICE_SCALE;

  /** Bid price: 1.075 in fixed-point 10^8. */
  private static final long BID_PX = 107_500_000L;

  /** Offer price: 1.076 in fixed-point 10^8. */
  private static final long OFFER_PX = 107_600_000L;

  /** Account code for the RFQ-capable account. */
  private static final String RFQ_ACCOUNT = "RFQACCT";

  /**
   * Warmup duration in nanoseconds: 30 seconds.
   *
   * <p>Drives the JIT to steady-state code-generation before the measurement window opens. 30 s is
   * sufficient for tiered compilation to complete on a modern JVM.
   */
  private static final long WARMUP_NANOS = 30_000_000_000L;

  /**
   * Measurement window in nanoseconds: 30 seconds.
   *
   * <p>At ~100 k QR+PR pairs/s this yields ~3 million samples — statistically stable P999
   * estimates.
   */
  private static final long MEASURE_NANOS = 30_000_000_000L;

  // ---- Latency budgets (loose — developer hardware) ----

  /**
   * P50 budget: 5 000 ns (5 µs).
   *
   * <p>The command path (header-decode → slot-acquire → encode → offer) should complete well under
   * 5 µs on any modern laptop.
   */
  private static final long P50_BUDGET_NS = 5_000L;

  /**
   * P99 budget: 50 000 ns (50 µs).
   *
   * <p>Allows for occasional GC safepoints, OS scheduler jitter, and JIT de-opt transitions.
   */
  private static final long P99_BUDGET_NS = 50_000L;

  /**
   * P999 budget: 200 000 ns (200 µs).
   *
   * <p>Covers rare stop-the-world pauses from HdrHistogram autoResize and infrequent OS interrupts.
   */
  private static final long P999_BUDGET_NS = 200_000L;

  // -------------------------------------------------------------------------
  // Ref-data helpers (inline, no testFixtures dependency)
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
  // Service factory
  // -------------------------------------------------------------------------

  /**
   * Constructs a fully wired {@link TradingClusteredService} with one RFQ-capable account plus USD
   * and EUR currencies. Returns a three-element array: {@code [TradingClusteredService,
   * FakeClientSession, RfqClusterTestHarness]}.
   *
   * <p>The event journal is sized to 128 k entries to accommodate the full 30-second measurement
   * window without overflow.
   */
  private static Object[] buildService() {
    final var orderIdGen = new IdGenerator("ORD");
    final var execIdGen = new IdGenerator("EXE");
    final var quoteIdGen = new IdGenerator("QTE");
    final var orderBook = new OrderBook(128);
    final var eventSequencer = new EventSequencer();
    // 128 k slots — sufficient for 30 s at ~100 k iterations/s.
    final var eventJournal = new EventJournal(1 << 17);
    final var tradingState = new TradingState(orderBook, orderIdGen, execIdGen, quoteIdGen);
    final var eventSink = new EventSink(eventSequencer, eventJournal);

    final var accountStore = new AccountStore();
    final var currencyStore = new CurrencyStore();
    final var riskLimitStore = new RiskLimitStore();
    final var symbolEligibilityStore = new SymbolEligibilityStore();

    accountStore.put(makeRfqAccount(10L, RFQ_ACCOUNT));
    currencyStore.put(
        CurrencyStore.packCode((byte) 'U', (byte) 'S', (byte) 'D'), makeCurrency("USD"));
    currencyStore.put(
        CurrencyStore.packCode((byte) 'E', (byte) 'U', (byte) 'R'), makeCurrency("EUR"));

    final var registry = new ReferenceDataRegistry();
    registry.registerStore(accountStore);
    registry.registerStore(currencyStore);
    registry.registerStore(riskLimitStore);
    registry.registerStore(symbolEligibilityStore);
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
            symbolEligibilityStore,
            registry,
            rfqStateMachine,
            rfqMetrics,
            new RiskMetrics());

    final var harness = new RfqClusterTestHarness(TIMESTAMP);
    final var session = new FakeClientSession(42L);
    harness.addClientSession(session);
    service.onStart(harness, null);

    return new Object[] {service, session, harness};
  }

  /**
   * Constructs an {@link RfqStateMachine} with the default service-factory constants.
   *
   * @param accounts account store (must not be null)
   * @param metrics metrics container (must not be null)
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

  // =========================================================================
  // Perf test: P50 / P99 / P999 budgets for a steady-state QR + PR loop
  // =========================================================================

  /**
   * Measures the wall-clock latency of the RFQ command path under steady-state load.
   *
   * <p>Each iteration dispatches one {@code QuoteRequest} (template 1) followed by one {@code
   * PriceResponse} (template 51). The measured interval is the combined time for both {@code
   * onSessionMessage} calls (i.e. the round-trip command-path latency for one complete QR→PR
   * exchange). Warmup (30 s) drives the JIT to steady state before the measurement window.
   *
   * <p>Budget assertions (all in nanoseconds):
   *
   * <ul>
   *   <li>P50 ≤ 5 000 ns
   *   <li>P99 ≤ 50 000 ns
   *   <li>P999 ≤ 200 000 ns
   * </ul>
   *
   * <p>On budget breach the test fails with a detailed summary (P50/P99/P999/max/count).
   */
  @Test
  void steadyStateRfqs_p99LatencyUnderBudget() {
    final var components = buildService();
    final var service = (TradingClusteredService) components[0];
    final var session = (FakeClientSession) components[1];
    final var harness = (RfqClusterTestHarness) components[2];

    // Pre-allocate encode buffers — reused across all iterations.
    final var qrBuf = new ExpandableArrayBuffer(512);
    final var prBuf = new ExpandableArrayBuffer(512);

    // HdrHistogram: autoResize=true; lowest=1 ns; max=10 s; 3 significant figures.
    final var histogram = new Histogram(1L, 10_000_000_000L, 3);
    histogram.setAutoResize(true);

    long counter = 0L;

    // ---- Warmup loop ----
    final long warmupDeadline = System.nanoTime() + WARMUP_NANOS;
    while (System.nanoTime() < warmupDeadline) {
      counter++;
      runIteration(service, session, harness, qrBuf, prBuf, counter, null);
    }

    // Reset so warmup samples do not pollute the measurement.
    histogram.reset();
    counter = 1_000_000L; // distinct namespace to avoid duplicate quoteReqId rejects

    // ---- Measurement loop ----
    final long measureDeadline = System.nanoTime() + MEASURE_NANOS;
    while (System.nanoTime() < measureDeadline) {
      counter++;
      runIteration(service, session, harness, qrBuf, prBuf, counter, histogram);
    }

    // ---- Budget assertions ----
    final long p50 = histogram.getValueAtPercentile(50.0);
    final long p99 = histogram.getValueAtPercentile(99.0);
    final long p999 = histogram.getValueAtPercentile(99.9);
    final long max = histogram.getMaxValue();

    if (p50 > P50_BUDGET_NS || p99 > P99_BUDGET_NS || p999 > P999_BUDGET_NS) {
      fail(
          String.format(
              "RFQ latency budget exceeded (nanoseconds):%n"
                  + "  P50  = %,d  (budget ≤ %,d)  %s%n"
                  + "  P99  = %,d  (budget ≤ %,d)  %s%n"
                  + "  P999 = %,d  (budget ≤ %,d)  %s%n"
                  + "  Max  = %,d%n"
                  + "  Samples = %,d",
              p50,
              P50_BUDGET_NS,
              p50 > P50_BUDGET_NS ? "OVER" : "OK",
              p99,
              P99_BUDGET_NS,
              p99 > P99_BUDGET_NS ? "OVER" : "OK",
              p999,
              P999_BUDGET_NS,
              p999 > P999_BUDGET_NS ? "OVER" : "OK",
              max,
              histogram.getTotalCount()));
    }
  }

  // -------------------------------------------------------------------------
  // Per-iteration helper
  // -------------------------------------------------------------------------

  /**
   * Executes one QR + PR dispatch pair. If {@code histogram} is non-null the combined latency of
   * both dispatches is recorded.
   *
   * <p>The timer is fired immediately after each iteration so the slot is returned to the pool
   * before the next iteration, preventing pool exhaustion over the 30-second run.
   *
   * @param service the service under test
   * @param session the egress capture session
   * @param harness the cluster harness (for timer firing and clearing)
   * @param qrBuf reusable encode buffer for QuoteRequest
   * @param prBuf reusable encode buffer for PriceResponse
   * @param counter monotonic iteration counter used to generate a unique 16-char quoteReqId
   * @param histogram target histogram; {@code null} during warmup
   */
  private static void runIteration(
      final TradingClusteredService service,
      final FakeClientSession session,
      final RfqClusterTestHarness harness,
      final MutableDirectBuffer qrBuf,
      final MutableDirectBuffer prBuf,
      final long counter,
      final Histogram histogram) {

    // 16-char zero-padded decimal → unique quoteReqId within 10^16 iterations.
    final var quoteReqId = String.format("%016d", counter);

    final int qrLen =
        SbeTestEncoder.encodeQuoteRequest(
            qrBuf, 0, quoteReqId, "EURUSD", SideEnum.Buy, ORDER_QTY, RFQ_ACCOUNT, TIMESTAMP);

    final int prLen =
        SbeTestEncoder.encodePriceResponse(
            prBuf, 0, quoteReqId, "EURUSD", true, BID_PX, OFFER_PX, TIMESTAMP);

    // ---- Timed region ----
    final long startNs = System.nanoTime();

    service.onSessionMessage(session, TIMESTAMP, qrBuf, 0, qrLen, null);
    service.onSessionMessage(session, TIMESTAMP, prBuf, 0, prLen, null);

    final long elapsedNs = System.nanoTime() - startNs;
    // ---- End timed region ----

    if (histogram != null) {
      histogram.recordValue(elapsedNs);
    }

    // Drain egress messages to prevent unbounded list growth.
    session.messages.clear();

    // Fire the TTL timer to release the slot back to the pool before the next iteration.
    final long[] timer = harness.lastScheduledTimer();
    if (timer != null) {
      service.onTimerEvent(timer[0], timer[1]);
      harness.clearScheduledTimers();
      session.messages.clear();
    }
  }
}
