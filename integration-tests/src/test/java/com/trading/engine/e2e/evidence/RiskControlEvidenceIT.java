package com.trading.engine.e2e.evidence;

import com.trading.engine.cluster.IdGenerator;
import com.trading.engine.cluster.OrderBook;
import com.trading.engine.cluster.TradingClusteredServiceFactory;
import com.trading.engine.cluster.handler.EventSink;
import com.trading.engine.cluster.handler.NewOrderSingleHandler;
import com.trading.engine.cluster.journal.EventJournal;
import com.trading.engine.cluster.metrics.RfqMetrics;
import com.trading.engine.cluster.refdata.AccountFixtures;
import com.trading.engine.cluster.refdata.AccountState;
import com.trading.engine.cluster.refdata.AccountStore;
import com.trading.engine.cluster.refdata.CurrencyFixtures;
import com.trading.engine.cluster.refdata.CurrencyStore;
import com.trading.engine.cluster.refdata.LoadRiskLimitHandler;
import com.trading.engine.cluster.refdata.ReferenceDataSeeder;
import com.trading.engine.cluster.refdata.RiskLimitFixtures;
import com.trading.engine.cluster.refdata.RiskLimitStore;
import com.trading.engine.cluster.refdata.SymbolEligibilityState;
import com.trading.engine.cluster.refdata.SymbolEligibilityStore;
import com.trading.engine.cluster.sequencer.EventSequencer;
import com.trading.engine.cluster.state.RfqStateMachine;
import com.trading.engine.cluster.state.TradingState;
import com.trading.engine.messages.FixedPointScale;
import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.LoadRiskLimitEncoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.NewOrderSingleDecoder;
import com.trading.engine.messages.sbe.OrdTypeEnum;
import com.trading.engine.messages.sbe.OrderRejectedEventDecoder;
import com.trading.engine.messages.sbe.RejectReasonEnum;
import com.trading.engine.messages.sbe.RiskLimitLoadRejectedEventDecoder;
import com.trading.engine.messages.sbe.RiskLimitLoadedEventDecoder;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.testsupport.aeron.FakeClientSession;
import com.trading.engine.testsupport.aeron.FakeCluster;
import com.trading.engine.testsupport.sbe.SbeTestEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * APP-62 §5.3 — FINRA 3110 / RTS 6 §9 boundary-fuzz evidence pack. Drives the cluster handlers
 * directly (not through Aeron) with boundary inputs for each pre-trade check, records the
 * (expected, observed) pair via {@link RiskEvidenceRecorder}, and lets {@link
 * EvidenceReportListener} render the regulator-facing markdown table.
 *
 * <p><b>Tagging.</b> Every method is tagged {@code @Tag("risk-evidence")} at the class level. The
 * default {@code :integration-tests:test} task excludes this tag (see {@code
 * integration-tests/build.gradle.kts}) — PR CI never runs the pack, and runtime is short enough
 * that the dedicated {@code riskControlEvidence} task completes in well under a minute.
 *
 * <p><b>Checks covered (plan §5.3).</b>
 *
 * <ul>
 *   <li>{@code PositionLimit} — Check 11e (CME PTRM Long-Qty / Short-Qty + saturation)
 *   <li>{@code FatFinger} — Check 11f (per-account knob, per-symbol §I override, fail-closed
 *       toggle, stale-reference)
 *   <li>{@code RiskLimitsNotLoaded} — Check 0a (SEC 15c3-5(b) fail-closed boot)
 *   <li>{@code SymbolEligibility} — Check 11g (Reg SHO short-sale + tradingAllowed gate)
 *   <li>{@code FourEyesViolation} — §H (MiFID II RTS 6 §1(2) dual control)
 * </ul>
 *
 * <p><b>Threading.</b> Each test is single-threaded — the cluster duty-cycle invariant requires it
 * and the {@link FakeCluster} fixture preserves that contract.
 *
 * <p><b>Allocation.</b> Off the hot path — allocates fixture buffers per test method. Acceptable
 * for an audit-only pack.
 */
@Tag("risk-evidence")
class RiskControlEvidenceIT {

  /** Stable cluster timestamp used by every boundary case. */
  private static final long TS = 1_700_000_000_000_000_000L;

  /** Whole-unit fixed-point price (1.0 in 10⁻⁸). */
  private static final long PRICE = FixedPointScale.PRICE_SCALE;

  /**
   * Whole-unit fixed-point quantity (1 unit). Reuses {@link FixedPointScale#PRICE_SCALE} because
   * the project shares the same 10⁸ scale factor for both prices and quantities (per the cluster's
   * fixed-point design). NOT a price — the alias keeps the boundary-row {@code inputValue} strings
   * readable: {@code workingLong=900_000_000, orderQty=100_000_000} maps to {@code currentLong=9
   * units, orderQty=1 unit, projected=10 units == limit}.
   */
  private static final long QTY = FixedPointScale.PRICE_SCALE;

  private static final String CHECK_POSITION_LIMIT = "PositionLimit";
  private static final String CHECK_FAT_FINGER = "FatFinger";
  private static final String CHECK_RISK_LIMITS_NOT_LOADED = "RiskLimitsNotLoaded";
  private static final String CHECK_SYMBOL_ELIGIBILITY = "SymbolEligibility";
  private static final String CHECK_FOUR_EYES = "FourEyesViolation";

  // ===========================================================================
  // PositionLimit (Check 11e)
  // ===========================================================================

  /**
   * Drives the §4 working-position check at five boundary cases: {@code projected==limit−1}, {@code
   * projected==limit}, {@code projected==limit+1}, {@code Long.MAX_VALUE} working position (safeAdd
   * saturation), and {@code positionLimitEnabled=false} (admit-bypass). The handler computes {@code
   * projected = currentLong + orderQty} for Buy; the test seeds {@code currentLong} so the
   * projected value lands exactly on the requested boundary.
   *
   * <p>The {@code safeAdd} defensive guard asserts {@code b &gt;= 0L} on the order-quantity input,
   * so a {@code Long.MIN_VALUE} delta probe through {@code applyWorkingPositionForTest} would trip
   * the assertion and is intentionally NOT exercised here — production NEVER produces a negative
   * working position (the only delta paths are admit/cancel, both with non-negative quantities).
   */
  @Test
  void positionLimit_boundaries() {
    // limit = 10 units (10 * PRICE_SCALE). Working-position is in fixed-point units.
    final long limit = 10L * FixedPointScale.PRICE_SCALE;
    // QTY = 1 unit; the handler computes projected = currentLong + QTY for Buy.

    // projected = limit-1 → admit
    recordPositionLimit(limit, limit - QTY - 1L, "projected=limit-1 (admit)", "(admit)");
    // projected = limit → admit (strict `>` check)
    recordPositionLimit(limit, limit - QTY, "projected=limit (admit, strict >)", "(admit)");
    // projected = limit+1 → reject
    recordPositionLimit(
        limit,
        limit - QTY + 1L,
        "projected=limit+1 (reject)",
        RejectReasonEnum.PositionLimitExceeded.name());
    // currentLong = Long.MAX_VALUE → safeAdd saturates to MAX_VALUE → reject
    recordPositionLimit(
        limit,
        Long.MAX_VALUE,
        "currentLong=Long.MAX_VALUE → safeAdd saturates (reject)",
        RejectReasonEnum.PositionLimitExceeded.name());
    // positionLimitEnabled = false → admit even at MAX_VALUE
    recordPositionLimitDisabled(Long.MAX_VALUE);
  }

  // ===========================================================================
  // FatFinger (Check 11f)
  // ===========================================================================

  /**
   * Drives the §5 fat-finger gate at six boundary cases — threshold−1 / threshold / threshold+1,
   * fail-closed no-reference reject, fail-open no-reference admit, stale-reference reject, and the
   * §I per-symbol override taking precedence over the per-account knob.
   */
  @Test
  void fatFinger_boundaries() {
    final long bps = 100L; // 100 bps = 1% deviation tolerance
    // threshold-1 (99 bps) → admit
    recordFatFinger(bps, 99L, true, true, "threshold-1 (admit)", "(admit)");
    // threshold (100 bps) → admit (strict `>` check)
    recordFatFinger(bps, 100L, true, true, "threshold (admit, strict >)", "(admit)");
    // threshold+1 (101 bps) → reject
    recordFatFinger(
        bps,
        101L,
        true,
        true,
        "threshold+1 (reject)",
        RejectReasonEnum.PriceTooFarFromMarket.name());
    // no reference + failClosed=true → reject
    recordFatFingerNoReference(
        bps,
        true,
        "no-reference, failClosed=true (reject)",
        RejectReasonEnum.PriceTooFarFromMarket.name());
    // no reference + failClosed=false → admit
    recordFatFingerNoReference(bps, false, "no-reference, failClosed=false (admit)", "(admit)");
    // stale reference → treated as no reference; fail-closed → reject
    recordFatFingerStaleReference(
        bps,
        "stale-reference, failClosed=true (reject)",
        RejectReasonEnum.PriceTooFarFromMarket.name());
    // §I per-symbol override (50 bps) tighter than account knob (1000 bps) at 75 bps → reject
    recordFatFingerPerSymbolOverride(
        1_000L,
        50L,
        75L,
        "per-symbol override (50) tighter than account knob (1000) at 75bps (reject)",
        RejectReasonEnum.PriceTooFarFromMarket.name());
    // crossed-book skip — no reference cached when bid > ask; admit by fall-through
    recordFatFingerCrossedBookSkip(
        bps, "crossed-book skip (no reference cached) (admit)", "(admit)");
  }

  // ===========================================================================
  // RiskLimitsNotLoaded (Check 0a)
  // ===========================================================================

  /**
   * Drives the §0a fail-closed boot check: account with no risk-limit record rejects, account with
   * a record passes through (admit happy path).
   */
  @Test
  void riskLimitsNotLoaded_boundaries() {
    recordRiskLimitsNotLoaded(
        false,
        "no risk-limit record loaded for account (reject)",
        RejectReasonEnum.RiskLimitsNotLoaded.name());
    recordRiskLimitsNotLoaded(true, "risk-limit record loaded for account (admit)", "(admit)");
  }

  // ===========================================================================
  // SymbolEligibility (Check 11g)
  // ===========================================================================

  /**
   * Drives the §G short-sale / restricted-symbol gate at five boundary cases: missing record,
   * tradingAllowed=false, Sell with shortSaleAllowed=false, Buy with shortSaleAllowed=false (the
   * conservative Phase-1 carve-out — Buy is admitted), and the all-permissive happy path.
   */
  @Test
  void symbolEligibility_boundaries() {
    recordSymbolEligibility(
        EligibilityScenario.NO_RECORD,
        SideEnum.Buy,
        "no eligibility record (reject)",
        RejectReasonEnum.RegulatoryRestriction.name());
    recordSymbolEligibility(
        EligibilityScenario.TRADING_DISALLOWED,
        SideEnum.Buy,
        "tradingAllowed=false (reject)",
        RejectReasonEnum.RegulatoryRestriction.name());
    recordSymbolEligibility(
        EligibilityScenario.SHORT_SALE_DISALLOWED,
        SideEnum.Sell,
        "Sell + shortSaleAllowed=false (reject)",
        RejectReasonEnum.RegulatoryRestriction.name());
    recordSymbolEligibility(
        EligibilityScenario.SHORT_SALE_DISALLOWED,
        SideEnum.Buy,
        "Buy + shortSaleAllowed=false (admit — Phase-1 carve-out)",
        "(admit)");
    recordSymbolEligibility(
        EligibilityScenario.PERMISSIVE, SideEnum.Buy, "all-permissive (admit)", "(admit)");
  }

  // ===========================================================================
  // FourEyesViolation (§H)
  // ===========================================================================

  /**
   * Drives the §H 4-eyes check at four boundary cases: equal IDs (reject), empty proposer (reject),
   * empty approver (reject), distinct non-empty (admit).
   */
  @Test
  void fourEyes_boundaries() {
    recordFourEyes(
        "PROPOSER".getBytes(StandardCharsets.US_ASCII),
        "PROPOSER".getBytes(StandardCharsets.US_ASCII),
        "proposer == approver (reject)",
        RejectReasonEnum.FourEyesViolation.name());
    recordFourEyes(
        new byte[0],
        "APPROVER".getBytes(StandardCharsets.US_ASCII),
        "empty proposer (reject)",
        RejectReasonEnum.FourEyesViolation.name());
    recordFourEyes(
        "PROPOSER".getBytes(StandardCharsets.US_ASCII),
        new byte[0],
        "empty approver (reject)",
        RejectReasonEnum.FourEyesViolation.name());
    recordFourEyes(
        "ALICE".getBytes(StandardCharsets.US_ASCII),
        "BOB".getBytes(StandardCharsets.US_ASCII),
        "distinct non-empty (admit)",
        "(admit)");
  }

  // ===========================================================================
  // Helpers — PositionLimit
  // ===========================================================================

  /**
   * Seeds the working-long position to {@code currentLong}, dispatches a {@code Buy QTY} NOS, and
   * records the (expected, observed) reject reason. The production handler computes {@code
   * projected = currentLong + orderQty} via {@code safeAdd}; the caller is responsible for choosing
   * {@code currentLong} so {@code projected} lands on the intended boundary.
   *
   * @param limit value to set as both {@code maxLongPosition} and {@code maxShortPosition}
   * @param currentLong working-long quantity to pre-seed via {@code applyWorkingPositionForTest}
   * @param boundaryCase short human-readable boundary label
   * @param expectedReason expected reject reason name or {@code "(admit)"}
   */
  private void recordPositionLimit(
      final long limit,
      final long currentLong,
      final String boundaryCase,
      final String expectedReason) {
    final var harness = newHarness();
    final var lim = harness.riskLimitStore.get(1L);
    lim.setPositionLimitEnabled(true);
    lim.setMaxLongPosition(limit);
    lim.setMaxShortPosition(limit);
    harness.handler.applyWorkingPositionForTest(
        1L, harness.eurusdSymbolHash, SideEnum.Buy, currentLong);

    harness.dispatchLimitNos("ORD-PL", "ACME", PRICE, SideEnum.Buy);
    final var observed = harness.lastObservedReason();
    final var observedCheck = harness.lastObservedCheck();
    final var pass = expectedReason.equals(observed);
    RiskEvidenceRecorder.record(
        CHECK_POSITION_LIMIT,
        boundaryCase,
        String.format(
            "maxLongPosition=%d, workingLong=%d, orderQty=%d, observedCheckId=%s",
            limit, currentLong, QTY, observedCheck),
        expectedReason,
        observed,
        pass);
  }

  /** Disabled-flag bypass: {@code positionLimitEnabled=false} admits even at Long.MAX_VALUE. */
  private void recordPositionLimitDisabled(final long currentLong) {
    final var harness = newHarness();
    final var lim = harness.riskLimitStore.get(1L);
    lim.setPositionLimitEnabled(false);
    lim.setMaxLongPosition(0L);
    harness.handler.applyWorkingPositionForTest(
        1L, harness.eurusdSymbolHash, SideEnum.Buy, currentLong);
    harness.dispatchLimitNos("ORD-PL-OFF", "ACME", PRICE, SideEnum.Buy);
    final var observed = harness.lastObservedReason();
    final var pass = "(admit)".equals(observed);
    RiskEvidenceRecorder.record(
        CHECK_POSITION_LIMIT,
        "positionLimitEnabled=false at Long.MAX_VALUE (admit-bypass)",
        String.format("maxLongPosition=0, workingLong=%d, positionLimitEnabled=false", currentLong),
        "(admit)",
        observed,
        pass);
  }

  // ===========================================================================
  // Helpers — FatFinger
  // ===========================================================================

  /**
   * Seeds a fresh mid via {@link NewOrderSingleHandler#updateLastQuotedMidForTest} so the order is
   * priced exactly {@code actualBps} above the mid. Driving via the {@code lastMid + (PRICE *
   * actualBps) / 10_000} formula matches the production deviationBps computation.
   */
  private void recordFatFinger(
      final long thresholdBps,
      final long actualBps,
      final boolean failClosed,
      final boolean haveReference,
      final String boundaryCase,
      final String expectedReason) {
    final var harness = newHarness();
    enableFatFinger(harness, 1L, thresholdBps, failClosed);
    if (haveReference) {
      harness.handler.updateLastQuotedMidForTest(
          harness.eurusdSymbolHash, PRICE - 1L, PRICE + 1L, TS);
    }
    final long price = PRICE + (PRICE * actualBps) / 10_000L;
    harness.dispatchLimitNos("ORD-FF", "ACME", price, SideEnum.Buy);
    final var observed = harness.lastObservedReason();
    final var pass = expectedReason.equals(observed);
    RiskEvidenceRecorder.record(
        CHECK_FAT_FINGER,
        boundaryCase,
        String.format(
            "thresholdBps=%d, actualBps=%d, failClosed=%s, refSeeded=%s",
            thresholdBps, actualBps, failClosed, haveReference),
        expectedReason,
        observed,
        pass);
  }

  /** No reference cached at all. {@code failClosed=true} → reject; {@code false} → admit. */
  private void recordFatFingerNoReference(
      final long thresholdBps,
      final boolean failClosed,
      final String boundaryCase,
      final String expectedReason) {
    final var harness = newHarness();
    enableFatFinger(harness, 1L, thresholdBps, failClosed);
    // NO updateLastQuotedMidForTest — fat-finger reference is missing.
    harness.dispatchLimitNos("ORD-FF-NOREF", "ACME", PRICE, SideEnum.Buy);
    final var observed = harness.lastObservedReason();
    final var pass = expectedReason.equals(observed);
    RiskEvidenceRecorder.record(
        CHECK_FAT_FINGER,
        boundaryCase,
        String.format("thresholdBps=%d, failClosed=%s, refSeeded=false", thresholdBps, failClosed),
        expectedReason,
        observed,
        pass);
  }

  /**
   * Stale reference: seeds the mid at a timestamp 1 hour in the past — beyond the {@code
   * LAST_PRICE_STALENESS_NANOS} window. Fail-closed path → reject.
   */
  private void recordFatFingerStaleReference(
      final long thresholdBps, final String boundaryCase, final String expectedReason) {
    final var harness = newHarness();
    enableFatFinger(harness, 1L, thresholdBps, true);
    // 1-hour-old reference: 3_600_000_000_000 nanos in the past — outside the staleness window.
    final long staleTs = TS - 3_600_000_000_000L;
    harness.handler.updateLastQuotedMidForTest(
        harness.eurusdSymbolHash, PRICE - 1L, PRICE + 1L, staleTs);
    harness.dispatchLimitNos("ORD-FF-STALE", "ACME", PRICE, SideEnum.Buy);
    final var observed = harness.lastObservedReason();
    final var pass = expectedReason.equals(observed);
    RiskEvidenceRecorder.record(
        CHECK_FAT_FINGER,
        boundaryCase,
        String.format("thresholdBps=%d, refStaleByHours=1, failClosed=true", thresholdBps),
        expectedReason,
        observed,
        pass);
  }

  /**
   * §I per-symbol override: account-wide knob is loose ({@code accountBps=1000}), per-symbol
   * override is tight ({@code overrideBps=50}); order priced at {@code actualBps=75} should reject
   * against the override (tighter wins).
   */
  private void recordFatFingerPerSymbolOverride(
      final long accountBps,
      final long overrideBps,
      final long actualBps,
      final String boundaryCase,
      final String expectedReason) {
    final var harness = newHarness();
    enableFatFinger(harness, 1L, accountBps, true);
    seedSymbolEligibilityOverride(harness.symbolEligibilityStore, "EURUSD", overrideBps);
    harness.handler.updateLastQuotedMidForTest(
        harness.eurusdSymbolHash, PRICE - 1L, PRICE + 1L, TS);
    final long price = PRICE + (PRICE * actualBps) / 10_000L;
    harness.dispatchLimitNos("ORD-FF-OVR", "ACME", price, SideEnum.Buy);
    final var observed = harness.lastObservedReason();
    final var pass = expectedReason.equals(observed);
    RiskEvidenceRecorder.record(
        CHECK_FAT_FINGER,
        boundaryCase,
        String.format(
            "accountBps=%d, overrideBps=%d, actualBps=%d", accountBps, overrideBps, actualBps),
        expectedReason,
        observed,
        pass);
  }

  /**
   * Crossed-book skip: when the inbound quote is crossed (bid &gt; ask) the {@code
   * updateLastQuotedMidForTest} call silently skips the cache write, leaving the reference missing.
   * With {@code failClosed=false} the gate falls through and admits.
   */
  private void recordFatFingerCrossedBookSkip(
      final long thresholdBps, final String boundaryCase, final String expectedReason) {
    final var harness = newHarness();
    enableFatFinger(harness, 1L, thresholdBps, false);
    // Bid > ask is crossed; production path skips the cache update.
    harness.handler.updateLastQuotedMidForTest(
        harness.eurusdSymbolHash, PRICE + 10L, PRICE - 10L, TS);
    harness.dispatchLimitNos("ORD-FF-CROSSED", "ACME", PRICE, SideEnum.Buy);
    final var observed = harness.lastObservedReason();
    final var pass = expectedReason.equals(observed);
    RiskEvidenceRecorder.record(
        CHECK_FAT_FINGER,
        boundaryCase,
        String.format("thresholdBps=%d, bid>ask (crossed), failClosed=false", thresholdBps),
        expectedReason,
        observed,
        pass);
  }

  // ===========================================================================
  // Helpers — RiskLimitsNotLoaded
  // ===========================================================================

  /**
   * Removes the risk-limit record for account 1 (when {@code loaded=false}) and dispatches a NOS.
   * Records the (expected, observed) reject reason. The seeded harness has account 1 record
   * present; {@code loaded=false} explicitly removes it.
   */
  private void recordRiskLimitsNotLoaded(
      final boolean loaded, final String boundaryCase, final String expectedReason) {
    // For the "not loaded" case build a harness whose risk-limit store is left empty
    // (RiskLimitStore
    // exposes no `remove(long)` — `clear()` is the supported eviction primitive). For the "loaded"
    // case the seeded harness already carries a permissive limit for account 1.
    final var harness = newHarness();
    if (!loaded) {
      harness.riskLimitStore.clear();
    }
    harness.dispatchLimitNos("ORD-RLN", "ACME", PRICE, SideEnum.Buy);
    final var observed = harness.lastObservedReason();
    final var pass = expectedReason.equals(observed);
    RiskEvidenceRecorder.record(
        CHECK_RISK_LIMITS_NOT_LOADED,
        boundaryCase,
        String.format("riskLimitStore.contains(account=1)=%s", loaded),
        expectedReason,
        observed,
        pass);
  }

  // ===========================================================================
  // Helpers — SymbolEligibility
  // ===========================================================================

  /** Eligibility-check scenarios driving §G boundary fuzz. */
  private enum EligibilityScenario {
    NO_RECORD,
    TRADING_DISALLOWED,
    SHORT_SALE_DISALLOWED,
    PERMISSIVE,
  }

  /** Drives §G with the given scenario + side and records the observed reason. */
  private void recordSymbolEligibility(
      final EligibilityScenario scenario,
      final SideEnum side,
      final String boundaryCase,
      final String expectedReason) {
    // For the no-record case we construct a harness with an EMPTY symbol-eligibility store. The
    // seeded harness pre-populates EURUSD via ReferenceDataSeeder; SymbolEligibilityStore exposes
    // no remove(long), so we re-wire with a fresh empty store instead.
    final var harness =
        scenario == EligibilityScenario.NO_RECORD
            ? newHarnessWithEmptySymbolEligibility()
            : newHarness();
    final var store = harness.symbolEligibilityStore;
    if (scenario != EligibilityScenario.NO_RECORD) {
      final var rec = store.get(harness.eurusdSymbolHash);
      switch (scenario) {
        case TRADING_DISALLOWED -> rec.setTradingAllowed(false);
        case SHORT_SALE_DISALLOWED -> rec.setShortSaleAllowed(false);
        case PERMISSIVE -> {
          /* defaults already permissive */
        }
        case NO_RECORD -> {
          /* unreachable */
        }
      }
    }
    harness.dispatchLimitNos("ORD-SE", "ACME", PRICE, side);
    final var observed = harness.lastObservedReason();
    final var pass = expectedReason.equals(observed);
    RiskEvidenceRecorder.record(
        CHECK_SYMBOL_ELIGIBILITY,
        boundaryCase,
        String.format("scenario=%s, side=%s", scenario.name(), side.name()),
        expectedReason,
        observed,
        pass);
  }

  // ===========================================================================
  // Helpers — FourEyesViolation
  // ===========================================================================

  /**
   * Drives the §H 4-eyes check by encoding a {@code LoadRiskLimit} SBE message with the given
   * proposer / approver bytes and dispatching it through {@link LoadRiskLimitHandler}. Records the
   * observed reject reason from the emitted {@code RiskLimitLoadRejectedEvent} (or {@code
   * "(admit)"} if a {@code RiskLimitLoadedEvent} is emitted instead).
   */
  private void recordFourEyes(
      final byte[] proposerId,
      final byte[] approverId,
      final String boundaryCase,
      final String expectedReason) {
    final var accountStore = new AccountStore();
    accountStore.put(
        AccountFixtures.account(
            1L, "ACME", AccountStatusEnum.Active, AccountState.Capabilities.CAN_TRADE));
    final var riskLimitStore = new RiskLimitStore();
    final var handler = new LoadRiskLimitHandler(riskLimitStore, accountStore);

    final MutableDirectBuffer src = new ExpandableArrayBuffer(256);
    final int srcLength = encodeLoadRiskLimitWith4EyesBytes(src, proposerId, approverId);
    final MutableDirectBuffer eventDst = new ExpandableArrayBuffer(256);

    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    header.wrap(src, 0);
    handler.onCommand(header, src, 0, srcLength, eventDst, 0, 99L, TS);

    final MessageHeaderDecoder out = new MessageHeaderDecoder();
    out.wrap(eventDst, 0);
    final String observed;
    if (out.templateId() == RiskLimitLoadRejectedEventDecoder.TEMPLATE_ID) {
      final var dec = new RiskLimitLoadRejectedEventDecoder();
      dec.wrap(eventDst, MessageHeaderDecoder.ENCODED_LENGTH, out.blockLength(), out.version());
      observed = dec.rejectReason().name();
    } else if (out.templateId() == RiskLimitLoadedEventDecoder.TEMPLATE_ID) {
      observed = "(admit)";
    } else {
      observed = "(unknown-template=" + out.templateId() + ")";
    }
    final var pass = expectedReason.equals(observed);
    RiskEvidenceRecorder.record(
        CHECK_FOUR_EYES,
        boundaryCase,
        String.format(
            "proposerLen=%d, approverLen=%d, equal=%s",
            proposerId.length, approverId.length, Arrays.equals(proposerId, approverId)),
        expectedReason,
        observed,
        pass);
  }

  /**
   * Encodes a {@code LoadRiskLimit} message with explicit proposer / approver byte arrays (the
   * standard {@link SbeTestEncoder#encodeLoadRiskLimit} auto-fills both with sentinel non-empty
   * distinct values, which would skip the §H gate). Mirrors the pattern from {@code
   * LoadRiskLimitHandlerTest}.
   */
  private static int encodeLoadRiskLimitWith4EyesBytes(
      final MutableDirectBuffer dst, final byte[] proposerId, final byte[] approverId) {
    final var headerEnc = new MessageHeaderEncoder();
    final var enc = new LoadRiskLimitEncoder();
    enc.wrapAndApplyHeader(dst, 0, headerEnc);
    final byte[] proposerPadded = padded16(proposerId);
    final byte[] approverPadded = padded16(approverId);
    enc.accountId(1L)
        .maxOrderSize(100L)
        .maxOrderNotional(0L)
        .maxDailyVolume(1000L)
        .putProposerId(proposerPadded, 0)
        .putApproverId(approverPadded, 0)
        .status(AccountStatusEnum.Active)
        .transactTime(0L);
    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  private static byte[] padded16(final byte[] src) {
    final byte[] out = new byte[16];
    System.arraycopy(src, 0, out, 0, Math.min(src.length, out.length));
    return out;
  }

  // ===========================================================================
  // Harness — direct NewOrderSingleHandler wiring (no Aeron)
  // ===========================================================================

  /**
   * Per-test harness wiring a {@link NewOrderSingleHandler} against fake cluster fixtures. Each
   * boundary case constructs a fresh harness so state never leaks between rows.
   */
  private static final class Harness {

    final AccountStore accountStore;
    final CurrencyStore currencyStore;
    final RiskLimitStore riskLimitStore;
    final SymbolEligibilityStore symbolEligibilityStore;
    final NewOrderSingleHandler handler;
    final FakeClientSession session;
    final FakeCluster fakeCluster;
    final EventSink eventSink;
    final MutableDirectBuffer msgBuf;
    final UnsafeBuffer decodeBuf;
    final long eurusdSymbolHash;

    Harness(
        final AccountStore accountStore,
        final CurrencyStore currencyStore,
        final RiskLimitStore riskLimitStore,
        final SymbolEligibilityStore symbolEligibilityStore,
        final NewOrderSingleHandler handler,
        final FakeClientSession session,
        final FakeCluster fakeCluster,
        final EventSink eventSink,
        final long eurusdSymbolHash) {
      this.accountStore = accountStore;
      this.currencyStore = currencyStore;
      this.riskLimitStore = riskLimitStore;
      this.symbolEligibilityStore = symbolEligibilityStore;
      this.handler = handler;
      this.session = session;
      this.fakeCluster = fakeCluster;
      this.eventSink = eventSink;
      this.msgBuf = new ExpandableArrayBuffer(512);
      this.decodeBuf = new UnsafeBuffer(0, 0);
      this.eurusdSymbolHash = eurusdSymbolHash;
    }

    /** Dispatch a Limit NOS through the wired handler. */
    void dispatchLimitNos(
        final String clOrdId, final String accountCode, final long price, final SideEnum side) {
      final int len =
          SbeTestEncoder.encodeNewOrderSingle(
              msgBuf,
              0,
              clOrdId,
              "EURUSD",
              side,
              OrdTypeEnum.Limit,
              price,
              QTY,
              accountCode,
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
    }

    /**
     * Returns the reject reason of the most-recently-emitted {@code OrderRejectedEvent}, or {@code
     * "(admit)"} when no reject was emitted (i.e. an {@code OrderCreatedEvent} was emitted
     * instead).
     */
    String lastObservedReason() {
      if (session.messages.isEmpty()) {
        return "(no-event)";
      }
      decodeBuf.wrap(session.messages.get(session.messages.size() - 1));
      final var hdr = new MessageHeaderDecoder();
      hdr.wrap(decodeBuf, 0);
      if (hdr.templateId() != OrderRejectedEventDecoder.TEMPLATE_ID) {
        return "(admit)";
      }
      final var dec = new OrderRejectedEventDecoder();
      dec.wrap(decodeBuf, MessageHeaderDecoder.ENCODED_LENGTH, hdr.blockLength(), hdr.version());
      return dec.rejectReason().name();
    }

    /** Returns the checkId of the most-recently-emitted reject, or {@code "(admit)"}. */
    String lastObservedCheck() {
      if (session.messages.isEmpty()) {
        return "(no-event)";
      }
      decodeBuf.wrap(session.messages.get(session.messages.size() - 1));
      final var hdr = new MessageHeaderDecoder();
      hdr.wrap(decodeBuf, 0);
      if (hdr.templateId() != OrderRejectedEventDecoder.TEMPLATE_ID) {
        return "(admit)";
      }
      final var dec = new OrderRejectedEventDecoder();
      dec.wrap(decodeBuf, MessageHeaderDecoder.ENCODED_LENGTH, hdr.blockLength(), hdr.version());
      final var checkId = dec.checkId();
      return checkId == null ? "(null)" : checkId.name();
    }
  }

  /** Build a fresh harness wired to the standard ReferenceDataSeeder + permissive symbol store. */
  private static Harness newHarness() {
    return newHarnessInternal(ReferenceDataSeeder.permissiveSymbolEligibilityStore());
  }

  /**
   * Build a fresh harness whose {@link SymbolEligibilityStore} is empty. Used by the §G "no
   * eligibility record" boundary case where the seeder's permissive EURUSD record would otherwise
   * mask the fail-closed reject path.
   */
  private static Harness newHarnessWithEmptySymbolEligibility() {
    return newHarnessInternal(new SymbolEligibilityStore());
  }

  /** Shared harness-build path parameterized by the {@link SymbolEligibilityStore} to wire. */
  private static Harness newHarnessInternal(final SymbolEligibilityStore symbolEligibilityStore) {
    final var accountStore = new AccountStore();
    final var currencyStore = new CurrencyStore();
    final var riskLimitStore = new RiskLimitStore();

    accountStore.put(
        AccountFixtures.account(
            1L, "ACME", AccountStatusEnum.Active, AccountState.Capabilities.CAN_TRADE));
    currencyStore.put(
        CurrencyStore.packCode((byte) 'U', (byte) 'S', (byte) 'D'), CurrencyFixtures.usd());
    riskLimitStore.put(RiskLimitFixtures.permissive(1L));

    final var fakeCluster = new FakeCluster(0L);
    final var orderBook = new OrderBook(128);
    final var orderIdGen = new IdGenerator("ORD");
    final var execIdGen = new IdGenerator("EXE");
    final var quoteIdGen = new IdGenerator("QTE");
    final var tradingState = new TradingState(orderBook, orderIdGen, execIdGen, quoteIdGen);
    final var sequencer = new EventSequencer();
    final var journal = new EventJournal(64);
    final var eventSink = new EventSink(sequencer, journal);
    eventSink.setCluster(fakeCluster);
    final var rfqMetrics = new RfqMetrics();
    final var rfqStateMachine =
        new RfqStateMachine(
            256,
            TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS,
            TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS,
            TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS,
            TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS,
            TradingClusteredServiceFactory.DEFAULT_RFQ_REQUEST_TIMEOUT_NANOS,
            TradingClusteredServiceFactory.DEFAULT_RFQ_RATE_LIMIT_PER_SESSION,
            TradingClusteredServiceFactory.DEFAULT_RFQ_RATE_LIMIT_WINDOW_NANOS,
            0,
            0,
            accountStore,
            rfqMetrics);
    final var handler =
        new NewOrderSingleHandler(
            tradingState, accountStore, currencyStore, riskLimitStore, symbolEligibilityStore);
    handler.wireRfqStateMachine(rfqStateMachine, rfqMetrics);

    final var session = new FakeClientSession(1L);
    fakeCluster.addClientSession(session);

    final long eurusdHash = packEurUsdSymbolHash();
    return new Harness(
        accountStore,
        currencyStore,
        riskLimitStore,
        symbolEligibilityStore,
        handler,
        session,
        fakeCluster,
        eventSink,
        eurusdHash);
  }

  /** Pack the "EURUSD" symbol bytes via {@link SymbolEligibilityState#packSymbolKey}. */
  private static long packEurUsdSymbolHash() {
    final byte[] sym = new byte[8];
    final byte[] src = "EURUSD".getBytes(StandardCharsets.US_ASCII);
    System.arraycopy(src, 0, sym, 0, src.length);
    return SymbolEligibilityState.packSymbolKey(sym, 0);
  }

  /** Configure the fat-finger gates on the given account. */
  private static void enableFatFinger(
      final Harness harness,
      final long accountId,
      final long deviationBps,
      final boolean failClosed) {
    final var limit = harness.riskLimitStore.get(accountId);
    limit.setFatFingerEnabled(true);
    limit.setFatFingerFailClosed(failClosed);
    limit.setPriceDeviationBps(deviationBps);
  }

  /** Seed (or overwrite) the symbol-eligibility override for the given symbol. */
  private static void seedSymbolEligibilityOverride(
      final SymbolEligibilityStore store, final String symbol, final long overrideBps) {
    final byte[] sym = new byte[8];
    final byte[] src = symbol.getBytes(StandardCharsets.US_ASCII);
    System.arraycopy(src, 0, sym, 0, Math.min(src.length, 8));
    final long hash = SymbolEligibilityState.packSymbolKey(sym, 0);
    var rec = store.get(hash);
    if (rec == null) {
      rec = new SymbolEligibilityState();
      rec.setSymbolBytes(sym, 0, sym.length);
      rec.setSymbolHash(hash);
      rec.setTradingAllowed(true);
      rec.setShortSaleAllowed(true);
    }
    rec.setPriceDeviationBpsOverride(overrideBps);
    store.put(rec);
  }
}
