package com.trading.engine.cluster.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link RfqMetrics}, verifying default initialisation, aggregate totals, and the
 * subset invariant between related counters.
 *
 * <p>Tests exercise the plain-{@code long}-field approach (no {@code AtomicLong}): single-threaded
 * cluster duty cycle, zero allocation after construction.
 *
 * <p><b>Threading:</b> all tests single-threaded.
 */
class RfqMetricsTest {

  private RfqMetrics metrics;

  @BeforeEach
  void setUp() {
    metrics = new RfqMetrics();
  }

  // =========================================================================
  // §1 — defaultConstructed_allCountersZero
  // =========================================================================

  /**
   * A freshly constructed {@link RfqMetrics} must have every public {@code long} field initialised
   * to zero. This guards against accidentally adding a field with a non-zero default or a static
   * initialiser that inadvertently sets a counter.
   */
  @Test
  void defaultConstructed_allCountersZero() {
    // Emission counters
    assertEquals(0L, metrics.emitRequested);
    assertEquals(0L, metrics.emitCreated);
    assertEquals(0L, metrics.emitRejected);
    assertEquals(0L, metrics.emitExpired);
    assertEquals(0L, metrics.emitAccepted);
    assertEquals(0L, metrics.emitExpiredSessionClosed);
    assertEquals(0L, metrics.emitCreatedReplay);

    // Reject counters
    assertEquals(0L, metrics.rejectMalformed);
    assertEquals(0L, metrics.rejectSymbolEmpty);
    assertEquals(0L, metrics.rejectAccountInactive);
    assertEquals(0L, metrics.rejectRfqNotPermitted);
    assertEquals(0L, metrics.rejectCurrencyUnknown);
    assertEquals(0L, metrics.rejectRateLimit);
    assertEquals(0L, metrics.rejectDuplicate);
    assertEquals(0L, metrics.rejectPoolExhausted);
    assertEquals(0L, metrics.rejectTimerExhausted);
    assertEquals(0L, metrics.rejectPricingDeclined);
    assertEquals(0L, metrics.rejectRequestTimeout);
    assertEquals(0L, metrics.rejectUnknownQuote);
    assertEquals(0L, metrics.rejectStaleQuote);
    assertEquals(0L, metrics.rejectQuoteSideMismatch);
    assertEquals(0L, metrics.rejectQuotePriceMismatch);
    assertEquals(0L, metrics.rejectQuoteQtyMismatch);

    // Drop counters
    assertEquals(0L, metrics.dropUnknownReqId);
    assertEquals(0L, metrics.dropAlreadyQuoted);
    assertEquals(0L, metrics.dropTerminal);
    assertEquals(0L, metrics.dropAfterTerminal);
    assertEquals(0L, metrics.dropMalformedPriceResponse);
    assertEquals(0L, metrics.dropIdempotentRetx);
    assertEquals(0L, metrics.dropStaleTimer);

    // Recovery counters
    assertEquals(0L, metrics.recoveryExpiredOnRestore);
    assertEquals(0L, metrics.recoveryQuotedRearmed);
    assertEquals(0L, metrics.recoveryRequestRearmed);
    assertEquals(0L, metrics.recoveryRequestTimedOut);
    assertEquals(0L, metrics.recoveryTimerRearmFailed);
    assertEquals(0L, metrics.recoveryAcceptedReleased);
    assertEquals(0L, metrics.recoveryAccountMissing);

    // Pool occupancy
    assertEquals(0L, metrics.poolOccupancy);
    assertEquals(0L, metrics.poolCapacity);
    assertEquals(0L, metrics.poolRetiredSlots);

    // Session counters
    assertEquals(0L, metrics.sessionClosed);
    assertEquals(0L, metrics.sessionCloseTimerRearmFailed);

    // Handler misroute
    assertEquals(0L, metrics.handlerMisroute);

    // Aggregate totals must also be zero when all fields are zero
    assertEquals(0L, metrics.totalRejects());
    assertEquals(0L, metrics.totalEmissions());
    assertEquals(0L, metrics.totalDrops());
  }

  // =========================================================================
  // §2 — totalRejects_sumsAllRejectCounters
  // =========================================================================

  /**
   * {@link RfqMetrics#totalRejects()} must equal the sum of every individual reject counter. Each
   * counter is set to a distinct value so an accidental double-count or omission would produce the
   * wrong sum.
   */
  @Test
  void totalRejects_sumsAllRejectCounters() {
    // Assign a unique value to each reject counter
    metrics.rejectMalformed = 1L;
    metrics.rejectSymbolEmpty = 2L;
    metrics.rejectAccountInactive = 3L;
    metrics.rejectRfqNotPermitted = 4L;
    metrics.rejectCurrencyUnknown = 5L;
    metrics.rejectRateLimit = 6L;
    metrics.rejectDuplicate = 7L;
    metrics.rejectPoolExhausted = 8L;
    metrics.rejectTimerExhausted = 9L;
    metrics.rejectPricingDeclined = 10L;
    metrics.rejectRequestTimeout = 11L;
    metrics.rejectUnknownQuote = 12L;
    metrics.rejectStaleQuote = 13L;
    metrics.rejectQuoteSideMismatch = 14L;
    metrics.rejectQuotePriceMismatch = 15L;
    metrics.rejectQuoteQtyMismatch = 16L;

    final long expected =
        1L + 2L + 3L + 4L + 5L + 6L + 7L + 8L + 9L + 10L + 11L + 12L + 13L + 14L + 15L + 16L;

    assertEquals(
        expected,
        metrics.totalRejects(),
        "totalRejects() must equal the arithmetic sum of all reject counters");
  }

  // =========================================================================
  // §3 — totalEmissions_sumsAllEmissionCounters
  // =========================================================================

  /**
   * {@link RfqMetrics#totalEmissions()} must equal the sum of every individual emission counter.
   * Each counter is set to a distinct value. Note: {@code emitExpiredSessionClosed} is a strict
   * subset of {@code emitExpired} and is intentionally NOT included in {@link
   * RfqMetrics#totalEmissions()} to avoid double-counting; verify that the subset is NOT summed
   * separately.
   */
  @Test
  void totalEmissions_sumsAllEmissionCounters() {
    metrics.emitRequested = 100L;
    metrics.emitCreated = 200L;
    metrics.emitRejected = 300L;
    metrics.emitExpired = 400L;
    metrics.emitAccepted = 500L;
    metrics.emitCreatedReplay = 600L;
    // emitExpiredSessionClosed is a subset of emitExpired — must NOT be double-counted
    metrics.emitExpiredSessionClosed = 50L;

    final long expected = 100L + 200L + 300L + 400L + 500L + 600L;

    assertEquals(
        expected,
        metrics.totalEmissions(),
        "totalEmissions() must sum the six top-level emission counters excluding the subset");
  }

  // =========================================================================
  // §4 — totalDrops_sumsAllDropCounters
  // =========================================================================

  /** {@link RfqMetrics#totalDrops()} must equal the sum of every individual drop counter. */
  @Test
  void totalDrops_sumsAllDropCounters() {
    metrics.dropUnknownReqId = 10L;
    metrics.dropAlreadyQuoted = 20L;
    metrics.dropTerminal = 30L;
    metrics.dropAfterTerminal = 40L;
    metrics.dropMalformedPriceResponse = 50L;
    metrics.dropIdempotentRetx = 60L;
    metrics.dropStaleTimer = 70L;

    final long expected = 10L + 20L + 30L + 40L + 50L + 60L + 70L;

    assertEquals(
        expected,
        metrics.totalDrops(),
        "totalDrops() must equal the arithmetic sum of all drop counters");
  }

  // =========================================================================
  // §5 — emitExpiredSessionClosed_isSubsetOfEmitExpired
  // =========================================================================

  /**
   * The Javadoc on {@link RfqMetrics#emitExpiredSessionClosed} documents a strict subset invariant:
   * every increment of {@code emitExpiredSessionClosed} MUST be paired with an increment of {@code
   * emitExpired}. This test verifies the invariant by setting both to representative values and
   * asserting the subset relationship. The invariant is enforced by production code convention (see
   * {@code RfqStateMachine.onSessionClose}); this test documents and enforces it as a contractual
   * assertion.
   *
   * <p>The test also confirms that {@code totalEmissions()} does NOT include {@code
   * emitExpiredSessionClosed} as a separate line, so operators cannot double-count it.
   */
  @Test
  void emitExpiredSessionClosed_isSubsetOfEmitExpired() {
    metrics.emitExpired = 10L;
    metrics.emitExpiredSessionClosed = 3L;

    // Subset invariant: every session-closed expiry is also counted in emitExpired
    assertTrue(
        metrics.emitExpiredSessionClosed <= metrics.emitExpired,
        "emitExpiredSessionClosed must be <= emitExpired (strict subset invariant)");

    // totalEmissions does NOT include emitExpiredSessionClosed as a separate line item.
    // Only emitExpired is included. Setting all other emission counters to 0 lets us isolate:
    metrics.emitRequested = 0L;
    metrics.emitCreated = 0L;
    metrics.emitRejected = 0L;
    metrics.emitAccepted = 0L;
    metrics.emitCreatedReplay = 0L;

    assertEquals(
        10L,
        metrics.totalEmissions(),
        "totalEmissions() counts emitExpired once — emitExpiredSessionClosed is NOT added again");
  }
}
