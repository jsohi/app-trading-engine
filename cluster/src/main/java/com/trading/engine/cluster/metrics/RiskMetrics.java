package com.trading.engine.cluster.metrics;

/**
 * Per-counter observability for the APP-62 pre-trade risk engine: position limits, fat-finger,
 * fail-closed boot, and PriceResponse-fed reference-cache hygiene. Holds primitive {@code long}
 * counters for every reject and silent-skip path documented in plan §3.8.
 *
 * <p>Using plain {@code long} fields (rather than {@code AtomicLong} or Aeron {@code
 * AtomicCounter}) is intentional and matches the {@link RfqMetrics} idiom: the cluster duty cycle
 * is single-threaded, so unsynchronized increments are safe and zero-allocation. Test code reads
 * counters via direct field access (package-default visibility on the test side). Future export to
 * Aeron CnC (when that infrastructure lands per APP-137) is a one-line change per counter — wrap
 * the field in an {@code AtomicCounter}.
 *
 * <p><b>Threading:</b> not thread-safe — single-threaded cluster duty cycle only.
 *
 * <p><b>Allocation:</b> zero allocation after construction.
 *
 * <p><b>Naming convention:</b> {@code risk.<dimension>.<reason>} where dimension is one of {reject,
 * skip, gauge}. Counter names are the field name verbatim — operators consuming the eventual Aeron
 * CnC labels see the same identifier used in test assertions.
 */
public final class RiskMetrics {

  // ---- Reject counters (one per new APP-62 check that can reject NOS) ----

  /**
   * Increments on every {@code OrderRejectedEvent} emitted with {@code
   * RejectReasonEnum.PositionLimitExceeded} (plan §3.3 check 11e). Counts both Long-Qty and
   * Short-Qty breaches under a single field — operators triage by GFLog WARN (which carries the
   * side).
   */
  public long rejectPositionLimit;

  /**
   * Increments on every {@code OrderRejectedEvent} emitted with {@code
   * RejectReasonEnum.PriceTooFarFromMarket} (plan §3.3 check 11f). Counts both the {@code
   * haveReference + deviation > threshold} path and the {@code !haveReference +
   * fatFingerFailClosed=true} fail-closed path.
   */
  public long rejectFatFinger;

  /**
   * Increments on every {@code OrderRejectedEvent} emitted with {@code
   * RejectReasonEnum.RiskLimitsNotLoaded} (plan §3.3 check 0a, §E fail-closed boot). A non-zero
   * steady-state value typically indicates a reference-data load gap and should page operations.
   */
  public long rejectRiskLimitsNotLoaded;

  // ---- Silent-skip counters on the PriceResponse → reference-cache feed path ----

  /**
   * {@code PriceResponse} bid/ask was crossed, locked, or contained a non-positive sentinel, so the
   * mid was not written to {@code lastQuotedMidPrice}. Documented as expected mid-day behaviour
   * during fast markets — operators alert only on sustained high rate.
   */
  public long priceCrossedLockedSkips;

  /**
   * {@code PriceResponse} carried a price above {@code MAX_REASONABLE_PRICE}, so the mid was not
   * written to the cache. A non-zero value indicates either a pricing-service decimal-place bug or
   * a hostile input — should page on first occurrence in production.
   */
  public long priceUpperBoundSkips;

  /**
   * {@code PriceResponse} arrived from a session ID other than the bootstrap-registered
   * pricing-service session. Reserved for the §3.10 provenance check, which is not yet wired into
   * {@code updateLastQuotedMid} (TODO(APP-62) tracked in the handler). Counter is added now so that
   * wiring is purely additive.
   */
  public long priceProvenanceRejects;

  /**
   * Fat-finger check was reached with no reference price loaded AND the per-account {@code
   * fatFingerFailClosed} knob was false, so the order was admitted without a fat-finger gate.
   * Reserved counter — the current cluster build is fail-closed-only by default, but the knob
   * exists in {@link com.trading.engine.cluster.refdata.RiskLimitState}.
   */
  public long fatFingerNoReferenceSkips;

  // ---- Gauges (snapshot of map sizes, updated periodically — see plan §3.8) ----

  /**
   * Current entry count across {@code accountSymbolWorkingLong} + {@code accountSymbolWorkingShort}
   * inner maps. Updated by the periodic gauge sweep (1 Hz from {@code onTimerEvent}); operators see
   * up to ~1 s lag. <b>Gauge cadence wiring is deferred to APP-137</b>; the field is added now so
   * the sweep change is a one-line callback.
   */
  public long positionMapSize;

  /**
   * Current entry count in {@code lastQuotedMidPrice}. Updated by the periodic gauge sweep. Bounded
   * by the number of distinct symbols ever seen on the pricing-service feed.
   */
  public long lastPriceCacheSize;

  /**
   * Current entry count in {@code accountDailyVolumeState}. Updated by the periodic gauge sweep.
   * Bounded by the number of distinct accounts that have submitted at least one NOS since the
   * current cluster-day rollover.
   */
  public long dailyVolumeMapSize;

  /** Constructs a {@link RiskMetrics} with all counters initialized to zero. */
  public RiskMetrics() {
    // All long fields default to 0L.
  }

  /**
   * Returns a snapshot total of all reject-family counters. Useful for integration-test assertions
   * that confirm "exactly one risk-reject occurred for this NOS".
   *
   * @return sum of all reject counters
   */
  public long totalRejects() {
    return rejectPositionLimit + rejectFatFinger + rejectRiskLimitsNotLoaded;
  }

  /**
   * Returns a snapshot total of all silent-skip counters on the reference-cache feed path. Useful
   * for assertions that the PriceResponse stream is health-checking cleanly.
   *
   * @return sum of all silent-skip counters
   */
  public long totalReferenceCacheSkips() {
    return priceCrossedLockedSkips
        + priceUpperBoundSkips
        + priceProvenanceRejects
        + fatFingerNoReferenceSkips;
  }
}
