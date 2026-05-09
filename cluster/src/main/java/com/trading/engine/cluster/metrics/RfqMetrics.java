package com.trading.engine.cluster.metrics;

/**
 * Per-counter observability for the RFQ command path. Holds primitive {@code long} counters for
 * every emission, reject, drop, and recovery code path documented in the APP-232 plan §12.1.
 *
 * <p>Using plain {@code long} fields (rather than {@code AtomicLong} or Aeron {@code
 * AtomicCounter}) is intentional: the cluster duty cycle is single-threaded, so unsynchronized
 * increments are safe and zero-allocation. Test code reads counters via the typed accessors. Future
 * export to Aeron CnC (when that infrastructure lands per APP-137) is a one-line change per counter
 * — wrap the field in an {@code AtomicCounter}.
 *
 * <p><b>Threading:</b> not thread-safe — single-threaded cluster duty cycle only.
 *
 * <p><b>Allocation:</b> zero allocation after construction.
 *
 * <p><b>Naming convention:</b> {@code rfq.<dimension>.<reason>} where dimension is one of {emit,
 * reject, drop, recovery, pool, session, handler}. The ASCII counter name is derivable from any
 * field name via {@code dotName(fieldName)}.
 */
public final class RfqMetrics {

  // ---- Emission counters (one per templateId emitted by the RFQ flow) ----

  /** Increments on every {@code QuoteRequestedEvent} (104) emission. */
  public long emitRequested;

  /** Increments on every {@code QuoteCreatedEvent} (105) emission (includes recovery sweep). */
  public long emitCreated;

  /** Increments on every {@code QuoteRejectedEvent} (106) emission (includes recovery sweep). */
  public long emitRejected;

  /** Increments on every {@code QuoteExpiredEvent} (107) emission (includes recovery sweep). */
  public long emitExpired;

  /** Increments on every successful NOS-with-quoteId acceptance (commit phase passes). */
  public long emitAccepted;

  /**
   * 107 emitted as a side effect of session close fast-fail. <b>Strict subset of</b> {@link
   * #emitExpired} — every increment of this counter MUST be paired with an increment of {@code
   * emitExpired} so callers using {@code totalEmissions} cannot double-count.
   */
  public long emitExpiredSessionClosed;

  /** Cached 105 re-emit when a duplicate byte-identical QuoteRequest arrives mid-QUOTED state. */
  public long emitCreatedReplay;

  // ---- Reject ladder counters (one per row of plan §3.1 / §3.2) ----

  /** {@code QuoteRequest} rejected: SBE message shorter than declared block length. */
  public long rejectMalformed;

  /** {@code QuoteRequest} rejected: symbol field is empty. */
  public long rejectSymbolEmpty;

  /** {@code QuoteRequest} rejected: account inactive, suspended, or unknown. */
  public long rejectAccountInactive;

  /** {@code QuoteRequest} rejected: account does not have CAN_RFQ capability. */
  public long rejectRfqNotPermitted;

  /** {@code QuoteRequest} rejected: currency or settlCurrency unknown. */
  public long rejectCurrencyUnknown;

  /** {@code QuoteRequest} rejected: per-session token-bucket rate limit exceeded. */
  public long rejectRateLimit;

  /** {@code QuoteRequest} rejected: duplicate quoteReqId with mismatched body. */
  public long rejectDuplicate;

  /** {@code QuoteRequest} rejected: RFQ slot pool is exhausted. */
  public long rejectPoolExhausted;

  /** {@code QuoteRequest} or PriceResponse rejected: Aeron timer pool exhausted. */
  public long rejectTimerExhausted;

  /** {@code PriceResponse} declined by pricing service (accepted=false). */
  public long rejectPricingDeclined;

  /** Request-timeout timer fired before {@code PriceResponse} arrived. */
  public long rejectRequestTimeout;

  /** NOS-with-quoteId references an unknown quoteId. */
  public long rejectUnknownQuote;

  /** NOS-with-quoteId references a quoteId that has expired (matches recentlyTerminal LRU). */
  public long rejectStaleQuote;

  /** NOS-with-quoteId side does not match the quoted side. */
  public long rejectQuoteSideMismatch;

  /** NOS-with-quoteId price differs beyond rfqAcceptPriceToleranceBps. */
  public long rejectQuotePriceMismatch;

  /** NOS-with-quoteId qty differs beyond rfqAcceptQtyToleranceBps. */
  public long rejectQuoteQtyMismatch;

  // ---- Silent-drop counters ----

  /** {@code PriceResponse} for an unknown quoteReqId — silently dropped. */
  public long dropUnknownReqId;

  /** {@code PriceResponse} arrived for a slot already in QUOTED state — silently dropped. */
  public long dropAlreadyQuoted;

  /** {@code PriceResponse} arrived for a slot in terminal state — silently dropped. */
  public long dropTerminal;

  /** Duplicate {@code QuoteRequest} matched a {@code recentlyTerminal} entry — silently dropped. */
  public long dropAfterTerminal;

  /** Malformed {@code PriceResponse} — silently dropped (no client session to reject toward). */
  public long dropMalformedPriceResponse;

  /** Duplicate byte-identical {@code QuoteRequest} retransmit — silently dropped. */
  public long dropIdempotentRetx;

  /** Timer fired with a stale generation — silently dropped. */
  public long dropStaleTimer;

  // ---- Recovery sweep counters ----

  /** Recovery sweep emitted 107 immediately because validUntil was already past. */
  public long recoveryExpiredOnRestore;

  /** Recovery sweep re-armed a TTL timer for a QUOTED slot with future deadline. */
  public long recoveryQuotedRearmed;

  /** Recovery sweep re-armed a request-timeout timer for a REQUESTED slot. */
  public long recoveryRequestRearmed;

  /** Recovery sweep emitted 106 because the request-timeout deadline had elapsed. */
  public long recoveryRequestTimedOut;

  /** Recovery sweep failed to rearm a timer (Aeron timer pool exhausted at recovery). */
  public long recoveryTimerRearmFailed;

  /** Recovery sweep released an ACCEPTED slot without emission (defensive — should never fire). */
  public long recoveryAcceptedReleased;

  /** Recovery sweep emitted with empty accountCode because account was deleted. */
  public long recoveryAccountMissing;

  // ---- Pool occupancy ----

  /** Current number of allocated (non-FREE) slots. Updated on every acquire/release. */
  public long poolOccupancy;

  /** Configured pool capacity (constant for the cluster lifetime). */
  public long poolCapacity;

  /** Slots permanently retired due to generation counter overflow. */
  public long poolRetiredSlots;

  // ---- Session lifecycle ----

  /** Number of sessions whose close triggered the §7.6a fast-fail path. */
  public long sessionClosed;

  /**
   * Number of timer-rearm failures during {@code onSessionClose} fast-fail. Distinct from {@link
   * #recoveryTimerRearmFailed} which is recorded only during snapshot recovery; operators track
   * these counters separately because the alerting and remediation paths differ.
   */
  public long sessionCloseTimerRearmFailed;

  // ---- Handler-misroute (defensive — should never fire in steady state) ----

  /** Handler received a command with the wrong templateId. */
  public long handlerMisroute;

  /**
   * Constructs an {@link RfqMetrics} with all counters initialized to zero. The {@link
   * #poolCapacity} field should be set explicitly by {@link
   * com.trading.engine.cluster.state.RfqStateMachine} at construction.
   */
  public RfqMetrics() {
    // All long fields default to 0L.
  }

  /**
   * Returns a snapshot total of all reject-family counters. Useful for IT assertions that confirm
   * "exactly one reject occurred".
   *
   * @return sum of all reject counters
   */
  public long totalRejects() {
    return rejectMalformed
        + rejectSymbolEmpty
        + rejectAccountInactive
        + rejectRfqNotPermitted
        + rejectCurrencyUnknown
        + rejectRateLimit
        + rejectDuplicate
        + rejectPoolExhausted
        + rejectTimerExhausted
        + rejectPricingDeclined
        + rejectRequestTimeout
        + rejectUnknownQuote
        + rejectStaleQuote
        + rejectQuoteSideMismatch
        + rejectQuotePriceMismatch
        + rejectQuoteQtyMismatch;
  }

  /**
   * Returns a snapshot total of all emission counters.
   *
   * @return sum of all emit counters
   */
  public long totalEmissions() {
    return emitRequested
        + emitCreated
        + emitRejected
        + emitExpired
        + emitAccepted
        + emitCreatedReplay;
  }

  /**
   * Returns a snapshot total of all silent-drop counters.
   *
   * @return sum of all drop counters
   */
  public long totalDrops() {
    return dropUnknownReqId
        + dropAlreadyQuoted
        + dropTerminal
        + dropAfterTerminal
        + dropMalformedPriceResponse
        + dropIdempotentRetx
        + dropStaleTimer;
  }
}
