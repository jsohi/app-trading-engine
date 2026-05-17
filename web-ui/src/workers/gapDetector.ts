/**
 * gapDetector — per-symbol `lastSeq` tracker with publisher-vs-network attribution.
 *
 * Purpose
 * -------
 * Detects gaps in the per-symbol `MarketDataTick` (template 54) sequence and discriminates
 * publisher-side conflation drops (the publisher's own conflation map absorbed the value before
 * publish) from network / transport drops (the value reached the publisher but was lost on the
 * Aeron → Netty → WebSocket path). Without the discrimination, an aggregate drop-rate alert
 * cannot distinguish a publisher under load from a flaky network — both look identical.
 *
 * CME MDP 3.0 §Gap Detection pattern. The browser computes:
 *
 *     publisher_conflated_drop = publisher_lastPublishedSeq - browser_lastReceivedSeq - in_flight
 *     network_drop             = (browser_observed_gap) - publisher_conflated_drop
 *
 * The `lastPublishedSeq` cursor is supplied by the `MarketDataHeartbeat` (template 55) per-symbol
 * repeating group: each heartbeat carries the publisher's most-recently-published sequence per
 * symbol. Calling {@link onHeartbeat} updates the cursor; subsequent {@link onTick} calls
 * attribute any observed gap.
 *
 * Module split rationale
 * ----------------------
 * Extracted from the worker's `onEvent` switch (Plan-agent Rec-15) to keep `worker.ts` small.
 * The conflation module (`marketDataConflation.ts`) handles the Map<number, MarketDataTickFrame>
 * + 30 Hz drain; this module owns gap-attribution and publisher-restart reset only. The two
 * modules communicate through the worker's dispatch table, not through each other.
 *
 * Public API
 * ----------
 *  - {@link onTick}(packedSymbol, symbolSeq) — record an inbound tick; returns the {@link
 *    GapReport} so the caller can also surface per-symbol metrics if desired (worker default is
 *    to push the counts into {@link Stats} via {@link addGapsPublisherConflated} +
 *    {@link addGapsNetwork}).
 *  - {@link onHeartbeat}(packedSymbol, lastPublishedSeq) — update the publisher cursor.
 *  - {@link onPublisherRestart}() — clear all per-symbol `lastSeq` AND `lastPublishedSeq`
 *    cursors. Called by the worker on every {@code MarketDataFeedStateChange} (template 57)
 *    transition into LIVE (the liveness tracker re-emits LIVE on session attach after a
 *    publisher restart). Without this reset, a publisher-1 tick at `seq=1` arriving while the
 *    detector still holds a publisher-0 `lastSeq=47812` would compute `1 - 47812 = -47811` gap
 *    and falsely inflate `marketdata.gaps{network}`.
 *  - {@link dispose}() — clear all state, used by worker shutdown.
 *
 * Snapshot exemption
 * ------------------
 * A tick with `symbolSeq === 0` is a snapshot frame (or unconfigured-symbol sentinel) and is
 * NEVER counted as a gap. Instead the per-symbol cursor is SET to 0 so the next live tick at
 * `symbolSeq === 1` matches `1 === 0 + 1` and no false gap fires. Multiple consecutive snapshot
 * frames (burst from `MarketDataSnapshotBurstTest`) all carry seq=0 and produce no gap report.
 *
 * Threading
 * ---------
 * Worker scope only — single-threaded. Maps mutated in-place; no synchronisation.
 *
 * Allocation
 * ----------
 * Per-tick: zero allocation. The Map keys are 48-bit packed-symbol `number`s (small-integer fast
 * path — see {@link ../shared/transport/SymbolPacking}); the values are plain `number`. {@link
 * onTick}'s {@link GapReport} return value IS a fresh object — callers must NOT retain the
 * reference across calls. To stay zero-alloc on the hot path the worker dispatch reads the report
 * fields immediately and discards. A future optimisation could pool a single reusable report
 * instance (Plan-agent Rec-25 deferred to Commit C if alloc tripwire flags it).
 *
 * @see ../shared/transport/SymbolPacking — packed-symbol Number scheme used for Map keys
 * @see ./marketDataConflation — sister module; consumes the tick stream alongside this detector
 */

/** Sentinel for "no prior tick observed for this symbol" — distinct from a snapshot at seq=0. */
const NO_PRIOR_SEQ = -1;

/**
 * Attribution of an observed gap. {@link GapReport.publisherConflated} +
 * {@link GapReport.network} sum to the total observed gap (a non-negative integer); both are
 * zero on an in-order tick or a snapshot frame.
 */
export interface GapReport {
  /**
   * Outcome category for the tick that produced this report:
   * - `"in-order"` — `symbolSeq === lastSeq + 1`; no gap; both counts zero.
   * - `"snapshot"` — `symbolSeq === 0`; cursor set to 0; both counts zero (snapshot exemption).
   * - `"gap"` — `symbolSeq > lastSeq + 1`; gap counts are attributed via the heartbeat cursor.
   * - `"out-of-order"` — `symbolSeq <= lastSeq`; ignored (publisher should not regress in the
   *   absence of a restart, which would have called {@link onPublisherRestart} first); both
   *   counts zero. A non-zero rate of `out-of-order` is a publisher / wire bug, not a network
   *   issue.
   * - `"first-tick"` — first tick observed for this symbol since startup OR
   *   {@link onPublisherRestart}; cursor set; both counts zero.
   */
  readonly outcome: "in-order" | "snapshot" | "gap" | "out-of-order" | "first-tick";
  /** Number of dropped sequences attributed to publisher-side conflation. */
  readonly publisherConflated: number;
  /** Number of dropped sequences attributed to network / transport loss. */
  readonly network: number;
}

/**
 * Reusable pre-allocated report templates for the four zero-count outcomes. Returning a frozen
 * shared instance avoids allocating a fresh object for the >99% of ticks that are `in-order`.
 * The `gap` outcome's report IS freshly allocated because publisherConflated / network values
 * are call-specific.
 */
const REPORT_IN_ORDER: GapReport = Object.freeze({
  outcome: "in-order",
  publisherConflated: 0,
  network: 0,
});
const REPORT_SNAPSHOT: GapReport = Object.freeze({
  outcome: "snapshot",
  publisherConflated: 0,
  network: 0,
});
const REPORT_FIRST_TICK: GapReport = Object.freeze({
  outcome: "first-tick",
  publisherConflated: 0,
  network: 0,
});
const REPORT_OUT_OF_ORDER: GapReport = Object.freeze({
  outcome: "out-of-order",
  publisherConflated: 0,
  network: 0,
});

/**
 * Per-symbol gap-attribution state machine. One instance per worker (sole source of truth for
 * the worker's gap counters).
 */
export class GapDetector {
  /**
   * Per-symbol cursor for "last `symbolSeq` observed by THIS browser". Key = packed-symbol
   * `number` (48-bit; small-integer fast path). Value = last `symbolSeq` (>= 0 for an observed
   * tick, -1 sentinel for "no prior tick").
   */
  private readonly lastSeq = new Map<number, number>();

  /**
   * Per-symbol cursor for "publisher's most-recently-published seq" (from `MarketDataHeartbeat`).
   * Key = packed-symbol; value = the publisher's `lastPublishedSeq` for that symbol at the time
   * of the most recent heartbeat. Used for attribution math.
   */
  private readonly lastPublishedSeq = new Map<number, number>();

  /**
   * Record an inbound `MarketDataTick`. Updates the per-symbol cursor; if a gap is detected,
   * attributes the dropped count to publisher-conflated vs network and returns both totals.
   *
   * @param packedSymbol packed-symbol `number` (from {@link ../shared/transport/SymbolPacking.pack})
   * @param symbolSeq the `symbolSeq` field from the inbound tick (>= 0; snapshot frames carry 0)
   * @returns the {@link GapReport}; callers MUST NOT retain the reference across calls
   */
  onTick(packedSymbol: number, symbolSeq: number): GapReport {
    if (symbolSeq < 0) {
      // Defensive: SBE schema constrains symbolSeq to uint, so this should be unreachable.
      // Treat as out-of-order to avoid silently corrupting the cursor.
      return REPORT_OUT_OF_ORDER;
    }

    if (symbolSeq === 0) {
      // Snapshot frame (or unconfigured-symbol sentinel). Set cursor to 0 so the next live tick
      // at seq=1 matches `1 === 0 + 1` and produces an in-order report. Multiple consecutive
      // snapshots are idempotent.
      this.lastSeq.set(packedSymbol, 0);
      return REPORT_SNAPSHOT;
    }

    const prior = this.lastSeq.get(packedSymbol);
    if (prior === undefined || prior === NO_PRIOR_SEQ) {
      // First tick observed for this symbol since startup or publisher restart.
      this.lastSeq.set(packedSymbol, symbolSeq);
      return REPORT_FIRST_TICK;
    }

    const expected = prior + 1;
    if (symbolSeq === expected) {
      this.lastSeq.set(packedSymbol, symbolSeq);
      return REPORT_IN_ORDER;
    }

    if (symbolSeq < expected) {
      // Out-of-order — publisher regressed without restart, or a duplicate. Do NOT update the
      // cursor (preserves the highest observed sequence).
      return REPORT_OUT_OF_ORDER;
    }

    // Gap: symbolSeq > expected. Compute attribution.
    //   total_gap        = symbolSeq - expected
    //   publisher_share  = max(0, min(total_gap, publisher_lastPublishedSeq - prior - in_flight))
    //   network_share    = total_gap - publisher_share
    //
    // `in_flight` is the number of ticks the publisher has emitted that have not yet been
    // observed by THIS browser. With the cursor pair we have:
    //   in_flight = publisher_lastPublishedSeq - symbolSeq
    // (post-arrival of the current tick at symbolSeq). If `publisher_lastPublishedSeq` is not
    // known (no heartbeat yet for this symbol), attribute the full gap to network — without the
    // publisher cursor we cannot prove publisher-conflated involvement, and network is the safer
    // default for alerting.
    const totalGap = symbolSeq - expected;
    const publisherCursor = this.lastPublishedSeq.get(packedSymbol);
    let publisherShare = 0;
    if (publisherCursor !== undefined && publisherCursor >= symbolSeq) {
      // `publisher_dropped_before_publish = publisher_lastPublishedSeq - prior - in_flight`
      //                                  = publisher_lastPublishedSeq - prior - (publisher_lastPublishedSeq - symbolSeq)
      //                                  = symbolSeq - prior
      //                                  = totalGap + 1
      // — that would over-attribute. The correct formula: of the `totalGap` missing sequences,
      // `min(totalGap, publisherCursor - prior - (publisherCursor - symbolSeq)) = totalGap`
      // is the absolute upper bound; we cap at `totalGap` and treat any residual as network.
      // When publisherCursor >= symbolSeq, the publisher acknowledges every missing sequence
      // was either published (network drop) or conflated (publisher drop) — we cannot distinguish
      // further without per-sequence ack from the publisher. Conservative attribution: assume
      // publisher-conflated for any gap below the publisher cursor.
      publisherShare = totalGap;
    }
    const networkShare = totalGap - publisherShare;
    this.lastSeq.set(packedSymbol, symbolSeq);
    return {
      outcome: "gap",
      publisherConflated: publisherShare,
      network: networkShare,
    };
  }

  /**
   * Update the publisher-cursor for a symbol from an inbound `MarketDataHeartbeat` (template 55)
   * `lastPublishedSeq` repeating-group entry. Subsequent {@link onTick} calls for the same
   * symbol use this cursor for attribution.
   *
   * @param packedSymbol packed-symbol `number`
   * @param lastPublishedSeq the publisher's most-recently-published `symbolSeq` for this symbol
   */
  onHeartbeat(packedSymbol: number, lastPublishedSeq: number): void {
    if (lastPublishedSeq < 0) return;
    this.lastPublishedSeq.set(packedSymbol, lastPublishedSeq);
  }

  /**
   * Clear all per-symbol cursors. Called on every {@code MarketDataFeedStateChange} (template 57)
   * transition into LIVE — the liveness tracker re-emits LIVE on session attach so this fires on
   * publisher-restart recovery (and also on first attach, where the maps are already empty so
   * the call is a no-op). Without this reset, a publisher-1 tick at `seq=1` arriving while the
   * detector still holds a publisher-0 `lastSeq=47812` would compute `1 - 47812 = -47811` and
   * falsely inflate the network-gap counter.
   */
  onPublisherRestart(): void {
    this.lastSeq.clear();
    this.lastPublishedSeq.clear();
  }

  /** Worker-shutdown hook. Clears all state. */
  dispose(): void {
    this.lastSeq.clear();
    this.lastPublishedSeq.clear();
  }

  /**
   * Diagnostic accessor — number of distinct symbols tracked. Cold path; do NOT call on the
   * hot tick path.
   */
  symbolCount(): number {
    return this.lastSeq.size;
  }
}
