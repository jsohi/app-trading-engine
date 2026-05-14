/**
 * position-stream — locally-aggregated net position per symbol from `FillUpdate` events.
 *
 * Filters `WorkerMessage → FillUpdate` then scans into an in-place
 * `Map<symbol, NetPosition>` keyed by symbol. Throttled rAF-aligned so
 * downstream `applyTransactionAsync` calls coalesce to one paint frame.
 *
 * VWAP rule (mirrors LMAX/Goldman position-tracker convention):
 *   - Same-side fill (or first fill from flat):
 *       newQty   = netQty ± fillQty                           (signed)
 *       newAvgPx = (avgPx * |netQty| + fillPx * fillQty)
 *                  / (|netQty| + fillQty)
 *   - Opposite-side fill that does NOT flip sign: reduces open qty,
 *     `avgPx` is preserved (closing trade realises P&L, not displayed here).
 *   - Opposite-side fill that flips sign: residual opens fresh, `avgPx`
 *     reset to `fillPx`.
 *   - Exactly flat (`netQty === 0n`): `avgPx = 0n`.
 *
 * Server-side `PositionProjection` (APP-25, Done) is the authoritative
 * source on reconnect; APP-37 ships local aggregation only. APP-38 covers
 * the real-WebSocket E2E with server resync.
 *
 * **Deletion contract**: this operator is monotonically additive — symbols
 * are never removed from the in-place Map. Consumers' delta-diff loops
 * (PositionsBlotter) only detect ADD + UPDATE, not REMOVE. Server-side
 * resync on reconnect (APP-38) is responsible for any "remove" semantics
 * via a snapshot replay that wipes the local Map at boot.
 *
 * Rationale: matches the in-place-Map / reference-equality projection
 * contract established by `price-stream.ts` so PositionsBlotter can reuse
 * the same delta-diff loop shape as PriceBlotter.
 *
 * Threading: main thread.
 * Allocation: zero per emission after the initial Map allocation (the same
 * Map identity is emitted every throttled tick; values are replaced on
 * change). New `NetPosition` object per changed symbol per fill — bounded
 * by symbol cardinality.
 *
 * Dependencies:
 *   - `@/shared/transport/MessageShape` — peer: `FillUpdate`, `NetPosition`.
 *   - `priceStream` — peer: same in-place Map shape, used by `PriceBlotter`.
 *
 * @see price-stream — peer, same Map/projection contract.
 * @see PositionsBlotter — downstream consumer.
 *
 * Plan reference: APP-37 §Scope item 2 / §position-stream.ts.
 */

import {
  animationFrameScheduler,
  type Observable,
  type OperatorFunction,
  defer,
  filter,
  scan,
  throttleTime,
} from "rxjs";

import {
  type FillUpdate,
  type NetPosition,
  type WorkerMessage,
} from "@/shared/transport/MessageShape";

const THROTTLE_NOMINAL_MS = 100;
const THROTTLE_BACKPRESSURE_MS = 250;

/**
 * Apply a single `FillUpdate` to the running `NetPosition` for its symbol.
 * Returns a new `NetPosition` object — the caller replaces the Map entry.
 *
 * Pure function; no side effects. Exposed for unit testing.
 *
 * VWAP rules (industry-standard position-tracker semantics):
 *   - Opening / adding to the open side (priorIsFlat OR same-sign INCREASE):
 *     `avgPx` is the volume-weighted average across all opening fills.
 *   - Reducing the open side (same-sign DECREASE, position not flat):
 *     `avgPx` is PRESERVED — the closing trade realises P&L but does not
 *     change the cost basis of the remaining open quantity.
 *   - Sign flip (close + reopen on the opposite side): the residual qty
 *     is fresh open exposure; `avgPx` resets to `fillPrice`.
 *   - Exactly flat (`newNetQty === 0n`): `avgPx = 0n`.
 */
export function applyFill(prior: NetPosition | undefined, fill: FillUpdate): NetPosition {
  const signedDelta: bigint = fill.side === "BUY" ? fill.fillQty : -fill.fillQty;
  const priorNetQty: bigint = prior?.netQty ?? 0n;
  const priorAvgPx: bigint = prior?.avgPx ?? 0n;
  const newNetQty: bigint = priorNetQty + signedDelta;

  // Flat after this fill — reset VWAP.
  if (newNetQty === 0n) {
    return {
      symbol: fill.symbol,
      netQty: 0n,
      avgPx: 0n,
      lastFillNanos: fill.serverNanos,
    };
  }

  const priorIsFlat: boolean = priorNetQty === 0n;
  // Boolean intermediates over (a) === (b) — Prettier strips defensive
  // parens, so use explicit intermediates that read straight-through.
  const priorIsPositive: boolean = priorNetQty > 0n;
  const newIsPositive: boolean = newNetQty > 0n;
  const sameSign: boolean = !priorIsFlat && priorIsPositive === newIsPositive;

  const priorAbs: bigint = priorNetQty > 0n ? priorNetQty : -priorNetQty;
  const newAbs: bigint = newNetQty > 0n ? newNetQty : -newNetQty;
  const increasingOpenSide: boolean = sameSign && newAbs > priorAbs;
  const reducingOpenSide: boolean = sameSign && newAbs < priorAbs;

  const newAvgPx: bigint = computeAvgPx({
    priorIsFlat,
    reducingOpenSide,
    increasingOpenSide,
    priorAvgPx,
    priorAbs,
    fillPrice: fill.fillPrice,
    fillQty: fill.fillQty,
  });

  return {
    symbol: fill.symbol,
    netQty: newNetQty,
    avgPx: newAvgPx,
    lastFillNanos: fill.serverNanos,
  };
}

/**
 * Compute the new VWAP for a non-flat post-fill position. Pure; documented
 * branches mirror the VWAP rules in `applyFill`'s JSDoc.
 */
function computeAvgPx(args: {
  readonly priorIsFlat: boolean;
  readonly reducingOpenSide: boolean;
  readonly increasingOpenSide: boolean;
  readonly priorAvgPx: bigint;
  readonly priorAbs: bigint;
  readonly fillPrice: bigint;
  readonly fillQty: bigint;
}): bigint {
  if (args.priorIsFlat) {
    // First fill from flat — VWAP is just the fill price.
    return args.fillPrice;
  }
  if (args.reducingOpenSide) {
    // Closing trade: PRESERVE avgPx. P&L realised elsewhere; remaining
    // open qty keeps its original cost basis.
    return args.priorAvgPx;
  }
  if (args.increasingOpenSide) {
    // Adding to the open side — VWAP-weight by absolute quantities.
    //
    // **No divide-by-zero**: `increasingOpenSide` is only true when
    // `sameSign && newAbs > priorAbs`. `sameSign` itself requires
    // `!priorIsFlat`, so `priorAbs > 0n`. `fillQty` is non-negative by
    // FIX semantics (the SBE `FillUpdate` decoder rejects negatives). So
    // `totalAbs = priorAbs + fillQty > 0n` — bigint division is safe.
    // (Gemini R3 review of PR #72.)
    const totalAbs: bigint = args.priorAbs + args.fillQty;
    return (args.priorAvgPx * args.priorAbs + args.fillPrice * args.fillQty) / totalAbs;
  }
  // Sign flipped (close + reopen): residual is fresh exposure; reset.
  return args.fillPrice;
}

/**
 * RxJS operator: aggregate per-symbol `FillUpdate` emissions into a shared
 * `Map<symbol, NetPosition>` and emit the (mutated-in-place) map per
 * throttled tick. Map identity is stable; consumers MUST diff via reference
 * equality of the entries (matches `priceStream` semantics).
 *
 * @param backpressure widens the throttle to 250 ms under BACKPRESSURE.
 *   APP-37 hard-codes `false`; APP-248 wires the toggle.
 */
export function positionStream(
  backpressure = false,
): OperatorFunction<WorkerMessage, ReadonlyMap<string, NetPosition>> {
  const interval = backpressure ? THROTTLE_BACKPRESSURE_MS : THROTTLE_NOMINAL_MS;
  return (source: Observable<WorkerMessage>) =>
    // defer(...) wraps the pipe so the `scan` seed Map (line 138) is
    // constructed PER SUBSCRIBER, not once at operator-factory time.
    // Without defer, a multi-subscriber pipe (or accidental re-pipe) would
    // share the seed Map across subscribers and leak fills between them.
    // PositionsBlotter is single-subscriber today, but the per-subscriber
    // contract makes this safe by construction.
    defer(() =>
      source.pipe(
        filter((m): m is FillUpdate => m.type === "fill"),
        // scan FIRST, throttle AFTER — fills must NOT be dropped (each fill
        // mutates the aggregate). Unlike priceStream (where individual price
        // updates are last-write-wins per symbol so pre-throttle drop is
        // acceptable), every fill changes the running netQty/avgPx and
        // dropping one corrupts the position permanently.
        scan<FillUpdate, Map<string, NetPosition>>((acc, fill) => {
          // In-place mutation — same Map identity emitted every tick.
          acc.set(fill.symbol, applyFill(acc.get(fill.symbol), fill));
          return acc;
        }, new Map<string, NetPosition>()),
        throttleTime(interval, animationFrameScheduler, {
          leading: true,
          trailing: true,
        }),
      ),
    );
}
