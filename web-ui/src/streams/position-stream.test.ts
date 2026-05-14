/**
 * Purpose: Unit tests for position-stream — pure `applyFill` function (VWAP
 * math, sign-flip, flat) and the `positionStream` operator (filter, in-place
 * Map identity).
 *
 * Rationale: applyFill is pure so it can be tested without RxJS plumbing.
 * Operator tests drive a Subject directly to control emission timing without
 * needing fake timers (throttleTime + animationFrameScheduler needs special
 * handling — we assert structural correctness only, not call timing).
 *
 * @see position-stream — system under test.
 * @see applyFill — pure function, main math target.
 */
import { describe, it, expect } from "vitest";
import { Subject, firstValueFrom, toArray } from "rxjs";
import { take } from "rxjs/operators";

import type { FillUpdate, NetPosition, WorkerMessage } from "@/shared/transport/MessageShape";
import { applyFill, positionStream } from "@/streams/position-stream";

// ── Helpers ────────────────────────────────────────────────────────────────
const SCALE = 100_000_000n; // 1e8

function makeFill(symbol: string, side: "BUY" | "SELL", qty: bigint, price: bigint): FillUpdate {
  return {
    type: "fill",
    clOrdId: "ORD-TEST",
    execId: "EXEC-TEST",
    symbol,
    side,
    fillQty: qty,
    fillPrice: price,
    serverNanos: 1_700_000_000_000_000_000n,
  };
}

// ── applyFill unit tests ───────────────────────────────────────────────────

describe("applyFill", () => {
  it("applyFill_firstBuyFromFlat_setsNetQtyToFillQtyAndAvgPxToFillPx", () => {
    const fill = makeFill("EUR/USD", "BUY", 10n * SCALE, 100n * SCALE);
    const result: NetPosition = applyFill(undefined, fill);

    expect(result.netQty).toBe(10n * SCALE);
    expect(result.avgPx).toBe(100n * SCALE);
    expect(result.symbol).toBe("EUR/USD");
  });

  it("applyFill_secondBuySameSide_runningVwap", () => {
    // Buy 10@100, then Buy 10@110. VWAP = (100*10 + 110*10)/(10+10) = 105.
    const fill1 = makeFill("EUR/USD", "BUY", 10n * SCALE, 100n * SCALE);
    const after1: NetPosition = applyFill(undefined, fill1);

    const fill2 = makeFill("EUR/USD", "BUY", 10n * SCALE, 110n * SCALE);
    const after2: NetPosition = applyFill(after1, fill2);

    expect(after2.netQty).toBe(20n * SCALE);
    // VWAP = (100 * 10 + 110 * 10) / (10 + 10) = 105.0 exactly in fixed-point.
    // Note: bigint integer division — (100*10 + 110*10) * SCALE / 20 = 105 * SCALE.
    expect(after2.avgPx).toBe(105n * SCALE);
  });

  it("applyFill_oppositeSideReducingPosition_avgPxPreserved", () => {
    // Buy 10@100, then Sell 5@110 (partial close). Industry-standard
    // position-tracker semantics: a closing trade realises P&L but does
    // NOT change the cost basis of the remaining open quantity. The
    // implementation preserves priorAvgPx in this branch.
    const fill1 = makeFill("EUR/USD", "BUY", 10n * SCALE, 100n * SCALE);
    const after1: NetPosition = applyFill(undefined, fill1);

    const fill2 = makeFill("EUR/USD", "SELL", 5n * SCALE, 110n * SCALE);
    const after2: NetPosition = applyFill(after1, fill2);

    expect(after2.netQty).toBe(5n * SCALE);
    // avgPx preserved at the opening fill's price (100 * SCALE).
    expect(after2.avgPx).toBe(100n * SCALE);
  });

  it("applyFill_signFlip_avgPxResetsToFillPx", () => {
    // Buy 5@100, then Sell 10@110 — position flips to -5 (short), avgPx = fill price.
    const fill1 = makeFill("EUR/USD", "BUY", 5n * SCALE, 100n * SCALE);
    const after1: NetPosition = applyFill(undefined, fill1);

    const fill2 = makeFill("EUR/USD", "SELL", 10n * SCALE, 110n * SCALE);
    const after2: NetPosition = applyFill(after1, fill2);

    expect(after2.netQty).toBe(-5n * SCALE);
    // Sign flipped — residual opens fresh at fill price.
    expect(after2.avgPx).toBe(110n * SCALE);
  });

  it("applyFill_closesPosition_returnsFlat", () => {
    // Buy 5@100, then Sell 5@110 — exactly flat.
    const fill1 = makeFill("EUR/USD", "BUY", 5n * SCALE, 100n * SCALE);
    const after1: NetPosition = applyFill(undefined, fill1);

    const fill2 = makeFill("EUR/USD", "SELL", 5n * SCALE, 110n * SCALE);
    const after2: NetPosition = applyFill(after1, fill2);

    expect(after2.netQty).toBe(0n);
    // Flat — VWAP resets to 0n.
    expect(after2.avgPx).toBe(0n);
  });
});

// ── positionStream operator tests ─────────────────────────────────────────

describe("positionStream", () => {
  it("positionStream_fillEventsOnly_filtersNonFills", async () => {
    const source$ = new Subject<WorkerMessage>();

    const result$ = source$.pipe(positionStream());

    // Collect first emission via take(1).
    const firstEmission = firstValueFrom(result$.pipe(take(1)));

    // Emit one fill and several non-fills.
    source$.next({ type: "price", symbol: "EUR/USD", bid: 108n, ask: 109n, serverNanos: 0n });
    source$.next({ type: "event", seq: 1n, eventType: "Test", details: "", serverNanos: 0n });
    source$.next({
      type: "order",
      clOrdId: "X",
      symbol: "EUR/USD",
      side: "BUY",
      qty: SCALE,
      price: 100n * SCALE,
      status: "OPEN",
      serverNanos: 0n,
    });
    // Now emit the fill — this is the only one that should affect the Map.
    source$.next(makeFill("EUR/USD", "BUY", SCALE, 100n * SCALE));

    const map = await firstEmission;
    expect(map.size).toBe(1);
    expect(map.has("EUR/USD")).toBe(true);
  });

  it("positionStream_inPlaceMap_identityStable", async () => {
    // The same Map reference is emitted every throttled tick —
    // consumers diff via entry reference equality, not Map identity.
    const source$ = new Subject<WorkerMessage>();

    // Collect 2 emissions.
    const twoEmissions = firstValueFrom(source$.pipe(positionStream(), take(2), toArray()));

    source$.next(makeFill("EUR/USD", "BUY", SCALE, 100n * SCALE));
    source$.next(makeFill("GBP/USD", "BUY", SCALE, 125n * SCALE));

    const emissions = await twoEmissions;
    const map1 = emissions[0]!;
    const map2 = emissions[1]!;

    // Both emissions are the SAME Map object (in-place mutation).
    expect(map1).toBe(map2);
    // By the time we inspect, both symbols are present (map2 = same ref as map1).
    expect(map2.size).toBe(2);
  });
});
