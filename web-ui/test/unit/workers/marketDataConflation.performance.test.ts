/**
 * marketDataConflation.performance.test.ts — cold-path PerformanceTimeline
 * observability tests for {@link MarketDataConflation.drain} (APP-244 Phase 3 C.5).
 *
 * Asserts that:
 *   - A drain with a non-empty conflation map calls `performance.mark` for the
 *     stable start + end marks AND `performance.measure` for the cycle measure.
 *   - A drain over an empty map performs ZERO PerformanceTimeline writes
 *     (zero-allocation no-op invariant).
 *
 * Approach: spy on `performance.mark` / `performance.measure` via `vi.spyOn`,
 * trigger drain in both modes, and assert call counts + arguments. Restores
 * the spies in afterEach so cross-test leakage is impossible.
 *
 * Threading: single-threaded (Vitest jsdom).
 */

import { describe, expect, it, beforeEach, afterEach, vi } from "vitest";
import type { MockInstance } from "vitest";
import {
  MarketDataConflation,
  PERF_MARK_DRAIN_START,
  PERF_MARK_DRAIN_END,
  PERF_MEASURE_DRAIN_CYCLE,
  type MarketDataTickFrame,
} from "@/workers/marketDataConflation";
import type { PriceUpdate } from "@/shared/transport/MessageShape";
import { pack } from "@/shared/transport/SymbolPacking";

const EUR = pack("EURUSD");

function makeFrame(symbol: string, bid: bigint, serverNanos = 1_000_000_000n): MarketDataTickFrame {
  return {
    symbol,
    bid,
    ask: bid + 10n,
    bidSize: 1_000_000_000n,
    askSize: 1_000_000_000n,
    ingressNanos: serverNanos - 500n,
    serverNanos,
  };
}

describe("MarketDataConflation.drain — PerformanceTimeline (APP-244 C.5)", () => {
  let emitted: PriceUpdate[];
  let conflation: MarketDataConflation;
  let markSpy: MockInstance<typeof performance.mark>;
  let measureSpy: MockInstance<typeof performance.measure>;

  beforeEach(() => {
    emitted = [];
    // Stable epoch-millis clock so latency math is deterministic
    const nowMillis = (): number => 1_000_000;
    conflation = new MarketDataConflation((u) => emitted.push(u), nowMillis);
    // Vitest's jsdom environment exposes performance.mark / performance.measure
    // (jsdom 22+ ships the PerformanceTimeline shim). Spies forward to the real
    // implementation so the cycle is end-to-end exercised.
    markSpy = vi.spyOn(performance, "mark");
    measureSpy = vi.spyOn(performance, "measure");
  });

  afterEach(() => {
    markSpy.mockRestore();
    measureSpy.mockRestore();
    conflation.dispose();
  });

  it("drain_emptyMap_writesNoMarksAndNoMeasures", () => {
    conflation.drain();

    expect(markSpy).not.toHaveBeenCalled();
    expect(measureSpy).not.toHaveBeenCalled();
    expect(emitted).toHaveLength(0);
  });

  it("drain_oneSymbolBuffered_writesStartMarkEndMarkAndMeasure", () => {
    conflation.onTick(EUR, makeFrame("EURUSD", 100_000_000n));

    conflation.drain();

    // Two marks (start, end) and one measure per non-empty drain cycle
    expect(markSpy).toHaveBeenCalledTimes(2);
    expect(markSpy).toHaveBeenNthCalledWith(1, PERF_MARK_DRAIN_START);
    expect(markSpy).toHaveBeenNthCalledWith(2, PERF_MARK_DRAIN_END);

    expect(measureSpy).toHaveBeenCalledTimes(1);
    expect(measureSpy).toHaveBeenCalledWith(
      PERF_MEASURE_DRAIN_CYCLE,
      PERF_MARK_DRAIN_START,
      PERF_MARK_DRAIN_END,
    );

    // Sink still fired exactly once (no observability side-effects)
    expect(emitted).toHaveLength(1);
  });

  it("drain_multipleConsecutiveDrainCycles_writesMarksOnlyForNonEmptyCycles", () => {
    // Cycle 1 — non-empty
    conflation.onTick(EUR, makeFrame("EURUSD", 100_000_000n));
    conflation.drain();

    // Cycle 2 — empty (no onTick between drains)
    conflation.drain();

    // Cycle 3 — non-empty again
    conflation.onTick(EUR, makeFrame("EURUSD", 101_000_000n));
    conflation.drain();

    // Two non-empty cycles × 2 marks each = 4 mark calls, NOT 6 (the empty cycle is a no-op)
    expect(markSpy).toHaveBeenCalledTimes(4);
    expect(measureSpy).toHaveBeenCalledTimes(2);
    expect(emitted).toHaveLength(2);
  });
});
