/**
 * marketDataConflation.test.ts — unit tests for MarketDataConflation per
 * Phase 3 Commit B.
 *
 * Covers: single onTick → one drain emit; rapid onTick same symbol →
 * latest-value-wins (one drain emit); multi-symbol → one emit per symbol;
 * install + drain via vi.useFakeTimers(); dispose stops timer + clears map.
 *
 * Uses vi.useFakeTimers() to control setInterval without actual wall time.
 *
 * Test naming follows `<unit>_<scenario>_<expectedBehavior>` per plan §5.8.
 *
 * Threading: single-threaded (Vitest jsdom).
 */

import { describe, expect, it, beforeEach, afterEach, vi } from "vitest";
import {
  MarketDataConflation,
  MARKET_DATA_RENDER_MS,
  type MarketDataTickFrame,
} from "@/workers/marketDataConflation";
import type { PriceUpdate } from "@/shared/transport/MessageShape";
import { pack } from "@/shared/transport/SymbolPacking";

// ─── Fixture helpers ─────────────────────────────────────────────────────────

const EUR = pack("EURUSD");
const GBP = pack("GBPUSD");
const JPY = pack("USDJPY");

function makeFrame(
  symbol: string,
  bid: bigint,
  serverNanos = 1_000_000_000n,
): MarketDataTickFrame {
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

// ─── Tests ───────────────────────────────────────────────────────────────────

describe("MarketDataConflation (synchronous drain)", () => {
  let emitted: PriceUpdate[];
  let conflation: MarketDataConflation;

  beforeEach(() => {
    emitted = [];
    // Inject a stable nowMillis that returns a fixed value so latency math is deterministic
    const nowMillis = (): number => 1_000_000; // 1e6 ms → 1e12 ns as BigInt
    conflation = new MarketDataConflation((u) => emitted.push(u), nowMillis);
  });

  it("onTick_singleTick_drainEmitsExactlyOneUpdate", () => {
    conflation.onTick(EUR, makeFrame("EURUSD", 110_000_000n));

    expect(conflation.pendingCount()).toBe(1);
    conflation.drain();

    expect(emitted).toHaveLength(1);
    expect(emitted[0]?.symbol).toBe("EURUSD");
    expect(emitted[0]?.bid).toBe(110_000_000n);
    expect(conflation.pendingCount()).toBe(0);
  });

  it("onTick_rapidTicksSameSymbol_drainEmitsOnlyLatestFrame", () => {
    const frame1 = makeFrame("EURUSD", 100_000_000n);
    const frame2 = makeFrame("EURUSD", 101_000_000n);
    const frame3 = makeFrame("EURUSD", 102_000_000n);

    conflation.onTick(EUR, frame1);
    conflation.onTick(EUR, frame2);
    conflation.onTick(EUR, frame3);

    expect(conflation.pendingCount()).toBe(1); // only one slot for EUR

    conflation.drain();

    expect(emitted).toHaveLength(1);
    // Must be the LAST frame (frame3)
    expect(emitted[0]?.bid).toBe(102_000_000n);
  });

  it("onTick_multipleSymbols_drainEmitsOneUpdatePerSymbol", () => {
    conflation.onTick(EUR, makeFrame("EURUSD", 110_000_000n));
    conflation.onTick(GBP, makeFrame("GBPUSD", 125_000_000n));
    conflation.onTick(JPY, makeFrame("USDJPY", 140_000_000n));

    expect(conflation.pendingCount()).toBe(3);
    conflation.drain();

    expect(emitted).toHaveLength(3);
    const symbols = emitted.map((u) => u.symbol).sort();
    expect(symbols).toEqual(["EURUSD", "GBPUSD", "USDJPY"]);
    expect(conflation.pendingCount()).toBe(0);
  });

  it("drain_emptyMap_producesNoEmitsAndIsIdempotent", () => {
    conflation.drain();
    conflation.drain();

    expect(emitted).toHaveLength(0);
  });

  it("drain_clearsMapAfterEachCycle", () => {
    conflation.onTick(EUR, makeFrame("EURUSD", 100_000_000n));
    conflation.drain();

    expect(conflation.pendingCount()).toBe(0);

    // Second drain cycle — nothing buffered, no emits
    conflation.drain();
    expect(emitted).toHaveLength(1); // still just the first one
  });

  it("emittedUpdate_containsCorrectBidAskAndSymbol", () => {
    const frame = makeFrame("EURUSD", 123_456_789n, 900_000_000_000n);
    conflation.onTick(EUR, frame);
    conflation.drain();

    const update = emitted[0];
    expect(update?.symbol).toBe("EURUSD");
    expect(update?.bid).toBe(123_456_789n);
    expect(update?.ask).toBe(123_456_799n); // bid + 10n
    expect(update?.bidSize).toBe(1_000_000_000n);
    expect(update?.askSize).toBe(1_000_000_000n);
    expect(update?.type).toBe("price");
  });

  it("emittedUpdate_publisherStackLatencyNanos_equalsServerNanosMinusIngressNanos", () => {
    const serverNanos = 2_000_000_000n;
    const ingressNanos = serverNanos - 500n;
    const frame: MarketDataTickFrame = {
      symbol: "EURUSD",
      bid: 100n,
      ask: 110n,
      bidSize: 1n,
      askSize: 1n,
      ingressNanos,
      serverNanos,
    };
    conflation.onTick(EUR, frame);
    conflation.drain();

    expect(emitted[0]?.publisherStackLatencyNanos).toBe(serverNanos - ingressNanos);
  });
});

describe("MarketDataConflation (timer-driven drain via vi.useFakeTimers)", () => {
  let emitted: PriceUpdate[];
  let conflation: MarketDataConflation;

  beforeEach(() => {
    vi.useFakeTimers();
    emitted = [];
    const nowMillis = (): number => performance.now();
    conflation = new MarketDataConflation((u) => emitted.push(u), nowMillis);
  });

  afterEach(() => {
    conflation.dispose();
    vi.useRealTimers();
  });

  it("install_afterTimerFires_drainIsInvoked", () => {
    conflation.onTick(EUR, makeFrame("EURUSD", 110_000_000n));
    expect(emitted).toHaveLength(0); // not yet drained

    conflation.install();

    vi.advanceTimersByTime(MARKET_DATA_RENDER_MS);

    expect(emitted).toHaveLength(1);
    expect(emitted[0]?.symbol).toBe("EURUSD");
  });

  it("install_idempotent_secondCallDoesNotArmSecondInterval", () => {
    conflation.install();
    conflation.install(); // second call is a no-op

    conflation.onTick(EUR, makeFrame("EURUSD", 100_000_000n));
    conflation.onTick(EUR, makeFrame("EURUSD", 200_000_000n));

    vi.advanceTimersByTime(MARKET_DATA_RENDER_MS);

    // Only one drain cycle fires (latest-value-wins)
    expect(emitted).toHaveLength(1);
    expect(emitted[0]?.bid).toBe(200_000_000n);
  });

  it("dispose_stopsTimer_noFurtherDrainsAfterDispose", () => {
    conflation.install();
    conflation.onTick(EUR, makeFrame("EURUSD", 100_000_000n));

    conflation.dispose();

    // Advance past the drain interval — no emit expected
    vi.advanceTimersByTime(MARKET_DATA_RENDER_MS * 5);

    expect(emitted).toHaveLength(0);
    expect(conflation.pendingCount()).toBe(0); // map was cleared by dispose
  });

  it("dispose_isIdempotent_secondDisposeDoesNotThrow", () => {
    conflation.install();
    conflation.dispose();

    expect(() => { conflation.dispose(); }).not.toThrow();
  });
});
