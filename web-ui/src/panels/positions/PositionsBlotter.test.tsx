/**
 * Purpose: Unit tests for PositionsBlotter — buffering, overflow, delta-diff
 * projection, and column-def bigint safety.
 *
 * Rationale: real <AgGridReact> is a no-op under jsdom; stub it to inspect
 * `applyTransactionAsync` calls. Drive messages$ via a controlled Subject.
 * positionStream is mocked so the test controls Map emissions directly,
 * bypassing throttleTime(animationFrameScheduler) which does not fire in jsdom.
 *
 * @see PositionsBlotter — system under test.
 * @see positionStream — mocked upstream operator.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, act } from "@testing-library/react";
import type { GridReadyEvent } from "ag-grid-community";
import { Subject } from "rxjs";
import type { OperatorFunction } from "rxjs";

import type { NetPosition, WorkerMessage } from "@/shared/transport/MessageShape";
import { __TEST_COLUMN_DEFS, PositionsBlotter } from "./PositionsBlotter";

// ── AG Grid stub ───────────────────────────────────────────────────────────
let _onGridReady: ((e: GridReadyEvent) => void) | undefined;
let _fakeApi: { applyTransactionAsync: ReturnType<typeof vi.fn> };

vi.mock("ag-grid-react", () => ({
  AgGridReact: (props: { onGridReady?: (e: GridReadyEvent) => void }) => {
    _onGridReady = props.onGridReady;
    return null;
  },
}));

// ── messages$ stub ─────────────────────────────────────────────────────────
let _messagesSubject: Subject<WorkerMessage>;

vi.mock("@/main-thread/messageSource", () => ({
  get messages$() {
    return _messagesSubject.asObservable();
  },
  startMessageSource: (): void => undefined,
  __resetMessageSourceForTests: (): void => undefined,
}));

// ── positionStream stub ────────────────────────────────────────────────────
// Mock so the test injects a controlled Map subject, bypassing
// throttleTime(animationFrameScheduler) which doesn't fire under jsdom.
let _positionSubject: Subject<ReadonlyMap<string, NetPosition>>;

vi.mock("@/streams/position-stream", () => ({
  positionStream:
    (): OperatorFunction<WorkerMessage, ReadonlyMap<string, NetPosition>> =>
    // Return an operator that IGNORES the upstream source and returns the
    // controlled _positionSubject so tests can push Maps directly.
    (_source) =>
      _positionSubject.asObservable(),
  applyFill: vi.fn(),
}));

// ── connection-store stub ─────────────────────────────────────────────────
vi.mock("@/stores/connection-store", () => ({
  connectionStore: {
    subscribe: () => (): void => undefined,
    getSnapshot: () => "CONNECTED" as const,
  },
  __resetConnectionStoreForTests: (): void => undefined,
}));

// ── Helpers ────────────────────────────────────────────────────────────────

function makePosition(symbol: string): NetPosition {
  return {
    symbol,
    netQty: 100_000_000n,
    avgPx: 108_500_000n,
    lastFillNanos: 1_700_000_000_000_000_000n,
  };
}

function fireFakeOnGridReady(): void {
  _fakeApi = { applyTransactionAsync: vi.fn() };
  _onGridReady?.({ api: _fakeApi } as unknown as GridReadyEvent);
}

// ── Tests ──────────────────────────────────────────────────────────────────

describe("PositionsBlotter", () => {
  beforeEach(() => {
    _messagesSubject = new Subject<WorkerMessage>();
    _positionSubject = new Subject<ReadonlyMap<string, NetPosition>>();
    _onGridReady = undefined;
    _fakeApi = { applyTransactionAsync: vi.fn() };
  });

  it("applyTransactionAsync_beforeOnGridReady_buffersUntilApiReady", () => {
    render(<PositionsBlotter />);

    // Emit a Map with 3 positions — subscribed but onGridReady not yet fired.
    act(() => {
      const map = new Map<string, NetPosition>([
        ["EUR/USD", makePosition("EUR/USD")],
        ["GBP/USD", makePosition("GBP/USD")],
        ["USD/JPY", makePosition("USD/JPY")],
      ]);
      _positionSubject.next(map);
    });

    expect(_fakeApi.applyTransactionAsync).not.toHaveBeenCalled();

    act(() => {
      fireFakeOnGridReady();
    });

    // Flush on onGridReady — at least one applyTransactionAsync call.
    expect(_fakeApi.applyTransactionAsync).toHaveBeenCalled();
  });

  it("applyTransactionAsync_afterOnGridReady_singleUpdate", () => {
    render(<PositionsBlotter />);

    act(() => {
      fireFakeOnGridReady();
    });

    act(() => {
      const map = new Map<string, NetPosition>([["EUR/USD", makePosition("EUR/USD")]]);
      _positionSubject.next(map);
    });
    act(() => {
      const pos2 = makePosition("GBP/USD");
      const map = new Map<string, NetPosition>([["GBP/USD", pos2]]);
      _positionSubject.next(map);
    });

    // Each throttled emission with changed entries produces one call.
    expect(_fakeApi.applyTransactionAsync.mock.calls.length).toBeGreaterThanOrEqual(1);
  });

  it("applyTransactionAsync_pendingOverflow_warnsOnceAndDropsRest", () => {
    const warnSpy = vi.spyOn(console, "warn").mockImplementation((): void => undefined);

    render(<PositionsBlotter />);

    act(() => {
      // Push a Map with 10_001 unique symbols — all are "new" (not in lastSeen),
      // so they all go into the update[] which exceeds PENDING_CAP (10_000).
      const bigMap = new Map<string, NetPosition>();
      for (let i = 0; i <= 10_000; i++) {
        const sym = `SYM-${String(i)}`;
        bigMap.set(sym, makePosition(sym));
      }
      _positionSubject.next(bigMap);
    });

    expect(warnSpy).toHaveBeenCalledTimes(1);
    expect(warnSpy.mock.calls[0]![0]).toMatch(/pending overflow/i);

    warnSpy.mockRestore();
  });

  it("columnDefs_valueGetter_neverReturnsNumber", () => {
    const samplePosition: NetPosition = {
      symbol: "EUR/USD",
      netQty: 100_000_000n,
      avgPx: 108_500_000n,
      lastFillNanos: 1_700_000_000_000_000_000n,
    };
    for (const colDef of __TEST_COLUMN_DEFS) {
      if (typeof colDef.valueGetter === "function") {
        const getter = colDef.valueGetter as (p: { data: NetPosition }) => unknown;
        const result: unknown = getter({ data: samplePosition });
        const name = colDef.headerName ?? "";
        expect(typeof result, `${name} valueGetter returned number`).not.toBe("number");
      }
    }
  });

  it("columnDefs_valueFormatter_bigintInput_returnsString", () => {
    for (const colDef of __TEST_COLUMN_DEFS) {
      if (typeof colDef.valueFormatter === "function") {
        const formatter = colDef.valueFormatter as unknown as (p: {
          value: unknown;
          data: unknown;
        }) => unknown;
        const result: unknown = formatter({
          value: 108_500_000n,
          data: {
            symbol: "EUR/USD",
            netQty: 108_500_000n,
            avgPx: 108_500_000n,
            lastFillNanos: 1_700_000_000_000_000_000n,
          },
        });
        const name = colDef.headerName ?? "";
        expect(typeof result, `${name} valueFormatter did not return string`).toBe("string");
      }
    }
  });

  it("netQtyFormatter_zeroValue_rendersZero", () => {
    // netQty=0n is a flat position — meaningful, must render as "0.00000000", NOT "—".
    const netQtyColDef = __TEST_COLUMN_DEFS.find((c) => c.headerName === "Net Qty");
    expect(netQtyColDef).toBeDefined();
    if (typeof netQtyColDef!.valueFormatter === "function") {
      const formatter = netQtyColDef!.valueFormatter as (p: {
        value: unknown;
        data: unknown;
      }) => unknown;
      const result: unknown = formatter({
        value: 0n,
        data: { symbol: "EUR/USD", netQty: 0n, avgPx: 0n, lastFillNanos: 0n },
      });
      expect(result).toBe("0.00000000");
    }
  });

  it("avgPxFormatter_zeroValue_rendersDash", () => {
    // avgPx=0n means no open position / flat — render as dash.
    const avgPxColDef = __TEST_COLUMN_DEFS.find((c) => c.headerName === "Avg Px");
    expect(avgPxColDef).toBeDefined();
    if (typeof avgPxColDef!.valueFormatter === "function") {
      const formatter = avgPxColDef!.valueFormatter as (p: {
        value: unknown;
        data: unknown;
      }) => unknown;
      const result: unknown = formatter({
        value: 0n,
        data: { symbol: "EUR/USD", netQty: 0n, avgPx: 0n, lastFillNanos: 0n },
      });
      expect(result).toBe("—");
    }
  });
});
