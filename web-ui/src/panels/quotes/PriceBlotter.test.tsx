/**
 * Purpose: Unit tests for PriceBlotter — delta-diff projection correctness,
 * remount full-resync invariant, and column-def bigint safety.
 *
 * Rationale: real <AgGridReact> is a no-op under jsdom; stub it to inspect
 * `applyTransactionAsync` calls. priceStream is mocked so the test controls
 * Map emissions directly, bypassing throttleTime(animationFrameScheduler)
 * which does not fire under jsdom.
 *
 * @see PriceBlotter — system under test.
 * @see priceStream — mocked upstream operator.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, act } from "@testing-library/react";
import type { GridReadyEvent } from "ag-grid-community";
import { Subject } from "rxjs";
import type { OperatorFunction } from "rxjs";

import type { PriceUpdate, WorkerMessage } from "@/shared/transport/MessageShape";
import { __TEST_COLUMN_DEFS, PriceBlotter } from "./PriceBlotter";

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

// ── priceStream stub ───────────────────────────────────────────────────────
// Bypass throttleTime(animationFrameScheduler) which does not fire in jsdom.
// The operator ignores its upstream and returns the controlled subject.
let _priceMapSubject: Subject<ReadonlyMap<string, PriceUpdate>>;

vi.mock("@/streams/price-stream", () => ({
  priceStream: (): OperatorFunction<PriceUpdate, ReadonlyMap<string, PriceUpdate>> => (_source) =>
    _priceMapSubject.asObservable(),
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

function makePrice(symbol: string, bid: bigint, ask: bigint): PriceUpdate {
  return {
    type: "price",
    symbol,
    bid,
    ask,
    serverNanos: 1_700_000_000_000_000_000n,
  };
}

function fireFakeOnGridReady(): void {
  _fakeApi = { applyTransactionAsync: vi.fn() };
  _onGridReady?.({ api: _fakeApi } as unknown as GridReadyEvent);
}

// ── Tests ──────────────────────────────────────────────────────────────────

describe("PriceBlotter", () => {
  beforeEach(() => {
    _messagesSubject = new Subject<WorkerMessage>();
    _priceMapSubject = new Subject<ReadonlyMap<string, PriceUpdate>>();
    _onGridReady = undefined;
    _fakeApi = { applyTransactionAsync: vi.fn() };
  });

  it("applyTransactionAsync_afterOnGridReady_singleUpdate", () => {
    const { unmount } = render(<PriceBlotter />);

    act(() => {
      fireFakeOnGridReady();
    });

    // Emit a price Map — PriceBlotter projects it via delta-diff.
    act(() => {
      const map = new Map<string, PriceUpdate>([
        ["EUR/USD", makePrice("EUR/USD", 108_500_000n, 108_700_000n)],
      ]);
      _priceMapSubject.next(map);
    });

    expect(_fakeApi.applyTransactionAsync.mock.calls.length).toBeGreaterThanOrEqual(1);

    unmount();
  });

  it("deltaDiff_onlyChangedSymbol_pushesOneRow", () => {
    // Map identity is stable; PriceBlotter's delta-diff uses per-entry reference
    // equality. Emit two separate Maps: the second shares one entry ref, changes another.
    const eur1 = makePrice("EUR/USD", 108_500_000n, 108_700_000n);
    const gbp1 = makePrice("GBP/USD", 125_000_000n, 125_200_000n);

    const { unmount } = render(<PriceBlotter />);

    act(() => {
      fireFakeOnGridReady();
    });

    // First emission — both symbols new, both pushed to update[].
    act(() => {
      const map1 = new Map<string, PriceUpdate>([
        ["EUR/USD", eur1],
        ["GBP/USD", gbp1],
      ]);
      _priceMapSubject.next(map1);
    });

    const callsBefore = _fakeApi.applyTransactionAsync.mock.calls.length;

    // Second emission — GBP/USD uses same reference (gbp1), only EUR/USD changes.
    const eur2 = makePrice("EUR/USD", 108_600_000n, 108_800_000n);
    act(() => {
      const map2 = new Map<string, PriceUpdate>([
        ["EUR/USD", eur2],
        ["GBP/USD", gbp1], // same reference as before → delta-diff skips
      ]);
      _priceMapSubject.next(map2);
    });

    // At least one new call with only the changed row.
    const callsAfter = _fakeApi.applyTransactionAsync.mock.calls.length;
    expect(callsAfter).toBeGreaterThan(callsBefore);

    // The most recent call should contain only EUR/USD.
    const lastCall = _fakeApi.applyTransactionAsync.mock.calls.at(-1)![0] as {
      update: PriceUpdate[];
    };
    expect(lastCall.update.every((p) => p.symbol === "EUR/USD")).toBe(true);

    unmount();
  });

  it("remount_freshLastSeen_triggersFullResync", () => {
    // On remount, lastSeen is fresh so the first emission produces all symbols.
    const eur = makePrice("EUR/USD", 108_500_000n, 108_700_000n);
    const gbp = makePrice("GBP/USD", 125_000_000n, 125_200_000n);

    const { unmount } = render(<PriceBlotter />);

    act(() => {
      fireFakeOnGridReady();
    });

    // Drive 2 symbols to seed lastSeen.
    act(() => {
      _priceMapSubject.next(
        new Map([
          ["EUR/USD", eur],
          ["GBP/USD", gbp],
        ]),
      );
    });

    // Unmount.
    unmount();

    // Reset mocks for remount measurement.
    _fakeApi = { applyTransactionAsync: vi.fn() };
    _onGridReady = undefined;

    // Fresh _priceMapSubject for the remounted component.
    _priceMapSubject = new Subject<ReadonlyMap<string, PriceUpdate>>();

    // Remount — fresh lastSeen inside useEffect.
    const { unmount: unmount2 } = render(<PriceBlotter />);

    act(() => {
      fireFakeOnGridReady();
    });

    // Emit same symbols with same references — remount's fresh lastSeen means
    // both entries are "new" (not in lastSeen yet), so both go into update[].
    act(() => {
      _priceMapSubject.next(
        new Map([
          ["EUR/USD", eur],
          ["GBP/USD", gbp],
        ]),
      );
    });

    // Full resync: at least one call on remount.
    expect(_fakeApi.applyTransactionAsync.mock.calls.length).toBeGreaterThanOrEqual(1);

    // The call should contain both symbols.
    const resyncCall = _fakeApi.applyTransactionAsync.mock.calls[0]![0] as {
      update: PriceUpdate[];
    };
    expect(resyncCall.update).toHaveLength(2);

    unmount2();
  });

  it("priceFormatter_zeroBidAndAsk_rendersDash", () => {
    const bidColDef = __TEST_COLUMN_DEFS.find((c) => c.headerName === "Bid");
    expect(bidColDef).toBeDefined();
    if (typeof bidColDef!.valueFormatter === "function") {
      const formatter = bidColDef!.valueFormatter as (p: {
        value: unknown;
        data: unknown;
      }) => unknown;
      const result: unknown = formatter({ value: 0n, data: makePrice("EUR/USD", 0n, 0n) });
      expect(result).toBe("—");
    }
  });

  it("spreadGetter_zeroSide_returnsNull", () => {
    // Spread valueGetter: if bid=0n OR ask=0n, return null (not zero).
    const spreadColDef = __TEST_COLUMN_DEFS.find((c) => c.headerName === "Spread");
    expect(spreadColDef).toBeDefined();
    if (typeof spreadColDef!.valueGetter === "function") {
      const getter = spreadColDef!.valueGetter as (p: { data: PriceUpdate }) => unknown;
      const result: unknown = getter({ data: makePrice("EUR/USD", 0n, 108_700_000n) });
      expect(result).toBeNull();

      const result2: unknown = getter({ data: makePrice("EUR/USD", 108_500_000n, 0n) });
      expect(result2).toBeNull();
    }
  });

  it("columnDefs_valueGetter_neverReturnsNumber", () => {
    const samplePrice: PriceUpdate = makePrice("EUR/USD", 108_500_000n, 108_700_000n);
    for (const colDef of __TEST_COLUMN_DEFS) {
      if (typeof colDef.valueGetter === "function") {
        const getter = colDef.valueGetter as (p: { data: PriceUpdate }) => unknown;
        const result: unknown = getter({ data: samplePrice });
        if (result !== null && result !== undefined) {
          const name = colDef.headerName ?? "";
          expect(typeof result, `${name} valueGetter returned number`).not.toBe("number");
        }
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
          data: makePrice("EUR/USD", 108_500_000n, 108_700_000n),
        });
        const name = colDef.headerName ?? "";
        expect(typeof result, `${name} valueFormatter did not return string`).toBe("string");
      }
    }
  });
});
