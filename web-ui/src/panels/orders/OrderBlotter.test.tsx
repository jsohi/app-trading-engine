/**
 * Purpose: Unit tests for OrderBlotter — buffering, overflow, column-def
 * bigint safety, and AG Grid transaction dispatch behaviour.
 *
 * Rationale: real <AgGridReact> is a no-op under jsdom (no ResizeObserver);
 * stub it so we can inspect `applyTransactionAsync` call counts and args.
 * Drive messages$ via a controlled Subject injected by the vi.mock factory.
 *
 * @see OrderBlotter — system under test.
 * @see messageSource — mocked upstream.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, act } from "@testing-library/react";
import type { GridReadyEvent } from "ag-grid-community";
import { Subject } from "rxjs";

import type { OrderUpdate, WorkerMessage } from "@/shared/transport/MessageShape";
import { __TEST_COLUMN_DEFS, OrderBlotter } from "./OrderBlotter";

// ── AG Grid stub ───────────────────────────────────────────────────────────
// vi.mock is hoisted before imports; factory must be a function, not a class.
let _onGridReady: ((e: GridReadyEvent) => void) | undefined;
let _fakeApi: { applyTransactionAsync: ReturnType<typeof vi.fn> };

vi.mock("ag-grid-react", () => {
  return {
    AgGridReact: (props: { onGridReady?: (e: GridReadyEvent) => void }) => {
      // Capture the callback so tests can fire it after mount.
      _onGridReady = props.onGridReady;
      return null;
    },
  };
});

// ── messages$ stub ─────────────────────────────────────────────────────────
// Controlled Subject the tests push into directly.
let _messagesSubject: Subject<WorkerMessage>;

vi.mock("@/main-thread/messageSource", () => {
  // The factory runs once; subsequent access uses the module's exported binding.
  // We expose the Subject as a closure so resetSubject() can swap it.
  return {
    get messages$() {
      return _messagesSubject.asObservable();
    },
    startMessageSource: (): void => undefined,
    __resetMessageSourceForTests: (): void => undefined,
  };
});

// ── connection-store stub ─────────────────────────────────────────────────
vi.mock("@/stores/connection-store", () => ({
  connectionStore: {
    subscribe: () => (): void => undefined,
    getSnapshot: () => "CONNECTED" as const,
  },
  __resetConnectionStoreForTests: (): void => undefined,
}));

// ── Helpers ────────────────────────────────────────────────────────────────

function makeOrder(id: string): OrderUpdate {
  return {
    type: "order",
    clOrdId: id,
    symbol: "EUR/USD",
    side: "BUY",
    qty: 100_000_000n,
    price: 108_500_000n,
    status: "OPEN",
    serverNanos: 1_700_000_000_000_000_000n,
  };
}

function fireFakeOnGridReady(): void {
  _fakeApi = { applyTransactionAsync: vi.fn() };
  _onGridReady?.({ api: _fakeApi } as unknown as GridReadyEvent);
}

// ── Tests ──────────────────────────────────────────────────────────────────

describe("OrderBlotter", () => {
  beforeEach(() => {
    _messagesSubject = new Subject<WorkerMessage>();
    _onGridReady = undefined;
    _fakeApi = { applyTransactionAsync: vi.fn() };
  });

  it("applyTransactionAsync_beforeOnGridReady_buffersUntilApiReady", () => {
    // Render first so useEffect subscribes to messages$, THEN emit 3 orders
    // before onGridReady fires. Subject only delivers to active subscribers.
    render(<OrderBlotter />);

    // Emit 3 orders — component is subscribed but onGridReady not yet fired.
    act(() => {
      _messagesSubject.next(makeOrder("ORD-001"));
      _messagesSubject.next(makeOrder("ORD-002"));
      _messagesSubject.next(makeOrder("ORD-003"));
    });

    // Grid API not yet ready — no calls yet.
    expect(_fakeApi.applyTransactionAsync).not.toHaveBeenCalled();

    act(() => {
      fireFakeOnGridReady();
    });

    // Flush on onGridReady — one call. All three clOrdIds are first-time-seen,
    // so AG Grid v33+ requires them in the `add` array (rows not yet matched
    // by getRowId). The `update` array is empty for this flush.
    expect(_fakeApi.applyTransactionAsync).toHaveBeenCalledTimes(1);
    const call = _fakeApi.applyTransactionAsync.mock.calls[0]![0] as {
      add: OrderUpdate[];
      update: OrderUpdate[];
    };
    expect(call.add).toHaveLength(3);
    expect(call.update).toHaveLength(0);
    expect(call.add.map((o: OrderUpdate) => o.clOrdId)).toEqual(["ORD-001", "ORD-002", "ORD-003"]);
  });

  it("applyTransactionAsync_afterOnGridReady_partitionsAddAndUpdate", () => {
    render(<OrderBlotter />);

    act(() => {
      fireFakeOnGridReady();
    });

    // First emissions for ORD-A and ORD-B are NEW rows — must land in `add`.
    act(() => {
      _messagesSubject.next(makeOrder("ORD-A"));
    });
    act(() => {
      _messagesSubject.next(makeOrder("ORD-B"));
    });
    // Second emission for ORD-A is now an existing row — must land in `update`.
    act(() => {
      _messagesSubject.next(makeOrder("ORD-A"));
    });

    expect(_fakeApi.applyTransactionAsync).toHaveBeenCalledTimes(3);
    const firstCall = _fakeApi.applyTransactionAsync.mock.calls[0]![0] as {
      add: OrderUpdate[];
      update: OrderUpdate[];
    };
    expect(firstCall.add[0]!.clOrdId).toBe("ORD-A");
    expect(firstCall.update).toHaveLength(0);

    const secondCall = _fakeApi.applyTransactionAsync.mock.calls[1]![0] as {
      add: OrderUpdate[];
      update: OrderUpdate[];
    };
    expect(secondCall.add[0]!.clOrdId).toBe("ORD-B");
    expect(secondCall.update).toHaveLength(0);

    const thirdCall = _fakeApi.applyTransactionAsync.mock.calls[2]![0] as {
      add: OrderUpdate[];
      update: OrderUpdate[];
    };
    expect(thirdCall.add).toHaveLength(0);
    expect(thirdCall.update[0]!.clOrdId).toBe("ORD-A");
  });

  it("partitionAndApply_duplicateIdsWithinBatch_firstAddsSecondUpdates", () => {
    // Regression for Gemini R1 review (HIGH) of PR #72: a batch containing
    // two emissions for the same first-time-seen clOrdId would previously
    // land BOTH in `add[]` and fire `onInsert` twice — duplicating the row
    // in AG Grid and corrupting OrderBlotter's FIFO. The hook now tracks
    // `batchSeenNewIds` in `partitionAndApply` so the second occurrence
    // lands in `update[]` and `onInsert` fires exactly once.
    //
    // Trigger: emit 2 orders with the same clOrdId BEFORE onGridReady so
    // they both land in `pending[]`. The flush calls partitionAndApply
    // with a 2-element batch where both entries share an id.
    _onGridReady = undefined;
    render(<OrderBlotter />);
    act(() => {
      _messagesSubject.next(makeOrder("DUP-ID"));
      _messagesSubject.next(makeOrder("DUP-ID"));
    });
    act(() => {
      fireFakeOnGridReady();
    });

    expect(_fakeApi.applyTransactionAsync).toHaveBeenCalledTimes(1);
    const call = _fakeApi.applyTransactionAsync.mock.calls[0]![0] as {
      add: OrderUpdate[];
      update: OrderUpdate[];
    };
    // First occurrence is the new row → add[]; second is the same id, so
    // dedup promotes it to update[].
    expect(call.add).toHaveLength(1);
    expect(call.add[0]!.clOrdId).toBe("DUP-ID");
    expect(call.update).toHaveLength(1);
    expect(call.update[0]!.clOrdId).toBe("DUP-ID");
  });

  it("applyTransactionAsync_pendingOverflow_warnsOnceAndDropsRest", () => {
    // Override the mock so onGridReady is never called (apiRef stays null).
    _onGridReady = undefined;

    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);

    render(<OrderBlotter />);

    act(() => {
      // Emit 10_001 orders — exceeds PENDING_CAP of 10_000.
      for (let i = 0; i < 10_001; i++) {
        _messagesSubject.next(makeOrder(`OVERFLOW-${String(i)}`));
      }
    });

    // Exactly one warning emitted for the entire overflow run.
    expect(warnSpy).toHaveBeenCalledTimes(1);
    expect(warnSpy.mock.calls[0]![0]).toMatch(/pending overflow/i);

    warnSpy.mockRestore();
  });

  it("columnDefs_valueGetter_neverReturnsNumber", () => {
    const sampleOrder: OrderUpdate = makeOrder("TEST");
    for (const colDef of __TEST_COLUMN_DEFS) {
      if (typeof colDef.valueGetter === "function") {
        const getter = colDef.valueGetter as (p: { data: OrderUpdate }) => unknown;
        const result: unknown = getter({ data: sampleOrder });
        const name = colDef.headerName ?? "";
        expect(typeof result, `${name} valueGetter returned number`).not.toBe("number");
      }
    }
  });

  it("columnDefs_valueFormatter_bigintInput_returnsString", () => {
    for (const colDef of __TEST_COLUMN_DEFS) {
      if (typeof colDef.valueFormatter === "function") {
        // Double-cast through unknown to bridge the incompatible ColDef param type.
        const formatter = colDef.valueFormatter as unknown as (p: {
          value: unknown;
          data: unknown;
        }) => unknown;
        const result: unknown = formatter({ value: 108_500_000n, data: makeOrder("TEST") });
        const name = colDef.headerName ?? "";
        expect(typeof result, `${name} valueFormatter did not return string`).toBe("string");
      }
    }
  });

  it("qtyFormatter_zeroValue_rendersZeroNotDash", () => {
    // qty=0n is LeavesQty=0 after fill — meaningful, must NOT render as dash.
    const qtyColDef = __TEST_COLUMN_DEFS.find((c) => c.headerName === "Qty");
    expect(qtyColDef).toBeDefined();
    if (typeof qtyColDef!.valueFormatter === "function") {
      const formatter = qtyColDef!.valueFormatter as (p: {
        value: unknown;
        data: unknown;
      }) => unknown;
      const result: unknown = formatter({ value: 0n, data: makeOrder("TEST") });
      expect(result).toBe("0.00000000");
    }
  });

  it("priceFormatter_zeroValue_rendersDash", () => {
    // price=0n is uninitialised → display as dash.
    const priceColDef = __TEST_COLUMN_DEFS.find((c) => c.headerName === "Price");
    expect(priceColDef).toBeDefined();
    if (typeof priceColDef!.valueFormatter === "function") {
      const formatter = priceColDef!.valueFormatter as (p: {
        value: unknown;
        data: unknown;
      }) => unknown;
      const result: unknown = formatter({ value: 0n, data: makeOrder("TEST") });
      expect(result).toBe("—");
    }
  });
});
