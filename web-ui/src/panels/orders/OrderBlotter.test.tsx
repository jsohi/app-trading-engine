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

    // Flush on onGridReady — one call with all 3 buffered orders.
    expect(_fakeApi.applyTransactionAsync).toHaveBeenCalledTimes(1);
    const call = _fakeApi.applyTransactionAsync.mock.calls[0]![0] as {
      update: OrderUpdate[];
    };
    expect(call.update).toHaveLength(3);
    expect(call.update.map((o: OrderUpdate) => o.clOrdId)).toEqual([
      "ORD-001",
      "ORD-002",
      "ORD-003",
    ]);
  });

  it("applyTransactionAsync_afterOnGridReady_singleUpdate", () => {
    render(<OrderBlotter />);

    act(() => {
      fireFakeOnGridReady();
    });

    act(() => {
      _messagesSubject.next(makeOrder("ORD-A"));
    });
    act(() => {
      _messagesSubject.next(makeOrder("ORD-B"));
    });

    // Each emission produces its own applyTransactionAsync call.
    expect(_fakeApi.applyTransactionAsync).toHaveBeenCalledTimes(2);
    const firstCall = _fakeApi.applyTransactionAsync.mock.calls[0]![0] as {
      update: OrderUpdate[];
    };
    expect(firstCall.update[0]!.clOrdId).toBe("ORD-A");
    const secondCall = _fakeApi.applyTransactionAsync.mock.calls[1]![0] as {
      update: OrderUpdate[];
    };
    expect(secondCall.update[0]!.clOrdId).toBe("ORD-B");
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
