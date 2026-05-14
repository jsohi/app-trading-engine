/**
 * OrderBlotter — AG Grid v35 streaming blotter for the Orders panel.
 *
 * Subscribes to `messages$.pipe(orderStream())` and pushes every emission
 * into the grid via `applyTransactionAsync`. Buffers updates between mount
 * and `onGridReady` so no emission is lost (capped at PENDING_CAP with a
 * one-shot warn latch).
 *
 * Rationale: matches the AG Grid v35 idiomatic shape — `onGridReady`
 * callback for api capture (NOT a ref nulled in effect cleanup, which
 * would break under React 19 StrictMode where the effect re-runs without
 * the grid actually remounting), `getRowId` for transaction matching,
 * `enableCellChangeFlash` per-column for price flash.
 *
 * Dev-mode row cap: `fakeStream` mints orders with monotonically-increasing
 * `clOrdId`, so every dev tick is a fresh row → unbounded growth at
 * ~360 rows/min. Cap at DEV_ROW_CAP via FIFO; emit paired remove+update.
 * Gated on `import.meta.env.DEV` — production (clOrdIds repeat per
 * amend/cancel) is unaffected.
 *
 * Threading: main thread (React 19 concurrent mode).
 * Allocation: per-emission `update` array of length 1; transient
 * `pending` array buffer (drained on onGridReady); FIFO `clOrdId[]`
 * (DEV only).
 *
 * Dependencies:
 *   - `@/main-thread/messageSource` — upstream `messages$` Observable.
 *   - `@/streams/order-stream` — peer operator (filters to OrderUpdate).
 *   - `@/streams/agGridResolvers` — `getOrderRowId`.
 *   - `@/shared/transport/format/toFixed8` — sanctioned bigint→display.
 *   - `@/shared/grid/agGridTheme` — peer theme.
 *
 * @see PriceBlotter — peer, same shape minus dev row cap + with delta-diff.
 * @see PositionsBlotter — peer, same shape minus dev row cap.
 *
 * Plan reference: APP-37 §OrderBlotter / §Implementation notes (Blotters shared pattern).
 */
import { type JSX, useEffect, useRef } from "react";
import { AgGridReact } from "ag-grid-react";
import {
  type ColDef,
  type GridApi,
  type GridReadyEvent,
  type ValueFormatterParams,
  type ValueGetterParams,
} from "ag-grid-community";

import { messages$ } from "@/main-thread/messageSource";
import { type OrderUpdate } from "@/shared/transport/MessageShape";
import { nanosToDate, toFixed8 } from "@/shared/transport/format/toFixed8";
import { themeQuartzDark } from "@/shared/grid/agGridTheme";
import { getOrderRowId } from "@/streams/agGridResolvers";
import { orderStream } from "@/streams/order-stream";

const PENDING_CAP = 10_000;
const DEV_ROW_CAP = 500;

const STATUS_CELL_CLASS_RULES: Record<string, (p: { value: unknown }) => boolean> = {
  "status-OPEN": (p) => p.value === "OPEN",
  "status-PARTIAL": (p) => p.value === "PARTIAL",
  "status-FILLED": (p) => p.value === "FILLED",
  "status-CANCELLED": (p) => p.value === "CANCELLED",
  "status-REJECTED": (p) => p.value === "REJECTED",
};

const COLUMN_DEFS: readonly ColDef<OrderUpdate>[] = [
  { headerName: "ClOrdId", field: "clOrdId", pinned: "left", width: 160 },
  { headerName: "Symbol", field: "symbol", width: 110 },
  {
    headerName: "Side",
    field: "side",
    width: 80,
    cellClassRules: {
      "side-buy": (p) => p.value === "BUY",
      "side-sell": (p) => p.value === "SELL",
    },
  },
  {
    headerName: "Status",
    field: "status",
    width: 110,
    cellClassRules: STATUS_CELL_CLASS_RULES,
  },
  {
    headerName: "Price",
    valueGetter: (p: ValueGetterParams<OrderUpdate>) => p.data?.price,
    valueFormatter: (p: ValueFormatterParams<OrderUpdate>) =>
      // Price-like — 0n is "uninitialised" → render as dash.
      p.value != null && p.value !== 0n ? toFixed8(p.value as bigint) : "—",
    enableCellChangeFlash: true,
    width: 140,
  },
  {
    headerName: "Qty",
    valueGetter: (p: ValueGetterParams<OrderUpdate>) => p.data?.qty,
    // FIX: qty=0n is meaningful (LeavesQty post-fill); render as 0, not dash.
    valueFormatter: (p: ValueFormatterParams<OrderUpdate>) =>
      p.value != null ? toFixed8(p.value as bigint) : "",
    width: 140,
  },
  {
    headerName: "Updated",
    valueGetter: (p: ValueGetterParams<OrderUpdate>) => p.data?.serverNanos,
    valueFormatter: (p: ValueFormatterParams<OrderUpdate>) =>
      p.value != null && p.value !== 0n
        ? nanosToDate(p.value as bigint)
            .toISOString()
            .slice(11, 23)
        : "—",
    width: 130,
  },
];

export function OrderBlotter(): JSX.Element {
  const apiRef = useRef<GridApi<OrderUpdate> | null>(null);
  // Component-scoped ref to the onGridReady callback, populated by the
  // subscription effect (which holds `flushPending` in its closure). The
  // grid invokes `onGridReady` exactly once per mount.
  const onGridReadyHandlerRef = useRef<((event: GridReadyEvent<OrderUpdate>) => void) | null>(null);

  useEffect(() => {
    // Per-mount state so a StrictMode remount triggers a clean buffer.
    const pending: OrderUpdate[] = [];
    let warnedOverflow = false;
    // Dev-only FIFO of clOrdIds for the row cap — gated to avoid prod overhead.
    const devFifo: string[] = [];
    const isDev: boolean = import.meta.env.DEV;

    function flushPending(): void {
      const api = apiRef.current;
      if (api === null || pending.length === 0) return;
      api.applyTransactionAsync({ update: [...pending] });
      if (isDev) {
        for (const o of pending) devFifo.push(o.clOrdId);
      }
      pending.length = 0;
      warnedOverflow = false;
    }

    const sub = messages$.pipe(orderStream()).subscribe((order) => {
      const api = apiRef.current;
      if (api === null) {
        if (pending.length < PENDING_CAP) {
          pending.push(order);
        } else if (!warnedOverflow) {
          console.warn(
            "OrderBlotter: pending overflow (cap=" +
              String(PENDING_CAP) +
              "); subsequent drops silenced until flush",
          );
          warnedOverflow = true;
        }
        return;
      }

      // Dev row cap: when FIFO is full, evict oldest and pair with the insert.
      if (isDev && devFifo.length >= DEV_ROW_CAP) {
        const oldest = devFifo.shift();
        if (oldest !== undefined) {
          api.applyTransactionAsync({
            remove: [{ clOrdId: oldest } as OrderUpdate],
            update: [order],
          });
          devFifo.push(order.clOrdId);
          return;
        }
      }

      api.applyTransactionAsync({ update: [order] });
      if (isDev) devFifo.push(order.clOrdId);
    });

    // Bind flushPending so onGridReady can reach it via the ref-callback.
    onGridReadyHandlerRef.current = (event: GridReadyEvent<OrderUpdate>) => {
      apiRef.current = event.api;
      flushPending();
    };

    return () => {
      sub.unsubscribe();
    };
  }, []);

  // Component-unmount-only cleanup: clear apiRef ONLY on real unmount, NOT
  // on StrictMode-induced effect re-runs. Empty deps + cleanup-only fn.
  useEffect(
    () => () => {
      apiRef.current = null;
    },
    [],
  );

  return (
    <div style={{ height: "100%", width: "100%" }}>
      <AgGridReact<OrderUpdate>
        theme={themeQuartzDark}
        columnDefs={COLUMN_DEFS as ColDef<OrderUpdate>[]}
        getRowId={(p) => getOrderRowId(p.data)}
        asyncTransactionWaitMillis={16}
        onGridReady={(event) => {
          onGridReadyHandlerRef.current?.(event);
        }}
        rowBuffer={20}
      />
    </div>
  );
}

/** @internal — exported for the column-def regression test. */
export const __TEST_COLUMN_DEFS = COLUMN_DEFS;
