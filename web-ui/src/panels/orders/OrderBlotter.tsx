/**
 * OrderBlotter — AG Grid v35 streaming blotter for the Orders panel.
 *
 * Subscribes to `messages$.pipe(orderStream())` and pushes every emission
 * into the grid via the shared `useGridStreamSink` hook (handles the
 * `add`-vs-`update` partition + buffering until `onGridReady`).
 *
 * Dev-mode row cap: `fakeStream` mints orders with monotonically-increasing
 * `clOrdId`, so every dev tick is a fresh row → unbounded growth at
 * ~360 rows/min. Cap at DEV_ROW_CAP via FIFO; when full, evict oldest +
 * insert new in a paired `applyDirect({ remove, add })` transaction.
 * Gated on `import.meta.env.DEV` — production (clOrdIds repeat per
 * amend/cancel) is unaffected.
 *
 * Threading: main thread.
 * Allocation: per-emission single-element batch handed to `sink.applyBatch`;
 * dev-mode FIFO `clOrdId[]` (DEV only).
 *
 * Dependencies:
 *   - `@/main-thread/messageSource` — upstream `messages$` Observable.
 *   - `@/streams/order-stream` — peer operator (filters to OrderUpdate).
 *   - `@/streams/agGridResolvers` — `getOrderRowId`.
 *   - `@/shared/grid/useGridStreamSink` — peer: shared blotter scaffolding.
 *   - `@/shared/transport/format/toFixed8` — sanctioned bigint→display.
 *
 * @see PriceBlotter — peer, same shape minus dev row cap + with delta-diff.
 * @see PositionsBlotter — peer, same shape minus dev row cap.
 *
 * Plan reference: APP-37 §OrderBlotter / §Implementation notes (Blotters shared pattern).
 */
import { type JSX, useEffect, useRef } from "react";
import { AgGridReact } from "ag-grid-react";
import { type ColDef, type ValueFormatterParams, type ValueGetterParams } from "ag-grid-community";

import { messages$ } from "@/main-thread/messageSource";
import { type OrderUpdate } from "@/shared/transport/MessageShape";
import { nanosToDate, toFixed8 } from "@/shared/transport/format/toFixed8";
import { themeQuartzDark } from "@/shared/grid/agGridTheme";
import { useGridStreamSink } from "@/shared/grid/useGridStreamSink";
import { getOrderRowId } from "@/streams/agGridResolvers";
import { orderStream } from "@/streams/order-stream";

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

const getRowId = (row: OrderUpdate): string => getOrderRowId(row);

export function OrderBlotter(): JSX.Element {
  const sink = useGridStreamSink<OrderUpdate>({
    panelName: "OrderBlotter",
    getRowId,
  });
  // Dev-only FIFO of clOrdIds. Gated by import.meta.env.DEV so prod is
  // unaffected (prod clOrdIds repeat via amend/cancel — natural cap).
  const devFifoRef = useRef<string[]>([]);
  const isDev: boolean = import.meta.env.DEV;

  useEffect(() => {
    const sub = messages$.pipe(orderStream()).subscribe((order) => {
      // Dev-row-cap fast path: evict-oldest + insert-new as a paired
      // transaction. Bypasses the shared `applyBatch` to keep the remove
      // and add in a single AG Grid call (no flicker).
      if (
        isDev &&
        sink.apiRef.current !== null &&
        !sink.hasInserted(order.clOrdId) &&
        devFifoRef.current.length >= DEV_ROW_CAP
      ) {
        const fifo = devFifoRef.current;
        const oldest = fifo.shift();
        if (oldest !== undefined) {
          // AG Grid remove[] matches by getRowId; only clOrdId is read.
          const removeStub: Pick<OrderUpdate, "clOrdId"> = { clOrdId: oldest };
          const applied = sink.applyDirect({
            remove: [removeStub as OrderUpdate],
            add: [order],
          });
          if (applied) {
            sink.markRemoved(oldest);
            sink.markInserted(order.clOrdId);
            fifo.push(order.clOrdId);
            return;
          }
        }
      }

      sink.applyBatch([order]);
      if (isDev && sink.hasInserted(order.clOrdId)) {
        // Track in FIFO only on first insert; subsequent updates to the
        // same clOrdId don't re-enter the FIFO. `hasInserted` is true now
        // because applyBatch just added it.
        const fifo = devFifoRef.current;
        if (!fifo.includes(order.clOrdId)) {
          fifo.push(order.clOrdId);
        }
      }
    });

    return () => {
      sub.unsubscribe();
    };
  }, [isDev, sink]);

  return (
    <div style={{ height: "100%", width: "100%" }}>
      <AgGridReact<OrderUpdate>
        theme={themeQuartzDark}
        columnDefs={COLUMN_DEFS as ColDef<OrderUpdate>[]}
        getRowId={(p) => getRowId(p.data)}
        asyncTransactionWaitMillis={16}
        onGridReady={sink.onGridReady}
        rowBuffer={20}
      />
    </div>
  );
}

/** @internal — exported for the column-def regression test. */
export const __TEST_COLUMN_DEFS = COLUMN_DEFS;
