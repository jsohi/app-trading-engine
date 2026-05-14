/**
 * PriceBlotter — AG Grid v35 streaming blotter for the Quotes panel.
 *
 * Subscribes to `messages$.pipe(filterToPrice, priceStream(false))`. The
 * upstream `priceStream` mutates an in-place Map per emission; this
 * component projects that Map down to the changed entries via reference
 * equality (a *projection*, NOT a coalescer — `priceStream`'s rAF-aligned
 * `throttleTime` remains the sole coalescer; the single-coalescer
 * invariant per `order-stream.ts` is preserved).
 *
 * `lastSeen` is declared INSIDE the `useEffect` body so a remount triggers
 * a fresh-empty Map → first emission produces a one-time full resync. This
 * is documented expected behaviour, NOT a flash storm regression.
 *
 * Dev-mode flash density: `fakeStream.makePrice` allocates a fresh
 * `PriceUpdate` object on every tick → reference inequality always → every
 * dev tick flashes every emitted symbol. By design.
 *
 * Threading: main thread.
 * Allocation: per-emission `update` array (bounded by symbol cardinality).
 *
 * Dependencies:
 *   - `@/main-thread/messageSource` — upstream `messages$`.
 *   - `@/streams/price-stream` — peer operator.
 *   - `@/streams/agGridResolvers` — `getPriceRowId`.
 *   - `@/shared/transport/format/toFixed8` — sanctioned bigint→display.
 *
 * @see OrderBlotter — peer; same shape minus delta-diff + plus dev row cap.
 * @see PositionsBlotter — peer; same delta-diff shape.
 *
 * Plan reference: APP-37 §PriceBlotter / §Implementation notes.
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
import { filter } from "rxjs";

import { messages$ } from "@/main-thread/messageSource";
import { type PriceUpdate, type WorkerMessage } from "@/shared/transport/MessageShape";
import { nanosToDate, toFixed8 } from "@/shared/transport/format/toFixed8";
import { themeQuartzDark } from "@/shared/grid/agGridTheme";
import { getPriceRowId } from "@/streams/agGridResolvers";
import { priceStream } from "@/streams/price-stream";

const PENDING_CAP = 10_000;

// Row data is structurally identical to PriceUpdate — spread is computed
// in valueGetter, not stored. Type alias (not empty-extends interface) to
// satisfy @typescript-eslint/no-empty-object-type.
type PriceRowData = PriceUpdate;

const COLUMN_DEFS: readonly ColDef<PriceRowData>[] = [
  { headerName: "Symbol", field: "symbol", pinned: "left", width: 110 },
  {
    headerName: "Bid",
    valueGetter: (p: ValueGetterParams<PriceRowData>) => p.data?.bid,
    valueFormatter: (p: ValueFormatterParams<PriceRowData>) =>
      p.value != null && p.value !== 0n ? toFixed8(p.value as bigint) : "—",
    enableCellChangeFlash: true,
    width: 140,
  },
  {
    headerName: "Ask",
    valueGetter: (p: ValueGetterParams<PriceRowData>) => p.data?.ask,
    valueFormatter: (p: ValueFormatterParams<PriceRowData>) =>
      p.value != null && p.value !== 0n ? toFixed8(p.value as bigint) : "—",
    enableCellChangeFlash: true,
    width: 140,
  },
  {
    headerName: "Spread",
    valueGetter: (p: ValueGetterParams<PriceRowData>) => {
      const d = p.data;
      if (!d || d.bid === 0n || d.ask === 0n) return null;
      return d.ask - d.bid;
    },
    valueFormatter: (p: ValueFormatterParams<PriceRowData>) =>
      p.value != null ? toFixed8(p.value as bigint) : "—",
    width: 120,
  },
  {
    headerName: "Updated",
    valueGetter: (p: ValueGetterParams<PriceRowData>) => p.data?.serverNanos,
    valueFormatter: (p: ValueFormatterParams<PriceRowData>) =>
      p.value != null && p.value !== 0n
        ? nanosToDate(p.value as bigint)
            .toISOString()
            .slice(11, 23)
        : "—",
    width: 130,
  },
];

export function PriceBlotter(): JSX.Element {
  const apiRef = useRef<GridApi<PriceRowData> | null>(null);
  const onGridReadyHandlerRef = useRef<((event: GridReadyEvent<PriceRowData>) => void) | null>(
    null,
  );

  useEffect(() => {
    // Per-mount state — fresh Map per remount triggers the documented
    // one-shot full resync invariant.
    const lastSeen = new Map<string, PriceUpdate>();
    const pending: PriceUpdate[] = [];
    let warnedOverflow = false;

    function flushPending(): void {
      const api = apiRef.current;
      if (api === null || pending.length === 0) return;
      api.applyTransactionAsync({ update: [...pending] });
      pending.length = 0;
      warnedOverflow = false;
    }

    const sub = messages$
      .pipe(
        filter((m: WorkerMessage): m is PriceUpdate => m.type === "price"),
        priceStream(false),
      )
      .subscribe((map) => {
        // Delta-diff projection: select only entries whose value reference
        // changed. priceStream's mutated Map identity is stable; the diff
        // is on the inner PriceUpdate object reference per symbol.
        const update: PriceUpdate[] = [];
        for (const [symbol, p] of map) {
          if (lastSeen.get(symbol) !== p) {
            update.push(p);
            lastSeen.set(symbol, p);
          }
        }
        if (update.length === 0) return;

        const api = apiRef.current;
        if (api !== null) {
          api.applyTransactionAsync({ update });
          return;
        }

        // Buffer until onGridReady fires. Per-message admission so the
        // shape matches OrderBlotter's overflow semantics.
        for (const u of update) {
          if (pending.length < PENDING_CAP) {
            pending.push(u);
          } else if (!warnedOverflow) {
            console.warn(
              "PriceBlotter: pending overflow (cap=" +
                String(PENDING_CAP) +
                "); subsequent drops silenced until flush",
            );
            warnedOverflow = true;
          }
        }
      });

    onGridReadyHandlerRef.current = (event: GridReadyEvent<PriceRowData>) => {
      apiRef.current = event.api;
      flushPending();
    };

    return () => {
      sub.unsubscribe();
    };
  }, []);

  useEffect(
    () => () => {
      apiRef.current = null;
    },
    [],
  );

  return (
    <div style={{ height: "100%", width: "100%" }}>
      <AgGridReact<PriceRowData>
        theme={themeQuartzDark}
        columnDefs={COLUMN_DEFS as ColDef<PriceRowData>[]}
        getRowId={(p) => getPriceRowId(p.data)}
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
