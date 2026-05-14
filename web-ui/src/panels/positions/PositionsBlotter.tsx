/**
 * PositionsBlotter — AG Grid v35 streaming blotter for the Positions panel.
 *
 * Subscribes to `messages$.pipe(positionStream())` (locally aggregates
 * `FillUpdate`s into per-symbol net positions). Same delta-diff projection
 * shape as PriceBlotter — `lastSeen` lives inside the `useEffect` body so
 * a remount triggers a one-shot full resync.
 *
 * Server-side `PositionProjection` (APP-25, Done) is the authoritative
 * source on reconnect; APP-37 ships local aggregation only. APP-38 covers
 * the real-WebSocket E2E with server resync.
 *
 * Threading: main thread.
 * Allocation: per-emission `update` array (bounded by symbol cardinality).
 *
 * Dependencies:
 *   - `@/main-thread/messageSource` — upstream `messages$`.
 *   - `@/streams/position-stream` — peer operator (filter+scan+throttle).
 *   - `@/streams/agGridResolvers` — `getPositionRowId`.
 *   - `@/shared/transport/format/toFixed8` — sanctioned bigint→display.
 *
 * @see PriceBlotter — peer; same delta-diff shape.
 * @see OrderBlotter — peer.
 *
 * Plan reference: APP-37 §PositionsBlotter.
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
import { type NetPosition } from "@/shared/transport/MessageShape";
import { nanosToDate, toFixed8 } from "@/shared/transport/format/toFixed8";
import { themeQuartzDark } from "@/shared/grid/agGridTheme";
import { getPositionRowId } from "@/streams/agGridResolvers";
import { positionStream } from "@/streams/position-stream";

const PENDING_CAP = 10_000;

const COLUMN_DEFS: readonly ColDef<NetPosition>[] = [
  { headerName: "Symbol", field: "symbol", pinned: "left", width: 110 },
  {
    headerName: "Net Qty",
    valueGetter: (p: ValueGetterParams<NetPosition>) => p.data?.netQty,
    // FIX: netQty=0n is meaningful (flat position) — render as 0, not dash.
    valueFormatter: (p: ValueFormatterParams<NetPosition>) =>
      p.value != null ? toFixed8(p.value as bigint) : "",
    cellClassRules: {
      "pos-long": (p) => typeof p.value === "bigint" && p.value > 0n,
      "pos-short": (p) => typeof p.value === "bigint" && p.value < 0n,
    },
    enableCellChangeFlash: true,
    width: 160,
  },
  {
    headerName: "Avg Px",
    valueGetter: (p: ValueGetterParams<NetPosition>) => p.data?.avgPx,
    // Price-like — 0n is "flat / no avg" → dash.
    valueFormatter: (p: ValueFormatterParams<NetPosition>) =>
      p.value != null && p.value !== 0n ? toFixed8(p.value as bigint) : "—",
    width: 140,
  },
  {
    headerName: "Last Fill",
    valueGetter: (p: ValueGetterParams<NetPosition>) => p.data?.lastFillNanos,
    valueFormatter: (p: ValueFormatterParams<NetPosition>) =>
      p.value != null && p.value !== 0n
        ? nanosToDate(p.value as bigint)
            .toISOString()
            .slice(11, 23)
        : "—",
    width: 130,
  },
];

export function PositionsBlotter(): JSX.Element {
  const apiRef = useRef<GridApi<NetPosition> | null>(null);
  const onGridReadyHandlerRef = useRef<((event: GridReadyEvent<NetPosition>) => void) | null>(null);

  useEffect(() => {
    const lastSeen = new Map<string, NetPosition>();
    // Set of symbols already inserted — required to partition each diff into
    // AG Grid's `add` (new rows) vs `update` (existing rows). Without this,
    // AG Grid silently drops first-emission rows for any symbol.
    const insertedIds = new Set<string>();
    const pending: NetPosition[] = [];
    let warnedOverflow = false;

    function partitionAndApply(api: GridApi<NetPosition>, batch: readonly NetPosition[]): void {
      const add: NetPosition[] = [];
      const update: NetPosition[] = [];
      for (const p of batch) {
        if (insertedIds.has(p.symbol)) {
          update.push(p);
        } else {
          add.push(p);
          insertedIds.add(p.symbol);
        }
      }
      if (add.length === 0 && update.length === 0) return;
      api.applyTransactionAsync({ add, update });
    }

    function flushPending(): void {
      const api = apiRef.current;
      if (api === null || pending.length === 0) return;
      partitionAndApply(api, pending);
      pending.length = 0;
      warnedOverflow = false;
    }

    const sub = messages$.pipe(positionStream(false)).subscribe((map) => {
      const changed: NetPosition[] = [];
      for (const [symbol, p] of map) {
        if (lastSeen.get(symbol) !== p) {
          changed.push(p);
          lastSeen.set(symbol, p);
        }
      }
      if (changed.length === 0) return;

      const api = apiRef.current;
      if (api !== null) {
        partitionAndApply(api, changed);
        return;
      }

      for (const u of changed) {
        if (pending.length < PENDING_CAP) {
          pending.push(u);
        } else if (!warnedOverflow) {
          console.warn(
            "PositionsBlotter: pending overflow (cap=" +
              String(PENDING_CAP) +
              "); subsequent drops silenced until flush",
          );
          warnedOverflow = true;
        }
      }
    });

    onGridReadyHandlerRef.current = (event: GridReadyEvent<NetPosition>) => {
      apiRef.current = event.api;
      flushPending();
    };

    return () => {
      sub.unsubscribe();
    };
  }, []);

  // No explicit apiRef cleanup — see OrderBlotter for rationale (rule 11).

  return (
    <div style={{ height: "100%", width: "100%" }}>
      <AgGridReact<NetPosition>
        theme={themeQuartzDark}
        columnDefs={COLUMN_DEFS as ColDef<NetPosition>[]}
        getRowId={(p) => getPositionRowId(p.data)}
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
