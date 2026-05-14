/**
 * useGridStreamSink — shared React hook for AG Grid streaming blotters.
 *
 * Owns the cross-blotter scaffolding: `apiRef` capture via `onGridReady`,
 * the pre-`onGridReady` pending buffer with one-shot overflow warn, the
 * `insertedIds` set, and the `add`-vs-`update` partition that AG Grid v33+
 * requires (`update` matches by getRowId; rows not yet in the grid are
 * silently dropped).
 *
 * Consumers:
 *   - `OrderBlotter` (plus its own dev-row-cap via `applyDirect`)
 *   - `PositionsBlotter`
 *   - `PriceBlotter`
 *   - Future APP-42 (events) / APP-40 (RFQ) blotters
 *
 * Rationale: extracts ~80 LOC of identical scaffolding that was previously
 * triplicated across the three blotters (and would compound with each new
 * panel). Centralising it means the AG Grid `add`/`update` partition rule
 * lives in ONE place — preventing future blotters from regressing on the
 * BLOCKER caught by /review Agent B on the original APP-37 commit.
 *
 * Threading: main thread (React).
 * Allocation: per-emission `add`/`update` arrays; transient `pending`
 * buffer (drained on `onGridReady`); single `Set<string>` of inserted ids
 * mutated in place. Zero closure allocation per emission — the returned
 * callbacks are stable (useCallback with empty deps).
 *
 * @see OrderBlotter — primary consumer + dev-row-cap escape hatch.
 * @see PositionsBlotter / PriceBlotter — peer consumers.
 *
 * Plan reference: APP-37 §Files to modify (post-/review extraction).
 */
import { useCallback, useRef } from "react";
import { type GridApi, type GridReadyEvent } from "ag-grid-community";

const DEFAULT_PENDING_CAP = 10_000;

export interface UseGridStreamSinkOptions<TRow> {
  /** Human-readable panel name; used in the overflow `console.warn`. */
  readonly panelName: string;
  /** Stable id resolver per row — same shape AG Grid uses for getRowId. */
  readonly getRowId: (row: TRow) => string;
  /** Optional override; default = 10_000. */
  readonly pendingCap?: number;
}

export interface UseGridStreamSinkResult<TRow> {
  /** Mutable ref to the AG Grid api. `null` until `onGridReady` fires. */
  readonly apiRef: React.RefObject<GridApi<TRow> | null>;
  /** Pass to `<AgGridReact onGridReady={...}/>`. */
  readonly onGridReady: (event: GridReadyEvent<TRow>) => void;
  /**
   * Push a batch of rows. Auto-partitions into `add` (first-time-seen) vs
   * `update` (existing); auto-buffers if the api is not yet ready.
   * Zero-arg: a no-op if the batch is empty.
   */
  readonly applyBatch: (batch: readonly TRow[]) => void;
  /**
   * Escape hatch for paired-transaction needs (e.g. OrderBlotter's dev row
   * cap: evict-oldest + insert-new in one call). The caller is responsible
   * for keeping `insertedIds` in sync via `markInserted` / `markRemoved`.
   * Returns `false` if the api is not yet ready (caller should buffer).
   */
  readonly applyDirect: (tx: {
    readonly add?: TRow[];
    readonly update?: TRow[];
    readonly remove?: TRow[];
  }) => boolean;
  /** Mark an id as inserted (after a successful `applyDirect({add})`). */
  readonly markInserted: (id: string) => void;
  /** Mark an id as removed (after a successful `applyDirect({remove})`). */
  readonly markRemoved: (id: string) => void;
  /** Read-only check used by callers (e.g. OrderBlotter's dev-row-cap). */
  readonly hasInserted: (id: string) => boolean;
}

export function useGridStreamSink<TRow>(
  options: UseGridStreamSinkOptions<TRow>,
): UseGridStreamSinkResult<TRow> {
  const { panelName, getRowId, pendingCap = DEFAULT_PENDING_CAP } = options;

  const apiRef = useRef<GridApi<TRow> | null>(null);
  // Per-mount state in refs — survives across renders without re-init.
  const insertedIdsRef = useRef<Set<string>>(new Set<string>());
  const pendingRef = useRef<TRow[]>([]);
  const warnedOverflowRef = useRef<boolean>(false);

  const partitionAndApply = useCallback(
    (api: GridApi<TRow>, batch: readonly TRow[]): void => {
      const inserted = insertedIdsRef.current;
      const add: TRow[] = [];
      const update: TRow[] = [];
      for (const row of batch) {
        const id = getRowId(row);
        if (inserted.has(id)) {
          update.push(row);
        } else {
          add.push(row);
          inserted.add(id);
        }
      }
      if (add.length === 0 && update.length === 0) return;
      api.applyTransactionAsync({ add, update });
    },
    [getRowId],
  );

  const flushPending = useCallback((): void => {
    const api = apiRef.current;
    const pending = pendingRef.current;
    if (api === null || pending.length === 0) return;
    partitionAndApply(api, pending);
    pending.length = 0;
    warnedOverflowRef.current = false;
  }, [partitionAndApply]);

  const onGridReady = useCallback(
    (event: GridReadyEvent<TRow>): void => {
      apiRef.current = event.api;
      flushPending();
    },
    [flushPending],
  );

  const applyBatch = useCallback(
    (batch: readonly TRow[]): void => {
      if (batch.length === 0) return;
      const api = apiRef.current;
      if (api !== null) {
        partitionAndApply(api, batch);
        return;
      }
      // Buffer until onGridReady fires. Per-row admission with one-shot warn.
      const pending = pendingRef.current;
      for (const row of batch) {
        if (pending.length < pendingCap) {
          pending.push(row);
        } else if (!warnedOverflowRef.current) {
          console.warn(
            panelName +
              ": pending overflow (cap=" +
              String(pendingCap) +
              "); subsequent drops silenced until flush",
          );
          warnedOverflowRef.current = true;
        }
      }
    },
    [panelName, pendingCap, partitionAndApply],
  );

  const applyDirect = useCallback(
    (tx: {
      readonly add?: TRow[];
      readonly update?: TRow[];
      readonly remove?: TRow[];
    }): boolean => {
      const api = apiRef.current;
      if (api === null) return false;
      api.applyTransactionAsync(tx);
      return true;
    },
    [],
  );

  const markInserted = useCallback((id: string): void => {
    insertedIdsRef.current.add(id);
  }, []);

  const markRemoved = useCallback((id: string): void => {
    insertedIdsRef.current.delete(id);
  }, []);

  const hasInserted = useCallback((id: string): boolean => {
    return insertedIdsRef.current.has(id);
  }, []);

  return {
    apiRef,
    onGridReady,
    applyBatch,
    applyDirect,
    markInserted,
    markRemoved,
    hasInserted,
  };
}
