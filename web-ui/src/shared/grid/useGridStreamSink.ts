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
 *   - `OrderBlotter` (plus its own dev-row-cap via `applyDirect` + `onInsert`)
 *   - `PositionsBlotter`
 *   - `PriceBlotter`
 *   - Future APP-42 (events) / APP-40 (RFQ) blotters
 *
 * Rationale: extracts ~80 LOC of identical scaffolding that was previously
 * triplicated across the three blotters (and would compound with each new
 * panel). Centralising it means the AG Grid `add`/`update` partition rule
 * lives in ONE place — preventing future blotters from regressing on the
 * BLOCKER caught by /review Agent B in earlier rounds.
 *
 * Insertion notification (`onInsert`): when the hook first inserts a row,
 * it invokes the consumer's `onInsert(id)` callback. This is the load-
 * bearing seam for any consumer that maintains an external row-tracking
 * structure (e.g. OrderBlotter's FIFO for the MAX_ROWS cap). Without this
 * callback, a consumer that only checks `hasInserted()` AFTER calling
 * `applyBatch` misses the buffered-then-flushed path entirely (the hook's
 * own `partitionAndApply` adds to `insertedIds` during flush, with no
 * consumer-visible signal).
 *
 * Capture-then-commit: `partitionAndApply` accumulates the `add`/`update`
 * arrays + the `toMark` id list first, then calls `applyTransactionAsync`,
 * then commits to `insertedIds` and fires `onInsert`. If AG Grid throws
 * synchronously the state stays consistent — `insertedIds` is not poisoned
 * with ids the grid never accepted.
 *
 * Threading: main thread (React).
 * Allocation: per-emission `add`/`update`/`toMark` arrays; transient
 * `pending` buffer (drained on `onGridReady`); single `Set<string>` of
 * inserted ids mutated in place. Returned object identity is stable via
 * `useMemo` so consumers can include it in `useEffect` deps safely.
 *
 * @see OrderBlotter — primary consumer + dev-row-cap escape hatch.
 * @see PositionsBlotter / PriceBlotter — peer consumers.
 *
 * Plan reference: APP-37 §Files to modify (post-/review extraction).
 */
import { useCallback, useMemo, useRef } from "react";
import { type GridApi, type GridReadyEvent } from "ag-grid-community";

const DEFAULT_PENDING_CAP = 10_000;

export interface UseGridStreamSinkOptions<TRow> {
  /** Human-readable panel name; used in the overflow `console.warn`. */
  readonly panelName: string;
  /** Stable id resolver per row — same shape AG Grid uses for getRowId. */
  readonly getRowId: (row: TRow) => string;
  /** Optional override; default = 10_000. */
  readonly pendingCap?: number;
  /**
   * Called by the hook EACH time it inserts a row (`add` path). The id is
   * the just-inserted row's `getRowId(row)` value. Fires for both the
   * direct `applyBatch` path AND the buffered-flush path inside
   * `onGridReady`. Consumers use this to maintain an external row-tracking
   * structure (e.g. OrderBlotter's MAX_ROWS FIFO). Pass a stable callback
   * (wrap in `useCallback` with empty deps).
   */
  readonly onInsert?: (id: string) => void;
}

export interface UseGridStreamSinkResult<TRow> {
  /** Mutable ref to the AG Grid api. `null` until `onGridReady` fires. */
  readonly apiRef: React.RefObject<GridApi<TRow> | null>;
  /** Pass to `<AgGridReact onGridReady={...}/>`. */
  readonly onGridReady: (event: GridReadyEvent<TRow>) => void;
  /**
   * Push a batch of rows. Auto-partitions into `add` (first-time-seen) vs
   * `update` (existing); auto-buffers if the api is not yet ready. No-op
   * if the batch is empty.
   */
  readonly applyBatch: (batch: readonly TRow[]) => void;
  /**
   * Escape hatch for paired-transaction needs (e.g. OrderBlotter's row
   * cap: evict-oldest + insert-new in one call). The caller is responsible
   * for keeping `insertedIds` in sync via `markInserted` / `markRemoved`.
   * THROWS if the api is not yet ready — buffering an arbitrary direct
   * transaction is not well-defined; callers must gate on
   * `apiRef.current !== null` first.
   */
  readonly applyDirect: (tx: {
    readonly add?: TRow[];
    readonly update?: TRow[];
    readonly remove?: readonly Partial<TRow>[];
  }) => void;
  /** Mark an id as inserted (after a successful `applyDirect({add})`). */
  readonly markInserted: (id: string) => void;
  /** Mark an id as removed (after a successful `applyDirect({remove})`). */
  readonly markRemoved: (id: string) => void;
  /** Read-only check used by callers (e.g. OrderBlotter's row-cap). */
  readonly hasInserted: (id: string) => boolean;
}

export function useGridStreamSink<TRow>(
  options: UseGridStreamSinkOptions<TRow>,
): UseGridStreamSinkResult<TRow> {
  const { panelName, getRowId, pendingCap = DEFAULT_PENDING_CAP, onInsert } = options;

  const apiRef = useRef<GridApi<TRow> | null>(null);
  // Per-mount state in refs — survives across renders without re-init.
  const insertedIdsRef = useRef<Set<string>>(new Set<string>());
  const pendingRef = useRef<TRow[]>([]);
  const warnedOverflowRef = useRef<boolean>(false);
  // Keep onInsert in a ref so consumers can pass an unstable callback
  // without invalidating the memoised result object. The hook reads the
  // CURRENT callback on every call site.
  const onInsertRef = useRef<typeof onInsert>(onInsert);
  onInsertRef.current = onInsert;

  const partitionAndApply = useCallback(
    (api: GridApi<TRow>, batch: readonly TRow[]): void => {
      const inserted = insertedIdsRef.current;
      const toAdd: TRow[] = [];
      const toUpdate: TRow[] = [];
      const toMark: string[] = [];
      for (const row of batch) {
        const id = getRowId(row);
        if (inserted.has(id)) {
          toUpdate.push(row);
        } else {
          toAdd.push(row);
          toMark.push(id);
        }
      }
      if (toAdd.length === 0 && toUpdate.length === 0) return;
      // Capture-then-commit: only mutate `inserted` AFTER the AG Grid call
      // succeeds. If `applyTransactionAsync` throws synchronously the set
      // is not poisoned with ids the grid never accepted.
      api.applyTransactionAsync({ add: toAdd, update: toUpdate });
      const notify = onInsertRef.current;
      for (const id of toMark) {
        inserted.add(id);
        notify?.(id);
      }
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
      readonly remove?: readonly Partial<TRow>[];
    }): void => {
      const api = apiRef.current;
      if (api === null) {
        // Documented contract: callers MUST gate on apiRef.current first.
        // Throwing here makes misuse loud — buffering an arbitrary direct
        // transaction (which can include `remove`) is not well-defined.
        throw new Error(panelName + ".applyDirect called before onGridReady (apiRef is null)");
      }
      // AG Grid v33+ `RowDataTransaction` types these fields as
      // `TRow[] | null` (not `| undefined`) under `exactOptionalPropertyTypes`.
      // Map missing fields to `null`. AG Grid accepts `Partial<TRow>` on the
      // `remove` path at runtime (matched by getRowId); the narrow type at
      // the hook boundary documents this.
      api.applyTransactionAsync({
        add: tx.add ?? null,
        update: tx.update ?? null,
        remove: (tx.remove as TRow[] | undefined) ?? null,
      });
    },
    [panelName],
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

  // Memoise the returned object so consumers can pass `sink` in useEffect
  // deps without triggering resubscribe-storms on parent re-renders.
  // Every callback above is `useCallback`-stable, so the memo deps capture
  // the actual stability surface.
  return useMemo(
    () => ({
      apiRef,
      onGridReady,
      applyBatch,
      applyDirect,
      markInserted,
      markRemoved,
      hasInserted,
    }),
    [onGridReady, applyBatch, applyDirect, markInserted, markRemoved, hasInserted],
  );
}
