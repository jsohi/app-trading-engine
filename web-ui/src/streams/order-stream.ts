/**
 * order-stream — single-coalescer pass-through for AG Grid.
 *
 * **Single-coalescer invariant per APP-36 §5.5**: NO upstream
 * `auditTime` / `bufferTime`. AG Grid's `applyTransactionAsync` is
 * the sole coalescer (its built-in debouncer aligns with its rAF
 * paint cycle); upstream coalescing causes double-debounce latency
 * (~76 ms p50 / 100+ ms p99). Caller calls `applyTransactionAsync`
 * with the array directly.
 *
 * Threading: main thread.
 *
 * Allocation: zero per emission (the array is constructed by the
 * upstream batch decoder and is consumed by AG Grid synchronously).
 *
 * Plan reference: §5.5 / §6 rows 21, 22, 33.
 */

import { type Observable, type OperatorFunction, filter, map } from "rxjs";

import { type OrderUpdate, type WorkerMessage } from "@/shared/transport/MessageShape";

/**
 * Filter a `WorkerMessage` stream to OrderUpdates only and pass them
 * through verbatim. AG Grid's `applyTransactionAsync` handles batching.
 */
export function orderStream(): OperatorFunction<WorkerMessage, OrderUpdate> {
  return (source: Observable<WorkerMessage>) =>
    source.pipe(
      filter((m): m is OrderUpdate => m.type === "order"),
      map((m) => m),
    );
}
