/**
 * Streams API surface — sealed public types for APP-37 / APP-40 / APP-42.
 *
 * **All `src/streams/**` operators run on the main thread**
 * (`workerClient` re-fans `MESSAGE_BATCH` synchronously into RxJS
 * subjects). `animationFrameScheduler` therefore drives off real
 * `requestAnimationFrame`. A future refactor that hoists a stream
 * into the worker silently breaks scheduling — this header comment
 * is the canonical warning.
 *
 * Threading: main thread.
 *
 * Allocation: per-emission boundaries documented per stream.
 *
 * Plan reference: §5.5 / §5.7 / §6 row 21.
 */

import { type Observable } from "rxjs";

import {
  type ConnectionState,
  type FillUpdate,
  type OrderUpdate,
  type PriceUpdate,
} from "@/shared/transport/MessageShape";

/** Versioned barrel — bumped on any non-additive change to stream contracts. */
export const STREAM_API_VERSION = 1;

/**
 * Aggregate of price updates per symbol. The Map is mutated in place
 * across emissions (zero-alloc). Downstream consumers MUST treat the
 * value as immutable for the duration of one tick — read-only snapshot
 * semantics. RxJS pattern; documented per §5.5.
 */
export type PriceStream = Observable<ReadonlyMap<string, PriceUpdate>>;

/** Per-tick array of order updates for AG Grid `applyTransactionAsync`. */
export type OrderStream = Observable<readonly OrderUpdate[]>;

/** Per-tick array of fill updates. */
export type FillStream = Observable<readonly FillUpdate[]>;

/**
 * Event-log virtualized-list snapshot tuple. The ring-buffer storage
 * is mutated in place; consumers read via the snapshot reference and
 * compare the version counter for change detection. `version` is a
 * `number` (monotonic per session; reset on reconnect; will not wrap
 * within session — at 5 k frames/s × 24 h ≈ 4×10⁸ < 2⁵³).
 */
export interface EventLogSnapshot {
  /** Read-only view of the ring contents (oldest → newest within capacity). */
  readonly entries: readonly { readonly templateId: number; readonly bytes: Uint8Array }[];
  /** Monotonic per-session counter for cheap change detection. */
  readonly version: number;
}

export type EventLogStream = Observable<EventLogSnapshot>;

/** Connection-state stream (BehaviorSubject under the hood). */
export type ConnectionStream = Observable<ConnectionState>;
