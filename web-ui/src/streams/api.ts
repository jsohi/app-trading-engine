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

/** A single event-log entry — opaque post-header SBE wire bytes + templateId. */
export interface EventLogEntry {
  readonly templateId: number;
  readonly bytes: Uint8Array;
}

/**
 * Event-log virtualized-list snapshot tuple. The ring-buffer storage
 * is mutated in place; consumers read via the snapshot reference and
 * compare the version counter for change detection. `version` is a
 * `number` (monotonic per session; reset on reconnect; will not wrap
 * within session — at 5 k frames/s × 24 h ≈ 4×10⁸ < 2⁵³).
 *
 * Per Gemini review (HIGH): `read(i)` exposes the ring at logical index
 * `[0, count)` (0 = oldest, count-1 = newest) so virtualized list
 * consumers can fetch only the visible window. Avoids the O(N) rebuild
 * of a copy-array on every event that an `entries: ReadonlyArray<...>`
 * shape would require — critical when capacity is 10 000 and event rate
 * is 5 k/s.
 *
 * Snapshot object identity is **stable** across emissions; only `count`
 * and `version` mutate. Consumers must use `version` for change
 * detection, not reference equality.
 */
export interface EventLogSnapshot {
  /** Number of valid entries currently in the ring (≤ capacity). */
  readonly count: number;
  /** Capacity of the ring (constant per stream). */
  readonly capacity: number;
  /** Monotonic per-session counter for cheap change detection. */
  readonly version: number;
  /**
   * Read entry at logical index `[0, count)`; 0 is the oldest entry,
   * `count - 1` is the newest. Returns `undefined` for out-of-range or
   * uninitialised slots. Zero-allocation.
   */
  read(logicalIndex: number): EventLogEntry | undefined;
}

export type EventLogStream = Observable<EventLogSnapshot>;

/** Connection-state stream (BehaviorSubject under the hood). */
export type ConnectionStream = Observable<ConnectionState>;
