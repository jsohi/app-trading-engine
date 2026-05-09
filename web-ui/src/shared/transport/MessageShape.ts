/**
 * Worker → Main thread message contract.
 *
 * This is the **single source of truth** for the Web Worker (APP-36)
 * `postMessage` payload shape. Every Phase 2 panel imports types from
 * here:
 *
 *   - APP-37 (Orders/Positions/Quotes blotters) consumes
 *     `OrderUpdate`, `FillUpdate`, `PriceUpdate`.
 *   - APP-42 (Event log) consumes `EventUpdate`.
 *   - APP-36 (Worker) MUST emit values structurally assignable to
 *     `WorkerMessage`. The mock `fakeStream.ts` emits the same shape
 *     so feature work can develop without the worker.
 *
 * Conventions:
 *   - `bigint` for SBE int64 / uint64 fields (prices, quantities,
 *     timestamps in epoch nanos). `Number()` coercion is BANNED —
 *     ESLint enforces (eslint.config.js: no-restricted-syntax).
 *   - Fixed-point pricing: `price` and similar are scaled by
 *     `100_000_000n` (PRICE_SCALE in @trading/sbe-codecs).
 *   - Timestamps: `serverNanos` is epoch nanoseconds as bigint;
 *     convert to `Date` only at render boundaries.
 */

export type Side = "BUY" | "SELL";

export interface PriceUpdate {
  readonly type: "price";
  readonly symbol: string;
  /** Fixed-point bid (scaled by 1e8). */
  readonly bid: bigint;
  /** Fixed-point ask (scaled by 1e8). */
  readonly ask: bigint;
  /** Server-side emit timestamp in epoch nanoseconds. */
  readonly serverNanos: bigint;
}

export interface OrderUpdate {
  readonly type: "order";
  readonly clOrdId: string;
  readonly symbol: string;
  readonly side: Side;
  /** Fixed-point quantity (scaled by 1e8). */
  readonly qty: bigint;
  /** Fixed-point limit price (scaled by 1e8). */
  readonly price: bigint;
  /** OPEN | PARTIAL | FILLED | CANCELLED | REJECTED. */
  readonly status: "OPEN" | "PARTIAL" | "FILLED" | "CANCELLED" | "REJECTED";
  readonly serverNanos: bigint;
}

export interface FillUpdate {
  readonly type: "fill";
  readonly clOrdId: string;
  readonly execId: string;
  readonly symbol: string;
  readonly side: Side;
  /** Fixed-point fill quantity (scaled by 1e8). */
  readonly fillQty: bigint;
  /** Fixed-point fill price (scaled by 1e8). */
  readonly fillPrice: bigint;
  readonly serverNanos: bigint;
}

export interface EventUpdate {
  readonly type: "event";
  /** Cluster sequence number — uint64 → bigint. */
  readonly seq: bigint;
  /** Short event type code (e.g. "OrderAccepted", "QuoteRequested"). */
  readonly eventType: string;
  /** Free-form details — schema-defined per event type. */
  readonly details: string;
  readonly serverNanos: bigint;
}

/**
 * Connection-stream state surfaced by the worker via `ConnectionStateMsg`.
 *
 * - `CONNECTING` — pre-AuthAck handshake.
 * - `CONNECTED` — steady state (post-AuthAck, pre-disturbance).
 * - `RECONNECTING` — backoff between failed attempts.
 * - `BACKPRESSURE` — server signaled SlowConsumer or local bufferedAmount over threshold.
 * - `STALE` — visibility-hidden + missed heartbeat; informational, not closed.
 * - `DOWN` — closed; auto-reconnect in progress.
 * - `DOWN_REQUIRES_USER_ACTION` — circuit-breaker tripped; manual reset needed.
 * - `SCHEMA_MISMATCH` — schema-id / version mismatch; no auto-reconnect (loop guard).
 * - `WORKER_DEAD` — worker crashed too many times in window; replace tab.
 * - `PROTOCOL_VIOLATION` — server sent invalid wire; no auto-reconnect.
 */
export type ConnectionState =
  | "CONNECTING"
  | "CONNECTED"
  | "RECONNECTING"
  | "BACKPRESSURE"
  | "STALE"
  | "DOWN"
  | "DOWN_REQUIRES_USER_ACTION"
  | "SCHEMA_MISMATCH"
  | "WORKER_DEAD"
  | "PROTOCOL_VIOLATION";

export interface ConnectionStateMsg {
  readonly type: "connection-state";
  readonly state: ConnectionState;
  readonly serverNanos: bigint;
}

/** Snapshot reassembly completed for a logical entity. */
export interface SnapshotComplete {
  readonly type: "snapshot-complete";
  /** Last-4 hex of the snapshotId UUID; never the full id. */
  readonly snapshotIdTrunc: string;
  /** Total bytes reassembled across all fragments. */
  readonly totalBytes: number;
  readonly serverNanos: bigint;
}

/** Replay finished — server emitted ReplayComplete (template 72). */
export interface ReplayCompleteMsg {
  readonly type: "replay-complete";
  /** Server-side reliable seqNo at end of replay. */
  readonly seqNo: bigint;
  readonly serverNanos: bigint;
}

/** Worker observed BACKPRESSURE state transition (entry, not steady state). */
export interface BackpressureWarning {
  readonly type: "backpressure-warning";
  /** Reason: server-signaled (code 9) or local bufferedAmount poll. */
  readonly source: "server" | "local-buffered-amount";
  readonly serverNanos: bigint;
}

/** Schema-id / version mismatch on first inbound frame. No auto-reconnect. */
export interface SchemaMismatch {
  readonly type: "schema-mismatch";
  readonly expectedSchemaId: number;
  readonly expectedVersion: number;
  readonly observedSchemaId: number;
  readonly observedVersion: number;
  readonly serverNanos: bigint;
}

/**
 * Periodic STATS broadcast — APP-245 bridges to OTel meter / vendor SDK
 * when vendor decision lands.
 */
export interface StatsMsg {
  readonly type: "stats";
  readonly framesDecoded: bigint;
  readonly bytesDecoded: bigint;
  readonly crcMismatches: bigint;
  readonly gaps: bigint;
  readonly reconnects: bigint;
  readonly replayFrames: bigint;
  readonly snapshotBytes: bigint;
  readonly bufferedAmountPeak: bigint;
  /** True for Firefox-RF / Safari low-resolution `performance.now()` users. */
  readonly degradedTimingMode: boolean;
  readonly serverNanos: bigint;
}

/**
 * Sealed worker → main error envelope. NEVER carries token or full sessionId.
 *
 * `code` mirrors broad categories that drive UI surfacing; granular server
 * `WebSocketErrorCode` values are surfaced via ConnectionState transitions.
 */
export interface ErrorMsg {
  readonly type: "error";
  readonly code: "INIT" | "AUTH" | "CRC" | "PROTOCOL" | "SCHEMA" | "BUFFER" | "WORKER";
  /** Last-4 hex of sessionId, only when relevant; never the full UUID. */
  readonly sessionIdTrunc?: string;
  /** Static-allowlist hint string; never errorText from server registry untrusted path. */
  readonly hint?: string;
}

/**
 * Discriminated union of every message a worker can post to the main
 * thread. Sole versioned barrel for the transport contract.
 */
export type WorkerMessage =
  | PriceUpdate
  | OrderUpdate
  | FillUpdate
  | EventUpdate
  | ConnectionStateMsg
  | SnapshotComplete
  | ReplayCompleteMsg
  | BackpressureWarning
  | SchemaMismatch
  | StatsMsg
  | ErrorMsg;

/**
 * Versioned shape contract — bumped on any non-additive change to
 * `WorkerMessage`. Consumers (APP-37 / APP-40 / APP-42) assert against
 * this at boot.
 */
export const MESSAGE_SHAPE_VERSION = 1;

/**
 * Exhaustiveness helper — `assertNever(x)` makes a `switch` over
 * `WorkerMessage["type"]` a compile-time error if a new variant is
 * added without a handler.
 *
 * Threading: any (compile-time only).
 * Allocation: zero (call site is the throw path).
 */
export function assertNever(x: never): never {
  // bigint-aware replacer: every WorkerMessage variant carries a `serverNanos`
  // bigint; a naive JSON.stringify would itself throw `TypeError: Do not know
  // how to serialize a BigInt` and mask the intended exhaustiveness diagnostic.
  const replacer = (_key: string, value: unknown): unknown =>
    typeof value === "bigint" ? value.toString() : value;
  throw new Error(`unhandled discriminant: ${JSON.stringify(x, replacer)}`);
}
