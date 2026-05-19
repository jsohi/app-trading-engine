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
  /**
   * Phase 3 Commit B — bid size (units, fixed-point scale 1e8 — matches `bid` / `ask`). Decoded
   * from the {@code MarketDataTick} (template 54) {@code bidSize} field. Surfaced for grid display
   * of top-of-book depth.
   */
  readonly bidSize: bigint;
  /** Phase 3 Commit B — ask size; see {@link bidSize}. */
  readonly askSize: bigint;
  /**
   * Phase 3 Commit B — publisher-stack ingress timestamp in epoch nanoseconds (the moment the
   * pricing-service adapter consumed the inbound mid-rate). Required to compute
   * {@link publisherStackLatencyNanos}; pinned on the wire-contract shape so a worker decoder
   * that omits the dual-timestamp read fails TypeScript exhaustiveness.
   */
  readonly ingressNanos: bigint;
  /** Server-side emit timestamp in epoch nanoseconds. */
  readonly serverNanos: bigint;
  /**
   * Phase 3 Commit B — derived publisher-stack latency: {@code serverNanos - ingressNanos}. Recorded
   * into the {@code marketdata.publish.latency.nanos} histogram via the periodic STATS surface.
   * Negative values indicate clock skew on the publisher box (should not happen — both stamps come
   * from the same monotonic clock); the worker decoder writes the raw subtraction with no clamp.
   */
  readonly publisherStackLatencyNanos: bigint;
  /**
   * Phase 3 Commit B — end-to-end latency: {@code performance.now()*1e6 - serverNanos} computed at
   * worker decode time. Meaningful within a single host; cross-box accuracy requires PTP / chrony
   * (see {@code docs/clock-sync.md}). Recorded into the {@code marketdata.end-to-end.latency.nanos}
   * histogram. Negative values indicate worker-host clock lag; the worker decoder writes the raw
   * subtraction with no clamp.
   */
  readonly endToEndLatencyNanos: bigint;
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

/**
 * Net position per symbol — locally aggregated from `FillUpdate` events by
 * `positionStream` (APP-37). NOT a worker-emitted message; computed in the
 * main thread for display. Server-side `PositionProjection` (APP-25) is the
 * authoritative source on reconnect; APP-37 ships local aggregation only.
 *
 * - `netQty` is SIGNED (BUY adds, SELL subtracts).
 * - `avgPx` is the running VWAP of the OPEN side (resets to 0n when flat).
 * - `lastFillNanos` is the most recent fill's `serverNanos`.
 *
 * Plan reference: APP-37 §Scope item 2.
 */
export interface NetPosition {
  readonly symbol: string;
  /** Signed net quantity (fixed-point, scale 1e8). */
  readonly netQty: bigint;
  /** Running VWAP of the OPEN side (fixed-point, scale 1e8). 0n when flat. */
  readonly avgPx: bigint;
  readonly lastFillNanos: bigint;
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
 * - `CONNECTING` — pre-AuthAck handshake (initial or post-RECONNECTING attempt).
 * - `CONNECTED` — steady state (post-AuthAck, pre-disturbance).
 * - `RECONNECTING` — closed; auto-reconnect in progress (worker is in the backoff
 *   window before reissuing INIT to the main thread). This is the canonical
 *   transient state during any auto-recoverable close — the worker pushes it
 *   from {@code ws.onclose} / {@code ws.onerror} when {@code reconnect.nextDelayMs}
 *   does NOT return FREEZE. Visual: amber (same group as {@code CONNECTING}).
 * - `BACKPRESSURE` — server signaled SlowConsumer or local bufferedAmount over threshold.
 * - `STALE` — visibility-hidden + missed heartbeat; informational, not closed.
 * - `DOWN` — RESERVED for future use (e.g., explicit user-initiated disconnect
 *   that is not the circuit breaker). The worker currently never pushes
 *   {@code DOWN}; any auto-recoverable close routes through {@code RECONNECTING}
 *   and any non-recoverable close routes through the dedicated terminal states
 *   ({@code DOWN_REQUIRES_USER_ACTION}, {@code SCHEMA_MISMATCH},
 *   {@code PROTOCOL_VIOLATION}, {@code WORKER_DEAD}).
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

/**
 * Market-data feed liveness state — surfaced by Phase 3 Commit 6's browser-side state machine.
 *
 * - `LIVE` — ticks flowing within the heartbeat-base × 1.5 window.
 * - `QUIET` — heartbeats arriving but no ticks; publisher up, all symbols idle.
 * - `STALE` — no fragment of any kind for the stale threshold (3 × heartbeat). Heartbeats alone
 *   do NOT clear STALE; only a real tick proves the price-feed path is healthy (EBS Direct /
 *   ICE Impact pattern).
 *
 * Separate from {@link ConnectionState} — a STALE market-data feed MUST NOT trip the WS
 * reconnect breaker; the transport is healthy, only the pricing feed is dead.
 */
export type FeedState = "LIVE" | "QUIET" | "STALE";

/**
 * Market-data feed-state transition message. Emitted by the worker on each inbound
 * {@code MarketDataFeedStateChange} (template 57) frame and on reconnect-reset. Consumed by
 * the main thread's `feedState$` BehaviorSubject in `messageSource.ts`.
 */
export interface FeedStateMsg {
  readonly type: "feed-state";
  readonly state: FeedState;
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
  /**
   * Cumulative count of inbound PriceResponse (template 51) frames the browser worker has seen.
   *
   * Template 51 is orchestrator-bound (Phase 3 plan §Gap 2 semantic separation): the cluster
   * routes PriceResponse to the orchestrator's session, never to the browser. A non-zero value
   * indicates a broadcast routing regression. Spec 07 (replay/reconnect) asserts this counter
   * stays at zero. Surfaced from the worker's per-second STATS emission.
   */
  readonly marketdataMisroutedRfq: bigint;
  /**
   * Phase 3 Commit B — cumulative market-data gaps attributed to publisher-side conflation
   * (publisher dropped before publish). Counterpart to {@link marketdataGapsNetwork}; together
   * they fully account for an observed gap. Bridged to `marketdata.gaps{reason="publisher-
   * conflated"}` by APP-245.
   */
  readonly marketdataGapsPublisherConflated: bigint;
  /**
   * Phase 3 Commit B — cumulative market-data gaps attributed to network / transport loss.
   * Bridged to `marketdata.gaps{reason="network"}`.
   */
  readonly marketdataGapsNetwork: bigint;
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
 * Phase 3 Commit B — per-account UI panel-to-slot mapping decoded from the
 * server's {@code WebSocketAuthAck} (template 61) {@code panelLayout} group.
 * The main thread reads this once at AuthAck time and mounts each panel
 * (e.g. {@code OrderEntryForm}) into its server-asserted slot. Empty
 * {@code panels} → main thread uses default slot bindings.
 */
export interface PanelLayoutMsg {
  readonly type: "PANEL_LAYOUT";
  readonly panels: ReadonlyArray<{ readonly panelId: string; readonly slot: string }>;
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
  | FeedStateMsg
  | SnapshotComplete
  | ReplayCompleteMsg
  | BackpressureWarning
  | SchemaMismatch
  | StatsMsg
  | ErrorMsg
  | PanelLayoutMsg;

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
