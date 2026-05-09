/**
 * WorkerProtocol — sealed main↔worker message types.
 *
 * **No runtime in this module** — types only. Runtime dispatch lives
 * in `worker.ts` (worker side) and `main-thread/workerClient.ts` (main
 * side); both depend on this contract.
 *
 * Critical invariants:
 *   1. **No token field in any message**. The token transits to the
 *      worker via a `MessagePort` carried in `INIT.tokenPort`; the
 *      port writes a single `TokenPortMessage` and `close()`s. The
 *      trading-app main thread never holds the token string.
 *   2. **Protocol-version validated as the first action** in worker
 *      `onmessage` per plan §6 row 19. Any inbound message whose
 *      `protocolVersion` ≠ `WORKER_PROTOCOL_VERSION` is rejected.
 *   3. `MESSAGE_BATCH` carries one `WorkerMessage[]` array per flush
 *      (≤ `BATCH_FLUSH_FRAMES` per §4.5). Structured-clone copy
 *      semantics; no `Transferable` except the snapshot-final
 *      ArrayBuffer exemption (§4.5).
 *
 * Threading:
 *   - This module is imported by both worker and main; both sides
 *     reference the same TS types.
 *
 * Allocation:
 *   - All values are types — zero runtime cost.
 *
 * Plan reference: §4.2 / §4.5 / §4.7 / §5.3 / §6 row 19.
 */

import type { WorkerMessage } from "@/shared/transport/MessageShape";

import { WORKER_PROTOCOL_VERSION } from "@/workers/WorkerTuning";
// Note: WORKER_PROTOCOL_VERSION is imported here for type-positions
// (`typeof WORKER_PROTOCOL_VERSION`) and runtime guards. Consumers
// must import from `@/workers/WorkerTuning` directly — re-exporting
// here was removed to avoid two import paths drifting.

// ─── Token-port channel (one-way, single-message) ──────────────────

/**
 * The single message a `tokenPort` may carry, written by the issuer
 * (e.g. APP-160 auth iframe) and read once by the worker.
 *
 * After receipt, the worker `close()`s the port on its side; the main
 * thread closed its side at port creation. The token is captured in
 * a worker-local `let` closure and never re-emitted.
 */
export interface TokenPortMessage {
  readonly type: "TOKEN";
  /** Opaque JWT bytes; treated as opaque by the worker. */
  readonly value: string;
}

// ─── Main → worker ─────────────────────────────────────────────────

/**
 * Bootstrap. Carries credential + watchdog ports as Transferable.
 * `wsUrl` is `VITE_WS_URL` from the build, asserted by `WsUrlValidator`.
 */
export interface InitMsg {
  readonly type: "INIT";
  readonly protocolVersion: typeof WORKER_PROTOCOL_VERSION;
  readonly wsUrl: string;
  /** One-way token port. Worker reads single TOKEN, then closes. */
  readonly tokenPort: MessagePort;
  /** Bidirectional watchdog port for PING/PONG liveness. */
  readonly watchdogPort: MessagePort;
}

/** Liveness ping from main; worker MUST PONG within WATCHDOG_PONG_DEADLINE_MS. */
export interface PingMsg {
  readonly type: "PING";
  readonly protocolVersion: typeof WORKER_PROTOCOL_VERSION;
  readonly mainNanos: bigint;
}

/** Manual user-driven reconnect; resets reconnect backoff cap. */
export interface ReconnectNowMsg {
  readonly type: "RECONNECT_NOW";
  readonly protocolVersion: typeof WORKER_PROTOCOL_VERSION;
}

/** Graceful close request; worker closes WS, posts final STATS, exits. */
export interface CloseMsg {
  readonly type: "CLOSE";
  readonly protocolVersion: typeof WORKER_PROTOCOL_VERSION;
}

export type MainToWorker = InitMsg | PingMsg | ReconnectNowMsg | CloseMsg;

// ─── Worker → main ─────────────────────────────────────────────────

/** PONG reply to a PING. */
export interface PongMsg {
  readonly type: "PONG";
  readonly protocolVersion: typeof WORKER_PROTOCOL_VERSION;
  /** Echo the main's PING nanos so main can compute RTT. */
  readonly echoMainNanos: bigint;
  readonly workerNanos: bigint;
}

/**
 * Decoded events for one batch (≤ BATCH_FLUSH_FRAMES). Copy semantics;
 * no Transferable except the snapshot-final ArrayBuffer exemption.
 */
export interface MessageBatchMsg {
  readonly type: "MESSAGE_BATCH";
  readonly protocolVersion: typeof WORKER_PROTOCOL_VERSION;
  readonly messages: ReadonlyArray<WorkerMessage>;
}

/**
 * Worker-fatal error. Surfaces via main-thread `workerClient`; main
 * decides on respawn (≤3 in 30 s window).
 */
export interface WorkerErrorMsg {
  readonly type: "ERROR";
  readonly protocolVersion: typeof WORKER_PROTOCOL_VERSION;
  /** Sealed error category; see MessageShape.ErrorMsg. */
  readonly code: "INIT" | "AUTH" | "CRC" | "PROTOCOL" | "SCHEMA" | "BUFFER" | "WORKER";
  readonly sessionIdTrunc?: string;
  readonly hint?: string;
}

export type WorkerToMain = PongMsg | MessageBatchMsg | WorkerErrorMsg;

// ─── Discriminator narrowing helpers ───────────────────────────────

/**
 * Type guard for a valid main → worker envelope. Validates the
 * protocol version BEFORE any branch on `type` per §6 row 19.
 */
export function isMainToWorker(x: unknown): x is MainToWorker {
  if (x === null || typeof x !== "object") return false;
  const o = x as { type?: unknown; protocolVersion?: unknown };
  if (o.protocolVersion !== WORKER_PROTOCOL_VERSION) return false;
  if (typeof o.type !== "string") return false;
  return o.type === "INIT" || o.type === "PING" || o.type === "RECONNECT_NOW" || o.type === "CLOSE";
}
