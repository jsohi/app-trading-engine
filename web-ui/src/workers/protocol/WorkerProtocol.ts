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

// ─── Command-port channel (bidirectional, multi-message) ──────────
//
// Plan §12 (APP-160). The main thread posts {@link CommandFramePortMessage}s
// onto the port; the worker pushes the payload bytes onto the wss send queue.
// On inbound wss frames matching templateId=70 (CommandAck), the worker decodes
// the correlationId + status and posts a {@link CommandAckPortMessage} back on
// the same port for main-thread routing.
//
// The bytes payload is ALWAYS the SBE-encoded NewOrderSingle frame (header +
// block) produced by `web-ui/src/sbe/encoders/NewOrderSingleEncoder.ts`. The
// worker does NOT re-validate the bytes — main-thread is the single point of
// truth for the wire layout.

export interface CommandFramePortMessage {
  readonly type: "COMMAND_FRAME";
  /** Wire bytes (SBE message header + block). */
  readonly bytes: Uint8Array;
  /**
   * Encoded length within {@code bytes} — the buffer may be a pooled
   * Uint8Array larger than the encoded frame.
   */
  readonly length: number;
  /**
   * Per-request correlation id (matches the SBE NewOrderSingle's clOrdId hash
   * the cluster echoes on CommandAck). Main-thread tracks this against its
   * pre-allocated slot table to resolve the awaiting Promise.
   */
  readonly correlationId: number;
}

export interface CommandAckPortMessage {
  readonly type: "COMMAND_ACK";
  readonly correlationId: number;
  /** "Accepted" | "Rejected" | "Duplicate" | "Throttled" — see CommandAckStatus enum. */
  readonly status: string;
  /** Optional textual reason for non-Accepted statuses. */
  readonly reasonCode?: string;
}

// ─── Token-port channel (one-way, single-message) ──────────────────

/**
 * Main → worker token-port message: an issued JWT. The first one is
 * consumed at INIT-time auth; subsequent ones are responses to a
 * worker-issued {@link TokenReauthRequestMessage} (Phase 3 Commit B).
 * The worker keeps the port open across the session lifetime to support
 * in-session reauth on {@code WebSocketError(AuthExpiringSoon)}.
 */
export interface TokenPortMessage {
  readonly type: "TOKEN";
  /** Opaque JWT bytes; treated as opaque by the worker. */
  readonly value: string;
}

/**
 * Worker → main token-port message: request a freshly-minted JWT
 * triggered by a server {@code WebSocketError(AuthExpiringSoon)}
 * (code 18). Main responds with a {@link TokenPortMessage}; the
 * worker validates the response's timing claims (nbf + exp ± 60s
 * leeway) and then runs the in-session reauth.
 */
export interface TokenReauthRequestMessage {
  readonly type: "REAUTH_REQUEST";
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
  /**
   * Optional bidirectional command port (APP-160). When present, the worker:
   *   - Receives {@link CommandFramePortMessage}s from main and forwards the
   *     payload bytes to the live wss send queue.
   *   - Decodes inbound CommandAck (templateId=70) wss frames and posts
   *     {@link CommandAckPortMessage}s back on this port for main-thread
   *     correlation by `correlationId`.
   * Absent in pre-APP-160 deployments — the worker treats it as "command
   * submission disabled" and routes nothing.
   */
  readonly commandPort?: MessagePort;
  /**
   * Backoff attempt counter to seed the worker's `Reconnect`
   * instance. Per Gemini review R10 (HIGH): WorkerClient persists
   * this across worker terminate+respawn cycles so the exponential
   * backoff progression continues across worker lifetimes (otherwise
   * every fresh worker resets to attempt=0 and the progressive
   * backoff collapses into a constant 0–500 ms window). Reset to 0
   * only on a successful AuthAck (signalled via the `connection-
   * state: CONNECTED` message).
   */
  readonly initialReconnectAttempt: number;
}

/** Liveness ping from main; worker MUST PONG within WATCHDOG_PONG_DEADLINE_MS. */
export interface PingMsg {
  readonly type: "PING";
  readonly protocolVersion: typeof WORKER_PROTOCOL_VERSION;
  readonly mainNanos: bigint;
}

/** Graceful close request; worker closes WS, posts final STATS, exits. */
export interface CloseMsg {
  readonly type: "CLOSE";
  readonly protocolVersion: typeof WORKER_PROTOCOL_VERSION;
}

// Per Gemini review (MEDIUM): no RECONNECT_NOW envelope. Manual
// "reconnect now" from main is implemented as a worker terminate +
// fresh spawn (see workerClient.reconnectNow), not via a message.
export type MainToWorker = InitMsg | PingMsg | CloseMsg;

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
  return o.type === "INIT" || o.type === "PING" || o.type === "CLOSE";
}
