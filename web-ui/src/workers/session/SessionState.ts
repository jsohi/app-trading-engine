/**
 * SessionState — pure state container for the worker's session.
 *
 * Holds the negotiated identity + cadence + reliable-stream cursor for
 * a single live WebSocket session. Mutated by AuthClient (handshake),
 * Heartbeat (activity-refresh), Reconnect (counter resets), GapTracker
 * (lastReliableSeqNo advance), and BackpressureController (degraded
 * timing flag).
 *
 * Threading: worker scope only; no concurrent access.
 *
 * Allocation: zero per access (fields are bigint / number / string;
 * UUID composite is two bigints).
 *
 * Plan reference: §2.5 / §2.6 / §2.8 / §2.9 / §5.2.
 */

import {
  CLIENT_HEARTBEAT_INTERVAL_DEFAULT_MS,
  SERVER_HEARTBEAT_INTERVAL_DEFAULT_MS,
} from "@/workers/WorkerTuning";

/**
 * UUID composite as decoded from the SBE wire — two int64 halves. The
 * worker keeps these as bigints to avoid lossy string conversions on
 * the per-frame path; only display-time code stringifies via the
 * `truncSessionId` helper below.
 */
export interface UuidComposite {
  readonly mostSignificantBits: bigint;
  readonly leastSignificantBits: bigint;
}

/**
 * Mutable session-state container. Constructed empty (pre-handshake);
 * `applyAuthAck` populates it once the server's AuthAck is received.
 */
export class SessionState {
  /** Negotiated session UUID for the current connection. `null` until AuthAck. */
  currentSessionId: UuidComposite | null = null;

  /** UUID of the prior session during a SessionResume window. Cleared on ReplayComplete or cold-start. */
  priorSessionId: UuidComposite | null = null;

  /** Last reliable seqNo successfully delivered to the dispatcher. */
  lastReliableSeqNo = 0n;

  /** Monotonic-ish wall-clock timestamp of last inbound frame (any kind). bigint nanos. */
  lastServerActivityNs = 0n;

  /** sub claim from the JWT for continuity check; never logged. */
  subClaim = "";

  /** Negotiated server-→client cadence (default 5000 ms). */
  serverHeartbeatIntervalMs: number = SERVER_HEARTBEAT_INTERVAL_DEFAULT_MS;

  /** Negotiated client-→server cadence (default 10 000 ms). */
  clientHeartbeatIntervalMs: number = CLIENT_HEARTBEAT_INTERVAL_DEFAULT_MS;

  /** Maximum simultaneous symbols per subscribe (from AuthAck.maxSubscriptions). */
  maxSubscriptions = 0;

  /** Server-asserted protocol version (from AuthAck.protocolVersion). */
  protocolVersion = 0;

  /** Consecutive auth-failure counter (codes 1 / 8). Resets only on a successful AuthAck. */
  consecutiveAuthFailures = 0;

  /** Wall-clock nanos of the last RateLimitExceeded event for the 5-min freeze window. */
  lastRateLimitAtNs = 0n;

  /** True for Firefox-RF / Safari low-resolution `performance.now()` users. */
  degradedTimingMode = false;

  /**
   * Apply an AuthAck — store negotiated values + reset relevant counters.
   * Falls back to defaults on `intervals === 0` per §A1.
   */
  applyAuthAck(
    sessionId: UuidComposite,
    protocolVersion: number,
    maxSubscriptions: number,
    serverHeartbeatIntervalMs: number,
    clientHeartbeatIntervalMs: number,
    sub: string,
  ): void {
    this.currentSessionId = sessionId;
    this.protocolVersion = protocolVersion;
    this.maxSubscriptions = maxSubscriptions;
    this.serverHeartbeatIntervalMs =
      serverHeartbeatIntervalMs > 0
        ? serverHeartbeatIntervalMs
        : SERVER_HEARTBEAT_INTERVAL_DEFAULT_MS;
    this.clientHeartbeatIntervalMs =
      clientHeartbeatIntervalMs > 0
        ? clientHeartbeatIntervalMs
        : CLIENT_HEARTBEAT_INTERVAL_DEFAULT_MS;
    this.subClaim = sub;
    this.consecutiveAuthFailures = 0;
  }

  /**
   * Cold-start clears both session ids per §2.6. Called from any close
   * that triggers cold-start (codes 1, 2, 4, 7-after-resume, 8, 11;
   * close-code 4xxx; PROTOCOL_VIOLATION).
   */
  coldStart(): void {
    this.currentSessionId = null;
    this.priorSessionId = null;
    this.lastReliableSeqNo = 0n;
  }

  /** Drop the prior-session pointer. Called on ReplayComplete after a SessionResume. */
  dropPriorSessionId(): void {
    this.priorSessionId = null;
  }
}

/**
 * Returns the last 4 hex characters of a UUID's least-significant bits
 * for log-safe display (never the full UUID per logging hygiene §6 row 28).
 */
export function truncSessionId(id: UuidComposite | null): string {
  if (id === null) return "null";
  const lsbHex = id.leastSignificantBits.toString(16).padStart(16, "0");
  return lsbHex.slice(-4);
}
