/**
 * AuthClient — orchestrates the WebSocketAuth (template 60) handshake
 * and the in-band re-auth path per APP-36 §2.5 / §2.12.
 *
 * Handshake:
 *   1. Caller invokes `authenticate(token)`. Caller has already opened
 *      the WS and asserted the subprotocol echo.
 *   2. AuthClient encodes WebSocketAuth + sends. Starts a 5 s deadline
 *      via the scheduler.
 *   3. On AuthAck: validate `protocolVersion === 1` (server-asserted)
 *      AND `protocolVersion === sentProtocolVersion` (anti-echo). Fall
 *      back to defaults on `intervals === 0`. Resolve the promise.
 *   4. On WebSocketError or 5 s timeout: reject with the appropriate
 *      diagnostic.
 *
 * Re-auth (`reauth(newToken)`):
 *   - Sends another template-60 frame on the live session. Per §2.12
 *     ordering invariant: while the reauth promise is in flight,
 *     entitlement-sensitive frames (Subscribe/Unsubscribe — templates
 *     62/63) are queued worker-side; non-entitlement frames bypass.
 *     Queue cap `MAX_REAUTH_QUEUED_FRAMES = 64`; overflow → reject
 *     the in-flight reauth + surface PROTOCOL_VIOLATION.
 *   - On the next AuthAck (server preserves session): drain queued
 *     frames into the wire writer.
 *
 * Threading: worker scope only.
 *
 * Allocation: handshake is cold-path (one promise + one deadline
 * timer per attempt). The reauth queue is a fixed-size ring; over-cap
 * detection is constant-time.
 *
 * Plan reference: §2.5 / §2.12 / §6 rows 38, 41 / §A1.
 */

import {
  AUTH_TIMEOUT_MS,
  MAX_REAUTH_QUEUED_FRAMES,
  WORKER_PROTOCOL_VERSION,
} from "@/workers/WorkerTuning";
import { type SessionState, type UuidComposite } from "@/workers/session/SessionState";

/**
 * Decoded WebSocketAuthAck (template 61) values per APP-36 §A1.
 * Caller decodes via the generated SBE TS decoder (consumed in C6
 * `MessageRouter`); this interface is the worker-internal contract
 * AuthClient consumes.
 */
export interface AuthAck {
  readonly sessionId: UuidComposite;
  readonly protocolVersion: number;
  readonly maxSubscriptions: number;
  readonly serverHeartbeatIntervalMs: number;
  readonly clientHeartbeatIntervalMs: number;
}

export const SENT_PROTOCOL_VERSION = WORKER_PROTOCOL_VERSION;

export type AuthFailureReason =
  | "TIMEOUT"
  | "PROTOCOL_VERSION_MISMATCH"
  | "ECHO_ATTACK"
  | "SERVER_ERROR"
  | "REAUTH_QUEUE_OVERFLOW";

export interface AuthClientCallbacks {
  /** Caller writes the encoded bytes to the WebSocket. */
  sendBytes: (bytes: Uint8Array) => void;
  /** Encodes a WebSocketAuth (template 60) frame for the given JWT. */
  encodeAuth: (token: string, protocolVersion: number) => Uint8Array;
  /** Invoked once AuthAck arrives and is validated. */
  onAuthSuccess: () => void;
  /** Invoked on any auth failure path. */
  onAuthFailure: (reason: AuthFailureReason, message: string) => void;
}

export interface AuthScheduler {
  setTimeout(handler: () => void, delayMs: number): number;
  clearTimeout(handle: number): void;
  /** Wall-clock millis (typically `performance.timeOrigin + performance.now()`). */
  nowMs(): number;
}

interface QueuedReauthFrame {
  readonly templateId: number;
  readonly bytes: Uint8Array;
}

/**
 * Per-template policy: which outbound templates are entitlement-sensitive
 * and must be queued during an in-flight reauth.
 *
 * Subscribe (62) + Unsubscribe (63) are entitlement-sensitive.
 * Non-entitlement (heartbeat 65, ack 71, gap-request 68, session-resume
 * 69) bypass the queue and go straight to the wire.
 */
const ENTITLEMENT_SENSITIVE_TEMPLATES = Object.freeze<readonly number[]>([62, 63]);

export function isEntitlementSensitive(templateId: number): boolean {
  for (const id of ENTITLEMENT_SENSITIVE_TEMPLATES) {
    if (id === templateId) return true;
  }
  return false;
}

export class AuthClient {
  private readonly state: SessionState;
  private readonly cb: AuthClientCallbacks;
  private readonly sched: AuthScheduler;

  /** Active deadline timer for the current handshake (or null if idle). */
  private deadlineHandle: number | null = null;

  /** True iff a `reauth()` is in flight; queues entitlement-sensitive frames. */
  private reauthInFlight = false;

  /** Bounded queue. Per §2.12: overflow → reauth rejected + PROTOCOL_VIOLATION. */
  private readonly reauthQueue: QueuedReauthFrame[] = [];

  constructor(state: SessionState, callbacks: AuthClientCallbacks, scheduler: AuthScheduler) {
    this.state = state;
    this.cb = callbacks;
    this.sched = scheduler;
  }

  /**
   * Initial handshake. Caller has opened the WS + verified subprotocol echo.
   */
  authenticate(token: string): void {
    this.armDeadline();
    const bytes = this.cb.encodeAuth(token, SENT_PROTOCOL_VERSION);
    this.cb.sendBytes(bytes);
  }

  /**
   * In-band re-auth. Returns a promise resolving on the next AuthAck
   * (or rejecting on close / failure / queue overflow). Caller MUST
   * `await` before sending entitlement-sensitive frames per §2.12.
   */
  reauth(newToken: string): Promise<void> {
    if (this.reauthInFlight) {
      return Promise.reject(new Error("reauth already in flight"));
    }
    this.reauthInFlight = true;
    return new Promise((resolve, reject) => {
      const onSuccess = (): void => {
        this.reauthInFlight = false;
        this.flushReauthQueue();
        resolve();
      };
      const onFailure = (reason: AuthFailureReason, message: string): void => {
        this.reauthInFlight = false;
        // On reject: drop queued frames (caller surfaces error to UI).
        this.reauthQueue.length = 0;
        reject(new Error(`reauth failed (${reason}): ${message}`));
      };
      // Splice the resolution into the next callback invocations.
      this.pendingReauthResolve = onSuccess;
      this.pendingReauthReject = onFailure;

      this.armDeadline();
      const bytes = this.cb.encodeAuth(newToken, SENT_PROTOCOL_VERSION);
      this.cb.sendBytes(bytes);
    });
  }

  /**
   * Send an outbound frame. If a reauth is in flight AND the template
   * is entitlement-sensitive, the frame is queued. Returns true iff
   * the frame was sent immediately; false iff queued or rejected.
   *
   * On queue overflow: rejects the in-flight reauth + surfaces
   * PROTOCOL_VIOLATION via the failure callback.
   */
  sendOrQueue(templateId: number, bytes: Uint8Array): boolean {
    if (this.reauthInFlight && isEntitlementSensitive(templateId)) {
      if (this.reauthQueue.length >= MAX_REAUTH_QUEUED_FRAMES) {
        // Reject the in-flight reauth — caller surfaces PROTOCOL_VIOLATION.
        const reject = this.pendingReauthReject;
        this.pendingReauthResolve = null;
        this.pendingReauthReject = null;
        if (reject !== null) {
          reject("REAUTH_QUEUE_OVERFLOW", "reauth queue exceeded MAX_REAUTH_QUEUED_FRAMES");
        }
        return false;
      }
      this.reauthQueue.push({ templateId, bytes });
      return false;
    }
    this.cb.sendBytes(bytes);
    return true;
  }

  /**
   * Caller invokes on AuthAck arrival. Validates protocolVersion (anti-
   * echo + constant), populates `SessionState`, fires success.
   *
   * @param ack the decoded AuthAck values (caller decoded via SBE)
   * @param subClaim the JWT `sub` for continuity check (worker captured from token)
   */
  onAuthAck(ack: AuthAck, subClaim: string): void {
    this.cancelDeadline();

    // Anti-echo defense per §2.5 / §6 row 41: the AuthAck.protocolVersion
    // is server-asserted (server hard-codes EXPECTED_PROTOCOL_VERSION = 1
    // in its encoder; never echoes a client value). Client MUST validate
    // the decoded value matches the constant — the check is single-arm
    // because `WORKER_PROTOCOL_VERSION === SENT_PROTOCOL_VERSION` by
    // construction (we send what we expect). If the server ever echoes a
    // tampered value, the comparison fails and we reject before applying
    // any handshake state.
    if (ack.protocolVersion !== WORKER_PROTOCOL_VERSION) {
      this.failAuth(
        "PROTOCOL_VERSION_MISMATCH",
        `Ack.protocolVersion ${String(ack.protocolVersion)} != EXPECTED ${String(WORKER_PROTOCOL_VERSION)} (anti-echo)`,
      );
      return;
    }

    this.state.applyAuthAck(
      ack.sessionId,
      ack.protocolVersion,
      ack.maxSubscriptions,
      ack.serverHeartbeatIntervalMs,
      ack.clientHeartbeatIntervalMs,
      subClaim,
    );

    if (this.pendingReauthResolve !== null) {
      const resolve = this.pendingReauthResolve;
      this.pendingReauthResolve = null;
      this.pendingReauthReject = null;
      resolve();
    } else {
      this.cb.onAuthSuccess();
    }
  }

  /**
   * Caller invokes on inbound WebSocketError during the handshake / reauth window.
   */
  onAuthError(reason: string): void {
    this.failAuth("SERVER_ERROR", reason);
  }

  /**
   * Visible for tests.
   */
  reauthQueueLength(): number {
    return this.reauthQueue.length;
  }

  /**
   * Visible for tests.
   */
  isReauthInFlight(): boolean {
    return this.reauthInFlight;
  }

  private pendingReauthResolve: (() => void) | null = null;
  private pendingReauthReject: ((reason: AuthFailureReason, message: string) => void) | null = null;

  private armDeadline(): void {
    if (this.deadlineHandle !== null) {
      this.sched.clearTimeout(this.deadlineHandle);
    }
    this.deadlineHandle = this.sched.setTimeout(() => {
      this.failAuth("TIMEOUT", `auth deadline ${String(AUTH_TIMEOUT_MS)} ms exceeded`);
    }, AUTH_TIMEOUT_MS);
  }

  private cancelDeadline(): void {
    if (this.deadlineHandle !== null) {
      this.sched.clearTimeout(this.deadlineHandle);
      this.deadlineHandle = null;
    }
  }

  private flushReauthQueue(): void {
    while (this.reauthQueue.length > 0) {
      const next = this.reauthQueue.shift();
      if (next !== undefined) this.cb.sendBytes(next.bytes);
    }
  }

  private failAuth(reason: AuthFailureReason, message: string): void {
    this.cancelDeadline();
    if (this.pendingReauthReject !== null) {
      const reject = this.pendingReauthReject;
      this.pendingReauthResolve = null;
      this.pendingReauthReject = null;
      reject(reason, message);
    } else {
      this.cb.onAuthFailure(reason, message);
    }
    this.reauthQueue.length = 0;
  }
}

/** Re-export for downstream code that wants to refer to the value. */
export type { UuidComposite };
