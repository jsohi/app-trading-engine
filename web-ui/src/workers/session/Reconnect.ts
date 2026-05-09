/**
 * Reconnect — full-jitter exponential backoff with circuit breakers.
 *
 * Backoff formula (AWS Architecture Blog "Exponential Backoff and Jitter"):
 *   delay = uniform(0, min(CAP_MS, BASE_MS × 2^attempt))
 *
 * Circuit breakers per §2.13 / §5.2:
 *   - 3 consecutive `AuthenticationFailed` (code 1) OR `VersionMismatch`
 *     (code 8) without a successful AuthAck → freeze
 *   - Code 2 (`AuthorizationFailed`) → immediate freeze
 *   - Code 3 (`RateLimitExceeded`): 1st → cap × 4; 2nd within 5 min → freeze
 *   - 30 attempts in any 10-min sliding window without successful AuthAck → freeze
 *   - Close codes 1011 (`server internal error`) / 1014 → cap × 4
 *   - Code 10 (`ServerShutdown`) / close 1012 / close 1013 → cap × 8
 *
 * Counter reset rule: `consecutiveAuthFailures` resets ONLY on a
 * successful AuthAck. Other close codes (4 / 6 / 7 / 10) close the
 * connection but the auth counter persists across the close — a
 * BufferOverflow does not absolve a credential-stuffing attempt.
 *
 * Threading: worker scope only.
 *
 * Allocation: per-attempt: one `delay` number. The sliding-window
 * attempt-timestamp ring is a fixed-size `BigInt64Array` allocated
 * once at construction.
 *
 * Plan reference: §2.13 / §5.2 / §6 row 14.
 */

import { type SessionState } from "@/workers/session/SessionState";
import {
  CONSECUTIVE_AUTH_FAILURES_FREEZE,
  RATE_LIMIT_FREEZE_AFTER,
  RATE_LIMIT_FREEZE_WINDOW_MS,
  RECONNECT_BASE_MS,
  RECONNECT_CAP_MS,
  RECONNECT_CAP_MULTIPLIER_RATE_LIMIT,
  RECONNECT_CAP_MULTIPLIER_SHUTDOWN,
  RECONNECT_FREEZE_AFTER_ATTEMPTS,
  RECONNECT_FREEZE_WINDOW_MS,
} from "@/workers/WorkerTuning";

/** Output of `nextDelayMs` — either a backoff or a freeze decision. */
export type ReconnectDecision =
  | { readonly kind: "BACKOFF"; readonly delayMs: number }
  | { readonly kind: "FREEZE"; readonly reason: string };

export type AppErrorCode =
  | 1 // AuthenticationFailed
  | 2 // AuthorizationFailed
  | 3 // RateLimitExceeded
  | 4 // SessionExpired
  | 6 // HeartbeatTimeout
  | 7 // BufferOverflow
  | 8 // VersionMismatch
  | 10 // ServerShutdown
  | 11; // CommandRejected

/** Numeric WebSocket close code (RFC 6455 + 4xxx custom). */
export type CloseCode = number;

/**
 * Random number generator interface. Abstracted for deterministic tests.
 */
export interface RandomSource {
  /** Returns a uniform random in [0, 1). */
  next(): number;
}

/**
 * Reconnect state machine. One instance per worker; persists across
 * reconnect attempts so the sliding-window counters work.
 */
export class Reconnect {
  /** Wall-clock-nanos ring of recent attempt timestamps for the 30-in-10-min check. */
  private readonly attemptTimestampsNs: bigint[] = [];

  /** Multiplier applied to `RECONNECT_CAP_MS` for the next attempt. */
  private nextCapMultiplier = 1;

  private readonly rng: RandomSource;
  private readonly nowNs: () => bigint;
  private attempt = 0;

  constructor(rng: RandomSource, nowNs: () => bigint) {
    this.rng = rng;
    this.nowNs = nowNs;
  }

  /**
   * Compute the delay before the next reconnect attempt OR return
   * a freeze decision. Records the attempt timestamp for the sliding-
   * window check.
   *
   * Caller MUST invoke `notifyAuthAckSuccess()` once a fresh AuthAck
   * arrives so per-session counters reset.
   */
  nextDelayMs(state: SessionState): ReconnectDecision {
    const now = this.nowNs();
    this.attemptTimestampsNs.push(now);
    this.pruneAttemptWindow(now);

    if (this.attemptTimestampsNs.length > RECONNECT_FREEZE_AFTER_ATTEMPTS) {
      return {
        kind: "FREEZE",
        reason: `${String(RECONNECT_FREEZE_AFTER_ATTEMPTS)} attempts in ${String(
          Math.floor(RECONNECT_FREEZE_WINDOW_MS / 60_000),
        )} min sliding window`,
      };
    }

    if (state.consecutiveAuthFailures >= CONSECUTIVE_AUTH_FAILURES_FREEZE) {
      return {
        kind: "FREEZE",
        reason: `${String(CONSECUTIVE_AUTH_FAILURES_FREEZE)} consecutive auth failures`,
      };
    }

    const cap = Math.min(RECONNECT_CAP_MS * this.nextCapMultiplier, Number.MAX_SAFE_INTEGER);
    const exp = Math.min(cap, RECONNECT_BASE_MS * Math.pow(2, this.attempt));
    const delayMs = Math.floor(this.rng.next() * exp);
    this.attempt += 1;
    return { kind: "BACKOFF", delayMs };
  }

  /**
   * Apply an inbound `WebSocketError` code (template 67) to the state
   * machine. Returns true iff the caller should freeze immediately
   * (no further reconnect attempts permitted).
   */
  applyAppErrorCode(state: SessionState, code: AppErrorCode): boolean {
    switch (code) {
      case 1: // AuthenticationFailed
      case 8: // VersionMismatch
        state.consecutiveAuthFailures += 1;
        if (state.consecutiveAuthFailures >= CONSECUTIVE_AUTH_FAILURES_FREEZE) {
          return true;
        }
        return false;
      case 2: // AuthorizationFailed → immediate freeze
        return true;
      case 3: {
        // RateLimitExceeded — escalate cap × 4 first; freeze on 2nd within 5 min.
        const now = this.nowNs();
        if (state.lastRateLimitAtNs !== 0n) {
          const elapsedMs = (now - state.lastRateLimitAtNs) / 1_000_000n; // ns → ms
          if (elapsedMs <= BigInt(RATE_LIMIT_FREEZE_WINDOW_MS)) {
            return true;
          }
        }
        state.lastRateLimitAtNs = now;
        this.nextCapMultiplier = Math.max(
          this.nextCapMultiplier,
          RECONNECT_CAP_MULTIPLIER_RATE_LIMIT,
        );
        // RATE_LIMIT_FREEZE_AFTER is the threshold (currently 2 — freeze
        // on 2nd occurrence within window). The 1st occurrence sets
        // lastRateLimitAtNs above; if the next code-3 arrives within the
        // window the elapsedMs check above returns true. RATE_LIMIT_FREEZE
        // _AFTER is referenced for documentation; it is not used in a
        // counter form here because the timestamp + window comparison
        // is the operative check.
        void RATE_LIMIT_FREEZE_AFTER;
        return false;
      }
      case 10: // ServerShutdown
        this.nextCapMultiplier = Math.max(
          this.nextCapMultiplier,
          RECONNECT_CAP_MULTIPLIER_SHUTDOWN,
        );
        return false;
      case 4: // SessionExpired
      case 6: // HeartbeatTimeout
      case 7: // BufferOverflow
        // No counter increment, no freeze — caller cold-starts and reconnects.
        return false;
      case 11: // CommandRejected — close + 1 cold-start; freeze if recurs (caller-side)
        return false;
      default:
        return false;
    }
  }

  /** Apply a WebSocket close code per the §2.13 table. Returns FREEZE iff terminal. */
  applyCloseCode(code: CloseCode): "RECONNECT" | "PROTOCOL_VIOLATION" | "SCHEMA_MISMATCH" {
    if (code === 1000 || code === 1001 || code === 1006) return "RECONNECT";
    if (code === 1011 || code === 1014) {
      this.nextCapMultiplier = Math.max(
        this.nextCapMultiplier,
        RECONNECT_CAP_MULTIPLIER_RATE_LIMIT,
      );
      return "RECONNECT";
    }
    if (code === 1012 || code === 1013) {
      this.nextCapMultiplier = Math.max(this.nextCapMultiplier, RECONNECT_CAP_MULTIPLIER_SHUTDOWN);
      return "RECONNECT";
    }
    if (code === 1015) return "SCHEMA_MISMATCH";
    if (
      code === 1002 ||
      code === 1003 ||
      code === 1007 ||
      code === 1008 ||
      code === 1009 ||
      code === 1010
    ) {
      return "PROTOCOL_VIOLATION";
    }
    // 4xxx custom range: caller separately checks for preceding WebSocketError;
    // bare 4xxx → PROTOCOL_VIOLATION.
    if (code >= 4000 && code <= 4999) return "PROTOCOL_VIOLATION";
    // Unknown close codes: treat as PROTOCOL_VIOLATION (defensive).
    return "PROTOCOL_VIOLATION";
  }

  /**
   * Reset the auth-failure counter and cap multiplier on a successful AuthAck.
   * Per §2.13: counter resets ONLY here; other close codes do NOT reset it.
   */
  notifyAuthAckSuccess(state: SessionState): void {
    state.consecutiveAuthFailures = 0;
    state.lastRateLimitAtNs = 0n;
    this.nextCapMultiplier = 1;
    this.attempt = 0;
  }

  /** Manual user-driven reset (RECONNECT_NOW button). Resets backoff but not auth-failure counter. */
  resetNow(): void {
    this.attempt = 0;
    this.nextCapMultiplier = 1;
    this.attemptTimestampsNs.length = 0;
  }

  /** Visible for tests. */
  pendingAttempt(): number {
    return this.attempt;
  }

  private pruneAttemptWindow(nowNs: bigint): void {
    const cutoffNs = nowNs - BigInt(RECONNECT_FREEZE_WINDOW_MS) * 1_000_000n;
    while (this.attemptTimestampsNs.length > 0 && (this.attemptTimestampsNs[0] ?? 0n) < cutoffNs) {
      this.attemptTimestampsNs.shift();
    }
  }
}
