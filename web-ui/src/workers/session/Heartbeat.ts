/**
 * Heartbeat — outbound ClientHeartbeat cadence + inbound server-deadline
 * tracker, both visibility-aware.
 *
 * Outbound: chained `setTimeout` based on `performance.now()` deadlines
 * (HTML §9.5.2 background-tab throttle to 1 Hz cannot starve us — the
 * deadline check fires regardless and we send if overdue).
 *
 * Inbound: tracks `lastServerActivityNs` (refreshed on ANY inbound
 * frame, not just heartbeats) and trips a "deadline exceeded" callback
 * when no activity for `3 × serverHeartbeatIntervalMs` (or under hidden
 * visibility, `max(3 × interval, 60 s)`).
 *
 * On `visibilitychange → 'visible'` the deadline anchor is reset to
 * `now` and an immediate outbound heartbeat is forced — defends
 * against tab-switch reconnect storms per §2.8.
 *
 * Threading: worker scope only. The DOM `visibilitychange` event
 * is observed in the main thread; the main thread informs the worker
 * via a `WorkerProtocol` message which calls `onVisibilityChange`.
 *
 * Allocation: zero per tick; one `setTimeout` registration per outbound
 * cycle. No `setInterval` (would not respect deadline-anchored chasing).
 *
 * Plan reference: §2.8 / §6 row 13.
 */

import { type SessionState } from "@/workers/session/SessionState";
import {
  HEARTBEAT_HIDDEN_FLOOR_MS,
  SERVER_HEARTBEAT_DEADLINE_MULTIPLIER,
} from "@/workers/WorkerTuning";

export interface HeartbeatCallbacks {
  /** Called when the outbound cadence fires; caller encodes + sends a ClientHeartbeat frame. */
  onOutboundDue: (clientNanos: bigint) => void;
  /** Called when the server-deadline trips; caller closes the WS + reconnects. */
  onServerDeadlineExceeded: (millisSinceLastActivity: number) => void;
}

/**
 * Heartbeat scheduler. Caller drives the outbound timer via
 * `setTimeout(callback, ms)`-style scheduling; the abstraction keeps
 * the heartbeat logic testable without real DOM clocks.
 */
export interface HeartbeatScheduler {
  setTimeout(handler: () => void, delayMs: number): number;
  clearTimeout(handle: number): void;
}

export class Heartbeat {
  private outboundHandle: number | null = null;
  private hidden = false;
  private readonly state: SessionState;
  private readonly cb: HeartbeatCallbacks;
  private readonly sched: HeartbeatScheduler;
  // Per /review HIGH (Agent B): inject a precision-preserving
  // `nowNs` instead of recomputing `BigInt(Math.floor(nowMs * 1e6))`
  // inline (which loses sub-millisecond precision; see time.ts
  // header for the full rationale and APP-36 R12).
  private readonly nowNs: () => bigint;

  constructor(
    state: SessionState,
    callbacks: HeartbeatCallbacks,
    scheduler: HeartbeatScheduler,
    nowNs: () => bigint,
  ) {
    this.state = state;
    this.cb = callbacks;
    this.sched = scheduler;
    this.nowNs = nowNs;
  }

  /**
   * Start sending outbound heartbeats. Called after AuthAck so the
   * negotiated `clientHeartbeatIntervalMs` is in effect.
   */
  start(): void {
    this.armOutbound();
  }

  /**
   * Stop the outbound timer (e.g. on session close). Does NOT reset
   * activity tracking — that lives in `SessionState.lastServerActivityNs`
   * and is reset by `coldStart()` on the SessionState directly.
   */
  stop(): void {
    if (this.outboundHandle !== null) {
      this.sched.clearTimeout(this.outboundHandle);
      this.outboundHandle = null;
    }
  }

  /**
   * Check the inbound server-deadline. Caller invokes this on every
   * timer tick (e.g. from the outbound-due path). Returns true if
   * the deadline was exceeded (and the callback was fired).
   *
   * @param activityNanos `state.lastServerActivityNs`
   * @returns true iff deadline exceeded
   */
  checkServerDeadline(activityNanos: bigint): boolean {
    if (activityNanos === 0n) {
      // Pre-handshake; no activity recorded yet — not a deadline issue.
      return false;
    }
    const baseDeadlineMs =
      SERVER_HEARTBEAT_DEADLINE_MULTIPLIER * this.state.serverHeartbeatIntervalMs;
    const deadlineMs = this.hidden
      ? Math.max(baseDeadlineMs, HEARTBEAT_HIDDEN_FLOOR_MS)
      : baseDeadlineMs;
    const elapsedNs = this.nowNs() - activityNanos;
    if (elapsedNs > BigInt(deadlineMs) * 1_000_000n) {
      // bigint → number on a bounded value (elapsedNs / 1_000_000n is the
      // deadline ms since last activity; the deadline check just fired so
      // we know it fits in number safely up to 2^53 ms ≈ 285 thousand years).
      // eslint-disable-next-line no-restricted-syntax -- bounded bigint→number for callback signature
      const elapsedMs = Number(elapsedNs / 1_000_000n);
      this.cb.onServerDeadlineExceeded(elapsedMs);
      return true;
    }
    return false;
  }

  /**
   * Apply a `visibilitychange` event observed on the main thread.
   * On visible: re-anchor the deadline (set lastServerActivityNs to
   * now) and force an immediate outbound heartbeat.
   */
  onVisibilityChange(visible: boolean): void {
    this.hidden = !visible;
    if (visible) {
      this.state.lastServerActivityNs = this.nowNs();
      this.fireOutboundNow();
    }
  }

  /** Visible for tests. */
  isHidden(): boolean {
    return this.hidden;
  }

  private armOutbound(): void {
    if (this.outboundHandle !== null) {
      this.sched.clearTimeout(this.outboundHandle);
    }
    this.outboundHandle = this.sched.setTimeout(() => {
      // Per Gemini review (MEDIUM): if `stop()` cleared outboundHandle
      // while this callback was queued/executing, exit so we don't re-arm
      // and keep firing heartbeats after shutdown.
      if (this.outboundHandle === null) return;
      this.fireOutboundNow();
    }, this.state.clientHeartbeatIntervalMs);
  }

  private fireOutboundNow(): void {
    this.cb.onOutboundDue(this.nowNs());
    this.armOutbound();
  }
}
