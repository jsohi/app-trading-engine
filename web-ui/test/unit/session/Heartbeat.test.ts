/**
 * Heartbeat.test.ts — unit tests for the outbound cadence + inbound
 * server-deadline logic per APP-36 §2.8.
 *
 * Uses a controllable `HeartbeatScheduler` (queue of pending timers + a
 * `tick()` function to fire the next due timer) and a controllable
 * `nowMs()` clock.
 *
 * Test naming follows `<unit>_<scenario>_<expected>` per plan §5.8.
 *
 * Threading: single-threaded (Vitest jsdom worker).
 */

import { describe, expect, it, beforeEach } from "vitest";
import {
  Heartbeat,
  type HeartbeatCallbacks,
  type HeartbeatScheduler,
} from "@/workers/session/Heartbeat";
import { SessionState } from "@/workers/session/SessionState";
import {
  CLIENT_HEARTBEAT_INTERVAL_DEFAULT_MS,
  SERVER_HEARTBEAT_INTERVAL_DEFAULT_MS,
  HEARTBEAT_HIDDEN_FLOOR_MS,
} from "@/workers/WorkerTuning";

// ─── Controllable scheduler ──────────────────────────────────────────────────

interface PendingTimer {
  handle: number;
  handler: () => void;
  delayMs: number;
}

function makeScheduler(): {
  scheduler: HeartbeatScheduler;
  /** Fire the next scheduled timer (FIFO). Returns false if none pending. */
  tick: () => boolean;
  pendingCount: () => number;
  clearAll: () => void;
} {
  let nextHandle = 1;
  const timers: PendingTimer[] = [];

  const scheduler: HeartbeatScheduler = {
    setTimeout(handler: () => void, delayMs: number): number {
      const handle = nextHandle++;
      timers.push({ handle, handler, delayMs });
      return handle;
    },
    clearTimeout(handle: number): void {
      const idx = timers.findIndex((t) => t.handle === handle);
      if (idx !== -1) timers.splice(idx, 1);
    },
  };

  return {
    scheduler,
    tick: (): boolean => {
      if (timers.length === 0) return false;
      const next = timers.shift();
      if (next !== undefined) next.handler();
      return true;
    },
    pendingCount: (): number => timers.length,
    clearAll: (): void => {
      timers.length = 0;
    },
  };
}

// ─── Tests ───────────────────────────────────────────────────────────────────

describe("Heartbeat", () => {
  let state: SessionState;
  let sched: ReturnType<typeof makeScheduler>;
  let outboundFired: bigint[];
  let deadlineExceeded: number[];
  let callbacks: HeartbeatCallbacks;
  let _nowMs: () => number; // retained for symmetry; unused after the bigint nowNs wiring
  let currentMs: number;
  let heartbeat: Heartbeat;

  beforeEach(() => {
    state = new SessionState();
    sched = makeScheduler();
    outboundFired = [];
    deadlineExceeded = [];
    callbacks = {
      onOutboundDue: (clientNanos: bigint): void => {
        outboundFired.push(clientNanos);
      },
      onServerDeadlineExceeded: (millisSinceLastActivity: number): void => {
        deadlineExceeded.push(millisSinceLastActivity);
      },
    };
    currentMs = 1_000; // start at 1 s to avoid 0 edge cases
    _nowMs = (): number => currentMs;
    // Test injects nowNs as a precision-preserving derivative of nowMs;
    // the integer-ms test fixture means there's no precision to lose,
    // so the canonical formula and the lossy formula produce identical
    // bigint values in this scope.
    const nowNs = (): bigint => BigInt(currentMs) * 1_000_000n;
    heartbeat = new Heartbeat(state, callbacks, sched.scheduler, nowNs);
  });

  it("outboundHeartbeat_firesEveryClientIntervalMs_respectingChainedSetTimeout", () => {
    // Use default 10 000 ms interval
    state.clientHeartbeatIntervalMs = CLIENT_HEARTBEAT_INTERVAL_DEFAULT_MS;
    heartbeat.start();

    // First timer registered
    expect(sched.pendingCount()).toBe(1);

    // Fire the timer → outbound heartbeat + re-arm
    sched.tick();
    expect(outboundFired.length).toBe(1);

    // After firing, a new timer should be re-armed (chained setTimeout)
    expect(sched.pendingCount()).toBe(1);

    // Fire again → second heartbeat
    sched.tick();
    expect(outboundFired.length).toBe(2);

    // Each clientNanos should be a positive bigint
    for (const ns of outboundFired) {
      expect(ns).toBeGreaterThan(0n);
    }
  });

  it("serverDeadline_3xInterval_tripsCallback", () => {
    state.serverHeartbeatIntervalMs = SERVER_HEARTBEAT_INTERVAL_DEFAULT_MS; // 5000 ms
    // Deadline = 3 × 5000 = 15 000 ms
    const deadlineMs = 3 * SERVER_HEARTBEAT_INTERVAL_DEFAULT_MS;

    // Record activity at t=1000 ms
    const activityNs = BigInt(Math.floor(1_000 * 1_000_000));
    state.lastServerActivityNs = activityNs;

    // Check at t=1000 + deadlineMs - 1 (not yet exceeded)
    currentMs = 1_000 + deadlineMs - 1;
    let tripped = heartbeat.checkServerDeadline(activityNs);
    expect(tripped).toBe(false);
    expect(deadlineExceeded.length).toBe(0);

    // Check at t=1000 + deadlineMs + 1 (exceeded)
    currentMs = 1_000 + deadlineMs + 1;
    tripped = heartbeat.checkServerDeadline(activityNs);
    expect(tripped).toBe(true);
    expect(deadlineExceeded.length).toBe(1);
    expect(deadlineExceeded[0]).toBeGreaterThan(deadlineMs - 1);
  });

  it("serverDeadline_visibilityHidden_relaxesToMax3xOr60s", () => {
    // Set a short interval (5000 ms) → 3×interval = 15 000 ms < 60 000 ms
    // Hidden-tab deadline = max(15 000, 60 000) = 60 000 ms
    state.serverHeartbeatIntervalMs = 5_000;
    heartbeat.onVisibilityChange(false); // set hidden

    const activityNs = BigInt(Math.floor(1_000 * 1_000_000)); // t=1000 ms
    state.lastServerActivityNs = activityNs;

    // At 3×interval = 15 001 ms elapsed (would trip without hidden floor)
    currentMs = 1_000 + 15_001;
    const notTripped = heartbeat.checkServerDeadline(activityNs);
    expect(notTripped).toBe(false);

    // At hidden floor = 60 001 ms elapsed → should trip
    currentMs = 1_000 + HEARTBEAT_HIDDEN_FLOOR_MS + 1;
    const tripped = heartbeat.checkServerDeadline(activityNs);
    expect(tripped).toBe(true);
    expect(deadlineExceeded.length).toBe(1);
  });

  it("visibilityChange_visible_reAnchorsActivityAndForcesHeartbeat", () => {
    state.clientHeartbeatIntervalMs = CLIENT_HEARTBEAT_INTERVAL_DEFAULT_MS;
    heartbeat.start();

    // Simulate being hidden then becoming visible
    heartbeat.onVisibilityChange(false);
    currentMs = 5_000;
    heartbeat.onVisibilityChange(true);

    // Should fire an immediate outbound heartbeat
    expect(outboundFired.length).toBe(1);

    // `lastServerActivityNs` should be re-anchored to now
    const expectedNs = BigInt(Math.floor(5_000 * 1_000_000));
    expect(state.lastServerActivityNs).toBe(expectedNs);

    // Heartbeat isHidden should now be false
    expect(heartbeat.isHidden()).toBe(false);
  });

  it("degradedTimingMode_simulatedSafariRoundsTo1ms", () => {
    // Safari clamps performance.now() to 1 ms resolution.
    // `clientNanos` = BigInt(Math.floor(nowMs() * 1_000_000))
    // With 1 ms precision, nowMs() returns an integer → clientNanos % 1_000_000n === 0n
    state.clientHeartbeatIntervalMs = 1_000;
    // Set nowMs to return only whole-millisecond values (simulates Safari)
    currentMs = 10_000; // integer ms
    heartbeat.start();
    sched.tick();

    expect(outboundFired.length).toBe(1);
    const clientNanos = outboundFired[0];
    if (clientNanos !== undefined) {
      // With clamped clock returning integer ms, nanos must be divisible by 1_000_000
      expect(clientNanos % 1_000_000n).toBe(0n);
    }
  });

  it("start_then_stop_clearsTimer", () => {
    state.clientHeartbeatIntervalMs = CLIENT_HEARTBEAT_INTERVAL_DEFAULT_MS;
    heartbeat.start();
    expect(sched.pendingCount()).toBe(1);

    heartbeat.stop();
    expect(sched.pendingCount()).toBe(0);

    // No outbound heartbeats should have fired
    expect(outboundFired.length).toBe(0);
  });
});
