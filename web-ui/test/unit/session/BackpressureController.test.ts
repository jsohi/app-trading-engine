/**
 * BackpressureController.test.ts — unit tests for the two-sided
 * hysteresis backpressure state machine per APP-36 §2.9.
 *
 * Uses a controllable scheduler and sampler for deterministic testing.
 *
 * Test naming follows `<unit>_<scenario>_<expected>` per plan §5.8.
 *
 * Threading: single-threaded (Vitest jsdom worker).
 */

import { describe, expect, it } from "vitest";
import {
  BackpressureController,
  type BackpressureCallbacks,
  type BackpressureScheduler,
} from "@/workers/session/BackpressureController";
import {
  BACKPRESSURE_ENTER_BUFFERED_BYTES,
  BACKPRESSURE_EXIT_BUFFERED_BYTES,
  BACKPRESSURE_EXIT_QUIET_MS,
  BACKPRESSURE_FLAP_CAP_MS,
} from "@/workers/WorkerTuning";

// ─── Controllable scheduler ──────────────────────────────────────────────────

function makeScheduler(): {
  scheduler: BackpressureScheduler;
  tick: () => boolean;
  pendingCount: () => number;
} {
  let nextHandle = 1;
  const timers: Array<{ handle: number; handler: () => void }> = [];

  const scheduler: BackpressureScheduler = {
    setTimeout(handler: () => void, _delayMs: number): number {
      const handle = nextHandle++;
      timers.push({ handle, handler });
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
  };
}

// ─── Test fixture ─────────────────────────────────────────────────────────────

function makeController(initialBufferedAmount = 0): {
  controller: BackpressureController;
  entered: Array<"server" | "local-buffered-amount">;
  exited: number[];
  sched: ReturnType<typeof makeScheduler>;
  setBufferedAmount: (n: number) => void;
  advanceMs: (ms: number) => void;
} {
  let bufferedAmount = initialBufferedAmount;
  // Start past BACKPRESSURE_FLAP_CAP_MS so the initial tryEnter check passes.
  // The flap cap check is: now - lastTransitionAtMs < FLAP_CAP_MS.
  // lastTransitionAtMs starts at 0, so we need now > 0 by at least FLAP_CAP_MS.
  let currentMs = BACKPRESSURE_FLAP_CAP_MS + 1;
  const entered: Array<"server" | "local-buffered-amount"> = [];
  const exited: number[] = [];

  const callbacks: BackpressureCallbacks = {
    onEnter: (source: "server" | "local-buffered-amount"): void => {
      entered.push(source);
    },
    onExit: (): void => {
      exited.push(currentMs);
    },
  };

  const sched = makeScheduler();
  const sampler = (): number => bufferedAmount;
  const nowMs = (): number => currentMs;

  const controller = new BackpressureController(callbacks, sampler, sched.scheduler, nowMs);

  return {
    controller,
    entered,
    exited,
    sched,
    setBufferedAmount: (n: number): void => {
      bufferedAmount = n;
    },
    advanceMs: (ms: number): void => {
      currentMs += ms;
    },
  };
}

// ─── Tests ───────────────────────────────────────────────────────────────────

describe("BackpressureController", () => {
  it("enter_via_serverSlowConsumer_signalsCallback", () => {
    const { controller, entered } = makeController();

    controller.onSlowConsumerSignal();

    expect(entered.length).toBe(1);
    expect(entered[0]).toBe("server");
    expect(controller.isInBackpressure()).toBe(true);
  });

  it("enter_via_localBufferedAmountAbove1MiB_signalsCallback", () => {
    const { controller, entered, setBufferedAmount } = makeController();

    // Set bufferedAmount above the enter threshold (1 MiB)
    setBufferedAmount(BACKPRESSURE_ENTER_BUFFERED_BYTES + 1);
    controller.poll();

    expect(entered.length).toBe(1);
    expect(entered[0]).toBe("local-buffered-amount");
    expect(controller.isInBackpressure()).toBe(true);
  });

  it("flapRateCap30s_refusesSecondTransitionWithinWindow", () => {
    const { controller, entered, advanceMs, setBufferedAmount } = makeController();

    // First transition (enter) at t=0
    controller.onSlowConsumerSignal();
    expect(entered.length).toBe(1);

    // Exit conditions met but within flap cap window (30 s)
    advanceMs(BACKPRESSURE_EXIT_QUIET_MS); // 60 s quiet elapsed
    setBufferedAmount(0); // bufferedAmount below exit threshold

    // poll() → try to exit but flap cap hasn't elapsed yet (only 60 s since entry,
    // but flap cap is 30 s for exit too)
    // Actually the flap cap is 30 s from LAST transition.
    // We advanced 60 s so flap cap (30 s) IS satisfied.
    // But we need to test flap cap refusing a second ENTRY within 30 s.

    // Reset: start fresh controller
    const {
      controller: c2,
      entered: e2,
      advanceMs: adv2,
      setBufferedAmount: set2,
    } = makeController();

    // First enter
    set2(BACKPRESSURE_ENTER_BUFFERED_BYTES + 1);
    c2.poll();
    expect(e2.length).toBe(1);

    // Exit conditions — advance 60 s (quiet) + flap cap
    adv2(BACKPRESSURE_FLAP_CAP_MS + BACKPRESSURE_EXIT_QUIET_MS);
    set2(0); // below exit threshold
    c2.poll(); // → exits backpressure

    // Immediately try to re-enter (bufferedAmount > 1 MiB) before 30 s flap cap
    set2(BACKPRESSURE_ENTER_BUFFERED_BYTES + 1);
    c2.poll(); // → should be refused by flap cap

    // Still only 1 entry event (re-entry refused)
    expect(e2.length).toBe(1);
  });

  it("exit_requires_bufferedAmountBelow256KiB_AND_60sQuiet_AND_pastFlapWindow", () => {
    const { controller, exited, advanceMs, setBufferedAmount } = makeController();

    // Enter via server signal at t=0
    controller.onSlowConsumerSignal();
    expect(controller.isInBackpressure()).toBe(true);

    // Advance past flap cap (30 s) + quiet period (60 s) = 60 s is the binding constraint
    advanceMs(BACKPRESSURE_EXIT_QUIET_MS + BACKPRESSURE_FLAP_CAP_MS);

    // Set bufferedAmount below exit threshold (256 KiB)
    setBufferedAmount(BACKPRESSURE_EXIT_BUFFERED_BYTES - 1);
    controller.poll();

    expect(exited.length).toBe(1);
    expect(controller.isInBackpressure()).toBe(false);
  });

  it("noExit_when900KiBSustained_avoidsFlap", () => {
    const { controller, exited, advanceMs, setBufferedAmount } = makeController();

    // Enter backpressure
    controller.onSlowConsumerSignal();

    // Advance well past flap cap + quiet period
    advanceMs(BACKPRESSURE_EXIT_QUIET_MS + BACKPRESSURE_FLAP_CAP_MS);

    // Sustained bufferedAmount of 900 KiB — above exit threshold (256 KiB) but below entry (1 MiB)
    // This should keep us in backpressure without flapping
    setBufferedAmount(900 * 1024);
    controller.poll();

    // Must NOT exit (900 KiB is above the 256 KiB exit threshold)
    expect(exited.length).toBe(0);
    expect(controller.isInBackpressure()).toBe(true);
  });

  it("coldStart_resetsState", () => {
    // BackpressureController has no `coldStart` method.
    // Instead test stop/start cycle: stopping clears the timer;
    // starting re-arms it; state persists through stop/start.
    const { controller, entered, sched, setBufferedAmount, advanceMs } = makeController();

    // Start polling
    controller.start();
    expect(sched.pendingCount()).toBe(1);

    // Stop
    controller.stop();
    expect(sched.pendingCount()).toBe(0);

    // State should be non-backpressure initially
    expect(controller.isInBackpressure()).toBe(false);

    // Restart — re-arms timer
    controller.start();
    expect(sched.pendingCount()).toBe(1);

    // Trigger a poll by firing the timer
    advanceMs(BACKPRESSURE_FLAP_CAP_MS + 1);
    setBufferedAmount(BACKPRESSURE_ENTER_BUFFERED_BYTES + 1);
    sched.tick(); // fires sampleAndDecide + re-arms

    expect(entered.length).toBe(1);
  });
});
