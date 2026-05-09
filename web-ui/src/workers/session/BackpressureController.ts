/**
 * BackpressureController — two-sided hysteresis for the BACKPRESSURE
 * connection-state per APP-36 §2.9.
 *
 * Entry triggers (any one):
 *   - `bufferedAmount > BACKPRESSURE_ENTER_BUFFERED_BYTES` (1 MiB)
 *   - inbound `WebSocketError{SlowConsumer=9}` from server
 *
 * Exit guards (ALL of):
 *   - `bufferedAmount < BACKPRESSURE_EXIT_BUFFERED_BYTES` (256 KiB)
 *   - `now - lastSlowConsumerNs >= BACKPRESSURE_EXIT_QUIET_MS` (60 s)
 *   - no pending BACKPRESSURE-eligible code in window
 *
 * Flap-rate cap: ≤ 1 transition per `BACKPRESSURE_FLAP_CAP_MS` (30 s).
 *
 * Side effects on entry: `AckSender.setBackpressure(true)` so client
 * acks 4× faster (server can advance watermark and free buffer faster);
 * caller widens price-stream throttle and shrinks event-log ring.
 *
 * Threading: worker scope only. Timer is chained `setTimeout` driven
 * by `performance.now()` deadlines (HTML §9.5.2 background-tab
 * throttle is fine — the deadline check fires regardless).
 *
 * Allocation: zero per tick.
 *
 * Plan reference: §2.9 / §6 row 24.
 */

import {
  BACKPRESSURE_ENTER_BUFFERED_BYTES,
  BACKPRESSURE_EXIT_BUFFERED_BYTES,
  BACKPRESSURE_EXIT_QUIET_MS,
  BACKPRESSURE_FLAP_CAP_MS,
  BACKPRESSURE_POLL_MS,
} from "@/workers/WorkerTuning";

export interface BackpressureCallbacks {
  /** Invoked when state transitions enter BACKPRESSURE. */
  onEnter: (source: "server" | "local-buffered-amount") => void;
  /** Invoked when state transitions exit BACKPRESSURE → CONNECTED. */
  onExit: () => void;
}

export interface BackpressureScheduler {
  setTimeout(handler: () => void, delayMs: number): number;
  clearTimeout(handle: number): void;
}

/** Caller passes a snapshot of `WebSocket.bufferedAmount` on each tick. */
export type BufferedAmountSampler = () => number;

export class BackpressureController {
  private inBackpressure = false;
  private lastTransitionAtMs = 0;
  private lastSlowConsumerAtMs = 0;
  private timerHandle: number | null = null;
  private readonly cb: BackpressureCallbacks;
  private readonly sampler: BufferedAmountSampler;
  private readonly sched: BackpressureScheduler;
  private readonly nowMs: () => number;

  constructor(
    callbacks: BackpressureCallbacks,
    sampler: BufferedAmountSampler,
    scheduler: BackpressureScheduler,
    nowMs: () => number,
  ) {
    this.cb = callbacks;
    this.sampler = sampler;
    this.sched = scheduler;
    this.nowMs = nowMs;
  }

  /** Start the polling timer. */
  start(): void {
    this.armTimer();
  }

  /** Stop the polling timer. */
  stop(): void {
    if (this.timerHandle !== null) {
      this.sched.clearTimeout(this.timerHandle);
      this.timerHandle = null;
    }
  }

  /** Server sent WebSocketError(SlowConsumer=9). May trigger entry. */
  onSlowConsumerSignal(): void {
    this.lastSlowConsumerAtMs = this.nowMs();
    if (!this.inBackpressure) {
      this.tryEnter("server");
    }
  }

  /**
   * Visible for tests + `worker.ts` integration: forces a single
   * sample-and-decide cycle right now (without rearming the timer).
   */
  poll(): void {
    this.sampleAndDecide();
  }

  /** Visible for tests. */
  isInBackpressure(): boolean {
    return this.inBackpressure;
  }

  private armTimer(): void {
    if (this.timerHandle !== null) {
      this.sched.clearTimeout(this.timerHandle);
    }
    this.timerHandle = this.sched.setTimeout(() => {
      // Per Gemini review (MEDIUM): if `stop()` cleared `timerHandle` while
      // this callback was queued/executing, exit early so we don't re-arm
      // the timer and resurrect the polling loop.
      if (this.timerHandle === null) return;
      this.sampleAndDecide();
      this.armTimer();
    }, BACKPRESSURE_POLL_MS);
  }

  private sampleAndDecide(): void {
    const bufferedAmount = this.sampler();
    if (this.inBackpressure) {
      this.tryExit(bufferedAmount);
    } else if (bufferedAmount > BACKPRESSURE_ENTER_BUFFERED_BYTES) {
      this.tryEnter("local-buffered-amount");
    }
  }

  private tryEnter(source: "server" | "local-buffered-amount"): void {
    const now = this.nowMs();
    if (now - this.lastTransitionAtMs < BACKPRESSURE_FLAP_CAP_MS) {
      // Flap cap — refuse the transition (caller stays in current state).
      return;
    }
    this.inBackpressure = true;
    this.lastTransitionAtMs = now;
    this.cb.onEnter(source);
  }

  private tryExit(bufferedAmount: number): void {
    if (bufferedAmount >= BACKPRESSURE_EXIT_BUFFERED_BYTES) return;
    const now = this.nowMs();
    if (now - this.lastSlowConsumerAtMs < BACKPRESSURE_EXIT_QUIET_MS) return;
    if (now - this.lastTransitionAtMs < BACKPRESSURE_FLAP_CAP_MS) return;
    this.inBackpressure = false;
    this.lastTransitionAtMs = now;
    this.cb.onExit();
  }
}
