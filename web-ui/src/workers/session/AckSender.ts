/**
 * AckSender — emits ClientAck (template 71) on a frame-count OR time
 * trigger, whichever fires first.
 *
 * Cadence per §2.9:
 *   - Steady state: every `ACK_INTERVAL_FRAMES_NOMINAL` reliable frames
 *     OR every `ACK_INTERVAL_MS` ms.
 *   - BACKPRESSURE state: every `ACK_INTERVAL_FRAMES_BACKPRESSURE` frames
 *     (4× faster) so the server can advance its watermark and free
 *     buffer faster. Time trigger unchanged.
 *
 * The ack carries the highest seqNo we've delivered in-order to the
 * dispatcher, taken from `SessionState.lastReliableSeqNo`.
 *
 * Threading: worker scope only.
 *
 * Allocation: zero per ack-trigger decision; the actual ack frame
 * encoding is a single `encodeBestEffort` allocation by the caller
 * who handles the wire write.
 *
 * Plan reference: §2.9 / §6 row 25.
 */

import {
  ACK_INTERVAL_FRAMES_BACKPRESSURE,
  ACK_INTERVAL_FRAMES_NOMINAL,
  ACK_INTERVAL_MS,
} from "@/workers/WorkerTuning";
import { type SessionState } from "@/workers/session/SessionState";

export interface AckSenderCallbacks {
  /** Invoked when an ack is due; caller encodes + sends the ClientAck frame for `lastReliableSeqNo`. */
  onAckDue: (lastReliableSeqNo: bigint) => void;
}

export class AckSender {
  private framesSinceLastAck = 0;
  private lastAckAtNs: bigint;
  private backpressure = false;
  private readonly state: SessionState;
  private readonly cb: AckSenderCallbacks;
  private readonly nowNs: () => bigint;

  constructor(state: SessionState, callbacks: AckSenderCallbacks, nowNs: () => bigint) {
    this.state = state;
    this.cb = callbacks;
    this.nowNs = nowNs;
    this.lastAckAtNs = nowNs();
  }

  /**
   * Notify the sender that one in-order reliable frame was just delivered.
   * May synchronously trigger an ack via the `onAckDue` callback.
   */
  onReliableFrameDelivered(): void {
    this.framesSinceLastAck += 1;
    if (this.framesSinceLastAck >= this.frameThreshold()) {
      this.flush();
      return;
    }
    const now = this.nowNs();
    const elapsedMs = (now - this.lastAckAtNs) / 1_000_000n;
    if (elapsedMs >= BigInt(ACK_INTERVAL_MS)) {
      this.flush();
    }
  }

  /**
   * Tick from the heartbeat / backpressure timer — gives the time-based
   * trigger a chance to fire even when no inbound frames arrive.
   */
  onTimerTick(): void {
    if (this.framesSinceLastAck === 0) return; // nothing to ack
    const now = this.nowNs();
    const elapsedMs = (now - this.lastAckAtNs) / 1_000_000n;
    if (elapsedMs >= BigInt(ACK_INTERVAL_MS)) {
      this.flush();
    }
  }

  /** Update BACKPRESSURE state — drops frame threshold from 100 → 25 per §2.9. */
  setBackpressure(enabled: boolean): void {
    this.backpressure = enabled;
  }

  /** Reset on session cold-start. */
  coldStart(): void {
    this.framesSinceLastAck = 0;
    this.lastAckAtNs = this.nowNs();
    this.backpressure = false;
  }

  /** Visible for tests. */
  pendingFrameCount(): number {
    return this.framesSinceLastAck;
  }

  private frameThreshold(): number {
    return this.backpressure ? ACK_INTERVAL_FRAMES_BACKPRESSURE : ACK_INTERVAL_FRAMES_NOMINAL;
  }

  private flush(): void {
    this.cb.onAckDue(this.state.lastReliableSeqNo);
    this.framesSinceLastAck = 0;
    this.lastAckAtNs = this.nowNs();
  }
}
