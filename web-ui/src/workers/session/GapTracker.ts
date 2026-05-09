/**
 * GapTracker — reliable-stream gap detection + bounded out-of-order buffer.
 *
 * For every reliable frame, expects `seqNo === lastReliableSeqNo + 1n`.
 * On a gap (seqNo > expected) emits a `WebSocketGapRequest` via the
 * caller-supplied `onGapRequest` and buffers the early frame in a
 * byte-bounded Map keyed by seqNo. Duplicates (`seqNo <= expected`)
 * are dropped silently. On gap-buffer overflow (over
 * `MAX_GAP_BUFFER_BYTES`) the caller-supplied `onBufferOverflow`
 * fires and the tracker enters dead state.
 *
 * Replay frames (FLAG_REPLAY) follow the same rules: a server replay
 * fills missing gaps in order; the tracker treats them indistinguishably
 * from live reliable frames once gap-fill completes.
 *
 * Threading: worker scope only.
 *
 * Allocation: per gap occurrence, one Map entry + one Uint8Array copy
 * of the buffered payload (the parser's view is invalidated after
 * onFrame returns, so we must copy). Cold path under steady state
 * (no gaps).
 *
 * Plan reference: §2.7 / §6 row 11.
 */

import { MAX_GAP_BUFFER_BYTES } from "@/workers/WorkerTuning";
import { type SessionState } from "@/workers/session/SessionState";

export interface GapRequestEvent {
  readonly fromSeqNo: bigint;
  readonly toSeqNo: bigint;
}

export interface GapTrackerCallbacks {
  /**
   * Invoked for each in-order reliable frame, including those released
   * after a gap-fill replay. The `flags` byte is the original envelope
   * `flags` (FLAG_RELIABLE + optional FLAG_REPLAY / FLAG_SNAPSHOT bits),
   * preserved so the caller can route correctly post-buffer.
   */
  onInOrderFrame: (seqNo: bigint, flags: number, payload: Uint8Array) => void;
  /** Invoked when a gap is first detected; caller encodes a WebSocketGapRequest frame. */
  onGapRequest: (ev: GapRequestEvent) => void;
  /** Invoked once when the byte-bounded buffer would overflow; tracker enters dead state. */
  onBufferOverflow: (bufferedBytes: number) => void;
}

/**
 * Result of `onReliableFrame`. Distinguishes delivered-now from
 * buffered-for-later so the caller does not double-route an
 * out-of-order frame (Gemini review HIGH).
 */
export type GapAcceptResult = "DELIVERED" | "BUFFERED" | "DROPPED";

interface BufferedFrame {
  readonly flags: number;
  readonly payload: Uint8Array;
}

/**
 * Reliable-stream gap tracker.
 *
 * Single-instance per session. State must be reset (`coldStart()`) on
 * a session-state cold-start.
 */
export class GapTracker {
  private readonly outOfOrder = new Map<bigint, BufferedFrame>();
  private bufferedBytes = 0;
  private dead = false;
  // Highest `toSeqNo` already requested. Suppresses GapRequest storms
  // when subsequent out-of-order frames arrive in the same gap window:
  // the server is already replaying [fromSeqNo, highestRequestedSeqNo],
  // so re-asking for a sub-interval is wasted bandwidth and may trip
  // server-side rate limits. Reset on `coldStart()`.
  private highestRequestedSeqNo = 0n;
  private readonly state: SessionState;
  private readonly cb: GapTrackerCallbacks;

  constructor(state: SessionState, callbacks: GapTrackerCallbacks) {
    this.state = state;
    this.cb = callbacks;
  }

  /**
   * Process an inbound reliable frame. Returns one of:
   *   - `"DELIVERED"` — frame was emitted via `onInOrderFrame` immediately
   *     (and any newly contiguous buffered frames were drained too).
   *   - `"BUFFERED"` — frame was buffered for later release; the caller
   *     MUST NOT route it (per Gemini review HIGH; the prior boolean
   *     contract caused out-of-order frames to be double-routed).
   *   - `"DROPPED"` — duplicate, out-of-window, or tracker is dead.
   */
  onReliableFrame(seqNo: bigint, flags: number, payload: Uint8Array): GapAcceptResult {
    if (this.dead) return "DROPPED";

    const expected = this.state.lastReliableSeqNo + 1n;
    if (seqNo <= this.state.lastReliableSeqNo) {
      // Duplicate / out-of-window → drop.
      return "DROPPED";
    }

    if (seqNo === expected) {
      // In-order delivery.
      this.deliver(seqNo, flags, payload);
      // Drain any buffered frames that are now in-order.
      this.drainBuffered();
      return "DELIVERED";
    }

    // Gap detected (seqNo > expected). Buffer + emit a single GapRequest
    // covering [expected, seqNo - 1n].
    if (!this.outOfOrder.has(seqNo)) {
      // Copy payload — the parser's view is invalidated on the next feed().
      const copy = new Uint8Array(payload.length);
      copy.set(payload);

      const newBufferedBytes = this.bufferedBytes + copy.length;
      if (newBufferedBytes > MAX_GAP_BUFFER_BYTES) {
        this.dead = true;
        this.cb.onBufferOverflow(newBufferedBytes);
        return "DROPPED";
      }

      this.outOfOrder.set(seqNo, { flags, payload: copy });
      this.bufferedBytes = newBufferedBytes;
    }

    // Per Gemini review (MEDIUM): GapRequest storm defense. Only emit a
    // new request when the gap window has *grown* beyond what we have
    // already asked the server to replay. If `seqNo - 1n` is at or below
    // `highestRequestedSeqNo`, the server is already replaying that
    // span; re-asking wastes bandwidth and trips rate limits. Reset on
    // `coldStart()` (the new connection has nothing in flight).
    const toSeqNo = seqNo - 1n;
    if (toSeqNo > this.highestRequestedSeqNo) {
      this.highestRequestedSeqNo = toSeqNo;
      this.cb.onGapRequest({ fromSeqNo: expected, toSeqNo });
    }
    return "BUFFERED";
  }

  /** Reset state on session-state cold-start. */
  coldStart(): void {
    this.outOfOrder.clear();
    this.bufferedBytes = 0;
    this.dead = false;
    this.highestRequestedSeqNo = 0n;
  }

  /** Visible for tests + watchdog assertions. */
  bufferedFrameCount(): number {
    return this.outOfOrder.size;
  }

  /** Visible for tests. */
  bufferedByteCount(): number {
    return this.bufferedBytes;
  }

  /** Visible for tests. */
  isDead(): boolean {
    return this.dead;
  }

  private deliver(seqNo: bigint, flags: number, payload: Uint8Array): void {
    this.state.lastReliableSeqNo = seqNo;
    this.cb.onInOrderFrame(seqNo, flags, payload);
  }

  private drainBuffered(): void {
    // Release contiguous in-order frames from the buffer.
    while (!this.dead) {
      const next = this.state.lastReliableSeqNo + 1n;
      const buffered = this.outOfOrder.get(next);
      if (buffered === undefined) return;
      this.outOfOrder.delete(next);
      this.bufferedBytes -= buffered.payload.length;
      this.deliver(next, buffered.flags, buffered.payload);
    }
  }
}
