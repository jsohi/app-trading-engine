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
  /** Invoked for each in-order reliable frame, including those released after gap-fill. */
  onInOrderFrame: (seqNo: bigint, payload: Uint8Array) => void;
  /** Invoked when a gap is first detected; caller encodes a WebSocketGapRequest frame. */
  onGapRequest: (ev: GapRequestEvent) => void;
  /** Invoked once when the byte-bounded buffer would overflow; tracker enters dead state. */
  onBufferOverflow: (bufferedBytes: number) => void;
}

/**
 * Reliable-stream gap tracker.
 *
 * Single-instance per session. State must be reset (`coldStart()`) on
 * a session-state cold-start.
 */
export class GapTracker {
  private readonly outOfOrder = new Map<bigint, Uint8Array>();
  private bufferedBytes = 0;
  private dead = false;
  private readonly state: SessionState;
  private readonly cb: GapTrackerCallbacks;

  constructor(state: SessionState, callbacks: GapTrackerCallbacks) {
    this.state = state;
    this.cb = callbacks;
  }

  /**
   * Process an inbound reliable frame. Returns true iff the frame was
   * accepted (delivered in-order or buffered for later release); false
   * iff the frame was dropped (duplicate / out-of-window) or the
   * tracker is dead.
   */
  onReliableFrame(seqNo: bigint, payload: Uint8Array): boolean {
    if (this.dead) return false;

    const expected = this.state.lastReliableSeqNo + 1n;
    if (seqNo <= this.state.lastReliableSeqNo) {
      // Duplicate / out-of-window → drop.
      return false;
    }

    if (seqNo === expected) {
      // In-order delivery.
      this.deliver(seqNo, payload);
      // Drain any buffered frames that are now in-order.
      this.drainBuffered();
      return true;
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
        return false;
      }

      this.outOfOrder.set(seqNo, copy);
      this.bufferedBytes = newBufferedBytes;
    }

    // Emit a GapRequest covering the missing interval. The server
    // replays only what is requested; we do not re-emit on every
    // subsequent out-of-order arrival to avoid storms — but since
    // a gap may contain multiple missing seqNos, the bounds here are
    // the full gap span at the moment of detection.
    this.cb.onGapRequest({ fromSeqNo: expected, toSeqNo: seqNo - 1n });
    return true;
  }

  /** Reset state on session-state cold-start. */
  coldStart(): void {
    this.outOfOrder.clear();
    this.bufferedBytes = 0;
    this.dead = false;
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

  private deliver(seqNo: bigint, payload: Uint8Array): void {
    this.state.lastReliableSeqNo = seqNo;
    this.cb.onInOrderFrame(seqNo, payload);
  }

  private drainBuffered(): void {
    // Release contiguous in-order frames from the buffer.
    while (!this.dead) {
      const next = this.state.lastReliableSeqNo + 1n;
      const buffered = this.outOfOrder.get(next);
      if (buffered === undefined) return;
      this.outOfOrder.delete(next);
      this.bufferedBytes -= buffered.length;
      this.deliver(next, buffered);
    }
  }
}
