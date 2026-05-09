/**
 * GapTracker.test.ts — unit tests for the reliable-stream gap detection
 * and bounded out-of-order buffer per APP-36 §2.7.
 *
 * Test naming follows `<unit>_<scenario>_<expected>` per plan §5.8.
 *
 * Threading: single-threaded (Vitest jsdom worker).
 * Allocation: test-only — Uint8Array payloads created in helpers.
 */

import { describe, expect, it } from "vitest";
import {
  GapTracker,
  type GapTrackerCallbacks,
  type GapRequestEvent,
} from "@/workers/session/GapTracker";
import { FLAG_RELIABLE } from "@/workers/frame/Flags";
import { SessionState } from "@/workers/session/SessionState";
import { MAX_GAP_BUFFER_BYTES } from "@/workers/WorkerTuning";

// ─── Helpers ─────────────────────────────────────────────────────────────────

function makePayload(len: number): Uint8Array {
  return Uint8Array.from({ length: len }, (_, i) => i & 0xff);
}

function makeTracker(): {
  tracker: GapTracker;
  state: SessionState;
  delivered: Array<{ seqNo: bigint; payload: Uint8Array }>;
  gapRequests: GapRequestEvent[];
  overflows: number[];
} {
  const state = new SessionState();
  const delivered: Array<{ seqNo: bigint; payload: Uint8Array }> = [];
  const gapRequests: GapRequestEvent[] = [];
  const overflows: number[] = [];

  const callbacks: GapTrackerCallbacks = {
    onInOrderFrame: (seqNo: bigint, _flags: number, payload: Uint8Array): void => {
      delivered.push({ seqNo, payload: payload.slice() });
    },
    onGapRequest: (ev: GapRequestEvent): void => {
      gapRequests.push({ fromSeqNo: ev.fromSeqNo, toSeqNo: ev.toSeqNo });
    },
    onBufferOverflow: (bufferedBytes: number): void => {
      overflows.push(bufferedBytes);
    },
  };

  return { tracker: new GapTracker(state, callbacks), state, delivered, gapRequests, overflows };
}

// ─── Tests ───────────────────────────────────────────────────────────────────

describe("GapTracker", () => {
  it("inOrderFrame_advancesLastReliableSeqNo", () => {
    const { tracker, state, delivered } = makeTracker();

    const payload = makePayload(16);
    const accepted = tracker.onReliableFrame(1n, FLAG_RELIABLE, payload);

    expect(accepted).toBe("DELIVERED");
    expect(state.lastReliableSeqNo).toBe(1n);
    expect(delivered.length).toBe(1);
    expect(delivered[0]?.seqNo).toBe(1n);
  });

  it("gap_emitsGapRequestForMissingInterval", () => {
    const { tracker, state, gapRequests } = makeTracker();

    // Deliver seqNo=1 (in-order)
    tracker.onReliableFrame(1n, FLAG_RELIABLE, makePayload(8));
    expect(state.lastReliableSeqNo).toBe(1n);

    // Deliver seqNo=4 (gap: 2 and 3 are missing)
    const accepted = tracker.onReliableFrame(4n, FLAG_RELIABLE, makePayload(8));
    expect(accepted).toBe("BUFFERED");

    // Should emit a gap request covering [2, 3]
    expect(gapRequests.length).toBe(1);
    expect(gapRequests[0]?.fromSeqNo).toBe(2n);
    expect(gapRequests[0]?.toSeqNo).toBe(3n);
  });

  it("outOfOrderFrame_buffered_thenReleasedOnGapFill", () => {
    const { tracker, state, delivered, gapRequests } = makeTracker();

    // seqNo=1 arrives in order
    tracker.onReliableFrame(1n, FLAG_RELIABLE, makePayload(4));
    expect(state.lastReliableSeqNo).toBe(1n);

    // seqNo=3 arrives out-of-order (gap at seqNo=2)
    tracker.onReliableFrame(3n, FLAG_RELIABLE, makePayload(4));
    expect(state.lastReliableSeqNo).toBe(1n); // still stuck at 1
    expect(gapRequests.length).toBe(1);

    // seqNo=2 arrives (gap fill) → should release 2 and 3
    tracker.onReliableFrame(2n, FLAG_RELIABLE, makePayload(4));
    expect(state.lastReliableSeqNo).toBe(3n);
    expect(delivered.length).toBe(3);
    expect(delivered[1]?.seqNo).toBe(2n);
    expect(delivered[2]?.seqNo).toBe(3n);
  });

  it("duplicateFrame_droppedSilently", () => {
    const { tracker, state, delivered, gapRequests } = makeTracker();

    tracker.onReliableFrame(1n, FLAG_RELIABLE, makePayload(8));
    expect(state.lastReliableSeqNo).toBe(1n);

    // Same seqNo again
    const dup = tracker.onReliableFrame(1n, FLAG_RELIABLE, makePayload(8));
    expect(dup).toBe("DROPPED");
    expect(delivered.length).toBe(1); // only the first
    expect(gapRequests.length).toBe(0);
  });

  it("bufferOverBytes_callsOnBufferOverflow_thenDead", () => {
    const { tracker, overflows } = makeTracker();

    // Deliver seqNo=1 in order
    tracker.onReliableFrame(1n, FLAG_RELIABLE, makePayload(8));

    // Now send a frame that would push buffer over MAX_GAP_BUFFER_BYTES.
    // We create a gap at seqNo=3 and give seqNo=3 a massive payload.
    // MAX_GAP_BUFFER_BYTES = 16 MiB. We need one chunk that exceeds it.
    const overflowPayload = new Uint8Array(MAX_GAP_BUFFER_BYTES + 1);
    tracker.onReliableFrame(3n, FLAG_RELIABLE, overflowPayload);

    expect(overflows.length).toBe(1);
    expect(tracker.isDead()).toBe(true);

    // Subsequent frames should be rejected
    const rejected = tracker.onReliableFrame(2n, FLAG_RELIABLE, makePayload(4));
    expect(rejected).toBe("DROPPED");
  });

  it("coldStart_clearsBuffer", () => {
    const { tracker, state, gapRequests } = makeTracker();

    tracker.onReliableFrame(1n, FLAG_RELIABLE, makePayload(8));
    // Create a gap (seqNo=3 with gap at seqNo=2)
    tracker.onReliableFrame(3n, FLAG_RELIABLE, makePayload(8));
    expect(gapRequests.length).toBe(1);
    expect(tracker.bufferedFrameCount()).toBe(1);

    // Cold start resets everything
    tracker.coldStart();
    expect(tracker.bufferedFrameCount()).toBe(0);
    expect(tracker.bufferedByteCount()).toBe(0);
    expect(tracker.isDead()).toBe(false);
    expect(state.lastReliableSeqNo).toBe(1n); // state.lastReliableSeqNo unchanged (coldStart is on tracker)
  });

  it("floodOfDuplicates_OConstant_perFrame", () => {
    const { tracker } = makeTracker();

    // Deliver seqNo=1 in order
    tracker.onReliableFrame(1n, FLAG_RELIABLE, makePayload(8));

    // Flood of 1000 duplicates with seqNo=1
    for (let i = 0; i < 1000; i++) {
      tracker.onReliableFrame(1n, FLAG_RELIABLE, makePayload(8));
    }

    // Buffer should remain empty (all duplicates dropped)
    expect(tracker.bufferedFrameCount()).toBe(0);
    expect(tracker.bufferedByteCount()).toBe(0);
  });
});
