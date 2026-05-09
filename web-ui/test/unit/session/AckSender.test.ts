/**
 * AckSender.test.ts — unit tests for the frame-count + time-based ACK
 * trigger per APP-36 §2.9.
 *
 * Test naming follows `<unit>_<scenario>_<expected>` per plan §5.8.
 *
 * Threading: single-threaded (Vitest jsdom worker).
 */

import { describe, expect, it } from "vitest";
import { AckSender, type AckSenderCallbacks } from "@/workers/session/AckSender";
import { SessionState } from "@/workers/session/SessionState";
import {
  ACK_INTERVAL_FRAMES_NOMINAL,
  ACK_INTERVAL_FRAMES_BACKPRESSURE,
  ACK_INTERVAL_MS,
} from "@/workers/WorkerTuning";

// ─── Helpers ─────────────────────────────────────────────────────────────────

function makeAckSender(): {
  sender: AckSender;
  state: SessionState;
  acks: bigint[];
  nowNs: () => bigint;
  advanceMs: (ms: number) => void;
} {
  const state = new SessionState();
  const acks: bigint[] = [];
  let currentNs = 0n;

  const callbacks: AckSenderCallbacks = {
    onAckDue: (lastReliableSeqNo: bigint): void => {
      acks.push(lastReliableSeqNo);
    },
  };

  const nowNs = (): bigint => currentNs;
  const advanceMs = (ms: number): void => {
    currentNs += BigInt(ms) * 1_000_000n;
  };

  return { sender: new AckSender(state, callbacks, nowNs), state, acks, nowNs, advanceMs };
}

// ─── Tests ───────────────────────────────────────────────────────────────────

describe("AckSender", () => {
  it("framesReachedNominalThreshold_triggersAck", () => {
    const { sender, state, acks } = makeAckSender();

    // Default (no backpressure) threshold = ACK_INTERVAL_FRAMES_NOMINAL = 100
    state.lastReliableSeqNo = 100n;

    // Deliver ACK_INTERVAL_FRAMES_NOMINAL - 1 frames: should NOT ack yet
    for (let i = 0; i < ACK_INTERVAL_FRAMES_NOMINAL - 1; i++) {
      sender.onReliableFrameDelivered();
    }
    expect(acks.length).toBe(0);
    expect(sender.pendingFrameCount()).toBe(ACK_INTERVAL_FRAMES_NOMINAL - 1);

    // One more frame → triggers ACK
    sender.onReliableFrameDelivered();
    expect(acks.length).toBe(1);
    expect(acks[0]).toBe(100n);
    expect(sender.pendingFrameCount()).toBe(0);
  });

  it("framesReachedBackpressureThreshold_25_triggersAck", () => {
    const { sender, state, acks } = makeAckSender();

    sender.setBackpressure(true);
    state.lastReliableSeqNo = 25n;

    // Deliver ACK_INTERVAL_FRAMES_BACKPRESSURE - 1 frames: no ack
    for (let i = 0; i < ACK_INTERVAL_FRAMES_BACKPRESSURE - 1; i++) {
      sender.onReliableFrameDelivered();
    }
    expect(acks.length).toBe(0);

    // 25th frame → triggers ACK
    sender.onReliableFrameDelivered();
    expect(acks.length).toBe(1);
    expect(acks[0]).toBe(25n);
  });

  it("timeElapsedAckIntervalMs_triggersAckEvenWithFewerFrames", () => {
    const { sender, state, acks, advanceMs } = makeAckSender();

    // Only deliver a few frames (well below frame threshold)
    state.lastReliableSeqNo = 5n;
    for (let i = 0; i < 5; i++) {
      sender.onReliableFrameDelivered();
    }
    expect(acks.length).toBe(0);

    // Advance clock past ACK_INTERVAL_MS (250 ms)
    advanceMs(ACK_INTERVAL_MS + 1);

    // Deliver one more frame — should trigger ack via time check
    sender.onReliableFrameDelivered();
    expect(acks.length).toBe(1);
  });

  it("onTimerTick_noFramesPending_doesNotFlush", () => {
    const { sender, acks, advanceMs } = makeAckSender();

    // Advance past ACK_INTERVAL_MS with no frames delivered
    advanceMs(ACK_INTERVAL_MS + 1);
    sender.onTimerTick();

    expect(acks.length).toBe(0);
    expect(sender.pendingFrameCount()).toBe(0);
  });

  it("coldStart_resetsCounters", () => {
    const { sender, state, acks } = makeAckSender();

    // Deliver some frames
    state.lastReliableSeqNo = 10n;
    for (let i = 0; i < 10; i++) {
      sender.onReliableFrameDelivered();
    }
    expect(sender.pendingFrameCount()).toBe(10);

    // Cold start resets frame counter
    sender.coldStart();
    expect(sender.pendingFrameCount()).toBe(0);

    // No ack should have been sent during coldStart
    expect(acks.length).toBe(0);
  });
});
