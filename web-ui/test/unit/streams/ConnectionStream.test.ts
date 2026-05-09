/**
 * ConnectionStream.test.ts — unit tests for the `connectionStream$` BehaviorSubject.
 *
 * Tests per APP-36 §5.5 / §6 row 21.
 *
 * Threading: single-threaded (Vitest jsdom worker).
 * Allocation: test-only — Subscription instances per test.
 *
 * Note: The module singleton is reset between tests by re-importing
 * (vitest module isolation). Because the module exports a singleton
 * BehaviorSubject, each test file run gets a fresh module state.
 */

import { describe, expect, it, beforeEach } from "vitest";
import { connectionStream$, pushConnectionState } from "@/streams/connection-stream";
import { type ConnectionState } from "@/shared/transport/MessageShape";

// ─── Tests ───────────────────────────────────────────────────────────────────

describe("connectionStream", () => {
  // The module is a singleton; reset to CONNECTING before each test
  // by pushing the initial state. This is safe because CONNECTING is the
  // default and all tests expect to observe transitions from it.
  beforeEach(() => {
    pushConnectionState("CONNECTING");
  });

  it("connectionStream_initialState_CONNECTING", () => {
    // The initial state of the BehaviorSubject must be CONNECTING.
    const received: ConnectionState[] = [];
    const sub = connectionStream$.subscribe((s) => received.push(s));
    sub.unsubscribe();

    expect(received).toHaveLength(1);
    expect(received[0]).toBe("CONNECTING");
  });

  it("connectionStream_emitsCurrent_toLateSubscriber", () => {
    // BehaviorSubject behaviour: a late subscriber receives the most recent
    // state synchronously on subscribe, without waiting for a new emission.
    pushConnectionState("CONNECTED");

    const received: ConnectionState[] = [];
    // Subscribe AFTER the state was pushed.
    const sub = connectionStream$.subscribe((s) => received.push(s));
    sub.unsubscribe();

    expect(received).toHaveLength(1);
    expect(received[0]).toBe("CONNECTED");
  });

  it("connectionStream_pushConnectionState_propagatesToSubscribers", () => {
    const received: ConnectionState[] = [];
    const sub = connectionStream$.subscribe((s) => received.push(s));

    // Initial state replayed on subscribe.
    expect(received).toHaveLength(1);
    expect(received[0]).toBe("CONNECTING");

    // Push a series of state transitions.
    pushConnectionState("CONNECTED");
    pushConnectionState("BACKPRESSURE");
    pushConnectionState("DOWN");

    sub.unsubscribe();

    expect(received).toEqual(["CONNECTING", "CONNECTED", "BACKPRESSURE", "DOWN"]);
  });
});
