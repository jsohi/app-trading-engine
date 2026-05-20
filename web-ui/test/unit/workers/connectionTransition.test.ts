/**
 * Unit tests for {@code nextConnectionState} — the pure dedupe predicate
 * extracted from {@code worker.ts:transitionConnection}.
 *
 * <p>The worker's `ws.onerror` handler pushes `RECONNECTING` defensively
 * because some browsers (Firefox) fire `error` BEFORE `close`. The
 * subsequent `ws.onclose` handler also pushes `RECONNECTING` on the
 * auto-recoverable branch. These tests pin the invariant that the
 * second push is a no-op (no duplicate `connection-state` envelope on
 * the message stream), preventing main-thread recorders / UI subscribers
 * from observing spurious duplicates.
 *
 * Threading: single-threaded Vitest harness.
 * Allocation: zero — pure function returning either the input reference
 *             or {@code null}.
 */

import { describe, it, expect } from "vitest";

import { nextConnectionState } from "@/workers/protocol/connectionTransition";

describe("nextConnectionState", () => {
  it("returns the new state on a genuine transition", () => {
    expect(nextConnectionState("CONNECTING", "CONNECTED")).toBe("CONNECTED");
    expect(nextConnectionState("CONNECTED", "RECONNECTING")).toBe("RECONNECTING");
    expect(nextConnectionState("RECONNECTING", "CONNECTED")).toBe("CONNECTED");
  });

  it("returns null when proposed === current (same-state dedupe)", () => {
    expect(nextConnectionState("RECONNECTING", "RECONNECTING")).toBeNull();
    expect(nextConnectionState("CONNECTED", "CONNECTED")).toBeNull();
    expect(nextConnectionState("STALE", "STALE")).toBeNull();
  });

  it("error-then-close sequence: second RECONNECTING dedupes", () => {
    // Simulate the Firefox-style ws.onerror + ws.onclose sequence
    // documented at worker.ts:454-462: both handlers push RECONNECTING;
    // only the first should produce a visible transition.
    const afterError = nextConnectionState("CONNECTED", "RECONNECTING");
    expect(afterError).toBe("RECONNECTING");
    // ...worker would commit `connectionState = "RECONNECTING"` here.
    const afterClose = nextConnectionState("RECONNECTING", "RECONNECTING");
    expect(afterClose).toBeNull();
  });

  it("dedupes across all terminal states", () => {
    expect(
      nextConnectionState("DOWN_REQUIRES_USER_ACTION", "DOWN_REQUIRES_USER_ACTION"),
    ).toBeNull();
    expect(nextConnectionState("SCHEMA_MISMATCH", "SCHEMA_MISMATCH")).toBeNull();
    expect(nextConnectionState("WORKER_DEAD", "WORKER_DEAD")).toBeNull();
    expect(nextConnectionState("PROTOCOL_VIOLATION", "PROTOCOL_VIOLATION")).toBeNull();
  });

  it("allows transitioning into and out of every terminal state", () => {
    expect(nextConnectionState("RECONNECTING", "DOWN_REQUIRES_USER_ACTION")).toBe(
      "DOWN_REQUIRES_USER_ACTION",
    );
    expect(nextConnectionState("CONNECTED", "SCHEMA_MISMATCH")).toBe("SCHEMA_MISMATCH");
    expect(nextConnectionState("CONNECTED", "WORKER_DEAD")).toBe("WORKER_DEAD");
    expect(nextConnectionState("CONNECTED", "PROTOCOL_VIOLATION")).toBe("PROTOCOL_VIOLATION");
  });
});
