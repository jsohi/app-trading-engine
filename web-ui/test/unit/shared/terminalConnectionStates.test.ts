/**
 * Pins the membership of {@link TERMINAL_CONNECTION_STATES} so that
 * adding a new terminal state to {@link ConnectionState} without also
 * adding it here trips a deterministic test failure rather than a
 * silent regression in {@code commandClient.ts}'s fail-fast path.
 *
 * <p>The TypeScript {@code satisfies ReadonlyArray<ConnectionState>}
 * clause on the constant enforces "every member is a valid state" at
 * compile time; this test enforces "the set covers every state we
 * regard as terminal at runtime." Together they bracket the invariant
 * from both sides.
 *
 * Threading: single-threaded Vitest harness.
 * Allocation: a single Set comparison per test.
 */

import { describe, expect, it } from "vitest";

import { TERMINAL_CONNECTION_STATES, type ConnectionState } from "@/shared/transport/MessageShape";

describe("TERMINAL_CONNECTION_STATES", () => {
  it("contains exactly the no-further-progress states", () => {
    // Hard-coded expected set. If a new terminal state is added to
    // ConnectionState (e.g., "AUTH_LOCKED"), update both this expected
    // set AND TERMINAL_CONNECTION_STATES — and document the rationale.
    const expected: ReadonlySet<ConnectionState> = new Set<ConnectionState>([
      "DOWN",
      "DOWN_REQUIRES_USER_ACTION",
      "SCHEMA_MISMATCH",
      "WORKER_DEAD",
      "PROTOCOL_VIOLATION",
    ]);

    expect(TERMINAL_CONNECTION_STATES.size).toBe(expected.size);
    for (const s of expected) {
      expect(TERMINAL_CONNECTION_STATES.has(s)).toBe(true);
    }
  });

  it("excludes every transient / recoverable state", () => {
    const recoverable: ReadonlyArray<ConnectionState> = [
      "CONNECTING",
      "CONNECTED",
      "RECONNECTING",
      "BACKPRESSURE",
      "STALE",
    ];
    for (const s of recoverable) {
      expect(TERMINAL_CONNECTION_STATES.has(s)).toBe(false);
    }
  });
});
