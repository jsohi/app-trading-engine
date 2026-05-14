/**
 * Purpose: Unit tests for ConnectionIndicator — verifies that each of the 10
 * ConnectionState values renders the correct colour-group class on .conn-dot
 * and the correct aria-label.
 *
 * Rationale: all 10 states must be covered to detect missing or misspelled
 * group assignments; mock connectionStore so the component renders
 * synchronously without a real RxJS subscription.
 *
 * @see ConnectionIndicator — system under test.
 * @see connectionStore — mocked upstream snapshot source.
 */
import { describe, it, expect, vi } from "vitest";
import { render, screen } from "@testing-library/react";

import type { ConnectionState } from "@/shared/transport/MessageShape";

// ── Colour-group mapping ───────────────────────────────────────────────────
// Matches ConnectionIndicator.tsx `colourGroup()` exactly.
const GREEN_STATES: ReadonlySet<ConnectionState> = new Set<ConnectionState>(["CONNECTED"]);
const AMBER_STATES: ReadonlySet<ConnectionState> = new Set<ConnectionState>([
  "CONNECTING",
  "RECONNECTING",
  "STALE",
  "BACKPRESSURE",
]);

function expectedGroup(state: ConnectionState): "green" | "amber" | "red" {
  if (GREEN_STATES.has(state)) return "green";
  if (AMBER_STATES.has(state)) return "amber";
  return "red";
}

// ── All 10 ConnectionState values ─────────────────────────────────────────
const ALL_STATES: readonly ConnectionState[] = [
  "CONNECTING",
  "CONNECTED",
  "RECONNECTING",
  "BACKPRESSURE",
  "STALE",
  "DOWN",
  "DOWN_REQUIRES_USER_ACTION",
  "SCHEMA_MISMATCH",
  "WORKER_DEAD",
  "PROTOCOL_VIOLATION",
];

// ── Tests ──────────────────────────────────────────────────────────────────
// vi.mock is hoisted; we use a per-test factory via `vi.doMock` inside the
// `it` bodies instead, which requires dynamic import for the component.
// Simpler approach: stub the module once with a writable `_state` and mutate
// it between renders.

// We control `_state` via the closure in the mock factory.
let _mockState: ConnectionState = "CONNECTING";

vi.mock("@/stores/connection-store", () => ({
  connectionStore: {
    subscribe: () => (): void => undefined,
    getSnapshot: () => _mockState,
  },
  __resetConnectionStoreForTests: (): void => undefined,
}));

// Lazy import after vi.mock is hoisted.
const { ConnectionIndicator } =
  await import("@/shared/layout/connection-indicator/ConnectionIndicator");

describe("ConnectionIndicator", () => {
  for (const state of ALL_STATES) {
    it(`connDot_state${state}_correctGroupAndAriaLabel`, () => {
      _mockState = state;
      render(<ConnectionIndicator />);

      const dot = screen.getByRole("status");
      const group = expectedGroup(state);

      expect(dot.classList.contains("conn-dot")).toBe(true);
      expect(
        dot.classList.contains(`conn-${group}`),
        `state ${state}: expected conn-${group}, got ${Array.from(dot.classList).join(" ")}`,
      ).toBe(true);
      expect(dot.getAttribute("aria-label")).toBe(`Connection: ${state}`);
    });
  }
});
