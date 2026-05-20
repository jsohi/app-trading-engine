/**
 * Pure dedupe predicate for the worker's {@code transitionConnection}
 * function. Extracted into its own module so the dedupe invariant is
 * unit-testable without spinning up a real Web Worker harness.
 *
 * Invariant: the worker pushes a `connection-state` envelope ONLY on a
 * genuine state change. Same-state transitions (e.g., Firefox's
 * `ws.onerror` followed by `ws.onclose`, both wanting to push
 * `RECONNECTING`) collapse into a single emission so main-thread
 * recorders / UI subscribers do not see spurious duplicates.
 *
 * Threading: pure function; safe to call from any thread.
 * Allocation: zero — returns either the input string reference or `null`.
 */

import type { ConnectionState } from "@/shared/transport/MessageShape";

/**
 * Decide whether a proposed {@link ConnectionState} transition should
 * fire.
 *
 * @param current  The state the worker currently holds.
 * @param proposed The state a caller (ws.onclose, ws.onerror, etc.) wants
 *                 to transition to.
 * @returns The new state to commit + emit, or {@code null} when the
 *          proposed value matches {@code current} (dedupe — the caller
 *          must NOT emit a `connection-state` envelope).
 */
export function nextConnectionState(
  current: ConnectionState,
  proposed: ConnectionState,
): ConnectionState | null {
  if (current === proposed) return null;
  return proposed;
}
