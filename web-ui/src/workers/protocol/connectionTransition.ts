/**
 * Pure transition predicate for the worker's {@code transitionConnection}
 * function. Extracted into its own module so the transition invariants
 * are unit-testable without spinning up a real Web Worker harness.
 *
 * Two invariants:
 *
 * <ol>
 *   <li><b>Dedupe.</b> Same-state transitions (e.g., Firefox's
 *       {@code ws.onerror} followed by {@code ws.onclose}, both wanting
 *       to push {@code RECONNECTING}) collapse into a single emission so
 *       main-thread recorders / UI subscribers do not see spurious
 *       duplicates.</li>
 *   <li><b>Terminal-state stickiness.</b> Once the worker enters a state
 *       in {@link TERMINAL_CONNECTION_STATES} (no further auto-recovery),
 *       a late OS event — especially a Firefox-style {@code ws.onerror}
 *       firing AFTER {@code ws.onclose} per the WHATWG spec note — must
 *       not silently re-arm the state machine into {@code RECONNECTING}
 *       and resurrect the amber indicator after the breaker has tripped.
 *       The predicate refuses any transition out of a terminal state
 *       except into ANOTHER terminal state (which is itself a real
 *       escalation worth surfacing).</li>
 * </ol>
 *
 * Threading: pure function; safe to call from any thread.
 * Allocation: zero — returns either the input string reference or `null`.
 */

import { type ConnectionState, TERMINAL_CONNECTION_STATES } from "@/shared/transport/MessageShape";

/**
 * Decide whether a proposed {@link ConnectionState} transition should
 * fire.
 *
 * @param current  The state the worker currently holds.
 * @param proposed The state a caller (ws.onclose, ws.onerror, etc.) wants
 *                 to transition to.
 * @returns The new state to commit + emit, or {@code null} when the
 *          transition is suppressed. Suppression reasons:
 *          (1) {@code proposed === current} (dedupe);
 *          (2) {@code current} is in {@link TERMINAL_CONNECTION_STATES}
 *              and {@code proposed} is NOT (terminal-state stickiness).
 */
export function nextConnectionState(
  current: ConnectionState,
  proposed: ConnectionState,
): ConnectionState | null {
  if (current === proposed) return null;
  if (TERMINAL_CONNECTION_STATES.has(current) && !TERMINAL_CONNECTION_STATES.has(proposed)) {
    return null;
  }
  return proposed;
}
