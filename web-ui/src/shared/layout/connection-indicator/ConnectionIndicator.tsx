/**
 * ConnectionIndicator — registered top-bar panel showing live connection state.
 *
 * Reads from `connectionStore` (NOT raw `connectionStream$`) so the
 * `useSyncExternalStore` bridge handles the React lifecycle. Pure
 * presentational; zero business logic.
 *
 * Colour groups:
 *   - green: `CONNECTED`
 *   - amber: `CONNECTING`, `RECONNECTING`, `STALE`, `BACKPRESSURE`
 *   - red:   `DOWN`, `DOWN_REQUIRES_USER_ACTION`, `SCHEMA_MISMATCH`,
 *            `WORKER_DEAD`, `PROTOCOL_VIOLATION`
 *
 * Threading: main thread.
 * Allocation: per-render only.
 *
 * Dependencies:
 *   - `@/stores/connection-store` — upstream snapshot source.
 *
 * @see connection-store — upstream `ExternalStore<ConnectionState>`.
 * @see connection-stream — transitive upstream Observable.
 *
 * Plan reference: APP-37 §ConnectionIndicator.
 */
import { type JSX } from "react";

import { type ConnectionState } from "@/shared/transport/MessageShape";
import { useStore } from "@/shared/store/createStore";
import { connectionStore } from "@/stores/connection-store";

import "./ConnectionIndicator.css";

const GREEN_STATES: ReadonlySet<ConnectionState> = new Set<ConnectionState>(["CONNECTED"]);
const AMBER_STATES: ReadonlySet<ConnectionState> = new Set<ConnectionState>([
  "CONNECTING",
  "RECONNECTING",
  "STALE",
  "BACKPRESSURE",
]);
// All remaining ConnectionState values map to red; checked via fallthrough.

function colourGroup(state: ConnectionState): "green" | "amber" | "red" {
  if (GREEN_STATES.has(state)) return "green";
  if (AMBER_STATES.has(state)) return "amber";
  return "red";
}

export function ConnectionIndicator(): JSX.Element {
  const state: ConnectionState = useStore(connectionStore);
  const group = colourGroup(state);
  return (
    <span className="conn-indicator" data-state={state}>
      <span
        className={`conn-dot conn-${group}`}
        role="status"
        aria-label={`Connection: ${state}`}
      />
      <span className="conn-label">{state}</span>
    </span>
  );
}
