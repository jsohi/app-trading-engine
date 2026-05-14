/**
 * connection-store — `useSyncExternalStore`-bridged ConnectionState.
 *
 * Wraps the existing `connectionStream$` via the production `createStore`
 * helper. Status-banner UI consumes via `useStore(connectionStore)`.
 *
 * Threading: main thread (React).
 *
 * Allocation: subscription per consumer.
 *
 * Plan reference: §5.6 / §6 row 34.
 */

import { createStore, type CreatedStore, type ExternalStore } from "@/shared/store/createStore";

import { type ConnectionState } from "@/shared/transport/MessageShape";

import { connectionStream$ } from "@/streams/connection-stream";

// Internally typed as CreatedStore so the test-only `__resetSnapshot()`
// escape hatch is reachable; the public export is narrowed to ExternalStore
// so application code cannot see it.
const _connectionStore: CreatedStore<ConnectionState> = createStore<ConnectionState>(
  connectionStream$,
  {
    name: "connection",
    initial: "CONNECTING",
  },
);

export const connectionStore: ExternalStore<ConnectionState> = _connectionStore;

/**
 * @internal — test-isolation reset. Called from the global `afterEach` in
 * `test/setup.ts` AFTER `cleanup()`. Re-seeds the snapshot back to
 * `"CONNECTING"` so the next test's first `useStore` mount reads a known
 * state independent of the previous test's terminal value.
 */
export function __resetConnectionStoreForTests(): void {
  _connectionStore.__resetSnapshot();
}
