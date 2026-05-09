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

import { createStore, type ExternalStore } from "@/shared/store/createStore";

import { type ConnectionState } from "@/shared/transport/MessageShape";

import { connectionStream$ } from "@/streams/connection-stream";

export const connectionStore: ExternalStore<ConnectionState> = createStore<ConnectionState>(
  connectionStream$,
  {
    name: "connection",
    initial: "CONNECTING",
  },
);
