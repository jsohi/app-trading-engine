/**
 * connection-stream — BehaviorSubject of ConnectionState transitions.
 *
 * `shareReplay({ bufferSize: 1, refCount: false })` for late subscribers.
 * `refCount: false` keeps the source alive for the app lifetime; HMR-
 * safe via `import.meta.hot?.dispose` (no-op in production where
 * `import.meta.hot` is absent).
 *
 * Threading: main thread.
 *
 * Allocation: per state-change emission only.
 *
 * Plan reference: §5.5 / §6 row 21.
 */

import { BehaviorSubject, type Observable, shareReplay } from "rxjs";

import { type ConnectionState } from "@/shared/transport/MessageShape";

const subject = new BehaviorSubject<ConnectionState>("CONNECTING");

/**
 * Public connection-state stream. Late subscribers receive the most
 * recent state immediately.
 */
export const connectionStream$: Observable<ConnectionState> = subject.pipe(
  shareReplay({ bufferSize: 1, refCount: false }),
);

/** Internal pusher — `workerClient` calls this on inbound `ConnectionStateMsg`. */
export function pushConnectionState(next: ConnectionState): void {
  subject.next(next);
}

/** HMR-safe dispose — module-level singleton cleanup. */
if (import.meta.hot) {
  import.meta.hot.dispose(() => {
    subject.complete();
  });
}
