/**
 * connection-stream — BehaviorSubject of ConnectionState transitions.
 *
 * Public `connectionStream$` is a `defer(() => _subject.asObservable())`
 * wrapper over a module-private `let _subject` so test-isolation reset
 * (`__resetConnectionStreamForTests()`) can swap the inner subject between
 * tests without invalidating the exported binding. BehaviorSubject already
 * replays its current value to each new subscriber, so no `shareReplay` is
 * needed — wrapping defer around shareReplay would create per-subscriber
 * shareReplay instances (defer evaluates its factory per subscribe),
 * defeating multicast.
 *
 * Rationale: matches the same defer-and-swap pattern used by
 * `messageSource._messages` so the two test-isolation reset helpers behave
 * identically.
 *
 * Threading: main thread.
 * Allocation: per state-change emission only.
 *
 * Dependencies:
 *   - `@/shared/transport/MessageShape` — peer: `ConnectionState` type.
 *
 * @see messageSource — peer, same defer-and-swap pattern.
 * @see connection-store — downstream consumer (createStore).
 * @see ConnectionIndicator — downstream consumer (via connection-store).
 *
 * Plan reference: APP-37 §Files to modify / §messages$ singleton design.
 */

import { BehaviorSubject, type Observable, defer } from "rxjs";

import { type ConnectionState } from "@/shared/transport/MessageShape";

// Module-private; `let` so `__resetConnectionStreamForTests()` can swap.
let _subject: BehaviorSubject<ConnectionState> = new BehaviorSubject<ConnectionState>("CONNECTING");

/**
 * Public connection-state stream. Late subscribers receive the most recent
 * state immediately (BehaviorSubject seed-replay). The `defer(...)` wrapper
 * reads the CURRENT `_subject` at subscribe time — required so test resets
 * that swap the inner subject are visible to fresh subscribers.
 */
export const connectionStream$: Observable<ConnectionState> = defer(() => _subject.asObservable());

/** Internal pusher — `workerClient` calls this on inbound `ConnectionStateMsg`. */
export function pushConnectionState(next: ConnectionState): void {
  _subject.next(next);
}

/**
 * @internal — test-isolation reset. Called from the global `afterEach` in
 * `test/setup.ts` AFTER `cleanup()` so React subscribers are unmounted
 * first. Completes the old subject (notifying any lingering subscribers
 * with `complete`) then swaps in a fresh BehaviorSubject seeded `CONNECTING`.
 *
 * Production code MUST NOT call this — it would terminate every active
 * subscription.
 */
export function __resetConnectionStreamForTests(): void {
  _subject.complete();
  _subject = new BehaviorSubject<ConnectionState>("CONNECTING");
}

/**
 * HMR-safe dispose — module-level singleton cleanup. Completes the OLD
 * subject so consumers retaining stale references to the OLD `defer`
 * closure see a `complete` notification (mirrors `messageSource.ts`'s
 * dev HMR shape). On module re-evaluation Vite re-runs the top-level
 * `let _subject = new BehaviorSubject(...)` initialiser, so the NEW
 * subject is supplied to fresh subscribers via the `defer(...)` wrapper —
 * no explicit reassignment in the dispose body needed.
 */
if (import.meta.hot) {
  import.meta.hot.dispose(() => {
    _subject.complete();
  });
}
