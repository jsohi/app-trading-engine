/**
 * messageSource — singleton broadcast point for all worker → main messages.
 *
 * Public `messages$` is an `Observable<WorkerMessage>` wrapping a
 * module-private `_messages: ReplaySubject<WorkerMessage>(1)` via
 * `defer(...)`. Downstream consumers (panels, future APP-42 event log,
 * future APP-40 RFQ panel) subscribe to `messages$`; the producer-only
 * `_messages` handle is the sole `.next()` site (in `startMessageSource`)
 * and the sole `.complete()`/reset site (in `__resetMessageSourceForTests`).
 *
 * Lifecycle:
 *   - Booted from `main.tsx` once via `startMessageSource()`. The internal
 *     `started` flag makes the call idempotent (HMR re-evaluation safe).
 *   - Dev mode: subscribes `fakeStream({ intervalMs: 250 })` and pushes
 *     `pushConnectionState("CONNECTED")` so the indicator turns green.
 *   - Prod mode: APP-160 (JWT auth) has not landed, so the prod branch
 *     ships as a LOUD STUB — `console.error` + `pushConnectionState
 *     ("DOWN_REQUIRES_USER_ACTION")` + early return. Full WorkerClient
 *     wiring lives as a commented-out template block, ready to swap in.
 *   - HMR teardown unsubscribes the dev stream AND `_messages.complete()`-s
 *     the OLD subject so consumers retaining stale references to the OLD
 *     `defer` closure see a `complete` notification.
 *
 * The `mode` parameter is for tests only (`import.meta.env.DEV` is a Vite
 * transform-time constant that `vi.stubEnv` does not reliably flip).
 *
 * Threading: main thread.
 * Allocation: subject construction at module load + per-emission `.next()`
 * (RxJS Subject internals); zero allocation in the broadcast path itself.
 *
 * Dependencies:
 *   - `@/streams/connection-stream` — downstream: `pushConnectionState`
 *     used to drive the indicator.
 *   - `@/mocks/fakeStream` — upstream (dev mode): synthetic data source.
 *   - `@/shared/transport/MessageShape` — peer: `WorkerMessage` type.
 *
 * @see fakeStream — dev-mode upstream.
 * @see connection-stream — downstream `pushConnectionState`.
 * @see workerClient — future prod upstream (APP-160 lands the wiring).
 *
 * Plan reference: APP-37 §Boot wiring / §`messages$` singleton design.
 */

import { type Observable, ReplaySubject, type Subscription, defer } from "rxjs";

import { fakeStream } from "@/mocks/fakeStream";
import { type WorkerMessage } from "@/shared/transport/MessageShape";
import { pushConnectionState } from "@/streams/connection-stream";

// Module-private; `let` so `__resetMessageSourceForTests()` can swap.
// Public binding stays stable across resets via the `defer(...)` wrapper —
// avoids the `export const` reassignment trap (ESM bindings are immutable).
let _messages = new ReplaySubject<WorkerMessage>(1);

/**
 * Singleton broadcast point for `WorkerMessage`s. Late subscribers
 * (panels mounting after the first tick, Storybook stories, integration
 * tests) get the most recent message replayed on subscribe.
 *
 * Producer-only — consumers MUST NOT call `.next()` / `.complete()`. The
 * public type is narrowed to `Observable<WorkerMessage>` so attempting
 * either is a TypeScript error.
 */
export const messages$: Observable<WorkerMessage> = defer(() => _messages.asObservable());

let started = false;
let _devSub: Subscription | null = null;

/**
 * Boot the message source. Idempotent — the `started` flag guards
 * HMR re-evaluation. Called once from `main.tsx`.
 *
 * @param mode override for tests; production callers omit.
 */
export function startMessageSource(
  mode: "dev" | "prod" = import.meta.env.DEV ? "dev" : "prod",
): void {
  if (started) return;
  started = true;

  if (mode === "dev") {
    _devSub = fakeStream({ intervalMs: 250 }).subscribe((m) => {
      _messages.next(m);
    });
    pushConnectionState("CONNECTED");

    // HMR teardown: unsubscribe AND complete the old subject so consumers
    // still holding the OLD `defer` closure get a `complete` notification.
    if (import.meta.hot) {
      import.meta.hot.dispose(() => {
        _devSub?.unsubscribe();
        _devSub = null;
        _messages.complete();
        started = false;
      });
    }
  } else {
    // APP-160 (JWT auth + prod token provider) has NOT landed. Prod boot
    // ships as a LOUD STUB so a prod-build smoke surfaces the gap loudly.
    // When APP-160 lands, replace the three lines below with the full
    // WorkerClient bridge (see commented template below).
    pushConnectionState("DOWN_REQUIRES_USER_ACTION");
    console.error("messageSource: prod token provider not landed (APP-160)");
    return;

    /* When APP-160 lands, replace the stub above with:
    const client = new WorkerClient({
      tokenProvider: prodTokenProvider,
      wsUrl: import.meta.env.VITE_WS_URL,
    });
    const subMessages = client.messages$.subscribe((m) => _messages.next(m));
    // skip(1): WorkerClient.connectionState$ is a BehaviorSubject seeded
    // "CONNECTING"; subscribing replays it synchronously. The async
    // start().catch() resolves on a later microtask, so it cannot beat
    // the seed. skip(1) drops the redundant CONNECTING re-push so any
    // worker-emitted state (or the catch-pushed WORKER_DEAD) becomes the
    // first value to propagate.
    const subState = client.connectionState$.pipe(skip(1)).subscribe(pushConnectionState);
    const subErrors = client.errors$.subscribe((e) =>
      console.warn("[worker]", e.code, e.hint),
    );
    void client.start().catch(() => pushConnectionState("WORKER_DEAD"));

    if (import.meta.hot) {
      import.meta.hot.dispose(() => {
        subMessages.unsubscribe();
        subState.unsubscribe();
        subErrors.unsubscribe();
        client.dispose();
        _messages.complete();
        started = false;
      });
    }
    */
  }
}

/**
 * @internal — test-isolation reset. Called from the global `afterEach` in
 * `test/setup.ts` AFTER `cleanup()` so React subscribers are unmounted
 * first. Completes the old subject (notifying any lingering subscribers)
 * and swaps in a fresh `ReplaySubject(1)`. The `defer(...)` wrapper picks
 * up the new subject on the next subscribe.
 *
 * Production code MUST NOT call this.
 *
 * @see __resetConnectionStreamForTests — sibling helper, called together.
 */
export function __resetMessageSourceForTests(): void {
  _devSub?.unsubscribe();
  _devSub = null;
  _messages.complete();
  _messages = new ReplaySubject<WorkerMessage>(1);
  started = false;
}
