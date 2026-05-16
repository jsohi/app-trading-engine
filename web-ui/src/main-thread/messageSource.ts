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
 *   - Both modes (pre-prod): drive the UI off `fakeStream({ intervalMs: 250 })`
 *     and push `pushConnectionState("CONNECTED")` so the indicator goes green
 *     and all three blotters populate. `prod` emits ONE `console.warn` so
 *     pre-prod state is observable.
 *   - When APP-160 lands, the `prod` branch swaps to the real WorkerClient
 *     wiring documented in `docs/messageSource-prod-wiring.md`. The mode
 *     parameter stays so the swap is a localised edit.
 *   - HMR teardown unsubscribes the fakeStream sub AND `_messages.complete()`-s
 *     the OLD subject so consumers retaining stale references to the OLD
 *     `defer` closure see a `complete` notification.
 *
 * The `mode` parameter is for tests only. The default reads
 * `import.meta.env.MODE` (a runtime string Vite emits as a real value, not
 * the transform-time `DEV` boolean) so the security-relevant prod-stub
 * branch can't silently flip if the Vite transform changes.
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
import { pushFeedState } from "@/streams/feed-state-stream";

// IMPORTANT: WorkerClient and devTokenProvider are imported DYNAMICALLY inside
// the VITE_E2E_REAL_BACKEND branch (see startMessageSource below). Static imports
// here would pull `@/workers/loadWorker` into every test setup's module graph
// BEFORE `vi.mock("@/workers/loadWorker", ...)` calls in individual test files
// take effect, breaking the WorkerClient unit tests' synthetic-Worker mock.
// Dynamic import keeps the test-mode dependency tree opt-in: only loaded when
// the env var is set. registerForceWsClose is a no-op outside test mode so we
// can keep its static import.
import { registerForceWsClose } from "@/main-thread/e2eHooks";

// Module-private; `let` so `__resetMessageSourceForTests()` can swap.
// Public binding stays stable across resets via the `defer(...)` wrapper —
// avoids the `export const` reassignment trap (ESM bindings are immutable).
//
// **ReplaySubject(1) scope** (Gemini R1 review of PR #72):
// The single-message buffer solves ONE specific problem — the boot-race
// where a panel mounts AFTER the first fakeStream tick has fired. With a
// plain `Subject`, that first tick would be lost and `messages$.subscribe`
// would see nothing until the next 250ms tick (flaky e2e). The replay of 1
// guarantees the most-recent message is replayed to late subscribers.
//
// What ReplaySubject(1) does NOT solve: a panel mounting hours into a
// session that needs the CURRENT state of every entity (all open orders,
// all current prices, all net positions) cannot reconstruct that from one
// replayed `WorkerMessage`. A larger buffer would help only marginally
// (still lossy beyond N messages). The correct architecture for full-state
// backfill is **APP-160's snapshot-on-connect frame**: the worker emits a
// snapshot of every active entity at the start of the stream so panels
// (or any subscriber) prime their local state from it. APP-37 ships before
// APP-160, so all panels are guaranteed-mount-at-boot in this build —
// the late-mount-full-state scenario is not yet a real user path.
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
  mode: "dev" | "prod" = import.meta.env.MODE === "production" ? "prod" : "dev",
): void {
  if (started) return;
  started = true;

  // Test-mode escape hatch (plan §4): real WorkerClient against the live
  // websocket-server. Vite inlines the env-var comparison at build time;
  // esbuild dead-code-eliminates the body for production builds. The
  // bundle-guard test (web-ui/test/integration/build-bundle.test.ts) greps
  // dist/*.js to prove no leakage. Defence-in-depth: also asserts DEV mode
  // — if a future build ever runs with VITE_E2E_REAL_BACKEND=true under
  // import.meta.env.PROD=true, fail loud rather than silently shipping the
  // test path.
  if (import.meta.env.VITE_E2E_REAL_BACKEND === "true") {
    if (import.meta.env.PROD) {
      throw new Error("VITE_E2E_REAL_BACKEND must only be set in dev builds (defence-in-depth)");
    }
    // Dynamic import keeps WorkerClient + the singleton's transitive imports
    // out of every test setup's module graph — only the test-mode branch loads
    // them. We use the process-wide singleton (workerClientSingleton.ts) so the
    // panel-form CommandClient and this inbound-stream subscription share ONE
    // worker / ONE wss session. Without the singleton each consumer would
    // spawn a parallel worker, burning a second auth slot and routing
    // CommandAck back to whichever worker emitted the original send (reviewer
    // A finding F-A1).
    void (async (): Promise<void> => {
      try {
        const { getWorkerClient, disposeWorkerClient } =
          await import("@/main-thread/workerClientSingleton");
        const client = getWorkerClient();
        const sub = client.messages$.subscribe({
          next: (m) => {
            _messages.next(m);
            // Phase 3 Commit 6 — fan out FeedStateMsg into the dedicated feedState$
            // BehaviorSubject so the UI can render a feed-stale banner independently
            // of the WS connection indicator. Separating the two streams ensures a
            // STALE market-data feed does not falsely surface as "disconnecting".
            if (m.type === "feed-state") {
              pushFeedState(m.state);
            }
          },
          error: (e: unknown) => {
            console.error("messageSource: WorkerClient stream error", e);
            pushConnectionState("DOWN");
          },
        });
        const stateSub = client.connectionState$.subscribe((s) => {
          pushConnectionState(s);
          // Reset-on-reconnect: every CONNECTED transition resets feedState$ to LIVE.
          // The prior state captured pre-disconnect is stale; the post-reconnect
          // server's first MarketDataFeedStateChange (template 57) frame will overwrite
          // this. Without the reset a STALE state from before the disconnect would
          // carry forward and falsely flag a healthy post-reconnect publisher.
          if (s === "CONNECTED") {
            pushFeedState("LIVE");
          }
        });
        registerForceWsClose(() => {
          // Tear down the singleton; next consumer call recreates it.
          disposeWorkerClient();
        });
        if (import.meta.hot) {
          import.meta.hot.dispose(() => {
            sub.unsubscribe();
            stateSub.unsubscribe();
            disposeWorkerClient();
            _messages.complete();
            started = false;
          });
        }
      } catch (e: unknown) {
        console.error("messageSource: real-backend bootstrap failed", e);
        pushConnectionState("DOWN");
      }
    })();
    return;
  }

  // Pre-prod build: both `dev` and `prod` modes drive the UI off the same
  // synthetic `fakeStream`. The mode parameter still distinguishes them so
  // when APP-160 lands the prod branch can swap to the real WorkerClient
  // wiring (see `docs/messageSource-prod-wiring.md`) without touching dev.
  // Until then `prod` emits ONE console.warn so the pre-prod state is
  // observable in DevTools / RUM — but the indicator turns green and the
  // blotters populate exactly like the dev path.
  if (mode === "prod") {
    console.warn(
      "messageSource: pre-prod build — driving fakeStream until APP-160 lands real auth + WorkerClient wiring",
    );
  }

  _devSub = fakeStream({ intervalMs: 250 }).subscribe({
    next: (m) => {
      _messages.next(m);
    },
    error: (err: unknown) => {
      // fakeStream is a `timer(...).pipe(map(...))` that never errors today,
      // but defensive: surface the error and degrade to DOWN so the indicator
      // visibly reflects the broken upstream. The real WorkerClient swap
      // (APP-160) will replace this whole branch — its error handling lives
      // in WorkerClient itself.
      console.error("messageSource: fakeStream upstream error", err);
      pushConnectionState("DOWN");
    },
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
