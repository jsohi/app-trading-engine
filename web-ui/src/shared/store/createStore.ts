/**
 * Generic React-side store adapter that bridges an RxJS Observable
 * stream into `useSyncExternalStore`. Designed to be the single
 * subscription seam between the Web Worker (APP-36) and React
 * components.
 *
 * Contract:
 *   - `getSnapshot()` returns a stable, immutable reference between
 *     stream emissions; `useSyncExternalStore` relies on identity
 *     equality to skip renders.
 *   - subscribe / unsubscribe symmetry: every subscribe MUST be
 *     matched by an unsubscribe (React hook lifecycle).
 *   - On remount, the most recent snapshot is replayed via
 *     `getSnapshot()` (no need to re-emit).
 *
 * Telemetry: records exactly one OTel span per `useStore` mount
 * (`web-ui.store.subscribe`) with attribute `store.name=<name>`. The
 * span ENDS immediately after subscribe — it represents lifecycle,
 * not duration. The hot-path message handler does NOT record spans.
 *
 * Threading model: main thread (React). The store itself is
 * single-subscriber per hook instance; the underlying observable
 * may be multicast.
 *
 * Allocation: subscribe-time only. `useSnapshot` returns the cached
 * value reference; no per-message allocation in the React layer.
 */
import { useSyncExternalStore } from "react";
import { type Observable, Subscription } from "rxjs";

import { tracer } from "@/shared/telemetry/otel";

export interface StoreOptions<T> {
  /**
   * Stable name for telemetry attribution. Span attribute
   * `store.name`. Use a constant string literal — never derive from
   * user input.
   */
  readonly name: string;
  /**
   * Initial snapshot used before the first emission. Must be
   * referentially stable across calls.
   */
  readonly initial: T;
}

export interface ExternalStore<T> {
  /**
   * Subscribe with a `useSyncExternalStore`-compatible callback.
   * The function shape is intentionally a property (not a method)
   * so it can be passed by reference without `.bind()` —
   * createStore returns closures, not methods.
   */
  readonly subscribe: (onStoreChange: () => void) => () => void;
  /** Read the latest snapshot (cached between emissions). */
  readonly getSnapshot: () => T;
}

/**
 * Test-only augmentation: the store returned by `createStore` exposes a
 * `__resetSnapshot()` escape hatch that the public `ExternalStore<T>`
 * interface deliberately does NOT carry. Consumers typed as `ExternalStore<T>`
 * literally cannot see this method (TypeScript structural typing), so the
 * hatch never leaks into application code. Only test-isolation helpers
 * (e.g. `__resetConnectionStoreForTests`) import the augmented return type.
 *
 * @internal
 */
export type CreatedStore<T> = ExternalStore<T> & {
  /**
   * @internal — re-seed the closure-local snapshot back to `options.initial`.
   * Used by test-isolation `afterEach` helpers; production code MUST NOT
   * call this. Listeners are NOT notified (afterEach runs after `cleanup()`
   * has unmounted the React tree, so `listeners.size === 0` by contract).
   * The next test's first `useStore` mount calls `getSnapshot()` and reads
   * the freshly-reset initial.
   */
  readonly __resetSnapshot: () => void;
};

/**
 * Build an external store from an RxJS Observable.
 *
 * @param source the upstream observable (emits whole snapshots).
 * @param options store name + initial snapshot.
 * @return a `CreatedStore<T>` (= `ExternalStore<T>` + test-only `__resetSnapshot`).
 *   Production consumers should annotate as `ExternalStore<T>` so the
 *   escape hatch is invisible.
 */
export function createStore<T>(source: Observable<T>, options: StoreOptions<T>): CreatedStore<T> {
  let snapshot: T = options.initial;
  const listeners = new Set<() => void>();
  let subscription: Subscription | null = null;

  function ensureSubscribed(): void {
    if (subscription !== null) return;
    // Re-entrancy guard: if `source` emits synchronously during
    // `source.subscribe(...)` (e.g. `BehaviorSubject`), the `next`
    // handler will fire BEFORE the assignment to `subscription`
    // completes. Without a parent Subscription pre-bound, listeners
    // notified inside that synchronous emission see `subscription`
    // as `null` — and a re-entrant `store.subscribe()` from one of
    // them would call `ensureSubscribed()` again, attaching a SECOND
    // upstream subscription. The fix: build a parent Subscription,
    // assign it to `subscription` BEFORE wiring the inner subscribe,
    // and `add()` the inner subscription to it. Now any re-entrant
    // call sees `subscription !== null` and short-circuits.
    const parent = new Subscription();
    subscription = parent;
    parent.add(
      source.subscribe({
        next: (value) => {
          snapshot = value;
          for (const fn of listeners) fn();
        },
        error: (err: unknown) => {
          // Errors are swallowed at the store boundary; APP-245 will
          // wire the telemetry channel for vendor-side ingestion. We
          // do NOT throw — that would crash every component subscribed.
          //
          // Three things must happen here:
          //   1) Record an error span (telemetry contract: span name
          //      `web-ui.store.error` with `store.name` attribute) so
          //      ops/RUM can detect upstream failure even before APP-245
          //      ships a real exporter — the NoopSpanProcessor swallows
          //      the span today, but the call site is wired correctly.
          //   2) Null `subscription` so a fresh subscriber can re-attempt
          //      via `ensureSubscribed()`. Without this the store stayed
          //      permanently dead after a single upstream error — every
          //      future subscribe found a non-null but broken subscription
          //      and silently produced stale snapshots.
          //   3) Notify listeners — forward-compat scaffolding for
          //      APP-245's terminal-state signal. NOTE: today this is
          //      effectively a no-op: `useSyncExternalStore` checks
          //      identity via `Object.is`; the snapshot reference is
          //      unchanged on error, so React skips re-render. The
          //      notify exists so APP-245 can layer a companion
          //      overlay (e.g., `terminal: { kind: "error" } | null`)
          //      with one code change and have every existing consumer
          //      re-render automatically. Until then, the user-visible
          //      deliverable on error is the OTel span above.
          // Telemetry contract:
          //   - error.type / error.message are computed via the same
          //     resolution order the OTel SDK uses inside
          //     `Span.recordException`, so the custom `error.*`
          //     attributes and the OTel-standard `exception.*` event
          //     attributes agree byte-for-byte for the documented
          //     primitive-code shapes (string / number / bigint /
          //     boolean / symbol). Object/array `err.code` shapes
          //     intentionally fall through to `err.name` (see the
          //     errorType helper) — the otel.ts contract docstring
          //     calls this out as the one documented divergence.
          //   - error.type:  err.code (any non-null, coerced to String
          //                  to match SDK behaviour for numeric codes
          //                  in legacy NodeJS ErrnoException), then
          //                  err.name, then typeof err for non-Error
          //                  throws.
          //   - error.message: err.message for Errors, otherwise the
          //                    sentinel-wrapped form so the custom and
          //                    OTel paths agree byte-for-byte. The
          //                    sentinel ("non-Error throw: ...") lets
          //                    log-greps distinguish "literal null was
          //                    thrown" from "code path stringified a
          //                    variable that happened to be null".
          const wrappedErr = toErrorForSpan(err);
          const errSpan = tracer.startSpan("web-ui.store.error", {
            attributes: {
              "store.name": options.name,
              "error.type": errorType(err),
              "error.message": wrappedErr.message,
            },
          });
          // Record via the OTel-idiomatic API so RUM backends that
          // read `exception.*` semantic-convention attributes also
          // see it. `recordException` adds an `exception` event on
          // the span populated with `exception.type` /
          // `exception.message` / `exception.stacktrace`. Custom
          // `error.*` attributes above are preserved for the
          // documented telemetry contract — and computed from the
          // same wrappedErr so they agree with the event byte-for-byte.
          errSpan.recordException(wrappedErr);
          errSpan.end();
          subscription = null;
          for (const fn of listeners) fn();
        },
        complete: () => {
          // Symmetric with `error` — the upstream is now terminal.
          // Null `subscription` so a future subscriber can attach a
          // fresh stream (cold sources like `defer` rebuild on the
          // next subscribe). The listener notify is forward-compat
          // scaffolding (same APP-245 caveat as the error path) —
          // currently a no-op because `snapshot` identity is
          // unchanged; ready to fire renders once a terminal-state
          // companion signal lands.
          subscription = null;
          for (const fn of listeners) fn();
        },
      }),
    );
  }

  function maybeUnsubscribe(): void {
    if (listeners.size === 0 && subscription !== null) {
      subscription.unsubscribe();
      subscription = null;
    }
  }

  return {
    subscribe(onStoreChange: () => void): () => void {
      // Lifecycle span — recorded once per subscribe, ended
      // synchronously. Span name + attribute are part of the
      // telemetry contract (web-ui-telemetry-contract).
      const span = tracer.startSpan("web-ui.store.subscribe", {
        attributes: { "store.name": options.name },
      });
      try {
        listeners.add(onStoreChange);
        ensureSubscribed();
      } finally {
        span.end();
      }
      return () => {
        listeners.delete(onStoreChange);
        maybeUnsubscribe();
      };
    },
    getSnapshot(): T {
      return snapshot;
    },
    __resetSnapshot(): void {
      // @internal — see CreatedStore<T> JSDoc. Re-seeds the closure-local
      // snapshot back to `options.initial`. Listeners are intentionally NOT
      // notified — by the time afterEach runs this, React has been unmounted
      // by the prior `cleanup()` call and `listeners.size === 0`.
      snapshot = options.initial;
    },
  };
}

/**
 * React hook wrapping an `ExternalStore<T>` for component use.
 * Returns the latest snapshot; the component re-renders when the
 * snapshot identity changes.
 */
export function useStore<T>(store: ExternalStore<T>): T {
  return useSyncExternalStore(store.subscribe, store.getSnapshot);
}

/**
 * Resolve the `error.type` attribute for the `web-ui.store.error`
 * span. Order matches the OTel SDK's `Span.recordException()` which
 * writes `exception.type` from `exception.code.toString()` first
 * (any truthy code, including numeric legacy NodeJS errno values),
 * then from `exception.name`. We coerce `err.code` via `String()`
 * for byte-stable lock-step.
 *
 * For non-Error throws (`null`, `undefined`, primitives, plain
 * objects), returns `typeof err` — these are abnormal Observable
 * contract violations and the type-of label is enough for ops to
 * spot them.
 *
 * @param err the value an upstream Observable threw / errored with
 * @return code (stringified), name, or typeof — never throws
 */
function errorType(err: unknown): string {
  if (err instanceof Error) {
    const errWithCode = err as Error & { readonly code?: unknown };
    const code = errWithCode.code;
    // SDK uses `if (exception.code)` (truthy) — match it. We restrict
    // the coercion to types whose String() form is information-bearing
    // (string, number, bigint, boolean, symbol). Object/array codes
    // are atypical and would stringify to "[object Object]" / a
    // comma-joined list — fall through to `err.name` instead.
    if (typeof code === "string" && code.length > 0) {
      return code;
    }
    if (typeof code === "number" && code !== 0) {
      return String(code);
    }
    if (typeof code === "bigint" && code !== 0n) {
      return code.toString();
    }
    if (typeof code === "boolean" && code) {
      return String(code);
    }
    if (typeof code === "symbol") {
      return code.toString();
    }
    return err.name;
  }
  return typeof err;
}

/**
 * Wrap a thrown value into an `Error` suitable for
 * `Span.recordException`. Real `Error` objects pass through; non-
 * Error throws (`null`, `undefined`, primitives, plain objects) are
 * wrapped with a sentinel-prefixed message so log greps can tell
 * "literal null was thrown" apart from "code path stringified a
 * variable that happened to be null".
 *
 * The wrapper's `.name` is set to `typeof err` so the OTel SDK's
 * `recordException` writes `exception.type = typeof err` — agreeing
 * byte-for-byte with the custom `error.type` attribute computed by
 * `errorType(err)` for non-Error throws.
 *
 * @param err the value an upstream Observable threw / errored with
 * @return a real Error (the original or a sentinel-prefixed wrapper)
 */
function toErrorForSpan(err: unknown): Error {
  if (err instanceof Error) {
    return err;
  }
  const wrapped = new Error(`non-Error throw: ${String(err)}`);
  // Override the default Error name so exception.type lines up with
  // error.type's `typeof err` for non-Error throws. NOTE: this also
  // changes `wrapped.toString()` to `"<typeof>: non-Error throw: ..."`
  // (Error.prototype.toString rebuilds from current name). The OTel
  // SDK reads name/message/stack as separate fields so telemetry is
  // unaffected; only an explicit `String(wrapped)` / interpolation
  // would see the new prefix. We don't do either anywhere in web-ui.
  Object.defineProperty(wrapped, "name", {
    value: typeof err,
    writable: true,
    configurable: true,
    enumerable: false,
  });
  return wrapped;
}
