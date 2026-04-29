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
 * Build an external store from an RxJS Observable.
 *
 * @param source the upstream observable (emits whole snapshots).
 * @param options store name + initial snapshot.
 * @return an `ExternalStore<T>` ready to feed `useSyncExternalStore`.
 */
export function createStore<T>(source: Observable<T>, options: StoreOptions<T>): ExternalStore<T> {
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
          //   3) Notify listeners — even if `snapshot` did not change, a
          //      React component that subscribed to this store should
          //      re-render so it can observe terminal state via any
          //      downstream signal it consumes (e.g. an error overlay
          //      that reads `store.getSnapshot()` or a hook companion).
          const errSpan = tracer.startSpan("web-ui.store.error", {
            attributes: {
              "store.name": options.name,
              "error.type": err instanceof Error ? err.name : typeof err,
              "error.message": err instanceof Error ? err.message : String(err),
            },
          });
          errSpan.end();
          subscription = null;
          for (const fn of listeners) fn();
        },
        complete: () => {
          // Symmetric with `error` — the upstream is now terminal.
          // Null `subscription` so a future subscriber can attach a
          // fresh stream (cold sources like `defer` rebuild on the
          // next subscribe). Notify listeners so they can re-read the
          // snapshot if any companion signal depends on completion.
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
