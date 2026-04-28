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
import type { Observable, Subscription } from "rxjs";

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
    subscription = source.subscribe({
      next: (value) => {
        snapshot = value;
        for (const fn of listeners) fn();
      },
      error: () => {
        // Errors are swallowed at the store boundary; APP-245 will
        // wire this to a telemetry channel. We do NOT throw — that
        // would crash every component subscribed.
      },
    });
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
