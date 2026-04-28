/**
 * Vitest contract tests for the createStore RxJS adapter.
 *
 * Covers:
 *   - subscribe / unsubscribe symmetry
 *   - getSnapshot stability before any emission
 *   - snapshot identity changes only on emission
 *   - lifecycle telemetry: exactly one `web-ui.store.subscribe`
 *     span per `subscribe` call, with `store.name` attribute.
 */
import { describe, expect, it } from "vitest";
import { Subject, type Observable } from "rxjs";

import { createStore } from "./createStore";
import { TEST_SPAN_EXPORTER } from "../../../test/setup";

describe("createStore", () => {
  it("subscribe_returnsInitialSnapshotBeforeFirstEmission_remainsStable", () => {
    const subject = new Subject<number>();
    const store = createStore(subject, { name: "t1", initial: 42 });
    expect(store.getSnapshot()).toBe(42);
    const snap1 = store.getSnapshot();
    const snap2 = store.getSnapshot();
    expect(snap1).toBe(snap2);
  });

  it("emit_propagatesValueToSnapshotAndNotifiesListeners_afterSubscribe", () => {
    const subject = new Subject<number>();
    const store = createStore(subject, { name: "t2", initial: 0 });
    let notifyCount = 0;
    const unsub = store.subscribe(() => {
      notifyCount += 1;
    });
    subject.next(7);
    expect(store.getSnapshot()).toBe(7);
    subject.next(11);
    expect(store.getSnapshot()).toBe(11);
    expect(notifyCount).toBe(2);
    unsub();
  });

  it("unsubscribe_lastListenerLeaving_releasesUpstreamSubscription", () => {
    const subject = new Subject<number>();
    const store = createStore(subject, { name: "t3", initial: 0 });
    const unsub = store.subscribe(() => undefined);
    expect(subject.observed).toBe(true);
    unsub();
    expect(subject.observed).toBe(false);
  });

  it("subscribe_recordsExactlyOneLifecycleSpan_withStoreNameAttribute", () => {
    const subject = new Subject<number>();
    const store = createStore(subject, { name: "telemetry-target", initial: 0 });
    TEST_SPAN_EXPORTER.reset();
    const unsub = store.subscribe(() => undefined);
    const spans = TEST_SPAN_EXPORTER.getFinishedSpans();
    const subscribeSpans = spans.filter((s) => s.name === "web-ui.store.subscribe");
    expect(subscribeSpans).toHaveLength(1);
    expect(subscribeSpans[0]?.attributes["store.name"]).toBe("telemetry-target");
    unsub();
  });

  it("upstreamError_recordsErrorSpanWithStoreNameAndErrorAttributes", () => {
    const subject = new Subject<number>();
    const store = createStore(subject, { name: "err-target", initial: 0 });
    const unsub = store.subscribe(() => undefined);
    TEST_SPAN_EXPORTER.reset();
    subject.error(new TypeError("boom"));
    const errorSpans = TEST_SPAN_EXPORTER.getFinishedSpans().filter(
      (s) => s.name === "web-ui.store.error",
    );
    expect(errorSpans).toHaveLength(1);
    expect(errorSpans[0]?.attributes["store.name"]).toBe("err-target");
    expect(errorSpans[0]?.attributes["error.type"]).toBe("TypeError");
    expect(errorSpans[0]?.attributes["error.message"]).toBe("boom");
    // Cleanup is best-effort here — the upstream errored, and the
    // listener is still in `listeners`. unsub still works.
    unsub();
  });

  it("upstreamError_thenResubscribe_reattachesUpstream_doesNotPermanentlyFreeze", () => {
    // Use a factory so each subscribe gets a fresh underlying source.
    // The store's `ensureSubscribed` is meant to re-attempt after an
    // error; with the R2 fix it nulls `subscription` on error so the
    // next subscribe wires up a new upstream stream.
    let factoryCalls = 0;
    const subjects: Subject<number>[] = [];
    const sourceFactory = new (class {
      // Build a tiny custom Observable-like source that returns a fresh
      // subject each subscribe. Mirrors what `defer(() => new Subject())`
      // would do but lets the test reach into each instance.
      subscribe(observer: {
        next: (v: number) => void;
        error: (e: unknown) => void;
        complete?: () => void;
      }): { unsubscribe: () => void } {
        factoryCalls += 1;
        const subject = new Subject<number>();
        subjects.push(subject);
        const sub = subject.subscribe({
          next: (v) => {
            observer.next(v);
          },
          error: (e) => {
            observer.error(e);
          },
          complete: () => {
            observer.complete?.();
          },
        });
        return {
          unsubscribe: () => {
            sub.unsubscribe();
          },
        };
      }
    })();

    const store = createStore<number>(sourceFactory as unknown as Observable<number>, {
      name: "recovery-target",
      initial: 0,
    });

    const unsub1 = store.subscribe(() => undefined);
    expect(factoryCalls).toBe(1);
    // Trigger upstream error on the first subject — store should null
    // its internal subscription.
    subjects[0]?.error(new Error("first failed"));
    unsub1();

    // Re-subscribe — this MUST cause `ensureSubscribed` to attach a
    // new upstream subscription, so the factory is called a second time.
    const received: number[] = [];
    const unsub2 = store.subscribe(() => {
      received.push(store.getSnapshot());
    });
    expect(factoryCalls).toBe(2);
    subjects[1]?.next(99);
    expect(received).toContain(99);
    unsub2();
  });
});
