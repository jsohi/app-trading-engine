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
import { Subject } from "rxjs";

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
});
