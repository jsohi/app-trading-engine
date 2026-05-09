/**
 * EventLogStream.test.ts — unit tests for the `eventLogStream()` ring-buffer operator.
 *
 * Tests per APP-36 §5.8 / §6 rows 21, 49.
 *
 * Threading: single-threaded (Vitest jsdom worker).
 * Allocation: test-only — Subject and EventLogSnapshot instances per test.
 */

import { describe, expect, it, beforeEach, afterEach } from "vitest";
import { Subject } from "rxjs";
import { eventLogStream } from "@/streams/event-log-stream";
import { type EventLogSnapshot } from "@/streams/api";
import {
  type WorkerMessage,
  type EventUpdate,
  type PriceUpdate,
} from "@/shared/transport/MessageShape";

// ─── Helpers ─────────────────────────────────────────────────────────────────

function makeEventUpdate(seq = 1n, eventType = "OrderAccepted"): EventUpdate {
  return {
    type: "event",
    seq,
    eventType,
    details: `details-${seq.toString()}`,
    serverNanos: seq * 1_000_000n,
  };
}

function makePriceUpdate(): PriceUpdate {
  return {
    type: "price",
    symbol: "EURUSD",
    bid: 100_000_000n,
    ask: 101_000_000n,
    serverNanos: 1_000_000_000n,
  };
}

function makeOrderMsg(): WorkerMessage {
  return {
    type: "order",
    clOrdId: "ORD-1",
    symbol: "EURUSD",
    side: "BUY",
    qty: 100_000_000n,
    price: 120_000_000n,
    status: "OPEN",
    serverNanos: 1_000_000_000n,
  };
}

// ─── Tests ───────────────────────────────────────────────────────────────────

describe("eventLogStream", () => {
  let source$: Subject<WorkerMessage>;

  beforeEach(() => {
    source$ = new Subject<WorkerMessage>();
  });

  afterEach(() => {
    source$.complete();
  });

  it("eventLogStream_capacity10000_default", () => {
    // Default capacity is 10 000; verify operator constructs and emits.
    const emitted: EventLogSnapshot[] = [];
    const op = eventLogStream();
    const sub = op(source$).subscribe((s) => emitted.push(s));

    // Emit one event — should appear.
    source$.next(makeEventUpdate(1n));

    sub.unsubscribe();

    expect(emitted).toHaveLength(1);
    expect(emitted[0]?.count).toBe(1);
    expect(emitted[0]?.capacity).toBe(10_000);
    expect(emitted[0]?.read(0)).toBeDefined();
    expect(emitted[0]?.version).toBe(1);
  });

  it("eventLogStream_backpressure_capacity2000", () => {
    // backpressure flag reduces capacity to 2 000 per §2.9.
    const emitted: EventLogSnapshot[] = [];
    const op = eventLogStream({ backpressure: true });
    const sub = op(source$).subscribe((s) => emitted.push(s));

    // Fill to 2 001 entries — the ring should hold at most 2 000.
    for (let i = 0; i < 2001; i++) {
      source$.next(makeEventUpdate(BigInt(i + 1)));
    }

    sub.unsubscribe();

    expect(emitted).toHaveLength(2001);
    // The last snapshot should have exactly 2 000 visible entries.
    const last = emitted[emitted.length - 1];
    expect(last).toBeDefined();
    expect(last?.count).toBe(2000);
    expect(last?.capacity).toBe(2000);
  });

  it("eventLogStream_overCapacity_dropsOldest_versionMonotonic", () => {
    // Capacity 3; emit 5 events — ring drops oldest entries.
    // Snapshot identity is stable across emissions, so `version` must be
    // captured at observe-time (not via post-hoc array inspection).
    const observedVersions: number[] = [];
    const observedCounts: number[] = [];
    let emissions = 0;
    const op = eventLogStream({ capacity: 3 });
    const sub = op(source$).subscribe((s) => {
      observedVersions.push(s.version);
      observedCounts.push(s.count);
      emissions += 1;
    });

    for (let i = 1; i <= 5; i++) {
      source$.next(makeEventUpdate(BigInt(i), `Event${String(i)}`));
    }

    sub.unsubscribe();

    expect(emissions).toBe(5);
    // Version must be strictly monotonically increasing (1, 2, 3, 4, 5).
    expect(observedVersions).toEqual([1, 2, 3, 4, 5]);
    // Count saturates at capacity.
    expect(observedCounts).toEqual([1, 2, 3, 3, 3]);
  });

  it("eventLogStream_snapshotRefStable_acrossEmissions", () => {
    // §6 row 49: the snapshot object reference is stable across emissions —
    // consumers diff via the `version` counter.
    const emitted: EventLogSnapshot[] = [];
    const op = eventLogStream({ capacity: 10 });
    const sub = op(source$).subscribe((s) => emitted.push(s));

    source$.next(makeEventUpdate(1n));
    source$.next(makeEventUpdate(2n));

    sub.unsubscribe();

    expect(emitted).toHaveLength(2);
    // Both emissions share the same snapshot identity (Gemini fix).
    expect(emitted[0]).toBe(emitted[1]);
    // version differs across emissions even though identity is stable
    // (we captured by ref so the final value is observed both times).
    expect(emitted[1]?.version).toBe(2);
  });

  it("eventLogStream_filtersEventTypeOnly_dropsPriceOrder", () => {
    // Only `type === 'event'` messages enter the ring; price and order are dropped.
    const emitted: EventLogSnapshot[] = [];
    const op = eventLogStream();
    const sub = op(source$).subscribe((s) => emitted.push(s));

    // Non-event messages — should produce no emissions.
    source$.next(makePriceUpdate());
    source$.next(makeOrderMsg());

    // One real event.
    source$.next(makeEventUpdate(1n));

    sub.unsubscribe();

    // Only the EventUpdate produces an emission.
    expect(emitted).toHaveLength(1);
    expect(emitted[0]?.version).toBe(1);
  });
});
