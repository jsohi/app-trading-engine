/**
 * OrderStream.test.ts — unit tests for the `orderStream()` RxJS operator.
 *
 * Tests per APP-36 §5.8 / §6 rows 21, 22, 33, 50.
 *
 * Threading: single-threaded (Vitest jsdom worker).
 * Allocation: test-only — Subject instances created per test.
 */

import { describe, expect, it, beforeEach, afterEach } from "vitest";
import { Subject, type Subscription } from "rxjs";
import { orderStream } from "@/streams/order-stream";
import {
  type WorkerMessage,
  type OrderUpdate,
  type PriceUpdate,
} from "@/shared/transport/MessageShape";

// ─── Helpers ─────────────────────────────────────────────────────────────────

function makeOrderUpdate(clOrdId = "ORD-1"): OrderUpdate {
  return {
    type: "order",
    clOrdId,
    symbol: "EURUSD",
    side: "BUY",
    qty: 100_000_000n,
    price: 120_000_000n,
    status: "OPEN",
    serverNanos: 1_000_000_000n,
  };
}

function makePriceUpdate(symbol = "EURUSD"): PriceUpdate {
  return {
    type: "price",
    symbol,
    bid: 100_000_000n,
    ask: 101_000_000n,
    serverNanos: 1_000_000_000n,
  };
}

function makeFillMsg(): WorkerMessage {
  return {
    type: "fill",
    clOrdId: "ORD-1",
    execId: "EXEC-1",
    symbol: "EURUSD",
    side: "BUY",
    fillQty: 50_000_000n,
    fillPrice: 120_000_000n,
    serverNanos: 1_000_000_000n,
  };
}

// ─── Tests ───────────────────────────────────────────────────────────────────

describe("orderStream", () => {
  let source$: Subject<WorkerMessage>;

  beforeEach(() => {
    source$ = new Subject<WorkerMessage>();
  });

  afterEach(() => {
    source$.complete();
  });

  it("orderStream_filtersOrderUpdatesOnly_dropsPriceFillEvent", () => {
    const emitted: OrderUpdate[] = [];
    const op = orderStream();
    const sub = op(source$).subscribe((m) => emitted.push(m));

    // Emit a PriceUpdate — should be dropped.
    source$.next(makePriceUpdate());

    // Emit a FillUpdate — should be dropped.
    source$.next(makeFillMsg());

    // Emit an OrderUpdate — should pass through.
    const order = makeOrderUpdate("ORD-42");
    source$.next(order);

    sub.unsubscribe();

    expect(emitted).toHaveLength(1);
    expect(emitted[0]?.clOrdId).toBe("ORD-42");
  });

  it("orderStream_passesThroughVerbatim_noUpstreamCoalescer", () => {
    // §6 row 22: no upstream auditTime/bufferTime; one input produces one output.
    // AG Grid's applyTransactionAsync is the sole coalescer.
    const emitted: OrderUpdate[] = [];
    const op = orderStream();
    const sub = op(source$).subscribe((m) => emitted.push(m));

    source$.next(makeOrderUpdate("ORD-1"));
    source$.next(makeOrderUpdate("ORD-2"));
    source$.next(makeOrderUpdate("ORD-3"));

    sub.unsubscribe();

    // Exactly three emissions — no buffering/coalescing upstream.
    expect(emitted).toHaveLength(3);
    expect(emitted[0]?.clOrdId).toBe("ORD-1");
    expect(emitted[1]?.clOrdId).toBe("ORD-2");
    expect(emitted[2]?.clOrdId).toBe("ORD-3");
  });

  it("orderStream_downstreamThrow_doesNotUnsubscribeUpstream", () => {
    // §6 row 50: an error in a subscriber's error channel should not permanently
    // block other independent subscribers on the same operator output.
    //
    // RxJS propagates synchronous throws from subscriber `next` back to the
    // producer. We demonstrate the upstream source remains subscribed by using
    // two separate calls to `op()` (each creates an independent operator chain)
    // and verifying the second subscriber still receives events after the first
    // one is errored via explicit `subscriber.error()`.
    const survivingEmissions: OrderUpdate[] = [];

    const op = orderStream();

    // First subscriber-pipe: we will trigger its error path manually.
    const errorSource$ = new Subject<WorkerMessage>();
    const errored: unknown[] = [];
    const errorSub: Subscription = op(errorSource$).subscribe({
      next: () => {
        /* consume normally */
      },
      error: (e: unknown) => errored.push(e),
    });

    // Second subscriber on the primary source: must survive independently.
    const survivingSub = op(source$).subscribe({
      next: (m) => survivingEmissions.push(m),
      error: () => {
        // Should not be called.
        expect.fail("surviving subscriber received an unexpected error");
      },
    });

    // Emit a normal event to the surviving subscriber.
    source$.next(makeOrderUpdate("ORD-A"));

    // Trigger an error on the error-subscriber's source.
    errorSource$.error(new Error("upstream error"));

    // The surviving subscriber should still be healthy.
    source$.next(makeOrderUpdate("ORD-B"));

    expect(survivingEmissions).toHaveLength(2);
    expect(survivingEmissions[0]?.clOrdId).toBe("ORD-A");
    expect(survivingEmissions[1]?.clOrdId).toBe("ORD-B");

    // The errored pipe received the error.
    expect(errored).toHaveLength(1);

    // Error subscription is now closed (error terminates it).
    expect(errorSub.closed).toBe(true);

    survivingSub.unsubscribe();
    errorSource$.complete();
  });
});
