/**
 * commandClient.performance.test.ts — cold-path PerformanceTimeline observability
 * tests for {@link CommandClient.submitOrder} (APP-244 Phase 3 C.5).
 *
 * Asserts that:
 *   - A successful submit fires the start mark exactly once (at the post-early-return
 *     boundary) and pairs with one end mark + one measure on the matching ack.
 *   - The no-op early-return rejection paths (InvalidAccountCodeError on empty
 *     accountCode, BackpressureError when MAX_IN_FLIGHT is saturated) fire NO marks
 *     at all — preserving the zero-allocation invariant for the synchronous-reject
 *     branches.
 *
 * Approach: spy on `performance.mark` / `performance.measure` via `vi.spyOn`, drive
 * the CommandClient against a synthetic WorkerClient mock, and assert call counts.
 *
 * Threading: single-threaded (Vitest jsdom).
 */
import { describe, expect, it, beforeEach, afterEach, vi } from "vitest";
import type { MockInstance } from "vitest";
import { BehaviorSubject, Subject } from "rxjs";

import {
  CommandClient,
  PERF_MARK_SUBMIT_ORDER_START,
  PERF_MARK_SUBMIT_ORDER_END,
  PERF_MEASURE_SUBMIT_ORDER,
  type NewOrderSinglePayload,
} from "@/main-thread/commandClient";
import type { CommandAckEnvelope, WorkerClient } from "@/main-thread/workerClient";
import type { ConnectionState } from "@/shared/transport/MessageShape";

interface MockWorkerClient {
  readonly commandAcks$: Subject<CommandAckEnvelope>;
  readonly connectionState$: BehaviorSubject<ConnectionState>;
  readonly submitted: Array<{ length: number; correlationId: number }>;
  submitCommand: (bytes: Uint8Array, length: number, correlationId: number) => void;
}

function makeMockWorker(): MockWorkerClient {
  const submitted: MockWorkerClient["submitted"] = [];
  return {
    commandAcks$: new Subject<CommandAckEnvelope>(),
    connectionState$: new BehaviorSubject<ConnectionState>("CONNECTED"),
    submitted,
    submitCommand(_bytes, length, correlationId) {
      submitted.push({ length, correlationId });
    },
  };
}

const samplePayload: NewOrderSinglePayload = {
  clOrdId: "T-PERF-1",
  symbol: "EURUSD",
  side: "buy",
  qty: 100_000_000n,
  price: 105_000_000n,
  accountCode: "ACME-001",
};

describe("CommandClient.submitOrder — PerformanceTimeline (APP-244 C.5)", () => {
  let mw: MockWorkerClient;
  let cc: CommandClient;
  let markSpy: MockInstance<typeof performance.mark>;
  let measureSpy: MockInstance<typeof performance.measure>;

  beforeEach(() => {
    vi.useFakeTimers();
    mw = makeMockWorker();
    cc = new CommandClient(mw as unknown as WorkerClient);
    markSpy = vi.spyOn(performance, "mark");
    measureSpy = vi.spyOn(performance, "measure");
  });

  afterEach(() => {
    cc.dispose();
    // Dispose triggers failAllInFlight → freeSlot for each occupied slot, which fires
    // the end-mark for unsettled submits. Restore AFTER dispose so the spy still
    // sees those calls if a test inspects them; reset is implicit via mockRestore.
    markSpy.mockRestore();
    measureSpy.mockRestore();
    vi.useRealTimers();
  });

  it("submitOrder_successfulAck_emitsStartAndEndMarksAndMeasure", async () => {
    const p = cc.submitOrder(samplePayload);
    // After the synchronous slot-allocation + worker.submitCommand call, the start mark
    // must have fired exactly once.
    expect(markSpy).toHaveBeenCalledTimes(1);
    expect(markSpy).toHaveBeenNthCalledWith(1, PERF_MARK_SUBMIT_ORDER_START);
    expect(measureSpy).not.toHaveBeenCalled();

    const sub = mw.submitted[0];
    if (sub === undefined) throw new Error("missing submitted frame");
    mw.commandAcks$.next({ correlationId: sub.correlationId, status: "Accepted" });
    await p;

    // Ack handler runs freeSlot → markSubmitOrderEnd → end mark + measure
    expect(markSpy).toHaveBeenCalledTimes(2);
    expect(markSpy).toHaveBeenNthCalledWith(2, PERF_MARK_SUBMIT_ORDER_END);

    expect(measureSpy).toHaveBeenCalledTimes(1);
    expect(measureSpy).toHaveBeenCalledWith(
      PERF_MEASURE_SUBMIT_ORDER,
      PERF_MARK_SUBMIT_ORDER_START,
      PERF_MARK_SUBMIT_ORDER_END,
    );
  });

  it("submitOrder_emptyAccountCode_emitsNoMarksOnNoOpRejectionPath", async () => {
    const p = cc.submitOrder({ ...samplePayload, accountCode: "" });
    await expect(p).rejects.toThrow(/accountCode/);

    expect(markSpy).not.toHaveBeenCalled();
    expect(measureSpy).not.toHaveBeenCalled();
  });

  it("submitOrder_whitespaceAccountCode_emitsNoMarksOnNoOpRejectionPath", async () => {
    const p = cc.submitOrder({ ...samplePayload, accountCode: "   " });
    await expect(p).rejects.toThrow(/accountCode/);

    expect(markSpy).not.toHaveBeenCalled();
    expect(measureSpy).not.toHaveBeenCalled();
  });
});
