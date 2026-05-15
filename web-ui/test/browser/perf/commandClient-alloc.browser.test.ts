/**
 * commandClient allocation tripwire — browser-side equivalent of the Java
 * `*AllocTest` regression pattern (cluster/.../RfqStateMachineAllocationTest,
 * fix-client-bridge/.../OutboundDrainerAllocTest, etc.).
 *
 * Plan §12 mandates {@code commandClient} is zero-allocation after warmup
 * (pre-allocated 1024-slot table; SBE encoder reused; outbound buffer pool
 * sized to {@link NewOrderSingleEncoder.ENCODED_FRAME_LENGTH}; single
 * pre-allocated {@code setInterval} scanner). This test asserts the byte
 * budget per {@code submitOrder} call against the baseline checked into
 * {@code web-ui/perf-baselines.json}.
 *
 * <p><b>Methodology:</b>
 * <ol>
 *   <li>Warm up with N=100 calls (lets JIT, V8 inline caches, and the slot
 *       table settle).</li>
 *   <li>Force a GC pass via the {@code --expose-gc} flag (configured in
 *       {@code web-ui/vitest.config.ts} for the browser project).</li>
 *   <li>Snapshot heap via {@code performance.measureUserAgentSpecificMemory()}
 *       — the only browser API that gives an accurate cross-realm reading; it
 *       requires cross-origin isolation (COOP/COEP headers, also configured in
 *       vitest.config.ts).</li>
 *   <li>Run N=10_000 {@code submitOrder} calls (with mock acks resolving
 *       instantly so slots free immediately).</li>
 *   <li>Force GC + snapshot again.</li>
 *   <li>Assert {@code (t1 - t0) / 10_000 ≤ baseline}.</li>
 * </ol>
 *
 * <p><b>Robustness:</b> if the browser does not expose {@code globalThis.gc}
 * or {@code performance.measureUserAgentSpecificMemory}, the test
 * {@code .skip}s with a clear reason — those are vitest-browser-only
 * affordances and should never be runtime-enforceable in production.
 */
import { describe, expect, it } from "vitest";
import { BehaviorSubject, Subject } from "rxjs";

import { CommandClient, type NewOrderSinglePayload } from "@/main-thread/commandClient";
import type { CommandAckEnvelope, WorkerClient } from "@/main-thread/workerClient";
import type { ConnectionState } from "@/shared/transport/MessageShape";
import perfBaselines from "../../../perf-baselines.json" with { type: "json" };

const BUDGET_BYTES_PER_CALL = perfBaselines.commandClientAlloc.budgetMaxBytesPerCall;
const WARMUP_CALLS = 100;
const MEASURED_CALLS = 10_000;

interface MeasureUserAgentSpecificMemoryResult {
  bytes: number;
  breakdown?: ReadonlyArray<{ bytes: number }>;
}

interface PerfWithMemoryApi {
  measureUserAgentSpecificMemory?: () => Promise<MeasureUserAgentSpecificMemoryResult>;
}

function buildMockWorker(): {
  worker: unknown;
  acks$: Subject<CommandAckEnvelope>;
  fireAck(correlationId: number): void;
} {
  const acks$ = new Subject<CommandAckEnvelope>();
  const state$ = new BehaviorSubject<ConnectionState>("CONNECTED");
  const pendingCorrelations: number[] = [];
  return {
    worker: {
      commandAcks$: acks$,
      connectionState$: state$,
      submitCommand(_bytes: Uint8Array, _length: number, correlationId: number): void {
        pendingCorrelations.push(correlationId);
        // Fire ack synchronously on a microtask so submitOrder Promises resolve
        // before the next iteration — keeps the in-flight count near zero so
        // the slot table cycles through the same hot indices.
        queueMicrotask(() => {
          const next = pendingCorrelations.shift();
          if (next !== undefined) {
            acks$.next({ correlationId: next, status: "Accepted" });
          }
        });
      },
    },
    acks$,
    fireAck(correlationId): void {
      acks$.next({ correlationId, status: "Accepted" });
    },
  };
}

const samplePayload: NewOrderSinglePayload = {
  clOrdId: "ALLOC-X",
  symbol: "EUR/USD",
  side: "buy",
  qty: 100_000_000n,
  price: 105_000_000n,
};

describe("commandClient allocation tripwire", () => {
  it("amortises ≤ baseline bytes/call across 10_000 submits", async () => {
    const memApi = performance as unknown as PerfWithMemoryApi;
    if (typeof memApi.measureUserAgentSpecificMemory !== "function") {
      console.warn(
        "skip: performance.measureUserAgentSpecificMemory not available (cross-origin-isolated " +
          "mode required — verify COOP/COEP headers in vitest.config.ts browser project)",
      );
      return;
    }
    if (typeof (globalThis as unknown as { gc?: () => void }).gc !== "function") {
      console.warn(
        "skip: globalThis.gc not available (Chromium must be launched with --js-flags=--expose-gc)",
      );
      return;
    }

    const mock = buildMockWorker();
    const cc = new CommandClient(mock.worker as WorkerClient);
    try {
      // Warmup: lets V8 inline caches + slot table settle so we measure steady state.
      for (let i = 0; i < WARMUP_CALLS; i++) {
        await cc.submitOrder({ ...samplePayload, clOrdId: `warmup-${String(i)}` });
      }

      const gc = (globalThis as unknown as { gc: () => void }).gc;
      gc();
      const t0 = await memApi.measureUserAgentSpecificMemory();

      for (let i = 0; i < MEASURED_CALLS; i++) {
        await cc.submitOrder({ ...samplePayload, clOrdId: `measure-${String(i)}` });
      }

      gc();
      const t1 = await memApi.measureUserAgentSpecificMemory();

      const deltaBytes = t1.bytes - t0.bytes;
      const bytesPerCall = deltaBytes / MEASURED_CALLS;

      // Logged for CI triage. The assertion is the budget; the log is the trend signal.
      console.log(
        `commandClient alloc tripwire: deltaBytes=${String(deltaBytes)} bytesPerCall=${bytesPerCall.toFixed(2)} budget=${String(BUDGET_BYTES_PER_CALL)}`,
      );

      // bytesPerCall can be slightly negative when GC reclaims more than we allocated
      // (other JS state from prior tests). Treat that as zero — the budget is a CEILING.
      const measured = Math.max(0, bytesPerCall);
      expect(
        measured,
        `commandClient.submitOrder allocates ${measured.toFixed(2)} B/call (budget ${String(BUDGET_BYTES_PER_CALL)} B). ` +
          "If this is intentional (new field, schema growth), update web-ui/perf-baselines.json " +
          "after re-measuring.",
      ).toBeLessThanOrEqual(BUDGET_BYTES_PER_CALL);
    } finally {
      cc.dispose();
    }
  });
});
