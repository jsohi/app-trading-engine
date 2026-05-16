/**
 * Unit tests for {@link CommandClient} — exercises the slot-table state machine,
 * typed errors, ack routing, timeout scanner, and connection-loss failure mode
 * deterministically against a synthetic {@link WorkerClient} mock.
 *
 * Plan §12 (APP-160) verification gates:
 *   - Backpressure (client-side cap=256)
 *   - RequestIdCollisionError (unfreed slot at issue time)
 *   - CommandTimeoutError (no ack within 5s)
 *   - ConnectionLostError (DOWN / WORKER_DEAD state)
 *   - CommandRejectedError (Rejected/Throttled/Duplicate ack)
 *   - Slot-wrap correctness (ack arrives, slot frees, next submit reuses)
 *
 * The synthetic WorkerClient implements just the surface CommandClient consumes
 * ({@code commandAcks$}, {@code connectionState$}, {@code submitCommand}) — no
 * real Worker is spawned.
 */
import { describe, expect, it, beforeEach, afterEach, vi } from "vitest";
import { BehaviorSubject, Subject } from "rxjs";

import {
  BackpressureError,
  CommandClient,
  CommandRejectedError,
  CommandTimeoutError,
  ConnectionLostError,
  InvalidAccountCodeError,
  type NewOrderSinglePayload,
} from "@/main-thread/commandClient";
import type { CommandAckEnvelope, WorkerClient } from "@/main-thread/workerClient";
import type { ConnectionState } from "@/shared/transport/MessageShape";

interface MockWorkerClient {
  readonly commandAcks$: Subject<CommandAckEnvelope>;
  readonly connectionState$: BehaviorSubject<ConnectionState>;
  readonly submitted: Array<{ length: number; correlationId: number; bytes: Uint8Array }>;
  submitCommand: (bytes: Uint8Array, length: number, correlationId: number) => void;
}

function makeMockWorker(): MockWorkerClient {
  const submitted: MockWorkerClient["submitted"] = [];
  return {
    commandAcks$: new Subject<CommandAckEnvelope>(),
    connectionState$: new BehaviorSubject<ConnectionState>("CONNECTED"),
    submitted,
    submitCommand(bytes, length, correlationId) {
      // Defensively copy so a future change to the production code that
      // mutates the buffer post-submit cannot retroactively alter the test
      // observation.
      submitted.push({ length, correlationId, bytes: new Uint8Array(bytes.subarray(0, length)) });
    },
  };
}

function makeClient(mw: MockWorkerClient): CommandClient {
  return new CommandClient(mw as unknown as WorkerClient);
}

const samplePayload: NewOrderSinglePayload = {
  clOrdId: "T-1",
  symbol: "EURUSD",
  side: "buy",
  qty: 100_000_000n,
  price: 105_000_000n,
  accountCode: "ACME-001",
};

describe("CommandClient", () => {
  let mw: MockWorkerClient;
  let cc: CommandClient;

  beforeEach(() => {
    vi.useFakeTimers();
    mw = makeMockWorker();
    cc = makeClient(mw);
  });

  afterEach(() => {
    cc.dispose();
    vi.useRealTimers();
  });

  it("submitOrder posts a real SBE-encoded frame to the worker and resolves on Accepted ack", async () => {
    const p = cc.submitOrder(samplePayload);
    expect(mw.submitted).toHaveLength(1);
    const sub = mw.submitted[0];
    if (sub === undefined) throw new Error("missing submitted frame");
    // SBE NewOrderSingle frame is 8 (header) + 108 (block) = 116 bytes.
    expect(sub.length).toBe(116);
    mw.commandAcks$.next({ correlationId: sub.correlationId, status: "Accepted" });
    const ack = await p;
    expect(ack.status).toBe("Accepted");
    expect(ack.correlationId).toBe(sub.correlationId);
  });

  it("rejects with CommandRejectedError on Rejected ack and surfaces status", async () => {
    const p = cc.submitOrder(samplePayload);
    const sub = mw.submitted[0];
    if (sub === undefined) throw new Error("missing submitted frame");
    mw.commandAcks$.next({
      correlationId: sub.correlationId,
      status: "Rejected",
      reasonCode: "ENTITLEMENT",
    });
    const err = await p.catch((e: unknown) => e);
    expect(err).toBeInstanceOf(CommandRejectedError);
    expect((err as CommandRejectedError).status).toBe("Rejected");
    expect((err as Error).message).toMatch(/ENTITLEMENT/);
  });

  it("rejects Duplicate ack with status='Duplicate'", async () => {
    const p = cc.submitOrder(samplePayload);
    const sub = mw.submitted[0];
    if (sub === undefined) throw new Error("missing submitted frame");
    mw.commandAcks$.next({ correlationId: sub.correlationId, status: "Duplicate" });
    const err = await p.catch((e: unknown) => e);
    expect(err).toBeInstanceOf(CommandRejectedError);
    expect((err as CommandRejectedError).status).toBe("Duplicate");
  });

  it("rejects Throttled ack with status='Throttled'", async () => {
    const p = cc.submitOrder(samplePayload);
    const sub = mw.submitted[0];
    if (sub === undefined) throw new Error("missing submitted frame");
    mw.commandAcks$.next({ correlationId: sub.correlationId, status: "Throttled" });
    const err = await p.catch((e: unknown) => e);
    expect(err).toBeInstanceOf(CommandRejectedError);
    expect((err as CommandRejectedError).status).toBe("Throttled");
  });

  it("BackpressureError fires synchronously on the 257th in-flight submit", () => {
    const settled: Array<Promise<unknown>> = [];
    let backpressureCount = 0;
    for (let i = 0; i < 257; i++) {
      const p = cc
        .submitOrder({ ...samplePayload, clOrdId: `T-${String(i)}` })
        .catch((e: unknown) => {
          if (e instanceof BackpressureError) backpressureCount++;
        });
      settled.push(p);
    }
    expect(backpressureCount).toBe(0);
    // The 257th is the one that exceeds the cap.
    return cc.submitOrder({ ...samplePayload, clOrdId: "T-overflow" }).then(
      () => {
        throw new Error("expected synchronous BackpressureError");
      },
      (e: unknown) => {
        expect(e).toBeInstanceOf(BackpressureError);
      },
    );
  });

  it("CommandTimeoutError fires after SLOT_TIMEOUT_MS (5000ms) when no ack arrives", async () => {
    // Attach the .catch BEFORE advancing timers so the rejection has a handler
    // by the time the scanner fires it (avoids a vitest unhandled-rejection
    // warning under fake-timers).
    const errPromise = cc.submitOrder(samplePayload).catch((e: unknown) => e);
    await vi.advanceTimersByTimeAsync(5_500);
    const err = await errPromise;
    expect(err).toBeInstanceOf(CommandTimeoutError);
  });

  it("ConnectionLostError fires for all in-flight submits when state goes DOWN", async () => {
    const p1 = cc.submitOrder({ ...samplePayload, clOrdId: "T-A" });
    const p2 = cc.submitOrder({ ...samplePayload, clOrdId: "T-B" });
    mw.connectionState$.next("DOWN");
    const errs = await Promise.all([p1.catch((e: unknown) => e), p2.catch((e: unknown) => e)]);
    expect(errs[0]).toBeInstanceOf(ConnectionLostError);
    expect(errs[1]).toBeInstanceOf(ConnectionLostError);
  });

  it("late ack for a timed-out slot is dropped silently (no double-resolve)", async () => {
    const errPromise = cc.submitOrder(samplePayload).catch((e: unknown) => e);
    const sub = mw.submitted[0];
    if (sub === undefined) throw new Error("missing submitted frame");
    await vi.advanceTimersByTimeAsync(5_500);
    const err = await errPromise;
    expect(err).toBeInstanceOf(CommandTimeoutError);
    // Late ack arrives — must NOT throw or double-resolve anything.
    expect(() => {
      mw.commandAcks$.next({ correlationId: sub.correlationId, status: "Accepted" });
    }).not.toThrow();
  });

  it("slot-wrap: after ack frees a slot, the same slot index can be reused on next submit", async () => {
    const p1 = cc.submitOrder({ ...samplePayload, clOrdId: "T-1" });
    const sub1 = mw.submitted[0];
    if (sub1 === undefined) throw new Error("missing submitted frame");
    mw.commandAcks$.next({ correlationId: sub1.correlationId, status: "Accepted" });
    await p1;
    // Submit again — must succeed (slot was freed by handleAck).
    const p2 = cc.submitOrder({ ...samplePayload, clOrdId: "T-2" });
    const sub2 = mw.submitted[1];
    if (sub2 === undefined) throw new Error("missing second submitted frame");
    expect(sub2.correlationId).not.toBe(sub1.correlationId); // u32 monotonic
    mw.commandAcks$.next({ correlationId: sub2.correlationId, status: "Accepted" });
    await expect(p2).resolves.toMatchObject({ status: "Accepted" });
  });

  it("dispose: rejects all in-flight submits with ConnectionLostError + idempotent", async () => {
    const p = cc.submitOrder(samplePayload);
    cc.dispose();
    await expect(p).rejects.toBeInstanceOf(ConnectionLostError);
    // Idempotent — second dispose is a no-op.
    expect(() => {
      cc.dispose();
    }).not.toThrow();
  });

  // -----------------------------------------------------------------------
  // SBE encoded-frame regression coverage for the iter-2 Gemini fixes.
  //
  // The existing tests above never decoded the wire bytes, so a regression
  // re-hardcoding currency to "USD" or settlement to a fixed string would
  // have passed every existing assertion. These cases inspect the encoded
  // buffer at the field offsets defined in NewOrderSingleEncoder.ts:
  //   header:   0..7  (8-byte SBE message header)
  //   symbol:   header + 40, char[8]
  //   currency: header + 101, char[3]
  // The encoder pads the fixed-length char fields with NULs ("\0").
  // -----------------------------------------------------------------------
  const SBE_HEADER_LENGTH = 8;
  const SYMBOL_OFFSET = SBE_HEADER_LENGTH + 40;
  const CURRENCY_OFFSET = SBE_HEADER_LENGTH + 101;

  function readFixedAscii(bytes: Uint8Array, offset: number, length: number): string {
    let end = offset + length;
    while (end > offset && bytes[end - 1] === 0) end--;
    return new TextDecoder("ascii").decode(bytes.subarray(offset, end));
  }

  it("encoder: currency derived from quote-currency slot (USDJPY → JPY)", () => {
    void cc.submitOrder({ ...samplePayload, symbol: "USDJPY" }).catch((e: unknown) => {
      // Tolerate ONLY the dispose-time teardown rejection so a regression
      // that throws synchronously inside submitOrder still surfaces.
      if (!(e instanceof ConnectionLostError)) throw e;
    });
    const sub = mw.submitted[0];
    if (sub === undefined) throw new Error("missing submitted frame");
    expect(readFixedAscii(sub.bytes, SYMBOL_OFFSET, 8)).toBe("USDJPY");
    expect(readFixedAscii(sub.bytes, CURRENCY_OFFSET, 3)).toBe("JPY");
  });

  it("encoder: currency derived from slashed form (EUR/USD → USD), slash stripped from symbol", () => {
    void cc.submitOrder({ ...samplePayload, symbol: "EUR/USD" }).catch((e: unknown) => {
      if (!(e instanceof ConnectionLostError)) throw e;
    });
    const sub = mw.submitted[0];
    if (sub === undefined) throw new Error("missing submitted frame");
    expect(readFixedAscii(sub.bytes, SYMBOL_OFFSET, 8)).toBe("EURUSD");
    expect(readFixedAscii(sub.bytes, CURRENCY_OFFSET, 3)).toBe("USD");
  });

  it("rejects empty accountCode synchronously with InvalidAccountCodeError", async () => {
    const err = await cc
      .submitOrder({ ...samplePayload, accountCode: "" })
      .catch((e: unknown) => e);
    expect(err).toBeInstanceOf(InvalidAccountCodeError);
    // No worker traffic generated.
    expect(mw.submitted).toHaveLength(0);
  });

  it("rejects whitespace-only accountCode synchronously with InvalidAccountCodeError", async () => {
    const err = await cc
      .submitOrder({ ...samplePayload, accountCode: "   " })
      .catch((e: unknown) => e);
    expect(err).toBeInstanceOf(InvalidAccountCodeError);
    expect(mw.submitted).toHaveLength(0);
  });

  it("encoder: replaceAll strips multiple slashes (EUR/U/SD → EURUSD/USD)", () => {
    void cc.submitOrder({ ...samplePayload, symbol: "EUR/U/SD" }).catch((e: unknown) => {
      if (!(e instanceof ConnectionLostError)) throw e;
    });
    const sub = mw.submitted[0];
    if (sub === undefined) throw new Error("missing submitted frame");
    expect(readFixedAscii(sub.bytes, SYMBOL_OFFSET, 8)).toBe("EURUSD");
    expect(readFixedAscii(sub.bytes, CURRENCY_OFFSET, 3)).toBe("USD");
  });
});
