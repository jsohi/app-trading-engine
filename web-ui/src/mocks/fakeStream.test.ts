/**
 * Fixture-driven test for the parallel-work contract: every emitted
 * value from `fakeStream()` matches the `WorkerMessage` shape that
 * APP-36's worker MUST also produce.
 */
import { describe, expect, it } from "vitest";
import { firstValueFrom } from "rxjs";

import { fakeStream } from "./fakeStream";
import { type WorkerMessage } from "@/shared/transport/MessageShape";

function isWorkerMessage(msg: unknown): msg is WorkerMessage {
  if (typeof msg !== "object" || msg === null) return false;
  const m = msg as Record<string, unknown>;
  if (typeof m.type !== "string") return false;
  if (typeof m.serverNanos !== "bigint") return false;
  switch (m.type) {
    case "price":
      return typeof m.symbol === "string" && typeof m.bid === "bigint" && typeof m.ask === "bigint";
    case "order":
      return (
        typeof m.clOrdId === "string" &&
        typeof m.symbol === "string" &&
        (m.side === "BUY" || m.side === "SELL") &&
        typeof m.qty === "bigint" &&
        typeof m.price === "bigint" &&
        typeof m.status === "string"
      );
    case "fill":
      return (
        typeof m.clOrdId === "string" &&
        typeof m.execId === "string" &&
        typeof m.fillQty === "bigint" &&
        typeof m.fillPrice === "bigint"
      );
    case "event":
      return (
        typeof m.seq === "bigint" &&
        typeof m.eventType === "string" &&
        typeof m.details === "string"
      );
    default:
      return false;
  }
}

describe("fakeStream", () => {
  it("emit_firstTick_matchesWorkerMessageShape", async () => {
    const stream = fakeStream({ intervalMs: 1, seed: 42 });
    const first = await firstValueFrom(stream);
    expect(isWorkerMessage(first)).toBe(true);
  });

  it("emit_first50Ticks_allMatchWorkerMessageShape", async () => {
    const stream = fakeStream({ intervalMs: 1, seed: 7 });
    const collected: WorkerMessage[] = [];
    await new Promise<void>((resolve, reject) => {
      const sub = stream.subscribe({
        next: (msg) => {
          collected.push(msg);
          if (collected.length >= 50) {
            sub.unsubscribe();
            resolve();
          }
        },
        error: reject,
      });
    });
    for (const msg of collected) {
      expect(isWorkerMessage(msg)).toBe(true);
    }
  });
});
