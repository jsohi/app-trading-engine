/**
 * In-process synthetic stream emitting price / order / fill / event
 * updates on a timer in the EXACT same shape the Web Worker
 * (APP-36) will postMessage. This is the parallel-work contract:
 * every Phase 2 panel imports types from
 * `@/shared/transport/MessageShape` and consumes structurally
 * equivalent data here.
 *
 * Threading model: main thread (RxJS Subject), single producer.
 *
 * Allocation: emits new objects per tick (acceptable in dev/mock
 * path; the real worker is the hot path that must avoid this).
 */
import { Observable, Subject, timer } from "rxjs";
import { map } from "rxjs/operators";

import {
  type EventUpdate,
  type FillUpdate,
  type OrderUpdate,
  type PriceUpdate,
  type WorkerMessage,
} from "@/shared/transport/MessageShape";

const PRICE_SCALE = 100_000_000n; // matches @trading/sbe-codecs PRICE_SCALE
const SYMBOLS: readonly string[] = ["EUR/USD", "GBP/USD", "USD/JPY"];

/**
 * Deterministic pseudo-random sequence so test runs are
 * reproducible. Linear congruential generator — NOT for security
 * use. Seeded once per `fakeStream()` call.
 */
function lcg(seed: number): () => number {
  let state = seed >>> 0;
  return (): number => {
    state = (state * 1103515245 + 12345) >>> 0;
    return (state >>> 16) & 0x7fff;
  };
}

function makePrice(rng: () => number, symbol: string, base: bigint): PriceUpdate {
  // Drift bid/ask by ±0.0005 around `base` (in fixed-point units).
  const jitter = BigInt(rng() % 50_000) - 25_000n;
  const bid = base + jitter;
  const ask = bid + 200_000n; // 2.0 pip spread
  return {
    type: "price",
    symbol,
    bid,
    ask,
    serverNanos: BigInt(Date.now()) * 1_000_000n,
  };
}

function makeOrder(rng: () => number, idx: number): OrderUpdate {
  const symbolIdx = rng() % SYMBOLS.length;
  return {
    type: "order",
    clOrdId: `MOCK-ORD-${String(idx).padStart(6, "0")}`,
    symbol: SYMBOLS[symbolIdx] ?? "EUR/USD",
    side: rng() % 2 === 0 ? "BUY" : "SELL",
    qty: BigInt(1_000_000) * PRICE_SCALE,
    price: BigInt(108_500_000),
    status: "OPEN",
    serverNanos: BigInt(Date.now()) * 1_000_000n,
  };
}

function makeFill(rng: () => number, idx: number): FillUpdate {
  const symbolIdx = rng() % SYMBOLS.length;
  return {
    type: "fill",
    clOrdId: `MOCK-ORD-${String(idx).padStart(6, "0")}`,
    execId: `MOCK-EXEC-${String(idx).padStart(6, "0")}`,
    symbol: SYMBOLS[symbolIdx] ?? "EUR/USD",
    side: rng() % 2 === 0 ? "BUY" : "SELL",
    fillQty: BigInt(500_000) * PRICE_SCALE,
    fillPrice: BigInt(108_510_000),
    serverNanos: BigInt(Date.now()) * 1_000_000n,
  };
}

function makeEvent(rng: () => number, seq: bigint): EventUpdate {
  const symbolIdx = rng() % SYMBOLS.length;
  return {
    type: "event",
    seq,
    eventType: "OrderAccepted",
    details: `${SYMBOLS[symbolIdx] ?? "EUR/USD"} BUY 1.0M`,
    serverNanos: BigInt(Date.now()) * 1_000_000n,
  };
}

export interface FakeStreamOptions {
  /** Tick interval in ms. Defaults to 250. */
  readonly intervalMs?: number;
  /** RNG seed for reproducibility. Defaults to 0xC0FFEE. */
  readonly seed?: number;
}

/**
 * Build a hot Observable of synthetic `WorkerMessage` values.
 * Each subscription drives the same upstream Subject.
 *
 * @param options optional tick interval / RNG seed.
 * @return Observable<WorkerMessage> emitting roughly every
 *   `intervalMs` until the subject completes (never, in dev).
 */
export function fakeStream(options: FakeStreamOptions = {}): Observable<WorkerMessage> {
  const intervalMs = options.intervalMs ?? 250;
  const rng = lcg(options.seed ?? 0xc0ffee);
  const subject = new Subject<WorkerMessage>();

  let counter = 0;
  let seq = 0n;
  const ticker = timer(0, intervalMs)
    .pipe(
      map<number, WorkerMessage>(() => {
        counter += 1;
        seq += 1n;
        const which = rng() % 4;
        switch (which) {
          case 0: {
            const symbolIdx = rng() % SYMBOLS.length;
            const symbol = SYMBOLS[symbolIdx] ?? "EUR/USD";
            const base = symbol === "USD/JPY" ? BigInt(149_200_000) * 100n : BigInt(108_500_000);
            return makePrice(rng, symbol, base);
          }
          case 1:
            return makeOrder(rng, counter);
          case 2:
            return makeFill(rng, counter);
          default:
            return makeEvent(rng, seq);
        }
      }),
    )
    .subscribe({
      next: (msg) => {
        subject.next(msg);
      },
      error: (err: unknown) => {
        subject.error(err);
      },
      complete: () => {
        subject.complete();
      },
    });

  return new Observable<WorkerMessage>((observer) => {
    const sub = subject.subscribe(observer);
    return () => {
      sub.unsubscribe();
      // Last subscriber → also stop the underlying timer subscription
      // to prevent leaks when the dev server hot-reloads.
      if (!subject.observed) {
        ticker.unsubscribe();
      }
    };
  });
}
