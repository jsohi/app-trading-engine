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
import { type Observable, defer, timer } from "rxjs";
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
 *
 * Note on `Date.now()` below: this mock SYNTHESISES timestamps for
 * dev. The CLAUDE.md/plan prohibition on `Date.now()` targets
 * server-time comparisons (where wall-clock skew breaks ordering
 * invariants); generating a coherent `serverNanos` for a fake stream
 * has no such hazard. The real worker (APP-36) will use the
 * server-emitted `serverNanos` from SBE frames — never `Date.now()`.
 */
function lcg(seed: number): () => number {
  let state = seed >>> 0;
  return (): number => {
    state = (state * 1103515245 + 12345) >>> 0;
    return (state >>> 16) & 0x7fff;
  };
}

function makePrice(rng: () => number, symbol: string, base: bigint): PriceUpdate {
  // Synthetic jitter around `base` in fixed-point units. The LCG returns
  // 0..32767 (15-bit), so `rng() % 50_000` is just `rng()` — i.e. the
  // jitter range is asymmetric: -25_000..+7_767 fixed-point units, which
  // is roughly -0.000250..+0.000078 in float terms. Asymmetry is fine
  // for a mock — we don't need cosmetic symmetry, only deterministic
  // movement. Real prices (post-APP-36) come from the worker decoding
  // SBE frames; this code never runs in production.
  const jitter = BigInt(rng() % 50_000) - 25_000n;
  const bid = base + jitter;
  const ask = bid + 200_000n; // 2.0 pip spread
  const serverNanos = BigInt(Date.now()) * 1_000_000n;
  return {
    type: "price",
    symbol,
    bid,
    ask,
    // Phase 3 Commit B additions — mock stream is dev/test only; pin to zero defaults so
    // downstream consumers see well-formed PriceUpdate payloads.
    bidSize: 1_000_000_000_000n,
    askSize: 1_000_000_000_000n,
    ingressNanos: serverNanos - 5_000n, // ~5 µs synthetic publisher-stack latency
    serverNanos,
    publisherStackLatencyNanos: 5_000n,
    endToEndLatencyNanos: 0n, // mock stream is in-process; no network round-trip
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
 * Build a cold Observable of synthetic `WorkerMessage` values.
 * Each subscription gets its own independent timer and RNG state,
 * starting fresh at element 0. This makes the stream:
 *   - **Lazy**: no timer fires until someone subscribes — no leak if
 *     the result is never consumed.
 *   - **Restartable**: a unsubscribe + resubscribe cleanly restarts
 *     the stream from element 0; no dead-after-last-unsubscribe state.
 *   - **Test-friendly**: each test that subscribes gets a deterministic
 *     sequence from the seed, independent of other subscriptions.
 *
 * @param options optional tick interval / RNG seed.
 * @return cold Observable<WorkerMessage>; element 0 fires on subscribe,
 *   then every `intervalMs` until unsubscribe.
 */
export function fakeStream(options: FakeStreamOptions = {}): Observable<WorkerMessage> {
  const intervalMs = options.intervalMs ?? 250;
  const seed = options.seed ?? 0xc0ffee;

  return defer<Observable<WorkerMessage>>(() => {
    // Per-subscription state: each subscriber gets a fresh RNG + counter
    // so deterministic seeds produce identical sequences across runs and
    // across multiple subscribers within a single run.
    const rng = lcg(seed);
    let counter = 0;
    let seq = 0n;

    return timer(0, intervalMs).pipe(
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
    );
  });
}
