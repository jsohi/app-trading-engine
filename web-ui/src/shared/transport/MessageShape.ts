/**
 * Worker → Main thread message contract.
 *
 * This is the **single source of truth** for the Web Worker (APP-36)
 * `postMessage` payload shape. Every Phase 2 panel imports types from
 * here:
 *
 *   - APP-37 (Orders/Positions/Quotes blotters) consumes
 *     `OrderUpdate`, `FillUpdate`, `PriceUpdate`.
 *   - APP-42 (Event log) consumes `EventUpdate`.
 *   - APP-36 (Worker) MUST emit values structurally assignable to
 *     `WorkerMessage`. The mock `fakeStream.ts` emits the same shape
 *     so feature work can develop without the worker.
 *
 * Conventions:
 *   - `bigint` for SBE int64 / uint64 fields (prices, quantities,
 *     timestamps in epoch nanos). `Number()` coercion is BANNED —
 *     ESLint enforces (eslint.config.js: no-restricted-syntax).
 *   - Fixed-point pricing: `price` and similar are scaled by
 *     `100_000_000n` (PRICE_SCALE in @trading/sbe-codecs).
 *   - Timestamps: `serverNanos` is epoch nanoseconds as bigint;
 *     convert to `Date` only at render boundaries.
 */

export type Side = "BUY" | "SELL";

export interface PriceUpdate {
  readonly type: "price";
  readonly symbol: string;
  /** Fixed-point bid (scaled by 1e8). */
  readonly bid: bigint;
  /** Fixed-point ask (scaled by 1e8). */
  readonly ask: bigint;
  /** Server-side emit timestamp in epoch nanoseconds. */
  readonly serverNanos: bigint;
}

export interface OrderUpdate {
  readonly type: "order";
  readonly clOrdId: string;
  readonly symbol: string;
  readonly side: Side;
  /** Fixed-point quantity (scaled by 1e8). */
  readonly qty: bigint;
  /** Fixed-point limit price (scaled by 1e8). */
  readonly price: bigint;
  /** OPEN | PARTIAL | FILLED | CANCELLED | REJECTED. */
  readonly status: "OPEN" | "PARTIAL" | "FILLED" | "CANCELLED" | "REJECTED";
  readonly serverNanos: bigint;
}

export interface FillUpdate {
  readonly type: "fill";
  readonly clOrdId: string;
  readonly execId: string;
  readonly symbol: string;
  readonly side: Side;
  /** Fixed-point fill quantity (scaled by 1e8). */
  readonly fillQty: bigint;
  /** Fixed-point fill price (scaled by 1e8). */
  readonly fillPrice: bigint;
  readonly serverNanos: bigint;
}

export interface EventUpdate {
  readonly type: "event";
  /** Cluster sequence number — uint64 → bigint. */
  readonly seq: bigint;
  /** Short event type code (e.g. "OrderAccepted", "QuoteRequested"). */
  readonly eventType: string;
  /** Free-form details — schema-defined per event type. */
  readonly details: string;
  readonly serverNanos: bigint;
}

export type WorkerMessage = PriceUpdate | OrderUpdate | FillUpdate | EventUpdate;
