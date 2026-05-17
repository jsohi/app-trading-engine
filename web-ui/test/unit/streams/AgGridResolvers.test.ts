/**
 * AgGridResolvers.test.ts — unit tests for the AG Grid `getRowId` pure functions.
 *
 * Tests per APP-36 §5.5 / §6 rows 32, 33.
 *
 * Threading: single-threaded (Vitest jsdom worker).
 * Allocation: zero (pure functions; no allocation tested).
 */

import { describe, expect, it } from "vitest";
import { getOrderRowId, getPriceRowId, getFillRowId } from "@/streams/agGridResolvers";
import {
  type OrderUpdate,
  type PriceUpdate,
  type FillUpdate,
} from "@/shared/transport/MessageShape";

// ─── Tests ───────────────────────────────────────────────────────────────────

describe("agGridResolvers", () => {
  it("getOrderRowId_returnsClOrdId", () => {
    const order: OrderUpdate = {
      type: "order",
      clOrdId: "ORD-12345",
      symbol: "EURUSD",
      side: "BUY",
      qty: 100_000_000n,
      price: 120_000_000n,
      status: "OPEN",
      serverNanos: 1_000_000_000n,
    };

    expect(getOrderRowId(order)).toBe("ORD-12345");
  });

  it("getPriceRowId_returnsSymbol", () => {
    const price: PriceUpdate = {
      type: "price",
      symbol: "GBPUSD",
      bid: 130_000_000n,
      ask: 131_000_000n,
      bidSize: 0n,
      askSize: 0n,
      ingressNanos: 0n,
      serverNanos: 1_000_000_000n,
      publisherStackLatencyNanos: 0n,
      endToEndLatencyNanos: 0n,
    };

    expect(getPriceRowId(price)).toBe("GBPUSD");
  });

  it("getFillRowId_returnsExecId", () => {
    const fill: FillUpdate = {
      type: "fill",
      clOrdId: "ORD-1",
      execId: "EXEC-99",
      symbol: "EURUSD",
      side: "SELL",
      fillQty: 50_000_000n,
      fillPrice: 120_000_000n,
      serverNanos: 1_000_000_000n,
    };

    expect(getFillRowId(fill)).toBe("EXEC-99");
  });
});
