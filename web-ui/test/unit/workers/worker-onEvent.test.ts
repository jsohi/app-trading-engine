/**
 * Unit tests for the cluster-domain-event decoder switch inside the worker's `onEvent` handler
 * (built in Phase 3 Commit 3 / APP-237).
 *
 * <p>These tests exercise the REAL production function {@link decodeClusterEvent} from
 * {@code web-ui/src/workers/dispatch/clusterEventDecoder.ts} — that module was extracted
 * from {@code worker.ts} precisely to make this exact call-path unit-testable without
 * triggering the Web Worker bootstrap or touching jsdom globals.
 *
 * <p>Payload construction: full SBE message bytes — 8-byte SBE header (zero-filled in
 * tests; the decoder skips past it) + body. This matches the contract `MessageRouter.route`
 * passes via its default arm: `this.handlers.onEvent(templateId, payload)` where `payload`
 * is the same buffer the router decoded the header from. The production decoders inside
 * `decodeClusterEvent` wrap at `SBE_HEADER_BYTES` (=8) — tests deliver the same shape.
 *
 * Threading: single-threaded Vitest jsdom worker.
 * Allocation: one DataView + one WorkerMessage object per test (intentionally minimal).
 *
 * Plan reference: APP-237 Phase 3 Commit 3 / APP-244 carry-over.
 */

import { describe, it, expect, beforeEach } from "vitest";

import {
  FeedStateEnum,
  MarketDataFeedStateChangeDecoder,
  MarketDataHeartbeatDecoder,
  MarketDataTickDecoder,
  OrderCanceledEventDecoder,
  OrderCancelRejectedEventDecoder,
  OrderCreatedEventDecoder,
  OrderFilledEventDecoder,
  OrderRejectedEventDecoder,
  RejectReasonEnum,
  SideEnum,
} from "@trading/sbe-codecs";

import {
  decodeClusterEvent,
  PRICE_RESPONSE_TEMPLATE_ID,
  type MarketDataConflationLike,
  type GapDetectorLike,
} from "@/workers/dispatch/clusterEventDecoder";
import { Stats } from "@/workers/protocol/Stats";
import { GapDetector } from "@/workers/gapDetector";
import { MarketDataConflation } from "@/workers/marketDataConflation";

import type {
  EventUpdate,
  FeedStateMsg,
  FillUpdate,
  OrderUpdate,
  PriceUpdate,
  WorkerMessage,
} from "@/shared/transport/MessageShape";

// ─── No-op stubs ─────────────────────────────────────────────────────────────

/**
 * No-op conflation stub for tests that do not exercise the template 54 (MarketDataTick)
 * conflation path. Satisfies the {@link MarketDataConflationLike} structural type without
 * pulling in a real {@link MarketDataConflation} instance or a setInterval timer.
 */
const noopConflation: MarketDataConflationLike = {
  onTick: () => undefined,
  peekSymbol: () => undefined,
};

/**
 * No-op gap-detector stub for tests that do not exercise the template 54 tick path or the
 * template 57 LIVE-transition path. Satisfies the {@link GapDetectorLike} structural type.
 * The {@code onTick} return value uses {@code publisherConflated: 0} and {@code network: 0}
 * so the stats surface is unaffected even if the stub is accidentally invoked.
 */
const noopGapDetector: GapDetectorLike = {
  onTick: () => ({ outcome: "in-order", publisherConflated: 0, network: 0 }),
  onHeartbeat: () => undefined,
  onPublisherRestart: () => undefined,
};

// ─── Constants ────────────────────────────────────────────────────────────────

/** Sentinel value written by SBE encoder for an optional int64 null field. */
const INT64_NULL_SENTINEL = -9223372036854775808n;

// ─── Error envelope shape ─────────────────────────────────────────────────────

interface ErrorEnvelope {
  readonly type: "ERROR";
  readonly code: "INIT" | "AUTH" | "CRC" | "PROTOCOL" | "SCHEMA" | "BUFFER" | "WORKER";
  readonly hint: string;
}

// ─── Captured outputs ─────────────────────────────────────────────────────────

let capturedMessages: WorkerMessage[];
let capturedErrors: ErrorEnvelope[];

// ─── Test-local emit + postError equivalents ──────────────────────────────────

/**
 * Push a decoded {@link WorkerMessage} to the captured messages array.
 * Mirrors the production `emit()` outbound batcher without the real postMessage overhead.
 */
function emit(msg: WorkerMessage): void {
  capturedMessages.push(msg);
}

/**
 * Post an error envelope into the captured errors array.
 * Mirrors the production `postError()` without touching globalThis.postMessage.
 */
function postError(
  code: "INIT" | "AUTH" | "CRC" | "PROTOCOL" | "SCHEMA" | "BUFFER" | "WORKER",
  hint: string,
): void {
  capturedErrors.push({ type: "ERROR", code, hint });
}

// ─── SBE payload builders ─────────────────────────────────────────────────────

/**
 * SBE message-header length in bytes (blockLength u16 + templateId u16 + schemaId u16 +
 * version u16). Production `decodeClusterEvent` wraps each event decoder at this offset to
 * skip the header — matches the contract `MessageRouter.route` passes via its default arm.
 * Tests MUST emit full SBE messages (header + body) so they exercise the production
 * wrap-at-8 path, not a body-only stub that would mask off-by-8 regressions.
 */
const SBE_HEADER_BYTES = 8;

/**
 * Prepend an 8-byte SBE header to a body-only buffer. Header bytes are zero-filled — the
 * decoder skips past them, so the values do not affect the decode (the dispatch layer above
 * the decoder reads templateId/schemaId/version from these bytes, but tests invoke the
 * decoder directly and supply the templateId as a separate argument).
 *
 * @param body the body-only message bytes (no header).
 * @returns full SBE message: 8 zero bytes + body.
 */
function prependSbeHeader(body: Uint8Array): Uint8Array {
  const out = new Uint8Array(SBE_HEADER_BYTES + body.byteLength);
  out.set(body, SBE_HEADER_BYTES);
  return out;
}

/**
 * Write a NUL-padded ASCII string into a DataView at a fixed-length char field.
 *
 * @param dv     - Target DataView.
 * @param offset - Byte offset within `dv` to start writing.
 * @param value  - ASCII string (truncated to `maxLen` if longer).
 * @param maxLen - Fixed field width in bytes; remaining bytes are zeroed.
 */
function writeFixedString(dv: DataView, offset: number, value: string, maxLen: number): void {
  const bytes = new TextEncoder().encode(value);
  for (let i = 0; i < maxLen; i++) {
    dv.setUint8(offset + i, i < bytes.length ? (bytes[i] ?? 0) : 0);
  }
}

/**
 * Build a body-only Uint8Array for {@link OrderCreatedEventDecoder} (templateId=100).
 *
 * <p>All unused fields are zero-initialised; only fields asserted by the calling test
 * are written. The buffer size equals {@link OrderCreatedEventDecoder.BLOCK_LENGTH}.
 */
function makeOrderCreatedPayload(fields: {
  seq: bigint;
  timestamp: bigint;
  clOrdId: string;
  symbol: string;
  side: number;
  price: bigint | null;
  orderQty: bigint;
}): Uint8Array {
  const buf = new ArrayBuffer(OrderCreatedEventDecoder.BLOCK_LENGTH);
  const dv = new DataView(buf);
  dv.setBigInt64(0, fields.seq, true);
  dv.setBigUint64(8, fields.timestamp, true);
  writeFixedString(dv, 56, fields.clOrdId, 20); // clOrdId at body[56]
  writeFixedString(dv, 76, fields.symbol, 8); // symbol at body[76]
  dv.setUint8(84, fields.side); // side at body[84]
  // price at body[87]: int64, null sentinel = INT64_NULL_SENTINEL
  const priceVal = fields.price ?? INT64_NULL_SENTINEL;
  dv.setBigInt64(87, priceVal, true);
  dv.setBigInt64(95, fields.orderQty, true); // orderQty at body[95]
  return prependSbeHeader(new Uint8Array(buf));
}

/**
 * Build a body-only Uint8Array for {@link OrderRejectedEventDecoder} (templateId=101).
 */
function makeOrderRejectedPayload(fields: {
  seq: bigint;
  timestamp: bigint;
  clOrdId: string;
  symbol: string;
  rejectReason: number;
  text: string;
}): Uint8Array {
  const buf = new ArrayBuffer(OrderRejectedEventDecoder.BLOCK_LENGTH);
  const dv = new DataView(buf);
  dv.setBigInt64(0, fields.seq, true);
  dv.setBigUint64(8, fields.timestamp, true);
  writeFixedString(dv, 16, fields.clOrdId, 20); // clOrdId at body[16]
  writeFixedString(dv, 36, fields.symbol, 8); // symbol at body[36]
  dv.setUint8(45, fields.rejectReason); // rejectReason at body[45]
  writeFixedString(dv, 66, fields.text, 64); // text at body[66]
  return prependSbeHeader(new Uint8Array(buf));
}

/**
 * Build a body-only Uint8Array for {@link OrderFilledEventDecoder} (templateId=102).
 *
 * <p>The {@link OrderFilledEventDecoder} has a {@code noLegs} repeating group; this builder
 * zeroes the group-dimension header (numInGroup=0, blockLength=45) so the decoder's group
 * iterator sees an empty group and the cursor advances past it correctly.
 */
function makeOrderFilledPayload(fields: {
  seq: bigint;
  timestamp: bigint;
  execId: string;
  clOrdId: string;
  symbol: string;
  side: number;
  lastPx: bigint;
  lastQty: bigint;
}): Uint8Array {
  // BLOCK_LENGTH + 4 bytes for the noLegs group-dimension header
  const buf = new ArrayBuffer(OrderFilledEventDecoder.BLOCK_LENGTH + 4);
  const dv = new DataView(buf);
  dv.setBigInt64(0, fields.seq, true);
  dv.setBigUint64(8, fields.timestamp, true);
  writeFixedString(dv, 16, fields.execId, 20); // execId at body[16]
  writeFixedString(dv, 56, fields.clOrdId, 20); // clOrdId at body[56]
  writeFixedString(dv, 76, fields.symbol, 8); // symbol at body[76]
  dv.setUint8(84, fields.side); // side at body[84]
  dv.setBigInt64(85, fields.lastPx, true); // lastPx at body[85]
  dv.setBigInt64(93, fields.lastQty, true); // lastQty at body[93]
  // noLegs group-dimension header at BLOCK_LENGTH: blockLength=45 (u16), numInGroup=0 (u16)
  dv.setUint16(OrderFilledEventDecoder.BLOCK_LENGTH, 45, true);
  dv.setUint16(OrderFilledEventDecoder.BLOCK_LENGTH + 2, 0, true);
  return prependSbeHeader(new Uint8Array(buf));
}

/**
 * Build a body-only Uint8Array for {@link OrderCanceledEventDecoder} (templateId=103).
 */
function makeOrderCanceledPayload(fields: {
  seq: bigint;
  timestamp: bigint;
  clOrdId: string;
  origClOrdId: string;
  symbol: string;
}): Uint8Array {
  // APP-151 phase 3 schema: body layout is
  //   seq(0..7) timestamp(8..15) orderId(16..35) execId(36..55) clOrdId(56..75)
  //   origClOrdId(76..95) symbol(96..103) side(104) cumQty(105..112)
  //   productType(113) cancelReason(114) → BLOCK_LENGTH=115.
  const buf = new ArrayBuffer(OrderCanceledEventDecoder.BLOCK_LENGTH);
  const dv = new DataView(buf);
  dv.setBigInt64(0, fields.seq, true);
  dv.setBigUint64(8, fields.timestamp, true);
  // orderId (16..35) + execId (36..55) left as zero-padded; not asserted by this test.
  writeFixedString(dv, 56, fields.clOrdId, 20); // clOrdId at body[56]
  writeFixedString(dv, 76, fields.origClOrdId, 20); // origClOrdId at body[76]
  writeFixedString(dv, 96, fields.symbol, 8); // symbol at body[96]
  return prependSbeHeader(new Uint8Array(buf));
}

/**
 * Build a body-only Uint8Array for {@link OrderCancelRejectedEventDecoder} (templateId=112).
 */
function makeOrderCancelRejectedPayload(fields: {
  seq: bigint;
  timestamp: bigint;
  clOrdId: string;
  origClOrdId: string;
  cxlRejReason: number;
  text: string;
}): Uint8Array {
  const buf = new ArrayBuffer(OrderCancelRejectedEventDecoder.BLOCK_LENGTH);
  const dv = new DataView(buf);
  dv.setBigInt64(0, fields.seq, true);
  dv.setBigUint64(8, fields.timestamp, true);
  writeFixedString(dv, 36, fields.clOrdId, 20); // clOrdId at body[36]
  writeFixedString(dv, 56, fields.origClOrdId, 20); // origClOrdId at body[56]
  dv.setUint8(78, fields.cxlRejReason); // cxlRejReason at body[78]
  writeFixedString(dv, 95, fields.text, 64); // text at body[95]
  return prependSbeHeader(new Uint8Array(buf));
}

/**
 * Build a minimal body-only payload for template 51 (PriceResponse).
 *
 * <p>PriceResponse is orchestrator-bound — the worker never decodes its fields.
 * A 4-byte stub is sufficient to trigger the misroute guard without a real encoder.
 */
function makePriceResponseStub(): Uint8Array {
  return new Uint8Array(4); // Minimal non-zero-length body; fields are never decoded.
}

/**
 * Build a body-only Uint8Array for {@link MarketDataTickDecoder} (templateId=54).
 *
 * <p>Field offsets extracted from {@code MarketDataTickDecoder.ts} (generated):
 * symbol[0..7], bidPrice@8, askPrice@16, bidSize@24, askSize@32,
 * symbolSeq@40, ingressNanos@48, serverNanos@56. BLOCK_LENGTH=64.
 *
 * @param fields - Tick fields; bidSize/askSize/symbolSeq/ingressNanos default to 0n.
 */
function makeMarketDataTickPayload(fields: {
  symbol: string;
  bidPrice: bigint;
  askPrice: bigint;
  serverNanos: bigint;
  bidSize?: bigint;
  askSize?: bigint;
  symbolSeq?: bigint;
  ingressNanos?: bigint;
}): Uint8Array {
  const buf = new ArrayBuffer(MarketDataTickDecoder.BLOCK_LENGTH);
  const dv = new DataView(buf);
  writeFixedString(dv, 0, fields.symbol, 8); // symbol at body[0]
  dv.setBigInt64(8, fields.bidPrice, true); // bidPrice at body[8]
  dv.setBigInt64(16, fields.askPrice, true); // askPrice at body[16]
  dv.setBigInt64(24, fields.bidSize ?? 0n, true); // bidSize at body[24]
  dv.setBigInt64(32, fields.askSize ?? 0n, true); // askSize at body[32]
  dv.setBigInt64(40, fields.symbolSeq ?? 0n, true); // symbolSeq at body[40]
  dv.setBigInt64(48, fields.ingressNanos ?? 0n, true); // ingressNanos at body[48]
  dv.setBigInt64(56, fields.serverNanos, true); // serverNanos at body[56]
  return prependSbeHeader(new Uint8Array(buf));
}

/**
 * Build a body-only Uint8Array for {@link MarketDataHeartbeatDecoder} (templateId=55).
 *
 * <p>Field layout: serverNanos@0 (int64), then the {@code lastPublishedSeq} repeating-group
 * dimension header (4 bytes: blockLength u16 + numInGroup u16). BLOCK_LENGTH=8. Since
 * template 55 is decoded but no WorkerMessage is emitted, this builder only needs a
 * syntactically valid frame — 0 group entries (numInGroup=0) keeps the decoder iteration
 * cursor well-defined.
 *
 * @param serverNanos - Server-side heartbeat timestamp in epoch nanoseconds.
 */
function makeMarketDataHeartbeatPayload(serverNanos: bigint): Uint8Array {
  // BLOCK_LENGTH (8) + 4 bytes for group-dimension header (blockLength u16 + numInGroup u16).
  const buf = new ArrayBuffer(MarketDataHeartbeatDecoder.BLOCK_LENGTH + 4);
  const dv = new DataView(buf);
  dv.setBigInt64(0, serverNanos, true); // serverNanos at body[0]
  // Group-dimension header at BLOCK_LENGTH: blockLength=16 per schema, numInGroup=0.
  dv.setUint16(MarketDataHeartbeatDecoder.BLOCK_LENGTH, 16, true);
  dv.setUint16(MarketDataHeartbeatDecoder.BLOCK_LENGTH + 2, 0, true);
  return prependSbeHeader(new Uint8Array(buf));
}

/**
 * Build a body-only Uint8Array for {@link MarketDataFeedStateChangeDecoder} (templateId=57).
 *
 * <p>Field offsets extracted from {@code MarketDataFeedStateChangeDecoder.ts} (generated):
 * state@0 (uint8), serverNanos@1 (int64). BLOCK_LENGTH=9.
 *
 * @param state       - {@link FeedStateEnum} wire byte (Live=0, Quiet=1, Stale=2).
 * @param serverNanos - Server-side state-change timestamp in epoch nanoseconds.
 */
function makeMarketDataFeedStateChangePayload(state: number, serverNanos: bigint): Uint8Array {
  const buf = new ArrayBuffer(MarketDataFeedStateChangeDecoder.BLOCK_LENGTH);
  const dv = new DataView(buf);
  dv.setUint8(0, state); // state at body[0]
  dv.setBigInt64(1, serverNanos, true); // serverNanos at body[1]
  return prependSbeHeader(new Uint8Array(buf));
}

// ─── Test suite ───────────────────────────────────────────────────────────────

describe("worker-onEvent — cluster domain-event decoder switch", () => {
  let stats: Stats;
  /**
   * Real {@link MarketDataConflation} instance bound to the test-local {@link emit} callback.
   * Used only by template 54 (MarketDataTick) tests — reset in {@code beforeEach} so each test
   * starts with an empty conflation map. Non-template-54 tests use {@link noopConflation}.
   */
  let conflation: MarketDataConflation;
  /**
   * Real {@link GapDetector} instance. Used by template 54 tests (onTick attribution) and by
   * template 57 LIVE-transition tests (onPublisherRestart). Non-market-data tests use
   * {@link noopGapDetector}.
   */
  let gapDetector: GapDetector;

  beforeEach(() => {
    capturedMessages = [];
    capturedErrors = [];
    stats = new Stats();
    conflation = new MarketDataConflation(emit);
    gapDetector = new GapDetector();
  });

  /**
   * Verifies that template 100 (OrderCreatedEvent) decodes all required fields and emits an
   * {@link OrderUpdate} with {@code status="OPEN"} and the correct bigint field values.
   */
  it("onEvent_template100_emits_OrderUpdate_with_status_OPEN_and_fields_populated", () => {
    const SEQ = 1001n;
    const TS = 1_700_000_000_000_000_001n;
    const PRICE = 105_000_000n; // 1.05 in fixed-point (scale 1e8)
    const QTY = 100_000_000n; // 1.00 in fixed-point (scale 1e8)

    const payload = makeOrderCreatedPayload({
      seq: SEQ,
      timestamp: TS,
      clOrdId: "TEST-ORD-001",
      symbol: "EURUSD",
      side: SideEnum.Buy,
      price: PRICE,
      orderQty: QTY,
    });

    const handled = decodeClusterEvent(OrderCreatedEventDecoder.TEMPLATE_ID, payload, {
      emit,
      postError,
      stats,
      conflation: noopConflation,
      gapDetector: noopGapDetector,
    });

    expect(handled).toBe(true);
    expect(capturedErrors).toHaveLength(0);
    expect(capturedMessages).toHaveLength(1);

    const msg = capturedMessages[0] as OrderUpdate;
    expect(msg.type).toBe("order");
    expect(msg.status).toBe("OPEN");
    expect(msg.clOrdId).toBe("TEST-ORD-001");
    expect(msg.symbol).toBe("EURUSD");
    expect(msg.side).toBe("BUY");
    expect(msg.qty).toBe(QTY);
    expect(msg.price).toBe(PRICE);
    expect(msg.serverNanos).toBe(TS);
  });

  /**
   * Verifies that template 100 with a null-sentinel price (market order) substitutes
   * {@code 0n} rather than propagating the sentinel value, per the worker's null-coalescing
   * guard: {@code price: price ?? 0n}.
   */
  it("onEvent_template100_priceNull_emits_OrderUpdate_price_eq_0n", () => {
    const TS = 1_700_000_000_000_000_002n;
    const QTY = 50_000_000n;

    const payload = makeOrderCreatedPayload({
      seq: 1002n,
      timestamp: TS,
      clOrdId: "MKT-ORD-001",
      symbol: "GBPUSD",
      side: SideEnum.Sell,
      price: null, // null sentinel → INT64_NULL_SENTINEL on the wire
      orderQty: QTY,
    });

    const handled = decodeClusterEvent(OrderCreatedEventDecoder.TEMPLATE_ID, payload, {
      emit,
      postError,
      stats,
      conflation: noopConflation,
      gapDetector: noopGapDetector,
    });

    expect(handled).toBe(true);
    expect(capturedErrors).toHaveLength(0);
    expect(capturedMessages).toHaveLength(1);

    const msg = capturedMessages[0] as OrderUpdate;
    expect(msg.type).toBe("order");
    expect(msg.price).toBe(0n);
    expect(msg.side).toBe("SELL");
    expect(msg.qty).toBe(QTY);
    expect(msg.serverNanos).toBe(TS);
  });

  /**
   * Verifies that template 101 (OrderRejectedEvent) emits an {@link EventUpdate} with
   * {@code eventType="OrderRejected"} and a details string that includes the reject reason
   * and free-text reason.
   */
  it("onEvent_template101_emits_EventUpdate_eventType_OrderRejected", () => {
    const SEQ = 2001n;
    const TS = 1_700_000_000_000_000_003n;

    const payload = makeOrderRejectedPayload({
      seq: SEQ,
      timestamp: TS,
      clOrdId: "REJ-ORD-001",
      symbol: "USDJPY",
      rejectReason: RejectReasonEnum.InsufficientQuantity,
      text: "qty below minimum",
    });

    const handled = decodeClusterEvent(OrderRejectedEventDecoder.TEMPLATE_ID, payload, {
      emit,
      postError,
      stats,
      conflation: noopConflation,
      gapDetector: noopGapDetector,
    });

    expect(handled).toBe(true);
    expect(capturedErrors).toHaveLength(0);
    expect(capturedMessages).toHaveLength(1);

    const msg = capturedMessages[0] as EventUpdate;
    expect(msg.type).toBe("event");
    expect(msg.eventType).toBe("OrderRejected");
    expect(msg.seq).toBe(SEQ);
    expect(msg.serverNanos).toBe(TS);
    // details must include the clOrdId, symbol, rejectReason ordinal, and text snippet
    expect(msg.details).toContain("clOrdId=REJ-ORD-001");
    expect(msg.details).toContain("symbol=USDJPY");
    expect(msg.details).toContain("reason=");
    expect(msg.details).toContain("qty below minimum");
  });

  /**
   * Verifies that template 102 (OrderFilledEvent) emits a {@link FillUpdate} with the
   * correct {@code lastPx}, {@code lastQty}, {@code execId}, and side label.
   */
  it("onEvent_template102_emits_FillUpdate_with_lastPx_and_lastQty", () => {
    const SEQ = 3001n;
    const TS = 1_700_000_000_000_000_004n;
    const LAST_PX = 108_520_000n; // 1.0852 in fixed-point
    const LAST_QTY = 75_000_000n; // 0.75 in fixed-point

    const payload = makeOrderFilledPayload({
      seq: SEQ,
      timestamp: TS,
      execId: "EXEC-001",
      clOrdId: "FILL-ORD-001",
      symbol: "AUDUSD",
      side: SideEnum.Sell,
      lastPx: LAST_PX,
      lastQty: LAST_QTY,
    });

    const handled = decodeClusterEvent(OrderFilledEventDecoder.TEMPLATE_ID, payload, {
      emit,
      postError,
      stats,
      conflation: noopConflation,
      gapDetector: noopGapDetector,
    });

    expect(handled).toBe(true);
    expect(capturedErrors).toHaveLength(0);
    expect(capturedMessages).toHaveLength(1);

    const msg = capturedMessages[0] as FillUpdate;
    expect(msg.type).toBe("fill");
    expect(msg.execId).toBe("EXEC-001");
    expect(msg.clOrdId).toBe("FILL-ORD-001");
    expect(msg.symbol).toBe("AUDUSD");
    expect(msg.side).toBe("SELL");
    expect(msg.fillPrice).toBe(LAST_PX);
    expect(msg.fillQty).toBe(LAST_QTY);
    expect(msg.serverNanos).toBe(TS);
  });

  /**
   * Verifies that template 103 (OrderCanceledEvent) emits an {@link EventUpdate} with
   * {@code eventType="OrderCanceled"} and a details string containing both the original
   * and the cancel clOrdIds.
   */
  it("onEvent_template103_emits_EventUpdate_eventType_OrderCanceled", () => {
    const SEQ = 4001n;
    const TS = 1_700_000_000_000_000_005n;

    const payload = makeOrderCanceledPayload({
      seq: SEQ,
      timestamp: TS,
      clOrdId: "CXL-ORD-002",
      origClOrdId: "ORIG-ORD-001",
      symbol: "EURUSD",
    });

    const handled = decodeClusterEvent(OrderCanceledEventDecoder.TEMPLATE_ID, payload, {
      emit,
      postError,
      stats,
      conflation: noopConflation,
      gapDetector: noopGapDetector,
    });

    expect(handled).toBe(true);
    expect(capturedErrors).toHaveLength(0);
    expect(capturedMessages).toHaveLength(1);

    const msg = capturedMessages[0] as EventUpdate;
    expect(msg.type).toBe("event");
    expect(msg.eventType).toBe("OrderCanceled");
    expect(msg.seq).toBe(SEQ);
    expect(msg.serverNanos).toBe(TS);
    expect(msg.details).toContain("clOrdId=CXL-ORD-002");
    expect(msg.details).toContain("origClOrdId=ORIG-ORD-001");
    expect(msg.details).toContain("symbol=EURUSD");
  });

  /**
   * Verifies that template 112 (OrderCancelRejectedEvent) emits an {@link EventUpdate} with
   * {@code eventType="OrderCancelRejected"} and details containing the reject reason and text.
   */
  it("onEvent_template112_emits_EventUpdate_eventType_OrderCancelRejected", () => {
    const SEQ = 5001n;
    const TS = 1_700_000_000_000_000_006n;

    const payload = makeOrderCancelRejectedPayload({
      seq: SEQ,
      timestamp: TS,
      clOrdId: "CXLREJ-ORD-003",
      origClOrdId: "ORIG-ORD-002",
      cxlRejReason: 1, // UnknownOrder
      text: "order not found",
    });

    const handled = decodeClusterEvent(OrderCancelRejectedEventDecoder.TEMPLATE_ID, payload, {
      emit,
      postError,
      stats,
      conflation: noopConflation,
      gapDetector: noopGapDetector,
    });

    expect(handled).toBe(true);
    expect(capturedErrors).toHaveLength(0);
    expect(capturedMessages).toHaveLength(1);

    const msg = capturedMessages[0] as EventUpdate;
    expect(msg.type).toBe("event");
    expect(msg.eventType).toBe("OrderCancelRejected");
    expect(msg.seq).toBe(SEQ);
    expect(msg.serverNanos).toBe(TS);
    expect(msg.details).toContain("clOrdId=CXLREJ-ORD-003");
    expect(msg.details).toContain("origClOrdId=ORIG-ORD-002");
    expect(msg.details).toContain("reason=");
    expect(msg.details).toContain("order not found");
  });

  /**
   * Verifies that template 51 (PriceResponse — orchestrator-bound) is the misroute regression
   * guard: receiving it increments {@code stats.marketdataMisroutedRfq} by exactly 1, posts a
   * PROTOCOL error whose hint mentions "misrouted PriceResponse", and emits NO WorkerMessage.
   *
   * <p>Three assertions are required:
   * (a) {@code stats.snapshot().marketdataMisroutedRfq === 1n} after one invocation.
   * (b) No WorkerMessage enqueued.
   * (c) Exactly one ERROR with code {@code "PROTOCOL"} and hint mentioning
   *     {@code "misrouted PriceResponse"}.
   */
  it("onEvent_template51_PriceResponse_misroute_incrementsStatsCounter_and_postsError_noWorkerMessage", () => {
    const payload = makePriceResponseStub();

    const handled = decodeClusterEvent(PRICE_RESPONSE_TEMPLATE_ID, payload, {
      emit,
      postError,
      stats,
      conflation: noopConflation,
      gapDetector: noopGapDetector,
    });

    // Misroute IS handled — return true so the worker does not double-fire its
    // unexpected-template fallback.
    expect(handled).toBe(true);

    // (a) Stats counter must be exactly 1 after one misroute.
    expect(stats.snapshot().marketdataMisroutedRfq).toBe(1n);

    // (b) No WorkerMessage emitted — misroute does not produce an event update.
    expect(capturedMessages).toHaveLength(0);

    // (c) Exactly one PROTOCOL error with the misroute hint.
    expect(capturedErrors).toHaveLength(1);
    const err = capturedErrors[0];
    if (err === undefined) throw new Error("expected error envelope");
    expect(err.code).toBe("PROTOCOL");
    expect(err.hint).toContain("misrouted PriceResponse");
  });

  /**
   * Verifies the `return false` contract: an unknown templateId MUST return {@code false} so
   * the worker's caller fires its own unknown-template fallback. No WorkerMessage emitted, no
   * error posted, no stats counter touched.
   *
   * <p>Uses templateId {@code 999} — guaranteed-unknown across all Phase 3 schema additions.
   * Earlier this test used templateId 54 (reserved for Commit 6's market-data branch) but
   * Commit 6 landed the MarketDataTick decoder, so 54 is now handled and the unknown-case
   * needs a different id.
   *
   * <p>Without this assertion a regression that causes the default-case arm to inadvertently
   * fall through to a `return true` or to an unguarded emit would go undetected.
   */
  it("onEvent_unknownTemplate_returnsFalse_noMessageNoErrorNoStatsIncrement", () => {
    const UNKNOWN_TEMPLATE_ID = 999;
    const payload = prependSbeHeader(new Uint8Array(16));

    const handled = decodeClusterEvent(UNKNOWN_TEMPLATE_ID, payload, {
      emit,
      postError,
      stats,
      conflation: noopConflation,
      gapDetector: noopGapDetector,
    });

    expect(handled).toBe(false);
    expect(capturedMessages).toHaveLength(0);
    expect(capturedErrors).toHaveLength(0);
    expect(stats.snapshot().marketdataMisroutedRfq).toBe(0n);
  });

  /**
   * Verifies that a truncated/malformed buffer for a valid event templateId causes the
   * try/catch in `onEvent` to post a PROTOCOL error, emit no WorkerMessage, and NOT
   * increment the misrouted-RFQ counter (corrupt payload is a decode error, not a misroute).
   *
   * <p>A 4-byte Uint8Array is well below {@link OrderCreatedEventDecoder.BLOCK_LENGTH} (156
   * bytes), so any field access (e.g. {@code dec.price()} reads 8 bytes at offset 87) will
   * throw a {@code RangeError} from the DataView.
   */
  it("onEvent_corruptPayload_postsProtocolError_noWorkerMessage_noStatsIncrement", () => {
    // Deliberately undersized payload — triggers RangeError inside the decoder.
    const corruptPayload = new Uint8Array(4);

    const handled = decodeClusterEvent(OrderCreatedEventDecoder.TEMPLATE_ID, corruptPayload, {
      emit,
      postError,
      stats,
      conflation: noopConflation,
      gapDetector: noopGapDetector,
    });

    // The production function returns true on error (handled — error posted).
    expect(handled).toBe(true);

    // Error posted — but NOT the misroute error; this is a decode failure.
    expect(capturedErrors).toHaveLength(1);
    const err = capturedErrors[0];
    if (err === undefined) throw new Error("expected error envelope");
    expect(err.code).toBe("PROTOCOL");
    // The hint must reference the failing templateId, not the misroute path.
    expect(err.hint).toContain("100");

    // No WorkerMessage produced.
    expect(capturedMessages).toHaveLength(0);

    // Misrouted-RFQ counter MUST NOT be incremented for a decode failure.
    expect(stats.snapshot().marketdataMisroutedRfq).toBe(0n);
  });

  // ─── Template 54 (MarketDataTick) ─────────────────────────────────────────

  /**
   * Verifies that template 54 (MarketDataTick) happy-path decodes all required
   * fields — symbol, bidPrice, askPrice, serverNanos — and emits a {@link PriceUpdate}
   * with the correct values. Other fields (bidSize/askSize/symbolSeq/ingressNanos) are
   * wired as 0n since the production decoder does not surface them on the PriceUpdate shape.
   *
   * <p>Price chosen to exercise a real fixed-point value (1.085 in scale 1e8).
   */
  it("onEvent_template54_emits_PriceUpdate_with_bid_ask_serverNanos", () => {
    const BID = 108_500_000n; // 1.085 in fixed-point (scale 1e8)
    const ASK = 108_510_000n; // 1.0851 in fixed-point (scale 1e8)
    const SERVER_NANOS = 1_700_000_000_000_000_001n;

    const payload = makeMarketDataTickPayload({
      symbol: "EURUSD",
      bidPrice: BID,
      askPrice: ASK,
      serverNanos: SERVER_NANOS,
    });

    const handled = decodeClusterEvent(MarketDataTickDecoder.TEMPLATE_ID, payload, {
      emit,
      postError,
      stats,
      conflation,
      gapDetector,
    });
    // Template 54 routes through conflation — drain synchronously to flush the
    // PriceUpdate into capturedMessages before asserting.
    conflation.drain();

    expect(handled).toBe(true);
    expect(capturedErrors).toHaveLength(0);
    expect(capturedMessages).toHaveLength(1);

    const msg = capturedMessages[0] as PriceUpdate;
    expect(msg.type).toBe("price");
    expect(msg.symbol).toBe("EURUSD");
    expect(msg.bid).toBe(BID);
    expect(msg.ask).toBe(ASK);
    expect(msg.serverNanos).toBe(SERVER_NANOS);
  });

  /**
   * Verifies that template 54 with negative prices still emits a {@link PriceUpdate} — the
   * dispatch layer trusts the wire bytes without client-side validation. The publisher's upstream
   * sanity check (positive price enforcement) is the correct boundary; the decoder propagates the
   * raw wire values as bigints without clamping or rejection.
   *
   * <p>Uses bidPrice=-1n / askPrice=-1n (the sentinel that a buggy publisher emitting "no price
   * available" might produce) to confirm the bigint round-trip is lossless.
   */
  it("onEvent_template54_negativePrice_stillEmits_PriceUpdate", () => {
    const NEG_ONE = -1n;

    const payload = makeMarketDataTickPayload({
      symbol: "GBPUSD",
      bidPrice: NEG_ONE,
      askPrice: NEG_ONE,
      serverNanos: 1_700_000_000_000_000_002n,
    });

    const handled = decodeClusterEvent(MarketDataTickDecoder.TEMPLATE_ID, payload, {
      emit,
      postError,
      stats,
      conflation,
      gapDetector,
    });
    // Template 54 routes through conflation — drain synchronously to flush the
    // PriceUpdate into capturedMessages before asserting.
    conflation.drain();

    expect(handled).toBe(true);
    expect(capturedErrors).toHaveLength(0);
    expect(capturedMessages).toHaveLength(1);

    const msg = capturedMessages[0] as PriceUpdate;
    expect(msg.type).toBe("price");
    expect(msg.bid).toBe(NEG_ONE);
    expect(msg.ask).toBe(NEG_ONE);
  });

  // ─── Template 55 (MarketDataHeartbeat) ────────────────────────────────────

  /**
   * Verifies that template 55 (MarketDataHeartbeat) decodes without error, returns
   * {@code true} (handled), and emits NO {@link WorkerMessage} — heartbeats are
   * liveness signals only (reserved for the browser-side liveness tracker). No
   * stats counter is affected and no error is posted.
   *
   * <p>Uses the minimal valid heartbeat: serverNanos set, lastPublishedSeq group
   * count = 0 so the group dimension header is syntactically present but empty.
   */
  it("onEvent_template55_emits_no_message_returns_true", () => {
    const payload = makeMarketDataHeartbeatPayload(1_700_000_000_000_000_003n);

    const handled = decodeClusterEvent(MarketDataHeartbeatDecoder.TEMPLATE_ID, payload, {
      emit,
      postError,
      stats,
      conflation: noopConflation,
      gapDetector: noopGapDetector,
    });

    expect(handled).toBe(true);
    expect(capturedMessages).toHaveLength(0);
    expect(capturedErrors).toHaveLength(0);
    expect(stats.snapshot().marketdataMisroutedRfq).toBe(0n);
  });

  // ─── Template 57 (MarketDataFeedStateChange) ──────────────────────────────

  /**
   * Verifies that template 57 with {@code FeedStateEnum.Live (0)} emits a
   * {@link FeedStateMsg} with {@code state="LIVE"} and the correct serverNanos.
   *
   * <p>Live is the normal operating state — the first real tick after subscription
   * or reconnect sends this before the tick stream begins.
   */
  it("onEvent_template57_state_LIVE_emits_FeedStateMsg_LIVE", () => {
    const SERVER_NANOS = 1_700_000_000_000_000_004n;

    const payload = makeMarketDataFeedStateChangePayload(FeedStateEnum.Live, SERVER_NANOS);

    const handled = decodeClusterEvent(MarketDataFeedStateChangeDecoder.TEMPLATE_ID, payload, {
      emit,
      postError,
      stats,
      conflation: noopConflation,
      gapDetector,
    });

    expect(handled).toBe(true);
    expect(capturedErrors).toHaveLength(0);
    expect(capturedMessages).toHaveLength(1);

    const msg = capturedMessages[0] as FeedStateMsg;
    expect(msg.type).toBe("feed-state");
    expect(msg.state).toBe("LIVE");
    expect(msg.serverNanos).toBe(SERVER_NANOS);
  });

  /**
   * Verifies that template 57 with {@code FeedStateEnum.Stale (2)} emits a
   * {@link FeedStateMsg} with {@code state="STALE"}. Stale is sent when no
   * fragments are observed for the configured stale-detection threshold.
   */
  it("onEvent_template57_state_STALE_emits_FeedStateMsg_STALE", () => {
    const SERVER_NANOS = 1_700_000_000_000_000_005n;

    const payload = makeMarketDataFeedStateChangePayload(FeedStateEnum.Stale, SERVER_NANOS);

    const handled = decodeClusterEvent(MarketDataFeedStateChangeDecoder.TEMPLATE_ID, payload, {
      emit,
      postError,
      stats,
      conflation: noopConflation,
      gapDetector: noopGapDetector,
    });

    expect(handled).toBe(true);
    expect(capturedErrors).toHaveLength(0);
    expect(capturedMessages).toHaveLength(1);

    const msg = capturedMessages[0] as FeedStateMsg;
    expect(msg.type).toBe("feed-state");
    expect(msg.state).toBe("STALE");
    expect(msg.serverNanos).toBe(SERVER_NANOS);
  });

  /**
   * Verifies that template 57 with {@code FeedStateEnum.Quiet (1)} emits a
   * {@link FeedStateMsg} with {@code state="QUIET"}. Quiet means heartbeats are
   * arriving but the tick stream is idle — a normal off-hours state for FX pairs.
   */
  it("onEvent_template57_state_QUIET_emits_FeedStateMsg_QUIET", () => {
    const SERVER_NANOS = 1_700_000_000_000_000_006n;

    const payload = makeMarketDataFeedStateChangePayload(FeedStateEnum.Quiet, SERVER_NANOS);

    const handled = decodeClusterEvent(MarketDataFeedStateChangeDecoder.TEMPLATE_ID, payload, {
      emit,
      postError,
      stats,
      conflation: noopConflation,
      gapDetector: noopGapDetector,
    });

    expect(handled).toBe(true);
    expect(capturedErrors).toHaveLength(0);
    expect(capturedMessages).toHaveLength(1);

    const msg = capturedMessages[0] as FeedStateMsg;
    expect(msg.type).toBe("feed-state");
    expect(msg.state).toBe("QUIET");
    expect(msg.serverNanos).toBe(SERVER_NANOS);
  });

  /**
   * Verifies the defensive {@code "LIVE"} default in {@code feedStateToLabel} when the wire
   * byte does not match any known {@link FeedStateEnum} ordinal. The documented safety
   * invariant is that a malformed inbound state cannot tip the UI into a permanent STALE
   * banner — an unknown byte must fall through to LIVE so the next legitimate template-57
   * frame corrects it.
   *
   * <p>Uses ordinal {@code 0x99} (153) — outside the valid set {0 Live, 1 Quiet, 2 Stale} and
   * below the SBE-codec NULL_VAL sentinel (255).
   */
  it("onEvent_template57_unknownByte_defaultsToFeedStateMsg_LIVE", () => {
    const SERVER_NANOS = 1_700_000_000_000_000_007n;
    const UNKNOWN_STATE_BYTE = 0x99;

    const payload = makeMarketDataFeedStateChangePayload(UNKNOWN_STATE_BYTE, SERVER_NANOS);

    const handled = decodeClusterEvent(MarketDataFeedStateChangeDecoder.TEMPLATE_ID, payload, {
      emit,
      postError,
      stats,
      conflation: noopConflation,
      gapDetector,
    });

    expect(handled).toBe(true);
    expect(capturedErrors).toHaveLength(0);
    expect(capturedMessages).toHaveLength(1);

    const msg = capturedMessages[0] as FeedStateMsg;
    expect(msg.type).toBe("feed-state");
    expect(msg.state).toBe("LIVE");
    expect(msg.serverNanos).toBe(SERVER_NANOS);
  });
});
