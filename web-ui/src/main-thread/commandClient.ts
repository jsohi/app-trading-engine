/**
 * commandClient — typed browser-→-cluster command submission API.
 *
 * Plan §12 (APP-160). Wraps the existing {@link WorkerClient} with a single
 * public method {@link CommandClient.submitOrder} that:
 *
 *  1. Encodes the {@link NewOrderSinglePayload} via the hand-written-but-
 *     wire-format-correct {@link NewOrderSingleEncoder} (templateId=4,
 *     blockLength=108, schemaId=1, schemaVersion=1 — verified byte-identical
 *     against the auto-generated {@code NewOrderSingleDecoder} by the encoder
 *     round-trip test).
 *  2. Allocates a slot in the pre-allocated 1024-slot {@link RequestIdTable}
 *     (slot index = {@code seq & 1023}; seq is a u32-masked monotonic counter).
 *  3. Posts the encoded frame via {@link WorkerClient.submitCommand}, which
 *     transfers the underlying {@link ArrayBuffer} into the worker for
 *     zero-copy hand-off onto the wss send queue.
 *  4. Returns a {@code Promise<CommandAckResult>} that resolves on the matching
 *     {@code CommandAck} (templateId=70, demuxed by {@code clientCmdSeqNo})
 *     with status {@code Accepted}, or rejects with a typed error for
 *     {@code Rejected}/{@code Throttled}/{@code Duplicate}/etc. Acks arrive
 *     via {@link WorkerClient.commandAcks$}.
 *
 * <p><b>Bounded outbound buffer</b>: cap=256, derivation: assumed peak submit
 * rate 100/s × p99 ack RTT 50ms × safety factor 50 = 250, rounded to power-of-2.
 * Overflow → synchronous {@link BackpressureError}; no worker traffic generated.
 *
 * <p><b>Allocation discipline</b>: zero-allocation after warmup. The slot
 * table is pre-allocated; the SBE encoder is reused; the outbound
 * {@link Uint8Array} pool is sized to the SBE NewOrderSingle frame length;
 * the timeout scanner is a single pre-allocated {@code setInterval} with a
 * fixed cursor and early-exit when the slot is free.
 *
 * <p><b>Threading</b>: main thread.
 */

import type { CommandAckEnvelope, WorkerClient } from "@/main-thread/workerClient";
import { NewOrderSingleEncoder } from "@/sbe/encoders/NewOrderSingleEncoder";
import {
  OrdTypeEnum,
  ProductTypeEnum,
  SettlTypeEnum,
  SideEnum,
  TenorEnum,
  TimeInForceEnum,
} from "@trading/sbe-codecs";
import type { Subscription } from "rxjs";

/** Pre-allocated slot count. Power of 2 so `seq & MASK` is the slot index. */
const SLOT_COUNT = 1024;
const SLOT_MASK = SLOT_COUNT - 1;

/** Bounded in-flight outbound submits. Documented derivation: 100/s × 50ms × 50 ≈ 250 → 256. */
const MAX_IN_FLIGHT = 256;

/** Per-slot timeout. 5s covers the worst-case ack RTT plus generous safety. */
const SLOT_TIMEOUT_MS = 5_000;

/** Scanner cadence for expired slots. 250ms keeps the worst-case timeout drift to ≤250ms. */
const SCAN_INTERVAL_MS = 250;

/** Outbound buffer pool size — exactly the encoded NewOrderSingle frame length (header + block). */
const POOL_BUFFER_SIZE = NewOrderSingleEncoder.ENCODED_FRAME_LENGTH;

/** Pricing scale factor: 10^8 (matches `FixedPointScale.PRICE_SCALE` on the server). */
const PRICE_SCALE = 100_000_000n;

/** Per the trading-schema.xml CommandAckStatus enum (Accepted/Rejected/Duplicate/Throttled). */
export type CommandAckStatusName = "Accepted" | "Rejected" | "Duplicate" | "Throttled";

export interface CommandAckResult {
  readonly status: CommandAckStatusName;
  readonly correlationId: number;
}

export interface NewOrderSinglePayload {
  readonly clOrdId: string;
  /** FX symbol matching `^[A-Z]{3}/[A-Z]{3}$` — validated client-side before submit. */
  readonly symbol: string;
  readonly side: "buy" | "sell";
  /** Fixed-point quantity (× 10^8). */
  readonly qty: bigint;
  /** Fixed-point price (× 10^8). */
  readonly price: bigint;
}

// ===========================================================================
// Typed errors
// ===========================================================================

export class BackpressureError extends Error {
  constructor() {
    super(`commandClient: outbound buffer full (cap=${String(MAX_IN_FLIGHT)})`);
    this.name = "BackpressureError";
  }
}

export class RequestIdCollisionError extends Error {
  constructor(seq: number) {
    super(`commandClient: request-id collision at slot ${String(seq & SLOT_MASK)}`);
    this.name = "RequestIdCollisionError";
  }
}

export class CommandTimeoutError extends Error {
  constructor(seq: number) {
    super(`commandClient: timeout waiting for CommandAck for seq=${String(seq)}`);
    this.name = "CommandTimeoutError";
  }
}

export class ConnectionLostError extends Error {
  constructor() {
    super("commandClient: WorkerClient connection lost; in-flight requests rejected");
    this.name = "ConnectionLostError";
  }
}

export class EncoderOverflowError extends Error {
  constructor(encodedLength: number) {
    super(
      `commandClient: encoded NewOrderSingle (${String(encodedLength)} bytes) exceeds POOL_BUFFER_SIZE (${String(POOL_BUFFER_SIZE)})`,
    );
    this.name = "EncoderOverflowError";
  }
}

export class CommandRejectedError extends Error {
  readonly status: CommandAckStatusName;
  constructor(status: CommandAckStatusName, reasonCode?: string) {
    super("commandClient: command " + status + (reasonCode ? " (reason=" + reasonCode + ")" : ""));
    this.name = "CommandRejectedError";
    this.status = status;
  }
}

// ===========================================================================
// Slot table — pre-allocated, never grown after construction.
// ===========================================================================

interface Slot {
  /** 0 means free; otherwise an in-flight u32 sequence number. */
  seq: number;
  resolve: ((r: CommandAckResult) => void) | null;
  reject: ((e: Error) => void) | null;
  /** Wall-clock deadline in ms. 0 when free. */
  deadlineMs: number;
}

function newSlot(): Slot {
  return { seq: 0, resolve: null, reject: null, deadlineMs: 0 };
}

// ===========================================================================
// CommandClient
// ===========================================================================

export class CommandClient {
  private readonly slots: Slot[];
  /** Pool of pre-allocated outbound Uint8Arrays (size = SBE frame length). */
  private readonly outboundPool: Uint8Array[];
  /** Reused SBE encoder flyweight. */
  private readonly encoder = new NewOrderSingleEncoder();
  private nextSeq = 0;
  private inFlight = 0;
  private readonly worker: WorkerClient;
  private readonly ackSub: Subscription;
  private readonly stateSub: Subscription;
  private scanTimer: ReturnType<typeof setInterval> | null = null;
  private disposed = false;

  constructor(worker: WorkerClient) {
    this.worker = worker;
    this.slots = new Array<Slot>(SLOT_COUNT);
    for (let i = 0; i < SLOT_COUNT; i++) this.slots[i] = newSlot();
    this.outboundPool = new Array<Uint8Array>(MAX_IN_FLIGHT);
    for (let i = 0; i < MAX_IN_FLIGHT; i++) {
      this.outboundPool[i] = new Uint8Array(POOL_BUFFER_SIZE);
    }

    this.ackSub = this.worker.commandAcks$.subscribe((ack) => {
      this.handleAck(ack);
    });
    this.stateSub = this.worker.connectionState$.subscribe((s) => {
      if (s === "DOWN" || s === "WORKER_DEAD") this.failAllInFlight(new ConnectionLostError());
    });

    // Single pre-allocated scanner — never per-request setTimeout. Walks all slots in
    // one pass; early-exits once it has seen as many populated slots as `inFlight`.
    this.scanTimer = setInterval(() => {
      this.scanExpired();
    }, SCAN_INTERVAL_MS);
  }

  /**
   * Submit a NewOrderSingle. Returns a promise that resolves on Accepted ack or rejects with one
   * of the typed errors above. Synchronous overflow checks (BackpressureError /
   * RequestIdCollisionError / EncoderOverflowError) reject before any worker traffic.
   */
  submitOrder(payload: NewOrderSinglePayload): Promise<CommandAckResult> {
    if (this.disposed) {
      return Promise.reject(new ConnectionLostError());
    }
    if (this.inFlight >= MAX_IN_FLIGHT) {
      return Promise.reject(new BackpressureError());
    }
    // u32 sequence: increment + mask. JS `number` is safe through 2^53; the mask ensures the
    // wire field stays in u32 range. 0 is reserved as "slot free" sentinel.
    // Compute the candidate seq + slot WITHOUT mutating this.nextSeq yet —
    // if the slot is occupied we reject without burning the seq number
    // (reviewer A LOW finding F-A7).
    let candidateSeq = (this.nextSeq + 1) & 0xffffffff;
    if (candidateSeq === 0) candidateSeq = 1;
    const slotIdx = candidateSeq & SLOT_MASK;
    const slot = this.slots[slotIdx];
    if (slot?.seq !== 0) {
      return Promise.reject(new RequestIdCollisionError(candidateSeq));
    }
    // Commit the seq advance — slot is free, we are taking it.
    this.nextSeq = candidateSeq;
    const seq = candidateSeq;
    return new Promise<CommandAckResult>((resolve, reject) => {
      slot.seq = seq;
      slot.resolve = resolve;
      slot.reject = reject;
      slot.deadlineMs = Date.now() + SLOT_TIMEOUT_MS;
      this.inFlight++;

      // Real SBE encode (no JSON envelope, no synthetic ack). The pool is indexed by slot —
      // each in-flight slot owns its own buffer, so concurrent submits cannot collide.
      const buf = this.outboundPool[slotIdx] ?? new Uint8Array(POOL_BUFFER_SIZE);
      const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
      try {
        this.encoder.wrapAndApplyHeader(view, 0).setFields({
          clOrdId: payload.clOrdId,
          quoteId: "",
          symbol: payload.symbol,
          side: payload.side === "buy" ? SideEnum.Buy : SideEnum.Sell,
          ordType: OrdTypeEnum.Limit,
          price: payload.price,
          orderQty: payload.qty,
          timeInForce: TimeInForceEnum.Day,
          transactTime: BigInt(Date.now()) * 1_000_000n,
          accountCode: "ACME-001",
          productType: ProductTypeEnum.Spot,
          settlDate: "20260518",
          settlType: SettlTypeEnum.Regular,
          currency: "USD",
          settlCurrency: "",
          tenor: TenorEnum.SN,
        });
      } catch (e: unknown) {
        this.freeSlot(slot);
        reject(e instanceof Error ? e : new Error(String(e)));
        return;
      }
      const length = NewOrderSingleEncoder.ENCODED_FRAME_LENGTH;
      if (length > POOL_BUFFER_SIZE) {
        this.freeSlot(slot);
        reject(new EncoderOverflowError(length));
        return;
      }
      try {
        // The buffer is transferred into the worker (zero-copy). After this call the main-thread
        // `buf` is detached; we replace it in the pool BEFORE the next submit reuses this slot.
        this.worker.submitCommand(buf, length, seq);
        // Re-allocate the pool slot — the previous buffer was transferred to the worker.
        this.outboundPool[slotIdx] = new Uint8Array(POOL_BUFFER_SIZE);
      } catch (e: unknown) {
        this.freeSlot(slot);
        reject(e instanceof Error ? e : new Error(String(e)));
      }
    });
  }

  dispose(): void {
    if (this.disposed) return;
    this.disposed = true;
    this.ackSub.unsubscribe();
    this.stateSub.unsubscribe();
    if (this.scanTimer !== null) {
      clearInterval(this.scanTimer);
      this.scanTimer = null;
    }
    this.failAllInFlight(new ConnectionLostError());
  }

  private handleAck(ack: CommandAckEnvelope): void {
    const slotIdx = ack.correlationId & SLOT_MASK;
    const slot = this.slots[slotIdx];
    if (slot?.seq !== ack.correlationId) {
      // Late ack (slot already evicted by timeout). Drop silently.
      return;
    }
    const resolve = slot.resolve;
    const reject = slot.reject;
    this.freeSlot(slot);
    if (ack.status === "Accepted") {
      resolve?.({ status: "Accepted", correlationId: ack.correlationId });
    } else {
      reject?.(new CommandRejectedError(ack.status, ack.reasonCode));
    }
  }

  private scanExpired(): void {
    if (this.inFlight === 0) return;
    const now = Date.now();
    let seen = 0;
    for (let i = 0; i < SLOT_COUNT && seen < this.inFlight; i++) {
      const slot = this.slots[i];
      if (slot === undefined) continue;
      if (slot.seq === 0) continue;
      seen++;
      if (slot.deadlineMs <= now) {
        const seq = slot.seq;
        const reject = slot.reject;
        this.freeSlot(slot);
        reject?.(new CommandTimeoutError(seq));
      }
    }
  }

  private failAllInFlight(err: Error): void {
    for (let i = 0; i < SLOT_COUNT; i++) {
      const slot = this.slots[i];
      if (slot === undefined) continue;
      if (slot.seq === 0) continue;
      const reject = slot.reject;
      this.freeSlot(slot);
      reject?.(err);
    }
    this.inFlight = 0;
  }

  private freeSlot(slot: Slot): void {
    if (slot.seq !== 0) this.inFlight--;
    slot.seq = 0;
    slot.resolve = null;
    slot.reject = null;
    slot.deadlineMs = 0;
  }
}

// PRICE_SCALE is exported for tests and OrderEntryForm input validation.
export { PRICE_SCALE };
