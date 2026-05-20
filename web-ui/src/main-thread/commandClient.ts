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

/**
 * Computes the spot-FX settlement date as YYYYMMDD (UTC) using the standard T+2
 * convention, skipping Saturday and Sunday. A full holiday calendar (US bank
 * holidays, currency-specific holidays for cross pairs, USD intermediation rule,
 * broken-date validation) is out of scope for this PR and is tracked under
 * APP-125 (Holiday / settlement calendars — value-date math for FX
 * spot/forward/swaps) for the production-tier order-entry surface; for dev
 * and pre-prod the weekend skip is sufficient — every business day still
 * produces a valid settlement date and matches the cluster-side date check
 * (see {@code NewOrderSingleHandler.validateSettlDate} in the {@code cluster}
 * module, which only checks YYYYMMDD shape + that the date is not in the past
 * at ack time). When APP-125 lands the holiday calendar, the cluster validator
 * will tighten and this function will need to consult the same calendar so a
 * Friday-before-a-bank-holiday submit does not start failing in production.
 *
 * TODO(APP-125): consume the holiday calendar when APP-125 ships.
 *
 * Exported AND covered by `commandClient.spotSettlementDate.test.ts`. The
 * production code path samples wall-clock once per submit (see {@code submitOrder})
 * and passes the SAME `Date` to both `transactTime` and this function so a
 * single submit cannot drift across a midnight UTC boundary mid-encode.
 */
export function spotSettlementDate(now: Date): string {
  const d = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
  let added = 0;
  while (added < 2) {
    d.setUTCDate(d.getUTCDate() + 1);
    const dow = d.getUTCDay(); // 0 = Sun, 6 = Sat
    if (dow !== 0 && dow !== 6) {
      added++;
    }
  }
  const y = d.getUTCFullYear();
  const m = d.getUTCMonth() + 1;
  const day = d.getUTCDate();
  return `${String(y)}${String(m).padStart(2, "0")}${String(day).padStart(2, "0")}`;
}

/** Per the trading-schema.xml CommandAckStatus enum (Accepted/Rejected/Duplicate/Throttled). */
export type CommandAckStatusName = "Accepted" | "Rejected" | "Duplicate" | "Throttled";

export interface CommandAckResult {
  readonly status: CommandAckStatusName;
  readonly correlationId: number;
}

export interface NewOrderSinglePayload {
  readonly clOrdId: string;
  /**
   * Canonical 6-char FX symbol (e.g. {@code "EURUSD"}). The OrderEntryForm
   * accepts the human-friendly slashed form ({@code "EUR/USD"}) and strips
   * the slash before constructing this payload.
   */
  readonly symbol: string;
  readonly side: "buy" | "sell";
  /** Fixed-point quantity (× 10^8). */
  readonly qty: bigint;
  /** Fixed-point price (× 10^8). */
  readonly price: bigint;
  /**
   * Authenticated user's account code. Caller MUST supply this from the
   * authenticated session context (the JWT {@code accounts} claim resolved at
   * AuthAck time); the OrderEntryForm threads it through from its
   * {@code accountCode} prop. Hardcoding it here would silently route every
   * order through the dev fixture's `ACME` account regardless of who is
   * signed in — the original Gemini HIGH finding on this path.
   */
  readonly accountCode: string;
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

/**
 * Thrown when {@link NewOrderSinglePayload.accountCode} is missing, empty, or
 * whitespace-only. The TypeScript type already prevents the missing case at
 * compile time, but `""` and `"   "` both satisfy `string` and would be
 * SBE-encoded as an effectively empty char field — which the cluster would
 * reject opaquely. Failing loudly on the client surfaces the bug at its origin.
 */
export class InvalidAccountCodeError extends Error {
  constructor() {
    super("commandClient: payload.accountCode is required and must be non-blank");
    this.name = "InvalidAccountCodeError";
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

/**
 * Browser-side wall-clock that produces fractional epoch-milliseconds via
 * {@code performance.timeOrigin + performance.now()}.
 *
 * CLAUDE.md §Clock Usage forbids {@code Date.now()} on the hot path. The browser
 * equivalent of an injected {@code EpochNanoClock} is
 * {@code performance.timeOrigin + performance.now()} — monotonic (no NTP step
 * inside a single document lifetime) and anchored to wall-clock epoch via the
 * Performance API spec. Returns a {@code number} (fractional ms); callers floor
 * before converting to {@code bigint} for SBE wire fields.
 *
 * Exposed as a default so tests can inject a deterministic clock without
 * patching {@code Date} globally.
 */
export type EpochMillisClock = () => number;

/** Default wall-clock — performance-based, NTP-immune within a document lifetime. */
export const defaultEpochMillisClock: EpochMillisClock = () =>
  performance.timeOrigin + performance.now();

/**
 * Stable {@link performance.mark} / {@link performance.measure} names for the submit-order
 * cold-path observability surface (APP-244 Phase 3 C.5).
 *
 * The browser PerformanceTimeline interns mark names — re-using the same three strings across
 * submits avoids the per-call string-allocation overhead that a templated name (e.g.
 * `submitOrder.${seq}`) would otherwise incur. The end-mark is keyed on the literal start
 * mark name, so concurrent submits all map to the same pair; `performance.measure` resolves
 * the most-recent start mark, giving an approximate-but-stable measurement that's good enough
 * for the cold-path budget tracking this exposes.
 *
 * Marks are GATED on real submission (post early-return); they never fire on the no-op
 * rejection paths (disposed / invalid account / backpressure / slot collision), preserving
 * the zero-allocation invariant for those branches.
 */
export const PERF_MARK_SUBMIT_ORDER_START = "submit.order.start";
export const PERF_MARK_SUBMIT_ORDER_END = "submit.order.end";
export const PERF_MEASURE_SUBMIT_ORDER = "submit.order";

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
  /** Wall-clock source for transactTime + settlement-date math. CLAUDE.md §Clock Usage. */
  private readonly epochMillisClock: EpochMillisClock;
  private scanTimer: ReturnType<typeof setInterval> | null = null;
  private disposed = false;

  /**
   * @param worker the underlying WorkerClient transport.
   * @param epochMillisClock OPTIONAL — injected wall-clock for deterministic testing. Defaults to
   *     {@code performance.timeOrigin + performance.now()} per CLAUDE.md §Clock Usage.
   */
  constructor(worker: WorkerClient, epochMillisClock: EpochMillisClock = defaultEpochMillisClock) {
    this.worker = worker;
    this.epochMillisClock = epochMillisClock;
    this.slots = new Array<Slot>(SLOT_COUNT);
    for (let i = 0; i < SLOT_COUNT; i++) this.slots[i] = newSlot();
    // The pool is indexed by `slotIdx = seq & SLOT_MASK` (range 0..SLOT_COUNT-1),
    // so it MUST be sized to SLOT_COUNT — not MAX_IN_FLIGHT. A pool sized to the
    // smaller MAX_IN_FLIGHT (256) was previously masked by a `?? new Uint8Array(...)`
    // fallback that allocated on demand for slotIdx ≥ 256, silently violating the
    // alloc-tripwire's "zero allocation after warmup" contract for any slot in
    // 256..1023. Sizing to SLOT_COUNT removes the fallback need entirely.
    this.outboundPool = new Array<Uint8Array>(SLOT_COUNT);
    for (let i = 0; i < SLOT_COUNT; i++) {
      this.outboundPool[i] = new Uint8Array(POOL_BUFFER_SIZE);
    }

    this.ackSub = this.worker.commandAcks$.subscribe((ack) => {
      this.handleAck(ack);
    });
    this.stateSub = this.worker.connectionState$.subscribe((s) => {
      // Eagerly fail every in-flight slot on transitions the breaker treats as
      // terminal (worker won't reconnect). RECONNECTING is intentionally NOT in
      // this set — the auto-recovery flow uses a separate slot timeout, and a
      // fast reconnect can still resolve in-flight Promises with the matching
      // CommandAck. DOWN is retained for symmetry with `messageSource.ts`'s
      // local fallback push paths (lines 151/187/217), which can still emit
      // DOWN when the WorkerClient stream errors out before any worker-side
      // transition is observed.
      if (
        s === "DOWN" ||
        s === "WORKER_DEAD" ||
        s === "DOWN_REQUIRES_USER_ACTION" ||
        s === "PROTOCOL_VIOLATION" ||
        s === "SCHEMA_MISMATCH"
      ) {
        this.failAllInFlight(new ConnectionLostError());
      }
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
    // The payload's `accountCode` is typed as a required string, but `""`,
    // `"   "`, `null`, and `undefined` all reach this line at runtime (the
    // first two are valid `string` values; the last two slip through an
    // untyped JS caller). All four would SBE-encode as an empty/whitespace
    // char field that the cluster would reject opaquely; fail loud at the
    // client edge instead.
    if (!payload.accountCode || payload.accountCode.trim() === "") {
      return Promise.reject(new InvalidAccountCodeError());
    }
    if (this.inFlight >= MAX_IN_FLIGHT) {
      return Promise.reject(new BackpressureError());
    }
    // u32 sequence: increment + mask. JS `number` is safe through 2^53; the mask
    // ensures the wire field stays in u32 range. 0 is reserved as "slot free"
    // sentinel. After u32 wrap (~2^32 submits) the candidate would be 0; we skip
    // to 1 so the slot index 0 remains the sentinel.
    let candidateSeq = (this.nextSeq + 1) & 0xffffffff;
    if (candidateSeq === 0) candidateSeq = 1;
    const slotIdx = candidateSeq & SLOT_MASK;
    const slot = this.slots[slotIdx];
    if (slot?.seq !== 0) {
      // Slot occupied — commit the seq advance SO the next call probes a
      // different slot. Without this commit, repeated rejections at the
      // wrap-around boundary (where candidateSeq = 1 and slot 1 is occupied
      // by a long-pending submit) would loop forever on the same candidateSeq.
      // Advancing past the collision is the right semantic: the number IS
      // burned by the in-flight occupier.
      this.nextSeq = candidateSeq;
      return Promise.reject(new RequestIdCollisionError(candidateSeq));
    }
    // Commit the seq advance — slot is free, we are taking it.
    this.nextSeq = candidateSeq;
    const seq = candidateSeq;
    return new Promise<CommandAckResult>((resolve, reject) => {
      slot.seq = seq;
      slot.resolve = resolve;
      slot.reject = reject;
      // performance.now() is monotonic — wall-clock skew (NTP step / VM snapshot
      // restore / DST adjust / OS sleep) cannot move it backwards or forwards
      // independent of elapsed wall time. Using `Date.now()` here would let
      // a clock step fire a timeout prematurely or never (already fixed in
      // WorkerClient.ts; this site is the same bug).
      slot.deadlineMs = performance.now() + SLOT_TIMEOUT_MS;
      this.inFlight++;
      // APP-244 Phase 3 C.5 — cold-path PerformanceTimeline mark for the submit-order
      // flow. Gated past the early-return rejection branches (disposed / invalid account /
      // backpressure / slot collision) so the no-op paths remain allocation-free. The
      // companion end mark + measure fires in handleAck / freeSlot.
      markSubmitOrderStart();

      // Real SBE encode (no JSON envelope, no synthetic ack). The pool is indexed by slot —
      // each in-flight slot owns its own buffer, so concurrent submits cannot collide. The
      // pool is fully populated in the constructor, so a missing slot here is an invariant
      // violation — fail loud rather than silently allocating a fresh Uint8Array (which would
      // also bypass the alloc-tripwire test's "zero-allocation after warmup" contract).
      const pooled = this.outboundPool[slotIdx];
      if (pooled === undefined) {
        this.freeSlot(slot);
        reject(
          new Error(
            `commandClient: outboundPool slot ${String(slotIdx)} missing — pool invariant violated`,
          ),
        );
        return;
      }
      const buf = pooled;
      const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
      try {
        // Derive currency from the symbol's quote-currency slot (last 3 chars
        // of the canonical 6-char form). For spot FX the settlement currency
        // equals the quote currency, so a USD/JPY order settles in JPY, not
        // the previously-hardcoded USD. `replaceAll` (not `replace`) so a
        // caller that passes "EUR/U/SD" still resolves to a 6-char canonical
        // form rather than leaving a stray slash that would shift the slice.
        //
        // settlDate is computed via spotSettlementDate() — T+2 calendar days
        // skipping Saturday/Sunday. A full holiday calendar is APP-125
        // (Holiday / settlement calendars — value-date math for FX
        // spot/forward/swaps); the dev fixture only exercises weekend skip.
        //
        // Wall-clock is sampled ONCE per submit (`nowMs` / `nowDate`) and
        // both `transactTime` and `settlDate` derive from the same instant.
        // Sampling twice would let a midnight-UTC tick between the two reads
        // emit a transactTime in day N nanos with a settlDate computed from
        // day N+1 — an off-by-one-business-day skew. Atomic by construction.
        //
        // Clock source is the injected `epochMillisClock` (defaults to
        // `performance.timeOrigin + performance.now()`) — `Date.now()` is
        // forbidden by CLAUDE.md §Clock Usage. `Math.floor` discards the
        // fractional ms before the BigInt × 1_000_000n nanos conversion.
        const canonicalSymbol = payload.symbol.replaceAll("/", "");
        const quoteCcy = canonicalSymbol.slice(3, 6);
        const nowMs = Math.floor(this.epochMillisClock());
        const nowDate = new Date(nowMs);
        this.encoder.wrapAndApplyHeader(view, 0).setFields({
          clOrdId: payload.clOrdId,
          quoteId: "",
          symbol: canonicalSymbol,
          side: payload.side === "buy" ? SideEnum.Buy : SideEnum.Sell,
          ordType: OrdTypeEnum.Limit,
          price: payload.price,
          orderQty: payload.qty,
          timeInForce: TimeInForceEnum.Day,
          transactTime: BigInt(nowMs) * 1_000_000n,
          accountCode: payload.accountCode,
          productType: ProductTypeEnum.Spot,
          settlDate: spotSettlementDate(nowDate),
          settlType: SettlTypeEnum.Regular,
          currency: quoteCcy,
          settlCurrency: "",
          tenor: TenorEnum.SN,
        });
      } catch (e: unknown) {
        this.freeSlot(slot);
        reject(e instanceof Error ? e : new Error(String(e)));
        return;
      }
      const length = NewOrderSingleEncoder.ENCODED_FRAME_LENGTH;
      // Invariant tripwire — POOL_BUFFER_SIZE is defined as
      // NewOrderSingleEncoder.ENCODED_FRAME_LENGTH so this branch is
      // unreachable today. Kept as defence-in-depth: if a future schema
      // change adds a variable-length field and the two constants drift
      // (the build catches a change to ENCODED_FRAME_LENGTH first), this
      // turns the regression into a typed rejection at the client edge
      // instead of a buffer overrun in the SBE encoder.
      if (length > POOL_BUFFER_SIZE) {
        this.freeSlot(slot);
        reject(new EncoderOverflowError(length));
        return;
      }
      try {
        // `submitCommand` uses structured-clone (NOT Transferable) so the
        // pooled buffer stays attached. The pool slot is therefore reusable
        // on the NEXT submit that lands in this slotIdx — genuinely zero-alloc
        // after warmup. (Previously the buffer was transferred and the pool
        // slot had to be re-allocated; that contradicted the alloc-tripwire.)
        this.worker.submitCommand(buf, length, seq);
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
    // Monotonic clock — paired with the `performance.now()`-based deadline above.
    const now = performance.now();
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
    if (slot.seq !== 0) {
      this.inFlight--;
      // APP-244 Phase 3 C.5 — paired end mark + measure for the submit-order cold-path
      // observability surface. Fires only when transitioning out of an actually-occupied
      // slot, so the early-return rejection paths (which call freeSlot with seq==0 on
      // the encoder-overflow / pool-missing branches via inFlight already being unchanged
      // before slot assignment) do not trigger the mark. We DO end the mark on the
      // post-submit pool-missing / encode-throw branches because at that point the slot
      // WAS taken (seq != 0) and the start mark fired.
      markSubmitOrderEnd();
    }
    slot.seq = 0;
    slot.resolve = null;
    slot.reject = null;
    slot.deadlineMs = 0;
  }
}

// ===========================================================================
// Cold-path PerformanceTimeline helpers (APP-244 Phase 3 C.5).
//
// Module-scoped functions, not class methods, so they are called via direct
// reference (no `this` binding allocation) from CommandClient hot paths.
// The `typeof` guards make them safe in Node / jsdom environments where
// `performance` is defined but `mark` / `measure` may be missing.
// ===========================================================================

/**
 * Module-scoped helper — emits the {@link PERF_MARK_SUBMIT_ORDER_START} mark on the cold-path
 * submit boundary. Safe under jsdom / Node where {@code performance.mark} may be undefined.
 * Stable mark string (interned by the browser) — no per-call string allocation.
 */
function markSubmitOrderStart(): void {
  if (typeof performance === "undefined" || typeof performance.mark !== "function") return;
  performance.mark(PERF_MARK_SUBMIT_ORDER_START);
}

/**
 * Module-scoped helper — emits the end mark + measure pair for a settled submit. Defensively
 * swallows the {@code SyntaxError} that {@code performance.measure} throws if the start mark
 * has been evicted from the PerformanceTimeline ring buffer.
 */
function markSubmitOrderEnd(): void {
  if (typeof performance === "undefined" || typeof performance.mark !== "function") return;
  performance.mark(PERF_MARK_SUBMIT_ORDER_END);
  try {
    performance.measure(
      PERF_MEASURE_SUBMIT_ORDER,
      PERF_MARK_SUBMIT_ORDER_START,
      PERF_MARK_SUBMIT_ORDER_END,
    );
  } catch {
    /* PerformanceTimeline buffer eviction or browser-quirk — ignore */
  }
}

// PRICE_SCALE is exported for tests and OrderEntryForm input validation.
export { PRICE_SCALE };
