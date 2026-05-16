/**
 * Cluster domain-event decoder — Phase 3 Commit 3.
 *
 * Pure-function dispatch table over inbound cluster event templateIds. The
 * worker calls {@link decodeClusterEvent} from its {@code onEvent} handler with
 * the templateId and the payload bytes; this module wraps the matching
 * SBE decoder, constructs the typed {@link WorkerMessage}, and either:
 * - invokes the {@code emit} callback to enqueue the message onto the
 *   worker→main batch, OR
 * - invokes {@code postError} to surface a PROTOCOL-channel error (template
 *   51 misroute or corrupt payload), OR
 * - returns {@code false} so the worker's default {@code
 *   onUnexpectedServerTemplate} handler fires (templates 54/55/57 reserved for
 *   Commit 6, plus any unknown server template).
 *
 * **Why extracted from worker.ts.** `worker.ts` is the Web Worker module entry
 * point with top-level side effects (WebSocket setup, postMessage listener,
 * stats timer). Importing it from a unit test triggers the bootstrap and
 * touches globals that vitest's jsdom environment doesn't provide. Extracting
 * the pure-function dispatch into a side-effect-free module lets the test
 * exercise the EXACT production code path against a captured `emit` / `postError`
 * / `stats` triple, NOT a re-implementation in the test fixture.
 *
 * Threading model: caller-driven; this module owns no shared state. Each call
 * mutates only the `deps.stats` instance and the (caller-provided) outbound
 * batch via the `emit` callback.
 *
 * Allocation: one DataView per call (over `payload.buffer`), plus one decoder
 * instance per call (`new XxxDecoder().wrap(...)`). Mirrors the CommandAck
 * decoder pattern at the worker's `onEvent` line 763. The `dec.toString` /
 * template-literal construction inside the EventUpdate cases allocates one
 * string per event; this is the dominant cost and is acceptable for non-hot
 * lifecycle events (rejected/canceled). Market-data (54/55/57) is handled in
 * Commit 6 with the alloc-tripwire-budgeted path.
 *
 * Dependencies: SBE decoders from `@trading/sbe-codecs`; the worker's outbound
 * `emit` callback (typed `WorkerMessage` → void); `Stats` counter surface;
 * the worker's `postError` callback.
 */

import {
  OrderCanceledEventDecoder,
  OrderCancelRejectedEventDecoder,
  OrderCreatedEventDecoder,
  OrderFilledEventDecoder,
  OrderRejectedEventDecoder,
  SideEnum,
} from "@trading/sbe-codecs";

import { type WorkerMessage } from "@/shared/transport/MessageShape";
import { type Stats } from "@/workers/protocol/Stats";

/**
 * SBE templateId for PriceResponse. Hard-coded rather than imported from a
 * decoder because PriceResponse is orchestrator-bound — the browser worker has
 * no PriceResponseDecoder import and is not expected to. The constant is
 * purely the regression-guard signature for the misroute counter.
 */
export const PRICE_RESPONSE_TEMPLATE_ID = 51;

/**
 * Worker-side error channel categories the dispatcher uses. Mirrors the
 * `ErrorMsg.code` discriminator on `WorkerMessage` so a future consolidation
 * (single source of truth) is a renaming, not a contract bump.
 */
export type WorkerErrorCode = "INIT" | "AUTH" | "CRC" | "PROTOCOL" | "SCHEMA" | "BUFFER" | "WORKER";

/** Injected callbacks the dispatcher uses to deliver decoded results and surface errors. */
export interface ClusterEventDeps {
  /** Enqueue a typed `WorkerMessage` onto the worker→main outbound batch. */
  readonly emit: (msg: WorkerMessage) => void;
  /** Surface a PROTOCOL-channel error to the main thread. */
  readonly postError: (code: WorkerErrorCode, hint: string) => void;
  /** Counter surface — receives the misroute increment on template 51. */
  readonly stats: Stats;
}

/**
 * Map a {@link SideEnum} wire ordinal to the {@code WorkerMessage.Side} string label. SBE
 * encodes side as a uint8 (`Buy=1`, `Sell=2`); the browser blotter consumes the string form. An
 * unrecognised wire byte defaults to {@code "BUY"} — the cluster's `SafeEnumMappers.safeSide`
 * applies the same fallback so a malformed inbound side cannot propagate as a typed wire value.
 */
function sideToLabel(ord: number): "BUY" | "SELL" {
  return ord === SideEnum.Sell ? "SELL" : "BUY";
}

/**
 * Dispatches the cluster domain event {@code payload} to the matching decoder
 * and emits the typed {@link WorkerMessage}.
 *
 * @param templateId the SBE templateId from the dispatch layer.
 * @param payload the body-only SBE bytes (header already stripped by `MessageRouter`).
 * @param deps callbacks for emit / postError / stats.
 * @returns `true` if the event was handled (a WorkerMessage was emitted, an
 *     error was posted, or the misroute counter incremented). `false` if the
 *     templateId is not one the dispatcher handles — the caller must invoke
 *     its `onUnexpectedServerTemplate` fallback.
 */
export function decodeClusterEvent(
  templateId: number,
  payload: Uint8Array,
  deps: ClusterEventDeps,
): boolean {
  try {
    const dv = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
    switch (templateId) {
      case OrderCreatedEventDecoder.TEMPLATE_ID: {
        const dec = new OrderCreatedEventDecoder().wrap(dv, 0);
        const price = dec.price();
        deps.emit({
          type: "order",
          clOrdId: dec.clOrdId(),
          symbol: dec.symbol(),
          side: sideToLabel(dec.side()),
          qty: dec.orderQty(),
          // Wire field is optional (null sentinel = market order). The
          // OrderBlotter renders 0n as the "—" placeholder via its column
          // valueFormatter, so 0n is a safe substitute that does not box.
          price: price ?? 0n,
          status: "OPEN",
          serverNanos: dec.timestamp(),
        });
        return true;
      }
      case OrderRejectedEventDecoder.TEMPLATE_ID: {
        const dec = new OrderRejectedEventDecoder().wrap(dv, 0);
        // EventUpdate (not OrderUpdate) — the OrderRejectedEvent SBE template
        // does not carry qty or price, so a partial OrderUpdate would corrupt
        // the existing row's fields under AG Grid's row-id merge. Lifecycle-
        // only events feed the event log; APP-244 follow-up will add status-
        // only OrderUpdate variants if blotter row status sync is needed.
        deps.emit({
          type: "event",
          seq: dec.sequenceNumber(),
          eventType: "OrderRejected",
          details: `clOrdId=${dec.clOrdId()} symbol=${dec.symbol()} reason=${String(dec.rejectReason())} text=${dec.text()}`,
          serverNanos: dec.timestamp(),
        });
        return true;
      }
      case OrderFilledEventDecoder.TEMPLATE_ID: {
        const dec = new OrderFilledEventDecoder().wrap(dv, 0);
        deps.emit({
          type: "fill",
          clOrdId: dec.clOrdId(),
          execId: dec.execId(),
          symbol: dec.symbol(),
          side: sideToLabel(dec.side()),
          fillQty: dec.lastQty(),
          fillPrice: dec.lastPx(),
          serverNanos: dec.timestamp(),
        });
        return true;
      }
      case OrderCanceledEventDecoder.TEMPLATE_ID: {
        const dec = new OrderCanceledEventDecoder().wrap(dv, 0);
        deps.emit({
          type: "event",
          seq: dec.sequenceNumber(),
          eventType: "OrderCanceled",
          details: `clOrdId=${dec.clOrdId()} origClOrdId=${dec.origClOrdId()} symbol=${dec.symbol()}`,
          serverNanos: dec.timestamp(),
        });
        return true;
      }
      case OrderCancelRejectedEventDecoder.TEMPLATE_ID: {
        const dec = new OrderCancelRejectedEventDecoder().wrap(dv, 0);
        deps.emit({
          type: "event",
          seq: dec.sequenceNumber(),
          eventType: "OrderCancelRejected",
          details: `clOrdId=${dec.clOrdId()} origClOrdId=${dec.origClOrdId()} reason=${String(dec.cxlRejReason())} text=${dec.text()}`,
          serverNanos: dec.timestamp(),
        });
        return true;
      }
      case PRICE_RESPONSE_TEMPLATE_ID: {
        // Regression guard: template 51 (PriceResponse) is orchestrator-
        // bound. If it reaches the browser, broadcast routing has regressed.
        // Increment the dedicated counter so spec 07 fails; surface as
        // PROTOCOL error so the UI flags it.
        deps.stats.incMarketdataMisroutedRfq();
        deps.postError(
          "PROTOCOL",
          `misrouted PriceResponse (template ${String(templateId)}) — orchestrator-bound, must not reach browser`,
        );
        return true;
      }
      default:
        // Templates 54/55/57 (Phase 3 Commit 6) and any other unknown server
        // template — caller's onUnexpectedServerTemplate handles.
        return false;
    }
  } catch (e: unknown) {
    deps.postError(
      "PROTOCOL",
      `event decode failed for template ${String(templateId)}: ${e instanceof Error ? e.message : String(e)}`,
    );
    return true;
  }
}
