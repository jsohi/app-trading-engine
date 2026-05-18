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
 *   onUnexpectedServerTemplate} handler fires (any unknown server template).
 *
 * <p>Templates handled as of Phase 3 Commit 6 (market-data broadcast slice):
 * <ul>
 *   <li>100/101/102/103/112 — order lifecycle events (Commit 3).</li>
 *   <li>51 — PriceResponse misroute regression guard (Commit 3).</li>
 *   <li>54 — {@code MarketDataTick} → {@code PriceUpdate}.</li>
 *   <li>55 — {@code MarketDataHeartbeat} — drained but no main-thread message; reserved for the
 *       browser-side liveness tracker (future commit).</li>
 *   <li>57 — {@code MarketDataFeedStateChange} → {@code FeedStateMsg}.</li>
 * </ul>
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
  FeedStateEnum,
  MarketDataFeedStateChangeDecoder,
  MarketDataHeartbeatDecoder,
  MarketDataTickDecoder,
  OrderCanceledEventDecoder,
  OrderCancelRejectedEventDecoder,
  OrderCreatedEventDecoder,
  OrderFilledEventDecoder,
  OrderRejectedEventDecoder,
  SideEnum,
} from "@trading/sbe-codecs";

import { type ErrorMsg, type FeedState, type WorkerMessage } from "@/shared/transport/MessageShape";
import {
  pack as packSymbolByString,
  packView as packSymbolFromView,
} from "@/shared/transport/SymbolPacking";
import { type Stats } from "@/workers/protocol/Stats";

/**
 * SBE message-header length in bytes (blockLength u16 + templateId u16 + schemaId u16 +
 * version u16). All decoders' {@code wrap(view, offset)} must skip past these 8 bytes to land
 * on the message body — matches the discipline in {@code MessageRouter} (where the same constant
 * gates {@code authAckDec.wrap(view, SBE_HEADER_BYTES)} etc.).
 */
const SBE_HEADER_BYTES = 8;

/**
 * SBE templateId for PriceResponse. Hard-coded rather than imported from a
 * decoder because PriceResponse is orchestrator-bound — the browser worker has
 * no PriceResponseDecoder import and is not expected to. The constant is
 * purely the regression-guard signature for the misroute counter.
 */
export const PRICE_RESPONSE_TEMPLATE_ID = 51;

/**
 * Worker-side error channel categories the dispatcher uses. Aliased to the canonical
 * {@link ErrorMsg.code} union from the worker→main wire contract so a future change to the
 * accepted error-code set is detected at compile time at every {@code postError} call site —
 * eliminates the silent-drift hazard of a structural-copy union here.
 */
export type WorkerErrorCode = ErrorMsg["code"];

/** Injected callbacks the dispatcher uses to deliver decoded results and surface errors. */
export interface ClusterEventDeps {
  /** Enqueue a typed `WorkerMessage` onto the worker→main outbound batch. */
  readonly emit: (msg: WorkerMessage) => void;
  /** Surface a PROTOCOL-channel error to the main thread. */
  readonly postError: (code: WorkerErrorCode, hint: string) => void;
  /** Counter surface — receives the misroute increment on template 51. */
  readonly stats: Stats;
  /**
   * Phase 3 Commit B — per-symbol conflation map + 30 Hz drain. Required: template 54 stages the
   * tick into conflation; the conflation drain emits the `PriceUpdate` via its own sink which the
   * worker bootstrap binds to the same `emit` callback.
   */
  readonly conflation: MarketDataConflationLike;
  /**
   * Phase 3 Commit B — per-symbol `symbolSeq` gap detector with publisher-vs-network attribution.
   * Required.
   */
  readonly gapDetector: GapDetectorLike;
}

/**
 * Structural type for {@link ../marketDataConflation.MarketDataConflation} — used as the
 * dependency contract so the dispatcher can be tested with a tiny stub without importing the
 * concrete class.
 */
export interface MarketDataConflationLike {
  onTick(packedSymbol: number, frame: MarketDataTickFrameLike): void;
  /**
   * Returns the symbol string of the latest buffered frame for {@code packedSymbol}, or
   * {@code undefined} if no frame is currently in the conflation map for that key. Used
   * by the dispatcher to short-circuit the per-tick {@code dec.symbol()} String allocation
   * (Gemini iter-2 review, MEDIUM, clusterEventDecoder.ts:283).
   */
  peekSymbol(packedSymbol: number): string | undefined;
}

/** Structural mirror of {@link ../marketDataConflation.MarketDataTickFrame}. */
export interface MarketDataTickFrameLike {
  readonly symbol: string;
  readonly bid: bigint;
  readonly ask: bigint;
  readonly bidSize: bigint;
  readonly askSize: bigint;
  readonly ingressNanos: bigint;
  readonly serverNanos: bigint;
}

/**
 * Structural type for {@link ../gapDetector.GapDetector} — used as the dependency contract.
 */
export interface GapDetectorLike {
  onTick(
    packedSymbol: number,
    symbolSeq: number,
  ): {
    readonly outcome: string;
    readonly publisherConflated: number;
    readonly network: number;
  };
  onHeartbeat(packedSymbol: number, lastPublishedSeq: number): void;
  onPublisherRestart(): void;
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
 * Map a {@link FeedStateEnum} wire ordinal to the {@link FeedState} string label. Defensive
 * default to {@code "LIVE"} on unknown wire bytes so a malformed inbound state cannot tip the
 * UI into a permanent STALE banner — the next legitimate state frame corrects it.
 */
function feedStateToLabel(ord: number): FeedState {
  if (ord === FeedStateEnum.Stale) return "STALE";
  if (ord === FeedStateEnum.Quiet) return "QUIET";
  return "LIVE";
}

/**
 * Dispatches the cluster domain event {@code payload} to the matching decoder
 * and emits the typed {@link WorkerMessage}.
 *
 * @param templateId the SBE templateId from the dispatch layer.
 * @param payload the full SBE message bytes — 8-byte SBE message header (blockLength /
 *     templateId / schemaId / version) followed by the message body. Matches the contract
 *     {@code MessageRouter.route} passes via its default arm:
 *     {@code this.handlers.onEvent(templateId, payload)} (the `payload` parameter is the same
 *     reference the router decoded the header from at offset 0). Decoders below wrap at
 *     {@code SBE_HEADER_BYTES} to land on the body.
 * @param deps callbacks for emit / postError / stats.
 * @returns `true` if the event was handled (a WorkerMessage was emitted, an
 *     error was posted, or the misroute counter incremented). `false` if the
 *     templateId is not one the dispatcher handles — the caller must invoke
 *     its `onUnexpectedServerTemplate` fallback (today the worker drops these
 *     silently). The exhaustive handled-template set is documented in the
 *     module-header bullet list above.
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
        const dec = new OrderCreatedEventDecoder().wrap(dv, SBE_HEADER_BYTES);
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
        const dec = new OrderRejectedEventDecoder().wrap(dv, SBE_HEADER_BYTES);
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
        const dec = new OrderFilledEventDecoder().wrap(dv, SBE_HEADER_BYTES);
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
        const dec = new OrderCanceledEventDecoder().wrap(dv, SBE_HEADER_BYTES);
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
        const dec = new OrderCancelRejectedEventDecoder().wrap(dv, SBE_HEADER_BYTES);
        deps.emit({
          type: "event",
          seq: dec.sequenceNumber(),
          eventType: "OrderCancelRejected",
          details: `clOrdId=${dec.clOrdId()} origClOrdId=${dec.origClOrdId()} reason=${String(dec.cxlRejReason())} text=${dec.text()}`,
          serverNanos: dec.timestamp(),
        });
        return true;
      }
      case MarketDataTickDecoder.TEMPLATE_ID: {
        const dec = new MarketDataTickDecoder().wrap(dv, SBE_HEADER_BYTES);
        const ingressNanos = dec.ingressNanos();
        const serverNanos = dec.serverNanos();
        // Gemini iter-2 review (MEDIUM, clusterEventDecoder.ts:283): avoid per-tick
        // String allocation from `dec.symbol()`. Pack directly from the underlying
        // DataView (zero alloc) and reuse the cached symbol string from the conflation
        // map if a prior tick within the same drain window already populated it. First
        // sight per symbol still allocates the String exactly once (via `dec.symbol()`),
        // but subsequent ticks for the same symbol pull from the cache.
        const packedSymbol = packSymbolFromView(dv, SBE_HEADER_BYTES + 0);
        const cachedSymbol = deps.conflation.peekSymbol(packedSymbol);
        const symbol = cachedSymbol ?? dec.symbol();
        // symbolSeq is int64 on the wire. The gap-attribution math runs in pure Number
        // space — bigint arithmetic on the hot path costs ~3x and per-symbol cursors stay
        // far below 2^53. Convert via explicit BigInt → Number truncation. The
        // `no-restricted-syntax` rule about Number coercion is for fixed-point money fields;
        // this is a sequence counter, not a price or quantity.
        // eslint-disable-next-line no-restricted-syntax
        const symbolSeq = Number(dec.symbolSeq());

        // Phase 3 Commit B routing: stage the tick into conflation + record gap attribution.
        // The conflation drain (30 Hz setInterval) emits the consolidated PriceUpdate
        // asynchronously via its own sink (the worker bootstrap binds it to the same `emit`
        // callback). gap counts feed Prometheus via the worker stats surface.
        deps.conflation.onTick(packedSymbol, {
          symbol,
          bid: dec.bidPrice(),
          ask: dec.askPrice(),
          bidSize: dec.bidSize(),
          askSize: dec.askSize(),
          ingressNanos,
          serverNanos,
        });
        const report = deps.gapDetector.onTick(packedSymbol, symbolSeq);
        if (report.publisherConflated > 0) {
          deps.stats.addGapsPublisherConflated(report.publisherConflated);
        }
        if (report.network > 0) {
          deps.stats.addGapsNetwork(report.network);
        }
        return true;
      }
      case MarketDataHeartbeatDecoder.TEMPLATE_ID: {
        const dec = new MarketDataHeartbeatDecoder().wrap(dv, SBE_HEADER_BYTES);
        // Touch the decoded serverNanos so the decoder is actually exercised (the alloc tripwire
        // would catch any silent regression). The serverNanos is used by the server-side
        // MarketDataSubscriptionLivenessTracker (Commit A) — the browser uses heartbeats for
        // the gap-attribution cursor below.
        void dec.serverNanos();
        // Phase 3 Commit B: update the gap detector's per-symbol publisher cursor from the
        // heartbeat's lastPublishedSeq repeating group. CME MDP 3.0 §Gap Detection — the
        // attribution math in gapDetector.onTick uses this cursor to discriminate publisher-
        // conflated drops from network drops on a subsequent observed gap. Each entry carries
        // (symbol, seq) where seq is the most-recently published symbolSeq for that symbol.
        // bigint → Number conversion is safe up to 2^53-1; symbolSeq is int64 on the wire but
        // realistic per-symbol counts stay far below that bound. Truncation guard via the
        // explicit Number cast keeps the gap-attribution math in pure Number space (no bigint
        // arithmetic on the hot path).
        const group = dec.lastPublishedSeq();
        while (group.hasNext()) {
          group.next();
          const packed = packSymbolByString(group.symbol());
          // Heartbeat seq is the publisher cursor (int64 on wire); kept in Number space
          // to match the gapDetector's Number-typed lastSeq map. Same rationale as the
          // tick-path symbolSeq cast above.
          // eslint-disable-next-line no-restricted-syntax
          deps.gapDetector.onHeartbeat(packed, Number(group.seq()));
        }
        return true;
      }
      case MarketDataFeedStateChangeDecoder.TEMPLATE_ID: {
        const dec = new MarketDataFeedStateChangeDecoder().wrap(dv, SBE_HEADER_BYTES);
        const label = feedStateToLabel(dec.state());
        // Phase 3 Commit B: on transition into LIVE the gap detector resets all per-symbol
        // cursors so a publisher-1 tick at seq=1 does NOT compute a negative gap against a
        // stale publisher-0 lastSeq. The liveness tracker re-emits LIVE on session attach so
        // this fires on publisher-restart recovery. Reset BEFORE the FeedStateMsg emit so the
        // main thread's `feedState$` transition and the worker's lastSeq clear are ordered
        // consistently (drain timer ticks at 33 ms can interleave with the dispatch).
        if (label === "LIVE") {
          deps.gapDetector.onPublisherRestart();
        }
        deps.emit({
          type: "feed-state",
          state: label,
          serverNanos: dec.serverNanos(),
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
        // Unknown server template — caller's onUnexpectedServerTemplate fallback handles
        // the silent-drop. The exhaustive set of explicitly-handled templates is documented
        // in this module's class-level Javadoc.
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
