/**
 * marketDataConflation — per-symbol latest-value conflation map + 30 Hz drain.
 *
 * Purpose
 * -------
 * Buffers inbound `MarketDataTick` (template 54) frames per symbol so the browser renders at a
 * UI-friendly cadence (~30 Hz) instead of bumping AG Grid for every raw tick (which can land at
 * up to 200 Hz per symbol from the pricing-service `MARKET_DATA_PUBLISH_CADENCE_MICROS = 5_000`).
 * The conflation map holds the LATEST frame per symbol; the drain emits one `PriceUpdate` per
 * symbol that received a tick since the last drain. Older frames within the drain window are
 * discarded by overwrite — exactly the "latest value per symbol" invariant the AG Grid stream
 * sink expects.
 *
 * Wire-up
 * -------
 * - {@link onTick}(packedSymbol, frame) — called by the worker's `onEvent` dispatch on every
 *   inbound `MarketDataTick`. O(1) Map upsert.
 * - {@link drain}() — exported so a test fixture can trigger the drain synchronously. In
 *   production, {@link install}() arms a `setInterval(drain, MARKET_DATA_RENDER_MS = 33)` once
 *   at worker module init; the interval handle is stored in a module-scoped `final` field for
 *   {@link dispose}() to clear on worker shutdown.
 * - {@link dispose}() — clears the conflation map + the setInterval handle. Idempotent.
 *
 * Module split rationale
 * ----------------------
 * Extracted from `worker.ts` per Plan-agent Rec-15 — the worker.ts file was already 700+ lines
 * before Phase 3, and inlining the conflation Map + drain + dispatch surface would have pushed
 * it past 1,100 lines, making the alloc tripwire's blame attribution diffuse.
 *
 * Packed-symbol Number keys
 * -------------------------
 * The Map is keyed by a 48-bit packed-symbol `number` (NOT `bigint`). Bigint values on V8 are
 * NOT interned — `Map.set(bigintKey, …)` allocates a fresh boxed key on every call even when
 * numerically equal. With 4 symbols × 200 Hz = 800 ticks/sec, a `Map<bigint, …>` would allocate
 * 800 bigint keys/sec → ~12.8 KB/sec churn just from key boxing. Switching to `Map<number, …>`
 * (small-integer fast path; zero alloc for safe-int values) eliminates this. The packing scheme
 * is documented in {@link ../shared/transport/SymbolPacking}.
 *
 * Drain consumer as stable reference
 * ----------------------------------
 * The `setInterval` callback is the EXPORTED `drain` function bound to the module-scoped state.
 * Storing it as a module-scoped `const` reference (assigned at module init) avoids per-tick
 * closure allocation. TS analog of the Java "SAM consumer is a `final` field" rule.
 *
 * Background-tab throttling
 * -------------------------
 * Chromium throttles `setInterval` to ≥ 1 Hz in background tabs. Plan §Commit 6 Risk #2 chose
 * mitigation (c): document the wake-up cost. When the tab returns to foreground after extended
 * background time, the next drain processes the most recent value per symbol (conflation
 * guarantees this) and a first-foreground frame-time spike is expected. See
 * `docs/full-stack-e2e.md` §Background-tab behaviour for the operator-facing note.
 *
 * Threading
 * ---------
 * Worker scope only — single-threaded. Map mutated in-place; no synchronisation.
 *
 * Allocation
 * ----------
 * Per `onTick`: zero allocation. Map keys are packed-symbol Numbers (small-integer fast path).
 * Values are {@link MarketDataTickFrame} instances — the caller (worker dispatch) MUST construct
 * the frame inline from the SBE decoder reads; the conflation module never wraps or copies.
 *
 * Per `drain` cycle: one transient closure for the emit-each iteration if `Map.forEach` is used;
 * a `for (const [k, v] of map)` `for-of` loop allocates an iterator. To stay zero-alloc, the
 * drain uses `map.forEach(drainConsumer)` where `drainConsumer` is a module-scoped stable
 * reference (allocated once at module init). The {@link PriceUpdate} struct constructed inside
 * `drainConsumer` IS a per-symbol allocation; this is unavoidable for the structured-clone
 * postMessage transfer. The alloc tripwire (Plan §Commit 6 tests) budgets 24 bytes/frame.
 *
 * Idle ticks (empty map) are zero-alloc — `forEach` over an empty map is O(0).
 *
 * Dispose
 * -------
 * {@link dispose}() clears the interval handle + the map. Idempotent so the worker's `terminate`
 * handler can call it without checking init state.
 *
 * @see ../shared/transport/SymbolPacking
 * @see ./gapDetector
 */

import type { PriceUpdate } from "../shared/transport/MessageShape";

/**
 * 30 Hz drain cadence in milliseconds. Matches the AG Grid `asyncTransactionWaitMillis = 16`
 * ceiling (Plan-agent Rec-12) — driving the conflation drain at ~33 ms lets AG Grid batch
 * transactions across two drain cycles without dropping frames.
 */
export const MARKET_DATA_RENDER_MS = 33;

/**
 * Stable {@link performance.mark} / {@link performance.measure} names for the drain-cycle
 * observability surface (APP-244 Phase 3 C.5).
 *
 * The browser PerformanceTimeline interns mark / measure names internally — re-using the same
 * three strings across cycles avoids the per-call string-allocation overhead that a templated
 * name (e.g. `drain.cycle.${count}`) would incur. Cold-path only: marks are placed exclusively
 * when a non-empty drain is about to execute (gated on `latest.size > 0`); the zero-tick idle
 * path performs no PerformanceTimeline writes.
 */
export const PERF_MARK_DRAIN_START = "ws.drain.cycle.start";
export const PERF_MARK_DRAIN_END = "ws.drain.cycle.end";
export const PERF_MEASURE_DRAIN_CYCLE = "ws.drain.cycle";

/**
 * The per-symbol frame buffered between drain cycles. Mirrors the {@link PriceUpdate} surface
 * but is internal to the conflation module — the drain converts it to a real {@link PriceUpdate}
 * for postMessage. Stored by value (not by reference into the SBE decoder) so the underlying
 * Aeron buffer can be reused.
 */
export interface MarketDataTickFrame {
  readonly symbol: string;
  readonly bid: bigint;
  readonly ask: bigint;
  readonly bidSize: bigint;
  readonly askSize: bigint;
  readonly ingressNanos: bigint;
  readonly serverNanos: bigint;
}

/**
 * Sink for the drained {@link PriceUpdate} batch. The worker passes a function that pushes onto
 * its `outboundBatch` array which is flushed via postMessage at the next batched send.
 */
export type DrainSink = (update: PriceUpdate) => void;

/**
 * Per-symbol conflation map + drain scheduler. One instance per worker (sole source of truth
 * for the latest tick per symbol).
 */
export class MarketDataConflation {
  /**
   * Per-symbol latest-frame buffer. Key = packed-symbol `number` (48-bit; small-integer fast
   * path). Value = the most recent {@link MarketDataTickFrame} since the last drain.
   */
  private readonly latest = new Map<number, MarketDataTickFrame>();

  /** Sink for drained {@link PriceUpdate}s — assigned by the constructor; stable reference. */
  private readonly sink: DrainSink;

  /**
   * Stable, pre-bound drain-consumer reference. `Map.forEach` calls this once per (value, key)
   * pair. Bound at construction so the bound `this` is captured exactly once; subsequent
   * `forEach` calls do not allocate a new closure. The implementation reads conflation state
   * via the bound `this` (TypeScript class methods auto-bind via the prototype, but for the
   * `forEach` callback contract we pass a method reference and TS infers the right `this`).
   */
  private readonly drainConsumer: (value: MarketDataTickFrame) => void;

  /**
   * `setInterval` handle from {@link install}. Stored so {@link dispose} can `clearInterval`.
   * `null` before install / after dispose. Type is `ReturnType<typeof setInterval>` so the
   * worker-scope `Window` vs `Node` `Timeout` divergence is handled correctly.
   */
  private intervalHandle: ReturnType<typeof setInterval> | null = null;

  /**
   * Epoch-time source in milliseconds. Gemini iter-2 review (CRITICAL,
   * marketDataConflation.ts:239): the previous default of bare {@code performance.now()}
   * returned milliseconds since {@code performance.timeOrigin} (page load), NOT since the
   * Unix epoch. Subtracting that from {@code value.serverNanos} (which IS epoch nanos from
   * the server's {@code EpochNanoClock}) produced nonsense — the two clocks have different
   * origins and the result was off by the elapsed time since page load.
   *
   * <p>The fix: default to {@code performance.timeOrigin + performance.now()} which IS
   * monotonic AND epoch-based — {@code performance.timeOrigin} is a constant epoch-ms value
   * captured at page-load, and {@code performance.now()} contributes the monotonic
   * high-resolution delta. The sum is therefore epoch milliseconds with sub-millisecond
   * precision, suitable for subtraction with server-side epoch nanos.
   *
   * <p>Injected via the constructor so the alloc-tripwire test can pass a deterministic
   * source. Cross-box latency math still requires PTP / chrony for clock sync; see
   * {@code docs/clock-sync.md}.
   */
  private readonly nowEpochMillis: () => number;

  /**
   * @param sink callback invoked once per drained {@link PriceUpdate}; typically pushes onto the
   *     worker's `outboundBatch` for batched postMessage transfer
   * @param nowEpochMillis epoch-millisecond clock (worker hot path forbids
   *     {@code Date.now()}, but the {@code performance.timeOrigin + performance.now()}
   *     compound IS allowed because it is monotonic AND epoch-based). Defaults to that
   *     compound; inject a fake in tests
   */
  constructor(
    sink: DrainSink,
    nowEpochMillis: () => number = () => performance.timeOrigin + performance.now(),
  ) {
    this.sink = sink;
    this.nowEpochMillis = nowEpochMillis;
    // Bind once at construction so the `setInterval` and `Map.forEach` callbacks reuse the same
    // closure reference across cycles — no per-tick allocation. The arrow-fn variant would
    // ALSO be allocated only once for the same reason, but explicit `bind` makes the intent
    // unambiguous to a code reviewer.
    this.drainConsumer = this.emitOne.bind(this);
  }

  /**
   * Record an inbound {@code MarketDataTick}. Overwrites the per-symbol slot (latest-value
   * conflation; older value within the drain window is discarded).
   *
   * @param packedSymbol the 48-bit packed-symbol `number` (from
   *     {@link ../shared/transport/SymbolPacking.pack} / `packBytes`)
   * @param frame the decoded tick frame; the conflation module retains the reference until the
   *     next drain — callers MUST NOT reuse the frame object across calls (cheap to construct
   *     inline at the worker dispatch since the values are all primitive bigints / strings)
   */
  onTick(packedSymbol: number, frame: MarketDataTickFrame): void {
    this.latest.set(packedSymbol, frame);
  }

  /**
   * Cold accessor returning the symbol-string of the latest buffered frame for the given
   * packed-symbol key, or {@code undefined} if no frame has been recorded since the last
   * drain. Used by the worker dispatcher to avoid allocating a fresh String via the SBE
   * decoder's {@code symbol()} method on every tick when the conflation map already holds
   * the canonical string from a prior tick within the same drain window (Gemini iter-2
   * review, MEDIUM, clusterEventDecoder.ts:283).
   */
  peekSymbol(packedSymbol: number): string | undefined {
    return this.latest.get(packedSymbol)?.symbol;
  }

  /**
   * Arm the periodic drain timer. Idempotent — a second call is a no-op so the worker's init
   * path can be re-entrant. Must be called once after construction in production; tests that
   * drive {@link drain} synchronously may skip it.
   */
  install(): void {
    if (this.intervalHandle !== null) return;
    // `setInterval` with a STABLE top-level callback reference — drainConsumer is the bound
    // method, but here we use a thin wrapper that calls this.drain() directly. The wrapper
    // arrow is allocated ONCE here at install time, not per tick.
    this.intervalHandle = setInterval(() => {
      this.drain();
    }, MARKET_DATA_RENDER_MS);
  }

  /**
   * Drain the conflation map: emit one {@link PriceUpdate} per buffered symbol via the sink,
   * then clear the map. Exported so tests can drive the drain synchronously; the production
   * `setInterval` calls this every {@link MARKET_DATA_RENDER_MS} ms.
   */
  drain(): void {
    if (this.latest.size === 0) {
      // Idle tick — fast path; zero allocation. The `forEach` over an empty map is O(0) but
      // still incurs a tiny interpreter overhead; the explicit early-return is cheaper and
      // documents the intent.
      //
      // Observability: PerformanceTimeline writes are GATED on this early-return — no `mark` /
      // `measure` calls on the idle path. Calling `performance.mark` on an idle tick would
      // still allocate a `PerformanceMark` entry and inflate the worker's PerformanceTimeline
      // buffer, which is exactly the no-op-allocation that APP-244 C.5 forbids.
      return;
    }
    // APP-244 Phase 3 C.5 — cold-path drain-cycle observability via the browser's
    // PerformanceTimeline. Mark names are stable interned strings (see PERF_MARK_*
    // constants above) so a busy worker re-uses the same three strings forever rather than
    // allocating templated names per cycle. `performance.measure` resolves the start mark by
    // name to a timestamp internally — no need to thread the start time through user code.
    //
    // performance.mark / measure are HostObject methods, present in both Window and
    // WorkerGlobalScope (the conflation module runs in a DedicatedWorker). The `typeof`
    // guards keep this safe under jsdom / Node test environments where `performance` exists
    // but `mark` may not — fall back to plain drain in that case.
    const perf: Performance | undefined =
      typeof performance !== "undefined" ? performance : undefined;
    const canMark = perf !== undefined && typeof perf.mark === "function";
    if (canMark) {
      perf.mark(PERF_MARK_DRAIN_START);
    }
    // Gemini iter-4 review (MEDIUM, marketDataConflation.ts:237): if the sink callback (which
    // eventually calls `postMessage`) throws, the map must still be cleared so the next drain
    // does not re-emit the same frames. Wrap in try-finally — the throw still propagates to
    // the setInterval callback (which logs + continues per the setTimeout error model), but
    // the cleared map ensures correctness on the next tick.
    try {
      this.latest.forEach(this.drainConsumer);
    } finally {
      this.latest.clear();
      if (canMark) {
        perf.mark(PERF_MARK_DRAIN_END);
        // measure() can throw a SyntaxError if the start mark was evicted (the
        // PerformanceTimeline has a ring-buffer cap). Swallow defensively — a missing
        // measure is strictly better than poisoning the drain cycle.
        try {
          perf.measure(PERF_MEASURE_DRAIN_CYCLE, PERF_MARK_DRAIN_START, PERF_MARK_DRAIN_END);
        } catch {
          /* PerformanceTimeline buffer eviction or browser-quirk — ignore */
        }
      }
    }
  }

  /**
   * Tear down the drain timer + clear the map. Idempotent.
   */
  dispose(): void {
    if (this.intervalHandle !== null) {
      clearInterval(this.intervalHandle);
      this.intervalHandle = null;
    }
    this.latest.clear();
  }

  /**
   * Diagnostic accessor — number of symbols with buffered ticks pending drain. Cold path.
   */
  pendingCount(): number {
    return this.latest.size;
  }

  /**
   * `Map.forEach` callback. Reads the value, computes the end-to-end latency at drain time
   * (NOT at decode time — `endToEndLatencyNanos` includes the conflation residency), and emits
   * a {@link PriceUpdate} via the configured sink.
   *
   * Allocation: ONE {@link PriceUpdate} struct per call (unavoidable for postMessage transfer).
   * Alloc-tripwire budget: 24 bytes/frame (Plan §Commit 6 tests).
   */
  private emitOne(value: MarketDataTickFrame): void {
    // Epoch-millis × 1e6 → epoch-nanos, comparable with `value.serverNanos` (also epoch-ns
    // from the server's EpochNanoClock). The clock source is
    // `performance.timeOrigin + performance.now()` by default — see the field Javadoc for
    // why this compound (vs bare `performance.now()`) is the correct latency basis.
    const nowEpochMillis = this.nowEpochMillis();
    const endToEndLatencyNanos = BigInt(Math.trunc(nowEpochMillis * 1e6)) - value.serverNanos;
    const publisherStackLatencyNanos = value.serverNanos - value.ingressNanos;
    this.sink({
      type: "price",
      symbol: value.symbol,
      bid: value.bid,
      ask: value.ask,
      bidSize: value.bidSize,
      askSize: value.askSize,
      ingressNanos: value.ingressNanos,
      serverNanos: value.serverNanos,
      publisherStackLatencyNanos,
      endToEndLatencyNanos,
    });
  }
}
