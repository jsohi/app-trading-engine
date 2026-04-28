/**
 * OpenTelemetry-JS bootstrap for the web-ui.
 *
 * 1A scope:
 *   - Production: `NoopSpanProcessor` (no exporter; spans are still
 *     created but immediately discarded). The seam is ready for
 *     APP-245 to swap in a real exporter (vendor TBD) without
 *     touching consumers.
 *   - Tests: install a `BasicTracerProvider` with `InMemorySpanExporter`
 *     via `__installTestTracerProvider()` (test-only export).
 *
 * Telemetry contract (versioned with this file):
 *   - Span names:
 *       web-ui.store.subscribe   — createStore subscribe
 *       web-ui.worker.start      — Web Worker bootstrap (APP-36)
 *       web-ui.worker.error      — Web Worker error handler
 *   - Required attributes:
 *       store.name (string)      — on web-ui.store.subscribe
 *       worker.id (string)       — on web-ui.worker.*
 *
 * Hot-path discipline:
 *   - NEVER call `tracer.startSpan` inside per-message handlers
 *     (`onmessage`, RxJS `next`). Enforced by the custom ESLint
 *     rule `local/no-span-in-hot-path`.
 *
 * Threading model: main thread (and Web Worker scope when imported
 * there). The OTel API is module-scoped; `initialiseTelemetry()`
 * is idempotent.
 */
import { trace, type Tracer, type TracerProvider } from "@opentelemetry/api";
import { BasicTracerProvider, type SpanProcessor } from "@opentelemetry/sdk-trace-base";

const TRACER_NAME = "web-ui";

let initialised = false;

/**
 * No-op SpanProcessor — implements the SpanProcessor contract but
 * discards everything. Replaced by APP-245 with a real exporter.
 */
class NoopSpanProcessor implements SpanProcessor {
  forceFlush(): Promise<void> {
    return Promise.resolve();
  }
  onStart(): void {
    // intentional no-op
  }
  onEnd(): void {
    // intentional no-op
  }
  shutdown(): Promise<void> {
    return Promise.resolve();
  }
}

/**
 * Initialise the OTel global tracer provider with a no-op processor.
 * Idempotent — repeated calls are safe and cheap.
 */
export function initialiseTelemetry(): void {
  if (initialised) return;
  const provider = new BasicTracerProvider({
    spanProcessors: [new NoopSpanProcessor()],
  });
  trace.setGlobalTracerProvider(provider);
  initialised = true;
}

/**
 * Test-only utility: replace the global tracer provider with one
 * configured by the caller. Allows installing an
 * `InMemorySpanExporter` for assertion-driven telemetry tests.
 *
 * Not part of the production surface. Resetting `initialised` so
 * the production initialiser is reusable across test boundaries.
 */
export function __installTestTracerProvider(provider: TracerProvider): void {
  trace.setGlobalTracerProvider(provider);
  initialised = true;
}

/**
 * Reset the initialisation flag so a subsequent
 * `initialiseTelemetry()` call wires the no-op provider back in.
 * Test-only.
 */
export function __resetTelemetryForTests(): void {
  initialised = false;
}

/**
 * Shared tracer used across the web-ui. Allocates the proxy on
 * first read; subsequent reads are zero-allocation.
 */
export const tracer: Tracer = trace.getTracer(TRACER_NAME);

export const TELEMETRY_TRACER_NAME = TRACER_NAME;
