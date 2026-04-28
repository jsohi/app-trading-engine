/**
 * Global Vitest setup. Runs once per worker before any test file.
 *
 * Responsibilities:
 *   - Initialise the OTel test tracer (in-memory exporter) so
 *     individual tests can assert span emission without re-bootstrapping.
 *   - Stub browser globals not provided by jsdom but expected by
 *     production code (e.g. `Worker` is patched per-test only —
 *     not here).
 */
import { afterEach, beforeAll, beforeEach } from "vitest";
import {
  BasicTracerProvider,
  InMemorySpanExporter,
  SimpleSpanProcessor,
} from "@opentelemetry/sdk-trace-base";

import { __installTestTracerProvider, __resetTelemetryForTests } from "@/shared/telemetry/otel";

export const TEST_SPAN_EXPORTER = new InMemorySpanExporter();

beforeAll(() => {
  const provider = new BasicTracerProvider({
    spanProcessors: [new SimpleSpanProcessor(TEST_SPAN_EXPORTER)],
  });
  __installTestTracerProvider(provider);
});

beforeEach(() => {
  TEST_SPAN_EXPORTER.reset();
});

afterEach(() => {
  __resetTelemetryForTests();
  __installTestTracerProvider(
    new BasicTracerProvider({
      spanProcessors: [new SimpleSpanProcessor(TEST_SPAN_EXPORTER)],
    }),
  );
});
