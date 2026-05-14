/**
 * Global Vitest setup. Runs once per worker before any test file.
 *
 * Responsibilities:
 *   - Initialise the OTel test tracer (in-memory exporter) — jsdom path
 *     only; gated so the browser project (where node_modules-only OTel
 *     transitive deps may not bundle to ESM cleanly) stays unaffected.
 *   - Register AG Grid v33+ Community modules globally so blotter mounts
 *     under jsdom (`App.test.tsx`, `PanelGrid.test.tsx`) don't trip the
 *     missing-module `console.error` that would break the existing
 *     `errorSpy.not.toHaveBeenCalled()` invariant. APP-37 dropped AG Grid
 *     Enterprise entirely, so no `LicenseManager` stub is needed any more.
 *   - Register `afterEach` cleanup IN STRICT ORDER (Vitest runs them in
 *     registration order):
 *       (1) `cleanup` (React Testing Library) — unmounts the React tree
 *           FIRST so blotter `useEffect` subscriptions are torn down
 *           before module singletons get reset.
 *       (2) `__resetMessageSourceForTests` — swaps the private
 *           `_messages` ReplaySubject; completes the old one.
 *       (3) `__resetConnectionStreamForTests` — swaps the private
 *           `_subject` BehaviorSubject in connection-stream.
 *       (4) `__resetConnectionStoreForTests` — re-seeds the store's
 *           closure-local snapshot back to "CONNECTING".
 *     Reverse order would fire singleton resets while React subscribers
 *     are still alive → blotters log to `console.error` mid-teardown.
 *   - Stub browser globals not provided by jsdom but expected by
 *     production code (e.g. `Worker` is patched per-test only — not here).
 *
 * Plan reference: APP-37 §Files to modify (test/setup.ts).
 */
import { afterEach, beforeAll, beforeEach } from "vitest";
import { cleanup } from "@testing-library/react";
import {
  BasicTracerProvider,
  InMemorySpanExporter,
  SimpleSpanProcessor,
} from "@opentelemetry/sdk-trace-base";
import { __installTestTracerProvider, __resetTelemetryForTests } from "@/shared/telemetry/otel";
import { __resetMessageSourceForTests } from "@/main-thread/messageSource";
import { __resetConnectionStreamForTests } from "@/streams/connection-stream";
import { __resetConnectionStoreForTests } from "@/stores/connection-store";

// Side-effect: register AG Grid v33+ Community modules globally so blotter
// mounts under jsdom don't trip a missing-module console.error.
import "@/shared/grid/registerAgGridModules";

export const TEST_SPAN_EXPORTER = new InMemorySpanExporter();

// Detect jsdom vs @vitest/browser Chromium. jsdom has `process` AND lacks
// a browser-real `window` shape; Chromium polyfills `process` for npm-pkg
// compat, so the `typeof window` check (jsdom defines window) is the
// reliable side.
const IS_JSDOM_ENV: boolean =
  typeof globalThis.process !== "undefined" && typeof window !== "undefined";

if (IS_JSDOM_ENV) {
  beforeAll(() => {
    const provider = new BasicTracerProvider({
      spanProcessors: [new SimpleSpanProcessor(TEST_SPAN_EXPORTER)],
    });
    __installTestTracerProvider(provider);
  });

  beforeEach(() => {
    TEST_SPAN_EXPORTER.reset();
  });
}

// afterEach order is LOAD-BEARING — see file header.
// (1) Unmount React tree first.
afterEach(() => {
  cleanup();
});
// (2) Reset messageSource singleton.
afterEach(() => {
  __resetMessageSourceForTests();
});
// (3) Reset connection-stream singleton.
afterEach(() => {
  __resetConnectionStreamForTests();
});
// (4) Reset connection-store snapshot.
afterEach(() => {
  __resetConnectionStoreForTests();
});

// OTel reset (jsdom-only).
if (IS_JSDOM_ENV) {
  afterEach(() => {
    __resetTelemetryForTests();
    __installTestTracerProvider(
      new BasicTracerProvider({
        spanProcessors: [new SimpleSpanProcessor(TEST_SPAN_EXPORTER)],
      }),
    );
  });
}
