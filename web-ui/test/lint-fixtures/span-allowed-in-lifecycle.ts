/**
 * POSITIVE-CASE fixture for `local/no-span-in-hot-path`. The rule
 * targets only `onmessage` / `next` handlers (the streaming hot path).
 * Lifecycle code paths — bootstrap, error handlers, subscribe wrappers
 * — are EXPLICITLY allowed to call `tracer.startSpan`. This fixture
 * exists so the test suite asserts the rule's negative space:
 *   - `lint-fixtures.test.ts` runs ESLint against this file and
 *     asserts the rule produces ZERO messages here, while the negative
 *     fixture (`span-in-hot-path.ts`) produces at least one.
 *
 * This file is in the lint-fixtures ignore block; it is linted only
 * via the `--no-ignore` CLI override in `lint-fixtures.test.ts`.
 */
import { tracer } from "@/shared/telemetry/otel";

// Lifecycle: a function NOT named `onmessage` or `next` may emit spans.
function bootstrapWorker(workerId: string): void {
  const span = tracer.startSpan("web-ui.worker.start", {
    attributes: { "worker.id": workerId },
  });
  span.end();
}

// Lifecycle: an error handler may emit a span (low-frequency, not hot).
// Note the function expression is bound to a property literally named
// `errorHandler` — explicitly NOT `next` or `onmessage` — so the rule
// does not flag it.
const obj = {
  errorHandler(err: unknown): void {
    const span = tracer.startSpan("web-ui.store.error", {
      attributes: {
        "error.type": err instanceof Error ? err.name : typeof err,
      },
    });
    span.end();
  },
};

export { bootstrapWorker, obj };
