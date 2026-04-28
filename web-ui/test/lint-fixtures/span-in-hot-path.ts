/**
 * INTENTIONAL ESLint violation fixture for the
 * `local/no-span-in-hot-path` rule. `tracer.startSpan` is called
 * inside an `onmessage` handler — the rule must report it.
 *
 * DO NOT FIX. DO NOT INCLUDE IN `npm run lint`.
 */
import { tracer } from "@/shared/telemetry/otel";

class FakeSocket {
  onmessage = (_ev: MessageEvent): void => {
    // Hot-path violation — startSpan() allocates per call.
    const span = tracer.startSpan("hot-path-violation");
    span.end();
  };
}

export { FakeSocket };
