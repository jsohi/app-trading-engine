/**
 * Placeholder Web Worker entry — replaced by APP-36 with the real
 * SBE-decoding RxJS streaming worker.
 *
 * 1A behaviour: emits a single `web-ui.worker.start` lifecycle span
 * and idles. The mock `fakeStream.ts` (used by the dev server)
 * provides the data the eventual worker will produce.
 *
 * Threading model: dedicated worker thread. Communicates with main
 * thread exclusively via `postMessage`.
 */
// TODO(APP-36): replace this placeholder with the SBE-decoding worker.
import { tracer } from "@/shared/telemetry/otel";

const span = tracer.startSpan("web-ui.worker.start", {
  attributes: { "worker.id": "placeholder-1A" },
});
span.end();

// Keep the worker alive but inert. Main-thread bootstrap is free
// to terminate it whenever it likes.
self.onmessage = (): void => {
  // intentional no-op until APP-36 wires the real onmessage handler.
};

export {};
