/**
 * Vite-canonical Web Worker loader.
 *
 * Why this lives in its own file:
 *   - Vite's worker plugin requires the literal `new Worker(new URL(...))`
 *     pattern at parse time to bundle the worker correctly.
 *   - APP-36 owns `./worker.ts`; this file's role is purely
 *     instantiation. Keeping it minimal lets APP-36 evolve worker
 *     internals without changing the consumer surface.
 *
 * Threading model: the returned `Worker` is a fresh thread with its
 * own message queue. Caller is responsible for posting and
 * disposing.
 */

/**
 * Construct a new web-ui Worker. Each call yields a fresh thread.
 *
 * @return a `Worker` instance; caller must `terminate()` on disposal.
 */
export function loadWorker(): Worker {
  return new Worker(new URL("./worker.ts", import.meta.url), {
    type: "module",
    name: "web-ui-worker",
  });
}
