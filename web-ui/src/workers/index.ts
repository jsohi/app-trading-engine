/**
 * Public surface for the worker module — `loadWorker` only.
 * Internal `worker.ts` is intentionally NOT re-exported (it must be
 * imported via the canonical Vite worker URL idiom in `loadWorker`).
 */
export { loadWorker } from "./loadWorker";
