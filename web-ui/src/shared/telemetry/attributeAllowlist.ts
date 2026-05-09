/**
 * OTel attribute-key allow-list — hot-path safety + log-hygiene control.
 *
 * Every attribute key set on any web-ui span MUST appear in this allow-list.
 * The custom ESLint rule `local/no-otel-attribute-outside-allowlist` enforces
 * this lexically by inspecting object literals passed to `*.startSpan(name, {
 * attributes: { ... } })` and `Span.setAttribute(...)` calls.
 *
 * Rationale:
 *   1. Prevents accidental leakage of secrets (token, sessionId-in-full,
 *      JWT claims) via span attributes on a future hot-path edit.
 *   2. Locks the OTel attribute surface APP-245 must consume — prevents
 *      drift between `STATS` counter names and span attribute names.
 *   3. Attribute string allocation is bounded — no dynamic key construction.
 *
 * Allow-list scope:
 *   - All `web-ui.store.*` / `web-ui.worker.*` span names registered in
 *     `otel.ts`.
 *   - The `gap.timings` attribute carries the last-100 gap timings ring
 *     on `web-ui.worker.gap-batch`; emitted as an array of bigint
 *     deltas converted to a delimited string (allocation cost amortised
 *     across ≤1/s rate-limited span emission per §3 / §6 row 18).
 *
 * Threading: import-only — values are imported into otel.ts at module
 * init. Frozen Set lookup is O(1).
 *
 * Allocation: zero (frozen Set built at module scope).
 *
 * Plan reference: §3 / §4.8 / §6 row 28 / §6 row 54.
 */

/**
 * Allow-listed OTel attribute keys for ALL web-ui spans. Backed by a frozen
 * source array (`Object.freeze(...)` on a `Set` is a no-op against `add`/
 * `delete` mutators — it freezes own properties only, not contents — so we
 * freeze the source array and rely on the `ReadonlySet` type for compile-time
 * mutation guards). Adding a key is a deliberate review-time decision.
 */
const OTEL_ATTRIBUTE_KEYS = Object.freeze<readonly string[]>([
  // Existing (createStore + worker bootstrap) — preserved as-is.
  "store.name",
  "error.type",
  "error.message",
  "exception.type",
  "exception.message",
  "exception.stacktrace",
  "worker.id",

  // APP-36 additions (§3 cold-path span names, attributes referenced in §5):
  "ws.url", // wss-host / route — never includes ?token=
  "schema.id",
  "schema.version",
  "session.id-trunc", // last-4 hex of sessionId; never the full UUID
  "close.code", // numeric WebSocket close code
  "error.code", // application-level WebSocketErrorCode (§2.13)
  "gap.from",
  "gap.to",
  "gap.count",
  "gap.timings", // delimited string of last-100 gap timings (rate-limited)
  "subprotocol", // value-asserted on handshake echo
  "reauth.outcome", // "success" | "rejected" | "queue-overflow"
  "snapshot.id-trunc", // last-4 hex of snapshotId
  "snapshot.fragment-index",
  "snapshot.total-fragments",
  "stats.frames-decoded",
  "stats.bytes-decoded",
  "stats.crc-mismatches",
  "stats.gaps",
  "stats.reconnects",
  "stats.replay-frames",
  "stats.snapshot-bytes",
  "stats.buffered-amount-peak",
  "stats.degraded-timing-mode",
]);

/**
 * Public allow-list — typed `ReadonlySet<string>` for compile-time mutation
 * guards. Built once at module init from the frozen source array.
 */
export const OTEL_ATTRIBUTE_ALLOWLIST: ReadonlySet<string> = new Set<string>(OTEL_ATTRIBUTE_KEYS);

const OTEL_SPAN_NAMES_SOURCE = Object.freeze<readonly string[]>([
  "web-ui.store.subscribe",
  "web-ui.store.error",
  "web-ui.worker.start",
  "web-ui.worker.error",
  "web-ui.worker.auth",
  "web-ui.worker.reconnect",
  "web-ui.worker.crc-mismatch",
  "web-ui.worker.schema-mismatch",
  "web-ui.worker.buffer-overflow",
  "web-ui.worker.protocol-violation",
  "web-ui.worker.gap-batch",
]);

/**
 * Allow-listed span names — paired with the attribute set above for
 * the OTel telemetry contract test (§5.8).
 */
export const OTEL_SPAN_NAMES: ReadonlySet<string> = new Set<string>(OTEL_SPAN_NAMES_SOURCE);
