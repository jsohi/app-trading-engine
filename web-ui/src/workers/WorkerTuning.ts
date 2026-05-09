/**
 * WorkerTuning — central, read-only constants for the SBE Web Worker.
 *
 * Single source of truth for all tunable thresholds in the worker
 * runtime: frame envelope caps, batching cadence, heartbeat intervals,
 * reconnect / circuit-breaker constants, snapshot caps, and the
 * reauth-queue cap. Co-locating them here lets the (BATCH_FLUSH_FRAMES,
 * POOL_CAP_PER_TEMPLATE) coupled-adjustment policy in plan v6 §4.6
 * be enforced by code review rather than scattered constants.
 *
 * Threading: import-only — values are imported into worker / session
 * code at module-init time. Frozen at first read.
 *
 * Allocation: zero (numeric / bigint literals at module scope).
 *
 * Plan reference: §2.5 / §2.7 / §2.8 / §2.10 / §2.12 / §4.4–§4.7 /
 * §5.2 / §6 row 47.
 */

// ─── Frame envelope (§2.1) ─────────────────────────────────────────
/** Maximum total length of a single SBE frame (header + payload). */
export const MAX_FRAME_BYTES = 4 * 1024 * 1024;

/** Receive ring buffer size — equal to MAX_FRAME_BYTES per §4.4. */
export const RING_BYTES = MAX_FRAME_BYTES;

// ─── Worker → main batching (§4.5) ─────────────────────────────────
/** Flush deadline for the chained-setTimeout batcher (~one render frame). */
export const BATCH_FLUSH_INTERVAL_MS = 16;

/** Size escape — flush early once batch reaches this many DTOs. */
export const BATCH_FLUSH_FRAMES = 64;

// ─── DTO pool (§4.6) ───────────────────────────────────────────────
/**
 * Free-list cap per template type. Coupled to BATCH_FLUSH_FRAMES at
 * 4× headroom so steady-state never exhausts. If BATCH_FLUSH_FRAMES is
 * bench-revised, this constant MUST be re-evaluated atomically (single
 * PR; both live in this file so they cannot drift).
 */
export const POOL_CAP_PER_TEMPLATE = 4 * BATCH_FLUSH_FRAMES;

// ─── Auth (§2.5 / §4.2) ────────────────────────────────────────────
/** Server-side AUTH_TIMEOUT_SECONDS = 5; client mirrors with margin handling via `performance.now()` deadline. */
export const AUTH_TIMEOUT_MS = 5_000;

/** Token-issuer port acquire deadline; over → DOWN_REQUIRES_USER_ACTION. */
export const TOKEN_ACQUIRE_TIMEOUT_MS = 5_000;

// ─── Heartbeats (§2.8) ─────────────────────────────────────────────
/** Default server-heartbeat cadence if AuthAck.serverHeartbeatIntervalMs == 0. */
export const SERVER_HEARTBEAT_INTERVAL_DEFAULT_MS = 5_000;

/** Default client-heartbeat cadence if AuthAck.clientHeartbeatIntervalMs == 0. */
export const CLIENT_HEARTBEAT_INTERVAL_DEFAULT_MS = 10_000;

/** Server-deadline multiplier (3× negotiated interval). */
export const SERVER_HEARTBEAT_DEADLINE_MULTIPLIER = 3;

/** Hidden-tab floor for server-deadline (max(N×interval, 60s)). */
export const HEARTBEAT_HIDDEN_FLOOR_MS = 60_000;

// ─── ClientAck watermarking (§2.9) ─────────────────────────────────
export const ACK_INTERVAL_FRAMES_NOMINAL = 100;
export const ACK_INTERVAL_FRAMES_BACKPRESSURE = 25;
export const ACK_INTERVAL_MS = 250;

// ─── Backpressure hysteresis (§2.9) ────────────────────────────────
export const BACKPRESSURE_ENTER_BUFFERED_BYTES = 1 * 1024 * 1024;
export const BACKPRESSURE_EXIT_BUFFERED_BYTES = 256 * 1024;
/** Minimum dwell since last SlowConsumer event before BACKPRESSURE → CONNECTED. */
export const BACKPRESSURE_EXIT_QUIET_MS = 60_000;
/** Flap-rate cap: at most 1 transition per window. */
export const BACKPRESSURE_FLAP_CAP_MS = 30_000;
/** bufferedAmount poll cadence (per-ack OR this interval, whichever first). */
export const BACKPRESSURE_POLL_MS = 250;

// ─── Reconnect (§5.2) ──────────────────────────────────────────────
export const RECONNECT_BASE_MS = 500;
export const RECONNECT_CAP_MS = 30_000;
/** Backoff cap multiplier on RateLimitExceeded (code 3) first occurrence. */
export const RECONNECT_CAP_MULTIPLIER_RATE_LIMIT = 4;
/** Backoff cap multiplier on ServerShutdown (code 10) and TryAgainLater (close 1013). */
export const RECONNECT_CAP_MULTIPLIER_SHUTDOWN = 8;
/** Sliding-window threshold: N attempts in WINDOW_MS without successful AuthAck → freeze. */
export const RECONNECT_FREEZE_AFTER_ATTEMPTS = 30;
export const RECONNECT_FREEZE_WINDOW_MS = 10 * 60_000;
/** Code-1/8 consecutive-auth-failure freeze threshold. */
export const CONSECUTIVE_AUTH_FAILURES_FREEZE = 3;
/** Code-3 consecutive freeze threshold within window. */
export const RATE_LIMIT_FREEZE_AFTER = 2;
export const RATE_LIMIT_FREEZE_WINDOW_MS = 5 * 60_000;

// ─── Gap detection (§2.7) ──────────────────────────────────────────
/** Out-of-order buffer cap (bytes, not frames). Over → close BufferOverflow. */
export const MAX_GAP_BUFFER_BYTES = 16 * 1024 * 1024;

// ─── Snapshot reassembly (§2.10) ───────────────────────────────────
export const MAX_FRAGMENT_BYTES = 16 * 1024;
export const MAX_SNAPSHOT_BYTES_PER_ID = 8 * 1024 * 1024;
export const MAX_TOTAL_INFLIGHT_SNAPSHOT_BYTES = 64 * 1024 * 1024;
export const MAX_INFLIGHT_SNAPSHOT_IDS = 8;
/** Per-snapshotId completion deadline; defends hostile-server memory pinning. */
export const SNAPSHOT_COMPLETION_DEADLINE_MS = 30_000;
/**
 * Hard cap on `totalFragments` from the wire. Defense-in-depth on top
 * of the byte-cap defense — prevents a malicious server from sending
 * a huge totalFragments value that would later hang the
 * `finaliseAndEmit` concatenation loop. 1_000_000 sits well above any
 * plausible legitimate snapshot (8 MiB / 8 B floor) and well below the
 * worker-thread loop-hang threshold. Per Gemini review R11 (MEDIUM):
 * hoisted from inline constant in SnapshotAssembler.
 */
export const MAX_TOTAL_FRAGMENTS = 1_000_000;

// ─── Re-auth queue (§2.12) ─────────────────────────────────────────
/** Max entitlement-sensitive frames queued during in-flight reauth; over → reauth rejected + PROTOCOL_VIOLATION. */
export const MAX_REAUTH_QUEUED_FRAMES = 64;

// ─── Watchdog (§4.7) ───────────────────────────────────────────────
export const WATCHDOG_PING_INTERVAL_MS = 1_000;
export const WATCHDOG_PONG_DEADLINE_MS = 250;
export const WATCHDOG_MISS_LIMIT = 3;
export const WATCHDOG_HIDDEN_RELAX_MULTIPLIER = 5;
export const WORKER_RESPAWN_LIMIT = 3;
export const WORKER_RESPAWN_WINDOW_MS = 30_000;

// ─── Subprotocol pinning (§2.5 / §A3) ──────────────────────────────
/** Hard-asserted subprotocol echo. Bump only on breaking handshake/envelope changes. */
export const SUBPROTOCOL = "trading-ws.v1";

// ─── Worker → main protocol version (§5.3) ─────────────────────────
export const WORKER_PROTOCOL_VERSION = 1;

// ─── Bench mix (§4.9) ──────────────────────────────────────────────
/**
 * Decode-bench template-mix declared first-cut (40% price / 30% order/fill events /
 * 20% snapshot fragments / 10% control). Provisional working number — to be
 * revised against APP-249 chaos traffic + APP-242 reliable-stream profile when those
 * publish; recorded as a bench artifact and reproducible.
 */
export const BENCH_MIX_PRICE_PCT = 40;
export const BENCH_MIX_ORDER_FILL_PCT = 30;
export const BENCH_MIX_SNAPSHOT_FRAG_PCT = 20;
export const BENCH_MIX_CONTROL_PCT = 10;
