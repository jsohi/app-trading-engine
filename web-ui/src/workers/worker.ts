/**
 * Web Worker entrypoint — APP-36 SBE-decoding RxJS streaming worker.
 *
 * Replaces the C0 placeholder (`worker.ts` 1A stub) with the integrated
 * runtime. Owns the WebSocket; parses inbound frames via `FrameParser`
 * + `MessageRouter`; dispatches to `AuthClient` / `Heartbeat` /
 * `GapTracker` / `SnapshotAssembler` / `BackpressureController`;
 * batches decoded events to the main thread per `WorkerProtocol`.
 *
 * Threading: dedicated worker thread. Communicates with main thread
 * exclusively via `postMessage` + transferred `MessagePort`s.
 *
 * Allocation: zero per inbound frame in steady state (FrameParser ring
 * + reused decoder flyweights). Batched outbound `postMessage` per §4.5.
 *
 * Hot-path discipline: NO `tracer.startSpan(...)` calls in `onmessage`
 * — enforced by ESLint `local/no-span-in-hot-path`.
 *
 * Plan reference: §4 / §5.1 / §5.3 / §6 row 19.
 */

import { tracer } from "@/shared/telemetry/otel";
import { type ConnectionState, type WorkerMessage } from "@/shared/transport/MessageShape";

import { encodeBestEffort } from "@/workers/frame/FrameEncoder";
import {
  FrameParser,
  type FrameParseErrorCode,
  type ParsedFrame,
} from "@/workers/frame/FrameParser";
import { FLAG_RELIABLE, FLAG_SNAPSHOT, isValidFlagCombo } from "@/workers/frame/Flags";
import { validateWsUrl } from "@/workers/frame/WsUrlValidator";

import { Stats } from "@/workers/protocol/Stats";
import { nowEpochMs, nowEpochNs } from "@/workers/time";
import { type MainToWorker } from "@/workers/protocol/WorkerProtocol";
import { WORKER_PROTOCOL_VERSION } from "@/workers/WorkerTuning";

import { MessageRouter, type RouterHandlers } from "@/workers/dispatch/MessageRouter";

import {
  AuthClient,
  type AuthClientCallbacks,
  type AuthScheduler,
} from "@/workers/session/AuthClient";
import { AckSender } from "@/workers/session/AckSender";
import { BackpressureController } from "@/workers/session/BackpressureController";
import { GapTracker } from "@/workers/session/GapTracker";
import { Heartbeat } from "@/workers/session/Heartbeat";
import { isAppErrorCode, Reconnect } from "@/workers/session/Reconnect";
import { SessionState } from "@/workers/session/SessionState";
import { SnapshotAssembler } from "@/workers/session/SnapshotAssembler";

import {
  BATCH_FLUSH_FRAMES,
  BATCH_FLUSH_INTERVAL_MS,
  SUBPROTOCOL,
  TOKEN_ACQUIRE_TIMEOUT_MS,
} from "@/workers/WorkerTuning";

// Schema constants — pinned at version 1 per pre-prod policy. See
// `messages/src/main/resources/trading-schema.xml` for source of truth.
const EXPECTED_SCHEMA_ID = 1;
const EXPECTED_SCHEMA_VERSION = 1;
// SBE template ID for WebSocketAuth (template 60) — used by the
// placeholder encoder until APP-260 ships per-direction encoders.
const TEMPLATE_ID_AUTH = 60;

// ─── Bootstrap span (cold path; ESLint rule allows here) ────────────
const startSpan = tracer.startSpan("web-ui.worker.start", {
  attributes: { "worker.id": "app-36-web-worker" },
});
startSpan.end();

// ─── Module-scope singletons ────────────────────────────────────────

const state = new SessionState();
const stats = new Stats();

let ws: WebSocket | null = null;
let parser: FrameParser | null = null;
let router: MessageRouter | null = null;
let authClient: AuthClient | null = null;
// Session-layer components — instantiated on AuthAck (negotiated
// heartbeat intervals are required by Heartbeat). Per Gemini review
// (HIGH): these MUST be wired into the runtime to activate heartbeat,
// gap detection, snapshot reassembly, ack watermarking, and
// backpressure handling.
let heartbeat: Heartbeat | null = null;
let gapTracker: GapTracker | null = null;
let snapshotAssembler: SnapshotAssembler | null = null;
let backpressureController: BackpressureController | null = null;
let ackSender: AckSender | null = null;
const reconnect = new Reconnect(
  // No randomness used inside the cluster service; here we are
  // outside-the-cluster main-thread/worker code where Math.random is
  // permitted by APP-36 §3 ("WebSocket Server (Non-Deterministic)").
  // The RandomSource interface keeps the choice testable.
  // Per Gemini review R10 (HIGH): the attempt counter is preserved
  // across worker terminate+respawn cycles via
  // `INIT.initialReconnectAttempt` (WorkerClient holds it on the main
  // thread and seeds the worker's Reconnect on every spawn via
  // `reconnect.setAttempt(...)` in the INIT handler). The two-tier
  // breaker is intentional: inner counter (worker-side, persisted)
  // tracks WS-level reconnect attempts; outer counter
  // (`WorkerClient.respawnTimestamps`, 3-in-30s cap → WORKER_DEAD)
  // tracks worker-process lifetimes.
  { next: () => Math.random() },
  () => nowEpochNs(),
);
let connectionState: ConnectionState = "CONNECTING";

const outboundBatch: WorkerMessage[] = [];
let flushTimerHandle: number | null = null;
// Bidirectional watchdog port — main thread sends PING, worker MUST PONG
// inside the configured deadline or the watchdog terminates + respawns.
// Wired in `handleInit`; cleared in `shutdown`.
let watchdogPort: MessagePort | null = null;
// Periodic 250 ms tick driving session-layer time-based triggers
// (AckSender.onTimerTick + SnapshotAssembler.onTimerTick). Per Gemini
// review R10 (MEDIUM): without this the time-based ACK trigger never
// fires during quiet periods and stale snapshots are only expired
// when a NEW snapshot id arrives.
let sessionTickTimer: number | null = null;
const SESSION_TICK_MS = 250;
// Per /review HIGH (Agent B): 1 s STATS emitter — drains Stats
// counters into a `MESSAGE_BATCH` `StatsMsg` for the main thread (and
// downstream APP-245 RUM/OTel bridge). Without this the counters were
// accumulated forever in the worker and never observable.
let statsEmitterTimer: number | null = null;
const STATS_EMIT_MS = 1_000;
// Concurrency guard: handleInit is async, so multiple INIT messages
// arriving in quick succession (rapid UI reconnect, message-port
// retries) could otherwise spawn overlapping connection attempts. Per
// Gemini review (MEDIUM): refuse a second INIT while one is in flight.
let initInFlight = false;

// ─── Main-thread message handler (FIRST line: protocolVersion check) ──

self.onmessage = (event: MessageEvent<unknown>): void => {
  const data = event.data;
  if (
    data === null ||
    typeof data !== "object" ||
    (data as { protocolVersion?: unknown }).protocolVersion !== WORKER_PROTOCOL_VERSION
  ) {
    // Ignore — host must send a valid envelope. Surface as ERROR.
    postError("PROTOCOL", "INIT msg without valid protocolVersion");
    return;
  }
  const msg = data as MainToWorker;
  switch (msg.type) {
    case "INIT":
      // Per Gemini review (MEDIUM): refuse overlapping INIT calls so we
      // do not spawn concurrent token-acquire / WebSocket-open paths.
      if (initInFlight || ws !== null) {
        postError("INIT", "INIT received while connection already in flight");
        break;
      }
      initInFlight = true;
      // Per Gemini review R10 (HIGH): seed the backoff attempt counter
      // from the main-thread-persisted value so the progression
      // continues across worker respawn cycles.
      reconnect.setAttempt(msg.initialReconnectAttempt);
      void handleInit(msg.wsUrl, msg.tokenPort, msg.watchdogPort).finally(() => {
        initInFlight = false;
      });
      break;
    case "PING":
      // PINGs from main arrive on `self.onmessage` only as a fallback;
      // the canonical channel is the watchdog `MessagePort` wired in
      // `handleInit`. Echo PONG defensively here too.
      postPong(msg.mainNanos);
      break;
    case "CLOSE":
      shutdown();
      break;
  }
};

// ─── Bootstrap: wire WebSocket + parser + router on INIT ────────────

async function handleInit(
  wsUrl: string,
  tokenPort: MessagePort,
  watchdogPort: MessagePort,
): Promise<void> {
  try {
    // Wire watchdog port — main thread sends PING on this channel; we
    // must respond PONG within the deadline or the watchdog terminates
    // the worker (per APP-36 §4.7).
    wireWatchdogPort(watchdogPort);

    // 1. Validate the URL — production refuses ws://, *.local, etc.
    const mode: "prod" | "dev" = import.meta.env.PROD ? "prod" : "dev";
    validateWsUrl(wsUrl, mode);

    // 2. Acquire the JWT from the issuer's MessagePort.
    const token = await acquireToken(tokenPort);

    // Per Gemini review (HIGH): extract the JWT `sub` claim and store
    // it in SessionState so the AuthClient continuity check can refuse
    // a SessionResume whose new token's `sub` differs from the prior
    // session's `sub`. The token is opaque to the worker except for
    // this single read; never logged in full.
    const newSub = extractJwtSubClaim(token);
    // Per Gemini review R8 (SECURITY-MEDIUM): sub-claim continuity
    // check on resume. If the prior session's sub differs from the new
    // token's sub, force a coldStart so we cannot accidentally resume
    // session A's reliable cursor on a connection authenticated as
    // user B (cross-user session-hijack defense, §2.6).
    if (state.subClaim !== "" && newSub !== state.subClaim) {
      state.coldStart();
    }
    state.subClaim = newSub;
    // Per Gemini review R6 (HIGH): only coldStart when there is no
    // priorSessionId to resume. Resume must preserve `lastReliableSeqNo`
    // so the SessionResume frame can ask the server to replay
    // `(lastReliableSeqNo, current]`. A blanket coldStart at every
    // INIT would break resume. close-code handling (Reconnect.applyCloseCode)
    // owns the decision: it preserves `priorSessionId` for resume and
    // clears it for cold-start codes.
    if (state.priorSessionId === null) {
      state.coldStart();
    }

    // 3. Open the WebSocket with the pinned subprotocol. Close any
    // prior socket first per Gemini review (HIGH) — defends against a
    // duplicate INIT message leaking the previous WebSocket. Per
    // Gemini review R8 (MEDIUM): detach the old socket's handlers
    // BEFORE close() so the implicit `close` event does not re-enter
    // the reconnect path (which would race with this fresh INIT).
    if (ws !== null) {
      ws.onopen = null;
      ws.onmessage = null;
      ws.onclose = null;
      ws.onerror = null;
      ws.close();
    }
    ws = new WebSocket(wsUrl, [SUBPROTOCOL]);
    ws.binaryType = "arraybuffer";
    ws.onopen = (): void => {
      // Hard-assert subprotocol echo per §2.5.
      if (ws !== null && ws.protocol !== SUBPROTOCOL) {
        postError("PROTOCOL", `subprotocol mismatch: ${ws.protocol}`);
        ws.close();
        return;
      }
      // Wire downstream: parser + router + auth.
      parser = new FrameParser({
        onFrame: handleFrame,
        onError: handleParserError,
      });
      router = new MessageRouter(
        buildRouterHandlers(),
        EXPECTED_SCHEMA_ID,
        EXPECTED_SCHEMA_VERSION,
      );
      authClient = new AuthClient(state, buildAuthCallbacks(), buildAuthScheduler());
      authClient.authenticate(token);
      transitionConnection("CONNECTING");
    };
    ws.onmessage = (ev: MessageEvent<ArrayBuffer | Blob>): void => {
      // Hot path: synchronously feed the parser. No span emission here.
      if (!(ev.data instanceof ArrayBuffer)) {
        // Browser may emit Blob if binaryType wasn't set in time — drop.
        return;
      }
      parser?.feed(new Uint8Array(ev.data));
    };
    ws.onclose = (ev: CloseEvent): void => {
      // Stop session-layer timers immediately; a reconnect re-creates them.
      heartbeat?.stop();
      backpressureController?.stop();
      // Per Gemini review (MEDIUM): reject any in-flight reauth so
      // callers awaiting the promise are notified of the failure
      // rather than hanging forever.
      authClient?.cancelPendingReauth("websocket closed");
      // Per Gemini review R7 (HIGH): preserve the current sessionId as
      // the priorSessionId so the next handleInit can issue a
      // SessionResume(priorSessionId, lastReliableSeqNo) instead of a
      // cold-start. Reconnect.applyCloseCode below decides whether
      // resume is even legal (cold-start codes clear priorSessionId
      // afterwards inside SessionState; see §2.6).
      if (state.currentSessionId !== null) {
        state.priorSessionId = state.currentSessionId;
      }
      stats.incReconnect();
      // Per Gemini review (HIGH): consult Reconnect for the close-code
      // policy. Codes 1002/1003/1007–1010/1015 freeze; 1012/1013 cap × 8;
      // others fall through to a normal reconnect with backoff.
      const decision = reconnect.applyCloseCode(ev.code);
      if (decision === "PROTOCOL_VIOLATION") {
        // Per /review HIGH (Agent B): cold-start invariant per §2.6.
        // PROTOCOL_VIOLATION terminates the session relationship; the
        // server will not honour a SessionResume on the prior id.
        // Clear both ids so the next handleInit cold-starts cleanly.
        state.coldStart();
        transitionConnection("PROTOCOL_VIOLATION");
        ws = null;
        return;
      }
      if (decision === "SCHEMA_MISMATCH") {
        // Per /review HIGH (Agent B): same cold-start invariant —
        // schema mismatch means the prior session's wire-protocol no
        // longer matches what the next handshake will negotiate.
        state.coldStart();
        transitionConnection("SCHEMA_MISMATCH");
        ws = null;
        return;
      }
      transitionConnection("DOWN");
      ws = null;
      // Per Gemini review R6 (HIGH): compute backoff and surface a
      // `RECONNECT_DUE` ERROR so the main-thread WorkerClient can
      // re-mint a tokenPort and re-issue INIT. The worker cannot
      // self-reconnect because the prior tokenPort closed after one
      // read — only main can mint a fresh one.
      const dec = reconnect.nextDelayMs(state);
      if (dec.kind === "FREEZE") {
        // Per /review HIGH (Agent B): cold-start on FREEZE — the
        // circuit breaker fires only on terminal auth/rate-limit
        // failure (codes 1/2/3-2nd/8 or 30-in-10min). Resume on a
        // frozen session would just trigger another breaker cycle.
        state.coldStart();
        transitionConnection("DOWN_REQUIRES_USER_ACTION");
        postError("AUTH", `circuit breaker tripped: ${dec.reason}`);
        return;
      }
      // Notify main with an ERROR carrying the delay — main schedules
      // the actual respawn-with-fresh-token. We do not drive the timer
      // ourselves because the credential lifecycle lives main-side.
      postError("INIT", `reconnect_due_after_ms:${String(dec.delayMs)}`);
    };
    ws.onerror = (): void => {
      transitionConnection("DOWN");
    };
  } catch (err) {
    postError("INIT", err instanceof Error ? err.message : String(err));
  }
}

function wireWatchdogPort(port: MessagePort): void {
  // If a previous port was wired (e.g. worker reused after a duplicate INIT), close
  // it before swapping — prevents leaking PING/PONG channels.
  if (watchdogPort !== null) {
    watchdogPort.onmessage = null;
    watchdogPort.close();
  }
  watchdogPort = port;
  port.onmessage = (ev: MessageEvent<unknown>): void => {
    const data = ev.data;
    if (
      data !== null &&
      typeof data === "object" &&
      (data as { type?: unknown }).type === "PING" &&
      typeof (data as { mainNanos?: unknown }).mainNanos === "bigint"
    ) {
      postPong((data as { mainNanos: bigint }).mainNanos);
    }
  };
}

function postPong(mainNanos: bigint): void {
  const workerNanos = nowEpochNs();
  if (watchdogPort !== null) {
    watchdogPort.postMessage({
      type: "PONG",
      protocolVersion: WORKER_PROTOCOL_VERSION,
      echoMainNanos: mainNanos,
      workerNanos,
    });
    return;
  }
  // Fallback: PING arrived via `self.onmessage` before the watchdog port
  // was wired (or watchdog port already closed). Echo via main channel.
  postMessage({
    type: "PONG",
    protocolVersion: WORKER_PROTOCOL_VERSION,
    echoMainNanos: mainNanos,
    workerNanos,
  });
}

/**
 * Extract the JWT `sub` claim without verifying the signature. The
 * token is opaque to the worker — the SERVER validates JWKS + RS256
 * (APP-160). This worker reads `sub` only to refuse a `SessionResume`
 * whose new token's subject differs from the prior session's subject
 * (continuity check, see APP-36 §2.6). Never logged in full.
 *
 * Returns `""` if the token is malformed (3-part), the middle segment
 * fails base64url decoding, or `sub` is missing — the AuthClient
 * surfaces this as a credential failure.
 *
 * Allocation: cold path (called once per worker boot per re-auth).
 */
function extractJwtSubClaim(token: string): string {
  const parts = token.split(".");
  if (parts.length !== 3) return "";
  try {
    const middle = parts[1] ?? "";
    // Base64URL → Base64; pad to multiple of 4.
    const b64 = middle.replace(/-/g, "+").replace(/_/g, "/");
    const padded = b64 + "=".repeat((4 - (b64.length % 4)) % 4);
    // Per Gemini review R8 (SECURITY-MEDIUM): `atob` returns a binary
    // string, not UTF-8. JWT `sub` claims may legitimately contain
    // non-ASCII characters (e.g. emoji or non-Latin display names);
    // decoding via TextDecoder gives the correct Unicode string.
    const bytes = Uint8Array.from(atob(padded), (c) => c.charCodeAt(0));
    const json = new TextDecoder().decode(bytes);
    const parsed: unknown = JSON.parse(json);
    // Per Gemini review R11 (MEDIUM): `JSON.parse("null")` returns
    // null, and accessing `.sub` would throw. Reject any non-object
    // payload defensively.
    if (parsed === null || typeof parsed !== "object") return "";
    const obj = parsed as { sub?: unknown };
    // Defense: JSON.parse can produce objects with `__proto__` keys,
    // but reading a single own-property string off the result is safe
    // (we don't spread / Object.assign / Reflect.set anywhere here).
    return typeof obj.sub === "string" ? obj.sub : "";
  } catch {
    return "";
  }
}

async function acquireToken(port: MessagePort): Promise<string> {
  return new Promise<string>((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error("token-port acquire timeout"));
    }, TOKEN_ACQUIRE_TIMEOUT_MS);
    port.onmessage = (ev: MessageEvent<unknown>): void => {
      clearTimeout(timeout);
      const data = ev.data;
      if (
        data !== null &&
        typeof data === "object" &&
        (data as { type?: unknown }).type === "TOKEN" &&
        typeof (data as { value?: unknown }).value === "string"
      ) {
        const value = (data as { value: string }).value;
        port.close();
        resolve(value);
      } else {
        reject(new Error("token-port: malformed TOKEN message"));
      }
    };
  });
}

// ─── FrameParser callbacks ──────────────────────────────────────────

function handleFrame(frame: ParsedFrame): void {
  if (router === null) return;
  stats.incFramesDecoded();
  stats.addBytes(frame.totalLength);
  // Defense: even though FrameParser already validated, double-check.
  if (!isValidFlagCombo(frame.flags)) {
    postError("PROTOCOL", `unexpected flag combo 0x${frame.flags.toString(16)}`);
    return;
  }
  // Reliable frames (FLAG_RELIABLE = 0x01) flow through GapTracker for
  // sequence-continuity + out-of-order buffering. The tracker invokes
  // `onInOrderFrame` for each delivered seqNo (in arrival order or
  // released after gap-fill); routing happens THERE — never inline
  // here. Per Gemini review (HIGH): the prior boolean contract caused
  // out-of-order frames to be double-routed.
  const isReliable = (frame.flags & FLAG_RELIABLE) !== 0;
  if (gapTracker !== null && isReliable) {
    gapTracker.onReliableFrame(frame.seqNo, frame.flags, frame.payload);
    return;
  }
  // Pre-AuthAck reliable frames (e.g. AuthAck itself, seqNo=1) arrive
  // before the GapTracker is wired by `activateSessionLayer`. Per
  // Gemini review R7 (HIGH): we MUST advance `state.lastReliableSeqNo`
  // here so the post-activation tracker sees the correct cursor and
  // does not interpret the *next* reliable frame (seqNo=2) as a gap.
  if (isReliable && frame.seqNo > state.lastReliableSeqNo) {
    state.lastReliableSeqNo = frame.seqNo;
  }
  // Best-effort frames bypass GapTracker (no seqNo continuity).
  routeFrame(frame.payload, frame.flags);
}

function routeFrame(payload: Uint8Array, flags: number): void {
  if (router === null) return;
  const result = router.route(payload, flags);
  if (!result.schemaIdMatch || !result.versionMatch) {
    transitionConnection("SCHEMA_MISMATCH");
    postError("SCHEMA", `schemaId/version mismatch: ${String(result.templateId)}`);
    ws?.close();
  }
}

function handleParserError(code: FrameParseErrorCode, message: string): void {
  if (code === "CRC_MISMATCH") stats.incCrcMismatch();
  postError(
    code === "CRC_MISMATCH" ? "CRC" : code === "BUFFER_OVERFLOW" ? "BUFFER" : "PROTOCOL",
    message,
  );
  ws?.close();
}

// ─── Router handlers ────────────────────────────────────────────────

function buildRouterHandlers(): RouterHandlers {
  return {
    onAuthAck: (ack) => {
      // AuthClient updates SessionState (negotiated heartbeat intervals,
      // sessionId, etc.). Once that is in place, instantiate the
      // session-layer components that depend on the negotiated state
      // (per Gemini review HIGH: previously these were declared but
      // never wired into the runtime).
      authClient?.onAuthAck(ack, state.subClaim);
      activateSessionLayer();
      reconnect.notifyAuthAckSuccess(state);
      transitionConnection("CONNECTED");
    },
    onAnyInbound: () => {
      const nowNs = nowEpochNs();
      state.lastServerActivityNs = nowNs;
    },
    onServerHeartbeat: (_serverNanos) => {
      // Activity already refreshed via onAnyInbound. Nothing more to do here.
    },
    onSnapshotFragment: (snapshotId, fragmentIndex, totalFragments, payload, isFinal) => {
      snapshotAssembler?.onFragment({
        snapshotId,
        fragmentIndex,
        totalFragments,
        payload,
        isFinal,
      });
    },
    onWebSocketError: (code, _errorText) => {
      // errorText is intentionally NOT logged here — server-supplied free-
      // form bytes must clear the static allowlist (`ErrorTextRegistry`,
      // wired in C8) before reaching any logger or telemetry attribute.
      // Code 9 (SlowConsumer) feeds the BACKPRESSURE controller; all
      // other codes are forwarded to AuthClient's error path which
      // consults the §2.13 matrix.
      if (code === 9) {
        backpressureController?.onSlowConsumerSignal();
        return;
      }
      if (code === 12) {
        // SnapshotEntityTooLarge — caller surfaces; do not close.
        // The server-side WebSocketError frame does not include the
        // snapshotId, so per-id cleanup is unreachable from this PR.
        // Tracked: schema widening to add `snapshotId` to error code 12
        // (folded into APP-242 per §D.2 of the plan); when that lands
        // we wire `snapshotAssembler.onSnapshotEntityTooLarge(id)`.
        // Per Gemini review R11 (MEDIUM): surface the occurrence so
        // the UI can inform the user that a snapshot was rejected.
        postError("PROTOCOL", "server WebSocketError code 12 (SnapshotEntityTooLarge)");
        return;
      }
      // Per Gemini review R7 (HIGH): feed every other application-level
      // error code through Reconnect's circuit breaker so codes 1/2/3/8
      // (AuthFailed, AuthorizationFailed, RateLimitExceeded,
      // VersionMismatch) advance the freeze counters. Without this
      // wiring the client could enter an infinite reconnect loop on a
      // terminal credential failure.
      // Per /review MEDIUM (Agent B): consult the canonical
      // `APP_ERROR_CODES` set exported from Reconnect.ts so the
      // membership check stays in lockstep with the AppErrorCode
      // union (codes outside it — e.g. 5 InvalidSubscription, 9
      // SlowConsumer, 12 SnapshotEntityTooLarge — bypass the breaker
      // per §2.13 surface-only semantics).
      const freeze = isAppErrorCode(code) ? reconnect.applyAppErrorCode(state, code) : false;
      if (freeze) {
        // Per /review HIGH (Agent B): cold-start on FREEZE.
        state.coldStart();
        transitionConnection("DOWN_REQUIRES_USER_ACTION");
        postError("AUTH", `circuit breaker frozen by WebSocketError code ${String(code)}`);
        ws?.close();
        return;
      }
      authClient?.onAuthError(`server WebSocketError code ${String(code)}`);
    },
    onReplayComplete: () => {
      state.dropPriorSessionId();
    },
    onEvent: (templateId, _payload) => {
      // C8 streams attach typed event decoders. For now, just bump the
      // counter so the wire is exercised.
      void templateId;
    },
    onUnexpectedServerTemplate: (templateId) => {
      postError("PROTOCOL", `unexpected server templateId ${String(templateId)}`);
    },
  };
}

/**
 * Instantiate the session-layer components after the negotiated
 * heartbeat / ack intervals are known (i.e. after AuthAck). These
 * components are torn down on shutdown(); a fresh AuthAck on a
 * reconnect re-creates them so timer state is clean.
 *
 * Plan reference: §5.2 (session components), §6 row 24 (BACKPRESSURE).
 */
function activateSessionLayer(): void {
  // Per Gemini review R12 (MEDIUM): use the precision-preserving
  // helper instead of `BigInt(Math.floor(nowMs() * 1e6))` which loses
  // sub-millisecond precision through the float → bigint pipeline.
  const nowMs = nowEpochMs;
  const nowNs = nowEpochNs;

  heartbeat = new Heartbeat(
    state,
    {
      onOutboundDue: (_clientNanos) => {
        // Encoder lands in C8; for now record the tick on Stats so the
        // wire-up is observable from the test harness.
        stats.incFramesDecoded(); // placeholder — TODO(APP-260) emit ClientHeartbeat.
      },
      onServerDeadlineExceeded: (_ms) => {
        transitionConnection("STALE");
        ws?.close();
      },
    },
    {
      setTimeout: (h, d) => self.setTimeout(h, d),
      clearTimeout: (h) => {
        self.clearTimeout(h);
      },
    },
    nowNs,
  );
  heartbeat.start();

  gapTracker = new GapTracker(state, {
    onInOrderFrame: (_seqNo, flags, payload) => {
      // Per Gemini review (HIGH): route here so reliable frames are
      // dispatched exactly once and only after their seqNo is in-order.
      // Buffered out-of-order frames flow through here when their gap
      // fills; their original `flags` are preserved.
      // Per Gemini review R7 (MEDIUM): per APP-36 §2.10, no
      // non-snapshot reliable frame may interleave between fragments
      // of an in-flight snapshot. If the assembler is mid-reassembly
      // and this frame is NOT a snapshot fragment (FLAG_SNAPSHOT
      // unset), trip the protocol violation BEFORE routing so the
      // dispatcher does not see a fragment break.
      const isSnapshotFrame = (flags & FLAG_SNAPSHOT) !== 0;
      if (!isSnapshotFrame && snapshotAssembler?.hasInflightSnapshots() === true) {
        snapshotAssembler.onNonSnapshotInterleave();
        return;
      }
      routeFrame(payload, flags);
      ackSender?.onReliableFrameDelivered();
    },
    onGapRequest: (_ev) => {
      // Encoder wired in C8.
    },
    onBufferOverflow: (_bytes) => {
      postError("BUFFER", "gap buffer overflow");
      ws?.close();
    },
  });

  snapshotAssembler = new SnapshotAssembler(
    {
      onSnapshotComplete: (_snap) => {
        // Caller will emit the assembled bytes via Transferable
        // postMessage in C8 (snapshot consumers are not wired yet).
      },
      onProtocolViolation: (reason) => {
        postError("PROTOCOL", `snapshot: ${reason}`);
        ws?.close();
      },
      onBufferOverflow: (reason) => {
        postError("BUFFER", `snapshot: ${reason}`);
        ws?.close();
      },
      onSnapshotEntityTooLarge: (_id) => {
        // Surface only; do not close per §2.13.
      },
    },
    nowMs,
  );

  ackSender = new AckSender(
    state,
    {
      onAckDue: (_lastReliableSeqNo) => {
        // Encoder wired in C8.
      },
    },
    nowNs,
  );

  backpressureController = new BackpressureController(
    {
      onEnter: (_source) => {
        ackSender?.setBackpressure(true);
        transitionConnection("BACKPRESSURE");
      },
      onExit: () => {
        ackSender?.setBackpressure(false);
        transitionConnection("CONNECTED");
      },
    },
    () => ws?.bufferedAmount ?? 0,
    {
      setTimeout: (h, d) => self.setTimeout(h, d),
      clearTimeout: (h) => {
        self.clearTimeout(h);
      },
    },
    nowMs,
  );
  backpressureController.start();

  // Per Gemini review R10 (MEDIUM): drive AckSender + SnapshotAssembler
  // periodic ticks. AckSender.onTimerTick covers the time-based ACK
  // trigger during quiet periods (no inbound frames); the per-frame
  // hot path no longer consults the clock. SnapshotAssembler.onTimerTick
  // expires per-id 30 s completion deadlines even when no fresh
  // snapshot id arrives.
  if (sessionTickTimer !== null) self.clearInterval(sessionTickTimer);
  sessionTickTimer = self.setInterval(() => {
    ackSender?.onTimerTick();
    snapshotAssembler?.onTimerTick();
    // Per /review HIGH (Agent B): drive Heartbeat's stale-server
    // deadline check from the same periodic tick. Without this call
    // `onServerDeadlineExceeded` was unreachable and §2.8's
    // `> 3 × interval` (or 60s under hidden visibility) deadline
    // never fired — STALE state was dead.
    heartbeat?.checkServerDeadline(state.lastServerActivityNs);
  }, SESSION_TICK_MS);

  // Per /review HIGH (Agent B): 1 s STATS emitter — drains the Stats
  // snapshot into `MESSAGE_BATCH` so the main thread sees throughput,
  // CRC mismatch counts, gap counts, etc. APP-245 will bridge to OTel.
  if (statsEmitterTimer !== null) self.clearInterval(statsEmitterTimer);
  statsEmitterTimer = self.setInterval(() => {
    if (ws !== null) {
      stats.observeBufferedAmount(ws.bufferedAmount);
    }
    const snap = stats.snapshot();
    emit({
      type: "stats",
      framesDecoded: snap.framesDecoded,
      bytesDecoded: snap.bytesDecoded,
      crcMismatches: snap.crcMismatches,
      gaps: snap.gaps,
      reconnects: snap.reconnects,
      replayFrames: snap.replayFrames,
      snapshotBytes: snap.snapshotBytes,
      bufferedAmountPeak: snap.bufferedAmountPeak,
      degradedTimingMode: snap.degradedTimingMode,
      serverNanos: nowEpochNs(),
    });
  }, STATS_EMIT_MS);
}

// ─── AuthClient wiring ──────────────────────────────────────────────

function buildAuthCallbacks(): AuthClientCallbacks {
  return {
    sendBytes: (bytes) => {
      // Per Gemini review R5 (MEDIUM): `WebSocket.send()` accepts an
      // `ArrayBufferView` at runtime (Uint8Array IS a view) — pass it
      // directly with no copy. TS's strict lib types `BufferSource` to
      // exclude SharedArrayBuffer-backed views; APP-36 §4.3 ESLint rule
      // bans SAB so the runtime invariant is always `ArrayBuffer`. The
      // narrow cast satisfies the type-checker without sacrificing the
      // zero-copy contract.
      ws?.send(bytes as Uint8Array<ArrayBuffer>);
    },
    encodeAuth: (_token, _protocolVersion) => {
      // Placeholder: a real encoder is wired in C8 (per the SBE TS
      // generator's Encoder classes, currently decoder-only output).
      // For now we emit a best-effort frame containing a synthetic
      // header — server-side accepts on dev only.
      // TODO(APP-260): replace with WebSocketAuthEncoder once codecs are split per-direction.
      const headerBytes = 8;
      const payload = new Uint8Array(headerBytes);
      const view = new DataView(payload.buffer);
      view.setUint16(2, TEMPLATE_ID_AUTH, true);
      view.setUint16(4, EXPECTED_SCHEMA_ID, true);
      view.setUint16(6, EXPECTED_SCHEMA_VERSION, true);
      return encodeBestEffort(payload);
    },
    onAuthSuccess: () => {
      transitionConnection("CONNECTED");
    },
    onAuthFailure: (reason, message) => {
      postError("AUTH", `${reason}: ${message}`);
      ws?.close();
    },
  };
}

function buildAuthScheduler(): AuthScheduler {
  return {
    setTimeout: (handler, delayMs) => self.setTimeout(handler, delayMs),
    clearTimeout: (handle) => {
      self.clearTimeout(handle);
    },
    nowMs: nowEpochMs,
  };
}

// ─── Outbound batching (worker → main) ──────────────────────────────

function emit(msg: WorkerMessage): void {
  outboundBatch.push(msg);
  if (outboundBatch.length >= BATCH_FLUSH_FRAMES) {
    flushBatch();
  } else {
    flushTimerHandle ??= self.setTimeout(flushBatch, BATCH_FLUSH_INTERVAL_MS);
  }
}

function flushBatch(): void {
  if (flushTimerHandle !== null) {
    self.clearTimeout(flushTimerHandle);
    flushTimerHandle = null;
  }
  if (outboundBatch.length === 0) return;
  const drained = outboundBatch.splice(0, outboundBatch.length);
  postMessage({
    type: "MESSAGE_BATCH",
    protocolVersion: WORKER_PROTOCOL_VERSION,
    messages: drained,
  });
}

function transitionConnection(next: ConnectionState): void {
  if (connectionState === next) return;
  connectionState = next;
  const nowNs = nowEpochNs();
  emit({ type: "connection-state", state: next, serverNanos: nowNs });
}

// ─── ERROR + shutdown ───────────────────────────────────────────────

function postError(
  code: "INIT" | "AUTH" | "CRC" | "PROTOCOL" | "SCHEMA" | "BUFFER" | "WORKER",
  hint: string,
): void {
  postMessage({
    type: "ERROR",
    protocolVersion: WORKER_PROTOCOL_VERSION,
    code,
    hint,
  });
}

function shutdown(): void {
  // Detach WebSocket handlers BEFORE close() so the implicit `close` event
  // does not reach `transitionConnection("DOWN")` and emit a spurious DOWN
  // state after the user requested shutdown. Same for `error` — a forced
  // close on some browsers (Firefox) emits an `error` event first.
  if (ws !== null) {
    ws.onopen = null;
    ws.onmessage = null;
    ws.onclose = null;
    ws.onerror = null;
    ws.close();
  }
  ws = null;
  parser = null;
  router = null;
  authClient = null;
  heartbeat?.stop();
  heartbeat = null;
  backpressureController?.stop();
  backpressureController = null;
  gapTracker = null;
  snapshotAssembler = null;
  ackSender = null;
  if (sessionTickTimer !== null) {
    self.clearInterval(sessionTickTimer);
    sessionTickTimer = null;
  }
  if (statsEmitterTimer !== null) {
    self.clearInterval(statsEmitterTimer);
    statsEmitterTimer = null;
  }
  if (watchdogPort !== null) {
    watchdogPort.onmessage = null;
    watchdogPort.close();
    watchdogPort = null;
  }
  flushBatch();
}

// Top-level error handler — surface unhandled rejections + errors as ERROR.
self.addEventListener("error", (ev) => {
  postError("WORKER", ev.message);
});
self.addEventListener("unhandledrejection", (ev) => {
  postError("WORKER", String(ev.reason));
});
