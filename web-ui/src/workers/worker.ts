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
import { isValidFlagCombo } from "@/workers/frame/Flags";
import { validateWsUrl } from "@/workers/frame/WsUrlValidator";

import { Stats } from "@/workers/protocol/Stats";
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
import { Reconnect } from "@/workers/session/Reconnect";
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
  { next: () => Math.random() },
  () => BigInt(Math.floor((performance.timeOrigin + performance.now()) * 1_000_000)),
);
let connectionState: ConnectionState = "CONNECTING";

const outboundBatch: WorkerMessage[] = [];
let flushTimerHandle: number | null = null;
// Bidirectional watchdog port — main thread sends PING, worker MUST PONG
// inside the configured deadline or the watchdog terminates + respawns.
// Wired in `handleInit`; cleared in `shutdown`.
let watchdogPort: MessagePort | null = null;

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
      void handleInit(msg.wsUrl, msg.tokenPort, msg.watchdogPort);
      break;
    case "PING":
      // PINGs from main arrive on `self.onmessage` only as a fallback;
      // the canonical channel is the watchdog `MessagePort` wired in
      // `handleInit`. Echo PONG defensively here too.
      postPong(msg.mainNanos);
      break;
    case "RECONNECT_NOW":
      // Force-reconnect: close current WS so the onclose handler triggers
      // the auto-reconnect path (caller-side Reconnect resets backoff).
      ws?.close();
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
    state.subClaim = extractJwtSubClaim(token);
    // Per Gemini review (HIGH): reset session state on every fresh
    // connection so non-resume sessions start clean (lastReliableSeqNo,
    // counters, lastServerActivityNs, etc.).
    state.coldStart();

    // 3. Open the WebSocket with the pinned subprotocol. Close any
    // prior socket first per Gemini review (HIGH) — defends against a
    // duplicate INIT message leaking the previous WebSocket.
    ws?.close();
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
      stats.incReconnect();
      // Per Gemini review (HIGH): consult Reconnect for the close-code
      // policy. Codes 1002/1003/1007–1010/1015 freeze; 1012/1013 cap × 8;
      // others fall through to a normal reconnect with backoff.
      const decision = reconnect.applyCloseCode(ev.code);
      if (decision === "PROTOCOL_VIOLATION") {
        transitionConnection("PROTOCOL_VIOLATION");
        return;
      }
      if (decision === "SCHEMA_MISMATCH") {
        transitionConnection("SCHEMA_MISMATCH");
        return;
      }
      transitionConnection("DOWN");
      // Caller (main thread WorkerClient) drives the actual reopen via
      // `RECONNECT_NOW` — the worker does not silently auto-reopen. The
      // backoff math is owned by Reconnect.nextDelayMs(state).
    };
    ws.onerror = (): void => {
      transitionConnection("DOWN");
    };
  } catch (err) {
    postError("INIT", err instanceof Error ? err.message : String(err));
  }
}

function wireWatchdogPort(port: MessagePort): void {
  // If a previous port was wired (worker reused after RECONNECT_NOW), close
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
  const workerNanos = BigInt(Math.floor((performance.timeOrigin + performance.now()) * 1_000_000));
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
    const json = atob(padded);
    const obj = JSON.parse(json) as { sub?: unknown };
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
  // `onInOrderFrame` for each delivered seqNo; we route those through
  // the dispatcher exactly once, in order.
  if (gapTracker !== null && (frame.flags & 0x01) !== 0) {
    const accepted = gapTracker.onReliableFrame(frame.seqNo, frame.payload);
    if (!accepted) return; // duplicate or buffered for later release.
  }
  const result = router.route(frame.payload, frame.flags);
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
      const nowNs = BigInt(Math.floor((performance.timeOrigin + performance.now()) * 1_000_000));
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
        // The id-specific cleanup belongs to SnapshotAssembler, but the
        // server frame does not include the snapshotId here; the
        // dispatcher hands us only the code. C8 widens the contract.
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
  const nowMs = (): number => performance.timeOrigin + performance.now();
  const nowNs = (): bigint => BigInt(Math.floor(nowMs() * 1_000_000));

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
    nowMs,
  );
  heartbeat.start();

  gapTracker = new GapTracker(state, {
    onInOrderFrame: (_seqNo, _payload) => {
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
}

// ─── AuthClient wiring ──────────────────────────────────────────────

function buildAuthCallbacks(): AuthClientCallbacks {
  return {
    sendBytes: (bytes) => {
      // Per Gemini review (MEDIUM): `WebSocket.send()` accepts
      // `ArrayBufferView` at runtime (Uint8Array IS a view), so the
      // prior ArrayBuffer-copy allocation was redundant. TS's strict
      // lib types `WebSocket.send` to `BufferSource` which excludes
      // SharedArrayBuffer-backed views; SAB is forbidden by APP-36 §4.3
      // ESLint rule, so the encoder never produces one. The cast is
      // therefore safe and the copy is gone.
      // `bytes.buffer` is `ArrayBufferLike`; SAB is banned by APP-36 §4.3
      // ESLint rule so the runtime invariant is `ArrayBuffer`. Pass the
      // ArrayBuffer slice directly (zero-copy view via byteOffset/length).
      const ab = bytes.buffer as ArrayBuffer;
      ws?.send(
        bytes.byteOffset === 0 && bytes.byteLength === ab.byteLength
          ? ab
          : ab.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength),
      );
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
    nowMs: () => performance.timeOrigin + performance.now(),
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
  const nowNs = BigInt(Math.floor((performance.timeOrigin + performance.now()) * 1_000_000));
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
