/**
 * workerClient — main-thread orchestrator for the SBE Web Worker.
 *
 * Spawns the worker via the existing `loadWorker()` loader, opens
 * the credential `MessagePort` from `tokenProvider`, posts INIT with
 * `[tokenPort, watchdogPort]` Transferables, listens for
 * `MESSAGE_BATCH` / `STATS` / `ERROR`, and surfaces them to the
 * caller as RxJS subjects + connection-state state machine.
 *
 * Watchdog: bidirectional PING/PONG over the watchdog `MessagePort`
 * with 1 s cadence + 250 ms PONG deadline. 3 consecutive misses →
 * `Worker.terminate()` + respawn (capped 3 in 30 s → `WORKER_DEAD`).
 *
 * Threading: main thread.
 *
 * Allocation: per worker spawn — one `Worker` instance + 2
 * `MessageChannel`s. Per `MESSAGE_BATCH` — one fanout to RxJS subjects;
 * the array is consumed synchronously and not retained.
 *
 * Plan reference: §4.2 / §4.7 / §5.4 / §6 rows 22, 23.
 */

import { Subject } from "rxjs";

import { loadWorker } from "@/workers/loadWorker";

import { type TokenProvider } from "@/main-thread/tokenProvider";

import { type WorkerErrorMsg, type WorkerToMain } from "@/workers/protocol/WorkerProtocol";
import { nowEpochNs } from "@/workers/time";

import { WORKER_PROTOCOL_VERSION } from "@/workers/WorkerTuning";

import { type ConnectionState, type WorkerMessage } from "@/shared/transport/MessageShape";

import { BehaviorSubject } from "rxjs";

import {
  WATCHDOG_MISS_LIMIT,
  WATCHDOG_PING_INTERVAL_MS,
  WATCHDOG_PONG_DEADLINE_MS,
  WORKER_RESPAWN_LIMIT,
  WORKER_RESPAWN_WINDOW_MS,
} from "@/workers/WorkerTuning";

/** CommandAck envelope surfaced by the worker on the commandPort (plan §12). */
export interface CommandAckEnvelope {
  readonly correlationId: number;
  readonly status: "Accepted" | "Rejected" | "Duplicate" | "Throttled";
  readonly reasonCode?: string;
}

/** Public surface — caller subscribes to receive worker → main events. */
export interface WorkerClientStreams {
  /** Discriminated union; consumers narrow on `msg.type`. */
  readonly messages$: Subject<WorkerMessage>;
  /** Sealed worker error envelope. */
  readonly errors$: Subject<WorkerErrorMsg>;
  /**
   * Connection state surfaced from the worker (CONNECTING / CONNECTED /
   * BACKPRESSURE / DOWN / ...). `BehaviorSubject` so late subscribers
   * always see the current state. Per Gemini review (HIGH): the worker
   * emits `connection-state` messages and the main-thread client must
   * surface them so the rest of the app (status banner, blotters,
   * reconnect UX) can react.
   */
  readonly connectionState$: BehaviorSubject<ConnectionState>;
  /**
   * CommandAck envelopes from the worker's commandPort (plan §12 / APP-160).
   * Subscribed by `commandClient` to resolve pending submitOrder Promises by
   * matching `correlationId` against its slot table.
   */
  readonly commandAcks$: Subject<CommandAckEnvelope>;
}

export interface WorkerClientOptions {
  readonly tokenProvider: TokenProvider;
  readonly wsUrl: string;
}

/**
 * Spawn a worker and wire the main-thread client. The returned object
 * exposes the public RxJS streams + a `dispose()` for HMR / app shutdown.
 *
 * On worker crash: respawns up to `WORKER_RESPAWN_LIMIT` times within
 * `WORKER_RESPAWN_WINDOW_MS`; over the cap, emits a final connection-
 * state `WORKER_DEAD` and stops. The caller surfaces this to the UI
 * (manual reconnect required).
 */
export class WorkerClient implements WorkerClientStreams {
  readonly messages$ = new Subject<WorkerMessage>();
  readonly errors$ = new Subject<WorkerErrorMsg>();
  readonly connectionState$ = new BehaviorSubject<ConnectionState>("CONNECTING");
  readonly commandAcks$ = new Subject<CommandAckEnvelope>();

  private readonly options: WorkerClientOptions;
  private worker: Worker | null = null;
  private watchdogChannel: MessageChannel | null = null;
  private commandChannel: MessageChannel | null = null;
  private pingTimer: number | null = null;
  private pongDeadlineTimer: number | null = null;
  private consecutiveMisses = 0;
  private respawnTimestamps: number[] = [];
  private reconnectTimer: number | null = null;
  // Per Gemini review R10 (HIGH): persist the backoff attempt counter
  // across worker terminate+respawn so the exponential progression
  // continues. Reset to 0 only on `connection-state: CONNECTED` (a
  // successful AuthAck — the same gate as worker-side
  // `Reconnect.notifyAuthAckSuccess`).
  private currentReconnectAttempt = 0;
  private dead = false;

  constructor(options: WorkerClientOptions) {
    this.options = options;
  }

  /** Boot the worker. Idempotent — repeated calls do nothing. */
  async start(): Promise<void> {
    if (this.dead || this.worker !== null) return;
    await this.spawn();
  }

  /** Manual reconnect — resets backoff and triggers a worker reset. */
  reconnectNow(): void {
    // Per Gemini review R11 (MEDIUM): the previous "active worker"
    // branch posted RECONNECT_NOW which closed the WS but then went
    // back through the worker's onclose handler — which consults
    // Reconnect.nextDelayMs and posts a `reconnect_due_after_ms`
    // back to main, scheduling a backoff respawn. That entirely
    // defeated the purpose of "Reconnect Now". Both branches
    // (mid-backoff + active connection) collapse to the same
    // terminate-and-spawn path so user-initiated reconnect is
    // genuinely immediate.
    if (this.dead) return;
    if (this.reconnectTimer !== null) {
      self.clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    if (this.worker !== null) {
      this.worker.terminate();
      this.worker = null;
    }
    this.stopWatchdog();
    this.watchdogChannel?.port1.close();
    this.watchdogChannel = null;
    this.commandChannel?.port1.close();
    this.commandChannel = null;
    this.spawn().catch((err: unknown) => {
      this.errors$.next({
        type: "ERROR",
        protocolVersion: WORKER_PROTOCOL_VERSION,
        code: "WORKER",
        hint: `reconnectNow respawn failed: ${err instanceof Error ? err.message : String(err)}`,
      });
    });
  }

  /** HMR / app-shutdown disposal. */
  dispose(): void {
    if (this.reconnectTimer !== null) {
      self.clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    this.stopWatchdog();
    if (this.worker !== null) {
      this.worker.postMessage({ type: "CLOSE", protocolVersion: WORKER_PROTOCOL_VERSION });
      this.worker.terminate();
      this.worker = null;
    }
    this.watchdogChannel?.port1.close();
    this.watchdogChannel = null;
    this.commandChannel?.port1.close();
    this.commandChannel = null;
    this.messages$.complete();
    this.errors$.complete();
    this.commandAcks$.complete();
  }

  private async spawn(): Promise<void> {
    const tokenPort = await this.options.tokenProvider();
    this.watchdogChannel = new MessageChannel();
    this.commandChannel = new MessageChannel();
    const worker = loadWorker();
    this.worker = worker;
    worker.onmessage = (ev: MessageEvent<WorkerToMain>) => {
      this.onWorkerMessage(ev.data);
    };
    worker.onerror = () => {
      this.handleWorkerCrash();
    };

    // Wire watchdog port (main side).
    this.watchdogChannel.port1.onmessage = (ev: MessageEvent<unknown>) => {
      this.onWatchdogPong(ev);
    };

    // Wire command port (main side, plan §12 / APP-160). Worker posts
    // CommandAck envelopes here when wss frames matching templateId=70 arrive.
    this.commandChannel.port1.onmessage = (ev: MessageEvent<unknown>) => {
      this.onCommandAck(ev.data);
    };

    // Send INIT with all three ports as Transferables.
    worker.postMessage(
      {
        type: "INIT",
        protocolVersion: WORKER_PROTOCOL_VERSION,
        wsUrl: this.options.wsUrl,
        tokenPort,
        watchdogPort: this.watchdogChannel.port2,
        commandPort: this.commandChannel.port2,
        initialReconnectAttempt: this.currentReconnectAttempt,
      },
      [tokenPort, this.watchdogChannel.port2, this.commandChannel.port2],
    );

    this.startWatchdog();
  }

  /**
   * Submit a pre-encoded command frame (SBE NewOrderSingle bytes from
   * {@link import("@/sbe/encoders/NewOrderSingleEncoder").NewOrderSingleEncoder})
   * to the worker for transmission on the wss send queue. The
   * {@code correlationId} is the main-thread slot key that {@link commandAcks$}
   * envelopes will carry back.
   */
  submitCommand(bytes: Uint8Array, length: number, correlationId: number): void {
    if (this.commandChannel === null) {
      throw new Error("WorkerClient.submitCommand: not started — call start() first");
    }
    // Structured-clone of the bytes (NOT Transferable). Reasoning:
    //   - The encoded NewOrderSingle is 116 bytes. Structured-clone of a
    //     Uint8Array of that size is ~50ns — well under the per-submit budget.
    //   - Transferable would detach `bytes.buffer` on the main side, forcing
    //     the caller's pool slot to be re-allocated (~116 B alloc per submit
    //     + GC pressure). That contradicts the commandClient `*AllocTest`
    //     baseline AND the documented "zero-allocation after warmup" contract.
    //   - The worker reads `bytes.subarray(0, length)`, so the trailing bytes
    //     of the pooled buffer (cloned by structured-clone) are ignored
    //     server-side. The clone of those bytes is a few extra nanoseconds —
    //     negligible vs. the GC pause cost of the alloc-per-submit path.
    this.commandChannel.port1.postMessage({
      type: "COMMAND_FRAME",
      bytes,
      length,
      correlationId,
    });
  }

  private onCommandAck(data: unknown): void {
    if (
      data === null ||
      typeof data !== "object" ||
      (data as { type?: unknown }).type !== "COMMAND_ACK"
    ) {
      return;
    }
    const env = data as { correlationId?: unknown; status?: unknown; reasonCode?: unknown };
    if (typeof env.correlationId !== "number" || typeof env.status !== "string") return;
    const status = env.status;
    if (
      status !== "Accepted" &&
      status !== "Rejected" &&
      status !== "Duplicate" &&
      status !== "Throttled"
    ) {
      return;
    }
    // Bound the reasonCode length defensively. The worker is the sole writer
    // today, but a bug or future spoofing path that ships a multi-megabyte
    // string would cost render time + memory in the UI. 256 chars covers every
    // currently-defined reason code (`NOT_CONNECTED`, `INVALID_LENGTH`,
    // `ENTITLEMENT`, `RATE_LIMIT`, etc.) with abundant headroom.
    const reasonCode =
      typeof env.reasonCode === "string" && env.reasonCode.length <= 256
        ? env.reasonCode
        : undefined;
    // Conditional spread satisfies exactOptionalPropertyTypes — `reasonCode`
    // is omitted entirely when absent rather than set to `undefined`.
    const ack: CommandAckEnvelope =
      reasonCode !== undefined
        ? { correlationId: env.correlationId, status, reasonCode }
        : { correlationId: env.correlationId, status };
    this.commandAcks$.next(ack);
  }

  private onWorkerMessage(msg: WorkerToMain): void {
    // Both sides import the same `WORKER_PROTOCOL_VERSION` constant so a
    // mismatch cannot happen by construction; if a future test injects
    // a forged envelope we narrow via the type system here.
    switch (msg.type) {
      case "MESSAGE_BATCH":
        for (const m of msg.messages) {
          this.messages$.next(m);
          // Per Gemini review (HIGH): connection-state updates are
          // emitted via the MESSAGE_BATCH stream (see worker.ts
          // transitionConnection). Surface them on a dedicated subject
          // so the rest of the app can subscribe.
          if (m.type === "connection-state") {
            this.connectionState$.next(m.state);
            // Per Gemini review R10 (HIGH): only reset the persisted
            // backoff counter on a successful AuthAck (the same gate
            // worker-side `Reconnect.notifyAuthAckSuccess` uses).
            // CONNECTED is the post-AuthAck state per
            // `transitionConnection("CONNECTED")` in worker.ts.
            if (m.state === "CONNECTED") {
              this.currentReconnectAttempt = 0;
            }
          }
        }
        break;
      case "PONG":
        this.consecutiveMisses = 0;
        this.clearPongDeadline();
        break;
      case "ERROR":
        // Per Gemini review R7 (HIGH): the worker's onclose handler
        // posts an INIT-coded ERROR with hint
        // `reconnect_due_after_ms:<N>` after consulting Reconnect's
        // backoff math. Parse the hint and schedule a respawn so
        // automatic reconnection actually fires. The error is also
        // forwarded to errors$ for observability.
        this.errors$.next(msg);
        if (msg.code === "INIT" && msg.hint?.startsWith("reconnect_due_after_ms:") === true) {
          const delayStr = msg.hint.slice("reconnect_due_after_ms:".length);
          const delayMs = Number.parseInt(delayStr, 10);
          if (Number.isFinite(delayMs) && delayMs >= 0) {
            this.scheduleReconnect(delayMs);
          }
        }
        break;
    }
  }

  /**
   * Schedule a worker respawn after `delayMs` (driven by Reconnect's
   * full-jitter backoff). Cancels any prior scheduled respawn.
   */
  private scheduleReconnect(delayMs: number): void {
    if (this.dead) return;
    if (this.reconnectTimer !== null) {
      self.clearTimeout(this.reconnectTimer);
    }
    this.reconnectTimer = self.setTimeout(() => {
      this.reconnectTimer = null;
      // Increment the persisted attempt counter so the next worker's
      // Reconnect picks up where this one left off (Gemini R10 HIGH).
      this.currentReconnectAttempt += 1;
      // Tear down the current worker (it has already closed its WS)
      // and respawn with a fresh tokenPort + watchdog channel.
      if (this.worker !== null) {
        this.worker.terminate();
        this.worker = null;
      }
      this.stopWatchdog();
      this.watchdogChannel?.port1.close();
      this.watchdogChannel = null;
      // Per reviewer B finding: close commandChannel.port1 + null the field on
      // every auto-respawn path (mirror watchdog cleanup). Prior versions
      // leaked one MessagePort + listener per reconnect cycle.
      this.commandChannel?.port1.close();
      this.commandChannel = null;
      this.spawn().catch((err: unknown) => {
        this.errors$.next({
          type: "ERROR",
          protocolVersion: WORKER_PROTOCOL_VERSION,
          code: "WORKER",
          hint: `scheduled reconnect failed: ${err instanceof Error ? err.message : String(err)}`,
        });
      });
    }, delayMs);
  }

  private startWatchdog(): void {
    this.stopWatchdog();
    this.pingTimer = self.setInterval(() => {
      this.sendPing();
    }, WATCHDOG_PING_INTERVAL_MS);
  }

  private stopWatchdog(): void {
    if (this.pingTimer !== null) {
      self.clearInterval(this.pingTimer);
      this.pingTimer = null;
    }
    this.clearPongDeadline();
  }

  private sendPing(): void {
    const port = this.watchdogChannel?.port1;
    if (port === undefined) return;
    port.postMessage({
      type: "PING",
      protocolVersion: WORKER_PROTOCOL_VERSION,
      mainNanos: nowEpochNs(),
    });
    this.armPongDeadline();
  }

  private armPongDeadline(): void {
    this.clearPongDeadline();
    this.pongDeadlineTimer = self.setTimeout(() => {
      this.consecutiveMisses += 1;
      if (this.consecutiveMisses >= WATCHDOG_MISS_LIMIT) {
        this.handleWorkerCrash();
      }
    }, WATCHDOG_PONG_DEADLINE_MS);
  }

  private clearPongDeadline(): void {
    if (this.pongDeadlineTimer !== null) {
      self.clearTimeout(this.pongDeadlineTimer);
      this.pongDeadlineTimer = null;
    }
  }

  private onWatchdogPong(_ev: MessageEvent<unknown>): void {
    // PONG handled here too (inbound on watchdog port); reset miss counter.
    this.consecutiveMisses = 0;
    this.clearPongDeadline();
  }

  private handleWorkerCrash(): void {
    if (this.dead) return;
    if (this.worker !== null) {
      this.worker.terminate();
      this.worker = null;
    }
    this.stopWatchdog();
    this.watchdogChannel?.port1.close();
    this.watchdogChannel = null;
    // Per reviewer B finding: close commandChannel.port1 + null the field on
    // every auto-respawn path (mirror watchdog cleanup).
    this.commandChannel?.port1.close();
    this.commandChannel = null;

    // Per /review LOW + Agent B MEDIUM: use monotonic `performance.now()`
    // instead of wall-clock `Date.now()`. This is elapsed-time math (the
    // 30 s respawn window); a wall-clock step (NTP, VM snapshot restore)
    // could falsely retain stale timestamps and trip WORKER_DEAD.
    const now = performance.now();
    this.respawnTimestamps.push(now);
    const cutoff = now - WORKER_RESPAWN_WINDOW_MS;
    this.respawnTimestamps = this.respawnTimestamps.filter((t) => t >= cutoff);

    if (this.respawnTimestamps.length > WORKER_RESPAWN_LIMIT) {
      this.dead = true;
      this.errors$.next({
        type: "ERROR",
        protocolVersion: WORKER_PROTOCOL_VERSION,
        code: "WORKER",
        hint: `worker respawn limit ${String(WORKER_RESPAWN_LIMIT)} exceeded in ${String(
          Math.floor(WORKER_RESPAWN_WINDOW_MS / 1000),
        )} s`,
      });
      // Push WORKER_DEAD onto connectionState$ so downstream subscribers
      // (commandClient.failAllInFlight + ConnectionIndicator + reconnect UX)
      // observe the terminal state immediately rather than waiting on a
      // 5s CommandTimeoutError. Reviewer A HIGH finding F-A2.
      this.connectionState$.next("WORKER_DEAD");
      return;
    }

    // Fresh token + respawn. Per Gemini review (MEDIUM): catch any
    // rejection from `spawn()` (e.g. tokenProvider hung, loadWorker
    // failed) and surface via `errors$` instead of leaving an
    // unhandled promise rejection that hides the failure mode.
    this.spawn().catch((err: unknown) => {
      this.errors$.next({
        type: "ERROR",
        protocolVersion: WORKER_PROTOCOL_VERSION,
        code: "WORKER",
        hint: `respawn failed: ${err instanceof Error ? err.message : String(err)}`,
      });
    });
  }
}
