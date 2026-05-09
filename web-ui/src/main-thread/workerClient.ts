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

  private readonly options: WorkerClientOptions;
  private worker: Worker | null = null;
  private watchdogChannel: MessageChannel | null = null;
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
    // Per Gemini review R9 (HIGH): if a backoff timer is currently
    // armed, the worker's WS is already null and `RECONNECT_NOW`
    // would be a no-op. Cancel the backoff and respawn immediately so
    // the user-facing "reconnect now" button is responsive even
    // mid-backoff window.
    if (this.reconnectTimer !== null) {
      self.clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
      // Per Gemini review R10 (HIGH): terminate the previous worker
      // BEFORE spawning a fresh one — without this, the prior worker
      // instance leaks (still alive in the background, holding its
      // WebSocket and timers) when reconnectNow fires mid-backoff.
      if (this.worker !== null) {
        this.worker.terminate();
        this.worker = null;
      }
      this.stopWatchdog();
      this.watchdogChannel?.port1.close();
      this.watchdogChannel = null;
      this.spawn().catch((err: unknown) => {
        this.errors$.next({
          type: "ERROR",
          protocolVersion: WORKER_PROTOCOL_VERSION,
          code: "WORKER",
          hint: `reconnectNow respawn failed: ${err instanceof Error ? err.message : String(err)}`,
        });
      });
      return;
    }
    if (this.worker === null) return;
    this.worker.postMessage({
      type: "RECONNECT_NOW",
      protocolVersion: WORKER_PROTOCOL_VERSION,
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
    this.messages$.complete();
    this.errors$.complete();
  }

  private async spawn(): Promise<void> {
    const tokenPort = await this.options.tokenProvider();
    this.watchdogChannel = new MessageChannel();
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

    // Send INIT with both ports as Transferables.
    worker.postMessage(
      {
        type: "INIT",
        protocolVersion: WORKER_PROTOCOL_VERSION,
        wsUrl: this.options.wsUrl,
        tokenPort,
        watchdogPort: this.watchdogChannel.port2,
        initialReconnectAttempt: this.currentReconnectAttempt,
      },
      [tokenPort, this.watchdogChannel.port2],
    );

    this.startWatchdog();
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
      mainNanos: BigInt(Math.floor((performance.timeOrigin + performance.now()) * 1_000_000)),
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

    const now = Date.now();
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
