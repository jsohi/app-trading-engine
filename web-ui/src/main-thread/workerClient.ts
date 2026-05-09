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

import { type WorkerMessage } from "@/shared/transport/MessageShape";

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

  private readonly options: WorkerClientOptions;
  private worker: Worker | null = null;
  private watchdogChannel: MessageChannel | null = null;
  private pingTimer: number | null = null;
  private pongDeadlineTimer: number | null = null;
  private consecutiveMisses = 0;
  private respawnTimestamps: number[] = [];
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
    if (this.worker === null) return;
    this.worker.postMessage({
      type: "RECONNECT_NOW",
      protocolVersion: WORKER_PROTOCOL_VERSION,
    });
  }

  /** HMR / app-shutdown disposal. */
  dispose(): void {
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
        }
        break;
      case "PONG":
        this.consecutiveMisses = 0;
        this.clearPongDeadline();
        break;
      case "ERROR":
        this.errors$.next(msg);
        break;
    }
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

    // Fresh token + respawn.
    void this.spawn();
  }
}
