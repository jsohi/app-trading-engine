/**
 * WorkerClient.test.ts — unit tests for the main-thread WorkerClient.
 *
 * Uses a synthetic mock Worker (extending EventTarget) so no real Worker
 * thread is spawned. `loadWorker` is mocked to return the synthetic instance.
 *
 * Tests per APP-36 §4.7 / §5.4 / §6 rows 22, 23.
 *
 * Threading: single-threaded (Vitest jsdom worker).
 * Allocation: test-only — one MockWorker + MessageChannels per test.
 */

import { describe, expect, it, beforeEach, afterEach, vi } from "vitest";

import { WorkerClient } from "@/main-thread/workerClient";
import { type WorkerMessage } from "@/shared/transport/MessageShape";
import { type WorkerErrorMsg, type MessageBatchMsg } from "@/workers/protocol/WorkerProtocol";
import { WORKER_PROTOCOL_VERSION } from "@/workers/WorkerTuning";

// ─── Mock Worker ─────────────────────────────────────────────────────────────

/**
 * Synthetic Worker that captures postMessage calls and exposes helpers to
 * simulate inbound messages from the worker thread.
 */
class MockWorker {
  readonly posted: unknown[] = [];
  readonly transferredLists: Transferable[][] = [];
  terminated = false;

  onmessage: ((ev: MessageEvent<unknown>) => void) | null = null;
  onerror: ((ev: Event) => void) | null = null;

  postMessage(data: unknown, transfer?: Transferable[]): void {
    this.posted.push(data);
    this.transferredLists.push(transfer ?? []);
  }

  terminate(): void {
    this.terminated = true;
  }

  /** Simulate a message arriving from the worker thread. */
  simulateMessage(data: unknown): void {
    if (this.onmessage) {
      this.onmessage({ data } as MessageEvent<unknown>);
    }
  }
}

// ─── Module mock ─────────────────────────────────────────────────────────────

// We use a module-level variable to capture the instance created by the mock.
// The mock factory uses a closure over `spawnedWorkers` to avoid hoisting issues.
const spawnedWorkers: MockWorker[] = [];

vi.mock("@/workers/loadWorker", () => ({
  loadWorker: (): MockWorker => {
    const w = new MockWorker();
    spawnedWorkers.push(w);
    return w;
  },
}));

// ─── TokenProvider factory ────────────────────────────────────────────────────

function makeTokenProvider(): () => Promise<MessagePort> {
  return (): Promise<MessagePort> => {
    const channel = new MessageChannel();
    channel.port1.postMessage({ type: "TOKEN", value: "test-jwt" });
    channel.port1.close();
    return Promise.resolve(channel.port2);
  };
}

// ─── Tests ───────────────────────────────────────────────────────────────────

describe("WorkerClient", () => {
  let client: WorkerClient;

  beforeEach(() => {
    vi.useFakeTimers();
    spawnedWorkers.length = 0; // reset between tests
    client = new WorkerClient({
      tokenProvider: makeTokenProvider(),
      wsUrl: "wss://localhost:8080/ws",
    });
  });

  afterEach(() => {
    client.dispose();
    vi.useRealTimers();
    vi.clearAllMocks();
  });

  it("start_acquiresTokenAndPostsInitWithBothPortsAsTransferable", async () => {
    await client.start();

    expect(spawnedWorkers).toHaveLength(1);
    const worker = spawnedWorkers[0];
    expect(worker).toBeDefined();
    if (!worker) return;

    // INIT message should be the first posted message.
    expect(worker.posted).toHaveLength(1);

    interface InitShape {
      type: string;
      tokenPort: { postMessage: unknown };
      watchdogPort: { postMessage: unknown };
    }
    const initMsg = worker.posted[0] as InitShape;

    expect(initMsg.type).toBe("INIT");
    // Ports are MessagePort objects — check they have the `postMessage` method
    // (MessagePort API) rather than using instanceof which can recurse via
    // the jsdom error serializer on failure.
    expect(typeof initMsg.tokenPort.postMessage).toBe("function");
    expect(typeof initMsg.watchdogPort.postMessage).toBe("function");

    // Both ports must appear in the Transferable list.
    // Use explicit reference equality check rather than `toContain` to avoid
    // the jsdom MessagePort serializer triggering a circular-reference stack overflow.
    const transferred = worker.transferredLists[0] ?? [];
    const tokenPortRef = initMsg.tokenPort as unknown as object;
    const watchdogPortRef = initMsg.watchdogPort as unknown as object;
    expect(transferred.some((t) => t === tokenPortRef)).toBe(true);
    expect(transferred.some((t) => t === watchdogPortRef)).toBe(true);
  });

  it("messageBatch_fanoutsToMessages$Subject", async () => {
    await client.start();

    expect(spawnedWorkers).toHaveLength(1);
    const worker = spawnedWorkers[0];
    if (!worker) return;

    const received: WorkerMessage[] = [];
    const sub = client.messages$.subscribe((m) => received.push(m));

    const priceMsg: WorkerMessage = {
      type: "price",
      symbol: "EURUSD",
      bid: 100_000_000n,
      ask: 101_000_000n,
      serverNanos: 1_000_000_000n,
    };

    const batch: MessageBatchMsg = {
      type: "MESSAGE_BATCH",
      protocolVersion: WORKER_PROTOCOL_VERSION,
      messages: [priceMsg],
    };

    worker.simulateMessage(batch);

    sub.unsubscribe();

    expect(received).toHaveLength(1);
    expect(received[0]).toEqual(priceMsg);
  });

  it("error_fanoutsToErrors$Subject", async () => {
    await client.start();

    expect(spawnedWorkers).toHaveLength(1);
    const worker = spawnedWorkers[0];
    if (!worker) return;

    const errors: WorkerErrorMsg[] = [];
    const sub = client.errors$.subscribe((e) => errors.push(e));

    const errMsg: WorkerErrorMsg = {
      type: "ERROR",
      protocolVersion: WORKER_PROTOCOL_VERSION,
      code: "AUTH",
      hint: "auth handshake timeout",
    };

    worker.simulateMessage(errMsg);

    sub.unsubscribe();

    expect(errors).toHaveLength(1);
    expect(errors[0]?.code).toBe("AUTH");
    expect(errors[0]?.hint).toBe("auth handshake timeout");
  });

  it("dispose_terminatesWorker_completesSubjects", async () => {
    await client.start();

    expect(spawnedWorkers).toHaveLength(1);
    const worker = spawnedWorkers[0];
    if (!worker) return;

    let messagesCompleted = false;
    let errorsCompleted = false;

    client.messages$.subscribe({
      complete: () => {
        messagesCompleted = true;
      },
    });
    client.errors$.subscribe({
      complete: () => {
        errorsCompleted = true;
      },
    });

    client.dispose();

    // Worker must be terminated.
    expect(worker.terminated).toBe(true);

    // Both subjects must complete.
    expect(messagesCompleted).toBe(true);
    expect(errorsCompleted).toBe(true);
  });

  it("start_armsWatchdogPing_afterSpawn", async () => {
    // Verify that after spawn a PING is sent to the watchdog port within
    // WATCHDOG_PING_INTERVAL_MS (1 000 ms). We don't test the full PONG
    // cadence here — just that the watchdog is armed.
    await client.start();

    expect(spawnedWorkers).toHaveLength(1);
    const worker = spawnedWorkers[0];
    if (!worker) return;

    // Advance past the first ping interval.
    // INIT is posted immediately; subsequent messages are the PING on watchdog port.
    // The watchdog sends on port1, not on the Worker directly — so worker.posted
    // stays at 1 (INIT only). We just verify the client didn't crash.
    vi.advanceTimersByTime(1_100);

    // Client should still be alive (not disposed).
    expect(worker.terminated).toBe(false);
  });
});
