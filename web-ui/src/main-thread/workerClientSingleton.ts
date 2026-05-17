/**
 * Process-wide singleton {@link WorkerClient} — owned at module-load time so
 * every consumer (messageSource for inbound stream, useOrderSubmission for
 * outbound commands, future panels) shares ONE worker / ONE wss session.
 *
 * <p>Per plan §12 (APP-160) + reviewer A finding F-A1 (R9):
 * spawning a second WorkerClient inside a panel mount opens a parallel wss
 * session, burns a second {@code maxConnectionsPerUser} slot, and routes
 * CommandAck back to whichever worker emitted the original send — leaving
 * the main blotter stream blind to acks. The singleton eliminates that
 * class of bug entirely.
 *
 * <p><b>Lifecycle:</b> lazy — first call to {@link getWorkerClient} constructs
 * the WorkerClient + calls {@code start()}. {@link disposeWorkerClient} tears
 * it down (HMR / app shutdown). Re-acquiring after dispose constructs a fresh
 * instance.
 *
 * <p><b>Test-mode only:</b> the entire APP-160 path is gated on
 * {@code import.meta.env.VITE_E2E_REAL_BACKEND === "true"}. Calling
 * {@code getWorkerClient()} outside test mode throws — production deployment
 * uses APP-160's iframe token issuer + a different bootstrap path.
 *
 * <p><b>Threading:</b> main thread.
 */
import { devTokenProvider } from "@/main-thread/devTokenProvider";
import { WorkerClient } from "@/main-thread/workerClient";

let _instance: WorkerClient | null = null;

function realBackendModeEnabled(): boolean {
  return import.meta.env.VITE_E2E_REAL_BACKEND === "true";
}

/**
 * Read-only access to the existing singleton — never constructs a new one and
 * never throws. Returns {@code null} when the singleton has not been started
 * (real-backend mode disabled, or {@link getWorkerClient} not yet called).
 *
 * <p>Use this for shell-level reactive subscriptions (Phase 3 Commit B
 * {@code panelLayout$} hook in {@code App.tsx}) that must render in BOTH
 * fake-stream demo mode AND real-backend mode without forcing the worker to
 * exist. The caller treats {@code null} as "no worker; use defaults".
 */
export function peekWorkerClient(): WorkerClient | null {
  return _instance;
}

/**
 * Get-or-create the singleton WorkerClient. Calls {@code start()} on first
 * construction; subsequent calls return the same instance (already started).
 */
export function getWorkerClient(): WorkerClient {
  if (!realBackendModeEnabled()) {
    throw new Error(
      "getWorkerClient() called outside test mode — APP-160's iframe issuer is the prod path",
    );
  }
  if (_instance !== null) return _instance;
  const wsUrl = import.meta.env.VITE_WS_URL ?? "wss://localhost:5173/ws";
  const wc = new WorkerClient({ tokenProvider: devTokenProvider, wsUrl });
  _instance = wc;
  void wc.start().catch((err: unknown) => {
    // Failure surfaces on connectionState$ → DOWN via the catch in
    // WorkerClient.start; downstream consumers observe it. We swallow here
    // so module-load doesn't reject the singleton-acquisition Promise.
    console.error("getWorkerClient: WorkerClient.start() failed", err);
  });
  return wc;
}

/**
 * Tear down the singleton (HMR / app shutdown). Subsequent
 * {@link getWorkerClient} calls construct a fresh instance.
 */
export function disposeWorkerClient(): void {
  if (_instance === null) return;
  _instance.dispose();
  _instance = null;
}

/** Test-only — reset the singleton between vitest tests. */
export function __resetWorkerClientSingletonForTests(): void {
  _instance?.dispose();
  _instance = null;
}
