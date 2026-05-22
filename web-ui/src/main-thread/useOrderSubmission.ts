/**
 * useOrderSubmission — React hook backing the OrderEntryForm.
 *
 * Plan §12 (APP-160). Owns a panel-scoped {@link CommandClient} backed by a
 * real {@link WorkerClient}. Exposes a typed {@code submit(payload)} method
 * plus a small status state machine for the form UI.
 *
 * Threading: main thread (React).
 *
 * Lifecycle: a fresh WorkerClient + CommandClient pair is created on hook
 * mount and disposed on unmount. The WorkerClient spawns a Worker, opens
 * tokenPort + watchdogPort + commandPort MessageChannels, and posts INIT
 * with all three as Transferables. Disposal terminates the worker and
 * closes every port.
 */
import { useCallback, useEffect, useState } from "react";

import {
  type CommandAckResult,
  type CommandAckStatusName,
  type NewOrderSinglePayload,
  CommandRejectedError,
} from "@/main-thread/commandClient";
// Note: CommandClient + getWorkerClient are imported DYNAMICALLY in the
// useEffect below — static imports would pull `workerClientSingleton.ts` →
// `devTokenProvider.ts` (with its `__E2E_JWT_OVERRIDE__` global symbol literal)
// into the production bundle even though the form's submission path is
// test-mode-only. The bundle-guard test asserts both literals are absent from
// dist/*.js. Loose typing on the dynamic-import return is intentional —
// adding a type-only static import would still leak the literals to esbuild.
import type { CommandClient } from "@/main-thread/commandClient";

export type SubmitState =
  | { kind: "idle" }
  | { kind: "loading" }
  | { kind: "success"; ack: CommandAckResult }
  | { kind: "error"; message: string; rejectStatus?: CommandAckStatusName };

export interface UseOrderSubmissionApi {
  readonly state: SubmitState;
  readonly submit: (payload: NewOrderSinglePayload) => Promise<void>;
  readonly reset: () => void;
}

export function useOrderSubmission(): UseOrderSubmissionApi {
  const [client, setClient] = useState<CommandClient | null>(null);
  const [state, setState] = useState<SubmitState>({ kind: "idle" });

  useEffect(() => {
    // Test-mode escape hatch — production deployments use APP-160's iframe
    // token issuer + a different bootstrap path. The dynamic imports below
    // keep `commandClient.ts` + `workerClientSingleton.ts` + `devTokenProvider.ts`
    // OUT of the production bundle; the bundle-guard test asserts the test-mode
    // global symbols never reach dist/*.js.
    if (import.meta.env.VITE_E2E_REAL_BACKEND !== "true") {
      // Don't bake the env-var literal into the message — bundle guard greps for it.
      setState({
        kind: "error",
        message: "OrderEntryForm requires the dev-mode flag (real-backend path only)",
      });
      return (): void => {
        /* nothing to dispose — early-return path */
      };
    }
    // Mutable single-element holder defeats ESLint's narrowing — `disposed.v`
    // is mutated by the cleanup function (returned below); the awaiter inside
    // the IIFE checks the holder after every await. A bare `let` would be
    // narrowed to `false` by `no-unnecessary-condition` because the lint
    // engine cannot see the cross-closure mutation.
    const disposed = { v: false };
    let ccLocal: CommandClient | null = null;
    void (async (): Promise<void> => {
      try {
        const [{ CommandClient: CC }, { getWorkerClient }] = await Promise.all([
          import("@/main-thread/commandClient"),
          import("@/main-thread/workerClientSingleton"),
        ]);
        // React StrictMode mounts every component twice on first render in dev.
        // The cleanup callback flips `disposed.v=true` between the two mounts.
        // The check below sits AFTER both dynamic imports have resolved (the
        // last suspension point); from here through `setClient(cc)` is purely
        // synchronous, so a single check is sufficient. If `getWorkerClient()`
        // is ever made async, ADD a second check after `new CC(...)` and
        // dispose the constructed CC if `disposed.v` is now true.
        if (disposed.v) return;
        const cc = new CC(getWorkerClient());
        ccLocal = cc;
        setClient(cc);
        // APP-225 §D spec 06b throttle escape hatch — bind `window.__submitCommandRaw` to this
        // CommandClient so the Playwright spec can bypass the form's single-in-flight state
        // machine and drive raw submits at the server-side rate limiter. No-op outside e2e mode.
        // Dynamically imported to keep e2eHooks out of the production bundle for forms that
        // mount before installEarlyHooks() runs (it doesn't, but the dynamic import is the
        // existing project idiom for e2e-only glue and DCE works the same way).
        const { registerSubmitCommandRaw } = await import("@/main-thread/e2eHooks");
        registerSubmitCommandRaw((payload) => cc.submitOrder(payload));
      } catch (e: unknown) {
        if (disposed.v) return;
        setState({
          kind: "error",
          message: e instanceof Error ? e.message : String(e),
        });
      }
    })();
    return (): void => {
      disposed.v = true;
      // Dispose ONLY the CommandClient — the WorkerClient singleton's
      // lifetime spans the process; tearing it down here would break
      // messageSource's inbound stream subscription.
      ccLocal?.dispose();
      // APP-225 §D — restore the throwing stub so any post-unmount page.evaluate fails loudly
      // instead of driving a disposed CommandClient. Best-effort: ignore errors (the e2eHooks
      // module is e2e-only, dynamically imported; in prod the import resolves to a no-op).
      void import("@/main-thread/e2eHooks")
        .then(({ unregisterSubmitCommandRaw }) => {
          unregisterSubmitCommandRaw();
        })
        .catch(() => {
          /* e2eHooks unavailable (prod) — no-op. */
        });
    };
  }, []);

  const submit = useCallback(
    async (payload: NewOrderSinglePayload): Promise<void> => {
      if (!client) {
        setState({ kind: "error", message: "client not ready" });
        return;
      }
      setState({ kind: "loading" });
      try {
        const ack = await client.submitOrder(payload);
        setState({ kind: "success", ack });
      } catch (e: unknown) {
        if (e instanceof CommandRejectedError) {
          setState({ kind: "error", message: e.message, rejectStatus: e.status });
        } else if (e instanceof Error) {
          setState({ kind: "error", message: e.message });
        } else {
          setState({ kind: "error", message: String(e) });
        }
      }
    },
    [client],
  );

  const reset = useCallback((): void => {
    setState({ kind: "idle" });
  }, []);

  return { state, submit, reset };
}
