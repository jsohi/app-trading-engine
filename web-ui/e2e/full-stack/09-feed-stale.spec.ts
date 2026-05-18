/**
 * Full-stack spec 9: market-data feed-stale lifecycle — LIVE → STALE → LIVE.
 *
 * Plan §Commit 9 / spec 09. Validates the complete feed-liveness state machine
 * visible in the browser when the pricing-service is killed and then restarted
 * mid-spec:
 *
 *   1. Baseline: `feedState$` is "LIVE" (pricing-service is running).
 *   2. Kill pricing-service (no more ticks on Aeron stream 204).
 *   3. Assert `feedState$` transitions to "STALE" within 5 s.
 *   4. Assert `connectionStream$` did NOT change — WS transport stays healthy.
 *   5. Restart pricing-service (ticks resume on stream 204).
 *   6. Assert `feedState$` transitions back to "LIVE" within 2 s.
 *      The first market-data tick (template 54) from the restarted adapter at
 *      `MARKET_DATA_PUBLISH_CADENCE_MICROS = 5_000 µs` proves the price-feed
 *      path is healthy (EBS Direct / ICE Impact discipline — heartbeats alone do
 *      NOT clear STALE).
 *   7. Per-spec metric: `marketdata.feed.state{state=STALE}` ≥ 1 (read from
 *      the Prometheus scrape endpoint when available; see §Harness Gap below).
 *
 * Harness gap — new infrastructure required
 * =========================================
 * The pricing-service runs as an {@code AgentRunner} thread inside the launcher
 * JVM (same process as the WebSocket server). Stopping only the pricing thread
 * without stopping the WS server requires one of:
 *
 *   A. Launching pricing-service as a SEPARATE JVM process (recommended):
 *      - `scripts/full-stack-e2e.sh` forks a second `./gradlew :pricing-service:run`
 *        JVM and exports its PID to `$E2E_PRICING_SERVICE_PID`.
 *      - The spec reads `process.env.E2E_PRICING_SERVICE_PID` and uses
 *        `execSync("kill -SIGTERM $pid")` to stop it.
 *      - Restart: `execSync("./gradlew :pricing-service:run &")` (or a
 *        dedicated restart script).
 *      - This is the recommended architecture: clean process boundary, no JDWP
 *        or management-API machinery.
 *
 *   B. A thin HTTP management endpoint (alternative):
 *      - Add a `POST /e2e/pricing/pause` + `POST /e2e/pricing/resume` Netty
 *        handler to the launcher (guarded by `VITE_E2E_REAL_BACKEND` env var).
 *      - The endpoint calls `pricingRef.get().agentRunner().close()` / re-init.
 *      - Spec calls `fetch("http://localhost:9999/e2e/pricing/pause")`.
 *
 * Until one of the above is in place this spec will compile and list correctly
 * but the {@link killPricingService} and {@link restartPricingService} helpers
 * will throw at runtime if `E2E_PRICING_SERVICE_PID` is not set. The spec is
 * guarded by `test.skip` when the env var is absent so the suite does not fail
 * for teams that haven't yet wired the separate-process harness.
 *
 * STALE threshold: `MARKET_DATA_STALE_THRESHOLD_NANOS = 3_000_000_000L` (3 s).
 * Budget for assertion: 5 s (leaves 2 s of headroom for observer round-trip +
 * network jitter). LIVE recovery budget: 2 s (first tick arrives within 5 ms of
 * adapter thread start; cold JVM adds ~500 ms; total << 2 s).
 */
import { expect, test } from "@playwright/test";
import { execSync, spawnSync } from "node:child_process";
import path from "node:path";
import { drainQuiescenceAndBaseline, readinessGate } from "./helpers";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/** Maximum wait for feedState$ to reach "STALE" after pricing-service is killed. */
const STALE_BUDGET_MS = 5_000;

/** Maximum wait for feedState$ to reach "LIVE" after pricing-service restarts. */
const LIVE_RECOVERY_BUDGET_MS = 2_000;

// ---------------------------------------------------------------------------
// Pricing-service kill / restart helpers
// ---------------------------------------------------------------------------

/**
 * Returns the pricing-service process PID from the harness-exported env var.
 * Throws a clear error when the env var is absent so the failure is actionable.
 *
 * @throws Error when E2E_PRICING_SERVICE_PID is not set (harness infrastructure
 *   gap — see §Harness Gap in the spec file-level comment above).
 */
function pricingServicePid(): number {
  const raw = process.env.E2E_PRICING_SERVICE_PID;
  if (!raw || raw.trim() === "") {
    throw new Error(
      "E2E_PRICING_SERVICE_PID is not set. " +
        "The full-stack harness must launch pricing-service as a separate JVM process and export its PID. " +
        "See the §Harness Gap section in web-ui/e2e/full-stack/09-feed-stale.spec.ts.",
    );
  }
  const pid = parseInt(raw, 10);
  if (isNaN(pid) || pid <= 0) {
    throw new Error(`E2E_PRICING_SERVICE_PID is not a valid PID: "${raw}"`);
  }
  return pid;
}

/**
 * Returns the absolute path to the pricing-service restart script.
 * Expects `scripts/restart-pricing-service.sh` relative to E2E_REPO_ROOT.
 *
 * The script must:
 *   1. Wait until the old PID is no longer alive.
 *   2. Re-fork pricing-service with the same Aeron dir as the original run.
 *   3. Write the new PID to a known location (or update E2E_PRICING_SERVICE_PID).
 *   4. Exit 0 when the new process is ready (e.g. "PRICING_READY" log line).
 */
function restartScriptPath(): string {
  const root = process.env.E2E_REPO_ROOT;
  if (!root) {
    throw new Error("E2E_REPO_ROOT not set — must run via scripts/full-stack-e2e.sh");
  }
  return path.resolve(root, "scripts/restart-pricing-service.sh");
}

/**
 * Kill the pricing-service process with SIGTERM and wait for it to exit.
 * Uses SIGKILL after a 3 s grace period to guard against hung shutdown hooks.
 *
 * Precondition: E2E_PRICING_SERVICE_PID must be set by the harness.
 */
function killPricingService(): void {
  const pid = pricingServicePid();
  console.log(`[spec 09] killing pricing-service (PID ${String(pid)}) with SIGTERM`);
  try {
    execSync(`kill -TERM ${String(pid)}`, { stdio: "pipe" });
  } catch {
    // Already dead — that's fine; the assertion will still exercise the STALE path
    // because the WS server's liveness tracker already detected the missing ticks.
    console.warn(`[spec 09] kill -TERM ${String(pid)} failed (process may already be down)`);
    return;
  }
  // Wait up to 3 s for graceful exit; fall back to SIGKILL.
  const deadline = Date.now() + 3_000;
  while (Date.now() < deadline) {
    try {
      execSync(`kill -0 ${String(pid)}`, { stdio: "pipe" });
      // Still alive — spin-wait.
    } catch {
      // kill -0 failed → process is gone.
      console.log(`[spec 09] pricing-service (PID ${String(pid)}) exited gracefully`);
      return;
    }
  }
  // Grace period elapsed; force-kill.
  try {
    execSync(`kill -KILL ${String(pid)}`, { stdio: "pipe" });
    console.log(`[spec 09] pricing-service (PID ${String(pid)}) force-killed`);
  } catch {
    /* already gone */
  }
}

/**
 * Restart the pricing-service via the harness restart script. Blocks until the
 * script exits (the script is responsible for ensuring the new process is ready).
 *
 * The restart script MUST update `E2E_PRICING_SERVICE_PID` (or write a pid file
 * that this spec can read) so subsequent specs / retry runs have the correct PID.
 */
function restartPricingService(): void {
  const script = restartScriptPath();
  console.log(`[spec 09] restarting pricing-service via ${script}`);
  const result = spawnSync("bash", [script], {
    stdio: "inherit",
    timeout: 15_000, // 15 s ceiling: JVM cold-start + Aeron connect + first publish
  });
  if (result.status !== 0) {
    throw new Error(
      `pricing-service restart script exited with code ${String(result.status)}. ` +
        `Check ${script} and e2e/logs/ for details.`,
    );
  }
  console.log("[spec 09] pricing-service restarted successfully");
}

// ---------------------------------------------------------------------------
// Spec
// ---------------------------------------------------------------------------

test.describe("feed-stale lifecycle", () => {
  // Guard: skip the test when the harness hasn't wired the separate-process
  // pricing-service yet. This prevents the suite from failing on teams that
  // haven't yet landed the harness infrastructure, while still compiling and
  // listing correctly so CI can report the pending gap.
  test.beforeAll(() => {
    if (!process.env.E2E_PRICING_SERVICE_PID) {
      console.warn(
        "[spec 09] E2E_PRICING_SERVICE_PID not set — test will be skipped. " +
          "See §Harness Gap in 09-feed-stale.spec.ts.",
      );
    }
  });
});

test(
  "feedState$ transitions LIVE→STALE on pricing-service kill and STALE→LIVE on restart",
  {
    annotation: {
      type: "plan-ref",
      description: "Phase 3 Commit 9 / spec 09 feed-stale",
    },
  },
  async ({ page }) => {
    // Guard: skip when harness infrastructure is absent.
    test.skip(
      !process.env.E2E_PRICING_SERVICE_PID,
      "E2E_PRICING_SERVICE_PID not set — pricing-service separate-process harness not wired. " +
        "See §Harness Gap in 09-feed-stale.spec.ts.",
    );

    // -----------------------------------------------------------------------
    // Step 1: Navigate + readiness gate + drain quiescence.
    // -----------------------------------------------------------------------
    await page.goto("/");
    await readinessGate(page);
    await drainQuiescenceAndBaseline(page);

    // -----------------------------------------------------------------------
    // Step 2: Assert baseline feedState$ is "LIVE".
    //
    // The spec subscribes to __e2eHooks.feedState$ and captures the current
    // value. If feedState$ is not LIVE at baseline the test environment is not
    // in a valid starting state and we abort early with a clear message.
    // -----------------------------------------------------------------------
    const baselineFeedState = await page.evaluate(
      (): Promise<string> =>
        new Promise<string>((resolve) => {
          const g = globalThis as unknown as {
            __e2eHooks?: {
              feedState$: {
                subscribe: (o: { next: (s: string) => void }) => { unsubscribe: () => void };
              };
            };
          };
          const hooks = g.__e2eHooks;
          if (!hooks?.feedState$) {
            resolve("HOOKS_UNAVAILABLE");
            return;
          }
          // BehaviorSubject: first emission is the current value (synchronous).
          const sub = hooks.feedState$.subscribe({
            next: (s) => {
              sub.unsubscribe();
              resolve(s);
            },
          });
        }),
    );
    expect(
      baselineFeedState,
      `feedState$ must be LIVE at test start (got ${baselineFeedState}); ` +
        "check that the pricing-service is running and emitting ticks.",
    ).toBe("LIVE");

    // -----------------------------------------------------------------------
    // Step 3: Install feed-state + connection-state recorders BEFORE the kill.
    //
    // Both recorders capture transitions in real time — no Playwright poll
    // interval can miss a sub-100 ms flash. The connectionState$ recorder is
    // the guard that proves the WS transport stayed healthy throughout.
    // -----------------------------------------------------------------------
    await page.evaluate(() => {
      const g = globalThis as unknown as {
        __feedStates?: Array<{ s: string; t: number }>;
        __feedStatesUnsub?: () => void;
        __connStatesDuringFeedTest?: Array<{ s: string; t: number }>;
        __connStatesDuringFeedTestUnsub?: () => void;
        __e2eHooks?: {
          feedState$: {
            subscribe: (o: { next: (s: string) => void }) => { unsubscribe: () => void };
          };
          connectionState$: {
            subscribe: (o: { next: (s: string) => void }) => { unsubscribe: () => void };
          };
        };
      };
      g.__feedStates = [];
      g.__connStatesDuringFeedTest = [];
      const hooks = g.__e2eHooks;
      if (hooks?.feedState$) {
        const sub = hooks.feedState$.subscribe({
          next: (s) => {
            if (Array.isArray(g.__feedStates)) {
              g.__feedStates.push({ s, t: performance.now() });
            }
          },
        });
        g.__feedStatesUnsub = () => {
          sub.unsubscribe();
        };
      }
      if (hooks?.connectionState$) {
        const sub = hooks.connectionState$.subscribe({
          next: (s) => {
            if (Array.isArray(g.__connStatesDuringFeedTest)) {
              g.__connStatesDuringFeedTest.push({ s, t: performance.now() });
            }
          },
        });
        g.__connStatesDuringFeedTestUnsub = () => {
          sub.unsubscribe();
        };
      }
    });

    // -----------------------------------------------------------------------
    // Step 4: Kill the pricing-service.
    //
    // After the kill, Aeron stream 204 goes silent. The WS server's
    // MarketDataSubscriptionLivenessTracker (on the aeron-egress thread)
    // detects no ticks for MARKET_DATA_STALE_THRESHOLD_NANOS = 3 s, then
    // emits MarketDataFeedStateChange(STALE) (template 57) to the browser.
    // The browser worker routes the template-57 frame through FeedStateMsg to
    // pushFeedState("STALE") on the main thread's feedState$ BehaviorSubject.
    // -----------------------------------------------------------------------
    const tKill = Date.now();
    killPricingService();
    console.log(`[spec 09] pricing-service killed at t=${String(tKill)}ms`);

    // -----------------------------------------------------------------------
    // Step 5: Assert feedState$ reaches "STALE" within 5 s.
    //
    // Poll by reading __feedStates[] — the recorder installed in step 3 never
    // misses a transition regardless of poll cadence. The 5 s budget is the
    // stale threshold (3 s) plus 2 s of headroom.
    // -----------------------------------------------------------------------
    await expect
      .poll(
        async (): Promise<string> => {
          const states = await page.evaluate(
            () =>
              (globalThis as unknown as { __feedStates?: Array<{ s: string }> }).__feedStates ?? [],
          );
          // Return the latest recorded state; empty array → "LIVE" (no change yet).
          return states.length > 0 ? (states[states.length - 1]?.s ?? "LIVE") : "LIVE";
        },
        {
          timeout: STALE_BUDGET_MS,
          message: `expected feedState$ to reach "STALE" within ${String(STALE_BUDGET_MS)} ms after pricing-service kill`,
        },
      )
      .toBe("STALE");

    const tStale = Date.now();
    console.log(
      `[spec 09] STALE observed: elapsed=${String(tStale - tKill)} ms (budget=${String(STALE_BUDGET_MS)} ms)`,
    );

    // -----------------------------------------------------------------------
    // Step 6: Assert WS transport stayed healthy — connectionState$ unchanged.
    //
    // The feed-state and connection-state streams are orthogonal by design:
    // a STALE market-data feed MUST NOT trip the WS reconnect breaker.
    // The connectionState$ recorder captured every transition since step 3.
    // We assert no state OTHER THAN "CONNECTED" was observed.
    // -----------------------------------------------------------------------
    const connTransitions = await page.evaluate(
      () =>
        (
          globalThis as unknown as {
            __connStatesDuringFeedTest?: Array<{ s: string; t: number }>;
          }
        ).__connStatesDuringFeedTest ?? [],
    );
    const nonConnectedTransitions = connTransitions.filter((t) => t.s !== "CONNECTED");
    expect(
      nonConnectedTransitions,
      `connectionState$ must stay CONNECTED throughout feed-stale test; ` +
        `observed non-CONNECTED transitions: ${JSON.stringify(nonConnectedTransitions)}`,
    ).toHaveLength(0);

    // -----------------------------------------------------------------------
    // Step 7: Restart pricing-service.
    //
    // The restart script re-forks the pricing-service JVM, waits for it to
    // connect to the shared Media Driver, and begins emitting ticks on Aeron
    // stream 204. The first tick (template 54) clears STALE on the server-side
    // liveness tracker (per EBS Direct discipline: only a real tick, NOT a
    // heartbeat, clears STALE). The tracker emits MarketDataFeedStateChange(LIVE)
    // (template 57) to the browser. The browser worker routes this through
    // FeedStateMsg to pushFeedState("LIVE").
    // -----------------------------------------------------------------------
    const tRestart = Date.now();
    restartPricingService();
    console.log(`[spec 09] pricing-service restarted at t=${String(tRestart)}ms`);

    // -----------------------------------------------------------------------
    // Step 8: Assert feedState$ returns to "LIVE" within 2 s.
    //
    // The synthetic adapter's publish cadence is MARKET_DATA_PUBLISH_CADENCE_MICROS
    // = 5_000 µs (5 ms). The first tick arrives within 5 ms of agent thread
    // start; cold JVM adds ~200–500 ms; total is well under 2 s.
    // -----------------------------------------------------------------------
    await expect
      .poll(
        async (): Promise<string> => {
          const states = await page.evaluate(
            () =>
              (globalThis as unknown as { __feedStates?: Array<{ s: string }> }).__feedStates ?? [],
          );
          return states.length > 0 ? (states[states.length - 1]?.s ?? "LIVE") : "LIVE";
        },
        {
          timeout: LIVE_RECOVERY_BUDGET_MS,
          message: `expected feedState$ to return to "LIVE" within ${String(LIVE_RECOVERY_BUDGET_MS)} ms after pricing-service restart`,
        },
      )
      .toBe("LIVE");

    const tLive = Date.now();
    console.log(
      `[spec 09] LIVE restored: elapsed from restart=${String(tLive - tRestart)} ms ` +
        `(budget=${String(LIVE_RECOVERY_BUDGET_MS)} ms); ` +
        `total STALE duration=${String(tLive - tKill)} ms`,
    );

    // -----------------------------------------------------------------------
    // Step 9: Per-spec metric — marketdata.feed.state{state=STALE} >= 1.
    //
    // The Prometheus scrape endpoint (ws-server /metrics) is not yet wired in
    // the current harness (PR 3/4 per WebSocketLauncher Javadoc). Until it is,
    // we assert via the transition counter: feedState$ went through at least one
    // STALE transition (captured in __feedStates above).
    //
    // When the /metrics endpoint is available, replace this block with:
    //   const resp = await page.request.get("http://localhost:<port>/metrics");
    //   const body = await resp.text();
    //   const match = body.match(/marketdata_feed_state_total\{.*state="STALE".*\} (\d+)/);
    //   expect(Number(match?.[1] ?? 0)).toBeGreaterThanOrEqual(1);
    // -----------------------------------------------------------------------
    const allFeedStates = await page.evaluate(
      () => (globalThis as unknown as { __feedStates?: Array<{ s: string }> }).__feedStates ?? [],
    );
    const staleTransitions = allFeedStates.filter((t) => t.s === "STALE");
    expect(
      staleTransitions.length,
      `per-spec metric: expected at least 1 STALE transition in feedState$ (proxy for ` +
        `marketdata.feed.state{state=STALE} >= 1 until Prometheus endpoint is wired)`,
    ).toBeGreaterThanOrEqual(1);

    // -----------------------------------------------------------------------
    // Step 10: Tear down recorders.
    // -----------------------------------------------------------------------
    await page.evaluate(() => {
      const g = globalThis as unknown as {
        __feedStatesUnsub?: () => void;
        __connStatesDuringFeedTestUnsub?: () => void;
      };
      if (typeof g.__feedStatesUnsub === "function") g.__feedStatesUnsub();
      if (typeof g.__connStatesDuringFeedTestUnsub === "function")
        g.__connStatesDuringFeedTestUnsub();
    });
  },
);
