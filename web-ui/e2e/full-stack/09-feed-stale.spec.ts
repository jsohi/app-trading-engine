/**
 * Full-stack spec 9: market-data feed-stale lifecycle — LIVE → STALE → LIVE.
 *
 * Plan §Commit 9 / spec 09. Validates the complete feed-liveness state machine
 * visible in the browser when the pricing-service is paused and then resumed
 * mid-spec:
 *
 *   1. Baseline: `feedState$` is "LIVE" (pricing-service is running).
 *   2. Pause pricing-service (no more ticks on Aeron stream 204).
 *   3. Assert `feedState$` transitions to "STALE" within 5 s.
 *   4. Assert `connectionStream$` did NOT change — WS transport stays healthy.
 *   5. Resume pricing-service (ticks resume on stream 204).
 *   6. Assert `feedState$` transitions back to "LIVE" within 2 s.
 *      The first market-data tick (template 54) from the restarted adapter at
 *      `MARKET_DATA_PUBLISH_CADENCE_MICROS = 5_000 µs` proves the price-feed
 *      path is healthy (EBS Direct / ICE Impact discipline — heartbeats alone do
 *      NOT clear STALE).
 *   7. Per-spec metric: `marketdata.feed.state{state=STALE}` ≥ 1 (read from
 *      the Prometheus scrape endpoint when available).
 *
 * Harness wiring — option A (HTTP management endpoint)
 * ====================================================
 * The pricing-service runs as an {@code AgentRunner} thread inside the launcher
 * JVM (same process as the WebSocket server). We cannot {@code kill -STOP} the
 * launcher PID — that would also pause the WS-server egress thread and the
 * heartbeats that carry the STALE/LIVE notifications.
 *
 * APP-244 Phase 3 picked option A: a thin JDK {@code HttpServer} management
 * endpoint exposed by {@link com.trading.engine.launcher.E2eManagementServer}.
 * It binds to {@code 127.0.0.1:$TRADING_E2E_MGMT_PORT} and exposes:
 *
 *   - {@code POST /e2e/pricing/pause}  — closes the pricing AgentRunner
 *   - {@code POST /e2e/pricing/resume} — re-launches the pricing AgentRunner
 *   - {@code GET  /e2e/health}         — readiness check
 *
 * The endpoint is gated behind {@code TRADING_E2E_MGMT_ENABLED=1} — production
 * deployments never set this env var, so the launcher's {@code fromEnvironment}
 * factory returns {@code null} and the endpoint is never even constructed.
 * Option B (separate JVM via {@code PricingServiceMain} + {@code
 * E2E_PRICING_SERVICE_PID}) is also viable but requires more harness wiring —
 * see APP-244 Phase 3 plan for rationale.
 *
 * The spec is guarded by {@code test.skip} when {@code TRADING_E2E_MGMT_PORT}
 * is unset so the suite does not fail for teams that haven't yet enabled the
 * harness env vars.
 *
 * STALE threshold: `MARKET_DATA_STALE_THRESHOLD_NANOS = 3_000_000_000L` (3 s).
 * Budget for assertion: 5 s (leaves 2 s of headroom for observer round-trip +
 * network jitter). LIVE recovery budget: 2 s (first tick arrives within 5 ms of
 * adapter thread start; cold JVM adds ~500 ms; total << 2 s).
 */
import { expect, test } from "@playwright/test";
import { drainQuiescenceAndBaseline, readinessGate } from "./helpers";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/** Maximum wait for feedState$ to reach "STALE" after pricing-service is killed. */
const STALE_BUDGET_MS = 5_000;

/** Maximum wait for feedState$ to reach "LIVE" after pricing-service restarts. */
const LIVE_RECOVERY_BUDGET_MS = 2_000;

// ---------------------------------------------------------------------------
// Pricing-service pause / resume helpers (E2E management HTTP endpoint)
// ---------------------------------------------------------------------------

/**
 * Returns the base URL of the launcher's E2E management endpoint, e.g.
 * `http://127.0.0.1:9876`. Reads `TRADING_E2E_MGMT_PORT` from the env so the
 * port is configurable per CI lane.
 *
 * @throws Error when TRADING_E2E_MGMT_PORT is not set.
 */
function managementBaseUrl(): string {
  const raw = process.env.TRADING_E2E_MGMT_PORT;
  if (!raw || raw.trim() === "") {
    throw new Error(
      "TRADING_E2E_MGMT_PORT is not set. " +
        "The full-stack harness must export it (and launch the launcher with TRADING_E2E_MGMT_ENABLED=1). " +
        "See the §Harness wiring section in web-ui/e2e/full-stack/09-feed-stale.spec.ts.",
    );
  }
  const port = parseInt(raw, 10);
  if (isNaN(port) || port < 1 || port > 65_535) {
    throw new Error(`TRADING_E2E_MGMT_PORT is not a valid TCP port: "${raw}"`);
  }
  return `http://127.0.0.1:${String(port)}`;
}

/**
 * POSTs to a management endpoint and returns the response body. Throws on
 * non-2xx status so the spec fails fast with an actionable error.
 */
async function mgmtPost(path: string): Promise<string> {
  const url = `${managementBaseUrl()}${path}`;
  const resp = await fetch(url, { method: "POST" });
  const body = await resp.text();
  if (!resp.ok) {
    throw new Error(
      `management endpoint POST ${path} failed: status=${String(resp.status)} body='${body}'`,
    );
  }
  return body;
}

/**
 * Pauses the pricing-service AgentRunner via the launcher's management HTTP
 * endpoint. The launcher's pricing-service stops publishing ticks on Aeron
 * stream 204 within ~1 idle-cycle (~µs); the rest of the launcher (WS server,
 * cluster client, egress thread) keeps running.
 *
 * Precondition: TRADING_E2E_MGMT_PORT must be set by the harness.
 */
async function killPricingService(): Promise<void> {
  console.log("[spec 09] pausing pricing-service via management endpoint");
  const body = await mgmtPost("/e2e/pricing/pause");
  if (body !== "paused") {
    throw new Error(`unexpected pause response body: '${body}' (expected 'paused')`);
  }
  console.log("[spec 09] pricing-service paused");
}

/**
 * Resumes the pricing-service AgentRunner via the launcher's management HTTP
 * endpoint. The launcher re-launches the pricing AgentRunner with the original
 * config; the first market-data tick (template 54) lands on Aeron stream 204
 * within a few hundred ms.
 */
async function restartPricingService(): Promise<void> {
  console.log("[spec 09] resuming pricing-service via management endpoint");
  const body = await mgmtPost("/e2e/pricing/resume");
  if (body !== "resumed") {
    throw new Error(`unexpected resume response body: '${body}' (expected 'resumed')`);
  }
  console.log("[spec 09] pricing-service resumed");
}

// ---------------------------------------------------------------------------
// Spec
// ---------------------------------------------------------------------------

test.describe("feed-stale lifecycle", () => {
  // Guard: skip the test when the harness hasn't enabled the launcher's E2E
  // management endpoint. This prevents the suite from failing on teams that
  // haven't yet set TRADING_E2E_MGMT_ENABLED=1 + TRADING_E2E_MGMT_PORT, while
  // still compiling and listing correctly so CI can report the pending gap.
  test.beforeAll(() => {
    if (!process.env.TRADING_E2E_MGMT_PORT) {
      console.warn(
        "[spec 09] TRADING_E2E_MGMT_PORT not set — test will be skipped. " +
          "See §Harness wiring in 09-feed-stale.spec.ts.",
      );
    }
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
        !process.env.TRADING_E2E_MGMT_PORT,
        "TRADING_E2E_MGMT_PORT not set — launcher E2E management endpoint not wired. " +
          "See §Harness wiring in 09-feed-stale.spec.ts.",
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
      await killPricingService();
      console.log(`[spec 09] pricing-service paused at t=${String(tKill)}ms`);

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
                (globalThis as unknown as { __feedStates?: Array<{ s: string }> }).__feedStates ??
                [],
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
      await restartPricingService();
      console.log(`[spec 09] pricing-service resumed at t=${String(tRestart)}ms`);

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
                (globalThis as unknown as { __feedStates?: Array<{ s: string }> }).__feedStates ??
                [],
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
      // Step 9: Per-spec metric — Prometheus counter
      // websocket_marketdata_feed_state_transitions_total >= 1.
      //
      // Primary assertion: fetch the launcher's Prometheus scrape endpoint
      // (MetricsHttpServer, default 127.0.0.1:9100) and look for the counter
      // emitted by MarketDataSubscriptionLivenessTracker on every LIVE/STALE/QUIET
      // transition. Micrometer maps the registered Counter name
      // "websocket.marketdata.feed.state.transitions" to
      // "websocket_marketdata_feed_state_transitions_total" in the Prometheus
      // exposition format (dots → underscores; "_total" suffix for Counter).
      //
      // The endpoint port is configurable via TRADING_METRICS_PORT (default 9100);
      // the harness sets this when it spawns the launcher with the management
      // endpoint enabled. We allow the env var to override the default so CI
      // lanes that use a non-default port still pass.
      // -----------------------------------------------------------------------
      const metricsPort = process.env.TRADING_METRICS_PORT ?? "9100";
      const metricsUrl = `http://127.0.0.1:${metricsPort}/metrics`;
      const metricsResp = await page.request.get(metricsUrl);
      expect(
        metricsResp.status(),
        `Prometheus scrape at ${metricsUrl} must return 200; got ${String(metricsResp.status())}`,
      ).toBe(200);
      const metricsBody = await metricsResp.text();
      // Match the counter line with any (or no) label set. Micrometer emits the
      // counter without state-labelled cardinality; only the cumulative total is
      // exposed. The value is a float (Prometheus convention) so we match a
      // permissive numeric pattern.
      const counterMatch = /^websocket_marketdata_feed_state_transitions_total(?:\{[^}]*\})?\s+([0-9.eE+-]+)/m.exec(metricsBody);
      expect(
        counterMatch,
        `per-spec metric: scrape body must contain ` +
          `websocket_marketdata_feed_state_transitions_total; full body=\n${metricsBody}`,
      ).not.toBeNull();
      // Prometheus counter values are emitted as plain floats in text format
      // (e.g. "3" or "3.0e0"); parseFloat is the canonical text-format parser.
      // Not a bigint coercion — these are scrape-format scalars, not SBE int64 fields.
      const counterValue = parseFloat(counterMatch?.[1] ?? "0");
      expect(
        counterValue,
        `per-spec metric: websocket_marketdata_feed_state_transitions_total must be ` +
          `>= 1 after LIVE→STALE→LIVE; got ${String(counterValue)}`,
      ).toBeGreaterThanOrEqual(1);

      // Secondary defence-in-depth check: the __feedStates recorder must also
      // have observed at least one STALE transition. This catches the case where
      // the scrape endpoint returns a stale total (e.g. previous test run) but
      // this specific run did not actually trigger a transition.
      const allFeedStates = await page.evaluate(
        () => (globalThis as unknown as { __feedStates?: Array<{ s: string }> }).__feedStates ?? [],
      );
      const staleTransitions = allFeedStates.filter((t) => t.s === "STALE");
      expect(
        staleTransitions.length,
        `defence-in-depth: __feedStates recorder must observe >= 1 STALE transition ` +
          `in this run (secondary to the Prometheus counter assertion above)`,
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
});
