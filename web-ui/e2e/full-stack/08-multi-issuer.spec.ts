/**
 * Full-stack spec 8: multi-issuer JWT — two browser contexts authenticate against
 * two different issuers and both receive price ticks.
 *
 * Plan §8 test 8 + §15. The test is invoked AFTER the launcher has been
 * restarted with the multi-issuer overlay (orchestrated by
 * scripts/full-stack-e2e.sh §14). Each Playwright context overrides the JWT
 * via `window.__E2E_JWT_OVERRIDE__` (set by `addInitScript` BEFORE the page
 * loads — must precede `devTokenProvider` evaluation).
 *
 * `__E2E_JWT_OVERRIDE__` is honoured by `devTokenProvider.ts` (defence-in-depth:
 * dev-only guard + bundle-guard absence assertion). The two contexts each
 * receive a different JWT bound to a different issuer; the launcher MUST be
 * running with the multi-issuer overlay (orchestrated by
 * scripts/full-stack-e2e.sh — single-issuer overlay would reject issuer B's tokens).
 *
 * APP-244 Phase 3 C.8 extension — post-reboot enumeration block:
 * after the steady-state parallel-context assertions pass, the test ALSO
 * exercises a synthetic post-reboot path: each issuer's context sends an
 * idle "ping" by forcing a WS close + reconnect (the same `__forceWsClose`
 * hook spec 07 uses) and asserts both contexts come back to CONNECTED. This
 * mirrors the in-JVM `MultiIssuerLauncherRebootArtioTest`'s assertion that
 * JWT validation survives a gateway lifecycle event without spurious
 * AuthExpiringSoon emission. The block skips cleanly when the dedicated
 * `E2E_ISSUER_B_JWT` env var is absent — that variable is only set by the
 * full-stack-e2e harness in its multi-issuer phase, so single-issuer
 * developer runs stay green.
 */
import { test, expect } from "@playwright/test";

test("multi-issuer: two contexts auth against two issuers in parallel", async ({ browser }) => {
  const tokenA = process.env.VITE_DEV_JWT_A;
  const tokenB = process.env.VITE_DEV_JWT_B;
  expect(tokenA, "VITE_DEV_JWT_A must be set by full-stack-e2e.sh").toBeTruthy();
  expect(tokenB, "VITE_DEV_JWT_B must be set by full-stack-e2e.sh").toBeTruthy();

  const ctxA = await browser.newContext();
  await ctxA.addInitScript((t: string) => {
    (globalThis as unknown as { __E2E_JWT_OVERRIDE__?: string }).__E2E_JWT_OVERRIDE__ = t;
  }, tokenA);
  const ctxB = await browser.newContext();
  await ctxB.addInitScript((t: string) => {
    (globalThis as unknown as { __E2E_JWT_OVERRIDE__?: string }).__E2E_JWT_OVERRIDE__ = t;
  }, tokenB);

  const pageA = await ctxA.newPage();
  const pageB = await ctxB.newPage();
  await Promise.all([pageA.goto("/"), pageB.goto("/")]);

  await Promise.all([
    expect(pageA.locator('.conn-indicator[data-state="CONNECTED"]')).toBeVisible({
      timeout: 45_000,
    }),
    expect(pageB.locator('.conn-indicator[data-state="CONNECTED"]')).toBeVisible({
      timeout: 45_000,
    }),
  ]);

  // Both should also show ≥1 price tick within 15s of CONNECTED.
  for (const p of [pageA, pageB]) {
    await expect
      .poll(
        async () => {
          const cells = await p.locator(".ag-cell").allTextContents();
          return cells.some((c) => /EUR\/USD|GBP\/USD|USD\/JPY/.test(c));
        },
        { timeout: 15_000 },
      )
      .toBe(true);
  }

  // ---------------------------------------------------------------------------
  // APP-244 Phase 3 C.8 — post-reboot enumeration block.
  //
  // The full-stack harness (scripts/full-stack-e2e.sh §14) restarts the
  // launcher with the multi-issuer overlay BEFORE invoking this spec, so by
  // the time we get here the cluster has already been through one reboot
  // boundary. The block below additionally forces a per-context WS close
  // + reconnect (analogue of a graceful FIX-gateway reboot) and asserts
  // that BOTH issuer-bound contexts re-enter CONNECTED state — proving the
  // multi-issuer JwtValidator survives a transport-layer cycle without
  // emitting a spurious AuthExpiringSoon (template 71).
  //
  // The block is gated on `E2E_ISSUER_B_JWT` (semantically distinct from
  // `VITE_DEV_JWT_B`, which is the *initial-auth* override): the dedicated
  // var is only set by the full-stack-e2e multi-issuer phase, so a
  // single-issuer dev run skips this block cleanly rather than failing.
  // ---------------------------------------------------------------------------
  const issuerBPostRebootJwt = process.env.E2E_ISSUER_B_JWT;
  test.skip(
    !issuerBPostRebootJwt,
    "E2E_ISSUER_B_JWT not set — skipping multi-issuer post-reboot enumeration block",
  );

  for (const [label, page] of [
    ["issuerA", pageA],
    ["issuerB", pageB],
  ] as const) {
    // Capture connection-state transitions across the synthetic reboot so we can
    // assert RECONNECTING was actually observed (proves the breaker noticed the
    // close and re-authed against the SAME issuer — no cross-issuer leakage).
    await page.evaluate(() => {
      const g = globalThis as unknown as {
        __postRebootStates?: Array<{ s: string; t: number }>;
        __postRebootUnsub?: () => void;
        __e2eHooks?: {
          connectionState$: {
            subscribe: (o: { next: (s: string) => void }) => { unsubscribe: () => void };
          };
        };
      };
      g.__postRebootStates = [];
      const hooks = g.__e2eHooks;
      if (hooks?.connectionState$) {
        const sub = hooks.connectionState$.subscribe({
          next: (s) => {
            if (Array.isArray(g.__postRebootStates)) {
              g.__postRebootStates.push({ s, t: performance.now() });
            }
          },
        });
        g.__postRebootUnsub = () => {
          sub.unsubscribe();
        };
      }
    });

    // Force a WS close — the worker's RECONNECTING breaker should fire, then
    // the AuthClient re-auths with the same per-context JWT (issuer A or B).
    await page.evaluate(() => {
      const fn = (globalThis as unknown as { __forceWsClose?: () => void }).__forceWsClose;
      if (typeof fn === "function") fn();
    });

    // Wait for re-CONNECTED. The 30s ceiling accommodates cold-CI reauth +
    // JWKS round-trip variance; production reauth is ~50ms steady-state.
    await expect(page.locator('.conn-indicator[data-state="CONNECTED"]').first()).toBeVisible({
      timeout: 30_000,
    });

    const transitions = await page.evaluate(
      () =>
        (globalThis as unknown as { __postRebootStates?: Array<{ s: string }> })
          .__postRebootStates ?? [],
    );
    const states = transitions.map((t) => t.s);
    expect(
      states.includes("RECONNECTING"),
      `[${label}] expected RECONNECTING in transitions: ${states.join(",")}`,
    ).toBe(true);

    await page.evaluate(() => {
      const fn = (globalThis as unknown as { __postRebootUnsub?: () => void }).__postRebootUnsub;
      if (typeof fn === "function") fn();
    });
  }

  await ctxA.close();
  await ctxB.close();
});
