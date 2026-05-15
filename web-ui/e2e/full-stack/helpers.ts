/**
 * Shared helpers for the full-stack Playwright suite (spec files 01-08).
 *
 * Plan §8: per-spec readiness gate, drain-quiescence baseline, deterministic
 * ClOrdID prefixes, FIX initiator path resolution, in-page connectionState$
 * recorder.
 */
import { type Page, expect } from "@playwright/test";
import { spawnSync } from "node:child_process";
import path from "node:path";
import { randomUUID } from "node:crypto";

/**
 * Resolves the absolute path of the integration-tests installDist binary —
 * spawnSync from inside a Playwright spec runs in Node with cwd=web-ui, so
 * relative paths break. The shell script exports E2E_REPO_ROOT explicitly.
 */
export function fixCliPath(): string {
  const root = process.env.E2E_REPO_ROOT;
  if (!root) {
    throw new Error("E2E_REPO_ROOT not set — must run via scripts/full-stack-e2e.sh");
  }
  return path.resolve(
    root,
    "integration-tests/build/install/integration-tests/bin/integration-tests",
  );
}

/**
 * Builds a ClOrdID with the given spec-number prefix and a short UUID suffix.
 *
 * Constraint: SBE schema caps ClOrdID at 20 bytes (matches FIX 4.4 ClOrdID
 * boundary). Format `S{NN}-{8-hex}` = 11 bytes — leaves 9 bytes for any per-leg
 * suffix the FIX test client appends (`-buy`, `-sell`, `-1`..`-99`). 8 hex
 * digits = 32 bits of entropy, ample for per-spec uniqueness.
 */
export function clOrdId(specNumber: string): string {
  // randomUUID() returns 36 chars w/ dashes; strip dashes and take 8 hex digits.
  const shortId = randomUUID().replace(/-/g, "").slice(0, 8);
  return `S${specNumber}-${shortId}`;
}

/**
 * Subscription-readiness gate: wait for the ConnectionIndicator to reach
 * CONNECTED. Plan §8 mandates 45 s ceiling — cold WS connect + AuthAck +
 * JWKS first-hit + worker spawn + cluster ingress session.
 *
 * Selector verified against ConnectionIndicator.tsx: wrapper carries
 * `data-state` (uppercase enum value); the dot carries `conn-{green|amber|red}`.
 */
export async function readinessGate(page: Page): Promise<void> {
  await expect(page.locator('.conn-indicator[data-state="CONNECTED"]').first()).toBeVisible({
    timeout: 45_000,
  });
}

/**
 * Drain-quiescence + baseline: wait until the AG Grid row count is stable
 * across two reads 500 ms apart (re-poll up to 5 s if not stable), then
 * snapshot the count for delta assertions.
 *
 * Returns a `Map<bladeName, baselineCount>` so the spec can assert deltas
 * (e.g. "10 NEW rows added", not "10 total rows").
 */
export async function drainQuiescenceAndBaseline(page: Page): Promise<Map<string, number>> {
  const blotters = ["orders", "positions", "prices"];
  const baseline = new Map<string, number>();

  for (const blotter of blotters) {
    let prev = -1;
    let stable = false;
    const deadline = Date.now() + 5_000;
    while (Date.now() < deadline) {
      const cur = await page
        .locator(`[data-testid="blotter-${blotter}"] .ag-row`)
        .count()
        .catch(() => 0);
      if (cur === prev) {
        stable = true;
        baseline.set(blotter, cur);
        break;
      }
      prev = cur;
      await page.waitForTimeout(500);
    }
    if (!stable) {
      // Last value seen is acceptable when the cluster is still draining; the
      // delta assertions in each spec MUST account for ±a few rows of noise
      // when this branch fires. Logged so a noisy CI run is investigable.
      console.warn(
        `drainQuiescenceAndBaseline: ${blotter} did not stabilise; using last value = ${String(prev)}`,
      );
      baseline.set(blotter, Math.max(0, prev));
    }
  }
  return baseline;
}

/**
 * Spawn the FIX initiator with the given args (in addition to host/port/scenario).
 * Returns the exit code; non-zero is a test failure (the spec asserts === 0).
 */
export function spawnFixCli(extraArgs: readonly string[]): number {
  const r = spawnSync(fixCliPath(), ["--host", "localhost", "--port", "19880", ...extraArgs], {
    stdio: "inherit",
  });
  return r.status ?? 1;
}
