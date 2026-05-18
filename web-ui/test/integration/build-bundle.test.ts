/**
 * Bundle-guard test — proves the test-mode escape hatch never ships to a
 * production bundle and that the gzip+brotli sizes stay under budget.
 *
 * Plan §4 + §9. Runs in the `unit` (node) vitest project — NOT the browser
 * project (vite build cannot execute under vitest-browser-playwright). Wired
 * into the `:web-ui:bundleGuard` Gradle task; explicitly NOT wired into
 * `:web-ui:test` because `vite build` cold-cost (~30s) is too high for the
 * inner dev loop.
 *
 * Invariants asserted:
 *
 * 1. Every emitted .js file under `web-ui/dist/` is FREE of these symbol
 *    names — they are test-mode escape hatches and must be DCE'd by esbuild
 *    in production:
 *      - VITE_E2E_REAL_BACKEND
 *      - VITE_DEV_JWT
 *      - any literal JWT (regex `eyJ[A-Za-z0-9_-]{20,}`)
 *      - __ordersGridApi
 *      - __forceWsClose
 *      - __cellFlashes
 *      - __e2eHooks
 *      - __E2E_JWT_OVERRIDE__
 *      - __connStates
 *      - __connStatesUnsub
 *      - feedState$ (plan §Commit 9 / spec 09 feed-stale; exposed via E2EHooks)
 *
 * 2. Sum of gzipped JS files ≤ baseline + 10% headroom.
 * 3. Sum of brotli-compressed JS files ≤ baseline + 10% headroom (matches
 *    what the production CDN serves).
 *
 * Baselines are checked into `web-ui/bundle-budget.json`. Regenerate with
 * `npm run e2e:full-stack -- --update-baselines` after a deliberate bump.
 */
import { describe, it, expect, beforeAll } from "vitest";
import { execSync } from "node:child_process";
import { readFileSync, readdirSync, statSync } from "node:fs";
import { gzipSync, brotliCompressSync } from "node:zlib";
import { resolve, join } from "node:path";

const REPO_ROOT = resolve(__dirname, "..", "..", "..");
const WEB_UI = resolve(REPO_ROOT, "web-ui");
const DIST = join(WEB_UI, "dist");
const BUDGET_FILE = join(WEB_UI, "bundle-budget.json");
const BUDGET_HEADROOM = 0.1;

const FORBIDDEN_SYMBOLS = [
  "VITE_E2E_REAL_BACKEND",
  "VITE_DEV_JWT",
  "__ordersGridApi",
  "__forceWsClose",
  "__cellFlashes",
  "__e2eHooks",
  "__E2E_JWT_OVERRIDE__",
  "__connStates",
  "__connStatesUnsub",
  // Phase 3 Commit 9 additions — plan §Q + §Commit 9 bundle-guard extension.
  // feedState$ is exposed via E2EHooks in e2eHooks.ts (DCE'd in prod builds).
  "feedState$",
];
const FORBIDDEN_JWT_REGEX = /eyJ[A-Za-z0-9_-]{20,}/;

interface BundleBudget {
  /** Recorded sum of gzipped *.js bytes (baseline). */
  readonly gzipBytes: number;
  /** Recorded sum of brotli-compressed *.js bytes (baseline). */
  readonly brotliBytes: number;
}

beforeAll(() => {
  // The test owns its own vite build so the assertions cannot pass against a
  // stale dist/ from a previous run. The cwd is the web-ui workspace because
  // npm run build resolves the local vite binary.
  execSync("npm run build", { cwd: WEB_UI, stdio: "inherit" });
}, 180_000);

describe("bundle-guard: production-bundle escape-hatch leakage", () => {
  it("does not contain any test-mode escape-hatch symbol", () => {
    const offenders: { file: string; symbol: string }[] = [];
    for (const f of jsFiles(DIST)) {
      const contents = readFileSync(f, "utf8");
      for (const sym of FORBIDDEN_SYMBOLS) {
        if (contents.includes(sym)) offenders.push({ file: f, symbol: sym });
      }
      const jwtMatch = FORBIDDEN_JWT_REGEX.exec(contents);
      if (jwtMatch)
        offenders.push({ file: f, symbol: `JWT pattern '${jwtMatch[0].slice(0, 20)}…'` });
    }
    expect(
      offenders,
      `forbidden symbols leaked into prod bundle: ${JSON.stringify(offenders, null, 2)}`,
    ).toEqual([]);
  });
});

describe("bundle-guard: size budget", () => {
  it("gzipped + brotli sums stay within budget + 10% headroom", () => {
    const budget = readBudget();
    const totals = computeTotals();
    const gzipMax = Math.floor(budget.gzipBytes * (1 + BUDGET_HEADROOM));
    const brotliMax = Math.floor(budget.brotliBytes * (1 + BUDGET_HEADROOM));
    expect(
      totals.gzipBytes,
      `gzip bundle: ${String(totals.gzipBytes)}B vs budget ${String(budget.gzipBytes)}B (max ${String(gzipMax)}B with 10% headroom)`,
    ).toBeLessThanOrEqual(gzipMax);
    expect(
      totals.brotliBytes,
      `brotli bundle: ${String(totals.brotliBytes)}B vs budget ${String(budget.brotliBytes)}B (max ${String(brotliMax)}B with 10% headroom)`,
    ).toBeLessThanOrEqual(brotliMax);
  });
});

function readBudget(): BundleBudget {
  let raw: string;
  try {
    raw = readFileSync(BUDGET_FILE, "utf8");
  } catch (e: unknown) {
    // Self-baselining (returning current totals as the budget) silently passes
    // every regression on a fresh checkout — defeats the entire test. Fail
    // loudly with a measure-and-commit instruction.
    throw new Error(
      `bundle-guard: missing baseline at ${BUDGET_FILE}. Run \`npm run build\`, ` +
        `compute gzip+brotli totals over dist/assets/*.js, and commit a starter ` +
        `bundle-budget.json. Original error: ${e instanceof Error ? e.message : String(e)}`,
    );
  }
  return JSON.parse(raw) as BundleBudget;
}

function computeTotals(): BundleBudget {
  let gz = 0;
  let br = 0;
  for (const f of jsFiles(DIST)) {
    const buf = readFileSync(f);
    gz += gzipSync(buf).length;
    br += brotliCompressSync(buf).length;
  }
  return { gzipBytes: gz, brotliBytes: br };
}

function jsFiles(dir: string): string[] {
  const out: string[] = [];
  const walk = (d: string): void => {
    for (const name of readdirSync(d)) {
      const full = join(d, name);
      const st = statSync(full);
      if (st.isDirectory()) walk(full);
      else if (full.endsWith(".js")) out.push(full);
    }
  };
  walk(dir);
  return out;
}
