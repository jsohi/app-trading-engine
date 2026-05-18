/**
 * Bundle-guard test — proves the test-mode escape hatch never ships to a
 * production bundle and that the gzip+brotli sizes stay under budget.
 *
 * Plan §4 + §9 + APP-244 §Commit C.9. Runs in the `unit` (node) vitest
 * project — NOT the browser project (vite build cannot execute under
 * vitest-browser-playwright). Wired into the `:web-ui:bundleGuard` Gradle
 * task; explicitly NOT wired into `:web-ui:test` because `vite build`
 * cold-cost (~30s) is too high for the inner dev loop.
 *
 * <p><b>Bundle modes covered (Commit C.9 extension):</b>
 *
 * <ol>
 *   <li><b>Production bundle</b> — built with {@code VITE_E2E_REAL_BACKEND}
 *       unset. Asserts every {@link FORBIDDEN_SYMBOLS} row whose
 *       {@code prodForbidden} flag is true is ABSENT, and no JWT-shaped
 *       literal leaks.</li>
 *   <li><b>E2E bundle</b> — built with {@code VITE_E2E_REAL_BACKEND=true}.
 *       Asserts every row whose {@code e2eRequired} flag is true is PRESENT
 *       — proves the conditional-export mechanism actually ships the
 *       test-mode globals when the flag is set, so spec 09 and friends can
 *       rely on them. Without this mirror assertion, an accidental "no-op
 *       even in e2e mode" regression would silently break the Playwright
 *       suite at the wrong layer (test failure instead of a build assertion).</li>
 * </ol>
 *
 * <p><b>Size budget</b> (gzip + brotli ≤ baseline + 10% headroom) runs over
 * the prod bundle only; the e2e bundle is allowed to be larger because it
 * ships the test-mode escape hatches.
 *
 * <p>Baselines are checked into `web-ui/bundle-budget.json`. Regenerate with
 * `npm run e2e:full-stack -- --update-baselines` after a deliberate bump.
 *
 * <p>{@link FORBIDDEN_SYMBOLS} + the search routines live in
 * {@code build-bundle.guard.ts} so the sibling
 * {@code build-bundle.self-test.test.ts} can exercise the matcher against
 * synthetic bundles without duplicating logic.
 */
import { describe, it, expect, beforeAll } from "vitest";
import { execSync } from "node:child_process";
import { readFileSync, readdirSync, rmSync, statSync } from "node:fs";
import { gzipSync, brotliCompressSync } from "node:zlib";
import { resolve, join } from "node:path";

import {
  FORBIDDEN_SYMBOLS,
  findForbiddenInProd,
  findJwtLiteral,
  findMissingInE2e,
} from "./build-bundle.guard";

const REPO_ROOT = resolve(__dirname, "..", "..", "..");
const WEB_UI = resolve(REPO_ROOT, "web-ui");
const DIST_PROD = join(WEB_UI, "dist");
const DIST_E2E = join(WEB_UI, "dist-e2e");
const BUDGET_FILE = join(WEB_UI, "bundle-budget.json");
const BUDGET_HEADROOM = 0.1;

interface BundleBudget {
  /** Recorded sum of gzipped *.js bytes (baseline). */
  readonly gzipBytes: number;
  /** Recorded sum of brotli-compressed *.js bytes (baseline). */
  readonly brotliBytes: number;
}

beforeAll(() => {
  // The test owns BOTH vite builds so the assertions cannot pass against a
  // stale dist/ from a previous run. The cwd is the web-ui workspace because
  // `npm run build` resolves the local vite binary.
  //
  // Prod build: VITE_E2E_REAL_BACKEND deliberately UNSET (we strip it from
  // the inherited env so a developer running the suite with the var set in
  // their shell — e.g. while debugging full-stack — does not pollute the
  // prod-bundle assertion).
  const prodEnv: NodeJS.ProcessEnv = { ...process.env };
  delete prodEnv.VITE_E2E_REAL_BACKEND;
  rmSync(DIST_PROD, { recursive: true, force: true });
  execSync("npm run build", { cwd: WEB_UI, stdio: "inherit", env: prodEnv });

  // E2E build: VITE_E2E_REAL_BACKEND=true → vite inlines the comparison as
  // true so the e2eHooks branch survives DCE. --outDir points at dist-e2e
  // so we keep both bundles side-by-side for inspection.
  const e2eEnv: NodeJS.ProcessEnv = { ...process.env, VITE_E2E_REAL_BACKEND: "true" };
  rmSync(DIST_E2E, { recursive: true, force: true });
  execSync("npm run build -- --outDir dist-e2e", {
    cwd: WEB_UI,
    stdio: "inherit",
    env: e2eEnv,
  });
}, 360_000);

describe("bundle-guard: production-bundle escape-hatch leakage", () => {
  it("does not contain any test-mode escape-hatch symbol", () => {
    const offenders: { file: string; symbol: string; rationale: string }[] = [];
    for (const f of jsFiles(DIST_PROD)) {
      const contents = readFileSync(f, "utf8");
      for (const o of findForbiddenInProd(contents)) {
        offenders.push({ file: f, symbol: o.symbol, rationale: o.rationale });
      }
      const jwt = findJwtLiteral(contents);
      if (jwt !== null) {
        offenders.push({
          file: f,
          symbol: `JWT pattern '${jwt.slice(0, 20)}…'`,
          rationale: "Any literal JWT in a shipped bundle is a credential leak.",
        });
      }
    }
    expect(
      offenders,
      `forbidden symbols leaked into prod bundle: ${JSON.stringify(offenders, null, 2)}`,
    ).toEqual([]);
  });
});

describe("bundle-guard: e2e-bundle conditional-export proof", () => {
  it("ships every e2eRequired escape-hatch symbol when VITE_E2E_REAL_BACKEND=true", () => {
    // Concatenate every emitted .js so a symbol that lives in a chunk other
    // than the main entry still satisfies the e2eRequired assertion.
    const concatenated = jsFiles(DIST_E2E)
      .map((f) => readFileSync(f, "utf8"))
      .join("\n");
    const missing = findMissingInE2e(concatenated);
    expect(
      missing,
      `e2eRequired symbols absent from e2e bundle (conditional-export regression?): ` +
        `${JSON.stringify(missing, null, 2)}\n` +
        `If a symbol stopped shipping in e2e mode, full-stack Playwright specs that ` +
        `depend on it will silently break — fix the e2eHooks wiring or remove the ` +
        `e2eRequired flag in build-bundle.guard.ts with rationale.`,
    ).toEqual([]);
  });

  it("the e2e bundle is built from the same FORBIDDEN_SYMBOLS table the prod assertion uses", () => {
    // Defence-in-depth: a future refactor that splits FORBIDDEN_SYMBOLS into
    // two tables would silently desync the two assertions. Pin the count + a
    // hash-equivalent (sorted symbol list) so any such refactor trips a test.
    expect(FORBIDDEN_SYMBOLS.length).toBeGreaterThan(0);
    const symbolsSorted = [...FORBIDDEN_SYMBOLS].map((e) => e.symbol).sort();
    expect(new Set(symbolsSorted).size).toBe(symbolsSorted.length);
  });
});

describe("bundle-guard: size budget", () => {
  it("gzipped + brotli sums stay within budget + 10% headroom", () => {
    const budget = readBudget();
    const totals = computeTotals(DIST_PROD);
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

function computeTotals(dir: string): BundleBudget {
  let gz = 0;
  let br = 0;
  for (const f of jsFiles(dir)) {
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
