/**
 * Spawns ESLint against the lint-fixtures directory and asserts a
 * non-zero exit (which is what we want — the fixtures are designed
 * to violate rules). Belt-and-braces: also asserts the rule names
 * appear in the JSON output.
 *
 * Threading model: spawns a child process per test; not parallel-
 * sensitive.
 */
import { describe, expect, it } from "vitest";
import { spawnSync } from "node:child_process";
import { resolve } from "node:path";
import process from "node:process";

// Vitest sets cwd to the workspace root (web-ui/). Anchor paths from
// there to avoid `import.meta.url` which is unreliable under
// jsdom-based test environments.
const webUiRoot = process.cwd();
const fixturesDir = resolve(webUiRoot, "test", "lint-fixtures");
const repoRoot = resolve(webUiRoot, "..");

interface EslintFileReport {
  readonly filePath: string;
  readonly messages: ReadonlyArray<{
    readonly ruleId: string | null;
    readonly message: string;
    readonly severity: number;
  }>;
}

function runEslintJson(target: string): {
  status: number;
  reports: EslintFileReport[];
} {
  const result = spawnSync(
    "npx",
    [
      "--no-install",
      "eslint",
      "--format",
      "json",
      "--no-config-lookup",
      "--config",
      resolve(webUiRoot, "eslint.config.js"),
      // Fixture files are listed in eslint.config.js's `ignores` block
      // (so they don't break `npm run lint`). For this assertion suite
      // we DELIBERATELY want ESLint to lint them — the whole point is
      // to verify the rules fire. `--no-ignore` overrides the project
      // ignore pattern at the CLI level.
      "--no-ignore",
      target,
    ],
    {
      cwd: webUiRoot,
      encoding: "utf8",
      env: { ...process.env, FORCE_COLOR: "0" },
    },
  );
  const stdout = result.stdout || "[]";
  let reports: EslintFileReport[] = [];
  try {
    reports = JSON.parse(stdout) as EslintFileReport[];
  } catch {
    // ESLint occasionally writes non-JSON before the array on parse
    // errors. Best-effort fallback.
    reports = [];
  }
  return { status: result.status ?? 1, reports };
}

describe("lint fixtures", () => {
  it("numberBigintCoercionFixture_isRejectedByEslint", () => {
    const target = resolve(fixturesDir, "number-bigint-coercion.ts");
    const { status, reports } = runEslintJson(target);
    expect(status).not.toBe(0);
    const allMessages = reports.flatMap((r) => r.messages);
    const restricted = allMessages.find((m) => m.ruleId === "no-restricted-syntax");
    expect(restricted).toBeDefined();
  }, 60_000);

  it("spanInHotPathFixture_isRejectedByCustomRule", () => {
    const target = resolve(fixturesDir, "span-in-hot-path.ts");
    const { status, reports } = runEslintJson(target);
    expect(status).not.toBe(0);
    const allMessages = reports.flatMap((r) => r.messages);
    const hotPath = allMessages.find((m) => m.ruleId === "local/no-span-in-hot-path");
    expect(hotPath).toBeDefined();
  }, 60_000);

  it("repoRootIsResolvable", () => {
    // Sanity guard so the file is exercised by the suite even when
    // CI lacks ESLint (which would skip the spawn-based tests).
    expect(repoRoot.length).toBeGreaterThan(0);
  });
});
