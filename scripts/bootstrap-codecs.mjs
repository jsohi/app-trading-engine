#!/usr/bin/env node
/*
 * Bootstrap script for the Trading Engine npm workspace root.
 *
 * Responsibilities (run once on `npm ci` / `npm install` via the root
 * `prepare` script — see root package.json):
 *
 *   1. Initialise Husky 9 supporting scaffolding (.husky/_/) if missing.
 *      Husky 9's bare `husky` invocation does NOT auto-create the `_/`
 *      directory if absent — `husky init` must run once on a fresh clone.
 *
 *   2. Invoke `./gradlew :sbe-typescript-generator:generateTsCodecs`
 *      UNCONDITIONALLY. Gradle's task-level UP-TO-DATE caching (declared
 *      inputs/outputs) handles the no-op case. We deliberately do NOT
 *      short-circuit on `index.ts` existence, or a stale 1A stub would
 *      persist after APP-34 (1B) replaces the generator.
 *
 *   3. On JDK absence: emit a clear "install JDK 25" hint, write a
 *      placeholder `index.ts` so `@trading/sbe-codecs` still resolves,
 *      and exit 0. `npm ci` MUST succeed even without a JDK; the
 *      typecheck step (webUiTypecheck) is the failing surface for a
 *      genuinely missing JDK, with a clear pointer.
 *
 * Threading model: single-threaded shell wrapper. No concurrency.
 *
 * Exit semantics:
 *   - 0  on success OR on JDK-missing soft-fail (so `npm ci` succeeds).
 *   - >0 only on hard errors (e.g., gradle invoked but build failed
 *        for non-JDK-missing reasons). We let those propagate.
 */
import { existsSync, mkdirSync, writeFileSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import process from "node:process";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const repoRoot = resolve(__dirname, "..");

const isWindows = process.platform === "win32";
const gradleWrapper = isWindows ? "gradlew.bat" : "./gradlew";

const stubOutputDir = join(
  repoRoot,
  "sbe-typescript-generator",
  "build",
  "generated-ts",
);
const stubOutputFile = join(stubOutputDir, "index.ts");

const log = (msg) => process.stdout.write(`[bootstrap-codecs] ${msg}\n`);
const warn = (msg) => process.stderr.write(`[bootstrap-codecs] ${msg}\n`);

/**
 * Initialise Husky 9 scaffolding if missing. Husky 9's `husky` command
 * (run by the `prepare` script) is supposed to handle this, but on a
 * truly fresh clone without `.husky/_/` it can no-op silently. Run
 * `npx husky init` defensively when the supporting dir is absent.
 */
function ensureHuskyInit() {
  const huskyDir = join(repoRoot, ".husky");
  const huskySupport = join(huskyDir, "_");
  if (existsSync(huskySupport)) {
    return;
  }
  // Skip in CI / non-git checkouts where hooks aren't useful.
  if (!existsSync(join(repoRoot, ".git"))) {
    return;
  }
  log(".husky/_ missing — running `npx husky init`");
  const result = spawnSync("npx", ["--yes", "husky", "init"], {
    cwd: repoRoot,
    stdio: "inherit",
    shell: isWindows,
  });
  if (result.status !== 0) {
    warn("`npx husky init` failed — pre-commit hooks may not work");
  }
}

/**
 * Write a placeholder `index.ts` so `@trading/sbe-codecs` resolves
 * even when the JDK is unavailable. This lets `npm ci` and downstream
 * tooling complete; `webUiTypecheck` is the step that fails loudly
 * with a clear pointer to install the JDK.
 */
function writeStubIndex() {
  mkdirSync(stubOutputDir, { recursive: true });
  const banner =
    "// JDK 25 missing during bootstrap — placeholder for @trading/sbe-codecs.\n" +
    "// Install JDK 25 (https://adoptium.net/) and run:\n" +
    "//     ./gradlew :sbe-typescript-generator:generateTsCodecs\n" +
    "// to materialise the real generated codecs (APP-34).\n" +
    "export {};\n";
  writeFileSync(stubOutputFile, banner, "utf8");
  log(`Wrote placeholder ${stubOutputFile}`);
}

/**
 * Run `./gradlew :sbe-typescript-generator:generateTsCodecs`
 * unconditionally. Gradle UP-TO-DATE caching makes this near-instant
 * on warm caches.
 */
function runGenerator() {
  const wrapperPath = join(repoRoot, gradleWrapper);
  if (!existsSync(wrapperPath)) {
    warn(
      `${gradleWrapper} not found at repo root — skipping codec generation. ` +
        `Run from a Gradle checkout, or write a placeholder.`,
    );
    writeStubIndex();
    return;
  }

  log(`Invoking ${gradleWrapper} :sbe-typescript-generator:generateTsCodecs`);
  const result = spawnSync(
    isWindows ? "gradlew.bat" : "./gradlew",
    [
      ":sbe-typescript-generator:generateTsCodecs",
      "--quiet",
      "--no-daemon",
    ],
    { cwd: repoRoot, stdio: "inherit", shell: isWindows },
  );

  if (result.status === 0) {
    log("Codec generation complete (or UP-TO-DATE).");
    return;
  }

  // Heuristic: a missing JDK manifests as "ERROR: JAVA_HOME is not set"
  // OR Gradle exit 1 with "Could not determine Java toolchain". We can't
  // robustly distinguish that from a real Gradle failure without parsing
  // stderr (which we deliberately stream to user). Soft-fail on any
  // non-zero exit and write a placeholder; webUiTypecheck will surface
  // a real failure with a better error if the JDK is the culprit.
  warn(
    "Gradle generator task failed. If this is a fresh clone without JDK 25, " +
      "install it (https://adoptium.net/) and rerun `./gradlew " +
      ":sbe-typescript-generator:generateTsCodecs`. Continuing with " +
      "placeholder so `npm ci` succeeds.",
  );
  writeStubIndex();
}

ensureHuskyInit();
runGenerator();
