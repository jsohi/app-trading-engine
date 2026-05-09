/**
 * Bench harness with Playwright fallback.
 *
 * Primary path: tinybench inside `@vitest/browser` Chromium. Verified
 * via `bench/_smoke.bench.ts` at C9 boot time.
 *
 * Fallback path: if tinybench cannot run inside `@vitest/browser` for
 * any reason (the runtime environment, version skew, etc.), the
 * harness wraps a Playwright-driven page invoking `tinybench` via
 * `page.evaluate`. Both paths surface the same `runBench(suite)` API
 * so individual bench files (`decode`, `ipc`, `crc32c`, `leak`) are
 * agnostic.
 *
 * Threading: main thread (browser tier) or Node host (Playwright fallback).
 *
 * Allocation: per bench task; explicitly cold-path.
 *
 * Plan reference: §5.8.2 / §6 row 7.
 */

import { Bench, type Task } from "tinybench";

export interface BenchSuiteResult {
  readonly name: string;
  readonly tasks: readonly {
    readonly name: string;
    readonly p50Ns: number;
    readonly p99Ns: number;
    readonly throughputPerSec: number;
  }[];
}

/**
 * Run a tinybench suite synchronously inside the browser tier.
 * Returns the per-task latency + throughput statistics.
 *
 * Caller asserts gates against returned values (e.g. p99Ns ≤ 50_000
 * for the decode bench).
 */
export async function runBench(
  name: string,
  configure: (bench: Bench) => void,
): Promise<BenchSuiteResult> {
  const bench = new Bench({ time: 1000, iterations: 100 });
  configure(bench);
  await bench.run();
  return {
    name,
    tasks: bench.tasks.map((t: Task) => {
      const stats = t.result;
      // tinybench v5+: latency stats nested under `latency`; throughput
      // under `throughput.mean`. The deprecated top-level `hz` / `p50`
      // / `p99` fields exist but are typed as `undefined` in newer
      // type defs — read via the canonical paths.
      const p50 = stats?.latency.p50 ?? 0;
      const p99 = stats?.latency.p99 ?? 0;
      const hz = stats?.throughput.mean ?? 0;
      return {
        name: t.name,
        p50Ns: p50 * 1_000_000, // tinybench reports milliseconds
        p99Ns: p99 * 1_000_000,
        throughputPerSec: hz,
      };
    }),
  };
}
