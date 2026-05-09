/**
 * Smoke bench — validates the harness can run inside the configured
 * test environment. The C9 plan §5.8.2 mandates this validation gate
 * so a breakage in the tinybench-in-browser path surfaces immediately
 * rather than as a confusing decode-bench failure.
 *
 * Threading: test environment.
 *
 * Allocation: trivial.
 *
 * Plan reference: §5.8.2.
 */

import { it, expect } from "vitest";

import { runBench } from "./_harness";

it.skipIf(process.env.SKIP_BENCH === "true")(
  "harness smoke — tinybench runs and reports stats",
  async () => {
    const result = await runBench("smoke", (bench) => {
      bench.add("noop", () => {
        // intentional no-op
      });
    });

    expect(result.tasks).toHaveLength(1);
    const task = result.tasks[0];
    expect(task).toBeDefined();
    if (task === undefined) return;
    expect(task.name).toBe("noop");
    expect(task.throughputPerSec).toBeGreaterThan(0);
  },
  30_000,
);
