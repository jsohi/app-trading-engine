/**
 * crc32c.bench.ts — throughput regression gate per APP-36 §4.9.
 *
 * Gates:
 *   - Chromium CI: ≥ 1 GB/s (CI fail < 800 MB/s)
 *   - Safari nightly: ≥ 600 MB/s
 *
 * Plan reference: §2.2 / §4.9 / §6 row 7.
 */

import { it, expect } from "vitest";

import { crc32c } from "@/workers/frame/Crc32c";

import { runBench } from "./_harness";

const BUFFER_BYTES = 4 * 1024 * 1024;
const TARGET_THROUGHPUT_BYTES_PER_SEC_CHROMIUM = 1_000_000_000; // ≥ 1 GB/s
const FAIL_THRESHOLD_BYTES_PER_SEC = 800_000_000; // CI fail below this

it.skipIf(process.env.SKIP_BENCH === "true")(
  "crc32c slicing-by-8 throughput >= 1 GB/s on Chromium / >= 800 MB/s gate",
  async () => {
    const buf = new Uint8Array(BUFFER_BYTES);
    // Pseudo-random fill (deterministic; no Math.random dependency).
    for (let i = 0; i < buf.length; i++) buf[i] = (i * 31 + 7) & 0xff;

    const result = await runBench("crc32c-slicing-by-8", (bench) => {
      bench.add("crc32c(4 MiB)", () => {
        crc32c(buf);
      });
    });

    const task = result.tasks[0];
    expect(task).toBeDefined();
    if (task === undefined) return;
    const throughputBytesPerSec = task.throughputPerSec * BUFFER_BYTES;
    console.log(
      `crc32c throughput: ${(throughputBytesPerSec / 1e9).toFixed(2)} GB/s (target ${(TARGET_THROUGHPUT_BYTES_PER_SEC_CHROMIUM / 1e9).toFixed(2)} GB/s)`,
    );
    expect(throughputBytesPerSec).toBeGreaterThanOrEqual(FAIL_THRESHOLD_BYTES_PER_SEC);
  },
  60_000,
);
