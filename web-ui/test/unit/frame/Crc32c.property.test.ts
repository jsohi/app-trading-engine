/**
 * Crc32c.property.test.ts — property-based tests for the CRC32C implementation.
 *
 * Uses fast-check to generate ≥ 10 000 random Uint8Array inputs, asserting:
 *  1. Chunk-at-random-midpoint, chained via seed, equals single-shot full pass.
 *  2. Identical inputs always produce identical outputs (determinism).
 *
 * Threading: single-threaded (Vitest jsdom worker).
 * Allocation: test-only — fast-check allocates arbitrary buffers for each case.
 */

import { describe, it } from "vitest";
import { assert, property, uint8Array, nat, integer } from "fast-check";

import { crc32c } from "@/workers/frame/Crc32c";

describe("crc32c — property-based: chunk-equivalence and determinism", () => {
  /**
   * For any random Uint8Array of length 1..1024, splitting at a random
   * midpoint and chaining the two halves via the seed API must produce
   * the same result as a single full-pass call.
   *
   * 10 000 cases; seed 12345 for reproducibility.
   */
  it("crc32c_randomBuffer_chunkedCrcEqualsFullPassCrc", () => {
    assert(
      property(
        // Array of length 1..1024, each element 0..255.
        uint8Array({ minLength: 1, maxLength: 1024 }),
        // Split midpoint: a value in [0, length] (inclusive, so zero-length
        // halves are exercised at the boundaries).
        nat(),
        (bytes, rawMid) => {
          const mid = rawMid % (bytes.length + 1); // clamp to [0, length]

          const full = crc32c(bytes, 0, bytes.length, 0) >>> 0;

          const firstCrc = crc32c(bytes, 0, mid, 0);
          const chained = crc32c(bytes, mid, bytes.length, firstCrc) >>> 0;

          return chained === full;
        },
      ),
      { numRuns: 10_000, seed: 12345 },
    );
  });

  /**
   * Determinism: calling crc32c twice with identical bytes, start, end, and seed
   * always returns the same value.
   */
  it("crc32c_sameInput_returnsSameOutputEachTime", () => {
    assert(
      property(
        uint8Array({ minLength: 0, maxLength: 512 }),
        integer({ min: 0, max: 4 }), // small seed variation
        (bytes, seedBase) => {
          const seed = seedBase * 0x11223344;
          const r1 = crc32c(bytes, 0, bytes.length, seed) >>> 0;
          const r2 = crc32c(bytes, 0, bytes.length, seed) >>> 0;
          return r1 === r2;
        },
      ),
      { numRuns: 10_000, seed: 12345 },
    );
  });

  /**
   * Three-chunk split: splitting a buffer at two random midpoints and chaining
   * three segments must still equal the single-pass result.
   * Provides additional coverage of the slicing-by-8 + tail boundary code.
   */
  it("crc32c_randomBuffer_threeChunksChainedEqualsFullPass", () => {
    assert(
      property(uint8Array({ minLength: 1, maxLength: 1024 }), nat(), nat(), (bytes, rawA, rawB) => {
        // Ensure a <= b <= length by sorting.
        const len = bytes.length;
        const a = rawA % (len + 1);
        const b = rawB % (len + 1);
        const lo = Math.min(a, b);
        const hi = Math.max(a, b);

        const full = crc32c(bytes, 0, len, 0) >>> 0;

        const c1 = crc32c(bytes, 0, lo, 0);
        const c2 = crc32c(bytes, lo, hi, c1);
        const c3 = crc32c(bytes, hi, len, c2) >>> 0;

        return c3 === full;
      }),
      { numRuns: 5_000, seed: 99999 },
    );
  });
});
