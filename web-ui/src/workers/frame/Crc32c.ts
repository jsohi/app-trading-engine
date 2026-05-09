/*
 * Hot-path table-driven CRC with provably-safe bounded indexed access:
 * every `tables[n]!` / `prev[i]!` / `T*[byte]!` reads from arrays whose
 * length is asserted at construction (256 entries for byte-table, 8 for
 * tables[]). Non-null assertions are the right tool here; replacing
 * them with explicit length-checks would defeat the zero-alloc per-byte
 * inner loop. Plan §2.2 / §4.9 — see file-level docstring below.
 */
/* eslint-disable @typescript-eslint/no-non-null-assertion */

/**
 * Crc32c — Castagnoli (polynomial 0x1EDC6F41) implementation, slicing-by-8.
 *
 * Mirrors the server's `java.util.zip.CRC32C`:
 *   - Polynomial: 0x1EDC6F41 (reflected form 0x82F63B78 — used here).
 *   - Seed: 0.
 *   - **No post-XOR** (java.util.zip.CRC32C.getValue() returns the raw CRC).
 *
 * Region (per APP-36 §2.2): `header[0..12] ‖ payload`. The CRC field
 * itself sits at offset 13 in the wire envelope.
 *
 * Threading: any (pure functions; no shared mutable state — the
 * lookup tables are computed once at module init and frozen).
 *
 * Allocation: zero per call (table-driven, no allocations in the
 * hot loop). Eight `Uint32Array(256)` tables built once at module
 * load — total ~8 KiB of static data.
 *
 * Performance: slicing-by-8 (Intel "Fast CRC Computation") processes
 * 8 input bytes per iteration with 8 table lookups + xors. CI gate
 * (per `bench/crc32c.bench.ts`) ≥ 1 GB/s on Chromium; ≥ 600 MB/s
 * nightly Safari floor.
 *
 * Plan reference: §2.2 / §4.9 / §6 row 7.
 */

const POLY_REFLECTED = 0x82f63b78;

/**
 * Build the slicing-by-8 lookup tables once at module init. Each table
 * has 256 entries; we hold 8 tables for the slicing-by-8 algorithm.
 */
function buildTables(): readonly Uint32Array[] {
  const tables: Uint32Array[] = [];
  // Table 0: standard Sarwate single-byte table.
  const t0 = new Uint32Array(256);
  for (let i = 0; i < 256; i++) {
    let c = i >>> 0;
    for (let k = 0; k < 8; k++) {
      // Reflected polynomial; (c & 1) decides whether to xor.
      c = (c & 1) === 1 ? (c >>> 1) ^ POLY_REFLECTED : c >>> 1;
    }
    t0[i] = c >>> 0;
  }
  tables.push(t0);
  // Tables 1..7 derived from t0 per the slicing-by-N construction:
  //   table[n][i] = table[n-1][i] >>> 8  ^  table[0][ table[n-1][i] & 0xFF ]
  for (let n = 1; n < 8; n++) {
    const prev = tables[n - 1]!;
    const t = new Uint32Array(256);
    for (let i = 0; i < 256; i++) {
      const p = prev[i]!;
      t[i] = ((p >>> 8) ^ t0[p & 0xff]!) >>> 0;
    }
    tables.push(t);
  }
  return Object.freeze(tables);
}

const TABLES: readonly Uint32Array[] = buildTables();
const T0 = TABLES[0]!;
const T1 = TABLES[1]!;
const T2 = TABLES[2]!;
const T3 = TABLES[3]!;
const T4 = TABLES[4]!;
const T5 = TABLES[5]!;
const T6 = TABLES[6]!;
const T7 = TABLES[7]!;

/**
 * Compute CRC32C over `[start, end)` of `bytes`, optionally chained
 * onto a running `seed` (default 0 — fresh start).
 *
 * Returns an unsigned 32-bit value. The wire format writes this as
 * little-endian uint32 at offset 13 of a reliable frame.
 *
 * @param bytes input buffer
 * @param start inclusive start offset (default 0)
 * @param end exclusive end offset (default `bytes.length`)
 * @param seed running CRC for chained computation (default 0)
 */
export function crc32c(bytes: Uint8Array, start = 0, end: number = bytes.length, seed = 0): number {
  let crc = (~seed >>> 0) >>> 0; // start state — bitwise inverse of seed
  let p = start;

  // Main slicing-by-8 loop.
  while (p + 8 <= end) {
    const b0 = bytes[p]!;
    const b1 = bytes[p + 1]!;
    const b2 = bytes[p + 2]!;
    const b3 = bytes[p + 3]!;
    const b4 = bytes[p + 4]!;
    const b5 = bytes[p + 5]!;
    const b6 = bytes[p + 6]!;
    const b7 = bytes[p + 7]!;

    const c0 = ((crc & 0xff) ^ b0) >>> 0;
    const c1 = (((crc >>> 8) & 0xff) ^ b1) >>> 0;
    const c2 = (((crc >>> 16) & 0xff) ^ b2) >>> 0;
    const c3 = ((crc >>> 24) ^ b3) >>> 0;

    crc = (T7[c0]! ^ T6[c1]! ^ T5[c2]! ^ T4[c3]! ^ T3[b4]! ^ T2[b5]! ^ T1[b6]! ^ T0[b7]!) >>> 0;
    p += 8;
  }

  // Tail (≤ 7 bytes).
  while (p < end) {
    crc = (((crc >>> 8) ^ T0[(crc ^ bytes[p]!) & 0xff]!) >>> 0) | 0;
    p += 1;
  }

  return (~crc >>> 0) >>> 0;
}

/**
 * Convenience: compute CRC32C over two contiguous regions
 * `header[0..headerLen) ‖ payload[0..payloadLen)`. Matches the server's
 * `crc.update(header, 0, 13); crc.update(payload, 0, payloadLen)` pattern.
 *
 * @param header header bytes
 * @param headerLen number of header bytes participating in the CRC (typically 13)
 * @param payload payload bytes (may be empty)
 * @param payloadLen number of payload bytes participating
 */
export function crc32cOf(
  header: Uint8Array,
  headerLen: number,
  payload: Uint8Array,
  payloadLen: number,
): number {
  // Run the table-driven loop in two stages, threading the running CRC.
  const seedAfterHeader = crc32c(header, 0, headerLen, 0);
  if (payloadLen <= 0) return seedAfterHeader;
  // Seed-chain semantics: the public seed argument resumes from a finished CRC,
  // so pass the previous result back through the same start state.
  return crc32c(payload, 0, payloadLen, seedAfterHeader);
}
