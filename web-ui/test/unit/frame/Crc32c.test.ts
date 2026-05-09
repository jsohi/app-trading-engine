/**
 * Crc32c.test.ts — unit tests for the Castagnoli CRC32C implementation.
 *
 * Validates against the five RFC 3720 §B.4 named test vectors, chunk-boundary
 * equivalence (chained seed), empty-input behavior, and the `crc32cOf`
 * two-region convenience function.
 *
 * Threading: single-threaded (Vitest jsdom worker).
 * Allocation: test-only; fixture `Uint8Array` allocations are intentional.
 */

import { describe, expect, it } from "vitest";

import { crc32c, crc32cOf } from "@/workers/frame/Crc32c";

// ─── RFC 3720 §B.4 test vectors ────────────────────────────────────────────

describe("crc32c — RFC 3720 §B.4 named vectors", () => {
  it("crc32c_32BytesOf0x00_equals0x8a9136aa", () => {
    const buf = new Uint8Array(32).fill(0x00);
    expect(crc32c(buf) >>> 0).toBe(0x8a9136aa);
  });

  it("crc32c_32BytesOf0xFF_equals0x62a8ab43", () => {
    const buf = new Uint8Array(32).fill(0xff);
    expect(crc32c(buf) >>> 0).toBe(0x62a8ab43);
  });

  it("crc32c_32AscendingBytes0x00to0x1F_equals0x46dd794e", () => {
    const buf = Uint8Array.from({ length: 32 }, (_, i) => i);
    expect(crc32c(buf) >>> 0).toBe(0x46dd794e);
  });

  it("crc32c_32DescendingBytes0x1Fto0x00_equals0x113fdb5c", () => {
    const buf = Uint8Array.from({ length: 32 }, (_, i) => 0x1f - i);
    expect(crc32c(buf) >>> 0).toBe(0x113fdb5c);
  });

  /**
   * RFC 3720 §B.4 iSCSI 48-byte reference pattern.
   * Exact byte sequence from the RFC; expected CRC = 0xd9963a56.
   */
  it("crc32c_48ByteIscsiPattern_equals0xd9963a56", () => {
    const buf = Uint8Array.from([
      0x01, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00,
      0x00, 0x18, 0x28, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00,
    ]);
    expect(crc32c(buf) >>> 0).toBe(0xd9963a56);
  });
});

// ─── Chunk-boundary equivalence ────────────────────────────────────────────

describe("crc32c — chunk-boundary equivalence (chained seed)", () => {
  it("crc32c_1024ByteBuffer_chunkedAtMidpointEqualsFullPass", () => {
    // Build a deterministic 1024-byte buffer (values are index mod 251, a prime).
    const buf = Uint8Array.from({ length: 1024 }, (_, i) => i % 251);

    const full = crc32c(buf) >>> 0;

    // First half: [0, 512); seed is 0 (default).
    const firstHalfCrc = crc32c(buf, 0, 512, 0);
    // Second half: [512, 1024); chained onto firstHalfCrc as seed.
    const chained = crc32c(buf, 512, 1024, firstHalfCrc) >>> 0;

    expect(chained).toBe(full);
  });

  it("crc32c_arbitraryOffsets_chunkedEqualsFull", () => {
    const buf = Uint8Array.from({ length: 256 }, (_, i) => (i * 37) & 0xff);

    const full = crc32c(buf, 10, 200) >>> 0;
    const firstSeed = crc32c(buf, 10, 100, 0);
    const chained = crc32c(buf, 100, 200, firstSeed) >>> 0;

    expect(chained).toBe(full);
  });
});

// ─── Empty input ────────────────────────────────────────────────────────────

describe("crc32c — empty input", () => {
  it("crc32c_emptyUint8Array_returnsZero", () => {
    expect(crc32c(new Uint8Array(0))).toBe(0);
  });

  it("crc32c_startEqualsEnd_returnsZero", () => {
    const buf = Uint8Array.from([0xde, 0xad, 0xbe, 0xef]);
    expect(crc32c(buf, 2, 2)).toBe(0);
  });
});

// ─── crc32cOf two-region convenience ───────────────────────────────────────

describe("crc32cOf — two-region concatenation equivalence", () => {
  it("crc32cOf_headerAndPayload_equalsManualConcat", () => {
    const header = Uint8Array.from([0x01, 0x02, 0x03, 0x04, 0x05]);
    const payload = Uint8Array.from([0xaa, 0xbb, 0xcc, 0xdd]);

    // Manual concat.
    const concat = new Uint8Array(header.length + payload.length);
    concat.set(header, 0);
    concat.set(payload, header.length);

    const expected = crc32c(concat) >>> 0;
    const actual = crc32cOf(header, header.length, payload, payload.length) >>> 0;

    expect(actual).toBe(expected);
  });

  it("crc32cOf_zeroPayloadLength_equalsHeaderOnly", () => {
    const header = Uint8Array.from([0x10, 0x20, 0x30]);
    const payload = Uint8Array.from([0xff, 0xff]); // length 0 used

    const expected = crc32c(header, 0, header.length) >>> 0;
    const actual = crc32cOf(header, header.length, payload, 0) >>> 0;

    expect(actual).toBe(expected);
  });

  it("crc32cOf_typicalFrameHeaderAndPayload_equalsManualConcat", () => {
    // Simulate a realistic 13-byte frame header + 20-byte payload.
    const header = Uint8Array.from({ length: 13 }, (_, i) => i + 0x11);
    const payload = Uint8Array.from({ length: 20 }, (_, i) => (i * 13) & 0xff);

    const concat = new Uint8Array(33);
    concat.set(header, 0);
    concat.set(payload, 13);

    const expected = crc32c(concat) >>> 0;
    const actual = crc32cOf(header, 13, payload, 20) >>> 0;

    expect(actual).toBe(expected);
  });

  it("crc32cOf_emptyHeaderAndPayload_returnsZero", () => {
    const emptyH = new Uint8Array(0);
    const emptyP = new Uint8Array(0);
    expect(crc32cOf(emptyH, 0, emptyP, 0)).toBe(0);
  });
});
