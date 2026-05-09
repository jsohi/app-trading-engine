/**
 * FrameEncoder.test.ts — unit tests for the outbound frame encoder.
 *
 * Round-trip tests use `FrameParser` as the oracle. Error-path tests verify
 * that the encoder rejects out-of-contract inputs without performing any
 * partial encoding.
 *
 * Threading: single-threaded (Vitest jsdom worker).
 * Allocation: test-only — frames allocated in fixture helpers.
 */

import { describe, expect, it } from "vitest";

import { encodeBestEffort, encodeReliable } from "@/workers/frame/FrameEncoder";
import {
  FrameParser,
  type ParsedFrame,
  type FrameParseErrorCode,
} from "@/workers/frame/FrameParser";
import {
  FLAG_RELIABLE,
  BEST_EFFORT_HEADER_SIZE as BEH,
  RELIABLE_HEADER_SIZE as RH,
} from "@/workers/frame/Flags";
import { MAX_FRAME_BYTES } from "@/workers/WorkerTuning";

// ─── Helpers ─────────────────────────────────────────────────────────────────

function roundTripParse(frame: Uint8Array): {
  parsed: ParsedFrame | null;
  error: { code: FrameParseErrorCode; message: string } | null;
} {
  let parsed: ParsedFrame | null = null;
  let error: { code: FrameParseErrorCode; message: string } | null = null;

  const parser = new FrameParser({
    onFrame: (f) => {
      parsed = {
        totalLength: f.totalLength,
        seqNo: f.seqNo,
        flags: f.flags,
        payload: f.payload.slice(),
      };
    },
    onError: (code, message) => {
      error = { code, message };
    },
  });

  parser.feed(frame);
  return { parsed, error };
}

function makePayload(len: number): Uint8Array {
  return Uint8Array.from({ length: len }, (_, i) => (i * 17 + 5) & 0xff);
}

// ─── encodeBestEffort — round-trip ───────────────────────────────────────────

describe("encodeBestEffort — round-trip via FrameParser", () => {
  it("encodeBestEffort_emptyPayload_roundTripsCorrectly", () => {
    const frame = encodeBestEffort(new Uint8Array(0));
    const { parsed, error } = roundTripParse(frame);

    expect(error).toBeNull();
    expect(parsed).not.toBeNull();
    expect(parsed!.flags).toBe(0x00);
    expect(parsed!.seqNo).toBe(0n);
    expect(parsed!.totalLength).toBe(BEH);
    expect(parsed!.payload).toHaveLength(0);
  });

  it("encodeBestEffort_nonEmptyPayload_allBytesPreserved", () => {
    const payload = makePayload(64);
    const frame = encodeBestEffort(payload);
    const { parsed, error } = roundTripParse(frame);

    expect(error).toBeNull();
    expect(parsed).not.toBeNull();
    expect(parsed!.payload).toEqual(payload);
    expect(parsed!.totalLength).toBe(BEH + 64);
  });

  it("encodeBestEffort_snapshotFlags_roundTripsWithCorrectFlags", () => {
    const payload = makePayload(10);
    const frame = encodeBestEffort(payload, 0x04); // FLAG_SNAPSHOT
    const { parsed, error } = roundTripParse(frame);

    expect(error).toBeNull();
    expect(parsed).not.toBeNull();
    expect(parsed!.flags).toBe(0x04);
  });
});

// ─── encodeBestEffort — error paths ──────────────────────────────────────────

describe("encodeBestEffort — rejects invalid inputs", () => {
  it("encodeBestEffort_withFlagReliable_throws", () => {
    expect(() => encodeBestEffort(new Uint8Array(4), FLAG_RELIABLE)).toThrow(
      /FLAG_RELIABLE must not be set/,
    );
  });

  it("encodeBestEffort_payloadExceedingMaxFrameBytes_throws", () => {
    // Payload that makes totalLength = MAX_FRAME_BYTES + 1.
    const oversizeLen = MAX_FRAME_BYTES - BEH + 1;
    // Avoid actually allocating 4 MiB in the test — we just need the call to throw.
    // Use a fake Uint8Array by crafting a minimal object (length check is all that's
    // done pre-allocation).
    const fakePayload = { length: oversizeLen } as Uint8Array;
    expect(() => encodeBestEffort(fakePayload)).toThrow(/exceeds MAX_FRAME_BYTES/);
  });
});

// ─── encodeReliable — round-trip ─────────────────────────────────────────────

describe("encodeReliable — round-trip via FrameParser", () => {
  it("encodeReliable_zeroPayload_roundTrips", () => {
    const frame = encodeReliable(1n, new Uint8Array(0), FLAG_RELIABLE);
    const { parsed, error } = roundTripParse(frame);

    expect(error).toBeNull();
    expect(parsed).not.toBeNull();
    expect(parsed!.flags & FLAG_RELIABLE).toBeTruthy();
    expect(parsed!.seqNo).toBe(1n);
    expect(parsed!.totalLength).toBe(RH);
    expect(parsed!.payload).toHaveLength(0);
  });

  it("encodeReliable_nonEmptyPayload_allBytesPreservedAndCrcVerified", () => {
    const payload = makePayload(100);
    const frame = encodeReliable(999n, payload, FLAG_RELIABLE);
    const { parsed, error } = roundTripParse(frame);

    // No CRC error means the CRC was valid.
    expect(error).toBeNull();
    expect(parsed).not.toBeNull();
    expect(parsed!.payload).toEqual(payload);
    expect(parsed!.seqNo).toBe(999n);
    expect(parsed!.totalLength).toBe(RH + 100);
  });

  it("encodeReliable_largeSeqNo_roundTrips", () => {
    const payload = makePayload(8);
    const seqNo = BigInt(Number.MAX_SAFE_INTEGER) + 1n;
    const frame = encodeReliable(seqNo, payload, FLAG_RELIABLE);
    const { parsed, error } = roundTripParse(frame);

    expect(error).toBeNull();
    expect(parsed!.seqNo).toBe(seqNo);
  });
});

// ─── encodeReliable — error paths ────────────────────────────────────────────

describe("encodeReliable — rejects invalid inputs", () => {
  it("encodeReliable_missingFlagReliable_throws", () => {
    // The runtime guard must fire when FLAG_RELIABLE is absent. We bypass the
    // TypeScript literal-type check with an explicit call-site wrapper so the
    // test remains a true runtime assertion and does not silently become dead
    // code if the guard is ever removed.
    // Cast through `unknown` then to a permissive function type so we can
    // pass `0` (typed `0` literal) where the public signature defaults to
    // `FLAG_RELIABLE`. `as unknown as` keeps the lint rule happy versus
    // `as any` while still bypassing the literal-default constraint.
    const looseEncode = encodeReliable as unknown as (
      seqNo: bigint,
      payload: Uint8Array,
      flags: number,
    ) => Uint8Array;
    const callWithZeroFlags = (): Uint8Array => looseEncode(1n, new Uint8Array(4), 0x00);
    expect(callWithZeroFlags).toThrow(/FLAG_RELIABLE must be set/);
  });

  it("encodeReliable_payloadExceedingMaxFrameBytes_throws", () => {
    const oversizeLen = MAX_FRAME_BYTES - RH + 1;
    const fakePayload = { length: oversizeLen } as Uint8Array;
    expect(() => encodeReliable(1n, fakePayload, FLAG_RELIABLE)).toThrow(/exceeds MAX_FRAME_BYTES/);
  });
});
