/**
 * GoldenFrame.test.ts — locks byte-exact frame format against a hand-decoded
 * fixture before any session-layer abstraction can mask a frame bug.
 *
 * This is C4.5 in the APP-36 commit topology: a single hand-curated frame
 * pinned to a checked-in hex fixture so a future code change that breaks
 * the wire encoding (or breaks `FrameParser` / `FrameEncoder` consistency)
 * fires here loudly rather than surfacing as a downstream test failure.
 *
 * The golden fixture is a 25-byte reliable WebSocketHeartbeat-style frame:
 *
 *   - totalLength u32 LE  = 25  (0x19, 0x00, 0x00, 0x00)
 *   - seqNo i64 LE        = 1   (0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00)
 *   - flags u8            = 0x01 (FLAG_RELIABLE)
 *   - crc32c u32 LE       = computed over header[0..12] || payload  (verified by hand below)
 *   - payload (8 bytes)   = 0x01 0x02 ... 0x08 (a synthetic SBE-shaped body)
 *
 * Threading: single-threaded (Vitest jsdom).
 * Allocation: trivial (one Uint8Array per test).
 *
 * Plan reference: APP-36 §B.6 row C4.5 / §2.1 / §2.2.
 */

import { describe, expect, it } from "vitest";

import { crc32cOf } from "@/workers/frame/Crc32c";
import { encodeReliable } from "@/workers/frame/FrameEncoder";
import {
  BEST_EFFORT_HEADER_SIZE as BEH,
  FLAG_RELIABLE,
  RELIABLE_HEADER_SIZE as RH,
} from "@/workers/frame/Flags";
import { FrameParser, type ParsedFrame } from "@/workers/frame/FrameParser";

/**
 * Hand-decoded golden fixture. Hex laid out byte-by-byte so a code reviewer
 * can verify the wire layout against `FrameParser.java` without running code.
 *
 *   bytes 0..3   : totalLength = 25 (little-endian)
 *   bytes 4..11  : seqNo = 1 (little-endian)
 *   byte  12     : flags = 0x01 (reliable)
 *   bytes 13..16 : crc32c (computed below — pinned via assertion)
 *   bytes 17..24 : 8-byte payload 0x01..0x08
 */
const GOLDEN_PAYLOAD = Uint8Array.from([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
const GOLDEN_SEQ_NO = 1n;
const GOLDEN_FLAGS = FLAG_RELIABLE; // 0x01
const GOLDEN_TOTAL_LENGTH = RH + GOLDEN_PAYLOAD.length; // 17 + 8 = 25

/** Header bytes 0..12 (without CRC). */
function buildHeaderNoCrc(): Uint8Array {
  const out = new Uint8Array(BEH);
  const view = new DataView(out.buffer);
  view.setUint32(0, GOLDEN_TOTAL_LENGTH, true);
  view.setBigInt64(4, GOLDEN_SEQ_NO, true);
  out[12] = GOLDEN_FLAGS;
  return out;
}

describe("GoldenFrame — byte-exact wire format pin", () => {
  it("goldenFrame_handDecodedFixture_matchesFrameEncoderOutput", () => {
    // 1. Compute the CRC by hand using the documented region:
    //    crc32cOf(header[0..12], 13, payload, len) per §2.2.
    const header = buildHeaderNoCrc();
    const expectedCrc = crc32cOf(header, BEH, GOLDEN_PAYLOAD, GOLDEN_PAYLOAD.length) >>> 0;

    // 2. Build the full 25-byte fixture by hand.
    const goldenFixture = new Uint8Array(GOLDEN_TOTAL_LENGTH);
    goldenFixture.set(header, 0);
    new DataView(goldenFixture.buffer).setUint32(13, expectedCrc, true);
    goldenFixture.set(GOLDEN_PAYLOAD, RH);

    // 3. FrameEncoder must emit exactly the same 25 bytes.
    const encoded = encodeReliable(GOLDEN_SEQ_NO, GOLDEN_PAYLOAD, GOLDEN_FLAGS);

    expect(encoded).toEqual(goldenFixture);
    expect(encoded).toHaveLength(GOLDEN_TOTAL_LENGTH);
    // The byte at offset 12 MUST equal the flags constant — pin against future
    // accidental flag-byte placement drift.
    expect(encoded[12]).toBe(GOLDEN_FLAGS);
    // Total-length field MUST be little-endian uint32.
    expect(encoded[0]).toBe(GOLDEN_TOTAL_LENGTH & 0xff);
    expect(encoded[1]).toBe(0x00);
    expect(encoded[2]).toBe(0x00);
    expect(encoded[3]).toBe(0x00);
  });

  it("goldenFrame_parserDecodesEncoderOutput_payloadAndHeaderExact", () => {
    // Round-trip through FrameParser: the payload bytes, seqNo, flags, and
    // totalLength must all match the hand-decoded values verbatim.
    const encoded = encodeReliable(GOLDEN_SEQ_NO, GOLDEN_PAYLOAD, GOLDEN_FLAGS);

    let parsed: ParsedFrame | null = null;
    let errorCode: string | null = null;
    const parser = new FrameParser({
      onFrame: (f) =>
        (parsed = {
          totalLength: f.totalLength,
          seqNo: f.seqNo,
          flags: f.flags,
          payload: f.payload.slice(),
        }),
      onError: (code) => (errorCode = code),
    });
    parser.feed(encoded);

    expect(errorCode).toBeNull();
    expect(parsed).not.toBeNull();
    const got = parsed as unknown as ParsedFrame;
    expect(got.totalLength).toBe(GOLDEN_TOTAL_LENGTH);
    expect(got.seqNo).toBe(GOLDEN_SEQ_NO);
    expect(got.flags).toBe(GOLDEN_FLAGS);
    expect(got.payload).toEqual(GOLDEN_PAYLOAD);
  });

  it("goldenFrame_corruptedPayloadByte_failsCrcMismatch", () => {
    // Flip one bit in the payload after encoding. The CRC must catch it.
    const encoded = encodeReliable(GOLDEN_SEQ_NO, GOLDEN_PAYLOAD, GOLDEN_FLAGS);
    encoded[RH + 3] = (encoded[RH + 3] ?? 0) ^ 0x01;

    let errorCode: string | null = null;
    const parser = new FrameParser({
      onFrame: () => {
        /* should not fire */
      },
      onError: (code) => (errorCode = code),
    });
    parser.feed(encoded);

    expect(errorCode).toBe("CRC_MISMATCH");
  });
});
