/**
 * FrameParser.test.ts — unit tests for the wire-envelope pull-parser.
 *
 * Uses `FrameEncoder` to build well-formed binary frames and feeds them
 * into `FrameParser` via `parser.feed(...)`. Assertions target the
 * `onFrame` and `onError` callbacks.
 *
 * Test naming follows the `<unit>_<scenario>_<expected>` convention as
 * required by plan §5.8.
 *
 * Threading: single-threaded (Vitest jsdom worker).
 * Allocation: test-only — frames allocated in fixture helpers.
 */

import { describe, expect, it } from "vitest";

import { encodeBestEffort, encodeReliable } from "@/workers/frame/FrameEncoder";
import {
  FrameParser,
  type FrameParseErrorCode,
  type ParsedFrame,
} from "@/workers/frame/FrameParser";
import { FLAG_RELIABLE } from "@/workers/frame/Flags";
import { MAX_FRAME_BYTES, RING_BYTES } from "@/workers/WorkerTuning";

// ─── Helpers ────────────────────────────────────────────────────────────────

/** Build a parser with Jest-compatible spy callbacks. */
function makeParser(): {
  parser: FrameParser;
  frames: ParsedFrame[];
  errors: Array<{ code: FrameParseErrorCode; message: string }>;
  onFrame: (f: ParsedFrame) => void;
  onError: (code: FrameParseErrorCode, message: string) => void;
} {
  const frames: ParsedFrame[] = [];
  const errors: Array<{ code: FrameParseErrorCode; message: string }> = [];

  // Capture a copy of the payload immediately (the view is invalidated on
  // the next feed(); we must snapshot it synchronously in the callback).
  const onFrame = (f: ParsedFrame): void => {
    frames.push({
      totalLength: f.totalLength,
      seqNo: f.seqNo,
      flags: f.flags,
      payload: f.payload.slice(), // copy while the view is valid
    });
  };
  const onError = (code: FrameParseErrorCode, message: string): void => {
    errors.push({ code, message });
  };

  return { parser: new FrameParser({ onFrame, onError }), frames, errors, onFrame, onError };
}

/** Build an arbitrary deterministic payload of the given length. */
function makePayload(len: number): Uint8Array {
  return Uint8Array.from({ length: len }, (_, i) => i & 0xff);
}

// ─── Happy-path tests ────────────────────────────────────────────────────────

describe("FrameParser — happy path", () => {
  it("feed_validBestEffortFrame_emitsOnFrame", () => {
    const { parser, frames, errors } = makeParser();
    const payload = makePayload(8);
    const frame = encodeBestEffort(payload, 0);

    parser.feed(frame);

    expect(errors).toHaveLength(0);
    expect(frames).toHaveLength(1);
    const f = frames[0]!;
    expect(f.flags).toBe(0x00);
    expect(f.seqNo).toBe(0n);
    expect(f.totalLength).toBe(13 + 8);
    expect(f.payload).toEqual(payload);
  });

  it("feed_validReliableFrame_emitsOnFrameWithCrcVerified", () => {
    const { parser, frames, errors } = makeParser();
    const payload = makePayload(20);
    const frame = encodeReliable(1n, payload, FLAG_RELIABLE);

    parser.feed(frame);

    expect(errors).toHaveLength(0);
    expect(frames).toHaveLength(1);
    const f = frames[0]!;
    expect(f.flags & FLAG_RELIABLE).toBeTruthy();
    expect(f.seqNo).toBe(1n);
    expect(f.totalLength).toBe(17 + 20);
    expect(f.payload).toEqual(payload);
  });

  it("feed_zeroPayloadReliableFrame_totalLength17_accepted", () => {
    const { parser, frames, errors } = makeParser();
    const frame = encodeReliable(42n, new Uint8Array(0), FLAG_RELIABLE);

    parser.feed(frame);

    expect(errors).toHaveLength(0);
    expect(frames).toHaveLength(1);
    expect(frames[0]!.totalLength).toBe(17);
    expect(frames[0]!.payload).toHaveLength(0);
  });
});

// ─── Protocol-violation rejection tests ─────────────────────────────────────

describe("FrameParser — protocol violations", () => {
  it("feed_totalLengthBelow13_rejectsProtocolViolation", () => {
    const { parser, errors } = makeParser();
    // Craft a raw 4-byte buffer with totalLength = 12 (< 13).
    const buf = new Uint8Array(13);
    const view = new DataView(buf.buffer);
    view.setUint32(0, 12, true);
    // Set valid flags byte to avoid that check firing first.
    buf[12] = 0x00;

    parser.feed(buf);

    expect(errors).toHaveLength(1);
    expect(errors[0]!.code).toBe("PROTOCOL_VIOLATION");
  });

  it("feed_totalLength0xFFFFFFFF_rejectedPreAllocation", () => {
    const { parser, errors } = makeParser();
    // totalLength = 0xFFFFFFFF > MAX_FRAME_BYTES → PROTOCOL_VIOLATION before any alloc.
    const buf = new Uint8Array(13);
    const view = new DataView(buf.buffer);
    view.setUint32(0, 0xffffffff, true);
    buf[12] = 0x00;

    parser.feed(buf);

    expect(errors).toHaveLength(1);
    expect(errors[0]!.code).toBe("PROTOCOL_VIOLATION");
    expect(parser.isDead()).toBe(true);
  });

  it("feed_oversizeFrame_rejectsProtocolViolation", () => {
    const { parser, errors } = makeParser();
    // totalLength = MAX_FRAME_BYTES + 1 → PROTOCOL_VIOLATION.
    const buf = new Uint8Array(13);
    const view = new DataView(buf.buffer);
    view.setUint32(0, MAX_FRAME_BYTES + 1, true);
    buf[12] = 0x00;

    parser.feed(buf);

    expect(errors).toHaveLength(1);
    expect(errors[0]!.code).toBe("PROTOCOL_VIOLATION");
  });

  it("feed_reservedFlagBitSet_rejectsProtocolViolation", () => {
    const { parser, errors } = makeParser();
    // totalLength = 13, flags = 0x10 (reserved bit 4).
    const buf = new Uint8Array(13);
    const view = new DataView(buf.buffer);
    view.setUint32(0, 13, true);
    view.setBigInt64(4, 0n, true);
    buf[12] = 0x10; // reserved bit 4

    parser.feed(buf);

    expect(errors).toHaveLength(1);
    expect(errors[0]!.code).toBe("PROTOCOL_VIOLATION");
  });

  it("feed_invalidFlagCombo_rejectsProtocolViolation", () => {
    const { parser, errors } = makeParser();
    // flags = 0x02 is not on the valid combo whitelist.
    const buf = new Uint8Array(13);
    const view = new DataView(buf.buffer);
    view.setUint32(0, 13, true);
    view.setBigInt64(4, 0n, true);
    buf[12] = 0x02;

    parser.feed(buf);

    expect(errors).toHaveLength(1);
    expect(errors[0]!.code).toBe("PROTOCOL_VIOLATION");
  });

  it("feed_reliableTotalLengthBelow17_rejectsProtocolViolation", () => {
    const { parser, errors } = makeParser();
    // flags = 0x01 (reliable) but totalLength = 16 (< 17).
    const buf = new Uint8Array(16);
    const view = new DataView(buf.buffer);
    view.setUint32(0, 16, true);
    view.setBigInt64(4, 0n, true);
    buf[12] = 0x01; // FLAG_RELIABLE

    parser.feed(buf);

    expect(errors).toHaveLength(1);
    expect(errors[0]!.code).toBe("PROTOCOL_VIOLATION");
  });

  it("feed_crcMismatchReliable_rejectsCrcMismatch", () => {
    const { parser, errors } = makeParser();
    const payload = makePayload(10);
    const frame = encodeReliable(1n, payload, FLAG_RELIABLE);

    // Corrupt one byte of the CRC field at offset 13.
    // Use the DataView API to satisfy strict-null checks on indexed Uint8Array access.
    const frameView = new DataView(frame.buffer);
    frameView.setUint8(13, (frameView.getUint8(13) ^ 0xff) & 0xff);

    parser.feed(frame);

    expect(errors).toHaveLength(1);
    expect(errors[0]!.code).toBe("CRC_MISMATCH");
    expect(parser.isDead()).toBe(true);
  });
});

// ─── Streaming / partial-feed tests ─────────────────────────────────────────

describe("FrameParser — streaming and multi-frame", () => {
  it("feed_partialFrame_redrivenAfterMoreBytes_emitsOnFrame", () => {
    const { parser, frames, errors } = makeParser();
    const payload = makePayload(16);
    const frame = encodeBestEffort(payload, 0);

    // Feed in two halves.
    const half = Math.floor(frame.length / 2);
    parser.feed(frame.subarray(0, half));
    expect(frames).toHaveLength(0);

    parser.feed(frame.subarray(half));
    expect(errors).toHaveLength(0);
    expect(frames).toHaveLength(1);
    expect(frames[0]!.payload).toEqual(payload);
  });

  it("feed_multipleFramesInOneFeed_emitsAllInOrder", () => {
    const { parser, frames, errors } = makeParser();
    const payload1 = makePayload(4);
    const payload2 = makePayload(8);
    const payload3 = makePayload(12);

    const f1 = encodeBestEffort(payload1, 0);
    const f2 = encodeBestEffort(payload2, 0);
    const f3 = encodeBestEffort(payload3, 0);

    const combined = new Uint8Array(f1.length + f2.length + f3.length);
    combined.set(f1, 0);
    combined.set(f2, f1.length);
    combined.set(f3, f1.length + f2.length);

    parser.feed(combined);

    expect(errors).toHaveLength(0);
    expect(frames).toHaveLength(3);
    expect(frames[0]!.payload).toEqual(payload1);
    expect(frames[1]!.payload).toEqual(payload2);
    expect(frames[2]!.payload).toEqual(payload3);
  });

  /**
   * Feed two segments slowly enough to force the ring-buffer compaction path.
   * The first feed fills the ring near the end; the second triggers compaction.
   */
  it("feed_ringTwoSegmentWrap_emitsOnFrameAcrossCompaction", () => {
    const { parser, frames, errors } = makeParser();

    // Build a small frame that will sit near the end of the ring.
    const payloadA = makePayload(4);
    const frameA = encodeBestEffort(payloadA, 0);

    // Feed it byte-by-byte to produce compaction: first consume it fully, then
    // feed another frame straddling the wrap.
    parser.feed(frameA);
    expect(frames).toHaveLength(1);

    // Now feed a second frame; after the first was consumed the parser reset to
    // offset 0, so a subsequent write has to work correctly.
    const payloadB = makePayload(8);
    const frameB = encodeBestEffort(payloadB, 0);

    // Send the second frame split across two feeds to exercise the compaction path.
    const split = 7; // less than the 13-byte header
    parser.feed(frameB.subarray(0, split));
    expect(frames).toHaveLength(1); // still only A

    parser.feed(frameB.subarray(split));
    expect(errors).toHaveLength(0);
    expect(frames).toHaveLength(2);
    expect(frames[1]!.payload).toEqual(payloadB);
  });

  /**
   * Feed more data than RING_BYTES allows without completing any frame.
   * The parser must emit BUFFER_OVERFLOW and become dead.
   *
   * Strategy: build one partial frame whose header announces a large
   * `totalLength` (MAX_FRAME_BYTES — the entire ring). The parser sees
   * the valid header and waits for the remaining payload bytes. We then
   * keep feeding small payload chunks that never finish the frame; after
   * the ring fills, BUFFER_OVERFLOW fires.
   */
  it("feed_ringFull_closesBufferOverflow", () => {
    const { parser, errors } = makeParser();

    // Construct a frame header that announces totalLength = MAX_FRAME_BYTES
    // (valid — exactly at the cap). We never send the payload, so the parser
    // will be stuck waiting for `MAX_FRAME_BYTES - BEST_EFFORT_HEADER_SIZE`
    // more bytes. Feeding small chunks until the ring is full triggers BUFFER_OVERFLOW.
    const partialHeader = new Uint8Array(13);
    const headerView = new DataView(partialHeader.buffer);
    // totalLength = MAX_FRAME_BYTES (valid range upper bound).
    headerView.setUint32(0, MAX_FRAME_BYTES, true);
    headerView.setBigInt64(4, 0n, true);
    partialHeader[12] = 0x00; // flags = best-effort (valid combo)

    // Feed the header first — parser reads totalLength and waits for payload.
    parser.feed(partialHeader);
    expect(errors).toHaveLength(0);
    expect(parser.isDead()).toBe(false);

    // Now drip small chunks. The ring has RING_BYTES = MAX_FRAME_BYTES.
    // After the header is consumed (but frame not yet complete), the unread
    // bytes = 13. Feed chunks of 1024 bytes until the ring cannot fit another.
    const chunk = new Uint8Array(1024).fill(0x00);
    let overflowFired = false;
    // RING_BYTES total; 13 already written; need to overflow with another chunk.
    const feedsNeeded = Math.ceil((RING_BYTES - 13) / 1024) + 2;

    for (let i = 0; i < feedsNeeded; i++) {
      if (parser.isDead()) {
        overflowFired = true;
        break;
      }
      parser.feed(chunk);
    }

    expect(overflowFired).toBe(true);
    expect(errors).toHaveLength(1);
    expect(errors[0]!.code).toBe("BUFFER_OVERFLOW");
  });

  it("feed_afterError_isNoOp", () => {
    const { parser, frames, errors } = makeParser();

    // Trigger a fatal error first.
    const bad = new Uint8Array(13);
    new DataView(bad.buffer).setUint32(0, 12, true); // totalLength=12 < 13
    parser.feed(bad);
    expect(errors).toHaveLength(1);
    expect(parser.isDead()).toBe(true);

    // Subsequent feeds with valid data are silently ignored.
    const good = encodeBestEffort(makePayload(4), 0);
    parser.feed(good);
    parser.feed(good);

    expect(errors).toHaveLength(1); // no new errors
    expect(frames).toHaveLength(0); // no frames emitted
  });
});
