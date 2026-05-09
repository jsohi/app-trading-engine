/*
 * Hot-path ring-buffer reads use `this.ring[idx]!` for byte access
 * inside provably-safe bounded ranges (we just verified `available >=
 * BEST_EFFORT_HEADER_SIZE` etc.). Plan §2.1 / §4.4 — see file-level
 * docstring below.
 */
/* eslint-disable @typescript-eslint/no-non-null-assertion */

/**
 * FrameParser — pull-parser for the wire envelope (APP-36 §2.1).
 *
 * The parser is a state machine over a caller-managed receive buffer.
 * The intended hot-path usage (in `worker.ts`, C6):
 *
 *   const parser = new FrameParser({ onFrame, onError });
 *   ws.onmessage = (event) => parser.feed(new Uint8Array(event.data));
 *
 * Where `onFrame(frame)` is invoked synchronously inside `feed()` for
 * every complete frame found, in order. The frame views are zero-copy
 * `Uint8Array` slices over the parser's internal ring buffer; the
 * callback MUST consume them synchronously (the buffer may be reused).
 *
 * Backpressure / overflow:
 *   - The receive buffer is a fixed `Uint8Array(MAX_FRAME_BYTES = 4 MiB)`
 *     ring. A single frame cannot exceed this length per §2.1.
 *   - If the parser would need more space than the ring offers (e.g.
 *     a partial frame whose remaining bytes don't fit), it surfaces
 *     `BUFFER_OVERFLOW` via `onError` and the caller MUST close the
 *     WebSocket; subsequent `feed()` calls are no-ops.
 *
 * Threading: single-threaded (worker scope only).
 *
 * Allocation: zero per frame after construction. The ring buffer is
 * the only persistent allocation; per-frame `Uint8Array` views into the
 * ring are allocated, but those are tiny (16 B header views) and
 * unavoidable on the JS side. Hot-path discipline.
 *
 * Plan reference: §2.1 / §2.2 / §4.4 / §6 rows 8, 9, 26, 27.
 */

import { crc32cOf } from "@/workers/frame/Crc32c";
import {
  BEST_EFFORT_HEADER_SIZE,
  FLAG_RELIABLE,
  isValidFlagCombo,
  RELIABLE_HEADER_SIZE,
} from "@/workers/frame/Flags";
import { MAX_FRAME_BYTES, RING_BYTES } from "@/workers/WorkerTuning";

/** Reason codes for `FrameParserCallbacks.onError`. */
export type FrameParseErrorCode =
  | "PROTOCOL_VIOLATION" // invalid flag combo, reserved bit set, totalLength out of range
  | "CRC_MISMATCH" // reliable-frame CRC32C mismatch
  | "BUFFER_OVERFLOW"; // ring full; cannot fit further bytes

/**
 * A decoded frame. The `payload` view is into the parser's ring buffer
 * and remains valid only for the duration of the `onFrame` callback —
 * subsequent `feed()` invocations may overwrite the underlying bytes.
 */
export interface ParsedFrame {
  /** Total envelope length in bytes (header + payload). */
  readonly totalLength: number;
  /** Monotonic per-session reliable sequence number; `0n` on best-effort. */
  readonly seqNo: bigint;
  /** Raw flag byte at offset 12; see `Flags.ts` for bit semantics. */
  readonly flags: number;
  /**
   * Zero-copy view of the SBE payload bytes (no header). The view is
   * tied to the parser's ring buffer and is invalidated on the next
   * `feed()` call — callers must process it synchronously.
   */
  readonly payload: Uint8Array;
}

export interface FrameParserCallbacks {
  /** Invoked synchronously for every complete frame. Must consume `payload` before returning. */
  onFrame: (frame: ParsedFrame) => void;
  /**
   * Invoked on a fatal parse error. After this fires, the parser
   * stops accepting input (further `feed()` calls are no-ops). The
   * caller MUST close the underlying WebSocket.
   */
  onError: (code: FrameParseErrorCode, message: string) => void;
}

/**
 * Pull-parser over a fixed-size ring buffer.
 *
 * Design notes:
 *   - The ring is a contiguous `Uint8Array(RING_BYTES)`. We track
 *     `readPos` (next byte to parse) and `writePos` (next byte to
 *     append). When `writePos` reaches the ring end and `readPos`
 *     has advanced, we compact (memmove the unread tail to offset 0)
 *     instead of wrapping. Ring-with-compaction is simpler than a
 *     true circular ring and adequate for `MAX_FRAME_BYTES == RING_BYTES`
 *     (we never carry more than one in-flight frame).
 *   - `feed(bytes)` first appends, then drains as many complete
 *     frames as the buffer holds, in a single synchronous turn.
 */
export class FrameParser {
  private readonly ring: Uint8Array;
  private readonly view: DataView;
  private readPos = 0;
  private writePos = 0;
  private dead = false;
  private readonly callbacks: FrameParserCallbacks;

  constructor(callbacks: FrameParserCallbacks) {
    this.ring = new Uint8Array(RING_BYTES);
    this.view = new DataView(this.ring.buffer);
    this.callbacks = callbacks;
  }

  /**
   * Feed inbound bytes (typically `new Uint8Array(messageEvent.data)`).
   * Drains as many complete frames as the buffer holds. No-op once a
   * fatal parse error has fired.
   */
  feed(bytes: Uint8Array): void {
    if (this.dead) return;
    if (bytes.length === 0) return;

    // 1. Append. Compact first if the trailing free space cannot fit
    //    these bytes plus everything still unread.
    const unread = this.writePos - this.readPos;
    if (unread + bytes.length > RING_BYTES) {
      this.fail("BUFFER_OVERFLOW", `cannot fit ${String(bytes.length)} bytes; ring is full`);
      return;
    }
    if (this.writePos + bytes.length > RING_BYTES) {
      // Compact: move unread bytes to offset 0.
      this.ring.copyWithin(0, this.readPos, this.writePos);
      this.writePos = unread;
      this.readPos = 0;
    }
    this.ring.set(bytes, this.writePos);
    this.writePos += bytes.length;

    // 2. Drain. Each iteration parses one complete frame OR returns
    // when there is not enough data. `this.fail()` sets `dead` and the
    // call site that triggered it always `return`s, so the loop
    // body's exits are: "return; not enough data" / "continue; frame
    // dispatched" / "return; fatal error". Using `for (;;)` because
    // typescript-eslint's no-unnecessary-condition flags `while (!dead)`
    // since we already guarded at the top of feed().
    for (;;) {
      const available = this.writePos - this.readPos;
      if (available < BEST_EFFORT_HEADER_SIZE) return; // wait for header

      const totalLength = this.view.getUint32(this.readPos, true);
      // Range check: 13 ≤ L ≤ MAX_FRAME_BYTES. We treat L = 0xFFFFFFFF
      // and L < 13 as PROTOCOL_VIOLATION (close + no auto-reconnect).
      // For reliable frames the lower bound is 17 — we enforce that
      // after we know the flag bit.
      if (totalLength < BEST_EFFORT_HEADER_SIZE || totalLength > MAX_FRAME_BYTES) {
        this.fail(
          "PROTOCOL_VIOLATION",
          `totalLength ${String(totalLength)} out of [${String(BEST_EFFORT_HEADER_SIZE)}, ${String(MAX_FRAME_BYTES)}]`,
        );
        return;
      }
      if (available < totalLength) return; // wait for full frame

      // Headers fully present; parse seqNo + flags.
      const seqNo = this.view.getBigInt64(this.readPos + 4, true);
      const flags = this.ring[this.readPos + 12]!;

      if (!isValidFlagCombo(flags)) {
        this.fail(
          "PROTOCOL_VIOLATION",
          `invalid flag combo 0x${flags.toString(16).padStart(2, "0")}`,
        );
        return;
      }

      const reliable = (flags & FLAG_RELIABLE) !== 0;
      if (reliable && totalLength < RELIABLE_HEADER_SIZE) {
        this.fail(
          "PROTOCOL_VIOLATION",
          `reliable totalLength ${String(totalLength)} below minimum ${String(RELIABLE_HEADER_SIZE)}`,
        );
        return;
      }

      // Reliable: verify CRC32C over header[0..12] ‖ payload.
      if (reliable) {
        const crcOnWire = this.view.getUint32(this.readPos + 13, true) >>> 0;
        const payloadStart = this.readPos + RELIABLE_HEADER_SIZE;
        const payloadLen = totalLength - RELIABLE_HEADER_SIZE;
        // Compute CRC over the contiguous ring slice [readPos .. payloadStart) || payload
        // by passing the same Uint8Array twice with the appropriate offsets.
        const headerSlice = new Uint8Array(
          this.ring.buffer,
          this.ring.byteOffset + this.readPos,
          BEST_EFFORT_HEADER_SIZE,
        );
        const payloadSlice = new Uint8Array(
          this.ring.buffer,
          this.ring.byteOffset + payloadStart,
          payloadLen,
        );
        const computed =
          crc32cOf(headerSlice, BEST_EFFORT_HEADER_SIZE, payloadSlice, payloadLen) >>> 0;
        if (computed !== crcOnWire) {
          this.fail(
            "CRC_MISMATCH",
            `crc32c mismatch: wire=0x${crcOnWire
              .toString(16)
              .padStart(8, "0")} computed=0x${computed.toString(16).padStart(8, "0")}`,
          );
          return;
        }
      }

      // Build payload view + dispatch.
      const headerSize = reliable ? RELIABLE_HEADER_SIZE : BEST_EFFORT_HEADER_SIZE;
      const payloadStart = this.readPos + headerSize;
      const payloadLen = totalLength - headerSize;
      const payload = new Uint8Array(
        this.ring.buffer,
        this.ring.byteOffset + payloadStart,
        payloadLen,
      );

      // Advance BEFORE dispatch so re-entrant feeds don't re-parse the same frame.
      this.readPos += totalLength;

      try {
        this.callbacks.onFrame({ totalLength, seqNo, flags, payload });
      } catch (err) {
        // The onFrame callback should not throw; if it does, surface
        // as PROTOCOL_VIOLATION so the caller closes the channel.
        this.fail(
          "PROTOCOL_VIOLATION",
          `onFrame threw: ${err instanceof Error ? err.message : String(err)}`,
        );
        return;
      }

      // If we've consumed everything, reset positions so the next
      // append starts at offset 0 (avoids unnecessary compaction churn).
      if (this.readPos === this.writePos) {
        this.readPos = 0;
        this.writePos = 0;
      }
    }
  }

  /**
   * Visible state for tests and watchdog assertions.
   * Returns the number of bytes currently buffered (unread).
   */
  pending(): number {
    return this.writePos - this.readPos;
  }

  /** Visible state for tests; true after a fatal error. */
  isDead(): boolean {
    return this.dead;
  }

  private fail(code: FrameParseErrorCode, message: string): void {
    if (this.dead) return;
    this.dead = true;
    this.callbacks.onError(code, message);
  }
}
