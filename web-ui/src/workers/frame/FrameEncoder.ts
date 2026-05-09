/**
 * FrameEncoder — encodes outbound WebSocket frames.
 *
 * All client → server frames are best-effort (13 B header, no CRC32C),
 * per APP-36 §2.4. The reliable encode path is included for tests and
 * for future server-driven flows that may require it; production worker
 * code only calls `encodeBestEffort`.
 *
 * Wire format mirrors `FrameParser.java` and the plan §2.1:
 *   13B header: totalLength u32 LE | seqNo i64 LE | flags u8
 *   17B header: same + crc32c u32 LE at offset 13
 *   payload:    SBE-encoded bytes (caller-supplied)
 *
 * Threading: any (pure functions; output buffers are caller-owned).
 *
 * Allocation: each `encode*` call allocates one output `Uint8Array` of
 * exactly `totalLength` bytes. The worker hot path uses these only for
 * outbound auth/heartbeat/ack/gap-request/session-resume frames — all
 * cold-path operations. Frame output is then handed to `WebSocket.send`.
 *
 * Plan reference: §2.1 / §2.2 / §5.1 / §6 row 8.
 */

import { crc32cOf } from "@/workers/frame/Crc32c";
import {
  BEST_EFFORT_HEADER_SIZE,
  FLAG_RELIABLE,
  RELIABLE_HEADER_SIZE,
} from "@/workers/frame/Flags";
import { MAX_FRAME_BYTES } from "@/workers/WorkerTuning";

const MIN_RELIABLE_TOTAL_LENGTH = RELIABLE_HEADER_SIZE; // zero-byte payload is legal

/**
 * Encode a best-effort frame. Caller supplies the SBE payload bytes;
 * envelope is wrapped here.
 *
 * @param payload SBE-encoded message bytes (may be empty)
 * @param flags optional flag bits (defaults to 0). MUST NOT include
 *   `FLAG_RELIABLE` — use `encodeReliable` for that path.
 */
export function encodeBestEffort(payload: Uint8Array, flags = 0): Uint8Array {
  if ((flags & FLAG_RELIABLE) !== 0) {
    throw new Error("encodeBestEffort: FLAG_RELIABLE must not be set; use encodeReliable");
  }
  const totalLength = BEST_EFFORT_HEADER_SIZE + payload.length;
  if (totalLength > MAX_FRAME_BYTES) {
    throw new Error(
      `encodeBestEffort: totalLength ${String(totalLength)} exceeds MAX_FRAME_BYTES ${String(MAX_FRAME_BYTES)}`,
    );
  }
  const out = new Uint8Array(totalLength);
  const view = new DataView(out.buffer);
  view.setUint32(0, totalLength, true);
  // seqNo = 0 on best-effort. setBigInt64 with 0n is well-defined.
  view.setBigInt64(4, 0n, true);
  out[12] = flags & 0xff;
  if (payload.length > 0) {
    out.set(payload, BEST_EFFORT_HEADER_SIZE);
  }
  return out;
}

/**
 * Encode a reliable frame with CRC32C over header[0..12] ‖ payload.
 *
 * @param seqNo monotonic per-session reliable sequence number
 * @param payload SBE-encoded message bytes (may be empty — `totalLength=17` is legal)
 * @param flags MUST include `FLAG_RELIABLE`. Replay / snapshot bits
 *   permitted per the §2.1 valid-combo whitelist.
 */
export function encodeReliable(
  seqNo: bigint,
  payload: Uint8Array,
  flags = FLAG_RELIABLE,
): Uint8Array {
  if ((flags & FLAG_RELIABLE) === 0) {
    throw new Error("encodeReliable: FLAG_RELIABLE must be set");
  }
  const totalLength = RELIABLE_HEADER_SIZE + payload.length;
  if (totalLength < MIN_RELIABLE_TOTAL_LENGTH) {
    throw new Error(
      `encodeReliable: totalLength ${String(totalLength)} below minimum ${String(MIN_RELIABLE_TOTAL_LENGTH)}`,
    );
  }
  if (totalLength > MAX_FRAME_BYTES) {
    throw new Error(
      `encodeReliable: totalLength ${String(totalLength)} exceeds MAX_FRAME_BYTES ${String(MAX_FRAME_BYTES)}`,
    );
  }
  const out = new Uint8Array(totalLength);
  const view = new DataView(out.buffer);
  view.setUint32(0, totalLength, true);
  view.setBigInt64(4, seqNo, true);
  out[12] = flags & 0xff;
  // CRC32C region = header[0..12] ‖ payload. Compute, then write the
  // little-endian uint32 at offset 13.
  const crc = crc32cOf(out, BEST_EFFORT_HEADER_SIZE, payload, payload.length);
  view.setUint32(13, crc >>> 0, true);
  if (payload.length > 0) {
    out.set(payload, RELIABLE_HEADER_SIZE);
  }
  return out;
}
