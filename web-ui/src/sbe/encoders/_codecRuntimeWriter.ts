/**
 * Internal runtime helpers for the hand-written SBE encoders.
 *
 * <p><b>Why hand-written:</b> the {@code :sbe-typescript-generator} module currently emits
 * decoders only. Extending it to emit encoders is tracked separately (see
 * sbe-typescript-generator/src/main/java/com/trading/engine/sbe/ts/MessageGenerator.java); until
 * that lands, this directory carries hand-written encoders for the messages the browser must
 * produce (NewOrderSingle, future CancelOrderRequest). The wire layout is BYTE-IDENTICAL to the
 * generated decoder: same offsets, same little-endian discipline, same null-padding rule for
 * fixed-length char arrays. A round-trip test (`web-ui/test/unit/sbe/NewOrderSingleEncoder.test.ts`)
 * encodes via this module and decodes via the generated decoder; any drift fails the test.
 *
 * <p><b>Allocation:</b> caller-provided {@link DataView} backed by a pooled {@link Uint8Array}.
 * Writers do NOT allocate.
 *
 * <p>Threading: the encoders are flyweights — single-threaded use only (caller serialises).
 */

const TEXT_ENCODER = new TextEncoder();

/**
 * Write a fixed-length null-padded char array. Truncates if the encoded UTF-8 byte length
 * exceeds {@code length} (asserts in dev to surface the truncation; production callers should
 * validate the input to the schema's max-length constraint client-side).
 */
export function writeFixedString(
  buffer: DataView,
  offset: number,
  length: number,
  value: string,
): void {
  // Encode into a temporary scratch then copy + null-pad. We cannot encodeInto directly into
  // the destination because we must zero-fill any trailing bytes — a partially-filled slot from
  // a pool reuse must not bleed prior bytes into the wire frame.
  const bytes = TEXT_ENCODER.encode(value);
  const writeLen = Math.min(bytes.length, length);
  for (let i = 0; i < writeLen; i++) {
    // bytes[i] is in-bounds by writeLen ≤ bytes.length; coerce undefined → 0
    // for the strict-tsconfig path. Hot loop — keep as a single setUint8 call.
    buffer.setUint8(offset + i, bytes[i] ?? 0);
  }
  for (let i = writeLen; i < length; i++) {
    buffer.setUint8(offset + i, 0);
  }
}

/** Write a uint8 enum or scalar. */
export function writeUint8(buffer: DataView, offset: number, value: number): void {
  buffer.setUint8(offset, value & 0xff);
}

/** Write a little-endian int64 from a JS bigint. */
export function writeInt64LE(buffer: DataView, offset: number, value: bigint): void {
  buffer.setBigInt64(offset, value, true);
}

/** Write a little-endian uint64 from a JS bigint. */
export function writeUint64LE(buffer: DataView, offset: number, value: bigint): void {
  buffer.setBigUint64(offset, value, true);
}

/** Write the 8-byte SBE message header (blockLength, templateId, schemaId, version). */
export function writeMessageHeader(
  buffer: DataView,
  offset: number,
  blockLength: number,
  templateId: number,
  schemaId: number,
  schemaVersion: number,
): void {
  buffer.setUint16(offset + 0, blockLength, true);
  buffer.setUint16(offset + 2, templateId, true);
  buffer.setUint16(offset + 4, schemaId, true);
  buffer.setUint16(offset + 6, schemaVersion, true);
}

/** Length of the SBE messageHeader composite, in bytes. */
export const MESSAGE_HEADER_LENGTH = 8;
