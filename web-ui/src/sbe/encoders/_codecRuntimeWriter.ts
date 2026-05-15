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

/**
 * Write a fixed-length null-padded char array directly from the source string's
 * char codes. Truncates if the source length exceeds {@code length}. Throws
 * `RangeError` if any character is outside the printable ASCII range (0x20–0x7E)
 * — SBE `Symbol`/`ClOrdID`/`Account` etc. are wire-pinned ASCII per the FIX
 * spec, so non-ASCII input is a programming error, not a silent corruption.
 *
 * <p><b>Allocation:</b> ZERO per call — no `TextEncoder.encode` (which would
 * allocate a fresh `Uint8Array` per fixed-string field × 14 fields per
 * `submitOrder`, contradicting the alloc-tripwire baseline). The hot path is a
 * tight `charCodeAt` loop directly into the caller-provided `DataView`.
 *
 * <p>The reviewer-flagged TextEncoder-per-call regression is fixed here.
 */
export function writeFixedString(
  buffer: DataView,
  offset: number,
  length: number,
  value: string,
): void {
  const writeLen = Math.min(value.length, length);
  for (let i = 0; i < writeLen; i++) {
    const code = value.charCodeAt(i);
    if (code < 0x20 || code > 0x7e) {
      throw new RangeError(
        `writeFixedString: non-ASCII byte 0x${code.toString(16)} at index ${String(i)} of "${value}"`,
      );
    }
    buffer.setUint8(offset + i, code);
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
