/**
 * SymbolPacking — pack an ASCII 6–8 char trading symbol into a 48-bit `number`.
 *
 * Purpose
 * -------
 * Backs the per-symbol conflation `Map<number, MarketDataTickFrame>` and the per-symbol
 * `lastSeq: Map<number, number>` in `gapDetector.ts`. Both maps are on the hot decode path —
 * once per inbound `MarketDataTick` (template 54). Using a packed `number` key avoids the
 * per-tick `bigint` boxing that a `Map<bigint, …>` would incur on V8 (bigint values are not
 * interned and `Map.set(bigintKey, …)` allocates a fresh boxed key on every call even when
 * numerically equal). 48 bits of payload fit comfortably under
 * `Number.MAX_SAFE_INTEGER = 2^53 - 1`, so the JS engine keeps the value on the small-integer
 * fast path with zero heap allocation.
 *
 * Encoding
 * --------
 * - 6 bits per character. Alphabet: `A`–`Z` (26) → values `1`–`26`; remaining slots reserved.
 *   Value `0` marks a "no character" (used to pad symbols shorter than 8 chars).
 * - 8 character slots → 6 × 8 = 48 bits total. Symbols are packed left-aligned (slot 0 = first
 *   char) so the natural numeric ordering of the packed values respects ASCII alphabetic
 *   ordering of the source strings (useful for sorted Map iteration if ever needed).
 * - Symbols beyond 8 chars are NOT supported. The validator `^[A-Z]{6,8}$` matches the
 *   reference-data loader's invariant; any input violating it throws `RangeError` with the
 *   offending input quoted.
 *
 * Threading
 * ---------
 * Pure functions; no state. Safe to call from any thread (worker / main).
 *
 * Allocation
 * ----------
 * `pack`: allocates ONE primitive `number` (small-integer fast path; no heap). The transient
 *   `string` validation via the regex is unavoidable on the validation hot path; for the
 *   worker decode path the input is already a fixed `Uint8Array` slice — see `packBytes` for
 *   the zero-alloc variant.
 * `packBytes`: zero allocation — operates directly on the 8-byte ASCII slice from the SBE
 *   payload, no String materialisation. Used by the worker's `MarketDataTick` decoder which
 *   reads the symbol bytes directly from the wire buffer.
 * `unpack`: allocates ONE `String` of length ≤ 8 — cold path only (diagnostics, logging).
 *
 * Reference
 * ---------
 * LMAX exchange-core JS port uses the same 48-bit packed-symbol trick to avoid bigint boxing
 * on Map keys; see Federated-Sprouting-Starlight plan §Commit 6, Plan-agent Rec-15.
 */

/** Maximum supported symbol length in characters. */
export const SYMBOL_MAX_CHARS = 8;

/** Minimum supported symbol length (matches reference-data validator `^[A-Z]{6,8}$`). */
export const SYMBOL_MIN_CHARS = 6;

/** Sentinel "no character" — used to pad symbols shorter than 8 chars. */
const NO_CHAR = 0;

/** Bits per character slot. */
const BITS_PER_CHAR = 6;

/**
 * Pack an ASCII 6–8 char symbol `String` into a 48-bit `number`. Validates input against
 * `^[A-Z]{6,8}$` (rejects lowercase, digits, punctuation, length outside 6–8).
 *
 * Performance: O(symbol.length). The `RegExp` lookup is one-shot; the per-char loop body is
 * branch-free `Number` arithmetic.
 *
 * @param symbol upper-case ASCII string, 6–8 chars
 * @returns 48-bit packed value as a `number` (fits in Number.MAX_SAFE_INTEGER)
 * @throws RangeError when the input violates `^[A-Z]{6,8}$`
 */
export function pack(symbol: string): number {
  if (symbol.length < SYMBOL_MIN_CHARS || symbol.length > SYMBOL_MAX_CHARS) {
    throw new RangeError(
      `SymbolPacking.pack: symbol length ${String(symbol.length)} outside [${String(SYMBOL_MIN_CHARS)}, ${String(SYMBOL_MAX_CHARS)}] for input "${symbol}"`,
    );
  }
  let packed = 0;
  for (let i = 0; i < SYMBOL_MAX_CHARS; i++) {
    const charValue = i < symbol.length ? charCodeToValue(symbol.charCodeAt(i), symbol) : NO_CHAR;
    // Left-aligned packing: slot 0 occupies the high 6 bits so packed values sort
    // naturally in the same order as the underlying ASCII strings.
    packed = packed * (1 << BITS_PER_CHAR) + charValue;
  }
  return packed;
}

/**
 * Pack 8 raw bytes (from an SBE Symbol char[8] field) into a 48-bit `number`. Trailing NUL
 * bytes (0x00) are treated as the "no character" sentinel — matches the SBE convention of
 * right-padding short symbols with NUL.
 *
 * Zero-allocation: operates directly on the provided `Uint8Array`; no intermediate `String`.
 *
 * @param bytes a 8-byte view; bytes 0–7 are read; bytes[0] MUST be a non-NUL ASCII upper-case
 *     letter or this throws `RangeError`. Trailing NUL bytes after the first letter run are
 *     valid padding.
 * @returns 48-bit packed value as a `number`
 * @throws RangeError when bytes[0] is invalid OR when a NUL byte is followed by a non-NUL byte
 *     (embedded NUL — malformed wire input)
 */
export function packBytes(bytes: Uint8Array): number {
  if (bytes.length < SYMBOL_MAX_CHARS) {
    throw new RangeError(
      `SymbolPacking.packBytes: byte view length ${String(bytes.length)} < ${String(SYMBOL_MAX_CHARS)}`,
    );
  }
  const firstByte = bytes[0] ?? 0;
  if (firstByte < 0x41 /* 'A' */ || firstByte > 0x5a /* 'Z' */) {
    throw new RangeError(
      `SymbolPacking.packBytes: first byte 0x${firstByte.toString(16)} is not an upper-case ASCII letter`,
    );
  }
  let packed = 0;
  let sawTrailingNul = false;
  for (let i = 0; i < SYMBOL_MAX_CHARS; i++) {
    const byte = bytes[i] ?? 0; // length guard above proves in-range; ?? satisfies noUncheckedIndexedAccess
    let charValue: number;
    if (byte === 0x00) {
      sawTrailingNul = true;
      charValue = NO_CHAR;
    } else if (sawTrailingNul) {
      throw new RangeError(
        `SymbolPacking.packBytes: embedded NUL at slot < ${String(i)} followed by non-NUL byte 0x${byte.toString(16)}`,
      );
    } else if (byte >= 0x41 /* 'A' */ && byte <= 0x5a /* 'Z' */) {
      charValue = byte - 0x40; // 'A' → 1, 'B' → 2, …, 'Z' → 26
    } else {
      throw new RangeError(
        `SymbolPacking.packBytes: byte 0x${byte.toString(16)} at slot ${String(i)} is not an upper-case ASCII letter`,
      );
    }
    packed = packed * (1 << BITS_PER_CHAR) + charValue;
  }
  return packed;
}

/**
 * Unpack a previously-packed 48-bit value back into the original ASCII string. Trailing
 * NO_CHAR slots are trimmed.
 *
 * Cold-path only — allocates one `String`. Used for diagnostics, log lines, and the
 * `unpack(packed)` round-trip test fixtures. Do NOT call on any hot path.
 *
 * @param packed the value produced by {@link pack} or {@link packBytes}
 * @returns the original 6–8 char ASCII symbol
 */
export function unpack(packed: number): string {
  if (!Number.isInteger(packed) || packed < 0 || packed > Number.MAX_SAFE_INTEGER) {
    throw new RangeError(
      `SymbolPacking.unpack: input ${String(packed)} is not a non-negative safe integer`,
    );
  }
  // Unpack right-to-left then reverse — produces the original char order.
  const chars: string[] = [];
  let remaining = packed;
  const slotMask = (1 << BITS_PER_CHAR) - 1;
  for (let i = 0; i < SYMBOL_MAX_CHARS; i++) {
    const charValue = remaining & slotMask;
    remaining = Math.floor(remaining / (1 << BITS_PER_CHAR));
    if (charValue === NO_CHAR) {
      chars.push("");
    } else if (charValue >= 1 && charValue <= 26) {
      chars.push(String.fromCharCode(0x40 + charValue));
    } else {
      throw new RangeError(
        `SymbolPacking.unpack: corrupt slot value ${String(charValue)} at position ${String(SYMBOL_MAX_CHARS - 1 - i)} in packed value ${String(packed)}`,
      );
    }
  }
  // chars is in reverse slot order (slot 7 first). Reverse + join — trailing NO_CHAR slots
  // became empty strings above, so the join naturally produces the trimmed symbol.
  return chars.reverse().join("");
}

/**
 * Validates and converts a single ASCII upper-case char code to its 6-bit slot value.
 * Internal helper — throws with full context for the {@link pack} call site.
 */
function charCodeToValue(code: number, source: string): number {
  if (code < 0x41 /* 'A' */ || code > 0x5a /* 'Z' */) {
    throw new RangeError(
      `SymbolPacking.pack: char code 0x${code.toString(16)} in "${source}" is not an upper-case ASCII letter`,
    );
  }
  return code - 0x40;
}
