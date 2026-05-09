/**
 * Flags — frame envelope flag bits and combo validation per APP-36 §2.1.
 *
 * Wire format constants mirror `websocket-server/.../FrameParser.java`.
 * Flag-combo whitelist is exact: any other combination is a
 * `PROTOCOL_VIOLATION` (worker closes; no auto-reconnect).
 *
 * Threading: any (pure constants + pure validation function).
 *
 * Allocation: zero (constants + boolean expression).
 *
 * Plan reference: §2.1 / §6 row 8.
 */

export const FLAG_RELIABLE = 0x01 as const;
export const FLAG_REPLAY = 0x02 as const;
export const FLAG_SNAPSHOT = 0x04 as const;
export const FLAG_SNAPSHOT_FINAL = 0x0c as const;

/** Reserved bits 4–7. Any of these set in inbound flags → close PROTOCOL_VIOLATION. */
export const RESERVED_FLAG_MASK = 0xf0 as const;

/**
 * Best-effort header size (no CRC32C).
 *
 * Layout: totalLength u32 LE (offset 0) + seqNo i64 LE (offset 4) +
 * flags u8 (offset 12) = 13 bytes.
 */
export const BEST_EFFORT_HEADER_SIZE = 13 as const;

/**
 * Reliable header size (best-effort + CRC32C u32 LE at offset 13).
 */
export const RELIABLE_HEADER_SIZE = 17 as const;

/**
 * Allowed flag-combo whitelist per §2.1 — any other combination is a
 * protocol violation. The allowed set is:
 *   - 0x00: best-effort, no replay, no snapshot
 *   - 0x01: reliable
 *   - 0x03: reliable + replay
 *   - 0x04: snapshot fragment (mid)
 *   - 0x0C: snapshot final (≡ FLAG_SNAPSHOT | 0x08)
 *   - 0x05 (reliable + snapshot fragment) — reliable snapshots permitted
 *     by §A1/§2.10 (the C9 cross-stack test extends fixtures with this)
 *   - 0x0D (reliable + snapshot final) — same rationale
 *
 * Note: snapshots can be either best-effort (0x04 / 0x0C) or reliable
 * (0x05 / 0x0D); both combos are valid. SnapshotAssembler distinguishes
 * by the FLAG_RELIABLE bit on each fragment.
 */
// Per /review MEDIUM (Gemini): hot-path membership check uses a Set
// for O(1) lookup instead of linear-scanning a frozen array. The set
// is allocated once at module init; the check is called per inbound
// frame so the constant-factor improvement matters at 5 k/s.
const VALID_FLAG_COMBOS: ReadonlySet<number> = new Set<number>([
  0x00, 0x01, 0x03, 0x04, 0x05, 0x0c, 0x0d,
]);

/**
 * Returns true iff the given flag byte is on the allow-list AND no
 * reserved bits (4–7) are set.
 *
 * @param flags inbound flag byte (uint8 at offset 12 of the envelope)
 */
export function isValidFlagCombo(flags: number): boolean {
  if ((flags & RESERVED_FLAG_MASK) !== 0) return false;
  return VALID_FLAG_COMBOS.has(flags);
}

/** Returns true iff the reliable bit is set. */
export function isReliable(flags: number): boolean {
  return (flags & FLAG_RELIABLE) !== 0;
}

/** Returns true iff the replay bit is set (only valid combined with reliable). */
export function isReplay(flags: number): boolean {
  return (flags & FLAG_REPLAY) !== 0;
}

/** Returns true iff this is a snapshot fragment (mid OR final). */
export function isSnapshot(flags: number): boolean {
  return (flags & FLAG_SNAPSHOT) !== 0;
}

/** Returns true iff this is the final fragment of a snapshot. */
export function isSnapshotFinal(flags: number): boolean {
  return (flags & FLAG_SNAPSHOT_FINAL) === FLAG_SNAPSHOT_FINAL;
}
