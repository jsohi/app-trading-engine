/**
 * Flags.test.ts — unit tests for flag constants and validation functions.
 *
 * Validates:
 *  - `isValidFlagCombo` accepts exactly the whitelist {0x00, 0x01, 0x03, 0x04, 0x05, 0x0c, 0x0d}.
 *  - `isValidFlagCombo` rejects all other single-byte combos (≥ 249 rejection cases).
 *  - `isValidFlagCombo` rejects any byte with reserved bits 4..7 set.
 *  - `isReliable`, `isReplay`, `isSnapshot`, `isSnapshotFinal` predicates.
 *
 * Threading: single-threaded (Vitest jsdom worker).
 * Allocation: zero (pure function calls, no buffers).
 */

import { describe, expect, it } from "vitest";

import {
  FLAG_RELIABLE,
  FLAG_REPLAY,
  FLAG_SNAPSHOT,
  FLAG_SNAPSHOT_FINAL,
  RESERVED_FLAG_MASK,
  isValidFlagCombo,
  isReliable,
  isReplay,
  isSnapshot,
  isSnapshotFinal,
} from "@/workers/frame/Flags";

// ─── Constant sanity checks ─────────────────────────────────────────────────

describe("Flags — constant values", () => {
  it("FLAG_RELIABLE_is0x01", () => {
    expect(FLAG_RELIABLE).toBe(0x01);
  });

  it("FLAG_REPLAY_is0x02", () => {
    expect(FLAG_REPLAY).toBe(0x02);
  });

  it("FLAG_SNAPSHOT_is0x04", () => {
    expect(FLAG_SNAPSHOT).toBe(0x04);
  });

  it("FLAG_SNAPSHOT_FINAL_is0x0c", () => {
    expect(FLAG_SNAPSHOT_FINAL).toBe(0x0c);
  });

  it("RESERVED_FLAG_MASK_is0xF0", () => {
    expect(RESERVED_FLAG_MASK).toBe(0xf0);
  });
});

// ─── isValidFlagCombo — whitelist acceptance ────────────────────────────────

describe("isValidFlagCombo — accepts whitelist combos", () => {
  const whitelist = [0x00, 0x01, 0x03, 0x04, 0x05, 0x0c, 0x0d] as const;

  for (const combo of whitelist) {
    it(`isValidFlagCombo_0x${combo.toString(16).padStart(2, "0")}_returnsTrue`, () => {
      expect(isValidFlagCombo(combo)).toBe(true);
    });
  }
});

// ─── isValidFlagCombo — rejection of non-whitelist combos ──────────────────

describe("isValidFlagCombo — rejects non-whitelist combos", () => {
  const whitelist = new Set([0x00, 0x01, 0x03, 0x04, 0x05, 0x0c, 0x0d]);

  // Spot-check a representative set of rejected values in the lower nibble range.
  const rejectedLower = [0x02, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0e, 0x0f];
  for (const combo of rejectedLower) {
    it(`isValidFlagCombo_0x${combo.toString(16).padStart(2, "0")}_returnsFalse`, () => {
      expect(isValidFlagCombo(combo)).toBe(false);
    });
  }

  // Exhaust all 256 possible uint8 values; any not in the whitelist must be rejected.
  it("isValidFlagCombo_allNonWhitelistValues_returnsFalse", () => {
    for (let i = 0; i <= 0xff; i++) {
      if (whitelist.has(i)) continue;
      expect(isValidFlagCombo(i)).toBe(false);
    }
  });
});

// ─── isValidFlagCombo — reserved bits 4..7 ─────────────────────────────────

describe("isValidFlagCombo — rejects reserved bits 4..7", () => {
  it("isValidFlagCombo_bit4Set_returnsFalse", () => {
    expect(isValidFlagCombo(0x10)).toBe(false);
  });

  it("isValidFlagCombo_bit5Set_returnsFalse", () => {
    expect(isValidFlagCombo(0x20)).toBe(false);
  });

  it("isValidFlagCombo_bit6Set_returnsFalse", () => {
    expect(isValidFlagCombo(0x40)).toBe(false);
  });

  it("isValidFlagCombo_bit7Set_returnsFalse", () => {
    expect(isValidFlagCombo(0x80)).toBe(false);
  });

  it("isValidFlagCombo_allReservedBitsSet_returnsFalse", () => {
    expect(isValidFlagCombo(0xf0)).toBe(false);
  });

  it("isValidFlagCombo_reservedBitCombinedWithValidLowerNibble_returnsFalse", () => {
    // 0x01 (FLAG_RELIABLE) is valid alone, but with bit 4 set must fail.
    expect(isValidFlagCombo(0x11)).toBe(false);
    // 0x04 (FLAG_SNAPSHOT) valid alone; with bit 5 set must fail.
    expect(isValidFlagCombo(0x24)).toBe(false);
  });
});

// ─── isReliable ─────────────────────────────────────────────────────────────

describe("isReliable — bit 0 predicate", () => {
  it("isReliable_0x01_returnsTrue", () => {
    expect(isReliable(0x01)).toBe(true);
  });

  it("isReliable_0x03_returnsTrue", () => {
    expect(isReliable(0x03)).toBe(true);
  });

  it("isReliable_0x05_returnsTrue", () => {
    expect(isReliable(0x05)).toBe(true);
  });

  it("isReliable_0x0d_returnsTrue", () => {
    expect(isReliable(0x0d)).toBe(true);
  });

  it("isReliable_0x00_returnsFalse", () => {
    expect(isReliable(0x00)).toBe(false);
  });

  it("isReliable_0x04_returnsFalse", () => {
    expect(isReliable(0x04)).toBe(false);
  });

  it("isReliable_0x0c_returnsFalse", () => {
    expect(isReliable(0x0c)).toBe(false);
  });
});

// ─── isReplay ───────────────────────────────────────────────────────────────

describe("isReplay — bit 1 predicate", () => {
  it("isReplay_0x02_returnsTrue", () => {
    expect(isReplay(0x02)).toBe(true);
  });

  it("isReplay_0x03_returnsTrue", () => {
    expect(isReplay(0x03)).toBe(true);
  });

  it("isReplay_0x00_returnsFalse", () => {
    expect(isReplay(0x00)).toBe(false);
  });

  it("isReplay_0x01_returnsFalse", () => {
    expect(isReplay(0x01)).toBe(false);
  });

  it("isReplay_0x04_returnsFalse", () => {
    expect(isReplay(0x04)).toBe(false);
  });
});

// ─── isSnapshot ─────────────────────────────────────────────────────────────

describe("isSnapshot — bit 2 predicate", () => {
  it("isSnapshot_0x04_returnsTrue", () => {
    expect(isSnapshot(0x04)).toBe(true);
  });

  it("isSnapshot_0x05_returnsTrue", () => {
    expect(isSnapshot(0x05)).toBe(true);
  });

  it("isSnapshot_0x0c_returnsTrue", () => {
    expect(isSnapshot(0x0c)).toBe(true);
  });

  it("isSnapshot_0x0d_returnsTrue", () => {
    expect(isSnapshot(0x0d)).toBe(true);
  });

  it("isSnapshot_0x00_returnsFalse", () => {
    expect(isSnapshot(0x00)).toBe(false);
  });

  it("isSnapshot_0x01_returnsFalse", () => {
    expect(isSnapshot(0x01)).toBe(false);
  });

  it("isSnapshot_0x03_returnsFalse", () => {
    expect(isSnapshot(0x03)).toBe(false);
  });
});

// ─── isSnapshotFinal ────────────────────────────────────────────────────────

describe("isSnapshotFinal — bits 2+3 predicate", () => {
  it("isSnapshotFinal_0x0c_returnsTrue", () => {
    expect(isSnapshotFinal(0x0c)).toBe(true);
  });

  it("isSnapshotFinal_0x0d_returnsTrue", () => {
    // 0x0d = FLAG_SNAPSHOT_FINAL | FLAG_RELIABLE — both bits 2+3 set.
    expect(isSnapshotFinal(0x0d)).toBe(true);
  });

  it("isSnapshotFinal_0x04_returnsFalse", () => {
    // bit 2 set but bit 3 not set — mid-fragment only.
    expect(isSnapshotFinal(0x04)).toBe(false);
  });

  it("isSnapshotFinal_0x00_returnsFalse", () => {
    expect(isSnapshotFinal(0x00)).toBe(false);
  });

  it("isSnapshotFinal_0x08_returnsFalse", () => {
    // bit 3 alone does not satisfy — FLAG_SNAPSHOT_FINAL requires bit 2 also.
    expect(isSnapshotFinal(0x08)).toBe(false);
  });

  it("isSnapshotFinal_0x01_returnsFalse", () => {
    expect(isSnapshotFinal(0x01)).toBe(false);
  });
});
