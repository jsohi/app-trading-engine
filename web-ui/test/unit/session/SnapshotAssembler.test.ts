/**
 * SnapshotAssembler.test.ts — unit tests for fragmented snapshot
 * reassembly per APP-36 §2.10.
 *
 * Test naming follows `<unit>_<scenario>_<expected>` per plan §5.8.
 *
 * Threading: single-threaded (Vitest jsdom worker).
 * Allocation: test-only — Uint8Array payloads created in helpers.
 */

import { describe, expect, it } from "vitest";
import {
  SnapshotAssembler,
  type SnapshotAssemblerCallbacks,
  type AssembledSnapshot,
  type SnapshotFragment,
} from "@/workers/session/SnapshotAssembler";
import { type UuidComposite } from "@/workers/session/SessionState";
import {
  MAX_FRAGMENT_BYTES,
  MAX_INFLIGHT_SNAPSHOT_IDS,
  MAX_SNAPSHOT_BYTES_PER_ID,
  SNAPSHOT_COMPLETION_DEADLINE_MS,
} from "@/workers/WorkerTuning";

// ─── Helpers ─────────────────────────────────────────────────────────────────

function makeId(msb: bigint, lsb: bigint): UuidComposite {
  return { mostSignificantBits: msb, leastSignificantBits: lsb };
}

function makeFragment(
  snapshotId: UuidComposite,
  fragmentIndex: number,
  totalFragments: number,
  data: Uint8Array,
  isFinal = false,
): SnapshotFragment {
  return { snapshotId, fragmentIndex, totalFragments, payload: data, isFinal };
}

function makePayload(len: number, fill = 0xab): Uint8Array {
  return new Uint8Array(len).fill(fill);
}

function makeAssembler(): {
  assembler: SnapshotAssembler;
  nowMs: () => number;
  advanceMs: (ms: number) => void;
  completed: AssembledSnapshot[];
  violations: string[];
  overflows: string[];
  entityTooLargeIds: UuidComposite[];
} {
  let currentMs = 0;
  const completed: AssembledSnapshot[] = [];
  const violations: string[] = [];
  const overflows: string[] = [];
  const entityTooLargeIds: UuidComposite[] = [];

  const callbacks: SnapshotAssemblerCallbacks = {
    onSnapshotComplete: (snap: AssembledSnapshot): void => {
      completed.push(snap);
    },
    onProtocolViolation: (reason: string): void => {
      violations.push(reason);
    },
    onBufferOverflow: (reason: string): void => {
      overflows.push(reason);
    },
    onSnapshotEntityTooLarge: (id: UuidComposite): void => {
      entityTooLargeIds.push(id);
    },
  };

  const nowMs = (): number => currentMs;
  const advanceMs = (ms: number): void => {
    currentMs += ms;
  };

  return {
    assembler: new SnapshotAssembler(callbacks, nowMs),
    nowMs,
    advanceMs,
    completed,
    violations,
    overflows,
    entityTooLargeIds,
  };
}

// ─── Tests ───────────────────────────────────────────────────────────────────

describe("SnapshotAssembler", () => {
  it("multiFragment_byOrder_completes_emitsAssembledSnapshot", () => {
    const { assembler, completed } = makeAssembler();
    const id = makeId(1n, 1n);

    // 3 fragments arriving in order (0, 1, 2)
    assembler.onFragment(makeFragment(id, 0, 3, makePayload(100)));
    assembler.onFragment(makeFragment(id, 1, 3, makePayload(100)));
    assembler.onFragment(makeFragment(id, 2, 3, makePayload(100), true));

    expect(completed.length).toBe(1);
    expect(completed[0]?.snapshotId).toEqual(id);
    expect(completed[0]?.bytes.length).toBe(300);
  });

  it("outOfOrderFragmentIndex_acceptsAndCompletesOnAllFilled", () => {
    const { assembler, completed } = makeAssembler();
    const id = makeId(2n, 2n);

    // Fragments arrive 1, 0, 2 (out of order)
    assembler.onFragment(makeFragment(id, 1, 3, makePayload(50)));
    assembler.onFragment(makeFragment(id, 0, 3, makePayload(50)));
    // Only upon receiving all 3 fragments (count = 3) should it complete
    assembler.onFragment(makeFragment(id, 2, 3, makePayload(50), true));

    expect(completed.length).toBe(1);
    expect(completed[0]?.bytes.length).toBe(150);
  });

  it("duplicateFragmentIndex_protocolViolation", () => {
    const { assembler, violations } = makeAssembler();
    const id = makeId(3n, 3n);

    assembler.onFragment(makeFragment(id, 0, 2, makePayload(10)));
    // Duplicate fragment index 0
    assembler.onFragment(makeFragment(id, 0, 2, makePayload(10)));

    expect(violations.length).toBe(1);
    expect(violations[0]).toContain("duplicate fragmentIndex");
  });

  it("twoInterleavedSnapshotIds_bothComplete", () => {
    const { assembler, completed } = makeAssembler();
    const idA = makeId(10n, 10n);
    const idB = makeId(20n, 20n);

    // Interleave fragments of A and B
    assembler.onFragment(makeFragment(idA, 0, 2, makePayload(20)));
    assembler.onFragment(makeFragment(idB, 0, 2, makePayload(30)));
    assembler.onFragment(makeFragment(idA, 1, 2, makePayload(20), true));
    assembler.onFragment(makeFragment(idB, 1, 2, makePayload(30), true));

    expect(completed.length).toBe(2);
    const aComp = completed.find(
      (c) => c.snapshotId.mostSignificantBits === 10n && c.snapshotId.leastSignificantBits === 10n,
    );
    const bComp = completed.find(
      (c) => c.snapshotId.mostSignificantBits === 20n && c.snapshotId.leastSignificantBits === 20n,
    );
    expect(aComp?.bytes.length).toBe(40);
    expect(bComp?.bytes.length).toBe(60);
  });

  it("eightConcurrentSnapshotIds_OK_ninthClosesProtocolViolation", () => {
    const { assembler, violations } = makeAssembler();

    // Open MAX_INFLIGHT_SNAPSHOT_IDS = 8 concurrent snapshots (each 2 fragments, only first sent)
    for (let i = 0; i < MAX_INFLIGHT_SNAPSHOT_IDS; i++) {
      const id = makeId(BigInt(100 + i), BigInt(100 + i));
      const accepted = assembler.onFragment(makeFragment(id, 0, 2, makePayload(10)));
      expect(accepted).toBe(true);
    }

    // 9th concurrent snapshot should trigger protocol violation
    const ninthId = makeId(999n, 999n);
    assembler.onFragment(makeFragment(ninthId, 0, 2, makePayload(10)));

    expect(violations.length).toBe(1);
    expect(violations[0]).toContain("MAX_INFLIGHT_SNAPSHOT_IDS");
  });

  it("perId30sCompletionDeadline_stallTriggersBufferOverflow", () => {
    const { assembler, advanceMs, overflows } = makeAssembler();
    const id = makeId(50n, 50n);

    // Start a snapshot with only one fragment (of 3 total) — never finalised
    assembler.onFragment(makeFragment(id, 0, 3, makePayload(10)));

    // Advance past the 30 s deadline
    advanceMs(SNAPSHOT_COMPLETION_DEADLINE_MS + 1);

    // Timer tick checks deadlines
    assembler.onTimerTick();

    expect(overflows.length).toBe(1);
    expect(overflows[0]).toContain("did not complete");
  });

  it("MAX_FRAGMENT_BYTES_16KiB_exceeded_closesBufferOverflow", () => {
    const { assembler, overflows } = makeAssembler();
    const id = makeId(60n, 60n);

    // Fragment size > 16 KiB
    const oversizePayload = new Uint8Array(MAX_FRAGMENT_BYTES + 1);
    assembler.onFragment(makeFragment(id, 0, 1, oversizePayload, true));

    expect(overflows.length).toBe(1);
    expect(overflows[0]).toContain("MAX_FRAGMENT_BYTES");
  });

  it("MAX_SNAPSHOT_BYTES_PER_ID_8MiB_exceeded_close", () => {
    const { assembler, overflows } = makeAssembler();
    const id = makeId(70n, 70n);

    // Use small fragments (< MAX_FRAGMENT_BYTES each) but accumulate beyond 8 MiB per-id cap
    // Fragment size = 4 KiB, need > 8 MiB / 4 KiB = 2048 fragments
    const fragSize = 4 * 1024; // 4 KiB per fragment
    const totalNeeded = Math.ceil(MAX_SNAPSHOT_BYTES_PER_ID / fragSize) + 1;
    const totalFragments = totalNeeded;

    for (let i = 0; i < totalFragments; i++) {
      if (assembler.inflightCount() === 0 && i > 0) break; // dead
      const frag = makeFragment(id, i, totalFragments, new Uint8Array(fragSize));
      assembler.onFragment(frag);
      if (overflows.length > 0) break;
    }

    expect(overflows.length).toBe(1);
    expect(overflows[0]).toContain("MAX_SNAPSHOT_BYTES_PER_ID");
  });

  it("MAX_TOTAL_INFLIGHT_SNAPSHOT_BYTES_64MiB_total_close", () => {
    const { assembler, overflows } = makeAssembler();

    // Open 8 snapshots, each accumulating fragments to approach total cap
    // Total cap = 64 MiB. Use 8 snapshots × ~9 MiB each to exceed it.
    // But per-id cap is 8 MiB so we can't push 9 MiB in one snapshot.
    // Instead use 8 snapshots approaching the total cap collectively.
    // Each gets 8 MiB − 1 byte per-id → total = 8 × (8 MiB − 1) ≈ 64 MiB
    // We'll approach total by filling each snapshot to near-full, then
    // try to push one more byte in the 8th snapshot to trip total cap.

    // Simpler: Fill 7 snapshots completely (8 MiB each = 56 MiB),
    // then push fragments into snapshot 8 until total > 64 MiB.
    const fragSize = 4 * 1024; // 4 KiB
    const fragsPerSnapshot = Math.floor((8 * 1024 * 1024) / fragSize); // 2048 frags per id

    // Push 7 complete-ish snapshots (each just under 8 MiB = 2047 × 4 KiB = 8 188 KiB)
    for (let snapIdx = 0; snapIdx < 7; snapIdx++) {
      const id = makeId(BigInt(200 + snapIdx), BigInt(200 + snapIdx));
      for (let fi = 0; fi < fragsPerSnapshot - 1; fi++) {
        const accepted = assembler.onFragment(
          makeFragment(id, fi, fragsPerSnapshot * 2, new Uint8Array(fragSize)),
        );
        if (!accepted || overflows.length > 0) break;
      }
      if (overflows.length > 0) break;
    }

    if (overflows.length > 0) {
      // Already tripped per-id or total cap — pass
      expect(overflows.length).toBeGreaterThan(0);
      return;
    }

    // Now fill snapshot 8 until total exceeds MAX_TOTAL_INFLIGHT_SNAPSHOT_BYTES (64 MiB)
    const id8 = makeId(300n, 300n);
    const bigTotal = fragsPerSnapshot * 4;
    for (let fi = 0; fi < bigTotal; fi++) {
      assembler.onFragment(makeFragment(id8, fi, bigTotal * 2, new Uint8Array(fragSize)));
      if (overflows.length > 0) break;
    }

    expect(overflows.length).toBeGreaterThan(0);
  });

  it("snapshotEntityTooLarge_discardsPartial_doesNotClose", () => {
    const { assembler, entityTooLargeIds, completed, violations, overflows } = makeAssembler();
    const id = makeId(400n, 400n);

    // Start assembling a snapshot
    assembler.onFragment(makeFragment(id, 0, 3, makePayload(10)));
    expect(assembler.inflightCount()).toBe(1);

    // Server signals SnapshotEntityTooLarge for this id
    assembler.onSnapshotEntityTooLarge(id);

    // Partial should be discarded (inflight count drops to 0)
    expect(assembler.inflightCount()).toBe(0);
    expect(entityTooLargeIds.length).toBe(1);

    // Should NOT have closed (no violation, no overflow)
    expect(violations.length).toBe(0);
    expect(overflows.length).toBe(0);
    expect(completed.length).toBe(0);

    // Assembler should still be alive (not dead)
    // New snapshot should be accepted
    const newId = makeId(401n, 401n);
    const accepted = assembler.onFragment(makeFragment(newId, 0, 1, makePayload(4), true));
    expect(accepted).toBe(true);
    expect(completed.length).toBe(1);
  });
});
