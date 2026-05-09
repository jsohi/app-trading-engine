/**
 * SnapshotAssembler — reassembles fragmented WebSocketSnapshot (template
 * 66) frames into logical snapshots per APP-36 §2.10.
 *
 * Per-`snapshotId` invariants:
 *   - Fragment indices are 0..totalFragments-1 (validated).
 *   - Duplicate fragment-index for the same snapshotId → close.
 *   - No non-snapshot reliable frame may interleave between fragments
 *     of the same snapshotId → close.
 *   - 30 s completion deadline from first fragment → if not satisfied,
 *     discard partial + close BUFFER_OVERFLOW.
 *   - Per-fragment 16 KiB cap.
 *   - Per-snapshotId 8 MiB cap.
 *   - Total in-flight 64 MiB cap.
 *   - ≤ 8 concurrent unfinalised snapshotIds.
 *
 * Threading: worker scope only.
 *
 * Allocation: per fragment, one Uint8Array copy (parser views are
 * invalidated). On finalisation, one concatenated `Uint8Array` per
 * complete snapshot (Transferable to main thread per §4.5 snapshot-
 * exemption).
 *
 * Plan reference: §2.10 / §6 row 12.
 */

import {
  MAX_FRAGMENT_BYTES,
  MAX_INFLIGHT_SNAPSHOT_IDS,
  MAX_SNAPSHOT_BYTES_PER_ID,
  MAX_TOTAL_INFLIGHT_SNAPSHOT_BYTES,
  SNAPSHOT_COMPLETION_DEADLINE_MS,
} from "@/workers/WorkerTuning";

import { type UuidComposite } from "@/workers/session/SessionState";

export interface SnapshotFragment {
  readonly snapshotId: UuidComposite;
  readonly fragmentIndex: number;
  readonly totalFragments: number;
  readonly payload: Uint8Array;
  /** True iff this is the final fragment (FLAG_SNAPSHOT_FINAL). */
  readonly isFinal: boolean;
}

export interface AssembledSnapshot {
  readonly snapshotId: UuidComposite;
  readonly bytes: Uint8Array;
}

export interface SnapshotAssemblerCallbacks {
  /** Invoked on a complete snapshot. Caller emits via Transferable postMessage per §4.5. */
  onSnapshotComplete: (snap: AssembledSnapshot) => void;
  /** Invoked on a protocol violation (e.g. duplicate fragment, > 8 ids, interleave). */
  onProtocolViolation: (reason: string) => void;
  /** Invoked on a buffer/byte/deadline overflow. */
  onBufferOverflow: (reason: string) => void;
  /** Invoked when the server signals SnapshotEntityTooLarge (code 12); discard partial; do not close. */
  onSnapshotEntityTooLarge: (snapshotId: UuidComposite) => void;
}

interface InflightSnapshot {
  readonly snapshotId: UuidComposite;
  readonly idKey: string;
  readonly totalFragments: number;
  readonly fragments: Uint8Array[];
  filledFragments: number;
  bytesAccumulated: number;
  readonly startedAtMs: number;
}

function uuidKey(id: UuidComposite): string {
  return `${id.mostSignificantBits.toString(16)}_${id.leastSignificantBits.toString(16)}`;
}

export class SnapshotAssembler {
  private readonly inflight = new Map<string, InflightSnapshot>();
  private totalBytesInflight = 0;
  private dead = false;
  private readonly cb: SnapshotAssemblerCallbacks;
  private readonly nowMs: () => number;

  constructor(callbacks: SnapshotAssemblerCallbacks, nowMs: () => number) {
    this.cb = callbacks;
    this.nowMs = nowMs;
  }

  /**
   * Apply a snapshot fragment. Returns true iff the fragment was
   * accepted; false on any error path (caller should not deliver).
   */
  onFragment(frag: SnapshotFragment): boolean {
    if (this.dead) return false;

    if (frag.payload.length > MAX_FRAGMENT_BYTES) {
      this.fail("buffer-overflow", `fragment ${String(frag.payload.length)} > MAX_FRAGMENT_BYTES`);
      return false;
    }
    if (frag.fragmentIndex < 0 || frag.fragmentIndex >= frag.totalFragments) {
      this.protocolViolation(
        `fragmentIndex ${String(frag.fragmentIndex)} out of [0, ${String(frag.totalFragments)})`,
      );
      return false;
    }

    const key = uuidKey(frag.snapshotId);
    let inflight = this.inflight.get(key);
    if (inflight === undefined) {
      // New snapshotId. Enforce concurrent-id cap.
      if (this.inflight.size >= MAX_INFLIGHT_SNAPSHOT_IDS) {
        this.protocolViolation(
          `> MAX_INFLIGHT_SNAPSHOT_IDS (${String(MAX_INFLIGHT_SNAPSHOT_IDS)}) concurrent snapshots`,
        );
        return false;
      }
      // Drop any expired snapshots before admitting a new one.
      this.expireStale();
      // Per Gemini review R9 (MEDIUM): hard-cap `totalFragments`. The
      // sparse-array fix from R8 prevents OOM at admission time, but
      // `finaliseAndEmit` later iterates `[0, totalFragments)` to
      // concatenate fragments — a 2^32 value would hang the worker
      // thread regardless of how few fragments actually arrived.
      // 1_000_000 is far above any plausible legitimate snapshot
      // (8 MiB / 8B = 1M absolute floor; 8 MiB / 16 KiB = 512 typical)
      // and well below the loop-hang threshold.
      const MAX_TOTAL_FRAGMENTS = 1_000_000;
      if (frag.totalFragments > MAX_TOTAL_FRAGMENTS) {
        this.protocolViolation(
          `totalFragments ${String(frag.totalFragments)} > MAX_TOTAL_FRAGMENTS (${String(MAX_TOTAL_FRAGMENTS)})`,
        );
        return false;
      }
      inflight = {
        snapshotId: frag.snapshotId,
        idKey: key,
        totalFragments: frag.totalFragments,
        // Per Gemini review R8 (HIGH): use a sparse array. `fragments[i]
        // = ...` allocates slots lazily; unused indices stay holes (no
        // eager `new Array(N)` allocation from an attacker-controlled
        // length). Memory growth is upper-bounded by
        // `bytesAccumulated <= MAX_SNAPSHOT_BYTES_PER_ID` (8 MiB) and
        // by `totalBytesInflight <= MAX_TOTAL_INFLIGHT_SNAPSHOT_BYTES`
        // (64 MiB) regardless of the wire-supplied totalFragments.
        fragments: [],
        filledFragments: 0,
        bytesAccumulated: 0,
        startedAtMs: this.nowMs(),
      };
      this.inflight.set(key, inflight);
    } else if (inflight.totalFragments !== frag.totalFragments) {
      this.protocolViolation(
        `totalFragments mismatch (got ${String(frag.totalFragments)} expected ${String(inflight.totalFragments)})`,
      );
      return false;
    }

    if (inflight.fragments[frag.fragmentIndex] !== undefined) {
      this.protocolViolation(`duplicate fragmentIndex ${String(frag.fragmentIndex)}`);
      return false;
    }

    // Copy payload — parser view is invalidated on next feed().
    const copy = new Uint8Array(frag.payload.length);
    copy.set(frag.payload);
    inflight.fragments[frag.fragmentIndex] = copy;
    inflight.filledFragments += 1;
    inflight.bytesAccumulated += copy.length;
    this.totalBytesInflight += copy.length;

    if (inflight.bytesAccumulated > MAX_SNAPSHOT_BYTES_PER_ID) {
      this.fail(
        "buffer-overflow",
        `snapshot ${String(inflight.bytesAccumulated)} > MAX_SNAPSHOT_BYTES_PER_ID`,
      );
      return false;
    }
    if (this.totalBytesInflight > MAX_TOTAL_INFLIGHT_SNAPSHOT_BYTES) {
      this.fail(
        "buffer-overflow",
        `total ${String(this.totalBytesInflight)} > MAX_TOTAL_INFLIGHT_SNAPSHOT_BYTES`,
      );
      return false;
    }

    if (inflight.filledFragments === inflight.totalFragments) {
      this.finaliseAndEmit(inflight);
    }
    // Per Gemini review (MEDIUM): the FLAG_SNAPSHOT_FINAL bit is a hint,
    // not an ordering invariant. Fragments may legitimately arrive
    // out-of-order (the assembler indexes by `fragmentIndex`); the final
    // flag may therefore appear on any fragment whose index is the
    // highest-emitted-so-far rather than strictly last-arriving. Trust
    // `filledFragments === totalFragments` as the sole completion gate;
    // do NOT trip protocolViolation just because final arrived first.
    return true;
  }

  /**
   * Server signalled `SnapshotEntityTooLarge` (code 12) — discard partial,
   * do NOT close. Caller surfaces the error to UI.
   */
  onSnapshotEntityTooLarge(snapshotId: UuidComposite): void {
    const key = uuidKey(snapshotId);
    const inflight = this.inflight.get(key);
    if (inflight !== undefined) {
      this.totalBytesInflight -= inflight.bytesAccumulated;
      this.inflight.delete(key);
    }
    this.cb.onSnapshotEntityTooLarge(snapshotId);
  }

  /**
   * Periodic tick from the heartbeat / backpressure timer — checks
   * per-snapshot completion deadlines and expires stale ids.
   */
  onTimerTick(): void {
    this.expireStale();
  }

  /**
   * Caller observed a non-snapshot reliable frame interleaved with a
   * snapshot in flight. Per §2.10, this is a protocol violation.
   *
   * Two overloads:
   *   - No arg: caller doesn't know which id is in flight (typical:
   *     `worker.ts` only sees the inbound frame's flags, not which
   *     snapshot is mid-reassembly). The assembler picks any inflight
   *     id for the violation message.
   *   - With `snapshotId`: caller has a specific id (test path).
   */
  onNonSnapshotInterleave(snapshotId?: UuidComposite): void {
    let key: string;
    if (snapshotId !== undefined) {
      key = uuidKey(snapshotId);
    } else {
      const firstKey = this.inflight.keys().next();
      key = firstKey.done === true ? "<none>" : firstKey.value;
    }
    this.protocolViolation(`non-snapshot reliable frame interleaved with snapshot ${key}`);
  }

  /**
   * True iff at least one snapshot is mid-reassembly. Caller checks
   * before invoking `onNonSnapshotInterleave()` (Gemini review R7
   * MEDIUM): only fire the violation when a snapshot is actually
   * in flight.
   */
  hasInflightSnapshots(): boolean {
    return this.inflight.size > 0;
  }

  /** Reset on session cold-start. */
  coldStart(): void {
    this.inflight.clear();
    this.totalBytesInflight = 0;
    this.dead = false;
  }

  /** Visible for tests. */
  inflightCount(): number {
    return this.inflight.size;
  }

  /** Visible for tests. */
  totalInflightBytes(): number {
    return this.totalBytesInflight;
  }

  private expireStale(): void {
    if (this.dead) return;
    const now = this.nowMs();
    for (const inflight of this.inflight.values()) {
      if (now - inflight.startedAtMs > SNAPSHOT_COMPLETION_DEADLINE_MS) {
        this.fail(
          "buffer-overflow",
          `snapshot ${inflight.idKey} did not complete within ${String(
            SNAPSHOT_COMPLETION_DEADLINE_MS,
          )} ms`,
        );
        return;
      }
    }
  }

  private finaliseAndEmit(inflight: InflightSnapshot): void {
    // Concatenate fragments in order. One allocation per snapshot.
    const total = new Uint8Array(inflight.bytesAccumulated);
    let offset = 0;
    for (let i = 0; i < inflight.totalFragments; i++) {
      const fragBytes = inflight.fragments[i];
      if (fragBytes === undefined) {
        // Defensive — should be caught by filledFragments accounting.
        this.protocolViolation(`fragment ${String(i)} missing at finalise`);
        return;
      }
      total.set(fragBytes, offset);
      offset += fragBytes.length;
    }
    this.totalBytesInflight -= inflight.bytesAccumulated;
    this.inflight.delete(inflight.idKey);
    this.cb.onSnapshotComplete({ snapshotId: inflight.snapshotId, bytes: total });
  }

  private protocolViolation(reason: string): void {
    if (this.dead) return;
    this.dead = true;
    this.cb.onProtocolViolation(reason);
  }

  private fail(_kind: "buffer-overflow", reason: string): void {
    if (this.dead) return;
    this.dead = true;
    this.cb.onBufferOverflow(reason);
  }
}
