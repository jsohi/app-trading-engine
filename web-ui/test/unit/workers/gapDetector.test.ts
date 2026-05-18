/**
 * gapDetector.test.ts — unit tests for GapDetector per Phase 3 Commit B.
 *
 * Covers: first-tick; in-order; gap attributed to network (no heartbeat
 * cursor); gap attributed to publisher (heartbeat cursor covers the gap);
 * out-of-order (older seq); snapshot exemption seq=0; publisher-restart
 * clears all state; multi-symbol independence.
 *
 * Test naming follows `<unit>_<scenario>_<expectedBehavior>` per plan §5.8.
 *
 * Threading: single-threaded (Vitest jsdom).
 */

import { describe, expect, it, beforeEach } from "vitest";
import { GapDetector } from "@/workers/gapDetector";
import { pack } from "@/shared/transport/SymbolPacking";

// ─── Fixtures ────────────────────────────────────────────────────────────────

const EUR = pack("EURUSD");
const GBP = pack("GBPUSD");

// ─── Tests ───────────────────────────────────────────────────────────────────

describe("GapDetector", () => {
  let detector: GapDetector;

  beforeEach(() => {
    detector = new GapDetector();
  });

  it("onTick_firstTickForSymbol_returnsFirstTickOutcome", () => {
    const report = detector.onTick(EUR, 1);

    expect(report.outcome).toBe("first-tick");
    expect(report.publisherConflated).toBe(0);
    expect(report.network).toBe(0);
  });

  it("onTick_consecutiveInOrderSeq_returnsInOrderOutcome", () => {
    detector.onTick(EUR, 1); // first-tick
    const report = detector.onTick(EUR, 2);

    expect(report.outcome).toBe("in-order");
    expect(report.publisherConflated).toBe(0);
    expect(report.network).toBe(0);
  });

  it("onTick_gapWithNoHeartbeatCursor_attributesEntireGapToNetwork", () => {
    detector.onTick(EUR, 1); // first-tick, cursor = 1
    // Seq 2 and 3 are missing — total gap = 2
    const report = detector.onTick(EUR, 4);

    expect(report.outcome).toBe("gap");
    // No heartbeat cursor → cannot attribute to publisher → all network
    expect(report.publisherConflated).toBe(0);
    expect(report.network).toBe(2);
  });

  it("onTick_gapWithHeartbeatCursorCovers_stillAttributesToNetwork_perGeminiR2Fix", () => {
    detector.onTick(EUR, 1); // cursor = 1
    // Heartbeat says publisher has published up to seq=5.
    // Per Gemini iter-2 review (HIGH, gapDetector.ts:207): publisher conflation reduces
    // the COUNT of emitted messages — conflated updates are never assigned a seq, so a
    // seq-counter gap ALWAYS reflects messages the publisher DID publish but the browser
    // failed to receive (network drop). The publisher cursor being current means the
    // publisher SENT those missing seqs → they are network drops, NOT conflation drops.
    detector.onHeartbeat(EUR, 5);
    const report = detector.onTick(EUR, 4); // seqs 2,3 missing

    expect(report.outcome).toBe("gap");
    expect(report.publisherConflated).toBe(0);
    expect(report.network).toBe(2);
  });

  it("onTick_olderSeqThanCursor_returnsOutOfOrderOutcome", () => {
    detector.onTick(EUR, 5); // cursor = 5
    const report = detector.onTick(EUR, 3); // regressed

    expect(report.outcome).toBe("out-of-order");
    expect(report.publisherConflated).toBe(0);
    expect(report.network).toBe(0);
    // Cursor must NOT have been updated (still 5)
    const nextInOrder = detector.onTick(EUR, 6);
    expect(nextInOrder.outcome).toBe("in-order");
  });

  it("onTick_symbolSeqZero_returnsSnapshotOutcome_nextLiveTickIsFirstTick", () => {
    const snapshotReport = detector.onTick(EUR, 0);

    expect(snapshotReport.outcome).toBe("snapshot");
    expect(snapshotReport.publisherConflated).toBe(0);
    expect(snapshotReport.network).toBe(0);

    // Per Gemini iter-2 review (HIGH, gapDetector.ts:161): snapshots in this protocol
    // do not carry the actual sequence number of the latest published update, so the
    // snapshot resets the cursor to NO_PRIOR_SEQ — the next live tick (regardless of
    // its sequence number) is classified as `first-tick`, NOT in-order. This avoids
    // a false gap when the next live tick has a seq number > 1.
    const liveReport = detector.onTick(EUR, 5);
    expect(liveReport.outcome).toBe("first-tick");
    expect(liveReport.publisherConflated).toBe(0);
    expect(liveReport.network).toBe(0);
  });

  it("onTick_multipleConsecutiveSnapshotFrames_allReturnSnapshot_noGap_nextLiveIsFirstTick", () => {
    // Burst of snapshot frames (seq=0) must be idempotent and never gap.
    expect(detector.onTick(EUR, 0).outcome).toBe("snapshot");
    expect(detector.onTick(EUR, 0).outcome).toBe("snapshot");
    expect(detector.onTick(EUR, 0).outcome).toBe("snapshot");

    // Per Gemini iter-2 review (HIGH, gapDetector.ts:161): the next live tick after a
    // snapshot burst is `first-tick` (cursor was reset to NO_PRIOR_SEQ), not in-order.
    expect(detector.onTick(EUR, 1).outcome).toBe("first-tick");
  });

  it("onPublisherRestart_clearsCursors_nextTickIsTreatedAsFirstTick", () => {
    detector.onTick(EUR, 100);
    detector.onHeartbeat(EUR, 100);

    detector.onPublisherRestart();

    // After restart, seq=1 should appear as first-tick, not a huge gap from 100→1
    const report = detector.onTick(EUR, 1);
    expect(report.outcome).toBe("first-tick");
    expect(report.publisherConflated).toBe(0);
    expect(report.network).toBe(0);
  });

  it("onTick_multiSymbol_trackersAreIndependent", () => {
    // EUR at seq=10, GBP at seq=20 — both first-tick
    detector.onTick(EUR, 10);
    detector.onTick(GBP, 20);

    // EUR next in-order is 11
    expect(detector.onTick(EUR, 11).outcome).toBe("in-order");
    // GBP next in-order is 21
    expect(detector.onTick(GBP, 21).outcome).toBe("in-order");
    // EUR gap (skipped 12) should not affect GBP
    const eurGap = detector.onTick(EUR, 13);
    expect(eurGap.outcome).toBe("gap");
    expect(eurGap.network).toBe(1);
    // GBP still in-order
    expect(detector.onTick(GBP, 22).outcome).toBe("in-order");
  });

  it("onPublisherRestart_clearsPublisherCursorToo_subsequentGapAttributedToNetwork", () => {
    detector.onTick(EUR, 1);
    detector.onHeartbeat(EUR, 50); // publisher cursor set to 50
    detector.onPublisherRestart();

    // After restart, new publisher at seq=1 → first-tick
    detector.onTick(EUR, 1);
    // Gap: seq 2,3 missing; publisher cursor was cleared so full gap → network
    const report = detector.onTick(EUR, 4);
    expect(report.outcome).toBe("gap");
    expect(report.publisherConflated).toBe(0);
    expect(report.network).toBe(2);
  });

  it("symbolCount_tracksDistinctSymbols", () => {
    expect(detector.symbolCount()).toBe(0);
    detector.onTick(EUR, 1);
    expect(detector.symbolCount()).toBe(1);
    detector.onTick(GBP, 1);
    expect(detector.symbolCount()).toBe(2);
    detector.onPublisherRestart();
    expect(detector.symbolCount()).toBe(0);
  });

  it("dispose_clearsAllState", () => {
    detector.onTick(EUR, 5);
    detector.onHeartbeat(EUR, 5);
    detector.dispose();

    expect(detector.symbolCount()).toBe(0);
    // After dispose, EUR is treated as first-tick again (no stale cursor)
    expect(detector.onTick(EUR, 1).outcome).toBe("first-tick");
  });
});
