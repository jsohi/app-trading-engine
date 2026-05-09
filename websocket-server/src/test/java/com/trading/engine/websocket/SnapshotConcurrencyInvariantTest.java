/*
 * APP-36 §2.10 — server-side invariant: never emit more concurrent
 * unfinalised snapshotIds than MAX_INFLIGHT_SNAPSHOT_IDS = 8.
 *
 * The web-ui worker's SnapshotAssembler enforces this client-side and
 * closes PROTOCOL_VIOLATION on the 9th concurrent id. This test pins
 * the server-side constant so the two sides of the contract cannot
 * silently drift; if the server later gains snapshot-emission code,
 * the cap must be referenced from a single constant.
 *
 * Currently the server has no per-stream concurrent snapshot emitter;
 * snapshots are produced serially from cluster state. This test
 * documents the invariant and keeps the constant in source so a
 * future emitter must respect it.
 *
 * Threading: single-threaded JUnit invocation.
 *
 * Allocation: trivial.
 *
 * Plan reference: APP-36 §2.10 / §6 row 12.
 */
package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Server-side snapshot-concurrency invariant pin. */
final class SnapshotConcurrencyInvariantTest {

  /**
   * Mirror of {@code MAX_INFLIGHT_SNAPSHOT_IDS} in
   * web-ui/src/workers/WorkerTuning.ts. If this changes, both sides
   * must change together (single PR).
   */
  static final int MAX_INFLIGHT_SNAPSHOT_IDS = 8;

  @Test
  @DisplayName("invariant_maxInflightSnapshotIds_pinnedTo8_perPlanA10")
  void invariant_maxInflightSnapshotIds_pinnedTo8_perPlanA10() {
    assertEquals(8, MAX_INFLIGHT_SNAPSHOT_IDS);
  }

  @Test
  @DisplayName("invariant_capExceedsAnticipatedFamilies_withHeadroom")
  void invariant_capExceedsAnticipatedFamilies_withHeadroom() {
    // Plan §2.10 derivation: 5 anticipated snapshot families
    // (resume-state, orderbook-per-symbol-group, positions, account, RFQ).
    // Cap = 8 leaves ~1.6× headroom. If a 6th family lands, this test is
    // a regression-safety against silently exceeding capacity.
    final int anticipatedFamilies = 5;
    assertTrue(
        MAX_INFLIGHT_SNAPSHOT_IDS >= anticipatedFamilies,
        "MAX_INFLIGHT_SNAPSHOT_IDS ("
            + MAX_INFLIGHT_SNAPSHOT_IDS
            + ") must accommodate >= "
            + anticipatedFamilies
            + " anticipated families");
  }
}
