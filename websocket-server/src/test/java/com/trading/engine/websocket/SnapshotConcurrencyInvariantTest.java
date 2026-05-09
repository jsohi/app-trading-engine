/*
 * APP-36 §2.10 — server-side invariant: never emit more concurrent
 * unfinalised snapshotIds than the cap that the web-ui worker enforces.
 *
 * The web-ui worker's SnapshotAssembler closes PROTOCOL_VIOLATION on the
 * (cap+1)th concurrent id. This test asserts the cap matches the TS-side
 * `MAX_INFLIGHT_SNAPSHOT_IDS` constant in `web-ui/src/workers/WorkerTuning.ts`
 * by parsing the TS source. If the TS side bumps the constant without
 * updating the server-side cap (this test) — or vice versa — the cross-
 * stack contract drifts and CI fails here.
 *
 * The server has no per-stream concurrent snapshot emitter today; this
 * pin is the source of truth for the invariant a future server emitter
 * MUST respect.
 *
 * Threading: single-threaded JUnit invocation.
 *
 * Allocation: trivial; non-hot-path test.
 *
 * Plan reference: APP-36 §2.10 / §6 row 12.
 */
package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Server-side snapshot-concurrency invariant pin, cross-checked against TS. */
final class SnapshotConcurrencyInvariantTest {

  /**
   * Server-side cap. The CI gate (this test) asserts this equals the TS-side {@code
   * MAX_INFLIGHT_SNAPSHOT_IDS} constant in {@code web-ui/src/workers/WorkerTuning.ts} so the
   * cross-stack contract cannot drift silently.
   */
  static final int MAX_INFLIGHT_SNAPSHOT_IDS = 8;

  /** Matches `export const MAX_INFLIGHT_SNAPSHOT_IDS = <int>;` in WorkerTuning.ts. */
  private static final Pattern TS_CONSTANT =
      Pattern.compile(
          "export\\s+const\\s+MAX_INFLIGHT_SNAPSHOT_IDS\\s*=\\s*(\\d+)\\s*;", Pattern.MULTILINE);

  @Test
  @DisplayName("invariant_serverCapMatchesTsCounterpart_inWorkerTuningTs")
  void invariant_serverCapMatchesTsCounterpart_inWorkerTuningTs() throws IOException {
    final Path tsFile = findRepoRoot().resolve("web-ui/src/workers/WorkerTuning.ts");
    assertTrue(Files.isRegularFile(tsFile), "expected to find " + tsFile);
    final String src = Files.readString(tsFile);
    final Matcher m = TS_CONSTANT.matcher(src);
    assertTrue(m.find(), "could not locate MAX_INFLIGHT_SNAPSHOT_IDS export in " + tsFile);
    final int tsValue = Integer.parseInt(m.group(1));
    assertEquals(
        MAX_INFLIGHT_SNAPSHOT_IDS,
        tsValue,
        "server-side cap ("
            + MAX_INFLIGHT_SNAPSHOT_IDS
            + ") and web-ui WorkerTuning.ts ("
            + tsValue
            + ") must match per APP-36 §2.10 cross-stack contract");
  }

  @Test
  @DisplayName("invariant_capExceedsAnticipatedFamilies_withHeadroom")
  void invariant_capExceedsAnticipatedFamilies_withHeadroom() {
    // Plan §2.10 derivation: 5 anticipated snapshot families
    // (resume-state, orderbook-per-symbol-group, positions, account, RFQ).
    // Cap = 8 leaves ~1.6× headroom. If a 6th family lands, this test is
    // regression-safety against silently exceeding capacity.
    final int anticipatedFamilies = 5;
    assertTrue(
        MAX_INFLIGHT_SNAPSHOT_IDS >= anticipatedFamilies,
        "MAX_INFLIGHT_SNAPSHOT_IDS ("
            + MAX_INFLIGHT_SNAPSHOT_IDS
            + ") must accommodate >= "
            + anticipatedFamilies
            + " anticipated families");
  }

  private static Path findRepoRoot() throws IOException {
    final var start = Paths.get("").toAbsolutePath();
    // Loop control variable `cur` is intentionally mutable per CLAUDE.md
    // carve-out for classic for-loop counters.
    for (Path cur = start; cur != null; cur = cur.getParent()) {
      if (Files.isDirectory(cur.resolve("web-ui"))
          && Files.isRegularFile(cur.resolve("settings.gradle.kts"))) {
        return cur;
      }
    }
    throw new IOException("could not locate repo root from " + start);
  }
}
