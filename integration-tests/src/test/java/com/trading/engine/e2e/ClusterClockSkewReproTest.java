package com.trading.engine.e2e;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * APP-225 §D7 reproducer — clock-skew safety under excessive wall-clock drift.
 *
 * <p><b>Invariant under test:</b> when two cluster nodes have wall-clock drift &gt; 5 s (configured
 * threshold), the cluster MUST refuse to elect a new leader rather than commit timestamps that
 * would corrupt the deterministic log.
 *
 * <p><b>Defect class:</b> clock-skew safety.
 *
 * <p><b>Status:</b> SKELETON. Disabled pending the implementation strategy. This reproducer needs a
 * real 3-node Aeron Cluster topology with an injectable {@code NanosecondClusterClock} (or
 * equivalent clock-offset shim) that can advance one node's perceived wall time by &gt; 5 s
 * relative to the others. The assertion must confirm that the cluster does NOT elect a new leader
 * (no leadership-change event observed) and logs a clock-skew guard event within the detection
 * window. The 5 s threshold must be a configurable constant so the test can set it to a small value
 * (e.g., 100 ms) without requiring actual system-time manipulation.
 *
 * <p><b>Threading:</b> single-threaded JUnit test method; the harness it eventually drives is
 * multi-process (real MediaDriver + 3-node cluster).
 *
 * <p><b>Allocation:</b> test path; allocation acceptable.
 */
@Tag("repro-d7")
@Disabled(
    "APP-225 §D7 skeleton — pending harness + injectable cluster clock + skew threshold config")
final class ClusterClockSkewReproTest {

  @Test
  void clusterNodeWallClockDrift_reproducesDefect_clusterRefusesLeaderElection() {
    // TODO(APP-225 §D7): implement the failure-injection harness for clock-skew safety.
    // Steps:
    //   1. Spin up a real 3-node Aeron Cluster with an injectable NanosecondClusterClock
    //      (or clock-offset shim) and a small skew threshold (e.g., 100 ms for test speed).
    //   2. Kill the current leader to trigger a re-election.
    //   3. Advance the skewed node's injected clock by threshold + 1 ms before it can vote.
    //   4. Monitor for leadership-change events over the next election timeout window.
    //   5. Assert: no new leader is elected (leadership-change event NOT observed).
    //   6. Assert: the skewed node emits a clock-skew guard log entry / metric counter.
    //   7. Reset the clock offset and assert that a leader IS elected in the next round.
    // Acceptance: cluster refuses election when skew > threshold; recovers after correction.
    throw new UnsupportedOperationException(
        "APP-225 §D7 ClusterClockSkewReproTest — see class Javadoc.");
  }
}
