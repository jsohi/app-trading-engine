package com.trading.engine.e2e;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * APP-225 §D7 reproducer — slow-consumer HOL blocking mid-replay.
 *
 * <p><b>Invariant under test:</b> when a client's WS recv-buffer fills mid-replay, the server MUST
 * evict the slow consumer (per APP-244 DWRR contract) rather than backpressuring the cluster event
 * sink.
 *
 * <p><b>Defect class:</b> backpressure-induced HOL (head-of-line) blocking.
 *
 * <p><b>Status:</b> SKELETON. Disabled pending the implementation strategy. This reproducer needs a
 * real MediaDriver topology plus a back-pressured NIO sink that stalls the slow client's
 * recv-buffer at a configurable fill level mid-replay. The assertion must confirm that the slow
 * client is evicted (connection closed by server) and that fast-path clients sharing the same
 * cluster event sink continue to receive events without stalling. APP-244 must be in tree for the
 * DWRR eviction policy to be available.
 *
 * <p><b>Threading:</b> single-threaded JUnit test method; the harness it eventually drives is
 * multi-process (real MediaDriver + cluster + gateway).
 *
 * <p><b>Allocation:</b> test path; allocation acceptable.
 */
@Tag("repro-d7")
@Disabled("APP-225 §D7 skeleton — pending harness + back-pressured NIO sink + APP-244 in tree")
final class SlowConsumerMidReplayReproTest {

  @Test
  void slowConsumerMidReplay_reproducesDefect_evictsSlowConsumerNotBackpressuresSink() {
    // TODO(APP-225 §D7): implement the failure-injection harness for slow-consumer HOL blocking.
    // Steps:
    //   1. Spin up a real MediaDriver + 3-node cluster + gateway + WebSocket server.
    //   2. Establish two WebSocket sessions: a "fast" client and a "slow" client.
    //   3. Trigger a replay of N events (e.g., 10 000 order snapshots) to both clients.
    //   4. Stall the slow client's NIO recv-buffer at ~50 % fill using a throttled NIO shim.
    //   5. Assert: the server evicts the slow client (connection closed with appropriate
    //      WS close code) within the DWRR eviction window (APP-244 contract).
    //   6. Assert: the fast client continues to receive events without any stall or gap.
    //   7. Assert: the cluster event sink's subscription backlog does not grow after eviction.
    // Acceptance: slow consumer evicted per DWRR; cluster event sink is not backpressured.
    throw new UnsupportedOperationException(
        "APP-225 §D7 SlowConsumerMidReplayReproTest — see class Javadoc.");
  }
}
