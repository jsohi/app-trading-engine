package com.trading.engine.e2e;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * APP-225 §D7 reproducer — leader-failover gap handling.
 *
 * <p><b>Invariant under test:</b> when the cluster leader is killed mid-flight, any in-flight NOS
 * that the gateway has already published to the cluster log MUST surface either an {@code
 * OrderCreatedEvent} or an {@code OrderRejectedEvent} from the new leader — no command is silently
 * dropped.
 *
 * <p><b>Defect class:</b> leader-failover gap handling.
 *
 * <p><b>Status:</b> SKELETON. Disabled pending the implementation strategy. This reproducer needs a
 * real 3-node Aeron Cluster topology plus a mechanism to SIGKILL the leader node immediately after
 * the gateway's {@code Publication.offer()} returns {@code BACK_PRESSURED} or a positive position —
 * i.e., after the cluster log has accepted the entry but before the state machine commits it. The
 * assertion must confirm that the surviving followers elect a new leader and that every NOS
 * admitted to the log produces exactly one outcome event (created or rejected), with no silent
 * drops detectable via event-sequence gap analysis.
 *
 * <p><b>Threading:</b> single-threaded JUnit test method; the harness it eventually drives is
 * multi-process (real MediaDriver + 3-node cluster + gateway).
 *
 * <p><b>Allocation:</b> test path; allocation acceptable.
 */
@Tag("repro-d7")
@Disabled("APP-225 §D7 skeleton — pending harness + leader SIGKILL mid-flight + failover detection")
final class ClusterNodeFailoverReproTest {

  @Test
  void clusterLeaderKilledMidFlight_reproducesDefect_inFlightNosProducesOutcomeFromNewLeader() {
    // TODO(APP-225 §D7): implement the failure-injection harness for leader-failover gap handling.
    // Steps:
    //   1. Spin up a real 3-node Aeron Cluster + MediaDriver + gateway.
    //   2. Identify the current leader node (cluster member with leadership role).
    //   3. Submit a NOS and immediately SIGKILL the leader node after Publication.offer()
    //      returns a positive position (entry admitted to log).
    //   4. Wait for the remaining two nodes to elect a new leader (Raft election).
    //   5. Assert: the gateway eventually receives either OrderCreatedEvent or
    //      OrderRejectedEvent for the in-flight NOS — not a timeout / silent drop.
    //   6. Assert: event sequence numbers have no gap spanning the leader transition.
    // Acceptance: every log-admitted NOS produces exactly one outcome; no silent drops.
    throw new UnsupportedOperationException(
        "APP-225 §D7 ClusterNodeFailoverReproTest — see class Javadoc.");
  }
}
