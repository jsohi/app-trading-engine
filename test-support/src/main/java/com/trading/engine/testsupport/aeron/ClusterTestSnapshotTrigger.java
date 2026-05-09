package com.trading.engine.testsupport.aeron;

import io.aeron.cluster.ClusterControl;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;

/**
 * Test-only helper that triggers a cluster snapshot in-process via Aeron's {@link
 * ClusterControl#findControlToggle(CountersReader)} + {@link ClusterControl.ToggleState#SNAPSHOT}
 * mechanism. Aeron 1.50.4 has no in-process {@code Cluster.takeSnapshot()} method; the only
 * supported trigger is the control-toggle counter, which integration tests exercise via this
 * wrapper.
 *
 * <p>Used by {@code RfqSnapshotRecoveryIT} and any other integration test that needs to drive a
 * snapshot mid-flow without spawning {@code ClusterTool snapshot} as an external process.
 *
 * <p><b>Threading.</b> Test-only — single-threaded usage. The underlying counter writes are
 * volatile / lock-free.
 *
 * <p><b>Allocation.</b> Allocates only at call time of {@link #trigger}; not on any production hot
 * path.
 *
 * @see io.aeron.cluster.ClusterControl
 * @see io.aeron.cluster.ClusterControl.ToggleState#SNAPSHOT
 */
public final class ClusterTestSnapshotTrigger {

  private ClusterTestSnapshotTrigger() {}

  /**
   * Triggers a snapshot on the cluster identified by the given counters reader and clusterId.
   * Locates the cluster's control-toggle counter and writes the {@code SNAPSHOT} toggle state.
   *
   * @param countersReader the cluster's CnC counters reader (typically obtained via {@code
   *     aeron.countersReader()})
   * @param clusterId the cluster's id (typically 0 for single-cluster deployments)
   * @return {@code true} if the toggle was applied; {@code false} if the toggle was rejected
   *     because the consensus module was already mid-state (e.g., already snapshotting)
   * @throws IllegalStateException if no control-toggle counter can be located in the reader —
   *     indicates the cluster is not running or counters are not yet published
   */
  public static boolean trigger(final CountersReader countersReader, final int clusterId) {
    final AtomicCounter toggle = ClusterControl.findControlToggle(countersReader, clusterId);
    if (toggle == null) {
      throw new IllegalStateException(
          "no cluster control-toggle counter found — is the cluster running?");
    }
    return ClusterControl.ToggleState.SNAPSHOT.toggle(toggle);
  }
}
