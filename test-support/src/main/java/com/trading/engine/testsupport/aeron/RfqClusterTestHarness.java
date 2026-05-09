package com.trading.engine.testsupport.aeron;

import io.aeron.cluster.service.Cluster;
import java.util.ArrayList;
import java.util.List;

/**
 * Test harness that captures {@link Cluster#scheduleTimer} invocations from production code so
 * integration tests can drive {@code RfqStateMachine.onTimerExpiry} manually at the recorded
 * deadlines. Subclass of {@link FakeCluster} that returns {@code true} from {@code scheduleTimer}
 * (instead of FakeCluster's default {@code false} which would always trigger the
 * timer-pool-exhausted reject path).
 *
 * <p>Used by {@code RfqLifecycleEventsIT} and {@code RfqSnapshotRecoveryIT}. Not for hot-path
 * production code.
 *
 * <p><b>Threading.</b> Single-threaded — IT driver thread.
 *
 * <p><b>Allocation.</b> Allocates ArrayList entries on every scheduleTimer call. Acceptable for IT
 * use; not on production hot path.
 *
 * @see ClusterTestSnapshotTrigger
 * @see FakeCluster
 */
public final class RfqClusterTestHarness extends FakeCluster {

  /**
   * Captured timer schedules in invocation order. Each entry: [correlationId, deadlineNanos]. Test
   * code drains this list and dispatches {@code onTimerEvent(correlationId, deadlineNanos)} on the
   * service to simulate timer firing at the deadline.
   */
  public final List<long[]> scheduledTimers = new ArrayList<>();

  /**
   * If non-null, the next call to {@link #scheduleTimer} returns this value instead of the default
   * {@code true}. Used to simulate the timer-pool-exhausted reject path.
   */
  public Boolean scheduleTimerNextResult;

  /**
   * Creates a harness with a fixed cluster timestamp.
   *
   * @param time the fixed timestamp returned by {@code time()}; cluster nanos
   */
  public RfqClusterTestHarness(final long time) {
    super(time);
  }

  /**
   * Captures the timer schedule and returns {@code true} (success) by default. Override via {@link
   * #scheduleTimerNextResult}.
   */
  @Override
  public boolean scheduleTimer(final long correlationId, final long deadline) {
    scheduledTimers.add(new long[] {correlationId, deadline});
    if (scheduleTimerNextResult != null) {
      final boolean v = scheduleTimerNextResult;
      scheduleTimerNextResult = null; // one-shot
      return v;
    }
    return true;
  }

  /**
   * Returns the most-recently-scheduled timer, or {@code null} if none. Convenient for ITs that
   * issue one command and assert the resulting timer.
   *
   * @return the [correlationId, deadlineNanos] pair of the most recent {@code scheduleTimer} call
   */
  public long[] lastScheduledTimer() {
    return scheduledTimers.isEmpty() ? null : scheduledTimers.get(scheduledTimers.size() - 1);
  }

  /** Resets the captured schedule list. */
  public void clearScheduledTimers() {
    scheduledTimers.clear();
  }
}
