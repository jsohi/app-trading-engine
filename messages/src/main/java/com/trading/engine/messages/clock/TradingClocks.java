package com.trading.engine.messages.clock;

import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.OffsetEpochNanoClock;

/**
 * Standard clock factory for the trading engine. All non-cluster processes that need epoch time
 * MUST use this factory.
 *
 * <p>Inside the cluster service: NEVER call this — use the cluster-supplied timestamp from {@code
 * onSessionMessage} / {@code onTimerEvent} callbacks.
 *
 * <p>Cross-box consistency requires PTP (IEEE 1588) or chrony at the OS level.
 */
public final class TradingClocks {

  private static final EpochNanoClock INSTANCE = new OffsetEpochNanoClock();

  private TradingClocks() {}

  /**
   * Returns the shared, zero-allocation epoch nanosecond clock backed by Agrona's {@link
   * OffsetEpochNanoClock}, which anchors {@code System.nanoTime()} to the epoch at construction and
   * periodically re-anchors. Thread-safe — safe to share across all threads in a process.
   */
  public static EpochNanoClock epochNanoClock() {
    return INSTANCE;
  }
}
