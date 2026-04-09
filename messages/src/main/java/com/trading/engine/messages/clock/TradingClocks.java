package com.trading.engine.messages.clock;

import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.OffsetEpochNanoClock;
import org.agrona.concurrent.SystemNanoClock;

/**
 * Standard clock factory for the trading engine. All non-cluster processes that need clocks MUST
 * use this factory.
 *
 * <p>Inside the cluster service: NEVER call this — use the cluster-supplied timestamp from {@code
 * onSessionMessage} / {@code onTimerEvent} callbacks.
 *
 * <p>Cross-box consistency requires PTP (IEEE 1588) or chrony at the OS level.
 */
public final class TradingClocks {

  private TradingClocks() {}

  /**
   * Returns the shared, zero-allocation epoch nanosecond clock backed by Agrona's {@link
   * OffsetEpochNanoClock}, which anchors {@code System.nanoTime()} to the epoch at construction and
   * periodically re-anchors. Thread-safe — safe to share across all threads in a process.
   *
   * <p>Uses the lazy-holder idiom so the clock is not constructed until the first call, avoiding
   * premature epoch-anchor sampling during class scanning or test-framework loading.
   */
  public static EpochNanoClock epochNanoClock() {
    return Holder.INSTANCE;
  }

  /**
   * Returns the shared monotonic nanosecond clock backed by Agrona's {@link SystemNanoClock}, which
   * delegates to {@code System.nanoTime()}. Thread-safe. Use for elapsed time and timeouts — not
   * for wall-clock timestamps.
   */
  public static NanoClock nanoClock() {
    return SystemNanoClock.INSTANCE;
  }

  private static final class Holder {
    static final EpochNanoClock INSTANCE = new OffsetEpochNanoClock();
  }
}
