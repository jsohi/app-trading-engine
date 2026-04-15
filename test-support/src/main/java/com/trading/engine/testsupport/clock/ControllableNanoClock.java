package com.trading.engine.testsupport.clock;

import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.NanoClock;

/**
 * Deterministic test clock implementing both {@link NanoClock} and {@link EpochNanoClock}.
 *
 * <p>Allows tests to control time precisely without wall-clock dependency. Both interfaces return
 * the same controlled value — this deliberately collapses monotonic (relative) and epoch-anchored
 * (absolute) time into a single controllable value, which is the correct simplification for
 * deterministic testing.
 *
 * <p>Not thread-safe — intended for single-threaded JUnit test methods.
 *
 * <p><b>Allocation.</b> Zero allocation after construction. All operations ({@link #nanoTime()},
 * {@link #advanceNanos(long)}, {@link #advanceMillis(long)}, {@link #advanceSeconds(long)}, {@link
 * #setNanos(long)}) are pure arithmetic on a single {@code long} field.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * var clock = new ControllableNanoClock(1_000_000_000L); // 1 second
 * clock.advanceNanos(5_000_000_000L);                    // now 6 seconds
 * assertEquals(6_000_000_000L, clock.nanoTime());
 * }</pre>
 *
 * @see org.agrona.concurrent.NanoClock
 * @see org.agrona.concurrent.EpochNanoClock
 */
public final class ControllableNanoClock implements NanoClock, EpochNanoClock {

  private long nanos;

  /**
   * Creates a clock starting at the specified time.
   *
   * @param initialNanos starting time in nanoseconds
   */
  public ControllableNanoClock(final long initialNanos) {
    this.nanos = initialNanos;
  }

  /** Creates a clock starting at time zero. */
  public ControllableNanoClock() {
    this(0L);
  }

  /**
   * Returns the current controlled time.
   *
   * @return current time in nanoseconds
   */
  @Override
  public long nanoTime() {
    return nanos;
  }

  /**
   * Advances the clock by the specified number of nanoseconds.
   *
   * @param deltaNanos nanoseconds to advance; must be &gt;= 0
   * @throws IllegalArgumentException if {@code deltaNanos} is negative
   */
  public void advanceNanos(final long deltaNanos) {
    if (deltaNanos < 0) {
      throw new IllegalArgumentException("deltaNanos must be >= 0, was: " + deltaNanos);
    }
    nanos = Math.addExact(nanos, deltaNanos);
  }

  /**
   * Advances the clock by the specified number of milliseconds.
   *
   * @param deltaMillis milliseconds to advance; must be &gt;= 0
   * @throws IllegalArgumentException if {@code deltaMillis} is negative
   */
  public void advanceMillis(final long deltaMillis) {
    if (deltaMillis < 0) {
      throw new IllegalArgumentException("deltaMillis must be >= 0, was: " + deltaMillis);
    }
    nanos = Math.addExact(nanos, Math.multiplyExact(deltaMillis, 1_000_000L));
  }

  /**
   * Advances the clock by the specified number of seconds.
   *
   * @param deltaSeconds seconds to advance; must be &gt;= 0
   * @throws IllegalArgumentException if {@code deltaSeconds} is negative
   */
  public void advanceSeconds(final long deltaSeconds) {
    if (deltaSeconds < 0) {
      throw new IllegalArgumentException("deltaSeconds must be >= 0, was: " + deltaSeconds);
    }
    nanos = Math.addExact(nanos, Math.multiplyExact(deltaSeconds, 1_000_000_000L));
  }

  /**
   * Sets the clock to an absolute time value.
   *
   * @param nanos absolute time in nanoseconds
   */
  public void setNanos(final long nanos) {
    this.nanos = nanos;
  }
}
