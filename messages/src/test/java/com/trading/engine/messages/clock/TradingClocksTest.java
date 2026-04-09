package com.trading.engine.messages.clock;

import static org.junit.jupiter.api.Assertions.*;

import org.agrona.concurrent.EpochNanoClock;
import org.junit.jupiter.api.Test;

final class TradingClocksTest {

  /** Epoch nanos for 2020-01-01T00:00:00Z — any reasonable current time must exceed this. */
  private static final long MIN_EPOCH_NANOS = 1_577_836_800_000_000_000L;

  @Test
  void epochNanoClockReturnsPlausibleValue() {
    final EpochNanoClock clock = TradingClocks.epochNanoClock();
    final long nanos = clock.nanoTime();
    assertTrue(nanos > MIN_EPOCH_NANOS, "Expected epoch nanos > 2020-01-01, got " + nanos);
  }

  @Test
  void successiveCallsAreMonotonicallyNonDecreasing() {
    final EpochNanoClock clock = TradingClocks.epochNanoClock();
    final long first = clock.nanoTime();
    final long second = clock.nanoTime();
    assertTrue(second >= first, "Expected monotonic: first=" + first + " second=" + second);
  }

  @Test
  void returnsSingletonInstance() {
    final EpochNanoClock a = TradingClocks.epochNanoClock();
    final EpochNanoClock b = TradingClocks.epochNanoClock();
    assertSame(a, b);
  }
}
