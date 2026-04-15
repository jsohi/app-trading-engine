package com.trading.engine.testsupport.clock;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.NanoClock;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ControllableNanoClock}.
 *
 * <p>Verifies time control, interface compliance, and advancement semantics.
 */
class ControllableNanoClockTest {

  @Test
  void constructor_default_startsAtZero() {
    final ControllableNanoClock clock = new ControllableNanoClock();
    assertEquals(0L, clock.nanoTime());
  }

  @Test
  void constructor_withInitialNanos_startsAtGivenValue() {
    final ControllableNanoClock clock = new ControllableNanoClock(1_000_000_000L);
    assertEquals(1_000_000_000L, clock.nanoTime());
  }

  @Test
  void advanceNanos_addsToCurrentTime() {
    final ControllableNanoClock clock = new ControllableNanoClock(100L);
    clock.advanceNanos(50L);
    assertEquals(150L, clock.nanoTime());
  }

  @Test
  void advanceMillis_convertsToNanos() {
    final ControllableNanoClock clock = new ControllableNanoClock();
    clock.advanceMillis(5L);
    assertEquals(5_000_000L, clock.nanoTime());
  }

  @Test
  void advanceSeconds_convertsToNanos() {
    final ControllableNanoClock clock = new ControllableNanoClock();
    clock.advanceSeconds(3L);
    assertEquals(3_000_000_000L, clock.nanoTime());
  }

  @Test
  void setNanos_overridesCurrentTime() {
    final ControllableNanoClock clock = new ControllableNanoClock(999L);
    clock.setNanos(42L);
    assertEquals(42L, clock.nanoTime());
  }

  @Test
  void multipleAdvancements_accumulate() {
    final ControllableNanoClock clock = new ControllableNanoClock(1_000_000_000L);
    clock.advanceNanos(500L);
    clock.advanceMillis(1L);
    clock.advanceSeconds(1L);
    assertEquals(2_001_000_500L, clock.nanoTime());
  }

  @Test
  void implementsNanoClock_returnsCorrectType() {
    final NanoClock clock = new ControllableNanoClock(42L);
    assertEquals(42L, clock.nanoTime());
  }

  @Test
  void implementsEpochNanoClock_returnsCorrectType() {
    final EpochNanoClock clock = new ControllableNanoClock(99L);
    assertEquals(99L, clock.nanoTime());
  }
}
