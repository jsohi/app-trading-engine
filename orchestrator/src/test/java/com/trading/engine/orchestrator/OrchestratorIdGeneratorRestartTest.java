package com.trading.engine.orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the §3.2 restart-safety claim on {@link OrchestratorIdGenerator}. Covers the
 * {@link OrchestratorIdGenerator#OrchestratorIdGenerator(String, EpochNanoClock)} clock-injected
 * constructor: disjoint sequences for different boot timestamps, deterministic single-arg baseline
 * comparison, {@code null} clock rejection, {@code Long.MAX_VALUE} clock boundary, and the special
 * case where a clock returning {@code 0} reproduces the deterministic constructor's sequence.
 */
class OrchestratorIdGeneratorRestartTest {

  /** ID buffer size — must hold the longest possible generated ID (20 bytes). */
  private static final int BUF_CAPACITY = 32;

  // ===========================================================================
  // Different clock values produce different first IDs (restart-safety §3.2)
  // ===========================================================================

  @Test
  void restartSafety_differentClockValues_firstIdsAreNotEqual() {
    // Two fake EpochNanoClock lambdas with different nanosecond values simulate successive
    // restarts.
    // Seed1 = Math.floorMod(1_000_000_000_000L >>> 20, MAX_COUNTER) = 953_674
    // Seed2 = Math.floorMod(2_000_000_000_000L >>> 20, MAX_COUNTER) = 1_907_348
    final EpochNanoClock clock1 = () -> 1_000_000_000_000L;
    final EpochNanoClock clock2 = () -> 2_000_000_000_000L;

    final var gen1 = new OrchestratorIdGenerator("QTE", clock1);
    final var gen2 = new OrchestratorIdGenerator("QTE", clock2);

    final var buf1 = new UnsafeBuffer(new byte[BUF_CAPACITY]);
    final var buf2 = new UnsafeBuffer(new byte[BUF_CAPACITY]);

    final int len1 = gen1.nextInto(buf1, 0);
    final int len2 = gen2.nextInto(buf2, 0);

    // byte-compare via ASCII string extraction — both buffers are the same capacity
    final var id1 = buf1.getStringWithoutLengthAscii(0, len1);
    final var id2 = buf2.getStringWithoutLengthAscii(0, len2);

    assertNotEquals(
        id1,
        id2,
        "generators seeded from different clock values must produce different first IDs; "
            + "id1="
            + id1
            + " id2="
            + id2);
  }

  @Test
  void restartSafety_differentClockValues_byteBufferContentsAreNotEqual() {
    // Raw byte-level comparison: copy 15 bytes from each buffer (QTE- prefix + 11 digits).
    final EpochNanoClock clock1 = () -> 1_000_000_000_000L;
    final EpochNanoClock clock2 = () -> 2_000_000_000_000L;

    final var gen1 = new OrchestratorIdGenerator("QTE", clock1);
    final var gen2 = new OrchestratorIdGenerator("QTE", clock2);

    final var buf1 = new UnsafeBuffer(new byte[BUF_CAPACITY]);
    final var buf2 = new UnsafeBuffer(new byte[BUF_CAPACITY]);

    final int len = gen1.nextInto(buf1, 0);
    gen2.nextInto(buf2, 0);

    // Extract ID bytes into arrays for comparison.
    final byte[] bytes1 = new byte[len];
    final byte[] bytes2 = new byte[len];
    buf1.getBytes(0, bytes1);
    buf2.getBytes(0, bytes2);

    // At least one byte must differ between the two rendered IDs.
    boolean anyDiffers = false;
    for (int i = 0; i < len; i++) {
      if (bytes1[i] != bytes2[i]) {
        anyDiffers = true;
        break;
      }
    }
    assertTrue(
        anyDiffers,
        "byte arrays of IDs from different-clock generators must differ at at least one position");
  }

  // ===========================================================================
  // Deterministic ctor baseline
  // ===========================================================================

  @Test
  void deterministicCtor_firstId_isQteZeros1() {
    final var gen = new OrchestratorIdGenerator("QTE");
    final var buf = new UnsafeBuffer(new byte[BUF_CAPACITY]);
    final int len = gen.nextInto(buf, 0);
    final var id = buf.getStringWithoutLengthAscii(0, len);
    assertEquals(
        "QTE-00000000001",
        id,
        "single-arg (deterministic) ctor must produce QTE-00000000001 as its first ID");
  }

  @Test
  void clockInjectedCtor_nonZeroClock_firstIdDiffersFromDeterministic() {
    // A non-zero clock seed bumps the counter away from 0, so first ID differs.
    final EpochNanoClock clock = () -> 1_000_000_000_000L;
    final var gen = new OrchestratorIdGenerator("QTE", clock);
    final var buf = new UnsafeBuffer(new byte[BUF_CAPACITY]);
    final int len = gen.nextInto(buf, 0);
    final var id = buf.getStringWithoutLengthAscii(0, len);
    assertNotEquals(
        "QTE-00000000001",
        id,
        "clock-seeded ctor with non-zero clock must produce a different first ID than the deterministic ctor");
  }

  // ===========================================================================
  // null clock → NullPointerException
  // ===========================================================================

  @Test
  void clockInjectedCtor_nullClock_throwsNullPointerException() {
    assertThrows(
        NullPointerException.class,
        () -> new OrchestratorIdGenerator("QTE", (EpochNanoClock) null),
        "null clock must throw NullPointerException");
  }

  // ===========================================================================
  // Long.MAX_VALUE clock — boundary safety
  // ===========================================================================

  @Test
  void clockInjectedCtor_longMaxValueClock_doesNotThrow() {
    // Long.MAX_VALUE >>> 20 = 8_796_093_022_207; floorMod(..., MAX_COUNTER) gives a valid seed.
    final EpochNanoClock clock = () -> Long.MAX_VALUE;
    final var gen = new OrchestratorIdGenerator("QTE", clock);
    // Counter must be in [0, MAX_COUNTER) immediately after construction.
    assertTrue(
        gen.currentCounter() >= 0L,
        "counter must be non-negative after construction with Long.MAX_VALUE clock");
    assertTrue(
        gen.currentCounter() < OrchestratorIdGenerator.MAX_COUNTER,
        "counter must be < MAX_COUNTER after construction with Long.MAX_VALUE clock");
  }

  @Test
  void clockInjectedCtor_longMaxValueClock_firstIdDoesNotThrow() {
    // Confirm that a subsequent nextInto() call succeeds without exhaustion.
    final EpochNanoClock clock = () -> Long.MAX_VALUE;
    final var gen = new OrchestratorIdGenerator("QTE", clock);
    final var buf = new UnsafeBuffer(new byte[BUF_CAPACITY]);
    // Should not throw IllegalStateException.
    final int len = gen.nextInto(buf, 0);
    assertEquals(gen.idByteLength(), len);
  }

  // ===========================================================================
  // clock returning 0L → same as deterministic ctor
  // ===========================================================================

  @Test
  void clockInjectedCtor_zeroNanoClock_counterIsZeroLikeDetministicCtor() {
    // 0L >>> 20 = 0; Math.floorMod(0, MAX_COUNTER) = 0 → counter = 0.
    final EpochNanoClock clock = () -> 0L;
    final var gen = new OrchestratorIdGenerator("QTE", clock);
    assertEquals(
        0L,
        gen.currentCounter(),
        "clock returning 0 must seed counter=0, same as the deterministic single-arg ctor");
  }

  @Test
  void clockInjectedCtor_zeroNanoClock_firstIdSameAsDeterministicCtor() {
    final EpochNanoClock clock = () -> 0L;
    final var genClocked = new OrchestratorIdGenerator("QTE", clock);
    final var genDet = new OrchestratorIdGenerator("QTE");

    final var bufClocked = new UnsafeBuffer(new byte[BUF_CAPACITY]);
    final var bufDet = new UnsafeBuffer(new byte[BUF_CAPACITY]);

    final int len = genClocked.nextInto(bufClocked, 0);
    genDet.nextInto(bufDet, 0);

    final var idClocked = bufClocked.getStringWithoutLengthAscii(0, len);
    final var idDet = bufDet.getStringWithoutLengthAscii(0, len);

    assertEquals(
        idDet,
        idClocked,
        "clock returning 0L must produce the same first ID as the deterministic single-arg ctor");
  }
}
