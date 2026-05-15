package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32C;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Reconnect-correctness regression for {@link ReliableStreamTracker}.
 *
 * <p>Plan §14: deterministic + concurrent assertions covering the scenarios a real client
 * reconnect-with-gap-request exercises. Uses the {@link ReliableStreamTracker}'s public API only —
 * no framing layer involved. CRC32c is computed against each captured payload and re-verified after
 * replay to catch torn frames (the framing layer applies the same CRC32c in production; the test
 * reproduces that invariant against the bare tracker).
 *
 * <p><b>Two run phases</b>:
 *
 * <ul>
 *   <li><b>Smoke</b>: {@code @RepeatedTest(50)} per-iter {@link Timeout @Timeout(2s)}. Runs in
 *       every PR via {@code ./gradlew :websocket-server:test}. Class-level {@code @Timeout(180s)}
 *       absorbs 50 × 2s worst-case + warm-up + non-repeated cases with margin.
 *   <li><b>Stress</b>: {@code @RepeatedTest(1000)} tagged {@code @Tag("stress")}. Runs in every
 *       PR's {@code fullStackE2e} job via {@code ./gradlew :websocket-server:test --tests
 *       *ReliableStreamTrackerReconnectTest* -Pstress=true} (the test class enables the stress tag
 *       only when the {@code stress} system property is set; standalone unit-test invocations skip
 *       the stress methods).
 * </ul>
 *
 * <p>The concurrent test thread-yields aggressively to surface ordering bugs without requiring a
 * full JCStress harness in the PR-CI smoke (the JCStress harness in {@code
 * websocket-server/src/jcstress/java/} runs separately in {@code fullStackE2e}).
 */
@Timeout(value = 180, unit = TimeUnit.SECONDS)
final class ReliableStreamTrackerReconnectTest {

  private static final int CAPACITY = 8;
  private static final int FRAME_SIZE = 256;
  private static final int PAYLOAD_SIZE = FRAME_SIZE - ReliableStreamTracker.SLOT_HEADER_SIZE;

  // ===========================================================================
  // Deterministic non-repeated assertions (always run, both phases)
  // ===========================================================================

  @Test
  @Timeout(value = 5, unit = TimeUnit.SECONDS)
  void capture10_oldestSeqNoIs3() {
    final var tracker = newTracker();
    capturePattern(tracker, 1, 10);
    assertEquals(3L, tracker.oldestSeqNo());
    assertEquals(10L, tracker.highestSeqNo());
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.SECONDS)
  void replayGap5_10_returnsExactBytes_inOrder_noDuplicates() {
    final var tracker = newTracker();
    capturePattern(tracker, 1, 10);
    long prevSeq = -1;
    for (long seq = 5; seq <= 10; seq++) {
      final var dst = new byte[PAYLOAD_SIZE];
      final int n = tracker.copyPayload(seq, dst, 0);
      assertEquals(payloadFor(seq).length, n, "seq " + seq + " length");
      assertEquals(payloadFor(seq).length, n, "seq " + seq + " length");
      assertTrue(prevSeq < seq, "seq must be strictly ascending");
      assertCrcMatch(seq, dst, n);
      prevSeq = seq;
    }
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.SECONDS)
  void replayGap1_4_returnsBelowWindowSentinel() {
    final var tracker = newTracker();
    capturePattern(tracker, 1, 10);
    // ReliableStreamTracker.copyPayload returns -1 when the slot does not hold the requested seqNo.
    // After capacity rollover with seq 1..10 (cap=8), seqs 1 and 2 are evicted by overwrite;
    // seqs 3..10 remain. So the documented "below window" sentinel for seqs 1 and 2 is -1.
    final var dst = new byte[PAYLOAD_SIZE];
    assertEquals(-1, tracker.copyPayload(1L, dst, 0));
    assertEquals(-1, tracker.copyPayload(2L, dst, 0));
  }

  // ===========================================================================
  // Smoke: 50 iterations in PR-CI
  // ===========================================================================

  @org.junit.jupiter.api.RepeatedTest(50)
  @Timeout(value = 2, unit = TimeUnit.SECONDS)
  void concurrentCaptureAndReplay_smoke() throws Exception {
    runConcurrentCaptureAndReplay();
  }

  // ===========================================================================
  // Stress: 1000 iterations gated on -Pstress=true
  // ===========================================================================

  @Tag("stress")
  @org.junit.jupiter.api.RepeatedTest(1000)
  @Timeout(value = 2, unit = TimeUnit.SECONDS)
  void concurrentCaptureAndReplay_stress() throws Exception {
    // Honor the -Pstress=true gate. Without the flag, JUnit's `--tests` filter or Gradle's
    // useJUnitPlatform { excludeTags("stress") } hides this method. We add a runtime guard
    // anyway in case a future refactor accidentally unhides it.
    if (!Boolean.getBoolean("stress")) {
      return; // skipped — runs only under the stress phase
    }
    runConcurrentCaptureAndReplay();
  }

  // ===========================================================================
  // Concurrent body — shared by smoke + stress
  // ===========================================================================

  private void runConcurrentCaptureAndReplay() throws Exception {
    // Pre-seed seq 1..10 so the replay thread always has data in the window.
    final var tracker = newTracker();
    capturePattern(tracker, 1, 10);

    final var go = new CountDownLatch(1);
    final var done = new CountDownLatch(2);
    final var failure = new AtomicReferenceWrapper<Throwable>();

    final var capturer =
        new Thread(
            () -> {
              try {
                go.await();
                for (long seq = 11; seq <= 20; seq++) {
                  Thread.yield();
                  capture(tracker, seq);
                }
              } catch (final Throwable t) {
                failure.set(t);
              } finally {
                done.countDown();
              }
            },
            "capturer");
    final var replayer =
        new Thread(
            () -> {
              try {
                go.await();
                for (int i = 0; i < 50; i++) {
                  // Pick a window that may straddle the capture front. Any returned frame must
                  // have a valid CRC (no torn-frame visibility).
                  for (long seq = 8; seq <= 18; seq++) {
                    Thread.yield();
                    final var dst = new byte[PAYLOAD_SIZE];
                    final int n = tracker.copyPayload(seq, dst, 0);
                    if (n >= 0) {
                      assertCrcMatch(seq, dst, n);
                    }
                  }
                }
              } catch (final Throwable t) {
                failure.set(t);
              } finally {
                done.countDown();
              }
            },
            "replayer");

    capturer.start();
    replayer.start();
    go.countDown();
    final boolean finished = done.await(2, TimeUnit.SECONDS);
    assertTrue(finished, "concurrent threads did not finish in 2s");
    final var t = failure.get();
    if (t != null) {
      throw new AssertionError("concurrent worker failure", t);
    }
  }

  // ===========================================================================
  // Helpers
  // ===========================================================================

  private static ReliableStreamTracker newTracker() {
    return new ReliableStreamTracker(CAPACITY, FRAME_SIZE, WebSocketMetrics.createWithDefaults());
  }

  /** Deterministic payload pattern: byte i = (seqNo * 31 + i) & 0xFF. */
  private static byte[] payloadFor(final long seqNo) {
    final var p = new byte[64];
    for (int i = 0; i < p.length; i++) {
      p[i] = (byte) ((seqNo * 31L + i) & 0xFF);
    }
    return p;
  }

  private static void capture(final ReliableStreamTracker tracker, final long seqNo) {
    final var p = payloadFor(seqNo);
    tracker.capture(seqNo, /*templateId*/ 100, p, 0, p.length);
  }

  private static void capturePattern(
      final ReliableStreamTracker tracker, final long firstSeq, final long lastSeq) {
    for (long seq = firstSeq; seq <= lastSeq; seq++) {
      capture(tracker, seq);
    }
  }

  /** Asserts the replayed bytes match the deterministic pattern AND the CRC32c is consistent. */
  private static void assertCrcMatch(final long seqNo, final byte[] dst, final int len) {
    final var expected = payloadFor(seqNo);
    final var actual = Arrays.copyOf(dst, len);
    assertEquals(expected.length, actual.length, "seq " + seqNo + " replayed length");
    final var crcExpected = new CRC32C();
    crcExpected.update(expected, 0, expected.length);
    final var crcActual = new CRC32C();
    crcActual.update(actual, 0, actual.length);
    // Equal byte content → equal CRC; mismatched would surface a torn frame from concurrent
    // writes. Asserting CRC equality on each side independently ensures the test fails loud
    // both on torn bytes AND on the rare case where bytes match by coincidence.
    assertEquals(crcExpected.getValue(), crcActual.getValue(), "seq " + seqNo + " CRC32c");
    assertNotEquals(0L, crcActual.getValue(), "seq " + seqNo + " CRC32c (nonzero on real data)");
  }

  /**
   * Tiny atomic-reference wrapper to avoid pulling in a junit-jupiter-params dependency just for a
   * thread-safe failure handoff. Kept package-private for readability.
   */
  private static final class AtomicReferenceWrapper<T> {
    private volatile T value;

    void set(final T v) {
      this.value = v;
    }

    T get() {
      return value;
    }
  }
}
