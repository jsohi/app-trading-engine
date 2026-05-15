package com.trading.engine.websocket;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Description;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.II_Result;

/**
 * JCStress: capture vs concurrent replay must never produce a torn frame.
 *
 * <p>Plan §14: nanosecond-resolution interleaving of one capture thread (writes seq=11) and one
 * replay thread (reads seq=11). Pre-seeded with seq 1..10 so the replay window is hot on entry. The
 * expected outcomes are:
 *
 * <ul>
 *   <li>{@code (1, 64)} — replay observed the in-progress write fully (length matches the
 *       deterministic payload).
 *   <li>{@code (-1, 0)} — replay observed nothing yet (slot still holds an evicted older seq).
 * </ul>
 *
 * <p>Any other (length, byteSum) combination is a forbidden interleaving (torn frame).
 */
@JCStressTest
@Description("Capture(seq=11) vs Replay(seq=11): never produces a torn frame.")
@Outcome(
    id = "0, 0",
    expect = Expect.ACCEPTABLE,
    desc = "replay saw nothing yet (length -1 mapped via guard)")
@Outcome(
    id = "64, 192",
    expect = Expect.ACCEPTABLE,
    desc = "replay saw fully-captured frame (XOR of seq=11 deterministic payload = 192)")
@Outcome(expect = Expect.FORBIDDEN, desc = "any other combination = torn frame")
@State
public class ReliableStreamTrackerCaptureReplayJCStress {

  private static final int CAPACITY = 8;
  private static final int FRAME_SIZE = 256;
  private static final int PAYLOAD_BYTES = 64;
  private static final long TARGET_SEQ = 11L;

  /**
   * Expected XOR of the deterministic payload for {@code TARGET_SEQ=11}, derived as {@code
   * XOR_{i=0..63}( (11 * 31 + i) & 0xFF )}. Computed below in a static initialiser so a future
   * schema/payload change cannot silently make the test pass for the wrong reason — mismatch
   * between {@link #EXPECTED_XOR} and the {@code @Outcome} id literal blows up at class-load before
   * JCStress runs a single iteration.
   */
  private static final int EXPECTED_XOR;

  static {
    int xor = 0;
    for (int i = 0; i < PAYLOAD_BYTES; i++) {
      xor ^= (int) ((TARGET_SEQ * 31L + i) & 0xFFL);
    }
    EXPECTED_XOR = xor;
    if (EXPECTED_XOR != 192) {
      throw new AssertionError(
          "ReliableStreamTrackerCaptureReplayJCStress: derived EXPECTED_XOR="
              + EXPECTED_XOR
              + " does NOT match the @Outcome id literal '64, 192' — update the @Outcome before"
              + " merging");
    }
  }

  private final ReliableStreamTracker tracker =
      new ReliableStreamTracker(CAPACITY, FRAME_SIZE, WebSocketMetrics.createWithDefaults());
  private final byte[] payload = newPayload(TARGET_SEQ);
  private final byte[] dst = new byte[PAYLOAD_BYTES];

  public ReliableStreamTrackerCaptureReplayJCStress() {
    // Pre-seed seq 1..10 so the replay window sits at the boundary.
    for (long seq = 1; seq <= 10; seq++) {
      final var p = newPayload(seq);
      tracker.capture(seq, 100, p, 0, p.length);
    }
  }

  @Actor
  public void capturer() {
    tracker.capture(TARGET_SEQ, 100, payload, 0, payload.length);
  }

  @Actor
  public void replayer(final II_Result r) {
    final int n = tracker.copyPayload(TARGET_SEQ, dst, 0);
    if (n < 0) {
      r.r1 = 0;
      r.r2 = 0;
      return;
    }
    r.r1 = n;
    // Sum first byte's pattern signature: byte 0 of seq=11 is (11 * 31) & 0xFF = 341 & 0xFF = 85
    // — but the result type is II so we encode "expected pattern observed" as a small int
    // signature. Compute the full XOR of bytes mod 256; on a torn frame this won't match the
    // expected pattern signature.
    int xor = 0;
    for (int i = 0; i < n; i++) {
      xor ^= dst[i] & 0xFF;
    }
    r.r2 = xor;
  }

  /** Same deterministic payload pattern as {@code ReliableStreamTrackerReconnectTest}. */
  private static byte[] newPayload(final long seqNo) {
    final var p = new byte[PAYLOAD_BYTES];
    for (int i = 0; i < p.length; i++) {
      p[i] = (byte) ((seqNo * 31L + i) & 0xFF);
    }
    return p;
  }
}
