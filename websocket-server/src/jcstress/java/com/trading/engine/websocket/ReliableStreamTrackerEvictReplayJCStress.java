package com.trading.engine.websocket;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Description;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.I_Result;

/**
 * JCStress: explicit {@code evict(seqNo)} vs concurrent gap-request for the same seqNo.
 *
 * <p>Plan §14: an explicit evict (called when a write to the wire failed before the frame reached
 * the client) must NEVER let replay return a phantom frame for that seqNo. The replay thread's
 * {@code copyPayload(seq)} must observe either the still-valid pre-evict bytes (if it raced ahead)
 * OR the post-evict {@code -1} sentinel. Anything else (partial frame, stale length, garbage bytes)
 * is forbidden.
 */
@JCStressTest
@Description("Evict(seq=5) vs Replay(seq=5): observed length is either valid (64) or -1.")
@Outcome(id = "64", expect = Expect.ACCEPTABLE, desc = "replay raced ahead of evict (full frame)")
@Outcome(id = "-1", expect = Expect.ACCEPTABLE, desc = "evict won (slot cleared)")
@Outcome(expect = Expect.FORBIDDEN, desc = "any other length = torn / phantom frame")
@State
public class ReliableStreamTrackerEvictReplayJCStress {

  private static final int CAPACITY = 8;
  private static final int FRAME_SIZE = 256;
  private static final int PAYLOAD_BYTES = 64;
  private static final long TARGET_SEQ = 5L;

  private final ReliableStreamTracker tracker =
      new ReliableStreamTracker(CAPACITY, FRAME_SIZE, WebSocketMetrics.createWithDefaults());
  private final byte[] dst = new byte[PAYLOAD_BYTES];

  public ReliableStreamTrackerEvictReplayJCStress() {
    final var p = new byte[PAYLOAD_BYTES];
    for (int i = 0; i < p.length; i++) {
      p[i] = (byte) ((TARGET_SEQ * 31L + i) & 0xFF);
    }
    tracker.capture(TARGET_SEQ, 100, p, 0, p.length);
  }

  @Actor
  public void evicter() {
    tracker.evict(TARGET_SEQ);
  }

  @Actor
  public void replayer(final I_Result r) {
    r.r1 = tracker.copyPayload(TARGET_SEQ, dst, 0);
  }
}
