package com.trading.engine.projections;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/**
 * Contract test for the {@link Projection} interface, exercised through a trivial counting
 * implementation. Locks down the "update lastProcessedSequence on every onEvent, reset zeros it"
 * contract that concrete projections (APP-25/26/27) will follow.
 */
class ProjectionTest {

  /** Minimal in-test projection: counts events by type, tracks the latest seqNo seen. */
  private static final class CountingProjection implements Projection {
    int totalEvents;
    int lastEventType;
    long lastSeqNo;

    @Override
    public void onEvent(
        final long seqNo,
        final int eventType,
        final DirectBuffer buffer,
        final int offset,
        final int length) {
      this.totalEvents++;
      this.lastEventType = eventType;
      this.lastSeqNo = seqNo;
    }

    @Override
    public long lastProcessedSequence() {
      return lastSeqNo;
    }

    @Override
    public void reset() {
      this.totalEvents = 0;
      this.lastEventType = 0;
      this.lastSeqNo = 0L;
    }
  }

  @Test
  void lastProcessedSequenceUpdatesOnEachEvent() {
    final CountingProjection p = new CountingProjection();
    final UnsafeBuffer empty = new UnsafeBuffer(new byte[0]);
    assertEquals(0L, p.lastProcessedSequence());

    p.onEvent(1L, 100, empty, 0, 0);
    assertEquals(1L, p.lastProcessedSequence());

    p.onEvent(2L, 102, empty, 0, 0);
    assertEquals(2L, p.lastProcessedSequence());
    assertEquals(2, p.totalEvents);
    assertEquals(102, p.lastEventType);
  }

  @Test
  void resetZerosAllState() {
    final CountingProjection p = new CountingProjection();
    final UnsafeBuffer empty = new UnsafeBuffer(new byte[0]);
    for (int i = 1; i <= 5; i++) {
      p.onEvent(i, 100 + i, empty, 0, 0);
    }
    assertEquals(5, p.totalEvents);
    assertEquals(5L, p.lastProcessedSequence());

    p.reset();
    assertEquals(0, p.totalEvents);
    assertEquals(0, p.lastEventType);
    assertEquals(0L, p.lastProcessedSequence());
  }
}
