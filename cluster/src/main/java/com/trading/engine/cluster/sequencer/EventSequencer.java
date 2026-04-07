package com.trading.engine.cluster.sequencer;

import java.nio.ByteOrder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * Deterministic, single-threaded, monotonic sequence number generator for domain events emitted by
 * the cluster service. Every domain event is assigned exactly one sequence number via {@link
 * #nextSequence()}; consumers use gaplessness to detect loss and replay determinism.
 *
 * <p>Semantics:
 *
 * <ul>
 *   <li>1-based counter; {@code 0} is the "nothing assigned yet" sentinel returned by {@link
 *       #currentSequence()} before the first call
 *   <li>Monotonic and gapless — every successful call returns {@code currentSequence() + 1}
 *   <li>Not thread-safe — single-threaded cluster duty cycle only
 *   <li>Deterministic — no randomness, no wall-clock time, no external dependencies
 *   <li>Counter is {@code long}; range is effectively infinite at any realistic event rate
 *       (~292,000 years at 1M events/sec), so no exhaustion guard is needed
 * </ul>
 *
 * <p>Snapshot state survives failover via {@link #saveTo}/{@link #loadFrom}. The serialized form is
 * the <em>next sequence to assign</em> (i.e. {@code counter + 1}), matching the SBE schema field
 * {@code EventSequencerSnapshot.nextSequence} (template 206) so a future SBE encode/decode call
 * site can write/read the long directly without translation. The value is written in little-endian
 * order to match {@code trading-schema.xml}'s {@code byteOrder="littleEndian"} declaration so
 * snapshot bytes survive cross-architecture transfer (e.g., disaster-recovery copies between
 * hosts).
 */
public final class EventSequencer {

  /** Number of bytes written by {@link #saveTo(MutableDirectBuffer, int)}. */
  public static final int SNAPSHOT_LENGTH = Long.BYTES;

  /** Last assigned sequence number (0 = nothing assigned yet). */
  private long counter;

  public EventSequencer() {
    this.counter = 0L;
  }

  /**
   * Assign and return the next sequence number. Called exactly once per domain event produced by
   * the cluster. Zero allocation.
   */
  public long nextSequence() {
    return ++counter;
  }

  /**
   * Current counter value — the last assigned sequence number, or {@code 0} if {@link
   * #nextSequence()} has never been called. Diagnostics only.
   */
  public long currentSequence() {
    return counter;
  }

  /**
   * Serialize the next sequence number to be assigned ({@code counter + 1}) into {@code buffer} at
   * {@code offset}. Writes exactly {@link #SNAPSHOT_LENGTH} bytes in little-endian order to match
   * the {@code EventSequencerSnapshot.nextSequence} SBE schema field.
   *
   * @return number of bytes written ({@link #SNAPSHOT_LENGTH})
   */
  public int saveTo(MutableDirectBuffer buffer, int offset) {
    buffer.putLong(offset, counter + 1L, ByteOrder.LITTLE_ENDIAN);
    return SNAPSHOT_LENGTH;
  }

  /**
   * Restore the next sequence number to be assigned from {@code buffer} at {@code offset}. After
   * restore, the next call to {@link #nextSequence()} returns the restored value. Rejects values
   * {@code < 1} — a valid next sequence is always {@code >= 1}.
   */
  public void loadFrom(DirectBuffer buffer, int offset) {
    long nextSequence = buffer.getLong(offset, ByteOrder.LITTLE_ENDIAN);
    if (nextSequence < 1L) {
      throw new IllegalStateException(
          "EventSequencer snapshot nextSequence must be >= 1, was " + nextSequence);
    }
    this.counter = nextSequence - 1L;
  }
}
