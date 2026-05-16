package com.trading.engine.pricing.market;

import java.util.ArrayList;
import java.util.List;
import org.agrona.DirectBuffer;

/**
 * Capturing test double for {@link BroadcastPublisher}.
 *
 * <p><b>Purpose.</b> Replaces the production Aeron {@code ExclusivePublication} in unit tests.
 * Every {@link #offer} call defensively copies the buffer slice into a {@code byte[]} and appends
 * it to an ordered list; callers decode the captured bytes using the real SBE codecs to assert
 * wire-format correctness.
 *
 * <p><b>Configurable result.</b> The return value of every {@code offer} call is controlled by
 * {@link #setNextResult(long)}. The default is {@code 1L} (success — a positive Aeron stream
 * position). Pass one of the {@link io.aeron.Publication} negative constants to exercise the
 * publisher's error-handling branches:
 *
 * <ul>
 *   <li>{@link io.aeron.Publication#BACK_PRESSURED} ({@code -2}) — retry path
 *   <li>{@link io.aeron.Publication#NOT_CONNECTED} ({@code -1}) — drop + counter
 *   <li>{@link io.aeron.Publication#ADMIN_ACTION} ({@code -3}) — drop + counter
 *   <li>{@link io.aeron.Publication#MAX_POSITION_EXCEEDED} ({@code -5}) — drop + counter
 *   <li>{@link io.aeron.Publication#CLOSED} ({@code -4}) — fatal throw
 * </ul>
 *
 * <p><b>Thread model.</b> Not thread-safe — single-threaded test usage only, matching the
 * single-writer invariant of {@link MarketDataPublisher}.
 *
 * <p><b>Allocation.</b> Allocates one {@code byte[]} per {@code offer} call. This is intentional:
 * the fake is used in correctness tests, not in the zero-alloc regression test ({@link
 * MarketDataPublisherAllocTest}).
 *
 * @see MarketDataPublisher
 * @see BroadcastPublisher
 */
final class FakeBroadcastPublisher implements BroadcastPublisher {

  /** Default term buffer length returned by {@link #termBufferLength()} (16 MiB). */
  private static final int DEFAULT_TERM_BUFFER_LENGTH = 16 * 1_024 * 1_024;

  /** Default position returned by {@link #position()} when no offer has been made. */
  private static final long DEFAULT_POSITION = 1L;

  private final List<byte[]> captured = new ArrayList<>();
  private long nextResult = 1L;

  /**
   * Sets the value that the next (and every subsequent) {@link #offer} call returns. The knob is
   * sticky — call again to reset to success ({@code 1L}) between test phases.
   *
   * @param result the Aeron offer return code to return; positive for success, one of the {@link
   *     io.aeron.Publication} negative constants for failure.
   */
  void setNextResult(final long result) {
    this.nextResult = result;
  }

  /**
   * Returns the number of {@code offer} calls received so far.
   *
   * @return the call count.
   */
  int offerCount() {
    return captured.size();
  }

  /**
   * Returns the captured byte slice from the {@code i}-th {@code offer} call (zero-based). The
   * slice is a defensive copy; mutations do not affect subsequent lookups.
   *
   * @param index zero-based call index.
   * @return the captured bytes.
   * @throws IndexOutOfBoundsException if {@code index >= offerCount()}.
   */
  byte[] capturedBytes(final int index) {
    return captured.get(index);
  }

  /**
   * Defensively copies {@code length} bytes starting at {@code offset} from {@code buffer} into a
   * new {@code byte[]}, appends it to {@link #captured}, then returns {@link #nextResult}.
   *
   * <p>{@inheritDoc}
   */
  @Override
  public long offer(final DirectBuffer buffer, final int offset, final int length) {
    final var copy = new byte[length];
    buffer.getBytes(offset, copy, 0, length);
    captured.add(copy);
    return nextResult;
  }

  /**
   * Returns a stable non-zero position. Tests that assert on MAX_POSITION_EXCEEDED log lines do not
   * need to verify the exact position value — returning a constant is sufficient.
   *
   * <p>{@inheritDoc}
   */
  @Override
  public long position() {
    return DEFAULT_POSITION;
  }

  /**
   * Returns the default 16 MiB term buffer length. Tests that assert on MAX_POSITION_EXCEEDED log
   * lines do not need to verify the exact value — returning a constant is sufficient.
   *
   * <p>{@inheritDoc}
   */
  @Override
  public int termBufferLength() {
    return DEFAULT_TERM_BUFFER_LENGTH;
  }
}
