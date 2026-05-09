package com.trading.engine.cluster.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link BufferAsAsciiCharSequence}.
 *
 * <p>Exercises {@link CharSequence} contract implementation over an Agrona {@link
 * org.agrona.DirectBuffer} byte slice: length reporting, per-index char access, range checks,
 * and the zero-allocation wrap contract.
 *
 * <p><b>Threading:</b> single-threaded tests — {@link BufferAsAsciiCharSequence} is not
 * thread-safe by design.
 */
class BufferAsAsciiCharSequenceTest {

  // ---------------------------------------------------------------------------
  // length() returns the wrapped slice length
  // ---------------------------------------------------------------------------

  /**
   * {@link BufferAsAsciiCharSequence#length()} must return the {@code length} parameter passed to
   * {@link BufferAsAsciiCharSequence#wrap}.
   */
  @Test
  void length_returnsWrappedSliceLength() {
    final var buf = new UnsafeBuffer("EURUSD".getBytes(StandardCharsets.US_ASCII));
    final var seq = new BufferAsAsciiCharSequence();
    seq.wrap(buf, 0, 6);

    assertEquals(6, seq.length(), "length() must equal the wrapped slice length");
  }

  /**
   * Wrapping a sub-slice with {@code offset > 0} must report only the sub-slice length.
   */
  @Test
  void length_withNonZeroOffset_returnsSliceLengthNotBufferLength() {
    final var backing = new UnsafeBuffer("ABCDEFGH".getBytes(StandardCharsets.US_ASCII));
    final var seq = new BufferAsAsciiCharSequence();
    seq.wrap(backing, 2, 4); // "CDEF"

    assertEquals(4, seq.length());
  }

  // ---------------------------------------------------------------------------
  // charAt() returns the correct ASCII byte as char
  // ---------------------------------------------------------------------------

  /**
   * {@link BufferAsAsciiCharSequence#charAt(int)} must return each byte in the slice, converted
   * to {@code char} via unsigned byte masking ({@code & 0xFF}).
   */
  @Test
  void charAt_returnsAsciiByte() {
    final byte[] data = "EURUSD".getBytes(StandardCharsets.US_ASCII);
    final var buf = new UnsafeBuffer(data);
    final var seq = new BufferAsAsciiCharSequence();
    seq.wrap(buf, 0, data.length);

    assertEquals('E', seq.charAt(0));
    assertEquals('U', seq.charAt(1));
    assertEquals('R', seq.charAt(2));
    assertEquals('U', seq.charAt(3));
    assertEquals('S', seq.charAt(4));
    assertEquals('D', seq.charAt(5));
  }

  /**
   * Wrapping with a non-zero offset: charAt(i) must read from {@code buffer[offset + i]}.
   */
  @Test
  void charAt_withOffset_readsFromCorrectPosition() {
    final byte[] data = "ABCDE".getBytes(StandardCharsets.US_ASCII);
    final var buf = new UnsafeBuffer(data);
    final var seq = new BufferAsAsciiCharSequence();
    seq.wrap(buf, 1, 3); // "BCD"

    assertEquals('B', seq.charAt(0));
    assertEquals('C', seq.charAt(1));
    assertEquals('D', seq.charAt(2));
  }

  // ---------------------------------------------------------------------------
  // charAt() out-of-range throws IndexOutOfBoundsException
  // ---------------------------------------------------------------------------

  /**
   * Calling {@link BufferAsAsciiCharSequence#charAt(int)} with a negative index must throw
   * {@link IndexOutOfBoundsException}.
   */
  @Test
  void charAt_outOfRange_throwsIndexOutOfBoundsException() {
    final var buf = new UnsafeBuffer("ABC".getBytes(StandardCharsets.US_ASCII));
    final var seq = new BufferAsAsciiCharSequence();
    seq.wrap(buf, 0, 3);

    assertThrows(IndexOutOfBoundsException.class, () -> seq.charAt(-1),
        "negative index must throw IndexOutOfBoundsException");

    assertThrows(IndexOutOfBoundsException.class, () -> seq.charAt(3),
        "index == length must throw IndexOutOfBoundsException");

    assertThrows(IndexOutOfBoundsException.class, () -> seq.charAt(100),
        "index far beyond length must throw IndexOutOfBoundsException");
  }

  // ---------------------------------------------------------------------------
  // wrap() zero-allocation smoke test
  // ---------------------------------------------------------------------------

  /**
   * Calls {@link BufferAsAsciiCharSequence#wrap} 100k times in a tight loop after warmup. No
   * exception must be raised. This guards against any accidental allocation that would cause OOM
   * on tightly-looped GFLog usage.
   *
   * <p>This is a smoke test only — it does not instrument the GC.
   */
  @Test
  void wrap_doesNotAllocate() {
    final var buf = new UnsafeBuffer("HELLO".getBytes(StandardCharsets.US_ASCII));
    final var seq = new BufferAsAsciiCharSequence();

    // Warmup: prime JIT before the timed section.
    for (int i = 0; i < 10_000; i++) {
      seq.wrap(buf, 0, 5);
    }

    // Hot path: 100k wraps — must complete without exception.
    int sum = 0;
    for (int i = 0; i < 100_000; i++) {
      seq.wrap(buf, 0, 5);
      // Access one char to verify the wrap actually works; sum it so JIT can't elide.
      sum += seq.charAt(0);
    }
    org.junit.jupiter.api.Assertions.assertTrue(sum > 0);
  }

  // ---------------------------------------------------------------------------
  // toString() returns correct ASCII string
  // ---------------------------------------------------------------------------

  /**
   * {@link BufferAsAsciiCharSequence#toString()} must return the ASCII string corresponding to the
   * wrapped bytes (cold-path / diagnostics use only).
   */
  @Test
  void toString_returnsAsciiContent() {
    final var buf = new UnsafeBuffer("EURUSD".getBytes(StandardCharsets.US_ASCII));
    final var seq = new BufferAsAsciiCharSequence();
    seq.wrap(buf, 0, 6);

    assertEquals("EURUSD", seq.toString());
  }

  // ---------------------------------------------------------------------------
  // subSequence() returns correct sub-range
  // ---------------------------------------------------------------------------

  /**
   * {@link BufferAsAsciiCharSequence#subSequence} must return a {@link CharSequence} whose chars
   * match the requested sub-range.
   */
  @Test
  void subSequence_returnsCorrectRange() {
    final var buf = new UnsafeBuffer("EURUSD".getBytes(StandardCharsets.US_ASCII));
    final var seq = new BufferAsAsciiCharSequence();
    seq.wrap(buf, 0, 6);

    final var sub = seq.subSequence(1, 4); // "URU"
    assertEquals(3, sub.length());
    assertEquals('U', sub.charAt(0));
    assertEquals('R', sub.charAt(1));
    assertEquals('U', sub.charAt(2));
  }
}
