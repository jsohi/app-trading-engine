package com.trading.engine.cluster.handler;

import org.agrona.DirectBuffer;

/**
 * Zero-allocation {@link CharSequence} adapter over a {@link DirectBuffer} byte slice for use with
 * GFLog's builder API. GFLog 3.0.7 accepts {@link CharSequence} in its append chain, making this
 * adapter the correct zero-allocation pattern for logging byte-array fields (e.g., quoteReqId,
 * quoteId, symbol) that are stored as {@code byte[]} in {@link
 * com.trading.engine.cluster.state.RfqSlot}.
 *
 * <p>This pattern is established in {@code AccountProjection.java} and is required because GFLog
 * has no {@code append(DirectBuffer)} overload — the only zero-alloc option is char-by-char via
 * {@link CharSequence}.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * private final BufferAsAsciiCharSequence quoteReqIdAscii =
 *     new BufferAsAsciiCharSequence();
 *
 * // In the hot path (zero allocation):
 * quoteReqIdAscii.wrap(slot.quoteReqIdBuffer, 0, slot.QUOTE_REQ_ID_LENGTH);
 * log.info().append("RFQ reqId=").append(quoteReqIdAscii).commit();
 * }</pre>
 *
 * <p><b>Threading:</b> not thread-safe — single-threaded cluster duty cycle only.
 *
 * <p><b>Allocation:</b> zero allocation after construction. Re-wrapped via {@link #wrap} on each
 * use.
 */
public final class BufferAsAsciiCharSequence implements CharSequence {

  private DirectBuffer buffer;
  private int offset;
  private int length;

  /**
   * Wraps the given {@link DirectBuffer} slice. The caller must ensure the buffer is not mutated
   * between this call and the completion of the GFLog append chain.
   *
   * @param buffer the source buffer; must not be null
   * @param offset the byte offset into the buffer where the ASCII content starts
   * @param length the number of bytes to expose as characters
   */
  public void wrap(final DirectBuffer buffer, final int offset, final int length) {
    this.buffer = buffer;
    this.offset = offset;
    this.length = length;
  }

  /**
   * Returns the number of ASCII characters exposed by this adapter.
   *
   * @return the length of the wrapped byte slice
   */
  @Override
  public int length() {
    return length;
  }

  /**
   * Returns the byte at position {@code index} as a {@code char}. Interprets the byte as a 7-bit
   * ASCII value — values above 0x7F are passed through as-is (not sanitized), which matches GFLog's
   * expectation for US-ASCII log fields.
   *
   * @param index position within the wrapped slice; must be in {@code [0, length())}
   * @return the ASCII character at the given index
   * @throws IndexOutOfBoundsException if {@code index} is out of range
   */
  @Override
  public char charAt(final int index) {
    if (index < 0 || index >= length) {
      throw new IndexOutOfBoundsException("index " + index + " out of range [0, " + length + ")");
    }
    return (char) (buffer.getByte(offset + index) & 0xFF);
  }

  /**
   * Returns a {@link CharSequence} sub-view of this adapter. <b>Allocates a new {@link
   * BufferAsAsciiCharSequence}</b> — intended for diagnostic/test use only. Not called on the hot
   * path.
   *
   * @param start the start index (inclusive)
   * @param end the end index (exclusive)
   * @return a new adapter wrapping the sub-range
   */
  @Override
  public CharSequence subSequence(final int start, final int end) {
    final var sub = new BufferAsAsciiCharSequence();
    sub.wrap(buffer, offset + start, end - start);
    return sub;
  }

  /**
   * Returns the ASCII bytes of the wrapped slice as a {@link String}. <b>Allocates a new {@link
   * String}</b> — cold-path / debug use only. Never call on the hot path.
   *
   * @return the ASCII string representation
   */
  @Override
  public String toString() {
    final var sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      sb.append((char) (buffer.getByte(offset + i) & 0xFF));
    }
    return sb.toString();
  }
}
