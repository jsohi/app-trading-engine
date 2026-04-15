package com.trading.engine.testsupport.sbe;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.OrderRejectedEventDecoder;
import com.trading.engine.messages.sbe.RejectReasonEnum;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * JUnit assertion helpers for SBE-encoded messages.
 *
 * <p>Provides domain-specific assertions that produce clear failure messages referencing FIX tag
 * names and SBE template IDs.
 *
 * <p>Thread-safe — all methods are stateless static functions.
 *
 * <p>Allocates decoder instances on every call. Test infrastructure only.
 */
public final class SbeMessageAssertions {

  private SbeMessageAssertions() {}

  /**
   * Asserts the SBE header templateId equals {@code expected} at offset 0.
   *
   * @param expected expected template ID
   * @param bytes SBE-encoded message bytes
   * @throws AssertionError if the template ID does not match
   */
  public static void assertTemplateId(final int expected, final byte[] bytes) {
    assertTemplateId(expected, new UnsafeBuffer(bytes), 0);
  }

  /**
   * Asserts the SBE header templateId at the given buffer offset.
   *
   * @param expected expected template ID
   * @param buffer buffer containing the SBE message
   * @param offset offset where the SBE header begins
   * @throws AssertionError if the template ID does not match
   */
  public static void assertTemplateId(
      final int expected, final DirectBuffer buffer, final int offset) {
    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    header.wrap(buffer, offset);
    assertEquals(
        expected,
        header.templateId(),
        () ->
            "Expected SBE templateId "
                + expected
                + " but was "
                + header.templateId()
                + " at offset "
                + offset);
  }

  /**
   * Decodes an OrderRejectedEvent from {@code bytes} and asserts the reject reason.
   *
   * @param bytes SBE-encoded OrderRejectedEvent (template 101)
   * @param expectedReason expected rejection reason enum value
   * @throws AssertionError if template ID is not 101 or reason does not match
   */
  public static void assertRejected(final byte[] bytes, final RejectReasonEnum expectedReason) {
    final UnsafeBuffer buf = new UnsafeBuffer(bytes);
    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    header.wrap(buf, 0);
    assertEquals(
        OrderRejectedEventDecoder.TEMPLATE_ID,
        header.templateId(),
        "Expected OrderRejectedEvent (template 101)");

    final OrderRejectedEventDecoder decoder = new OrderRejectedEventDecoder();
    decoder.wrap(buf, MessageHeaderDecoder.ENCODED_LENGTH, header.blockLength(), header.version());
    assertEquals(
        expectedReason,
        decoder.rejectReason(),
        () -> "Expected reject reason " + expectedReason + " but was " + decoder.rejectReason());
  }

  /**
   * Asserts that a buffer contains exactly {@code expectedCount} concatenated SBE messages, using
   * caller-provided per-message encoded lengths.
   *
   * <p>Each entry in {@code encodedLengths} is the total encoded length of one message (header +
   * body + any repeating groups), as returned by the {@code SbeTestEncoder.encodeXxx()} methods.
   * This avoids the offset-calculation pitfall of relying on {@code blockLength()} alone, which
   * does not account for repeating groups or variable-length data.
   *
   * @param buffer buffer containing concatenated SBE messages
   * @param expectedId expected template ID for every message
   * @param encodedLengths per-message encoded lengths (header + body); array length is the expected
   *     message count
   * @throws AssertionError if any template ID does not match or the buffer contains fewer messages
   *     than {@code encodedLengths.length}
   */
  public static void assertMessageCount(
      final DirectBuffer buffer, final int expectedId, final int... encodedLengths) {
    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    int offset = 0;
    for (int i = 0; i < encodedLengths.length; i++) {
      header.wrap(buffer, offset);
      final int msgIndex = i;
      assertEquals(
          expectedId, header.templateId(), () -> "Unexpected templateId at message #" + msgIndex);
      offset += encodedLengths[i];
    }
  }
}
