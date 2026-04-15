package com.trading.engine.testsupport.sbe;

import com.trading.engine.messages.sbe.AccountLoadRejectedEventDecoder;
import com.trading.engine.messages.sbe.AccountLoadedEventDecoder;
import com.trading.engine.messages.sbe.CurrencyLoadRejectedEventDecoder;
import com.trading.engine.messages.sbe.CurrencyLoadedEventDecoder;
import com.trading.engine.messages.sbe.ExecutionReportDecoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.OrderCanceledEventDecoder;
import com.trading.engine.messages.sbe.OrderCreatedEventDecoder;
import com.trading.engine.messages.sbe.OrderFilledEventDecoder;
import com.trading.engine.messages.sbe.OrderRejectedEventDecoder;
import com.trading.engine.messages.sbe.RiskLimitLoadRejectedEventDecoder;
import com.trading.engine.messages.sbe.RiskLimitLoadedEventDecoder;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Static utility for type-safe SBE message decoding in tests.
 *
 * <p>Consolidates 30+ duplicated decoder patterns (header-wrap, templateId-check, body-wrap) into a
 * single well-documented entry point. Every typed {@code decodeXxx} method verifies that the SBE
 * header's {@code templateId} matches the expected decoder's {@link
 * OrderCreatedEventDecoder#TEMPLATE_ID TEMPLATE_ID} constant and throws {@link AssertionError} on
 * mismatch, catching encoding/routing bugs early.
 *
 * <p>The returned decoder wraps the supplied buffer via the SBE flyweight pattern. Modifying the
 * buffer after this call invalidates the decoder's state.
 *
 * <p>Thread-safe -- all methods are stateless static functions. Every method creates a local {@link
 * MessageHeaderDecoder} instance; there is no shared mutable state.
 *
 * <p>This class is test infrastructure only; it allocates decoder instances on every call.
 */
public final class SbeTestDecoder {

  private SbeTestDecoder() {}

  // -----------------------------------------------------------------------
  // Header inspection
  // -----------------------------------------------------------------------

  /**
   * Reads the SBE {@code templateId} from the message header at the given offset.
   *
   * @param buffer buffer containing the SBE message
   * @param offset byte offset where the header starts
   * @return the templateId encoded in the header
   */
  public static int templateId(final DirectBuffer buffer, final int offset) {
    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    header.wrap(buffer, offset);
    return header.templateId();
  }

  /**
   * Reads the SBE {@code templateId} from a byte array (header at offset 0).
   *
   * @param bytes SBE-encoded message bytes
   * @return the templateId encoded in the header
   */
  public static int templateId(final byte[] bytes) {
    return templateId(new UnsafeBuffer(bytes), 0);
  }

  // -----------------------------------------------------------------------
  // Generic header wrapper
  // -----------------------------------------------------------------------

  /**
   * Wraps and returns a {@link MessageHeaderDecoder} for manual iteration over multiple messages in
   * a buffer.
   *
   * <p>The returned header is a flyweight over the supplied buffer. Callers can read {@code
   * blockLength()}, {@code templateId()}, {@code schemaId()}, and {@code version()} to dispatch to
   * the appropriate body decoder.
   *
   * @param buffer buffer containing the SBE message
   * @param offset byte offset where the header starts
   * @return a header decoder wrapped at the given offset
   */
  public static MessageHeaderDecoder wrapHeader(final DirectBuffer buffer, final int offset) {
    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    header.wrap(buffer, offset);
    return header;
  }

  // -----------------------------------------------------------------------
  // Order event decoders
  // -----------------------------------------------------------------------

  /**
   * Decodes an {@link OrderCreatedEventDecoder} from the buffer, verifying the header templateId
   * matches {@link OrderCreatedEventDecoder#TEMPLATE_ID}.
   *
   * @param buffer buffer containing the SBE-encoded OrderCreatedEvent
   * @param offset byte offset where the message header starts
   * @return a decoder flyweight wrapped over the message body
   * @throws AssertionError if the header templateId does not match
   */
  public static OrderCreatedEventDecoder decodeOrderCreated(
      final DirectBuffer buffer, final int offset) {
    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    header.wrap(buffer, offset);
    if (header.templateId() != OrderCreatedEventDecoder.TEMPLATE_ID) {
      throw new AssertionError(
          "Expected templateId "
              + OrderCreatedEventDecoder.TEMPLATE_ID
              + " but was "
              + header.templateId());
    }
    final OrderCreatedEventDecoder decoder = new OrderCreatedEventDecoder();
    decoder.wrap(
        buffer,
        offset + MessageHeaderDecoder.ENCODED_LENGTH,
        header.blockLength(),
        header.version());
    return decoder;
  }

  /**
   * Decodes an {@link OrderCreatedEventDecoder} from a byte array (header at offset 0).
   *
   * @param bytes SBE-encoded OrderCreatedEvent bytes
   * @return a decoder flyweight wrapped over the message body
   * @throws AssertionError if the header templateId does not match
   */
  public static OrderCreatedEventDecoder decodeOrderCreated(final byte[] bytes) {
    return decodeOrderCreated(new UnsafeBuffer(bytes), 0);
  }

  /**
   * Decodes an {@link OrderRejectedEventDecoder} from the buffer, verifying the header templateId
   * matches {@link OrderRejectedEventDecoder#TEMPLATE_ID}.
   *
   * @param buffer buffer containing the SBE-encoded OrderRejectedEvent
   * @param offset byte offset where the message header starts
   * @return a decoder flyweight wrapped over the message body
   * @throws AssertionError if the header templateId does not match
   */
  public static OrderRejectedEventDecoder decodeOrderRejected(
      final DirectBuffer buffer, final int offset) {
    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    header.wrap(buffer, offset);
    if (header.templateId() != OrderRejectedEventDecoder.TEMPLATE_ID) {
      throw new AssertionError(
          "Expected templateId "
              + OrderRejectedEventDecoder.TEMPLATE_ID
              + " but was "
              + header.templateId());
    }
    final OrderRejectedEventDecoder decoder = new OrderRejectedEventDecoder();
    decoder.wrap(
        buffer,
        offset + MessageHeaderDecoder.ENCODED_LENGTH,
        header.blockLength(),
        header.version());
    return decoder;
  }

  /**
   * Decodes an {@link OrderRejectedEventDecoder} from a byte array (header at offset 0).
   *
   * @param bytes SBE-encoded OrderRejectedEvent bytes
   * @return a decoder flyweight wrapped over the message body
   * @throws AssertionError if the header templateId does not match
   */
  public static OrderRejectedEventDecoder decodeOrderRejected(final byte[] bytes) {
    return decodeOrderRejected(new UnsafeBuffer(bytes), 0);
  }

  /**
   * Decodes an {@link OrderFilledEventDecoder} from the buffer, verifying the header templateId
   * matches {@link OrderFilledEventDecoder#TEMPLATE_ID}.
   *
   * @param buffer buffer containing the SBE-encoded OrderFilledEvent
   * @param offset byte offset where the message header starts
   * @return a decoder flyweight wrapped over the message body
   * @throws AssertionError if the header templateId does not match
   */
  public static OrderFilledEventDecoder decodeOrderFilled(
      final DirectBuffer buffer, final int offset) {
    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    header.wrap(buffer, offset);
    if (header.templateId() != OrderFilledEventDecoder.TEMPLATE_ID) {
      throw new AssertionError(
          "Expected templateId "
              + OrderFilledEventDecoder.TEMPLATE_ID
              + " but was "
              + header.templateId());
    }
    final OrderFilledEventDecoder decoder = new OrderFilledEventDecoder();
    decoder.wrap(
        buffer,
        offset + MessageHeaderDecoder.ENCODED_LENGTH,
        header.blockLength(),
        header.version());
    return decoder;
  }

  /**
   * Decodes an {@link OrderFilledEventDecoder} from a byte array (header at offset 0).
   *
   * @param bytes SBE-encoded OrderFilledEvent bytes
   * @return a decoder flyweight wrapped over the message body
   * @throws AssertionError if the header templateId does not match
   */
  public static OrderFilledEventDecoder decodeOrderFilled(final byte[] bytes) {
    return decodeOrderFilled(new UnsafeBuffer(bytes), 0);
  }

  /**
   * Decodes an {@link OrderCanceledEventDecoder} from the buffer, verifying the header templateId
   * matches {@link OrderCanceledEventDecoder#TEMPLATE_ID}.
   *
   * @param buffer buffer containing the SBE-encoded OrderCanceledEvent
   * @param offset byte offset where the message header starts
   * @return a decoder flyweight wrapped over the message body
   * @throws AssertionError if the header templateId does not match
   */
  public static OrderCanceledEventDecoder decodeOrderCanceled(
      final DirectBuffer buffer, final int offset) {
    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    header.wrap(buffer, offset);
    if (header.templateId() != OrderCanceledEventDecoder.TEMPLATE_ID) {
      throw new AssertionError(
          "Expected templateId "
              + OrderCanceledEventDecoder.TEMPLATE_ID
              + " but was "
              + header.templateId());
    }
    final OrderCanceledEventDecoder decoder = new OrderCanceledEventDecoder();
    decoder.wrap(
        buffer,
        offset + MessageHeaderDecoder.ENCODED_LENGTH,
        header.blockLength(),
        header.version());
    return decoder;
  }

  /**
   * Decodes an {@link OrderCanceledEventDecoder} from a byte array (header at offset 0).
   *
   * @param bytes SBE-encoded OrderCanceledEvent bytes
   * @return a decoder flyweight wrapped over the message body
   * @throws AssertionError if the header templateId does not match
   */
  public static OrderCanceledEventDecoder decodeOrderCanceled(final byte[] bytes) {
    return decodeOrderCanceled(new UnsafeBuffer(bytes), 0);
  }

  // -----------------------------------------------------------------------
  // Reference data event decoders
  // -----------------------------------------------------------------------

  /**
   * Decodes an {@link AccountLoadedEventDecoder} from the buffer, verifying the header templateId
   * matches {@link AccountLoadedEventDecoder#TEMPLATE_ID}.
   *
   * @param buffer buffer containing the SBE-encoded AccountLoadedEvent
   * @param offset byte offset where the message header starts
   * @return a decoder flyweight wrapped over the message body
   * @throws AssertionError if the header templateId does not match
   */
  public static AccountLoadedEventDecoder decodeAccountLoaded(
      final DirectBuffer buffer, final int offset) {
    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    header.wrap(buffer, offset);
    if (header.templateId() != AccountLoadedEventDecoder.TEMPLATE_ID) {
      throw new AssertionError(
          "Expected templateId "
              + AccountLoadedEventDecoder.TEMPLATE_ID
              + " but was "
              + header.templateId());
    }
    final AccountLoadedEventDecoder decoder = new AccountLoadedEventDecoder();
    decoder.wrap(
        buffer,
        offset + MessageHeaderDecoder.ENCODED_LENGTH,
        header.blockLength(),
        header.version());
    return decoder;
  }

  /**
   * Decodes an {@link AccountLoadedEventDecoder} from a byte array (header at offset 0).
   *
   * @param bytes SBE-encoded AccountLoadedEvent bytes
   * @return a decoder flyweight wrapped over the message body
   * @throws AssertionError if the header templateId does not match
   */
  public static AccountLoadedEventDecoder decodeAccountLoaded(final byte[] bytes) {
    return decodeAccountLoaded(new UnsafeBuffer(bytes), 0);
  }

  /**
   * Decodes an {@link AccountLoadRejectedEventDecoder} from the buffer, verifying the header
   * templateId matches {@link AccountLoadRejectedEventDecoder#TEMPLATE_ID}.
   *
   * @param buffer buffer containing the SBE-encoded AccountLoadRejectedEvent
   * @param offset byte offset where the message header starts
   * @return a decoder flyweight wrapped over the message body
   * @throws AssertionError if the header templateId does not match
   */
  public static AccountLoadRejectedEventDecoder decodeAccountLoadRejected(
      final DirectBuffer buffer, final int offset) {
    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    header.wrap(buffer, offset);
    if (header.templateId() != AccountLoadRejectedEventDecoder.TEMPLATE_ID) {
      throw new AssertionError(
          "Expected templateId "
              + AccountLoadRejectedEventDecoder.TEMPLATE_ID
              + " but was "
              + header.templateId());
    }
    final AccountLoadRejectedEventDecoder decoder = new AccountLoadRejectedEventDecoder();
    decoder.wrap(
        buffer,
        offset + MessageHeaderDecoder.ENCODED_LENGTH,
        header.blockLength(),
        header.version());
    return decoder;
  }

  /**
   * Decodes an {@link AccountLoadRejectedEventDecoder} from a byte array (header at offset 0).
   *
   * @param bytes SBE-encoded AccountLoadRejectedEvent bytes
   * @return a decoder flyweight wrapped over the message body
   * @throws AssertionError if the header templateId does not match
   */
  public static AccountLoadRejectedEventDecoder decodeAccountLoadRejected(final byte[] bytes) {
    return decodeAccountLoadRejected(new UnsafeBuffer(bytes), 0);
  }

  /**
   * Decodes a {@link CurrencyLoadedEventDecoder} from the buffer, verifying the header templateId
   * matches {@link CurrencyLoadedEventDecoder#TEMPLATE_ID}.
   *
   * @param buffer buffer containing the SBE-encoded CurrencyLoadedEvent
   * @param offset byte offset where the message header starts
   * @return a decoder flyweight wrapped over the message body
   * @throws AssertionError if the header templateId does not match
   */
  public static CurrencyLoadedEventDecoder decodeCurrencyLoaded(
      final DirectBuffer buffer, final int offset) {
    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    header.wrap(buffer, offset);
    if (header.templateId() != CurrencyLoadedEventDecoder.TEMPLATE_ID) {
      throw new AssertionError(
          "Expected templateId "
              + CurrencyLoadedEventDecoder.TEMPLATE_ID
              + " but was "
              + header.templateId());
    }
    final CurrencyLoadedEventDecoder decoder = new CurrencyLoadedEventDecoder();
    decoder.wrap(
        buffer,
        offset + MessageHeaderDecoder.ENCODED_LENGTH,
        header.blockLength(),
        header.version());
    return decoder;
  }

  /**
   * Decodes a {@link CurrencyLoadedEventDecoder} from a byte array (header at offset 0).
   *
   * @param bytes SBE-encoded CurrencyLoadedEvent bytes
   * @return a decoder flyweight wrapped over the message body
   * @throws AssertionError if the header templateId does not match
   */
  public static CurrencyLoadedEventDecoder decodeCurrencyLoaded(final byte[] bytes) {
    return decodeCurrencyLoaded(new UnsafeBuffer(bytes), 0);
  }

  /**
   * Decodes a {@link CurrencyLoadRejectedEventDecoder} from the buffer, verifying the header
   * templateId matches {@link CurrencyLoadRejectedEventDecoder#TEMPLATE_ID}.
   *
   * @param buffer buffer containing the SBE-encoded CurrencyLoadRejectedEvent
   * @param offset byte offset where the message header starts
   * @return a decoder flyweight wrapped over the message body
   * @throws AssertionError if the header templateId does not match
   */
  public static CurrencyLoadRejectedEventDecoder decodeCurrencyLoadRejected(
      final DirectBuffer buffer, final int offset) {
    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    header.wrap(buffer, offset);
    if (header.templateId() != CurrencyLoadRejectedEventDecoder.TEMPLATE_ID) {
      throw new AssertionError(
          "Expected templateId "
              + CurrencyLoadRejectedEventDecoder.TEMPLATE_ID
              + " but was "
              + header.templateId());
    }
    final CurrencyLoadRejectedEventDecoder decoder = new CurrencyLoadRejectedEventDecoder();
    decoder.wrap(
        buffer,
        offset + MessageHeaderDecoder.ENCODED_LENGTH,
        header.blockLength(),
        header.version());
    return decoder;
  }

  /**
   * Decodes a {@link CurrencyLoadRejectedEventDecoder} from a byte array (header at offset 0).
   *
   * @param bytes SBE-encoded CurrencyLoadRejectedEvent bytes
   * @return a decoder flyweight wrapped over the message body
   * @throws AssertionError if the header templateId does not match
   */
  public static CurrencyLoadRejectedEventDecoder decodeCurrencyLoadRejected(final byte[] bytes) {
    return decodeCurrencyLoadRejected(new UnsafeBuffer(bytes), 0);
  }

  /**
   * Decodes a {@link RiskLimitLoadedEventDecoder} from the buffer, verifying the header templateId
   * matches {@link RiskLimitLoadedEventDecoder#TEMPLATE_ID}.
   *
   * @param buffer buffer containing the SBE-encoded RiskLimitLoadedEvent
   * @param offset byte offset where the message header starts
   * @return a decoder flyweight wrapped over the message body
   * @throws AssertionError if the header templateId does not match
   */
  public static RiskLimitLoadedEventDecoder decodeRiskLimitLoaded(
      final DirectBuffer buffer, final int offset) {
    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    header.wrap(buffer, offset);
    if (header.templateId() != RiskLimitLoadedEventDecoder.TEMPLATE_ID) {
      throw new AssertionError(
          "Expected templateId "
              + RiskLimitLoadedEventDecoder.TEMPLATE_ID
              + " but was "
              + header.templateId());
    }
    final RiskLimitLoadedEventDecoder decoder = new RiskLimitLoadedEventDecoder();
    decoder.wrap(
        buffer,
        offset + MessageHeaderDecoder.ENCODED_LENGTH,
        header.blockLength(),
        header.version());
    return decoder;
  }

  /**
   * Decodes a {@link RiskLimitLoadedEventDecoder} from a byte array (header at offset 0).
   *
   * @param bytes SBE-encoded RiskLimitLoadedEvent bytes
   * @return a decoder flyweight wrapped over the message body
   * @throws AssertionError if the header templateId does not match
   */
  public static RiskLimitLoadedEventDecoder decodeRiskLimitLoaded(final byte[] bytes) {
    return decodeRiskLimitLoaded(new UnsafeBuffer(bytes), 0);
  }

  /**
   * Decodes a {@link RiskLimitLoadRejectedEventDecoder} from the buffer, verifying the header
   * templateId matches {@link RiskLimitLoadRejectedEventDecoder#TEMPLATE_ID}.
   *
   * @param buffer buffer containing the SBE-encoded RiskLimitLoadRejectedEvent
   * @param offset byte offset where the message header starts
   * @return a decoder flyweight wrapped over the message body
   * @throws AssertionError if the header templateId does not match
   */
  public static RiskLimitLoadRejectedEventDecoder decodeRiskLimitLoadRejected(
      final DirectBuffer buffer, final int offset) {
    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    header.wrap(buffer, offset);
    if (header.templateId() != RiskLimitLoadRejectedEventDecoder.TEMPLATE_ID) {
      throw new AssertionError(
          "Expected templateId "
              + RiskLimitLoadRejectedEventDecoder.TEMPLATE_ID
              + " but was "
              + header.templateId());
    }
    final RiskLimitLoadRejectedEventDecoder decoder = new RiskLimitLoadRejectedEventDecoder();
    decoder.wrap(
        buffer,
        offset + MessageHeaderDecoder.ENCODED_LENGTH,
        header.blockLength(),
        header.version());
    return decoder;
  }

  /**
   * Decodes a {@link RiskLimitLoadRejectedEventDecoder} from a byte array (header at offset 0).
   *
   * @param bytes SBE-encoded RiskLimitLoadRejectedEvent bytes
   * @return a decoder flyweight wrapped over the message body
   * @throws AssertionError if the header templateId does not match
   */
  public static RiskLimitLoadRejectedEventDecoder decodeRiskLimitLoadRejected(final byte[] bytes) {
    return decodeRiskLimitLoadRejected(new UnsafeBuffer(bytes), 0);
  }

  // -----------------------------------------------------------------------
  // Gateway decoder
  // -----------------------------------------------------------------------

  /**
   * Decodes an {@link ExecutionReportDecoder} from the buffer, verifying the header templateId
   * matches {@link ExecutionReportDecoder#TEMPLATE_ID}.
   *
   * @param buffer buffer containing the SBE-encoded ExecutionReport
   * @param offset byte offset where the message header starts
   * @return a decoder flyweight wrapped over the message body
   * @throws AssertionError if the header templateId does not match
   */
  public static ExecutionReportDecoder decodeExecutionReport(
      final DirectBuffer buffer, final int offset) {
    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    header.wrap(buffer, offset);
    if (header.templateId() != ExecutionReportDecoder.TEMPLATE_ID) {
      throw new AssertionError(
          "Expected templateId "
              + ExecutionReportDecoder.TEMPLATE_ID
              + " but was "
              + header.templateId());
    }
    final ExecutionReportDecoder decoder = new ExecutionReportDecoder();
    decoder.wrap(
        buffer,
        offset + MessageHeaderDecoder.ENCODED_LENGTH,
        header.blockLength(),
        header.version());
    return decoder;
  }

  /**
   * Decodes an {@link ExecutionReportDecoder} from a byte array (header at offset 0).
   *
   * @param bytes SBE-encoded ExecutionReport bytes
   * @return a decoder flyweight wrapped over the message body
   * @throws AssertionError if the header templateId does not match
   */
  public static ExecutionReportDecoder decodeExecutionReport(final byte[] bytes) {
    return decodeExecutionReport(new UnsafeBuffer(bytes), 0);
  }
}
