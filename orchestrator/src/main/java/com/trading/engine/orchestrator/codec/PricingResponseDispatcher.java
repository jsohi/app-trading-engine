package com.trading.engine.orchestrator.codec;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.PriceResponseDecoder;
import com.trading.engine.messages.sbe.PriceValidationResponseDecoder;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

/**
 * Zero-allocation SBE message dispatcher for inbound pricing service responses on Aeron IPC stream
 * 201. Uses {@link ControlledFragmentHandler} so that back-pressure on outbound publications can
 * abort and replay the inbound fragment.
 *
 * <p>Inbound messages are dispatched based on the SBE {@code templateId} in the message header:
 *
 * <ul>
 *   <li>{@link PriceResponseDecoder#TEMPLATE_ID} (51) — pricing service responds with a quote (or
 *       decline). Dispatched to {@link PricingResponseHandler#onPriceResponse}.
 *   <li>{@link PriceValidationResponseDecoder#TEMPLATE_ID} (53) — pricing service confirms or
 *       rejects an order price. Dispatched to {@link
 *       PricingResponseHandler#onPriceValidationResponse}.
 *   <li>Unknown template IDs are logged at WARN level and consumed via {@link Action#CONTINUE}.
 * </ul>
 *
 * <p><b>Length validation.</b> Two-stage guard prevents SBE decoder exceptions on truncated
 * fragments: (1) fragment must be at least as large as the SBE message header, (2) fragment must be
 * at least header + blockLength to fully wrap the body decoder.
 *
 * <p><b>Back-pressure protocol.</b> Each handler callback returns an {@link Action}. If the handler
 * returns {@link Action#ABORT}, this dispatcher propagates that action to Aeron, which re-delivers
 * the fragment on the next {@code controlledPoll()} call.
 *
 * <p><b>Allocation:</b> zero allocation after construction. All SBE flyweight decoders are
 * pre-allocated as instance fields.
 *
 * <p><b>Threading:</b> not thread-safe — single-threaded orchestrator duty-cycle thread only.
 *
 * @see PricingResponseHandler
 * @see GatewayMessageDispatcher
 */
public final class PricingResponseDispatcher implements ControlledFragmentHandler {

  private static final Log LOG = LogFactory.getLog(PricingResponseDispatcher.class);

  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final PriceResponseDecoder priceResponseDecoder = new PriceResponseDecoder();
  private final PriceValidationResponseDecoder validationResponseDecoder =
      new PriceValidationResponseDecoder();

  private final PricingResponseHandler handler;

  /**
   * Callback interface for pricing response dispatch. Implementations receive pre-wrapped SBE
   * decoders positioned over the message body; they must not retain references to the decoder or
   * the buffer beyond the scope of the callback invocation.
   */
  public interface PricingResponseHandler {

    /**
     * Handle an inbound PriceResponse (templateId=51).
     *
     * @param decoder pre-wrapped PriceResponse decoder — do not retain past return
     * @param buffer the underlying DirectBuffer containing the full fragment
     * @param offset byte offset of the fragment start (including SBE header)
     * @param length byte length of the complete fragment
     * @return {@link Action#CONTINUE} on success, {@link Action#ABORT} on back-pressure
     */
    ControlledFragmentHandler.Action onPriceResponse(
        PriceResponseDecoder decoder, DirectBuffer buffer, int offset, int length);

    /**
     * Handle an inbound PriceValidationResponse (templateId=53).
     *
     * @param decoder pre-wrapped PriceValidationResponse decoder — do not retain past return
     * @param buffer the underlying DirectBuffer containing the full fragment
     * @param offset byte offset of the fragment start (including SBE header)
     * @param length byte length of the complete fragment
     * @return {@link Action#CONTINUE} on success, {@link Action#ABORT} on back-pressure
     */
    ControlledFragmentHandler.Action onPriceValidationResponse(
        PriceValidationResponseDecoder decoder, DirectBuffer buffer, int offset, int length);
  }

  /**
   * Construct a dispatcher that delegates decoded messages to the given handler.
   *
   * @param handler callback handler for pricing responses — must not be null
   * @throws NullPointerException if {@code handler} is null
   */
  public PricingResponseDispatcher(final PricingResponseHandler handler) {
    if (handler == null) {
      throw new NullPointerException("handler");
    }
    this.handler = handler;
  }

  @Override
  public Action onFragment(
      final DirectBuffer buffer, final int offset, final int length, final Header header) {

    // Stage 1: fragment must contain at least the SBE message header
    if (length < MessageHeaderDecoder.ENCODED_LENGTH) {
      LOG.warn()
          .append("Fragment too short for SBE header: length=")
          .append(length)
          .append(" required=")
          .append(MessageHeaderDecoder.ENCODED_LENGTH)
          .commit();
      return Action.CONTINUE;
    }

    headerDecoder.wrap(buffer, offset);
    final int templateId = headerDecoder.templateId();
    final int blockLength = headerDecoder.blockLength();

    // Stage 2: fragment must contain header + body block
    if (length < MessageHeaderDecoder.ENCODED_LENGTH + blockLength) {
      LOG.warn()
          .append("Fragment too short for body: templateId=")
          .append(templateId)
          .append(" blockLength=")
          .append(blockLength)
          .append(" fragmentLength=")
          .append(length)
          .commit();
      return Action.CONTINUE;
    }

    return switch (templateId) {
      case PriceResponseDecoder.TEMPLATE_ID -> {
        priceResponseDecoder.wrap(
            buffer,
            offset + MessageHeaderDecoder.ENCODED_LENGTH,
            blockLength,
            headerDecoder.version());
        yield handler.onPriceResponse(priceResponseDecoder, buffer, offset, length);
      }
      case PriceValidationResponseDecoder.TEMPLATE_ID -> {
        validationResponseDecoder.wrap(
            buffer,
            offset + MessageHeaderDecoder.ENCODED_LENGTH,
            blockLength,
            headerDecoder.version());
        yield handler.onPriceValidationResponse(validationResponseDecoder, buffer, offset, length);
      }
      default -> {
        LOG.warn()
            .append("Unknown pricing templateId=")
            .append(templateId)
            .append(" length=")
            .append(length)
            .commit();
        yield Action.CONTINUE;
      }
    };
  }
}
