package com.trading.engine.pricing.codec;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.PriceRequestDecoder;
import com.trading.engine.messages.sbe.PriceValidationRequestDecoder;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

/**
 * Zero-allocation SBE message dispatcher for the pricing service. Uses {@link
 * ControlledFragmentHandler} so that back-pressure on the response publication can abort and replay
 * the inbound fragment.
 *
 * <p>Inbound messages are dispatched based on the SBE {@code templateId} in the message header:
 *
 * <ul>
 *   <li>{@link PriceRequestDecoder#TEMPLATE_ID} (50) — cluster asks the pricing service to price an
 *       RFQ. Dispatched to {@link PricingMessageHandler#onPriceRequest}.
 *   <li>{@link PriceValidationRequestDecoder#TEMPLATE_ID} (52) — cluster asks the pricing service
 *       to validate an order price. Dispatched to {@link
 *       PricingMessageHandler#onPriceValidationRequest}.
 *   <li>Unknown template IDs are logged at WARN level and the fragment is consumed (returns {@link
 *       Action#CONTINUE}).
 * </ul>
 *
 * <p><b>Back-pressure protocol.</b> Each handler callback returns an {@link Action}. If the handler
 * returns {@link Action#ABORT} (e.g., the response publication is back-pressured), this dispatcher
 * propagates that action to Aeron, which will re-deliver the fragment on the next {@code
 * controlledPoll()} call. This eliminates the need for an application-level retry loop and
 * guarantees no message loss on a slow response path.
 *
 * <p><b>Allocation.</b> Zero allocation after construction. The {@link MessageHeaderDecoder},
 * {@link PriceRequestDecoder}, and {@link PriceValidationRequestDecoder} flyweights are
 * pre-allocated as instance fields and re-wrapped on every {@code onFragment} call.
 *
 * <p><b>Threading.</b> Not thread-safe — single-threaded pricing-service duty-cycle thread only.
 *
 * @see PricingMessageHandler
 * @see PricingResponseEncoder
 */
public final class PricingMessageDispatcher implements ControlledFragmentHandler {

  private static final Log LOG = LogFactory.getLog(PricingMessageDispatcher.class);

  // --- Pre-allocated SBE flyweight decoders (reused on every onFragment call) ---

  /**
   * SBE message header decoder. Wrapped at the start of every fragment to extract the {@code
   * templateId}, {@code blockLength}, and {@code version} needed to dispatch and wrap the body
   * decoder.
   */
  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();

  /**
   * SBE decoder for PriceRequest (templateId=50). Pre-allocated to avoid per-fragment allocation;
   * wrapped over the message body only when the header's templateId matches.
   */
  private final PriceRequestDecoder priceRequestDecoder = new PriceRequestDecoder();

  /**
   * SBE decoder for PriceValidationRequest (templateId=52). Pre-allocated to avoid per-fragment
   * allocation; wrapped over the message body only when the header's templateId matches.
   */
  private final PriceValidationRequestDecoder validationRequestDecoder =
      new PriceValidationRequestDecoder();

  /** Callback handler that processes decoded pricing messages. */
  private final PricingMessageHandler handler;

  /**
   * Callback interface for pricing message dispatch. Implementations receive pre-wrapped SBE
   * decoders positioned over the message body; they must not retain references to the decoder or
   * the buffer beyond the scope of the callback invocation.
   *
   * <p>Each callback returns an {@link Action} to control Aeron's fragment consumption:
   *
   * <ul>
   *   <li>{@link Action#CONTINUE} — message processed, advance to the next fragment.
   *   <li>{@link Action#ABORT} — back-pressured; Aeron will re-deliver this fragment.
   *   <li>{@link Action#BREAK} — stop polling after this fragment (no re-delivery).
   *   <li>{@link Action#COMMIT} — commit consumption position up to and including this fragment.
   * </ul>
   */
  public interface PricingMessageHandler {

    /**
     * Handle an inbound PriceRequest (templateId=50). The decoder is already wrapped over the
     * message body and valid for the duration of this call.
     *
     * @param decoder pre-wrapped PriceRequest decoder — do not retain past return
     * @param buffer the underlying DirectBuffer containing the full fragment
     * @param offset byte offset of the fragment start (including SBE header)
     * @param length byte length of the complete fragment
     * @return {@link Action#CONTINUE} on success, {@link Action#ABORT} on back-pressure
     */
    ControlledFragmentHandler.Action onPriceRequest(
        PriceRequestDecoder decoder, DirectBuffer buffer, int offset, int length);

    /**
     * Handle an inbound PriceValidationRequest (templateId=52). The decoder is already wrapped over
     * the message body and valid for the duration of this call.
     *
     * @param decoder pre-wrapped PriceValidationRequest decoder — do not retain past return
     * @param buffer the underlying DirectBuffer containing the full fragment
     * @param offset byte offset of the fragment start (including SBE header)
     * @param length byte length of the complete fragment
     * @return {@link Action#CONTINUE} on success, {@link Action#ABORT} on back-pressure
     */
    ControlledFragmentHandler.Action onPriceValidationRequest(
        PriceValidationRequestDecoder decoder, DirectBuffer buffer, int offset, int length);
  }

  /**
   * Construct a dispatcher that delegates decoded messages to the given handler.
   *
   * @param handler callback handler for pricing messages — must not be null
   * @throws NullPointerException if {@code handler} is null
   */
  public PricingMessageDispatcher(final PricingMessageHandler handler) {
    if (handler == null) {
      throw new NullPointerException("handler");
    }
    this.handler = handler;
  }

  // ===========================================================================
  // ControlledFragmentHandler
  // ===========================================================================

  /**
   * Decode the SBE message header, dispatch to the appropriate handler based on {@code templateId},
   * and return the handler's {@link Action} to Aeron. Unknown template IDs are logged at WARN level
   * and consumed via {@link Action#CONTINUE}. Zero allocation on every call.
   *
   * @param buffer the buffer containing the fragment data
   * @param offset byte offset of the fragment within the buffer
   * @param length byte length of the fragment
   * @param header Aeron fragment header (provides position, session, flags)
   * @return the {@link Action} returned by the handler, or {@link Action#CONTINUE} for unknown
   *     templates
   */
  @Override
  public Action onFragment(
      final DirectBuffer buffer, final int offset, final int length, final Header header) {

    headerDecoder.wrap(buffer, offset);
    final int templateId = headerDecoder.templateId();

    return switch (templateId) {
      case PriceRequestDecoder.TEMPLATE_ID -> {
        priceRequestDecoder.wrap(
            buffer,
            offset + MessageHeaderDecoder.ENCODED_LENGTH,
            headerDecoder.blockLength(),
            headerDecoder.version());
        yield handler.onPriceRequest(priceRequestDecoder, buffer, offset, length);
      }
      case PriceValidationRequestDecoder.TEMPLATE_ID -> {
        validationRequestDecoder.wrap(
            buffer,
            offset + MessageHeaderDecoder.ENCODED_LENGTH,
            headerDecoder.blockLength(),
            headerDecoder.version());
        yield handler.onPriceValidationRequest(validationRequestDecoder, buffer, offset, length);
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
