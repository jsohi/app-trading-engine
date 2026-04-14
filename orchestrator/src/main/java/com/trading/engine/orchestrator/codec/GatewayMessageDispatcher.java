package com.trading.engine.orchestrator.codec;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.NewOrderSingleDecoder;
import com.trading.engine.messages.sbe.QuoteRequestDecoder;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

/**
 * Zero-allocation SBE message dispatcher for inbound gateway messages on Aeron IPC stream 100. Uses
 * {@link ControlledFragmentHandler} so that back-pressure on outbound publications can abort and
 * replay the inbound fragment.
 *
 * <p>Inbound messages are dispatched based on the SBE {@code templateId} in the message header:
 *
 * <ul>
 *   <li>{@link QuoteRequestDecoder#TEMPLATE_ID} (1) — client requests a quote. Dispatched to {@link
 *       GatewayMessageHandler#onQuoteRequest}.
 *   <li>{@link NewOrderSingleDecoder#TEMPLATE_ID} (4) — client sends a new order (direct or RFQ
 *       accept). Dispatched to {@link GatewayMessageHandler#onNewOrderSingle}.
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
 * @see GatewayMessageHandler
 * @see PricingResponseDispatcher
 */
public final class GatewayMessageDispatcher implements ControlledFragmentHandler {

  private static final Log LOG = LogFactory.getLog(GatewayMessageDispatcher.class);

  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final QuoteRequestDecoder quoteRequestDecoder = new QuoteRequestDecoder();
  private final NewOrderSingleDecoder newOrderSingleDecoder = new NewOrderSingleDecoder();

  private final GatewayMessageHandler handler;

  /**
   * Callback interface for gateway message dispatch. Implementations receive pre-wrapped SBE
   * decoders positioned over the message body; they must not retain references to the decoder or
   * the buffer beyond the scope of the callback invocation.
   *
   * <p>Each callback returns an {@link Action} to control Aeron's fragment consumption:
   *
   * <ul>
   *   <li>{@link Action#CONTINUE} — message processed, advance to the next fragment.
   *   <li>{@link Action#ABORT} — back-pressured; Aeron will re-deliver this fragment.
   * </ul>
   */
  public interface GatewayMessageHandler {

    /**
     * Handle an inbound QuoteRequest (templateId=1, FIX 35=R).
     *
     * @param decoder pre-wrapped QuoteRequest decoder — do not retain past return
     * @param buffer the underlying DirectBuffer containing the full fragment
     * @param offset byte offset of the fragment start (including SBE header)
     * @param length byte length of the complete fragment
     * @return {@link Action#CONTINUE} on success, {@link Action#ABORT} on back-pressure
     */
    Action onQuoteRequest(QuoteRequestDecoder decoder, DirectBuffer buffer, int offset, int length);

    /**
     * Handle an inbound NewOrderSingle (templateId=4, FIX 35=D).
     *
     * @param decoder pre-wrapped NewOrderSingle decoder — do not retain past return
     * @param buffer the underlying DirectBuffer containing the full fragment
     * @param offset byte offset of the fragment start (including SBE header)
     * @param length byte length of the complete fragment
     * @return {@link Action#CONTINUE} on success, {@link Action#ABORT} on back-pressure
     */
    Action onNewOrderSingle(
        NewOrderSingleDecoder decoder, DirectBuffer buffer, int offset, int length);
  }

  /**
   * Construct a dispatcher that delegates decoded messages to the given handler.
   *
   * @param handler callback handler for gateway messages — must not be null
   * @throws NullPointerException if {@code handler} is null
   */
  public GatewayMessageDispatcher(final GatewayMessageHandler handler) {
    if (handler == null) {
      throw new NullPointerException("handler");
    }
    this.handler = handler;
  }

  /**
   * Dispatches one inbound Aeron fragment from the gateway after SBE header/body length validation.
   * Decodes the template ID and delegates to the appropriate handler method.
   *
   * @param buffer inbound fragment buffer
   * @param offset fragment start offset within the buffer
   * @param length fragment length in bytes
   * @param header Aeron logbuffer header metadata
   * @return {@link Action#CONTINUE} when consumed, or handler-provided action (including {@link
   *     Action#ABORT} for back-pressure re-delivery)
   */
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
      case QuoteRequestDecoder.TEMPLATE_ID -> {
        quoteRequestDecoder.wrap(
            buffer,
            offset + MessageHeaderDecoder.ENCODED_LENGTH,
            blockLength,
            headerDecoder.version());
        yield handler.onQuoteRequest(quoteRequestDecoder, buffer, offset, length);
      }
      case NewOrderSingleDecoder.TEMPLATE_ID -> {
        newOrderSingleDecoder.wrap(
            buffer,
            offset + MessageHeaderDecoder.ENCODED_LENGTH,
            blockLength,
            headerDecoder.version());
        yield handler.onNewOrderSingle(newOrderSingleDecoder, buffer, offset, length);
      }
      default -> {
        LOG.warn()
            .append("Unknown gateway templateId=")
            .append(templateId)
            .append(" length=")
            .append(length)
            .commit();
        yield Action.CONTINUE;
      }
    };
  }
}
