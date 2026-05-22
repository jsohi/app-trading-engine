package com.trading.engine.cluster.handler;

import com.trading.engine.cluster.state.TradingState;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.ResumeTradingCommandDecoder;
import com.trading.engine.messages.sbe.TradingHaltClearedEventEncoder;
import io.aeron.cluster.service.ClientSession;
import java.util.Arrays;
import java.util.Objects;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Handles {@code ResumeTradingCommand} (template 18): clears the cluster-wide trading-halt flag in
 * {@link TradingState} and emits a {@code TradingHaltClearedEvent} (118) for audit. Mirror of
 * {@link HaltTradingCommandHandler}.
 *
 * <p><b>Idempotency:</b> issuing the command while already cleared is a no-op for the state but
 * still emits an event. The {@code previouslyHalted} field on the event distinguishes "real"
 * clear-transitions from re-clears, so operator audit logs capture every admin attempt.
 *
 * <p><b>Authorization:</b> the cluster trusts whatever lands on its ingress; gateway-side admin
 * auth + role-check gates the command before it reaches this handler (APP-153).
 *
 * <p><b>Input validation:</b> the handler trusts that the inbound SBE message conforms to the
 * schema (reason fits in the 64-byte {@code Text} field, block length matches). SBE encoders /
 * Aeron framing on the gateway side enforce this before the message ever reaches cluster ingress; a
 * malformed wire payload would surface at decoder-wrap time with a clear bounds exception rather
 * than silent corruption. Consistent with every other {@link CommandHandler} in this module.
 *
 * <p><b>Threading:</b> single-threaded cluster duty cycle. No synchronization required.
 *
 * <p><b>Allocation:</b> zero allocation after construction. Pre-allocates the encoder, header,
 * scratch buffer, and reason byte array.
 *
 * @see TradingState#setTradingHalted(boolean)
 * @see HaltTradingCommandHandler
 */
public final class ResumeTradingCommandHandler implements CommandHandler {

  /** See {@link HaltTradingCommandHandler#REASON_BYTES} for rationale. */
  private static final int REASON_BYTES = 64;

  /** See {@link HaltTradingCommandHandler#EGRESS_BUFFER_BYTES} for rationale. */
  private static final int EGRESS_BUFFER_BYTES = 256;

  private final TradingState tradingState;

  private final ResumeTradingCommandDecoder decoder = new ResumeTradingCommandDecoder();
  private final TradingHaltClearedEventEncoder encoder = new TradingHaltClearedEventEncoder();
  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();

  private final UnsafeBuffer egressBuffer = new UnsafeBuffer(new byte[EGRESS_BUFFER_BYTES]);
  private final byte[] reasonScratch = new byte[REASON_BYTES];

  /**
   * @param tradingState the cluster trading state (must not be null) — owns the halt flag this
   *     handler clears
   */
  public ResumeTradingCommandHandler(final TradingState tradingState) {
    this.tradingState = Objects.requireNonNull(tradingState, "tradingState");
  }

  @Override
  public int commandTemplateId() {
    return ResumeTradingCommandDecoder.TEMPLATE_ID;
  }

  @Override
  public void onCommand(
      final ClientSession session,
      final long clusterTimestamp,
      final DirectBuffer buffer,
      final int offset,
      final int length,
      final int blockLength,
      final int version,
      final EventSink eventSink) {

    // 1. Decode inbound: wrap past the header (already consumed by the service for dispatch).
    decoder.wrap(buffer, offset + MessageHeaderEncoder.ENCODED_LENGTH, blockLength, version);
    Arrays.fill(reasonScratch, (byte) 0);
    decoder.getReason(reasonScratch, 0);

    // 2. Capture pre-transition state for the audit event (BEFORE mutating).
    final boolean wasHalted = tradingState.isTradingHalted();

    // 3. Apply state. Idempotent — re-clears are no-ops for the flag but still emit below.
    tradingState.setTradingHalted(false);

    // 4. Encode the audit event. EventSink stamps sequenceNumber and timestamp at egress.
    encoder.wrapAndApplyHeader(egressBuffer, 0, headerEncoder);
    encoder.sequenceNumber(0L);
    encoder.timestamp(0L);
    encoder.adminSessionId(session != null ? session.id() : 0L);
    encoder.previouslyHalted((short) (wasHalted ? 1 : 0));
    encoder.putReason(reasonScratch, 0);

    final int eventLen = MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();
    eventSink.emit(clusterTimestamp, egressBuffer, 0, eventLen);
  }
}
