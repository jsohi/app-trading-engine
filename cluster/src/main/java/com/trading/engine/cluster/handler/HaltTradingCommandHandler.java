package com.trading.engine.cluster.handler;

import com.trading.engine.cluster.state.TradingState;
import com.trading.engine.messages.sbe.HaltTradingCommandDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.TradingHaltActivatedEventEncoder;
import io.aeron.cluster.service.ClientSession;
import java.util.Arrays;
import java.util.Objects;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Handles {@code HaltTradingCommand} (template 17): sets the cluster-wide trading-halt flag in
 * {@link TradingState} and emits a {@code TradingHaltActivatedEvent} (117) for audit. While the
 * flag is set, {@link NewOrderSingleHandler} rejects every {@code NewOrderSingle} with {@code
 * RejectReasonEnum.TradingHalted} at Check 0 of validation (before any other check).
 *
 * <p><b>Idempotency:</b> issuing the command while already halted is a no-op for the state but
 * still emits an event. The {@code previouslyHalted} field on the event distinguishes "real"
 * transitions from re-halts, so operator audit logs capture every admin attempt without conflating
 * intent and effect. This matches the LMAX / CME ops-audit convention.
 *
 * <p><b>Authorization:</b> the cluster trusts whatever lands on its ingress; gateway-side admin
 * auth + role-check gates the command before it reaches this handler. See APP-153 for the gateway
 * admin route work.
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
 * scratch buffer, and reason byte array; the decoder is constructed once and re-wrapped per
 * command.
 *
 * @see TradingState#setTradingHalted(boolean)
 * @see ResumeTradingCommandHandler
 */
public final class HaltTradingCommandHandler implements CommandHandler {

  /**
   * SBE-encoded {@code Text} fields in this schema are fixed 64-byte char arrays (see {@code
   * trading-schema.xml} type definition: {@code <type name="Text" primitiveType="char"
   * length="64"/>}). Sized to match exactly so {@code getReason}/{@code putReason} round-trip
   * without truncation.
   */
  private static final int REASON_BYTES = 64;

  /**
   * Sized for {@code MessageHeaderEncoder.ENCODED_LENGTH} (8) plus the {@code
   * TradingHaltActivatedEventEncoder} block (sequenceNumber 8 + timestamp 8 + adminSessionId 8 +
   * previouslyHalted 1 + reason 64 = 89). 256 bytes provides ample headroom for any future schema
   * additions.
   */
  private static final int EGRESS_BUFFER_BYTES = 256;

  private final TradingState tradingState;

  private final HaltTradingCommandDecoder decoder = new HaltTradingCommandDecoder();
  private final TradingHaltActivatedEventEncoder encoder = new TradingHaltActivatedEventEncoder();
  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();

  private final UnsafeBuffer egressBuffer = new UnsafeBuffer(new byte[EGRESS_BUFFER_BYTES]);
  private final byte[] reasonScratch = new byte[REASON_BYTES];

  /**
   * @param tradingState the cluster trading state (must not be null) — owns the halt flag this
   *     handler toggles
   */
  public HaltTradingCommandHandler(final TradingState tradingState) {
    this.tradingState = Objects.requireNonNull(tradingState, "tradingState");
  }

  /**
   * {@inheritDoc}
   *
   * @return {@link HaltTradingCommandDecoder#TEMPLATE_ID} (SBE template 17)
   */
  @Override
  public int commandTemplateId() {
    return HaltTradingCommandDecoder.TEMPLATE_ID;
  }

  /**
   * Decodes one {@code HaltTradingCommand}, sets {@link TradingState#setTradingHalted}{@code
   * (true)}, and emits one {@code TradingHaltActivatedEvent} (template 117) via {@link EventSink}.
   *
   * <p>Idempotent — issuing the command while the flag is already set emits the audit event with
   * {@code previouslyHalted=1} but does not change state.
   *
   * @param session the originating Aeron Cluster session (may be {@code null} on the test path; the
   *     event then carries {@code adminSessionId=0})
   * @param clusterTimestamp the cluster-assigned timestamp in epoch nanos (stamped onto the emitted
   *     event by {@link EventSink#emit})
   * @param buffer the inbound SBE message buffer (header + body)
   * @param offset the start offset of the SBE message header in {@code buffer}
   * @param length the total message length (header + body)
   * @param blockLength the SBE block length from the decoded message header
   * @param version the SBE schema version from the decoded message header
   * @param eventSink the sink that stamps sequence number + timestamp and broadcasts the emitted
   *     event; must not be {@code null}
   */
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

    // 1. Decode inbound: wrap past the header (the service already consumed the header for
    // dispatch). The reason text is copied into scratch so the encoder can later re-emit it
    // without retaining a reference to the inbound buffer.
    decoder.wrap(buffer, offset + MessageHeaderEncoder.ENCODED_LENGTH, blockLength, version);
    Arrays.fill(reasonScratch, (byte) 0);
    decoder.getReason(reasonScratch, 0);

    // 2. Capture pre-transition state for the audit event (BEFORE mutating).
    final boolean wasHalted = tradingState.isTradingHalted();

    // 3. Apply state. Idempotent — re-halts are no-ops for the flag but still emit below.
    tradingState.setTradingHalted(true);

    // 4. Encode the audit event. EventSink stamps sequenceNumber and timestamp at egress, so we
    // write placeholder zeros here (matches OrderCreatedEvent / OrderRejectedEvent convention).
    encoder.wrapAndApplyHeader(egressBuffer, 0, headerEncoder);
    encoder.sequenceNumber(0L);
    encoder.timestamp(0L);
    // session == null on the test path (NewOrderSingleHandlerTradingHaltTest pattern); 0 is a
    // safe sentinel since Aeron Cluster never assigns session id 0.
    encoder.adminSessionId(session != null ? session.id() : 0L);
    encoder.previouslyHalted((short) (wasHalted ? 1 : 0));
    encoder.putReason(reasonScratch, 0);

    final int eventLen = MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();
    eventSink.emit(clusterTimestamp, egressBuffer, 0, eventLen);
  }
}
