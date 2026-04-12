package com.trading.engine.cluster.handler;

import io.aeron.cluster.service.ClientSession;
import org.agrona.DirectBuffer;

/**
 * Handler for a single trading command type (e.g., NewOrderSingle, CancelOrderRequest). Each
 * implementation is registered by its {@link #commandTemplateId()} in the service's dispatch map.
 *
 * <p><b>Contract:</b> the handler decodes the command, validates it against cluster state, encodes
 * one or more domain events into a pre-allocated buffer, and emits them via {@link EventSink#emit}.
 * After emitting, the handler applies the event to the cluster's {@link
 * com.trading.engine.cluster.state.TradingState} (event-sourced: state is derived from the event,
 * not the command).
 *
 * <p><b>Threading:</b> single-threaded cluster duty cycle. No synchronization required.
 *
 * <p><b>Allocation:</b> zero allocation on the hot path. Implementations pre-allocate all SBE
 * flyweight decoders, encoders, and scratch buffers at construction time.
 *
 * <p><b>Design rationale:</b> passes {@code blockLength} and {@code version} as primitives
 * (extracted from the SBE header by the service) instead of the mutable {@link
 * com.trading.engine.messages.sbe.MessageHeaderDecoder} flyweight. This prevents handlers from
 * accidentally re-wrapping the service's dispatch decoder, which would corrupt dispatch state.
 *
 * <p><b>Dual handler pattern:</b> reference-data commands (templateIds 11-16) use the legacy {@link
 * com.trading.engine.cluster.refdata.ReferenceDataLoader} interface. Trading commands use this
 * interface. Both share the same {@link com.trading.engine.cluster.sequencer.EventSequencer}
 * instance for gapless sequencing. See APP-176 for future unification.
 *
 * @see EventSink
 * @see com.trading.engine.cluster.state.TradingState
 */
public interface CommandHandler {

  /**
   * Returns the SBE template ID of the command this handler processes.
   *
   * @return the command template ID (e.g., {@code NewOrderSingleDecoder.TEMPLATE_ID})
   */
  int commandTemplateId();

  /**
   * Processes a single command message. The handler decodes the command, validates, encodes domain
   * events, emits them via {@code eventSink}, and applies state changes.
   *
   * <p>The buffer contains the full SBE message (header + body). The handler should wrap its
   * decoder at {@code offset + MessageHeaderEncoder.ENCODED_LENGTH} using the provided {@code
   * blockLength} and {@code version}.
   *
   * @param session the client session that sent the command (for egress reply)
   * @param clusterTimestamp the cluster-assigned timestamp in epoch nanos
   * @param buffer the inbound message buffer (header + body)
   * @param offset the start offset of the SBE message header in the buffer
   * @param length the total message length (header + body)
   * @param blockLength the SBE block length from the decoded message header
   * @param version the SBE schema version from the decoded message header
   * @param eventSink the sink for emitting sequenced, journaled domain events
   */
  void onCommand(
      ClientSession session,
      long clusterTimestamp,
      DirectBuffer buffer,
      int offset,
      int length,
      int blockLength,
      int version,
      EventSink eventSink);
}
