package com.trading.engine.cluster.refdata;

import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * Handles a single SBE command templateId by decoding the command, validating it, upserting the
 * underlying {@link ReferenceDataStore}, and emitting a Loaded / LoadRejected event into a
 * caller-provided buffer. The {@link ReferenceDataRegistry} routes inbound cluster commands to the
 * loader registered for the matching {@link #commandTemplateId()}.
 *
 * <p><b>Output convention.</b> {@link #onCommand} writes a complete SBE event message (8-byte
 * header + body) into {@code eventDst} starting at {@code eventDstOffset} and returns the total
 * bytes written. Both successful (Loaded) and rejected (LoadRejected) outcomes write an event — the
 * caller does not need a separate "did it succeed" signal because the event templateId
 * distinguishes them. Returning 0 means "no event was written" (reserved for unexpected shapes;
 * loaders should always emit an event in normal operation).
 *
 * <p><b>Zero allocation.</b> Implementations hold pre-allocated SBE flyweight encoders / decoders
 * as fields. The hot path (decode → validate → upsert → encode) must not allocate. Validation throw
 * branches are not on the hot path.
 *
 * <p><b>Determinism.</b> Loaders run inside the cluster duty cycle. They must not consult
 * wall-clock time, randomness, or external state — only the in-cluster ref-data stores. The {@code
 * clusterTimestampNanos} parameter is the deterministic timestamp from {@code
 * ClusteredService.onSessionMessage}, suitable for stamping events.
 */
public interface ReferenceDataLoader {

  /**
   * SBE templateId of the command this loader handles (e.g., LoadAccount = 11, LoadCurrency = 13,
   * LoadRiskLimit = 15).
   */
  int commandTemplateId();

  /**
   * Decode the command at {@code src[srcOffset..srcOffset+srcLength]}, validate, upsert the
   * underlying store, and write a Loaded / LoadRejected event into {@code eventDst} starting at
   * {@code eventDstOffset}.
   *
   * @param header pre-wrapped {@link MessageHeaderDecoder} positioned over the command — the loader
   *     can read {@code header.blockLength()} / {@code header.version()} for SBE {@code wrap()}
   *     parameters
   * @param src buffer containing the command body, starting at {@code srcOffset +
   *     MessageHeaderDecoder.ENCODED_LENGTH}
   * @param srcOffset start offset of the command (the SBE header)
   * @param srcLength total bytes of the command (header + body)
   * @param eventDst destination buffer for the emitted event
   * @param eventDstOffset start offset where the event message will be written
   * @param sequenceNumber monotonic event sequence number from {@code EventSequencer}
   * @param clusterTimestampNanos cluster timestamp (epoch nanos) from {@code onSessionMessage}
   * @return total bytes of the emitted event message (header + body), or 0 if no event was written
   */
  int onCommand(
      MessageHeaderDecoder header,
      DirectBuffer src,
      int srcOffset,
      int srcLength,
      MutableDirectBuffer eventDst,
      int eventDstOffset,
      long sequenceNumber,
      long clusterTimestampNanos);
}
