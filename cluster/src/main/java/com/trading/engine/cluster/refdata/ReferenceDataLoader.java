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
 * <p><b>Output convention.</b> {@link #onCommand} writes one or more complete SBE event messages
 * (8-byte header + body each) into {@code eventDst} starting at {@code eventDstOffset} and returns
 * the total bytes written across all emitted events. Most loaders emit a single event (a Loaded or
 * LoadRejected outcome distinguished by templateId), but APP-62 §D {@code LoadRiskLimitHandler}
 * emits two events back-to-back (LoadedEvent followed by ChangedEvent). The cluster (see {@code
 * TradingClusteredService#walkAndDispatchRefDataEvents}) walks each emitted SBE header, assigns a
 * fresh authoritative sequence number from {@code EventSequencer} per event, rewrites the {@code
 * sequenceNumber} field in-place, and journals + offers each event independently — so a handler's
 * stamp on {@code sequenceNumber} is best-effort and gets overwritten. Returning 0 means "no event
 * was written" (reserved for unexpected shapes; loaders should always emit an event in normal
 * operation).
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
