package com.trading.engine.cluster.refdata;

import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * Handles a batch SBE command templateId (e.g., LoadAccountBatch = 12, LoadCurrencyBatch = 14,
 * LoadRiskLimitBatch = 16). Iterates the command's repeating group, applies each record through the
 * same internal upsert path as the corresponding single-record {@link ReferenceDataLoader}, and
 * emits one event per record (so projection consumers see exactly one event per upserted/rejected
 * record, regardless of whether the source command was single or batched).
 *
 * <p><b>Per-record events vs. summary event.</b> This interface emits one event per record. The
 * events are written contiguously into the caller-provided buffer; the return value is the total
 * bytes written. The caller (cluster's onSessionMessage) is responsible for publishing the event
 * stream slice to the egress / event publication channel. The reason for per-record (vs. a single
 * batch summary) is replay symmetry — projections consume the event stream and shouldn't have to
 * know whether an account was loaded via a single or batch command.
 *
 * <p><b>Atomicity.</b> The batch is NOT all-or-nothing — each record is validated and upserted
 * independently. A bad record in the middle of a batch produces a LoadRejected event for that
 * record and the batch continues. This matches exchange-core's start-of-day load semantics.
 *
 * <p><b>Zero allocation.</b> Same hot-path discipline as {@link ReferenceDataLoader}.
 */
public interface ReferenceDataBatchLoader {

  /** SBE templateId of the batch command this loader handles. */
  int batchCommandTemplateId();

  /**
   * Iterate the batch's repeating group and process each record. For each record, write a Loaded /
   * LoadRejected event into {@code eventDst}. Returns the total bytes of all emitted events.
   *
   * @return total bytes of all events written, or 0 if the batch was empty
   */
  int onBatchCommand(
      MessageHeaderDecoder header,
      DirectBuffer src,
      int srcOffset,
      int srcLength,
      MutableDirectBuffer eventDst,
      int eventDstOffset,
      long firstSequenceNumber,
      long clusterTimestampNanos);
}
