package com.trading.engine.projections;

import org.agrona.DirectBuffer;

/**
 * A CQRS read-model projection. Consumes domain events from the cluster's event stream and builds a
 * query-optimized read model (e.g. an orders view, positions view, quotes view).
 *
 * <p><b>No snapshots.</b> Projections do not implement any snapshot / restore methods. They recover
 * by replaying every event from Aeron Archive position 0 via {@link #reset()} followed by a full
 * archive replay. The trade-off is slower startup in exchange for architectural simplicity — a
 * projection is always a pure function of the immutable event log, no bifurcated "snapshot
 * consistency vs. replay" recovery paths. This matches the default behaviour of Axon Framework (via
 * {@code TrackingEventProcessor.resetTokens()}), EventStoreDB's projection management API, and
 * Lagom read-side processors. Only frameworks with very large state stores (Kafka Streams, ksqlDB)
 * consistently snapshot read-side state; CQRS-ES frameworks do not.
 *
 * <p><b>The Aeron Archive log is never truncated.</b> Projections depend on full replay being
 * available for the lifetime of the system. See the project {@code CLAUDE.md} for the rule.
 *
 * <p><b>Threading:</b> implementations are invoked single-threaded from the {@link EventConsumer}
 * poll loop. No synchronization is required inside projection methods, and implementations MUST NOT
 * block (no I/O on the dispatch thread, no lock acquisition, no allocation) — a slow projection
 * back-pressures the entire read-side consumer, and under the wrong wiring could even back-pressure
 * the cluster itself.
 *
 * <p><b>Byte-oriented contract:</b> the event payload is delivered as raw bytes in a {@link
 * DirectBuffer} slice, not as a decoded POJO. Implementations wrap their own SBE flyweight decoder
 * (from the {@code messages} module) over the slice and read fields directly. This matches
 * EventStoreDB's raw subscription API and Aeron Cluster's {@code ClusteredService.onSessionMessage}
 * contract, and is the only shape compatible with the trading engine's zero-allocation hot-path
 * discipline.
 */
public interface Projection {

  /**
   * Consume one event from the cluster's event stream.
   *
   * @param seqNo the event's sequence number as seen by the {@link EventConsumer}. During Wave 3
   *     scaffolding this is the consumer's ingress counter (messages dispatched since consumer
   *     start); APP-8 (Wave 4) will replace it with the authoritative sequence number extracted
   *     from the event payload once the cluster → projections event format is finalized
   * @param eventType raw {@code EventTypeEnum} wire value from the SBE schema (e.g. 100 for {@code
   *     OrderCreatedEvent}); consumers can match against {@code
   *     com.trading.engine.messages.sbe.EventTypeEnum} constants if they need typed access
   * @param buffer the consumer's view of the event payload — read-only, do NOT retain past the
   *     call. Copy any bytes the projection needs to keep.
   * @param offset start offset of the event bytes inside {@code buffer}
   * @param length number of event bytes starting at {@code offset}
   */
  void onEvent(
      final long seqNo,
      final int eventType,
      final DirectBuffer buffer,
      final int offset,
      final int length);

  /**
   * The latest {@code seqNo} this projection has finished processing, or {@code 0} if no event has
   * been consumed (or the projection has just been {@link #reset()}). Implementations MUST update
   * this on every successful {@link #onEvent} call.
   *
   * <p>Used by {@link ProjectionRegistry#getLag()} and {@link ProjectionRegistry#isHealthy()} for
   * diagnostics. Not a durable checkpoint — projections recover via full replay, not resumption
   * from this value.
   */
  long lastProcessedSequence();

  /**
   * Clear all in-memory read-model state back to the empty initial state, and reset {@link
   * #lastProcessedSequence()} to {@code 0}. Called at the start of a full archive replay, before
   * the first {@link #onEvent} of the replay. Implementations MUST be idempotent and MUST NOT
   * retain any state (cached lookups, counters, derived tables) from before the reset.
   */
  void reset();
}
