package com.trading.engine.cluster.handler;

import static io.aeron.Publication.ADMIN_ACTION;
import static io.aeron.Publication.BACK_PRESSURED;
import static io.aeron.cluster.service.ClientSession.MOCKED_OFFER;

import com.trading.engine.cluster.journal.EventJournal;
import com.trading.engine.cluster.sequencer.EventSequencer;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.function.Consumer;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * Centralizes the domain event emission pipeline: sequence number stamping, cluster timestamp
 * stamping, journal append, and session offer with backpressure retry.
 *
 * <p><b>Convention:</b> all domain events (template IDs 100-116) carry {@code sequenceNumber} at
 * body offset 0 and {@code timestamp} at body offset 8. This is verified by compile-time assertions
 * in the test suite (see {@code EventSinkTest.eventEncoderOffsetConventionHolds}). EventSink stamps
 * both fields at fixed offsets ({@code MessageHeaderEncoder.ENCODED_LENGTH + 0} and {@code + 8})
 * using raw {@code putLong}, avoiding SBE encoder dependency.
 *
 * <p><b>Threading:</b> single-threaded cluster duty cycle. No synchronization required.
 *
 * <p><b>Allocation:</b> zero allocation. Uses a dedicated {@link MessageHeaderDecoder} for journal
 * template ID extraction (avoids clobbering the service's dispatch decoder).
 *
 * <p><b>Error handling:</b> journal failure (monotonicity violation) is fatal — the exception
 * propagates to the cluster duty cycle, triggering Aeron failover. This is the correct behavior for
 * a deterministic state machine. Session offer failure after retry exhaustion closes the session,
 * forcing the client to reconnect and resync from the journal.
 *
 * @see CommandHandler
 * @see EventSequencer
 * @see EventJournal
 */
public final class EventSink {

  private static final int HDR_LEN = MessageHeaderEncoder.ENCODED_LENGTH;
  private static final ByteOrder BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;
  private static final int MAX_BACKPRESSURE_RETRY = 128;

  private final EventSequencer sequencer;
  private final EventJournal journal;

  /** Dedicated header decoder for reading templateId during journal append. */
  private final MessageHeaderDecoder journalHeaderDecoder = new MessageHeaderDecoder();

  private Cluster cluster;

  /**
   * Pre-allocated, mutable broadcast context. Set on entry to {@link #emit} and consumed by {@link
   * #broadcastConsumer} inside the {@link Cluster#forEachClientSession} call below.
   *
   * <p><b>Aeron contract this design depends on (single-line CONTRACT, must hold):</b> {@link
   * Cluster#forEachClientSession(Consumer)} invokes the consumer SYNCHRONOUSLY on the duty-cycle
   * thread for every active session BEFORE returning. This is the documented Aeron 1.50.x behaviour
   * — the API is named "forEach" precisely because it is a synchronous fold. If a future Aeron
   * version changes this to a deferred / async iteration, this design SILENTLY BREAKS (the consumer
   * would read the wrong buffer).
   *
   * <p><b>Regression guard:</b> {@code EventSinkBroadcastTest} asserts that all sessions receive
   * the per-emit buffer bytes WITHIN the {@code emit()} call return — not after. Any future Aeron
   * upgrade that defers iteration will fail this test loudly.
   *
   * <p>Single-writer (cluster duty cycle) ⇒ no synchronisation required between the field writes
   * below and the field reads inside {@link #broadcastConsumer}.
   */
  private MutableDirectBuffer broadcastBuffer;

  private int broadcastOffset;
  private int broadcastLength;

  /**
   * Final-field {@link Consumer} bound once at construction so {@link #emit} can call {@link
   * Cluster#forEachClientSession(Consumer)} without allocating a new SAM per call. The consumer
   * reads {@link #broadcastBuffer}/{@link #broadcastOffset}/{@link #broadcastLength} which the
   * caller stamps on every {@code emit} invocation under the synchronous-iteration contract
   * documented above.
   */
  private final Consumer<ClientSession> broadcastConsumer =
      session -> offerToSession(session, broadcastBuffer, broadcastOffset, broadcastLength);

  /**
   * Creates an EventSink wired to the given sequencer and journal.
   *
   * @param sequencer the monotonic event sequence number generator (must not be null)
   * @param journal the bounded event journal for projection catch-up (must not be null)
   */
  public EventSink(final EventSequencer sequencer, final EventJournal journal) {
    this.sequencer = Objects.requireNonNull(sequencer, "sequencer");
    this.journal = Objects.requireNonNull(journal, "journal");
  }

  /**
   * Sets the cluster reference for backpressure idle strategy. Called once from {@code
   * TradingClusteredService.onStart()}.
   *
   * @param cluster the Aeron cluster instance
   */
  public void setCluster(final Cluster cluster) {
    this.cluster = cluster;
  }

  /**
   * Emits a domain event: assigns a gapless sequence number, stamps the cluster timestamp, appends
   * to the journal, and broadcasts to every connected cluster client session with backpressure
   * retry.
   *
   * <p>The buffer must contain a fully encoded SBE message (header + body) with placeholder values
   * at the {@code sequenceNumber} (body offset 0) and {@code timestamp} (body offset 8) fields.
   * This method overwrites both fields with the authoritative values.
   *
   * <p><b>Broadcast invariant.</b> Every domain event reaches every connected cluster client
   * session; per-client filtering is the consumer's job (see {@link
   * io.aeron.cluster.service.Cluster#forEachClientSession(Consumer)} below for the iteration
   * contract). The originator-only branch was removed in Phase 3 Commit 2 — there is no fallback,
   * no kill switch, and no "skip if cluster unwired" path. {@link #setCluster(Cluster)} MUST be
   * called once during bootstrap before the first {@code emit()}; failure to wire the cluster
   * surfaces as an immediate {@link NullPointerException} at the field access below, which is the
   * correct failure mode (loud, not silent).
   *
   * <p><b>Why broadcast (rationale).</b>
   *
   * <ul>
   *   <li>The websocket-server and the gateway each open their own cluster session. Every domain
   *       event must reach every cluster client so per-client filters downstream can route them. A
   *       FIX-injected order's {@code OrderCreated} MUST surface in the browser's {@code
   *       OrderBlotter} via the websocket-server's session (which performs per-browser {@code
   *       SubscriptionFilter} routing); the gateway is not the only consumer.
   *   <li>The gateway is NOT at risk of re-publishing other clients' events as bogus
   *       ExecutionReports: it correlates inbound {@code OrderCreated} to the originating FIX
   *       session by {@code clOrdId} / {@code account}, and silently drops events whose {@code
   *       clOrdId} it never sent (verified by {@code GatewayClusterEgressListener}).
   *   <li>Cross-tenant data isolation is enforced downstream: browser ↔ ws-server via {@code
   *       SubscriptionFilter.matches(...)} per session, guarded by per-account {@code
   *       entitledSymbolsByAccount} (Phase 3 hardening); gateway ↔ FIX client via the {@code
   *       clOrdId}-keyed correlation table. The cluster's job is to journal + broadcast; routing is
   *       the consumer's job.
   *   <li>{@code CommandAck} (templateId 70) IS broadcast; the websocket-server's drain handler
   *       only forwards acks whose {@code clientCmdSeqNo} matches an in-flight slot for THAT
   *       browser session (per {@code commandClient}'s 1024-slot table). Acks for
   *       gateway-originated commands carry a different sequence range and are dropped client-side.
   * </ul>
   *
   * <p>Backpressure is handled per-session by {@link #offerToSession}; a slow client cannot starve
   * fast ones because each iteration retries independently.
   *
   * <p>Iteration uses {@link Cluster#forEachClientSession(Consumer)} (Aeron's zero-alloc
   * alternative to {@code clientSessions().iterator()}) and a final-field consumer bound at
   * construction. This satisfies the cluster-service no-heap-alloc invariant — the SAM is allocated
   * ONCE in the constructor; every emit call mutates the per-call broadcast context fields and
   * calls {@code forEachClientSession} with the same consumer reference. Single-writer (cluster
   * duty cycle) ensures the field reads inside the consumer see the writes from the line above
   * without any synchronisation.
   *
   * @param clusterTimestamp the cluster-assigned timestamp in epoch nanos
   * @param buffer the pre-encoded SBE message buffer (header + body)
   * @param offset the start offset of the SBE message header
   * @param length the total message length (header + body)
   * @return the assigned sequence number
   * @throws NullPointerException if {@link #setCluster(Cluster)} was not called before this method
   */
  public long emit(
      final long clusterTimestamp,
      final MutableDirectBuffer buffer,
      final int offset,
      final int length) {
    // 1. Assign gapless sequence number
    final long seqNo = sequencer.nextSequence();

    // 2. Stamp seqNo at body offset 0, timestamp at body offset 8
    buffer.putLong(offset + HDR_LEN, seqNo, BYTE_ORDER);
    buffer.putLong(offset + HDR_LEN + 8, clusterTimestamp, BYTE_ORDER);

    // 3. Read templateId for journal dispatch.
    // Header bytes [offset, offset+HDR_LEN) are not modified by the stamps above
    // (which target body offsets HDR_LEN+0 and HDR_LEN+8), so the wrap reads the original
    // templateId.
    journalHeaderDecoder.wrap(buffer, offset);
    final int templateId = journalHeaderDecoder.templateId();

    // 4. Append to journal (fatal on monotonicity violation — triggers Aeron failover)
    journal.append(seqNo, templateId, buffer, offset, length);

    // 5. Broadcast to ALL connected cluster client sessions. Unconditional — no fallback path.
    //    The NPE on `cluster` is the correct failure mode if setCluster() was not called.
    this.broadcastBuffer = buffer;
    this.broadcastOffset = offset;
    this.broadcastLength = length;
    cluster.forEachClientSession(broadcastConsumer);

    return seqNo;
  }

  /**
   * Returns the event sequencer for snapshot encode/decode and ref-data dispatch. The ref-data
   * dispatch path accesses the sequencer directly because {@link
   * com.trading.engine.cluster.refdata.ReferenceDataLoader} pre-stamps sequence numbers in its
   * event buffer (legacy pattern — see APP-176 for future unification).
   *
   * @return the event sequencer
   */
  public EventSequencer sequencer() {
    return sequencer;
  }

  /**
   * Offers a message to a client session with bounded-retry backpressure handling. Public so that
   * {@link com.trading.engine.cluster.TradingClusteredService} can call it for the ref-data
   * dispatch path (which journals events separately and then offers to session).
   *
   * <p>If retries exhaust or the session returns a non-retryable result (NOT_CONNECTED, CLOSED,
   * MAX_POSITION_EXCEEDED), the session is closed. The underlying domain event is already
   * journaled, so a client that does not see an ACK can reconnect and resync from the journal.
   *
   * @param session the client session (null is tolerated for test paths)
   * @param src the message buffer
   * @param offset the start offset
   * @param length the message length
   */
  public void offerToSession(
      final ClientSession session, final DirectBuffer src, final int offset, final int length) {
    if (session == null) {
      return; // Unit tests may pass null for the unused ref-data session case.
    }
    for (int attempt = 0; attempt < MAX_BACKPRESSURE_RETRY; attempt++) {
      final long result = session.offer(src, offset, length);
      if (result >= 0L || result == MOCKED_OFFER) {
        resetIdleStrategy();
        return;
      }
      if (result == BACK_PRESSURED || result == ADMIN_ACTION) {
        if (cluster != null) {
          cluster.idleStrategy().idle();
        }
        continue;
      }
      // Non-retryable — quarantine the session so the cluster framework tears it down.
      resetIdleStrategy();
      session.close();
      return;
    }
    // Retry exhausted on persistent BACK_PRESSURED / ADMIN_ACTION.
    resetIdleStrategy();
    session.close();
  }

  /** Reset the idle strategy after a retry loop to prevent elevated park time on the next call. */
  private void resetIdleStrategy() {
    if (cluster != null) {
      cluster.idleStrategy().reset();
    }
  }
}
