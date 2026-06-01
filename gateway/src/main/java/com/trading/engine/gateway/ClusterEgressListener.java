package com.trading.engine.gateway;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.trading.engine.messages.sbe.ExecutionReportDecoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.OrderCancelRejectDecoder;
import com.trading.engine.messages.sbe.OrderCanceledEventDecoder;
import com.trading.engine.messages.sbe.OrderCreatedEventDecoder;
import com.trading.engine.messages.sbe.OrderExpiredEventDecoder;
import com.trading.engine.messages.sbe.OrderRejectedEventDecoder;
import com.trading.engine.messages.sbe.QuoteDecoder;
import io.aeron.cluster.client.ControlledEgressListener;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

/**
 * Controlled egress listener for cluster responses. Implements the full decode → correlate →
 * dispatch pipeline:
 *
 * <ol>
 *   <li>Decode the SBE message header to determine the templateId.
 *   <li>Wrap the appropriate SBE decoder over the message body.
 *   <li>Extract the correlation key (ClOrdID for orders, QuoteReqID for quotes).
 *   <li>Look up the originating FIX session via {@link SessionLookup}.
 *   <li>Invoke the {@link EgressCallback} to deliver the response to the FIX session.
 * </ol>
 *
 * <p><b>Flow control.</b> If the FIX session cannot accept the message (back-pressured), the
 * callback returns {@code false} and this listener returns {@link Action#ABORT}, causing Aeron to
 * re-deliver the message on the next {@code controlledPollEgress()} call.
 *
 * <p><b>Allocation.</b> Zero allocation after construction. All SBE decoders and scratch buffers
 * are pre-allocated at construction time and reused on every {@code onMessage} call.
 *
 * <p><b>Threading.</b> Not thread-safe — single-threaded gateway duty-cycle thread only.
 */
public final class ClusterEgressListener implements ControlledEgressListener {

  private static final Log LOG = LogFactory.getLog(ClusterEgressListener.class);

  // --- Pre-allocated SBE flyweights ---
  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final ExecutionReportDecoder erDecoder = new ExecutionReportDecoder();
  private final OrderCancelRejectDecoder cxlRejDecoder = new OrderCancelRejectDecoder();
  private final QuoteDecoder quoteDecoder = new QuoteDecoder();
  private final OrderCreatedEventDecoder orderCreatedDecoder = new OrderCreatedEventDecoder();
  private final OrderRejectedEventDecoder orderRejectedDecoder = new OrderRejectedEventDecoder();
  private final OrderCanceledEventDecoder orderCanceledDecoder = new OrderCanceledEventDecoder();
  private final OrderExpiredEventDecoder orderExpiredDecoder = new OrderExpiredEventDecoder();

  // --- Scratch buffers for correlation key extraction (zero-alloc) ---
  private final byte[] clOrdIdScratch = new byte[ExecutionReportDecoder.clOrdIdLength()];
  private final byte[] cxlClOrdIdScratch = new byte[OrderCancelRejectDecoder.clOrdIdLength()];
  private final byte[] quoteReqIdScratch = new byte[QuoteDecoder.quoteReqIdLength()];
  private final byte[] ocClOrdIdScratch = new byte[OrderCreatedEventDecoder.clOrdIdLength()];
  private final byte[] orClOrdIdScratch = new byte[OrderRejectedEventDecoder.clOrdIdLength()];
  private final byte[] oxlClOrdIdScratch = new byte[OrderCanceledEventDecoder.clOrdIdLength()];
  private final byte[] oxpClOrdIdScratch = new byte[OrderExpiredEventDecoder.clOrdIdLength()];

  // --- Collaborators (injected, not owned) ---
  private final SbeToFixTranslator translator;
  private final SessionLookup sessionLookup;
  private final InFlightTracker inFlightTracker;
  private final EgressCallback callback;

  // --- Last-used correlation key (set by handleXxx, read by egress callback for cleanup) ---
  private byte[] lastCorrelationScratch;
  private int lastCorrelationLen;

  // --- Reconnection signal (read by ClusterClient on the same duty-cycle thread) ---
  private boolean reconnectNeeded;

  /**
   * Callback invoked by this listener to deliver a decoded cluster response to the appropriate FIX
   * session. The listener has already wrapped the SBE decoder over the message body; the callback
   * should read the decoded fields via this listener's accessor methods (e.g., {@link
   * #executionReportDecoder()}).
   */
  @FunctionalInterface
  public interface EgressCallback {

    /**
     * Deliver a cluster response to the FIX session identified by {@code sessionKey}.
     *
     * @param sessionKey gateway session key from {@link SessionLookup}
     * @param templateId SBE templateId of the decoded message
     * @param timestamp cluster timestamp from the egress message
     * @return {@code true} if delivered successfully, {@code false} if the session is
     *     back-pressured (causes {@link Action#ABORT} and re-delivery)
     */
    boolean onEgressMessage(long sessionKey, int templateId, long timestamp);
  }

  /**
   * @param translator SBE-to-FIX translator (one per duty-cycle thread, not shared)
   * @param sessionLookup correlates ClOrdID/QuoteReqID → FIX session key
   * @param inFlightTracker tracks pending requests for timeout detection
   * @param callback delivers decoded responses to the FIX session
   */
  public ClusterEgressListener(
      final SbeToFixTranslator translator,
      final SessionLookup sessionLookup,
      final InFlightTracker inFlightTracker,
      final EgressCallback callback) {
    if (translator == null) {
      throw new NullPointerException("translator");
    }
    if (sessionLookup == null) {
      throw new NullPointerException("sessionLookup");
    }
    if (inFlightTracker == null) {
      throw new NullPointerException("inFlightTracker");
    }
    if (callback == null) {
      throw new NullPointerException("callback");
    }
    this.translator = translator;
    this.sessionLookup = sessionLookup;
    this.inFlightTracker = inFlightTracker;
    this.callback = callback;
  }

  // ===========================================================================
  // ControlledEgressListener
  // ===========================================================================

  @Override
  public Action onMessage(
      final long clusterSessionId,
      final long timestamp,
      final DirectBuffer buffer,
      final int offset,
      final int length,
      final Header header) {

    headerDecoder.wrap(buffer, offset);
    final int templateId = headerDecoder.templateId();

    return switch (templateId) {
      case ExecutionReportDecoder.TEMPLATE_ID -> handleExecutionReport(buffer, offset, timestamp);
      case OrderCancelRejectDecoder.TEMPLATE_ID ->
          handleOrderCancelReject(buffer, offset, timestamp);
      case QuoteDecoder.TEMPLATE_ID -> handleQuote(buffer, offset, timestamp);
      case OrderCreatedEventDecoder.TEMPLATE_ID -> handleOrderCreated(buffer, offset, timestamp);
      case OrderRejectedEventDecoder.TEMPLATE_ID -> handleOrderRejected(buffer, offset, timestamp);
      case OrderCanceledEventDecoder.TEMPLATE_ID -> handleOrderCanceled(buffer, offset, timestamp);
      case OrderExpiredEventDecoder.TEMPLATE_ID -> handleOrderExpired(buffer, offset, timestamp);
      // QuoteRequestReject (templateId=3) is NOT handled here — it comes from the orchestrator
      // via stream 101 (OrchestratorResponseListener), not from the cluster egress.
      default -> {
        LOG.warn().append("Unhandled egress templateId=").append(templateId).commit();
        yield Action.CONTINUE;
      }
    };
  }

  @Override
  public void onSessionEvent(
      final long correlationId,
      final long clusterSessionId,
      final long leadershipTermId,
      final int leaderMemberId,
      final EventCode code,
      final String detail) {
    LOG.info()
        .append("Cluster session event: code=")
        .append(code.name())
        .append(" detail=")
        .append(detail)
        .append(" leader=")
        .append(leaderMemberId)
        .commit();

    if (code == EventCode.ERROR
        || code == EventCode.CLOSED
        || code == EventCode.AUTHENTICATION_REJECTED) {
      reconnectNeeded = true;
    }
  }

  @Override
  public void onNewLeader(
      final long clusterSessionId,
      final long leadershipTermId,
      final int leaderMemberId,
      final String ingressEndpoints) {
    LOG.info()
        .append("Cluster leader change: leader=")
        .append(leaderMemberId)
        .append(" term=")
        .append(leadershipTermId)
        .commit();
  }

  // ===========================================================================
  // Message handlers
  // ===========================================================================

  private Action handleExecutionReport(
      final DirectBuffer buffer, final int offset, final long timestamp) {
    erDecoder.wrap(
        buffer,
        offset + MessageHeaderDecoder.ENCODED_LENGTH,
        headerDecoder.blockLength(),
        headerDecoder.version());
    erDecoder.getClOrdId(clOrdIdScratch, 0);
    final int clOrdIdLen = trimNullPadding(clOrdIdScratch);

    lastCorrelationScratch = clOrdIdScratch;
    lastCorrelationLen = clOrdIdLen;

    final long sessionKey = sessionLookup.findByCorrelationId(clOrdIdScratch, 0, clOrdIdLen);
    if (sessionKey == SessionLookup.NULL_SESSION) {
      // Cluster responded but the FIX session disconnected — clear the in-flight entry
      // (no timeout needed) and move on.
      inFlightTracker.onResponseReceived(clOrdIdScratch, 0, clOrdIdLen);
      LOG.info().append("Orphaned ExecutionReport: clOrdIdLen=").append(clOrdIdLen).commit();
      return Action.CONTINUE;
    }

    final boolean delivered =
        callback.onEgressMessage(sessionKey, ExecutionReportDecoder.TEMPLATE_ID, timestamp);
    if (delivered) {
      // Clear in-flight only after successful delivery — on ABORT (backpressure), the message
      // will be re-delivered and the entry remains tracked for timeout protection.
      inFlightTracker.onResponseReceived(clOrdIdScratch, 0, clOrdIdLen);
      return Action.CONTINUE;
    }
    return Action.ABORT;
  }

  private Action handleOrderCancelReject(
      final DirectBuffer buffer, final int offset, final long timestamp) {
    cxlRejDecoder.wrap(
        buffer,
        offset + MessageHeaderDecoder.ENCODED_LENGTH,
        headerDecoder.blockLength(),
        headerDecoder.version());
    cxlRejDecoder.getClOrdId(cxlClOrdIdScratch, 0);
    final int clOrdIdLen = trimNullPadding(cxlClOrdIdScratch);
    lastCorrelationScratch = cxlClOrdIdScratch;
    lastCorrelationLen = clOrdIdLen;

    final long sessionKey = sessionLookup.findByCorrelationId(cxlClOrdIdScratch, 0, clOrdIdLen);
    if (sessionKey == SessionLookup.NULL_SESSION) {
      inFlightTracker.onResponseReceived(cxlClOrdIdScratch, 0, clOrdIdLen);
      LOG.info().append("Orphaned OrderCancelReject: clOrdIdLen=").append(clOrdIdLen).commit();
      return Action.CONTINUE;
    }

    final boolean delivered =
        callback.onEgressMessage(sessionKey, OrderCancelRejectDecoder.TEMPLATE_ID, timestamp);
    if (delivered) {
      inFlightTracker.onResponseReceived(cxlClOrdIdScratch, 0, clOrdIdLen);
      return Action.CONTINUE;
    }
    return Action.ABORT;
  }

  private Action handleQuote(final DirectBuffer buffer, final int offset, final long timestamp) {
    quoteDecoder.wrap(
        buffer,
        offset + MessageHeaderDecoder.ENCODED_LENGTH,
        headerDecoder.blockLength(),
        headerDecoder.version());
    quoteDecoder.getQuoteReqId(quoteReqIdScratch, 0);
    final int quoteReqIdLen = trimNullPadding(quoteReqIdScratch);
    lastCorrelationScratch = quoteReqIdScratch;
    lastCorrelationLen = quoteReqIdLen;

    // Quotes are correlated by QuoteReqID, not ClOrdID. Use the same session lookup
    // because the gateway registers both ClOrdID and QuoteReqID in the same map.
    final long sessionKey = sessionLookup.findByCorrelationId(quoteReqIdScratch, 0, quoteReqIdLen);
    if (sessionKey == SessionLookup.NULL_SESSION) {
      inFlightTracker.onResponseReceived(quoteReqIdScratch, 0, quoteReqIdLen);
      LOG.info().append("Orphaned Quote: quoteReqIdLen=").append(quoteReqIdLen).commit();
      return Action.CONTINUE;
    }

    final boolean delivered =
        callback.onEgressMessage(sessionKey, QuoteDecoder.TEMPLATE_ID, timestamp);
    if (delivered) {
      inFlightTracker.onResponseReceived(quoteReqIdScratch, 0, quoteReqIdLen);
      return Action.CONTINUE;
    }
    return Action.ABORT;
  }

  private Action handleOrderCreated(
      final DirectBuffer buffer, final int offset, final long timestamp) {
    orderCreatedDecoder.wrap(
        buffer,
        offset + MessageHeaderDecoder.ENCODED_LENGTH,
        headerDecoder.blockLength(),
        headerDecoder.version());
    orderCreatedDecoder.getClOrdId(ocClOrdIdScratch, 0);
    final int clOrdIdLen = trimNullPadding(ocClOrdIdScratch);

    lastCorrelationScratch = ocClOrdIdScratch;
    lastCorrelationLen = clOrdIdLen;

    final long sessionKey = sessionLookup.findByCorrelationId(ocClOrdIdScratch, 0, clOrdIdLen);
    if (sessionKey == SessionLookup.NULL_SESSION) {
      inFlightTracker.onResponseReceived(ocClOrdIdScratch, 0, clOrdIdLen);
      LOG.info().append("Orphaned OrderCreatedEvent: clOrdIdLen=").append(clOrdIdLen).commit();
      return Action.CONTINUE;
    }

    final boolean delivered =
        callback.onEgressMessage(sessionKey, OrderCreatedEventDecoder.TEMPLATE_ID, timestamp);
    if (delivered) {
      inFlightTracker.onResponseReceived(ocClOrdIdScratch, 0, clOrdIdLen);
      return Action.CONTINUE;
    }
    return Action.ABORT;
  }

  private Action handleOrderRejected(
      final DirectBuffer buffer, final int offset, final long timestamp) {
    orderRejectedDecoder.wrap(
        buffer,
        offset + MessageHeaderDecoder.ENCODED_LENGTH,
        headerDecoder.blockLength(),
        headerDecoder.version());
    orderRejectedDecoder.getClOrdId(orClOrdIdScratch, 0);
    final int clOrdIdLen = trimNullPadding(orClOrdIdScratch);

    lastCorrelationScratch = orClOrdIdScratch;
    lastCorrelationLen = clOrdIdLen;

    final long sessionKey = sessionLookup.findByCorrelationId(orClOrdIdScratch, 0, clOrdIdLen);
    if (sessionKey == SessionLookup.NULL_SESSION) {
      inFlightTracker.onResponseReceived(orClOrdIdScratch, 0, clOrdIdLen);
      LOG.info().append("Orphaned OrderRejectedEvent: clOrdIdLen=").append(clOrdIdLen).commit();
      return Action.CONTINUE;
    }

    final boolean delivered =
        callback.onEgressMessage(sessionKey, OrderRejectedEventDecoder.TEMPLATE_ID, timestamp);
    if (delivered) {
      inFlightTracker.onResponseReceived(orClOrdIdScratch, 0, clOrdIdLen);
      return Action.CONTINUE;
    }
    return Action.ABORT;
  }

  /**
   * Handles a cluster-emitted {@code OrderCanceledEvent} (SBE template 103). Correlation by {@code
   * clOrdId} mirrors {@link #handleOrderRejected}. APP-151 phase 2 — the cluster's session-close
   * orphan-cancel path (APP-151 phase 1) emits these, and this handler routes them through to the
   * counterparty FIX session as an ExecutionReport with ExecType=Canceled.
   *
   * <p>If the FIX session has already disconnected by the time the cancel arrives (the common case,
   * since the cluster emits the cancel BECAUSE the session disconnected), the in-flight tracker is
   * drained and the event is dropped. The cancel is durably journaled in the cluster log
   * regardless; the gateway is purely the egress hop. When Artio re-establishes the session, any
   * pending offline-queued events are delivered via Artio's standard replay flow.
   */
  private Action handleOrderCanceled(
      final DirectBuffer buffer, final int offset, final long timestamp) {
    orderCanceledDecoder.wrap(
        buffer,
        offset + MessageHeaderDecoder.ENCODED_LENGTH,
        headerDecoder.blockLength(),
        headerDecoder.version());
    orderCanceledDecoder.getClOrdId(oxlClOrdIdScratch, 0);
    final int clOrdIdLen = trimNullPadding(oxlClOrdIdScratch);

    lastCorrelationScratch = oxlClOrdIdScratch;
    lastCorrelationLen = clOrdIdLen;

    final long sessionKey = sessionLookup.findByCorrelationId(oxlClOrdIdScratch, 0, clOrdIdLen);
    if (sessionKey == SessionLookup.NULL_SESSION) {
      inFlightTracker.onResponseReceived(oxlClOrdIdScratch, 0, clOrdIdLen);
      LOG.info().append("Orphaned OrderCanceledEvent: clOrdIdLen=").append(clOrdIdLen).commit();
      return Action.CONTINUE;
    }

    final boolean delivered =
        callback.onEgressMessage(sessionKey, OrderCanceledEventDecoder.TEMPLATE_ID, timestamp);
    if (delivered) {
      inFlightTracker.onResponseReceived(oxlClOrdIdScratch, 0, clOrdIdLen);
      return Action.CONTINUE;
    }
    return Action.ABORT;
  }

  /**
   * Handles a cluster-emitted {@code OrderExpiredEvent} (SBE template 121, APP-62 §J). Correlation
   * by {@code clOrdId} mirrors {@link #handleOrderCanceled}. The cluster's idle-timeout reaper (and
   * future TIF-driven expiries) emits these so the gateway can translate to FIX 4.4 ExecutionReport
   * with ExecType=Expired (tag 150='C') — semantically distinct from ExecType=Canceled ('4'), which
   * is what {@link #handleOrderCanceled} handles.
   *
   * <p>If the FIX session has already disconnected by the time the expire arrives (common for the
   * idle-timeout case — the session is precisely the one that went quiet), the in-flight tracker is
   * drained and the event is dropped. The expire is durably journaled in the cluster log
   * regardless; the gateway is purely the egress hop.
   *
   * <p>Uses a dedicated {@code oxpClOrdIdScratch} buffer (separate from {@code oxlClOrdIdScratch})
   * so the gateway's correlation-extraction scratch invariants remain honoured across interleaved
   * cancel and expire deliveries — see {@code lastCorrelationScratch} javadoc.
   *
   * @param buffer egress message buffer positioned at the SBE header offset
   * @param offset byte offset of the SBE header within {@code buffer}
   * @param timestamp cluster timestamp from the egress message header
   * @return {@link Action#CONTINUE} on successful delivery / orphan drop; {@link Action#ABORT} on
   *     FIX session back-pressure (triggers re-delivery on the next poll)
   */
  private Action handleOrderExpired(
      final DirectBuffer buffer, final int offset, final long timestamp) {
    orderExpiredDecoder.wrap(
        buffer,
        offset + MessageHeaderDecoder.ENCODED_LENGTH,
        headerDecoder.blockLength(),
        headerDecoder.version());
    orderExpiredDecoder.getClOrdId(oxpClOrdIdScratch, 0);
    // Primitive locals bare (no `final`) per memory rule feedback_final_primitives_autoboxing.md.
    int clOrdIdLen = trimNullPadding(oxpClOrdIdScratch);

    lastCorrelationScratch = oxpClOrdIdScratch;
    lastCorrelationLen = clOrdIdLen;

    final long sessionKey = sessionLookup.findByCorrelationId(oxpClOrdIdScratch, 0, clOrdIdLen);
    if (sessionKey == SessionLookup.NULL_SESSION) {
      inFlightTracker.onResponseReceived(oxpClOrdIdScratch, 0, clOrdIdLen);
      LOG.info().append("Orphaned OrderExpiredEvent: clOrdIdLen=").append(clOrdIdLen).commit();
      return Action.CONTINUE;
    }

    boolean delivered =
        callback.onEgressMessage(sessionKey, OrderExpiredEventDecoder.TEMPLATE_ID, timestamp);
    if (delivered) {
      inFlightTracker.onResponseReceived(oxpClOrdIdScratch, 0, clOrdIdLen);
      return Action.CONTINUE;
    }
    return Action.ABORT;
  }

  // ===========================================================================
  // Accessors — callback reads pre-wrapped decoders without re-decoding
  // ===========================================================================

  /** Returns the pre-wrapped header decoder, positioned over the last received message. */
  public MessageHeaderDecoder headerDecoder() {
    return headerDecoder;
  }

  /**
   * Returns the pre-wrapped ExecutionReport decoder. Valid only when the last {@code onMessage}
   * invocation handled templateId {@link ExecutionReportDecoder#TEMPLATE_ID}.
   */
  public ExecutionReportDecoder executionReportDecoder() {
    return erDecoder;
  }

  /**
   * Returns the pre-wrapped OrderCancelReject decoder. Valid only when the last {@code onMessage}
   * invocation handled templateId {@link OrderCancelRejectDecoder#TEMPLATE_ID}.
   */
  public OrderCancelRejectDecoder orderCancelRejectDecoder() {
    return cxlRejDecoder;
  }

  /**
   * Returns the pre-wrapped Quote decoder. Valid only when the last {@code onMessage} invocation
   * handled templateId {@link QuoteDecoder#TEMPLATE_ID}.
   */
  public QuoteDecoder quoteDecoder() {
    return quoteDecoder;
  }

  /**
   * Returns the pre-wrapped OrderCreatedEvent decoder. Valid only when the last {@code onMessage}
   * invocation handled templateId {@link OrderCreatedEventDecoder#TEMPLATE_ID}.
   */
  public OrderCreatedEventDecoder orderCreatedDecoder() {
    return orderCreatedDecoder;
  }

  /**
   * Returns the pre-wrapped OrderRejectedEvent decoder. Valid only when the last {@code onMessage}
   * invocation handled templateId {@link OrderRejectedEventDecoder#TEMPLATE_ID}.
   */
  public OrderRejectedEventDecoder orderRejectedDecoder() {
    return orderRejectedDecoder;
  }

  /**
   * Returns the pre-wrapped OrderCanceledEvent decoder. Valid only when the last {@code onMessage}
   * invocation handled templateId {@link OrderCanceledEventDecoder#TEMPLATE_ID}. APP-151 phase 2.
   *
   * @return the pre-wrapped {@link OrderCanceledEventDecoder} positioned over the last received
   *     cancel-event message body — never null; reading fields outside the templateId-103
   *     invocation window returns stale bytes from a previous message
   */
  public OrderCanceledEventDecoder orderCanceledDecoder() {
    return orderCanceledDecoder;
  }

  /**
   * Returns the pre-wrapped OrderExpiredEvent decoder. Valid only when the last {@code onMessage}
   * invocation handled templateId {@link OrderExpiredEventDecoder#TEMPLATE_ID}. APP-62 §J.
   *
   * @return the pre-wrapped {@link OrderExpiredEventDecoder} positioned over the last received
   *     expire-event message body — never null; reading fields outside the templateId-121
   *     invocation window returns stale bytes from a previous message
   */
  public OrderExpiredEventDecoder orderExpiredDecoder() {
    return orderExpiredDecoder;
  }

  /** Returns the SBE-to-FIX translator for use by the callback. */
  public SbeToFixTranslator translator() {
    return translator;
  }

  /**
   * Returns the scratch buffer containing the last-extracted correlation ID bytes. Valid only
   * within the scope of an {@link EgressCallback#onEgressMessage} invocation. Used by the callback
   * to clean up the correlation entry in {@link SessionRegistry} after successful delivery.
   */
  public byte[] lastCorrelationScratch() {
    return lastCorrelationScratch;
  }

  /** Returns the significant byte length of the last-extracted correlation ID. */
  public int lastCorrelationLen() {
    return lastCorrelationLen;
  }

  /**
   * Returns {@code true} if a cluster session error or close was received, signalling the client
   * should reconnect. Reset by calling {@link #clearReconnectNeeded()}.
   */
  public boolean isReconnectNeeded() {
    return reconnectNeeded;
  }

  /** Clear the reconnect signal after the client has initiated reconnection. */
  public void clearReconnectNeeded() {
    reconnectNeeded = false;
  }

  // ===========================================================================
  // Internal helpers
  // ===========================================================================

  /**
   * Returns the number of significant bytes in a null-padded fixed-length SBE char field, i.e., the
   * index of the first {@code \0} byte, or the full array length if no null is found.
   */
  static int trimNullPadding(final byte[] buf) {
    for (int i = 0; i < buf.length; i++) {
      if (buf[i] == 0) {
        return i;
      }
    }
    return buf.length;
  }
}
