package com.trading.engine.gateway;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.trading.engine.messages.sbe.ExecutionReportDecoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.OrderCancelRejectDecoder;
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

  // --- Scratch buffers for correlation key extraction (zero-alloc) ---
  private final byte[] clOrdIdScratch = new byte[ExecutionReportDecoder.clOrdIdLength()];
  private final byte[] cxlClOrdIdScratch = new byte[OrderCancelRejectDecoder.clOrdIdLength()];
  private final byte[] quoteReqIdScratch = new byte[QuoteDecoder.quoteReqIdLength()];

  // --- Collaborators (injected, not owned) ---
  private final SbeToFixTranslator translator;
  private final SessionLookup sessionLookup;
  private final InFlightTracker inFlightTracker;
  private final EgressCallback callback;

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

  /** Returns the SBE-to-FIX translator for use by the callback. */
  public SbeToFixTranslator translator() {
    return translator;
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
