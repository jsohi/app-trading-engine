package com.trading.engine.gateway;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.trading.engine.messages.sbe.ExecutionReportDecoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.NewOrderSingleDecoder;
import com.trading.engine.messages.sbe.QuoteDecoder;
import com.trading.engine.messages.sbe.QuoteRequestRejectDecoder;
import io.aeron.Publication;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.NanoClock;

/**
 * Controlled fragment handler for Aeron IPC stream 101 (orchestrator → gateway responses).
 * Dispatches incoming SBE messages by templateId to the appropriate routing logic:
 *
 * <ul>
 *   <li>Quote (templateId=2): correlate by QuoteReqID → translate to FIX → send to client
 *   <li>QuoteRequestReject (templateId=3): correlate by QuoteReqID → translate to FIX → send
 *   <li>NewOrderSingle (templateId=4): extract ClOrdID → re-register correlation → forward to
 *       cluster via {@link ClusterClient#offerTracked}
 *   <li>ExecutionReport (templateId=5): correlate by ClOrdID → translate to FIX → send to client
 *       (orchestrator-sourced rejects, not cluster responses)
 * </ul>
 *
 * <p><b>Correlation lifecycle.</b> For Quote, QuoteRequestReject, and reject ExecutionReport, the
 * correlation entry is removed from {@link SessionRegistry} after successful delivery. For NOS
 * forwarding, the correlation is re-registered with a fresh timestamp (the original registration
 * may have been TTL-swept during a long RFQ lifecycle) and an {@link InFlightTracker} entry is
 * created for cluster timeout coverage.
 *
 * <p><b>Back-pressure.</b> Returns {@link Action#ABORT} when the FIX session's send buffer is full
 * or the cluster offer fails on transient back-pressure, causing Aeron to re-deliver the fragment
 * on the next poll.
 *
 * <p><b>Allocation.</b> Zero allocation after construction. All SBE decoders and scratch buffers
 * are pre-allocated at construction time and reused on every {@link #onFragment} call.
 *
 * <p><b>Threading.</b> Not thread-safe — single-threaded gateway duty-cycle thread only.
 *
 * @see ClusterEgressListener
 * @see ClusterClient
 */
public final class OrchestratorResponseListener implements ControlledFragmentHandler {

  private static final Log LOG = LogFactory.getLog(OrchestratorResponseListener.class);

  // --- Pre-allocated SBE flyweights ---
  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final QuoteDecoder quoteDecoder = new QuoteDecoder();
  private final QuoteRequestRejectDecoder qrrDecoder = new QuoteRequestRejectDecoder();
  private final ExecutionReportDecoder erDecoder = new ExecutionReportDecoder();
  private final NewOrderSingleDecoder nosDecoder = new NewOrderSingleDecoder();

  // --- Scratch buffers for correlation key extraction (zero-alloc) ---
  private final byte[] quoteReqIdScratch = new byte[QuoteDecoder.quoteReqIdLength()];
  private final byte[] qrrQuoteReqIdScratch =
      new byte[QuoteRequestRejectDecoder.quoteReqIdLength()];
  private final byte[] erClOrdIdScratch = new byte[ExecutionReportDecoder.clOrdIdLength()];
  private final byte[] nosClOrdIdScratch = new byte[NewOrderSingleDecoder.clOrdIdLength()];

  // --- Collaborators (injected, not owned) ---
  private final SessionLookup sessionLookup;
  private final SessionRegistry registry;
  private final ClusterClient clusterClient;
  private final ResponseCallback callback;
  private final NanoClock nanoClock;

  // --- Diagnostic counters ---
  private long quotesReceived;
  private long qrrReceived;
  private long nosForwardedToCluster;
  private long nosClusterOfferFailed;
  private long rejectErsReceived;
  private long backPressureAborts;
  private long sessionNotFound;

  /**
   * Callback invoked by this listener to deliver an orchestrator response to the appropriate FIX
   * session. The listener has already wrapped the SBE decoder over the message body; the callback
   * reads decoded fields via this listener's accessor methods.
   */
  @FunctionalInterface
  public interface ResponseCallback {

    /**
     * Deliver an orchestrator response to the FIX session identified by {@code sessionKey}.
     *
     * @param sessionKey gateway session key from {@link SessionLookup}
     * @param templateId SBE templateId of the decoded message
     * @param timestamp zero (orchestrator responses do not carry a cluster timestamp)
     * @return {@code true} if delivered successfully, {@code false} if the session is
     *     back-pressured (causes {@link Action#ABORT} and re-delivery)
     */
    boolean onOrchestratorResponse(long sessionKey, int templateId, long timestamp);
  }

  /**
   * Constructs the orchestrator response listener with all dependencies.
   *
   * @param sessionLookup correlates ClOrdID/QuoteReqID → FIX session key
   * @param registry session + correlation registry for removal and re-registration
   * @param clusterClient cluster client for NOS forwarding (stream 101 → cluster ingress)
   * @param nanoClock monotonic clock for correlation re-registration timestamps
   * @param callback delivers decoded responses to the FIX session
   */
  public OrchestratorResponseListener(
      final SessionLookup sessionLookup,
      final SessionRegistry registry,
      final ClusterClient clusterClient,
      final NanoClock nanoClock,
      final ResponseCallback callback) {
    if (sessionLookup == null) {
      throw new NullPointerException("sessionLookup");
    }
    if (registry == null) {
      throw new NullPointerException("registry");
    }
    if (clusterClient == null) {
      throw new NullPointerException("clusterClient");
    }
    if (nanoClock == null) {
      throw new NullPointerException("nanoClock");
    }
    if (callback == null) {
      throw new NullPointerException("callback");
    }
    this.sessionLookup = sessionLookup;
    this.registry = registry;
    this.clusterClient = clusterClient;
    this.nanoClock = nanoClock;
    this.callback = callback;
  }

  // ===========================================================================
  // ControlledFragmentHandler
  // ===========================================================================

  /**
   * Dispatches one inbound Aeron fragment from the orchestrator (stream 101) after SBE header
   * validation. Routes by templateId to the appropriate handler method.
   *
   * @param buffer inbound fragment buffer
   * @param offset fragment start offset within the buffer
   * @param length fragment length in bytes
   * @param header Aeron logbuffer header metadata
   * @return {@link Action#CONTINUE} when consumed, or {@link Action#ABORT} for back-pressure
   *     re-delivery
   */
  @Override
  public Action onFragment(
      final DirectBuffer buffer, final int offset, final int length, final Header header) {

    if (length < MessageHeaderDecoder.ENCODED_LENGTH) {
      LOG.warn()
          .append("Orchestrator fragment too short: length=")
          .append(length)
          .append(" required=")
          .append(MessageHeaderDecoder.ENCODED_LENGTH)
          .commit();
      return Action.CONTINUE;
    }

    headerDecoder.wrap(buffer, offset);
    final int templateId = headerDecoder.templateId();
    final int blockLength = headerDecoder.blockLength();
    final int version = headerDecoder.version();
    final int bodyOffset = offset + MessageHeaderDecoder.ENCODED_LENGTH;

    if (length < MessageHeaderDecoder.ENCODED_LENGTH + blockLength) {
      LOG.warn()
          .append("Orchestrator fragment body truncated: length=")
          .append(length)
          .append(" headerLen=")
          .append(MessageHeaderDecoder.ENCODED_LENGTH)
          .append(" blockLength=")
          .append(blockLength)
          .commit();
      return Action.CONTINUE;
    }

    return switch (templateId) {
      case QuoteDecoder.TEMPLATE_ID -> handleQuote(buffer, bodyOffset, blockLength, version);
      case QuoteRequestRejectDecoder.TEMPLATE_ID ->
          handleQuoteRequestReject(buffer, bodyOffset, blockLength, version);
      case NewOrderSingleDecoder.TEMPLATE_ID ->
          handleNosForward(buffer, offset, length, bodyOffset, blockLength, version);
      case ExecutionReportDecoder.TEMPLATE_ID ->
          handleRejectExecutionReport(buffer, bodyOffset, blockLength, version);
      default -> {
        LOG.warn().append("Unknown orchestrator templateId=").append(templateId).commit();
        yield Action.CONTINUE;
      }
    };
  }

  // ===========================================================================
  // Message handlers
  // ===========================================================================

  /** Quote (templateId=2): correlate by QuoteReqID → callback → remove correlation. */
  private Action handleQuote(
      final DirectBuffer buffer, final int bodyOffset, final int blockLength, final int version) {
    quoteDecoder.wrap(buffer, bodyOffset, blockLength, version);
    quoteDecoder.getQuoteReqId(quoteReqIdScratch, 0);
    final int quoteReqIdLen = trimNullPadding(quoteReqIdScratch);

    final long sessionKey = sessionLookup.findByCorrelationId(quoteReqIdScratch, 0, quoteReqIdLen);
    if (sessionKey == SessionLookup.NULL_SESSION) {
      sessionNotFound++;
      LOG.info()
          .append("Orphaned orchestrator Quote: quoteReqIdLen=")
          .append(quoteReqIdLen)
          .commit();
      return Action.CONTINUE;
    }

    final boolean delivered =
        callback.onOrchestratorResponse(sessionKey, QuoteDecoder.TEMPLATE_ID, 0L);
    if (delivered) {
      registry.removeCorrelation(quoteReqIdScratch, 0, quoteReqIdLen);
      quotesReceived++;
      return Action.CONTINUE;
    }
    backPressureAborts++;
    return Action.ABORT;
  }

  /** QuoteRequestReject (templateId=3): correlate by QuoteReqID → callback → remove correlation. */
  private Action handleQuoteRequestReject(
      final DirectBuffer buffer, final int bodyOffset, final int blockLength, final int version) {
    qrrDecoder.wrap(buffer, bodyOffset, blockLength, version);
    qrrDecoder.getQuoteReqId(qrrQuoteReqIdScratch, 0);
    final int quoteReqIdLen = trimNullPadding(qrrQuoteReqIdScratch);

    final long sessionKey =
        sessionLookup.findByCorrelationId(qrrQuoteReqIdScratch, 0, quoteReqIdLen);
    if (sessionKey == SessionLookup.NULL_SESSION) {
      sessionNotFound++;
      LOG.info()
          .append("Orphaned orchestrator QuoteRequestReject: quoteReqIdLen=")
          .append(quoteReqIdLen)
          .commit();
      return Action.CONTINUE;
    }

    final boolean delivered =
        callback.onOrchestratorResponse(sessionKey, QuoteRequestRejectDecoder.TEMPLATE_ID, 0L);
    if (delivered) {
      registry.removeCorrelation(qrrQuoteReqIdScratch, 0, quoteReqIdLen);
      qrrReceived++;
      return Action.CONTINUE;
    }
    backPressureAborts++;
    return Action.ABORT;
  }

  /**
   * NewOrderSingle (templateId=4): validated NOS forwarded from orchestrator. Extract ClOrdID,
   * re-register correlation with fresh timestamp, forward to cluster via {@link
   * ClusterClient#offerTracked}, and create {@link InFlightTracker} entry for cluster timeout.
   *
   * <p>The correlation was originally registered when the gateway sent the NOS to the orchestrator,
   * but may have been TTL-swept during a long RFQ lifecycle. Re-registration ensures the cluster's
   * ExecutionReport can be routed back to the correct FIX session.
   */
  private Action handleNosForward(
      final DirectBuffer buffer,
      final int fragmentOffset,
      final int fragmentLength,
      final int bodyOffset,
      final int blockLength,
      final int version) {
    nosDecoder.wrap(buffer, bodyOffset, blockLength, version);
    nosDecoder.getClOrdId(nosClOrdIdScratch, 0);
    final int clOrdIdLen = trimNullPadding(nosClOrdIdScratch);

    // Look up the session to get the sessionKey for re-registration
    final long sessionKey = sessionLookup.findByCorrelationId(nosClOrdIdScratch, 0, clOrdIdLen);
    if (sessionKey == SessionLookup.NULL_SESSION) {
      // Session disconnected while RFQ was in-flight — drop the NOS
      sessionNotFound++;
      LOG.info()
          .append("Orphaned orchestrator NOS forward: clOrdIdLen=")
          .append(clOrdIdLen)
          .commit();
      return Action.CONTINUE;
    }

    // Forward the raw SBE bytes to the cluster (includes SBE header + body).
    // Publish-before-mutate: offer FIRST, re-register correlation only on success.
    final long result =
        clusterClient.offerTracked(
            buffer, fragmentOffset, fragmentLength, nosClOrdIdScratch, 0, clOrdIdLen);

    if (result >= 0) {
      // Re-register correlation with fresh timestamp (the original may have been TTL-swept
      // during a long RFQ lifecycle). Done AFTER successful offer to avoid stale entries.
      registry.registerCorrelation(
          nosClOrdIdScratch, 0, clOrdIdLen, sessionKey, nanoClock.nanoTime());
      nosForwardedToCluster++;
      return Action.CONTINUE;
    }
    if (result == Publication.BACK_PRESSURED || result == Publication.ADMIN_ACTION) {
      backPressureAborts++;
      return Action.ABORT;
    }

    // Terminal: cluster NOT_CONNECTED, CLOSED, MAX_POSITION_EXCEEDED — drop the NOS.
    // The client will time out via their application-level timeout; the gateway's correlation
    // TTL sweep will clean the registry entry.
    nosClusterOfferFailed++;
    LOG.warn()
        .append("Cluster offer failed for NOS forward: result=")
        .append(result)
        .append(" clOrdIdLen=")
        .append(clOrdIdLen)
        .commit();
    return Action.CONTINUE;
  }

  /**
   * ExecutionReport (templateId=5): orchestrator-sourced reject (validation failure, NOS-too-large,
   * etc.). Correlate by ClOrdID → callback → remove correlation. These rejects did not transit the
   * cluster, so no in-flight timeout tracking applies.
   */
  private Action handleRejectExecutionReport(
      final DirectBuffer buffer, final int bodyOffset, final int blockLength, final int version) {
    erDecoder.wrap(buffer, bodyOffset, blockLength, version);
    erDecoder.getClOrdId(erClOrdIdScratch, 0);
    final int clOrdIdLen = trimNullPadding(erClOrdIdScratch);

    final long sessionKey = sessionLookup.findByCorrelationId(erClOrdIdScratch, 0, clOrdIdLen);
    if (sessionKey == SessionLookup.NULL_SESSION) {
      sessionNotFound++;
      LOG.info().append("Orphaned orchestrator reject ER: clOrdIdLen=").append(clOrdIdLen).commit();
      return Action.CONTINUE;
    }

    final boolean delivered =
        callback.onOrchestratorResponse(sessionKey, ExecutionReportDecoder.TEMPLATE_ID, 0L);
    if (delivered) {
      registry.removeCorrelation(erClOrdIdScratch, 0, clOrdIdLen);
      rejectErsReceived++;
      return Action.CONTINUE;
    }
    backPressureAborts++;
    return Action.ABORT;
  }

  // ===========================================================================
  // Accessors — callback reads pre-wrapped decoders without re-decoding
  // ===========================================================================

  /** Returns the pre-wrapped Quote decoder. Valid during {@link ResponseCallback} invocation. */
  public QuoteDecoder quoteDecoder() {
    return quoteDecoder;
  }

  /**
   * Returns the pre-wrapped QuoteRequestReject decoder. Valid during {@link ResponseCallback}
   * invocation.
   */
  public QuoteRequestRejectDecoder quoteRequestRejectDecoder() {
    return qrrDecoder;
  }

  /**
   * Returns the pre-wrapped ExecutionReport decoder. Valid during {@link ResponseCallback}
   * invocation for orchestrator-sourced reject ExecutionReports.
   */
  public ExecutionReportDecoder executionReportDecoder() {
    return erDecoder;
  }

  // --- Diagnostic counter accessors ---

  /** Number of Quote responses successfully delivered. */
  public long quotesReceived() {
    return quotesReceived;
  }

  /** Number of QuoteRequestReject responses successfully delivered. */
  public long qrrReceived() {
    return qrrReceived;
  }

  /** Number of NOS messages forwarded to cluster. */
  public long nosForwardedToCluster() {
    return nosForwardedToCluster;
  }

  /** Number of NOS cluster offer terminal failures (NOT_CONNECTED, CLOSED). */
  public long nosClusterOfferFailed() {
    return nosClusterOfferFailed;
  }

  /** Number of reject ExecutionReports successfully delivered. */
  public long rejectErsReceived() {
    return rejectErsReceived;
  }

  /** Number of ABORT returns due to back-pressure. */
  public long backPressureAborts() {
    return backPressureAborts;
  }

  /** Number of responses for disconnected sessions. */
  public long sessionNotFound() {
    return sessionNotFound;
  }

  // ===========================================================================
  // Internal helpers
  // ===========================================================================

  /**
   * Returns the number of significant bytes in a null-padded fixed-length SBE char field. Matches
   * {@link ClusterEgressListener#trimNullPadding(byte[])}.
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
