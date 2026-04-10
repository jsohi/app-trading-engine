package com.trading.engine.gateway;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.trading.engine.fix.decoder_flyweight.MassQuoteDecoder;
import com.trading.engine.fix.decoder_flyweight.MultilegOrderCancelReplaceRequestDecoder;
import com.trading.engine.fix.decoder_flyweight.NewOrderMultilegDecoder;
import com.trading.engine.fix.decoder_flyweight.NewOrderSingleDecoder;
import com.trading.engine.fix.decoder_flyweight.OrderCancelRequestDecoder;
import com.trading.engine.fix.decoder_flyweight.QuoteRequestDecoder;
import io.aeron.Publication;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import uk.co.real_logic.artio.library.OnMessageInfo;
import uk.co.real_logic.artio.library.SessionHandler;
import uk.co.real_logic.artio.messages.DisconnectReason;
import uk.co.real_logic.artio.session.Session;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

/**
 * Per-session Artio {@link SessionHandler} that decodes inbound FIX messages, translates them to
 * SBE, and offers them to the Aeron Cluster via the {@link ClusterClient}. Registers correlation
 * identifiers (ClOrdID, QuoteReqID) in the {@link SessionRegistry} for response routing.
 *
 * <p><b>Supported message types:</b>
 *
 * <ul>
 *   <li>NewOrderSingle (35=D)
 *   <li>OrderCancelRequest (35=F)
 *   <li>QuoteRequest (35=R)
 *   <li>MassQuote (35=i)
 *   <li>NewOrderMultileg (35=AB)
 *   <li>MultilegOrderCancelReplace (35=AC)
 * </ul>
 *
 * Unsupported message types receive a BusinessMessageReject (35=j).
 *
 * <p><b>Back-pressure.</b> If the cluster cannot accept the message ({@link
 * Publication#BACK_PRESSURED} or {@link Publication#ADMIN_ACTION}), returns {@link Action#ABORT} so
 * Artio re-delivers the FIX message on the next poll.
 *
 * <p><b>Allocation.</b> Zero allocation on the hot path. All decoders and scratch buffers are
 * pre-allocated. The {@link FixToSbeTranslator} and SBE buffer are shared across all sessions on
 * the same duty-cycle thread.
 *
 * <p><b>Threading.</b> Not thread-safe — single-threaded gateway duty-cycle thread only.
 */
public final class FixSessionHandler implements SessionHandler {

  private static final Log LOG = LogFactory.getLog(FixSessionHandler.class);

  private static final byte[] CLUSTER_UNAVAILABLE =
      "Cluster unavailable".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
  private static final byte[] SYSTEM_SHUTTING_DOWN =
      "System shutting down".getBytes(java.nio.charset.StandardCharsets.US_ASCII);

  private final Session session;
  private final long sessionKey;
  private final ClusterClient clusterClient;
  private final FixToSbeTranslator translator;
  private final SessionRegistry registry;
  private final RejectEmitter rejectEmitter;
  private final MutableAsciiBuffer asciiBuffer;
  private final MutableDirectBuffer sbeBuffer;
  private final DrainingSupplier drainingSupplier;

  // Pre-allocated FIX decoders (one per message type, reused across calls)
  private final NewOrderSingleDecoder nosDecoder = new NewOrderSingleDecoder();
  private final OrderCancelRequestDecoder cxlDecoder = new OrderCancelRequestDecoder();
  private final QuoteRequestDecoder quoteReqDecoder = new QuoteRequestDecoder();
  private final MassQuoteDecoder massQuoteDecoder = new MassQuoteDecoder();
  private final NewOrderMultilegDecoder multilegDecoder = new NewOrderMultilegDecoder();
  private final MultilegOrderCancelReplaceRequestDecoder multilegCxlDecoder =
      new MultilegOrderCancelReplaceRequestDecoder();

  // Scratch buffer for extracting correlation bytes from FIX char[] fields (zero-alloc)
  private final byte[] correlationScratch = new byte[20];

  // Scratch buffer for reject text in rejectOnError (avoids getBytes() allocation)
  private final byte[] rejectTextScratch = new byte[64];

  /**
   * Callback to check whether the gateway is in draining mode (graceful shutdown). Avoids coupling
   * FixSessionHandler directly to FixGateway.
   */
  @FunctionalInterface
  public interface DrainingSupplier {
    boolean isDraining();
  }

  /**
   * @param session Artio session (not null)
   * @param clusterClient cluster client for offering SBE messages
   * @param translator shared FIX→SBE translator (single-threaded, safe to share)
   * @param registry session + correlation registry
   * @param rejectEmitter pre-allocated reject encoder
   * @param asciiBuffer shared ASCII buffer for FIX decoding
   * @param sbeBuffer shared buffer for SBE encoding output
   * @param drainingSupplier returns true when gateway is draining (graceful shutdown)
   */
  public FixSessionHandler(
      final Session session,
      final ClusterClient clusterClient,
      final FixToSbeTranslator translator,
      final SessionRegistry registry,
      final RejectEmitter rejectEmitter,
      final MutableAsciiBuffer asciiBuffer,
      final MutableDirectBuffer sbeBuffer,
      final DrainingSupplier drainingSupplier) {
    this.session = session;
    this.sessionKey = session.id();
    this.clusterClient = clusterClient;
    this.translator = translator;
    this.registry = registry;
    this.rejectEmitter = rejectEmitter;
    this.asciiBuffer = asciiBuffer;
    this.sbeBuffer = sbeBuffer;
    this.drainingSupplier = drainingSupplier;
  }

  // ===========================================================================
  // SessionHandler
  // ===========================================================================

  @Override
  public Action onMessage(
      final DirectBuffer buffer,
      final int offset,
      final int length,
      final int libraryId,
      final Session session,
      final int sequenceIndex,
      final long messageType,
      final long timestampInNs,
      final long position,
      final OnMessageInfo messageInfo) {

    asciiBuffer.wrap(buffer, offset, length);

    if (drainingSupplier.isDraining()) {
      rejectEmitter.emit(
          session,
          session.lastReceivedMsgSeqNum(),
          messageType,
          com.trading.engine.fix.BusinessRejectReason.OTHER.representation(),
          SYSTEM_SHUTTING_DOWN,
          0,
          SYSTEM_SHUTTING_DOWN.length);
      return CONTINUE;
    }

    if (messageType == NewOrderSingleDecoder.MESSAGE_TYPE) {
      return handleNewOrderSingle(session, length, messageType);
    } else if (messageType == OrderCancelRequestDecoder.MESSAGE_TYPE) {
      return handleOrderCancelRequest(session, length, messageType);
    } else if (messageType == QuoteRequestDecoder.MESSAGE_TYPE) {
      return handleQuoteRequest(session, length, messageType);
    } else if (messageType == MassQuoteDecoder.MESSAGE_TYPE) {
      return handleMassQuote(session, length, messageType);
    } else if (messageType == NewOrderMultilegDecoder.MESSAGE_TYPE) {
      return handleNewOrderMultileg(session, length, messageType);
    } else if (messageType == MultilegOrderCancelReplaceRequestDecoder.MESSAGE_TYPE) {
      return handleMultilegCancelReplace(session, length, messageType);
    }

    rejectEmitter.emit(
        session,
        session.lastReceivedMsgSeqNum(),
        messageType,
        com.trading.engine.fix.BusinessRejectReason.UNSUPPORTED_MESSAGE_TYPE.representation(),
        null,
        0,
        0);
    LOG.warn()
        .append("Unsupported messageType=")
        .append(messageType)
        .append(" sessionId=")
        .append(sessionKey)
        .commit();
    return CONTINUE;
  }

  @Override
  public void onSessionStart(final Session session) {
    LOG.info().append("Session started: sessionId=").append(session.id()).commit();
  }

  @Override
  public void onTimeout(final int libraryId, final Session session) {
    LOG.warn().append("Session timeout: sessionId=").append(session.id()).commit();
  }

  @Override
  public void onSlowStatus(
      final int libraryId, final Session session, final boolean hasBecomeSlow) {
    LOG.info()
        .append("Session slow status: sessionId=")
        .append(session.id())
        .append(" slow=")
        .append(hasBecomeSlow)
        .commit();
  }

  @Override
  public Action onDisconnect(
      final int libraryId, final Session session, final DisconnectReason reason) {
    LOG.info()
        .append("Session disconnected: sessionId=")
        .append(sessionKey)
        .append(" reason=")
        .append(reason.name())
        .commit();
    registry.removeSession(sessionKey);
    return CONTINUE;
  }

  // ===========================================================================
  // Message handlers
  // ===========================================================================

  private Action handleNewOrderSingle(
      final Session session, final int length, final long messageType) {
    try {
      nosDecoder.decode(asciiBuffer, 0, length);
      final int sbeLen = translator.translateNewOrderSingle(nosDecoder, sbeBuffer, 0);
      final int corrLen = copyCharsToBytes(nosDecoder.clOrdID(), nosDecoder.clOrdIDLength());
      return offerAndRegister(session, sbeLen, corrLen, messageType);
    } catch (final Exception ex) {
      return rejectOnError(session, messageType, ex);
    }
  }

  private Action handleOrderCancelRequest(
      final Session session, final int length, final long messageType) {
    try {
      cxlDecoder.decode(asciiBuffer, 0, length);
      final int sbeLen = translator.translateOrderCancelRequest(cxlDecoder, sbeBuffer, 0);
      final int corrLen = copyCharsToBytes(cxlDecoder.clOrdID(), cxlDecoder.clOrdIDLength());
      return offerAndRegister(session, sbeLen, corrLen, messageType);
    } catch (final Exception ex) {
      return rejectOnError(session, messageType, ex);
    }
  }

  private Action handleQuoteRequest(
      final Session session, final int length, final long messageType) {
    try {
      quoteReqDecoder.decode(asciiBuffer, 0, length);
      final int sbeLen = translator.translateQuoteRequest(quoteReqDecoder, sbeBuffer, 0);
      final int corrLen =
          copyCharsToBytes(quoteReqDecoder.quoteReqID(), quoteReqDecoder.quoteReqIDLength());
      return offerAndRegister(session, sbeLen, corrLen, messageType);
    } catch (final Exception ex) {
      return rejectOnError(session, messageType, ex);
    }
  }

  private Action handleMassQuote(final Session session, final int length, final long messageType) {
    try {
      massQuoteDecoder.decode(asciiBuffer, 0, length);
      final int sbeLen = translator.translateMassQuote(massQuoteDecoder, sbeBuffer, 0);
      final int corrLen =
          copyCharsToBytes(massQuoteDecoder.quoteID(), massQuoteDecoder.quoteIDLength());
      return offerAndRegister(session, sbeLen, corrLen, messageType);
    } catch (final Exception ex) {
      return rejectOnError(session, messageType, ex);
    }
  }

  private Action handleNewOrderMultileg(
      final Session session, final int length, final long messageType) {
    try {
      multilegDecoder.decode(asciiBuffer, 0, length);
      final int sbeLen = translator.translateNewOrderMultileg(multilegDecoder, sbeBuffer, 0);
      final int corrLen =
          copyCharsToBytes(multilegDecoder.clOrdID(), multilegDecoder.clOrdIDLength());
      return offerAndRegister(session, sbeLen, corrLen, messageType);
    } catch (final Exception ex) {
      return rejectOnError(session, messageType, ex);
    }
  }

  private Action handleMultilegCancelReplace(
      final Session session, final int length, final long messageType) {
    try {
      multilegCxlDecoder.decode(asciiBuffer, 0, length);
      final int sbeLen =
          translator.translateMultilegOrderCancelReplace(multilegCxlDecoder, sbeBuffer, 0);
      final int corrLen =
          copyCharsToBytes(multilegCxlDecoder.clOrdID(), multilegCxlDecoder.clOrdIDLength());
      return offerAndRegister(session, sbeLen, corrLen, messageType);
    } catch (final Exception ex) {
      return rejectOnError(session, messageType, ex);
    }
  }

  // ===========================================================================
  // Internal helpers
  // ===========================================================================

  /**
   * Offer the SBE-encoded message to the cluster and register the correlation in the registry.
   * Returns ABORT on backpressure so Artio re-delivers.
   */
  private Action offerAndRegister(
      final Session session, final int sbeLen, final int correlationLen, final long messageType) {
    final long result =
        clusterClient.offerTracked(sbeBuffer, 0, sbeLen, correlationScratch, 0, correlationLen);

    if (result >= 0) {
      // Register correlation only after successful offer — avoids orphan entries on failure/ABORT.
      registry.registerCorrelation(correlationScratch, 0, correlationLen, sessionKey);
      return CONTINUE;
    }
    if (result == Publication.BACK_PRESSURED || result == Publication.ADMIN_ACTION) {
      return ABORT;
    }

    // NOT_CONNECTED, CLOSED, MAX_POSITION_EXCEEDED — cluster is down
    LOG.warn()
        .append("Cluster offer failed: result=")
        .append(result)
        .append(" sessionId=")
        .append(sessionKey)
        .commit();
    rejectEmitter.emit(
        session,
        session.lastReceivedMsgSeqNum(),
        messageType,
        com.trading.engine.fix.BusinessRejectReason.OTHER.representation(),
        CLUSTER_UNAVAILABLE,
        0,
        CLUSTER_UNAVAILABLE.length);
    return CONTINUE;
  }

  /** Send a BusinessMessageReject for a translation or processing error. Zero-alloc path. */
  private Action rejectOnError(final Session session, final long messageType, final Exception ex) {
    LOG.warn()
        .append("Translation error: sessionId=")
        .append(sessionKey)
        .append(" msgType=")
        .append(messageType)
        .append(" error=")
        .append(ex.getMessage())
        .commit();
    final int textLen = copyExceptionMessage(ex, rejectTextScratch);
    rejectEmitter.emit(
        session,
        session.lastReceivedMsgSeqNum(),
        messageType,
        RejectEmitter.mapExceptionToRejectReason(ex),
        textLen > 0 ? rejectTextScratch : null,
        0,
        textLen);
    return CONTINUE;
  }

  /**
   * Copy the exception message into a pre-allocated scratch buffer without allocating. Returns the
   * number of bytes written, or 0 if the message is null.
   */
  private static int copyExceptionMessage(final Exception ex, final byte[] dst) {
    final String msg = ex.getMessage();
    if (msg == null) {
      return 0;
    }
    final int len = Math.min(msg.length(), dst.length);
    for (int i = 0; i < len; i++) {
      dst[i] = (byte) msg.charAt(i);
    }
    return len;
  }

  /**
   * Copy FIX char[] field to byte[] scratch buffer for correlation registration. Returns the number
   * of bytes written.
   */
  private int copyCharsToBytes(final char[] src, final int srcLen) {
    final int len = Math.min(srcLen, correlationScratch.length);
    for (int i = 0; i < len; i++) {
      correlationScratch[i] = (byte) src[i];
    }
    return len;
  }
}
