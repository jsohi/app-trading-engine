package com.trading.engine.gateway;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.trading.engine.fix.BusinessRejectReason;
import com.trading.engine.fix.builder.BusinessMessageRejectEncoder;

/**
 * Zero-allocation emitter for FIX BusinessMessageReject (35=j) messages. Used by the gateway to
 * reject inbound messages that fail translation, target an unsupported message type, or arrive when
 * the cluster is unavailable.
 *
 * <p><b>FIX compliance.</b> The FIX specification requires a response to every application-level
 * message. Silent drops violate the protocol and leave clients uncertain about order state. This
 * emitter sends a structured reject with reason code and descriptive text.
 *
 * <p><b>Allocation.</b> The {@link BusinessMessageRejectEncoder} and scratch buffers are
 * pre-allocated at construction time. The encoder is {@link BusinessMessageRejectEncoder#reset()
 * reset} before each use.
 *
 * <p><b>Threading.</b> Not thread-safe — single-threaded gateway duty-cycle thread only.
 */
public final class RejectEmitter {

  private static final Log LOG = LogFactory.getLog(RejectEmitter.class);

  private final BusinessMessageRejectEncoder rejectEncoder = new BusinessMessageRejectEncoder();

  // Scratch buffers for RefMsgType encoding (max 2 chars for "AB", "AC", etc.)
  private final byte[] refMsgTypeScratch = new byte[2];

  /**
   * Emit a BusinessMessageReject (35=j) to the given FIX session.
   *
   * @param session gateway session to send the reject to
   * @param refSeqNum MsgSeqNum of the rejected message (tag 45)
   * @param refMsgType MsgType of the rejected message as Artio long encoding (tag 372)
   * @param reason FIX BusinessRejectReason code (tag 380)
   * @param text human-readable rejection text (tag 58)
   * @param textOffset offset within {@code text}
   * @param textLength number of significant bytes in {@code text}
   * @return the result of {@link GatewaySession#trySend}, or {@code -1L} if session is not
   *     connected
   */
  public long emit(
      final GatewaySession session,
      final int refSeqNum,
      final long refMsgType,
      final int reason,
      final byte[] text,
      final int textOffset,
      final int textLength) {

    if (!session.isConnected()) {
      return -1L;
    }

    rejectEncoder.reset();
    rejectEncoder.refSeqNum(refSeqNum);

    // Encode refMsgType from the long-packed Artio format back to ASCII bytes.
    final int refMsgTypeLen = encodeMsgType(refMsgType);
    rejectEncoder.refMsgType(refMsgTypeScratch, 0, refMsgTypeLen);

    rejectEncoder.businessRejectReason(reason);

    if (text != null && textLength > 0) {
      rejectEncoder.text(text, textOffset, textLength);
    }

    final long position = session.trySend(rejectEncoder);
    if (position < 0) {
      LOG.warn()
          .append("Failed to send BusinessMessageReject: position=")
          .append(position)
          .append(" refSeqNum=")
          .append(refSeqNum)
          .commit();
    }
    return position;
  }

  /**
   * Convenience overload that accepts a {@link BusinessRejectReason} enum.
   *
   * @see #emit(GatewaySession, int, long, int, byte[], int, int)
   */
  public long emit(
      final GatewaySession session,
      final int refSeqNum,
      final long refMsgType,
      final BusinessRejectReason reason,
      final byte[] text,
      final int textOffset,
      final int textLength) {
    return emit(
        session, refSeqNum, refMsgType, reason.representation(), text, textOffset, textLength);
  }

  /**
   * Map a translator exception to a FIX BusinessRejectReason code.
   *
   * @param ex the exception thrown by the translator
   * @return appropriate FIX BusinessRejectReason code
   */
  public static int mapExceptionToRejectReason(final Exception ex) {
    if (ex instanceof IllegalStateException) {
      final String msg = ex.getMessage();
      if (msg != null && (msg.contains("Unsupported") || msg.contains("unmapped"))) {
        return BusinessRejectReason.UNSUPPORTED_MESSAGE_TYPE.representation();
      }
    }
    return BusinessRejectReason.OTHER.representation();
  }

  /**
   * Encode an Artio long-packed MsgType back to ASCII bytes in {@link #refMsgTypeScratch}. Single
   * char types (e.g., 'D' = 68L) produce 1 byte; two-char types (e.g., 'AB' = 16961L) produce 2.
   *
   * @return the number of significant bytes written to the scratch buffer
   */
  private int encodeMsgType(final long msgType) {
    if (msgType <= 0xFF) {
      refMsgTypeScratch[0] = (byte) msgType;
      return 1;
    }
    // Two-char MsgType: first char in low byte, second char in high byte (Artio packs chars
    // left-to-right into ascending byte positions of the long).
    refMsgTypeScratch[0] = (byte) (msgType & 0xFF);
    refMsgTypeScratch[1] = (byte) ((msgType >> 8) & 0xFF);
    return 2;
  }
}
