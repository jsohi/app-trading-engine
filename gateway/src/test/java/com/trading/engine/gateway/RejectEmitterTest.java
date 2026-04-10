package com.trading.engine.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.fix.BusinessRejectReason;
import com.trading.engine.fix.builder.BusinessMessageRejectEncoder;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RejectEmitterTest {

  private static final long MSG_TYPE_D = 68L; // NewOrderSingle
  private static final long MSG_TYPE_AB = 16961L; // NewOrderMultileg

  private RejectEmitter emitter;
  private FakeGatewaySession session;

  @BeforeEach
  void setUp() {
    emitter = new RejectEmitter();
    session = new FakeGatewaySession(1L);
    session.setLastReceivedMsgSeqNum(42);
  }

  // ===========================================================================
  // Emit with int reason code
  // ===========================================================================

  @Test
  void emitSingleCharMsgType() {
    final byte[] text = "Bad order".getBytes(StandardCharsets.US_ASCII);
    final long result =
        emitter.emit(
            session,
            42,
            MSG_TYPE_D,
            BusinessRejectReason.OTHER.representation(),
            text,
            0,
            text.length);

    assertTrue(result >= 0);
    assertEquals(1, session.sentEncoders.size());
    assertTrue(session.sentEncoders.get(0) instanceof BusinessMessageRejectEncoder);
  }

  @Test
  void emitTwoCharMsgType() {
    final long result =
        emitter.emit(
            session,
            10,
            MSG_TYPE_AB,
            BusinessRejectReason.UNSUPPORTED_MESSAGE_TYPE.representation(),
            null,
            0,
            0);

    assertTrue(result >= 0);
    assertEquals(1, session.sentEncoders.size());
  }

  @Test
  void emitWithNullText() {
    final long result =
        emitter.emit(
            session, 1, MSG_TYPE_D, BusinessRejectReason.OTHER.representation(), null, 0, 0);

    assertTrue(result >= 0);
    assertEquals(1, session.sentEncoders.size());
  }

  @Test
  void emitWithEmptyText() {
    final long result =
        emitter.emit(
            session, 1, MSG_TYPE_D, BusinessRejectReason.OTHER.representation(), new byte[0], 0, 0);

    assertTrue(result >= 0);
    assertEquals(1, session.sentEncoders.size());
  }

  // ===========================================================================
  // Emit with enum reason
  // ===========================================================================

  @Test
  void emitWithEnumReason() {
    final byte[] text = "Unsupported".getBytes(StandardCharsets.US_ASCII);
    final long result =
        emitter.emit(
            session,
            5,
            MSG_TYPE_D,
            BusinessRejectReason.UNSUPPORTED_MESSAGE_TYPE,
            text,
            0,
            text.length);

    assertTrue(result >= 0);
    assertEquals(1, session.sentEncoders.size());
  }

  // ===========================================================================
  // Disconnected session
  // ===========================================================================

  @Test
  void emitToDisconnectedSessionReturnsMinusOne() {
    session.setConnected(false);
    final long result =
        emitter.emit(
            session, 1, MSG_TYPE_D, BusinessRejectReason.OTHER.representation(), null, 0, 0);

    assertEquals(-1L, result);
    assertEquals(0, session.sentEncoders.size());
  }

  // ===========================================================================
  // Back-pressure on trySend
  // ===========================================================================

  @Test
  void emitReturnsNegativeOnBackPressure() {
    session.setTrySendResult(-2L); // simulate back-pressure
    final long result =
        emitter.emit(
            session, 1, MSG_TYPE_D, BusinessRejectReason.OTHER.representation(), null, 0, 0);

    assertEquals(-2L, result);
    assertEquals(1, session.sentEncoders.size()); // trySend was still called
  }

  // ===========================================================================
  // mapExceptionToRejectReason
  // ===========================================================================

  @Test
  void mapUnsupportedIllegalStateException() {
    final Exception ex = new IllegalStateException("Unsupported FIX Side(54): Z");
    assertEquals(
        BusinessRejectReason.UNSUPPORTED_MESSAGE_TYPE.representation(),
        RejectEmitter.mapExceptionToRejectReason(ex));
  }

  @Test
  void mapUnmappedIllegalStateException() {
    final Exception ex = new IllegalStateException("unmapped enum value");
    assertEquals(
        BusinessRejectReason.UNSUPPORTED_MESSAGE_TYPE.representation(),
        RejectEmitter.mapExceptionToRejectReason(ex));
  }

  @Test
  void mapGenericIllegalStateException() {
    final Exception ex = new IllegalStateException("something else entirely");
    assertEquals(
        BusinessRejectReason.OTHER.representation(), RejectEmitter.mapExceptionToRejectReason(ex));
  }

  @Test
  void mapArithmeticException() {
    final Exception ex = new ArithmeticException("overflow");
    assertEquals(
        BusinessRejectReason.OTHER.representation(), RejectEmitter.mapExceptionToRejectReason(ex));
  }

  @Test
  void mapNullMessageIllegalStateException() {
    final Exception ex = new IllegalStateException((String) null);
    assertEquals(
        BusinessRejectReason.OTHER.representation(), RejectEmitter.mapExceptionToRejectReason(ex));
  }

  // ===========================================================================
  // Encoder reuse (multiple emits reuse the same encoder)
  // ===========================================================================

  @Test
  void multipleEmitsReuseEncoder() {
    final byte[] text1 = "first".getBytes(StandardCharsets.US_ASCII);
    final byte[] text2 = "second".getBytes(StandardCharsets.US_ASCII);

    emitter.emit(
        session,
        1,
        MSG_TYPE_D,
        BusinessRejectReason.OTHER.representation(),
        text1,
        0,
        text1.length);
    emitter.emit(
        session,
        2,
        MSG_TYPE_AB,
        BusinessRejectReason.UNSUPPORTED_MESSAGE_TYPE.representation(),
        text2,
        0,
        text2.length);

    assertEquals(2, session.sentEncoders.size());
    // Both sends should use the same encoder instance (pre-allocated, reset between calls)
    assertSameEncoderType(session.sentEncoders.get(0));
    assertSameEncoderType(session.sentEncoders.get(1));
  }

  private static void assertSameEncoderType(final Object encoder) {
    assertTrue(
        encoder instanceof BusinessMessageRejectEncoder,
        "Expected BusinessMessageRejectEncoder, got " + encoder.getClass().getName());
  }
}
