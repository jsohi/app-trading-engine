package com.trading.engine.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.ExecTypeEnum;
import com.trading.engine.messages.sbe.ExecutionReportDecoder;
import com.trading.engine.messages.sbe.ExecutionReportEncoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.OrderCancelRejectDecoder;
import com.trading.engine.messages.sbe.OrderCancelRejectEncoder;
import com.trading.engine.messages.sbe.OrderCreatedEventDecoder;
import com.trading.engine.messages.sbe.OrderCreatedEventEncoder;
import com.trading.engine.messages.sbe.OrderExpiredEventDecoder;
import com.trading.engine.messages.sbe.OrderExpiredEventEncoder;
import com.trading.engine.messages.sbe.OrderRejectedEventDecoder;
import com.trading.engine.messages.sbe.OrderRejectedEventEncoder;
import com.trading.engine.messages.sbe.QuoteDecoder;
import com.trading.engine.messages.sbe.QuoteEncoder;
import com.trading.engine.messages.sbe.RejectReasonEnum;
import com.trading.engine.messages.sbe.SideEnum;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ClusterEgressListenerTest {

  private static final long SESSION_KEY = 42L;
  private static final long TIMESTAMP = 1_000_000L;
  private static final String CL_ORD_ID = "ORD-00000000001";
  private static final String QUOTE_REQ_ID = "QR-000000000001";

  private final MutableDirectBuffer buffer = new ExpandableArrayBuffer(512);
  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  private final SbeToFixTranslator translator = new SbeToFixTranslator();
  private final InFlightTracker inFlightTracker =
      new InFlightTracker(16, TimeUnit.SECONDS.toNanos(5), 20);

  private final AtomicLong lastSessionKey = new AtomicLong(-1);
  private final AtomicInteger lastTemplateId = new AtomicInteger(-1);
  private final AtomicLong lastTimestamp = new AtomicLong(-1);
  private boolean callbackDelivers = true;

  private ClusterEgressListener listener;

  @BeforeEach
  void setUp() {
    final SessionLookup sessionLookup =
        (clOrdId, offset, length) -> {
          final String id = new String(clOrdId, offset, length, StandardCharsets.US_ASCII);
          if (CL_ORD_ID.equals(id) || QUOTE_REQ_ID.equals(id)) {
            return SESSION_KEY;
          }
          return SessionLookup.NULL_SESSION;
        };

    final ClusterEgressListener.EgressCallback callback =
        (sessionKey, templateId, timestamp) -> {
          lastSessionKey.set(sessionKey);
          lastTemplateId.set(templateId);
          lastTimestamp.set(timestamp);
          return callbackDelivers;
        };

    listener = new ClusterEgressListener(translator, sessionLookup, inFlightTracker, callback);
  }

  // ===========================================================================
  // ExecutionReport dispatch
  // ===========================================================================

  @Test
  void onMessage_executionReport_dispatchesToCallback() {
    final int len = encodeExecutionReport(CL_ORD_ID);

    final Action action = listener.onMessage(1L, TIMESTAMP, buffer, 0, len, null);

    assertEquals(Action.CONTINUE, action);
    assertEquals(SESSION_KEY, lastSessionKey.get());
    assertEquals(ExecutionReportDecoder.TEMPLATE_ID, lastTemplateId.get());
    assertEquals(TIMESTAMP, lastTimestamp.get());
  }

  @Test
  void onMessage_executionReport_sessionNotFound_continues() {
    final int len = encodeExecutionReport("UNKNOWN-CL-ORD-ID");

    final Action action = listener.onMessage(1L, TIMESTAMP, buffer, 0, len, null);

    assertEquals(Action.CONTINUE, action);
    assertEquals(-1, lastSessionKey.get());
  }

  @Test
  void onMessage_executionReport_callbackReturnsFalse_aborts() {
    callbackDelivers = false;
    final int len = encodeExecutionReport(CL_ORD_ID);

    final Action action = listener.onMessage(1L, TIMESTAMP, buffer, 0, len, null);

    assertEquals(Action.ABORT, action);
  }

  @Test
  void onMessage_executionReport_abort_preservesInFlightEntry() {
    callbackDelivers = false;
    final byte[] id = CL_ORD_ID.getBytes(StandardCharsets.US_ASCII);
    inFlightTracker.onCommandSent(id, 0, id.length, 1_000L);
    assertEquals(1, inFlightTracker.size());

    final int len = encodeExecutionReport(CL_ORD_ID);
    final Action action = listener.onMessage(1L, TIMESTAMP, buffer, 0, len, null);

    assertEquals(Action.ABORT, action);
    assertEquals(1, inFlightTracker.size()); // entry preserved for timeout protection
  }

  @Test
  void onMessage_executionReport_clearsInFlightEntry() {
    final byte[] id = CL_ORD_ID.getBytes(StandardCharsets.US_ASCII);
    inFlightTracker.onCommandSent(id, 0, id.length, 1_000L);
    assertEquals(1, inFlightTracker.size());

    final int len = encodeExecutionReport(CL_ORD_ID);
    listener.onMessage(1L, TIMESTAMP, buffer, 0, len, null);

    assertEquals(0, inFlightTracker.size());
  }

  // ===========================================================================
  // OrderCancelReject dispatch
  // ===========================================================================

  @Test
  void onMessage_orderCancelReject_dispatchesToCallback() {
    final int len = encodeOrderCancelReject(CL_ORD_ID);

    final Action action = listener.onMessage(1L, TIMESTAMP, buffer, 0, len, null);

    assertEquals(Action.CONTINUE, action);
    assertEquals(SESSION_KEY, lastSessionKey.get());
    assertEquals(OrderCancelRejectDecoder.TEMPLATE_ID, lastTemplateId.get());
  }

  @Test
  void onMessage_orderCancelReject_sessionNotFound_continues() {
    final int len = encodeOrderCancelReject("UNKNOWN-CL-ORD-ID");

    final Action action = listener.onMessage(1L, TIMESTAMP, buffer, 0, len, null);

    assertEquals(Action.CONTINUE, action);
    assertEquals(-1, lastSessionKey.get());
  }

  // ===========================================================================
  // Quote dispatch
  // ===========================================================================

  @Test
  void onMessage_quote_routesByQuoteReqId() {
    final int len = encodeQuote(QUOTE_REQ_ID);

    final Action action = listener.onMessage(1L, TIMESTAMP, buffer, 0, len, null);

    assertEquals(Action.CONTINUE, action);
    assertEquals(SESSION_KEY, lastSessionKey.get());
    assertEquals(QuoteDecoder.TEMPLATE_ID, lastTemplateId.get());
  }

  @Test
  void onMessage_quote_sessionNotFound_continues() {
    final int len = encodeQuote("UNKNOWN-QR-ID");

    final Action action = listener.onMessage(1L, TIMESTAMP, buffer, 0, len, null);

    assertEquals(Action.CONTINUE, action);
    assertEquals(-1, lastSessionKey.get());
  }

  // ===========================================================================
  // Unknown templateId
  // ===========================================================================

  @Test
  void onMessage_unknownTemplateId_continues() {
    // Write a header with a bogus templateId.
    headerEncoder.wrap(buffer, 0).blockLength(0).templateId(999).schemaId(1).version(1);
    final int len = MessageHeaderEncoder.ENCODED_LENGTH;

    final Action action = listener.onMessage(1L, TIMESTAMP, buffer, 0, len, null);

    assertEquals(Action.CONTINUE, action);
    assertEquals(-1, lastSessionKey.get());
  }

  // ===========================================================================
  // Decoder accessors
  // ===========================================================================

  @Test
  void decoderAccessors_returnNonNull() {
    assertNotNull(listener.headerDecoder());
    assertNotNull(listener.executionReportDecoder());
    assertNotNull(listener.orderCancelRejectDecoder());
    assertNotNull(listener.quoteDecoder());
    assertNotNull(listener.translator());
  }

  @Test
  void decoderAccessors_returnSameInstance() {
    assertSame(listener.executionReportDecoder(), listener.executionReportDecoder());
    assertSame(listener.orderCancelRejectDecoder(), listener.orderCancelRejectDecoder());
    assertSame(listener.quoteDecoder(), listener.quoteDecoder());
    assertSame(listener.translator(), listener.translator());
  }

  @Test
  void executionReportDecoder_positionedAfterOnMessage() {
    final int len = encodeExecutionReport(CL_ORD_ID);
    listener.onMessage(1L, TIMESTAMP, buffer, 0, len, null);

    final var dec = listener.executionReportDecoder();
    final byte[] scratch = new byte[ExecutionReportDecoder.clOrdIdLength()];
    dec.getClOrdId(scratch, 0);
    final String decoded =
        new String(
            scratch, 0, ClusterEgressListener.trimNullPadding(scratch), StandardCharsets.US_ASCII);
    assertEquals(CL_ORD_ID, decoded);
  }

  // ===========================================================================
  // Session events
  // ===========================================================================

  @Test
  void onSessionEvent_error_setsReconnectNeeded() {
    assertFalse(listener.isReconnectNeeded());
    listener.onSessionEvent(0, 1L, 1L, 0, EventCode.ERROR, "test error");
    assertTrue(listener.isReconnectNeeded());
  }

  @Test
  void onSessionEvent_closed_setsReconnectNeeded() {
    listener.onSessionEvent(0, 1L, 1L, 0, EventCode.CLOSED, "session closed");
    assertTrue(listener.isReconnectNeeded());
  }

  @Test
  void onSessionEvent_authenticationRejected_setsReconnectNeeded() {
    listener.onSessionEvent(0, 1L, 1L, 0, EventCode.AUTHENTICATION_REJECTED, "auth rejected");
    assertTrue(listener.isReconnectNeeded());
  }

  @Test
  void onSessionEvent_ok_doesNotSetReconnectNeeded() {
    listener.onSessionEvent(0, 1L, 1L, 0, EventCode.OK, "ok");
    assertFalse(listener.isReconnectNeeded());
  }

  @Test
  void clearReconnectNeeded_resetsFlag() {
    listener.onSessionEvent(0, 1L, 1L, 0, EventCode.ERROR, "err");
    assertTrue(listener.isReconnectNeeded());
    listener.clearReconnectNeeded();
    assertFalse(listener.isReconnectNeeded());
  }

  // ===========================================================================
  // trimNullPadding
  // ===========================================================================

  @Test
  void trimNullPadding_fullBuffer() {
    assertEquals(5, ClusterEgressListener.trimNullPadding("HELLO".getBytes()));
  }

  @Test
  void trimNullPadding_withTrailingNulls() {
    final byte[] buf = new byte[10];
    buf[0] = 'A';
    buf[1] = 'B';
    assertEquals(2, ClusterEgressListener.trimNullPadding(buf));
  }

  @Test
  void trimNullPadding_allNulls() {
    assertEquals(0, ClusterEgressListener.trimNullPadding(new byte[5]));
  }

  // ===========================================================================
  // OrderCreatedEvent dispatch
  // ===========================================================================

  @Test
  void onMessage_orderCreatedEvent_dispatchesToCallback() {
    final int len = encodeOrderCreatedEvent(CL_ORD_ID);

    final Action action = listener.onMessage(1L, TIMESTAMP, buffer, 0, len, null);

    assertEquals(Action.CONTINUE, action);
    assertEquals(SESSION_KEY, lastSessionKey.get());
    assertEquals(OrderCreatedEventDecoder.TEMPLATE_ID, lastTemplateId.get());
    assertEquals(TIMESTAMP, lastTimestamp.get());
  }

  @Test
  void onMessage_orderCreatedEvent_sessionNotFound_continues() {
    final int len = encodeOrderCreatedEvent("UNKNOWN-CL-ORD-ID");

    final Action action = listener.onMessage(1L, TIMESTAMP, buffer, 0, len, null);

    assertEquals(Action.CONTINUE, action);
    assertEquals(-1, lastSessionKey.get());
  }

  @Test
  void onMessage_orderCreatedEvent_callbackReturnsFalse_aborts() {
    callbackDelivers = false;
    final int len = encodeOrderCreatedEvent(CL_ORD_ID);

    final Action action = listener.onMessage(1L, TIMESTAMP, buffer, 0, len, null);

    assertEquals(Action.ABORT, action);
  }

  @Test
  void onMessage_orderCreatedEvent_clearsInFlightEntry() {
    final byte[] id = CL_ORD_ID.getBytes(StandardCharsets.US_ASCII);
    inFlightTracker.onCommandSent(id, 0, id.length, 1_000L);
    assertEquals(1, inFlightTracker.size());

    final int len = encodeOrderCreatedEvent(CL_ORD_ID);
    listener.onMessage(1L, TIMESTAMP, buffer, 0, len, null);

    assertEquals(0, inFlightTracker.size());
  }

  @Test
  void orderCreatedDecoder_positionedAfterOnMessage() {
    final int len = encodeOrderCreatedEvent(CL_ORD_ID);
    listener.onMessage(1L, TIMESTAMP, buffer, 0, len, null);

    final var dec = listener.orderCreatedDecoder();
    final byte[] scratch = new byte[OrderCreatedEventDecoder.clOrdIdLength()];
    dec.getClOrdId(scratch, 0);
    final String decoded =
        new String(
            scratch, 0, ClusterEgressListener.trimNullPadding(scratch), StandardCharsets.US_ASCII);
    assertEquals(CL_ORD_ID, decoded);
  }

  // ===========================================================================
  // OrderRejectedEvent dispatch
  // ===========================================================================

  @Test
  void onMessage_orderRejectedEvent_dispatchesToCallback() {
    final int len = encodeOrderRejectedEvent(CL_ORD_ID);

    final Action action = listener.onMessage(1L, TIMESTAMP, buffer, 0, len, null);

    assertEquals(Action.CONTINUE, action);
    assertEquals(SESSION_KEY, lastSessionKey.get());
    assertEquals(OrderRejectedEventDecoder.TEMPLATE_ID, lastTemplateId.get());
    assertEquals(TIMESTAMP, lastTimestamp.get());
  }

  @Test
  void onMessage_orderRejectedEvent_sessionNotFound_continues() {
    final int len = encodeOrderRejectedEvent("UNKNOWN-CL-ORD-ID");

    final Action action = listener.onMessage(1L, TIMESTAMP, buffer, 0, len, null);

    assertEquals(Action.CONTINUE, action);
    assertEquals(-1, lastSessionKey.get());
  }

  @Test
  void onMessage_orderRejectedEvent_callbackReturnsFalse_aborts() {
    callbackDelivers = false;
    final int len = encodeOrderRejectedEvent(CL_ORD_ID);

    final Action action = listener.onMessage(1L, TIMESTAMP, buffer, 0, len, null);

    assertEquals(Action.ABORT, action);
  }

  @Test
  void onMessage_orderRejectedEvent_clearsInFlightEntry() {
    final byte[] id = CL_ORD_ID.getBytes(StandardCharsets.US_ASCII);
    inFlightTracker.onCommandSent(id, 0, id.length, 1_000L);
    assertEquals(1, inFlightTracker.size());

    final int len = encodeOrderRejectedEvent(CL_ORD_ID);
    listener.onMessage(1L, TIMESTAMP, buffer, 0, len, null);

    assertEquals(0, inFlightTracker.size());
  }

  @Test
  void orderRejectedDecoder_positionedAfterOnMessage() {
    final int len = encodeOrderRejectedEvent(CL_ORD_ID);
    listener.onMessage(1L, TIMESTAMP, buffer, 0, len, null);

    final var dec = listener.orderRejectedDecoder();
    final byte[] scratch = new byte[OrderRejectedEventDecoder.clOrdIdLength()];
    dec.getClOrdId(scratch, 0);
    final String decoded =
        new String(
            scratch, 0, ClusterEgressListener.trimNullPadding(scratch), StandardCharsets.US_ASCII);
    assertEquals(CL_ORD_ID, decoded);
  }

  // ===========================================================================
  // Decoder accessor coverage for new decoders
  // ===========================================================================

  @Test
  void decoderAccessors_returnNonNull_newDecoders() {
    assertNotNull(listener.orderCreatedDecoder());
    assertNotNull(listener.orderRejectedDecoder());
    assertNotNull(listener.orderExpiredDecoder(), "APP-62 §J orderExpiredDecoder must be non-null");
  }

  @Test
  void decoderAccessors_returnSameInstance_newDecoders() {
    assertSame(listener.orderCreatedDecoder(), listener.orderCreatedDecoder());
    assertSame(listener.orderRejectedDecoder(), listener.orderRejectedDecoder());
    assertSame(
        listener.orderExpiredDecoder(),
        listener.orderExpiredDecoder(),
        "orderExpiredDecoder must return the same pre-allocated flyweight instance");
  }

  // ===========================================================================
  // OrderExpiredEvent dispatch (APP-62 §J)
  // ===========================================================================

  @Test
  void onMessage_orderExpiredEvent_dispatchesToCallback() {
    final int len = encodeOrderExpiredEvent(CL_ORD_ID);

    final Action action = listener.onMessage(1L, TIMESTAMP, buffer, 0, len, null);

    assertEquals(Action.CONTINUE, action);
    assertEquals(SESSION_KEY, lastSessionKey.get());
    assertEquals(OrderExpiredEventDecoder.TEMPLATE_ID, lastTemplateId.get());
    assertEquals(TIMESTAMP, lastTimestamp.get());
  }

  @Test
  void onMessage_orderExpiredEvent_sessionNotFound_continues() {
    final int len = encodeOrderExpiredEvent("UNKNOWN-CL-ORD-ID");

    final Action action = listener.onMessage(1L, TIMESTAMP, buffer, 0, len, null);

    assertEquals(Action.CONTINUE, action);
    assertEquals(-1, lastSessionKey.get());
  }

  @Test
  void onMessage_orderExpiredEvent_callbackReturnsFalse_aborts() {
    callbackDelivers = false;
    final int len = encodeOrderExpiredEvent(CL_ORD_ID);

    final Action action = listener.onMessage(1L, TIMESTAMP, buffer, 0, len, null);

    assertEquals(Action.ABORT, action);
  }

  @Test
  void orderExpiredDecoder_positionedAfterOnMessage() {
    final int len = encodeOrderExpiredEvent(CL_ORD_ID);
    listener.onMessage(1L, TIMESTAMP, buffer, 0, len, null);

    final var dec = listener.orderExpiredDecoder();
    final byte[] scratch = new byte[OrderExpiredEventDecoder.clOrdIdLength()];
    dec.getClOrdId(scratch, 0);
    final String decoded =
        new String(
            scratch, 0, ClusterEgressListener.trimNullPadding(scratch), StandardCharsets.US_ASCII);
    assertEquals(CL_ORD_ID, decoded);
  }

  // ===========================================================================
  // SBE encoding helpers
  // ===========================================================================

  private int encodeExecutionReport(final String clOrdId) {
    final var enc = new ExecutionReportEncoder();
    enc.wrapAndApplyHeader(buffer, 0, headerEncoder);
    enc.clOrdId(clOrdId);
    enc.execType(ExecTypeEnum.New);
    enc.side(SideEnum.Buy);
    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  private int encodeOrderCancelReject(final String clOrdId) {
    final var enc = new OrderCancelRejectEncoder();
    enc.wrapAndApplyHeader(buffer, 0, headerEncoder);
    enc.clOrdId(clOrdId);
    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  private int encodeQuote(final String quoteReqId) {
    final var enc = new QuoteEncoder();
    enc.wrapAndApplyHeader(buffer, 0, headerEncoder);
    enc.quoteReqId(quoteReqId);
    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  private int encodeOrderCreatedEvent(final String clOrdId) {
    final var enc = new OrderCreatedEventEncoder();
    enc.wrapAndApplyHeader(buffer, 0, headerEncoder);
    enc.clOrdId(clOrdId);
    enc.side(SideEnum.Buy);
    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  private int encodeOrderRejectedEvent(final String clOrdId) {
    final var enc = new OrderRejectedEventEncoder();
    enc.wrapAndApplyHeader(buffer, 0, headerEncoder);
    enc.clOrdId(clOrdId);
    enc.side(SideEnum.Buy);
    enc.rejectReason(RejectReasonEnum.UnknownSymbol);
    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  /**
   * Encodes a minimal SBE {@code OrderExpiredEvent} (template 121, APP-62 §J) for egress dispatch
   * tests. Only the fields the egress listener reads (clOrdId for correlation, side for the
   * required required-field check) are populated; other fields default to NULL/zero.
   */
  private int encodeOrderExpiredEvent(final String clOrdId) {
    final var enc = new OrderExpiredEventEncoder();
    enc.wrapAndApplyHeader(buffer, 0, headerEncoder);
    enc.clOrdId(clOrdId);
    enc.side(SideEnum.Buy);
    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }
}
