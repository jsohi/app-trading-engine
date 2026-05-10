package com.trading.engine.fixbridge.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.fixbridge.audit.AuditAction;
import com.trading.engine.fixbridge.audit.AuditLogger;
import com.trading.engine.fixbridge.json.MutableParsedMessage;
import com.trading.engine.fixbridge.quote.SessionId;
import com.trading.engine.fixbridge.quote.SessionQuoteIndex;
import com.trading.engine.fixbridge.ratelimit.PerTypeRateLimiter;
import com.trading.engine.websocket.JwtValidator.ValidatedClaims;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link RoutingBridgeFrameDispatcher} — real routing of inbound bridge frames to
 * the correct {@link FixCommandSink} method.
 *
 * <p>Tests each message type (QUOTE_REQUEST, ACCEPT_QUOTE, REJECT_QUOTE, NEW_ORDER_SINGLE,
 * CANCEL_ORDER, ORDER_STATUS_REQUEST) and verifies the dispatcher:
 *
 * <ul>
 *   <li>Calls the right sink method exactly once.
 *   <li>Passes the same {@code parsed} and {@code nowNs} arguments.
 *   <li>Emits one audit entry with the correct {@link AuditAction}.
 *   <li>For QUOTE_REQUEST: also updates {@link SessionQuoteIndex}.
 * </ul>
 *
 * <p>TYPE_NONE and TYPE_AUTH are silently dropped (no sink call, no audit).
 *
 * <p><b>Threading.</b> Single-threaded — test-only.
 *
 * <p><b>Allocation.</b> Test-only — allocation is acceptable.
 */
final class RoutingBridgeFrameDispatcherTest {

  // ---------------------------------------------------------------------------
  // Recording collaborators.
  // ---------------------------------------------------------------------------

  private static final class RecordingFixCommandSink implements FixCommandSink {

    final List<String> calledMethods = new ArrayList<>();
    final List<MutableParsedMessage> parsedArgs = new ArrayList<>();
    final List<Long> nowNsArgs = new ArrayList<>();

    @Override
    public long sendQuoteRequest(final MutableParsedMessage parsed, final long nowNs) {
      record("sendQuoteRequest", parsed, nowNs);
      return 0L;
    }

    @Override
    public long sendAcceptQuote(final MutableParsedMessage parsed, final long nowNs) {
      record("sendAcceptQuote", parsed, nowNs);
      return 0L;
    }

    @Override
    public long handleRejectQuote(final MutableParsedMessage parsed, final long nowNs) {
      record("handleRejectQuote", parsed, nowNs);
      return FixCommandSink.NO_SEND;
    }

    @Override
    public long sendNewOrderSingle(final MutableParsedMessage parsed, final long nowNs) {
      record("sendNewOrderSingle", parsed, nowNs);
      return 0L;
    }

    @Override
    public long sendCancelOrder(final MutableParsedMessage parsed, final long nowNs) {
      record("sendCancelOrder", parsed, nowNs);
      return 0L;
    }

    @Override
    public long sendOrderStatusRequest(final MutableParsedMessage parsed, final long nowNs) {
      record("sendOrderStatusRequest", parsed, nowNs);
      return FixCommandSink.NO_SEND;
    }

    private void record(final String method, final MutableParsedMessage p, final long ns) {
      calledMethods.add(method);
      parsedArgs.add(p);
      nowNsArgs.add(ns);
    }
  }

  private static final class RecordingAuditLogger implements AuditLogger {

    final List<AuditAction> actions = new ArrayList<>();
    final List<Long> timestamps = new ArrayList<>();

    @Override
    public void record(
        final long tsNs,
        final String userId,
        final String jti,
        final String sourceIp,
        final AuditAction action,
        final String symbol,
        final String side,
        final String qtyStr,
        final String priceStr,
        final String ordType,
        final String tif,
        final String account,
        final String clOrdId,
        final String origClOrdId,
        final String quoteId,
        final String result,
        final String failureReason,
        final String traceparent) {
      actions.add(action);
      timestamps.add(tsNs);
    }

    @Override
    public boolean isWritable() {
      return true;
    }
  }

  // ---------------------------------------------------------------------------
  // Shared setup.
  // ---------------------------------------------------------------------------

  private RecordingFixCommandSink sink;
  private RecordingAuditLogger auditLogger;
  private SessionQuoteIndex quoteIndex;
  private RoutingBridgeFrameDispatcher dispatcher;
  private BridgeSession session;
  private MutableParsedMessage parsed;

  @BeforeEach
  void setUp() {
    sink = new RecordingFixCommandSink();
    auditLogger = new RecordingAuditLogger();
    quoteIndex = new SessionQuoteIndex();
    dispatcher = new RoutingBridgeFrameDispatcher(sink, quoteIndex, auditLogger, "192.168.1.1");

    // Minimal BridgeSession.
    final var claims =
        new ValidatedClaims("user-001", "jti-001", List.of(), Long.MAX_VALUE, true, List.of());
    session =
        new BridgeSession(
            new SessionId("sess-001"),
            claims,
            InetAddress.getLoopbackAddress(),
            new OutboundQueue(16),
            new PerTypeRateLimiter(0L));

    parsed = new MutableParsedMessage();
  }

  /** Populate a valid reqId slice into the parsed flyweight's scratch buffer. */
  private void setReqId(final String reqId) {
    final byte[] bytes = reqId.getBytes(StandardCharsets.UTF_8);
    System.arraycopy(bytes, 0, parsed.scratch, 0, bytes.length);
    parsed.reqIdOff = 0;
    parsed.reqIdLen = bytes.length;
  }

  // ---------------------------------------------------------------------------
  // QUOTE_REQUEST.
  // ---------------------------------------------------------------------------

  @Test
  void dispatch_quoteRequest_callsSendQuoteRequestExactlyOnce() {
    setReqId("R-001");
    parsed.type = MutableParsedMessage.TYPE_QUOTE_REQUEST;
    final long nowNs = 1_000_000L;

    dispatcher.dispatch(session, parsed, MutableParsedMessage.TYPE_QUOTE_REQUEST, nowNs);

    assertEquals(1, sink.calledMethods.size());
    assertEquals("sendQuoteRequest", sink.calledMethods.get(0));
    assertSame(parsed, sink.parsedArgs.get(0));
    assertEquals(nowNs, sink.nowNsArgs.get(0));
  }

  @Test
  void dispatch_quoteRequest_auditsQuoteRequestReceived() {
    setReqId("R-002");
    final long nowNs = 2_000_000L;

    dispatcher.dispatch(session, parsed, MutableParsedMessage.TYPE_QUOTE_REQUEST, nowNs);

    assertEquals(1, auditLogger.actions.size());
    assertEquals(AuditAction.QUOTE_REQUEST_RECEIVED, auditLogger.actions.get(0));
  }

  @Test
  void dispatch_quoteRequest_updatesSessionQuoteIndex() {
    setReqId("R-003");
    final long nowNs = 3_000_000L;

    assertEquals(0, quoteIndex.reqIdCount(), "Precondition: index must be empty");
    dispatcher.dispatch(session, parsed, MutableParsedMessage.TYPE_QUOTE_REQUEST, nowNs);

    assertTrue(quoteIndex.reqIdCount() > 0, "SessionQuoteIndex must have been updated");
  }

  // ---------------------------------------------------------------------------
  // ACCEPT_QUOTE.
  // ---------------------------------------------------------------------------

  @Test
  void dispatch_acceptQuote_callsSendAcceptQuoteExactlyOnce() {
    parsed.type = MutableParsedMessage.TYPE_ACCEPT_QUOTE;
    final long nowNs = 10_000_000L;

    dispatcher.dispatch(session, parsed, MutableParsedMessage.TYPE_ACCEPT_QUOTE, nowNs);

    assertEquals(1, sink.calledMethods.size());
    assertEquals("sendAcceptQuote", sink.calledMethods.get(0));
    assertSame(parsed, sink.parsedArgs.get(0));
    assertEquals(nowNs, sink.nowNsArgs.get(0));
  }

  @Test
  void dispatch_acceptQuote_auditsAcceptQuoteReceived() {
    final long nowNs = 11_000_000L;

    dispatcher.dispatch(session, parsed, MutableParsedMessage.TYPE_ACCEPT_QUOTE, nowNs);

    assertEquals(1, auditLogger.actions.size());
    assertEquals(AuditAction.ACCEPT_QUOTE_RECEIVED, auditLogger.actions.get(0));
  }

  // ---------------------------------------------------------------------------
  // REJECT_QUOTE.
  // ---------------------------------------------------------------------------

  @Test
  void dispatch_rejectQuote_callsHandleRejectQuoteExactlyOnce() {
    parsed.type = MutableParsedMessage.TYPE_REJECT_QUOTE;
    final long nowNs = 20_000_000L;

    dispatcher.dispatch(session, parsed, MutableParsedMessage.TYPE_REJECT_QUOTE, nowNs);

    assertEquals(1, sink.calledMethods.size());
    assertEquals("handleRejectQuote", sink.calledMethods.get(0));
    assertSame(parsed, sink.parsedArgs.get(0));
    assertEquals(nowNs, sink.nowNsArgs.get(0));
  }

  @Test
  void dispatch_rejectQuote_auditsRejectQuoteReceived() {
    final long nowNs = 21_000_000L;

    dispatcher.dispatch(session, parsed, MutableParsedMessage.TYPE_REJECT_QUOTE, nowNs);

    assertEquals(1, auditLogger.actions.size());
    assertEquals(AuditAction.REJECT_QUOTE_RECEIVED, auditLogger.actions.get(0));
  }

  // ---------------------------------------------------------------------------
  // NEW_ORDER_SINGLE.
  // ---------------------------------------------------------------------------

  @Test
  void dispatch_newOrderSingle_callsSendNewOrderSingleExactlyOnce() {
    parsed.type = MutableParsedMessage.TYPE_NEW_ORDER_SINGLE;
    final long nowNs = 30_000_000L;

    dispatcher.dispatch(session, parsed, MutableParsedMessage.TYPE_NEW_ORDER_SINGLE, nowNs);

    assertEquals(1, sink.calledMethods.size());
    assertEquals("sendNewOrderSingle", sink.calledMethods.get(0));
    assertSame(parsed, sink.parsedArgs.get(0));
    assertEquals(nowNs, sink.nowNsArgs.get(0));
  }

  @Test
  void dispatch_newOrderSingle_auditsNewOrderReceived() {
    final long nowNs = 31_000_000L;

    dispatcher.dispatch(session, parsed, MutableParsedMessage.TYPE_NEW_ORDER_SINGLE, nowNs);

    assertEquals(1, auditLogger.actions.size());
    assertEquals(AuditAction.NEW_ORDER_RECEIVED, auditLogger.actions.get(0));
  }

  // ---------------------------------------------------------------------------
  // CANCEL_ORDER.
  // ---------------------------------------------------------------------------

  @Test
  void dispatch_cancelOrder_callsSendCancelOrderExactlyOnce() {
    parsed.type = MutableParsedMessage.TYPE_CANCEL_ORDER;
    final long nowNs = 40_000_000L;

    dispatcher.dispatch(session, parsed, MutableParsedMessage.TYPE_CANCEL_ORDER, nowNs);

    assertEquals(1, sink.calledMethods.size());
    assertEquals("sendCancelOrder", sink.calledMethods.get(0));
    assertSame(parsed, sink.parsedArgs.get(0));
    assertEquals(nowNs, sink.nowNsArgs.get(0));
  }

  @Test
  void dispatch_cancelOrder_auditsCancelOrderReceived() {
    final long nowNs = 41_000_000L;

    dispatcher.dispatch(session, parsed, MutableParsedMessage.TYPE_CANCEL_ORDER, nowNs);

    assertEquals(1, auditLogger.actions.size());
    assertEquals(AuditAction.CANCEL_ORDER_RECEIVED, auditLogger.actions.get(0));
  }

  // ---------------------------------------------------------------------------
  // ORDER_STATUS_REQUEST.
  // ---------------------------------------------------------------------------

  @Test
  void dispatch_orderStatusRequest_callsSendOrderStatusRequestExactlyOnce() {
    parsed.type = MutableParsedMessage.TYPE_ORDER_STATUS_REQUEST;
    final long nowNs = 50_000_000L;

    dispatcher.dispatch(session, parsed, MutableParsedMessage.TYPE_ORDER_STATUS_REQUEST, nowNs);

    assertEquals(1, sink.calledMethods.size());
    assertEquals("sendOrderStatusRequest", sink.calledMethods.get(0));
    assertSame(parsed, sink.parsedArgs.get(0));
    assertEquals(nowNs, sink.nowNsArgs.get(0));
  }

  @Test
  void dispatch_orderStatusRequest_auditsOrderStatusRequest() {
    final long nowNs = 51_000_000L;

    dispatcher.dispatch(session, parsed, MutableParsedMessage.TYPE_ORDER_STATUS_REQUEST, nowNs);

    assertEquals(1, auditLogger.actions.size());
    assertEquals(AuditAction.ORDER_STATUS_REQUEST, auditLogger.actions.get(0));
  }

  // ---------------------------------------------------------------------------
  // Silent-drop cases: TYPE_NONE and TYPE_AUTH.
  // ---------------------------------------------------------------------------

  @Test
  void dispatch_typeNone_silentlyDropped_noSinkCall_noAudit() {
    dispatcher.dispatch(session, parsed, MutableParsedMessage.TYPE_NONE, 1L);

    assertTrue(sink.calledMethods.isEmpty(), "TYPE_NONE must not invoke the sink");
    assertTrue(auditLogger.actions.isEmpty(), "TYPE_NONE must not emit an audit entry");
  }

  @Test
  void dispatch_typeAuth_silentlyDropped_noSinkCall_noAudit() {
    dispatcher.dispatch(session, parsed, MutableParsedMessage.TYPE_AUTH, 1L);

    assertTrue(sink.calledMethods.isEmpty(), "TYPE_AUTH must not invoke the sink");
    assertTrue(auditLogger.actions.isEmpty(), "TYPE_AUTH must not emit an audit entry");
  }

  // ---------------------------------------------------------------------------
  // Audit timing: the recorded timestamp matches nowNs.
  // ---------------------------------------------------------------------------

  @Test
  void dispatch_newOrderSingle_auditTimestampMatchesNowNs() {
    final long nowNs = 999_123_456_789L;

    dispatcher.dispatch(session, parsed, MutableParsedMessage.TYPE_NEW_ORDER_SINGLE, nowNs);

    // The AuditLogger receives nowNs as its tsNs argument.
    assertEquals(1, auditLogger.timestamps.size());
    assertEquals(nowNs, auditLogger.timestamps.get(0));
  }

  // ---------------------------------------------------------------------------
  // Non-null constructor guard.
  // ---------------------------------------------------------------------------

  @Test
  void constructor_nullSink_throwsNullPointerException() {
    try {
      new RoutingBridgeFrameDispatcher(null, quoteIndex, auditLogger, "127.0.0.1");
      throw new AssertionError("Expected NullPointerException for null sink");
    } catch (final NullPointerException e) {
      // expected
    }
  }

  @Test
  void constructor_nullQuoteIndex_throwsNullPointerException() {
    try {
      new RoutingBridgeFrameDispatcher(sink, null, auditLogger, "127.0.0.1");
      throw new AssertionError("Expected NullPointerException for null quoteIndex");
    } catch (final NullPointerException e) {
      // expected
    }
  }

  @Test
  void constructor_nullAuditLogger_throwsNullPointerException() {
    try {
      new RoutingBridgeFrameDispatcher(sink, quoteIndex, null, "127.0.0.1");
      throw new AssertionError("Expected NullPointerException for null auditLogger");
    } catch (final NullPointerException e) {
      // expected
    }
  }

  @Test
  void constructor_nullRemoteIp_throwsNullPointerException() {
    try {
      new RoutingBridgeFrameDispatcher(sink, quoteIndex, auditLogger, null);
      throw new AssertionError("Expected NullPointerException for null remoteIp");
    } catch (final NullPointerException e) {
      // expected
    }
  }
}
