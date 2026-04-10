package com.trading.engine.gateway;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.trading.engine.fix.builder.HeaderEncoder;
import com.trading.engine.fix.builder.NewOrderSingleEncoder;
import com.trading.engine.fix.builder.OrderCancelRequestEncoder;
import com.trading.engine.fix.decoder_flyweight.NewOrderSingleDecoder;
import com.trading.engine.fix.decoder_flyweight.OrderCancelRequestDecoder;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import java.util.concurrent.TimeUnit;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.SystemNanoClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import uk.co.real_logic.artio.fields.DecimalFloat;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

/**
 * Unit tests for {@link FixSessionHandler}. Uses a DISCONNECTED {@link ClusterClient} (offer
 * returns NOT_CONNECTED) to exercise the reject path. Verifies message dispatch, draining mode,
 * unsupported message types, disconnect cleanup, and BusinessMessageReject emission.
 *
 * <p>Happy-path cluster offer testing (connected cluster, correlation registration, ABORT on
 * back-pressure) requires a connected ClusterClient and is covered by integration tests (APP-18).
 */
class FixSessionHandlerTest {

  private static final long SESSION_KEY = 42L;

  private FakeGatewaySession gatewaySession;
  private ClusterClient clusterClient;
  private FixToSbeTranslator translator;
  private SessionRegistry registry;
  private RejectEmitter rejectEmitter;
  private MutableAsciiBuffer asciiBuffer;
  private MutableDirectBuffer sbeBuffer;
  private boolean draining;
  private FixSessionHandler handler;

  @BeforeEach
  void setUp() {
    gatewaySession = new FakeGatewaySession(SESSION_KEY);
    translator = new FixToSbeTranslator();
    registry = new SessionRegistry(100, 10, 64);
    rejectEmitter = new RejectEmitter();
    asciiBuffer = new MutableAsciiBuffer(new byte[4096]);
    sbeBuffer = new ExpandableDirectByteBuffer(1024);
    draining = false;

    // Build a DISCONNECTED ClusterClient — offer() returns NOT_CONNECTED.
    final SbeToFixTranslator sbeToFixTranslator = new SbeToFixTranslator();
    final InFlightTracker inFlightTracker = new InFlightTracker(16, TimeUnit.SECONDS.toNanos(5));
    final ClusterEgressListener egressListener =
        new ClusterEgressListener(
            sbeToFixTranslator,
            registry,
            inFlightTracker,
            (sessionKey, templateId, timestamp) -> true);
    clusterClient =
        ClusterClient.builder()
            .aeronDirectoryName("/tmp/aeron-test-handler")
            .ingressEndpoints("0=localhost:20110,1=localhost:20111,2=localhost:20112")
            .egressChannel("aeron:udp")
            .egressListener(egressListener)
            .messageTimeoutNs(TimeUnit.SECONDS.toNanos(5))
            .keepAliveIntervalNs(TimeUnit.SECONDS.toNanos(1))
            .reconnectBaseDelayNs(TimeUnit.MILLISECONDS.toNanos(100))
            .reconnectMaxDelayNs(TimeUnit.SECONDS.toNanos(10))
            .maxReconnectAttempts(3)
            .errorHandler(e -> {})
            .nanoClock(SystemNanoClock.INSTANCE)
            .inFlightTracker(inFlightTracker)
            .build();

    // Register the session so it can be found by egress/disconnect
    registry.tryRegisterSession(SESSION_KEY, 100L, gatewaySession);

    handler =
        new FixSessionHandler(
            gatewaySession,
            clusterClient,
            translator,
            registry,
            rejectEmitter,
            asciiBuffer,
            sbeBuffer,
            () -> draining);
  }

  // ===========================================================================
  // Helper: encode a FIX message into the asciiBuffer, return wire length
  // ===========================================================================

  private int encodeNos(final String clOrdId) {
    final NewOrderSingleEncoder enc = new NewOrderSingleEncoder();
    final HeaderEncoder hdr = enc.header();
    hdr.senderCompID("CLIENT").targetCompID("EXCH").msgSeqNum(1);
    hdr.sendingTime("20260407-12:00:00".getBytes());
    enc.clOrdID(clOrdId);
    enc.instrument().symbol("EURUSD");
    enc.side('1');
    enc.transactTime("20260407-12:00:00".getBytes());
    enc.ordType('2');
    enc.price(new DecimalFloat(150_25L, 2));
    enc.orderQtyData().orderQty(new DecimalFloat(100L, 0));
    enc.account("ACME");
    enc.currency("USD");

    final MutableAsciiBuffer wire = new MutableAsciiBuffer(new byte[2048]);
    final long encodedResult = enc.encode(wire, 0);
    final int wireOffset = (int) (encodedResult >>> 32);
    final int wireLen = (int) encodedResult;
    asciiBuffer.wrap(wire, wireOffset, wireLen);
    return wireLen;
  }

  private int encodeCancelRequest(final String clOrdId, final String origClOrdId) {
    final OrderCancelRequestEncoder enc = new OrderCancelRequestEncoder();
    final HeaderEncoder hdr = enc.header();
    hdr.senderCompID("CLIENT").targetCompID("EXCH").msgSeqNum(2);
    hdr.sendingTime("20260407-12:00:00".getBytes());
    enc.clOrdID(clOrdId);
    enc.origClOrdID(origClOrdId);
    enc.instrument().symbol("EURUSD");
    enc.side('1');
    enc.transactTime("20260407-12:00:00".getBytes());

    final MutableAsciiBuffer wire = new MutableAsciiBuffer(new byte[2048]);
    final long encodedResult = enc.encode(wire, 0);
    final int wireOffset = (int) (encodedResult >>> 32);
    final int wireLen = (int) encodedResult;
    asciiBuffer.wrap(wire, wireOffset, wireLen);
    return wireLen;
  }

  private Action dispatch(final long messageType, final int wireLen) {
    return handler.onMessage(asciiBuffer, 0, wireLen, 1, null, 0, messageType, 0L, 0L, null);
  }

  // ===========================================================================
  // NewOrderSingle (35=D) — cluster unavailable → BusinessMessageReject
  // ===========================================================================

  @Test
  void nosClusterUnavailableSendsReject() {
    final int wireLen = encodeNos("ORD-001");
    final Action action = dispatch(NewOrderSingleDecoder.MESSAGE_TYPE, wireLen);

    assertEquals(CONTINUE, action);
    // Cluster is DISCONNECTED, so offer fails → BusinessMessageReject sent
    assertEquals(1, gatewaySession.sentEncoders.size());
  }

  // ===========================================================================
  // OrderCancelRequest (35=F) — cluster unavailable → BusinessMessageReject
  // ===========================================================================

  @Test
  void cancelRequestClusterUnavailableSendsReject() {
    final int wireLen = encodeCancelRequest("CXL-001", "ORD-001");
    final Action action = dispatch(OrderCancelRequestDecoder.MESSAGE_TYPE, wireLen);

    assertEquals(CONTINUE, action);
    assertEquals(1, gatewaySession.sentEncoders.size());
  }

  // ===========================================================================
  // Unsupported message type → BusinessMessageReject
  // ===========================================================================

  @Test
  void unsupportedMessageTypeSendsReject() {
    final int wireLen = encodeNos("ORD-002");
    // Use a bogus message type that doesn't match any handler
    final Action action = dispatch(99999L, wireLen);

    assertEquals(CONTINUE, action);
    assertEquals(1, gatewaySession.sentEncoders.size());
  }

  // ===========================================================================
  // Draining mode → BusinessMessageReject with "System shutting down"
  // ===========================================================================

  @Test
  void drainingModeSendsRejectWithoutForwarding() {
    draining = true;
    final int wireLen = encodeNos("ORD-003");
    final Action action = dispatch(NewOrderSingleDecoder.MESSAGE_TYPE, wireLen);

    assertEquals(CONTINUE, action);
    // Reject was sent (draining), not forwarded to cluster
    assertEquals(1, gatewaySession.sentEncoders.size());
  }

  // ===========================================================================
  // onDisconnect removes session from registry
  // ===========================================================================

  @Test
  void onDisconnectRemovesSessionFromRegistry() {
    assertEquals(1, registry.sessionCount());

    handler.onDisconnect(
        1, null, uk.co.real_logic.artio.messages.DisconnectReason.REMOTE_DISCONNECT);

    assertEquals(0, registry.sessionCount());
    assertNull(registry.findSession(SESSION_KEY));
  }

  // Note: onSessionStart, onTimeout, onSlowStatus are logging-only callbacks that require
  // an Artio Session object (concrete class, no public constructor). Tested in integration
  // (APP-18).
}
