package com.trading.engine.gateway;

import static com.trading.engine.testsupport.buffer.SbeFieldUtil.zeroPad;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.ExecTypeEnum;
import com.trading.engine.messages.sbe.ExecutionReportDecoder;
import com.trading.engine.messages.sbe.ExecutionReportEncoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.OrdStatusEnum;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.testsupport.clock.ControllableNanoClock;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link FixGateway}'s egress callback ({@link FixGateway#onEgressMessage}) and
 * session management. Tests the response routing pipeline: cluster response → find session →
 * translate SBE → FIX → trySend.
 *
 * <p>Does NOT test Artio FixEngine/FixLibrary lifecycle (onStart/doWork/onClose) which requires a
 * live Artio engine. Those paths are covered by integration tests (APP-18).
 */
class FixGatewayTest {

  private static final long SESSION_KEY = 100L;
  private static final long TIMESTAMP = 1_700_000_000_000_000_000L;

  private final ControllableNanoClock clock = new ControllableNanoClock(1_000_000_000L);
  private SessionRegistry registry;
  private SbeToFixTranslator sbeToFixTranslator;
  private InFlightTracker inFlightTracker;
  private ClusterEgressListener egressListener;
  private FakeGatewaySession fakeSession;
  private FixGateway gateway;

  // SBE encoding helpers
  private final MutableDirectBuffer sbeBuffer = new ExpandableArrayBuffer(512);
  private final MessageHeaderEncoder sbeHeaderEncoder = new MessageHeaderEncoder();
  private final ExecutionReportEncoder sbeErEncoder = new ExecutionReportEncoder();

  @BeforeEach
  void setUp() {
    registry = new SessionRegistry(100, 10, 64);
    sbeToFixTranslator = new SbeToFixTranslator();
    inFlightTracker = new InFlightTracker(16, TimeUnit.SECONDS.toNanos(5));

    // Build a FixGateway for egress testing only (no Artio engine lifecycle)
    gateway =
        new FixGateway(
            "localhost",
            9880,
            "aeron:ipc",
            "/tmp/fix-logs-test",
            "GATEWAY",
            java.util.List.of("CLIENT"),
            registry,
            new FixToSbeTranslator(),
            new RejectEmitter(),
            inFlightTracker,
            clock);

    // Build the egress listener with gateway's callback
    egressListener =
        new ClusterEgressListener(
            sbeToFixTranslator, registry, inFlightTracker, gateway::onEgressMessage);

    // Deferred init with a dummy ClusterClient (won't be used for egress tests)
    final ClusterClient clusterClient =
        ClusterClient.builder()
            .aeronDirectoryName("/tmp/aeron-test-gw")
            .ingressEndpoints("0=localhost:20110,1=localhost:20111,2=localhost:20112")
            .egressChannel("aeron:udp")
            .egressListener(egressListener)
            .messageTimeoutNs(TimeUnit.SECONDS.toNanos(5))
            .keepAliveIntervalNs(TimeUnit.SECONDS.toNanos(1))
            .reconnectBaseDelayNs(TimeUnit.MILLISECONDS.toNanos(100))
            .reconnectMaxDelayNs(TimeUnit.SECONDS.toNanos(10))
            .maxReconnectAttempts(3)
            .errorHandler(e -> {})
            .nanoClock(clock)
            .inFlightTracker(inFlightTracker)
            .build();
    gateway.init(clusterClient, egressListener);

    // Register a fake session
    fakeSession = new FakeGatewaySession(SESSION_KEY);
    registry.tryRegisterSession(SESSION_KEY, 100L, fakeSession);
  }

  // ===========================================================================
  // Helpers: encode SBE messages for egress testing
  // ===========================================================================

  /**
   * Encode a minimal SBE ExecutionReport into the buffer and drive it through the egress listener
   * so that the SBE decoders are pre-wrapped when the callback fires.
   */
  private int encodeSbeExecutionReport(final String clOrdId) {
    sbeErEncoder.wrapAndApplyHeader(sbeBuffer, 0, sbeHeaderEncoder);
    sbeErEncoder.putOrderId(zeroPad("ORD-001", 20), 0);
    sbeErEncoder.putExecId(zeroPad("EXE-001", 20), 0);
    sbeErEncoder.putClOrdId(zeroPad(clOrdId, 20), 0);
    sbeErEncoder.execType(ExecTypeEnum.New);
    sbeErEncoder.ordStatus(OrdStatusEnum.New);
    sbeErEncoder.putSymbol(zeroPad("EURUSD", 8), 0);
    sbeErEncoder.side(SideEnum.Buy);
    sbeErEncoder.leavesQty(100_000_000L);
    sbeErEncoder.cumQty(0L);
    sbeErEncoder.avgPx(ExecutionReportEncoder.avgPxNullValue());
    sbeErEncoder.transactTime(TIMESTAMP);
    sbeErEncoder.putText(zeroPad("", 64), 0);
    sbeErEncoder.putCurrency(new byte[] {'U', 'S', 'D'}, 0);
    sbeErEncoder.putSettlCurrency(new byte[] {'U', 'S', 'D'}, 0);
    sbeErEncoder.noLegsCount(0);
    return MessageHeaderEncoder.ENCODED_LENGTH + sbeErEncoder.encodedLength();
  }

  // ===========================================================================
  // onEgressMessage — session found, trySend succeeds
  // ===========================================================================

  @Test
  void onEgressMessageDeliversExecutionReportToSession() {
    final String clOrdId = "ORD-EGRESS-001";
    final int sbeLen = encodeSbeExecutionReport(clOrdId);

    // Register correlation so the egress listener can find the session
    final byte[] clOrdIdBytes = clOrdId.getBytes(StandardCharsets.US_ASCII);
    registry.registerCorrelation(
        clOrdIdBytes, 0, clOrdIdBytes.length, SESSION_KEY, clock.nanoTime());
    inFlightTracker.onCommandSent(clOrdIdBytes, 0, clOrdIdBytes.length, clock.nanoTime());

    // Drive through egress listener (which calls gateway.onEgressMessage)
    final Action action = egressListener.onMessage(1L, TIMESTAMP, sbeBuffer, 0, sbeLen, null);

    assertEquals(Action.CONTINUE, action);
    // Session received the translated FIX ExecutionReport
    assertEquals(1, fakeSession.sentEncoders.size());
    assertTrue(
        fakeSession.sentEncoders.get(0)
            instanceof com.trading.engine.fix.builder.ExecutionReportEncoder);
    // Correlation cleaned up after successful delivery
    assertEquals(
        SessionLookup.NULL_SESSION,
        registry.findByCorrelationId(clOrdIdBytes, 0, clOrdIdBytes.length));
  }

  // ===========================================================================
  // onEgressMessage — session disconnected → ACK (true)
  // ===========================================================================

  @Test
  void onEgressMessageForDisconnectedSessionReturnsTrue() {
    // Remove the session to simulate disconnect
    registry.removeSession(SESSION_KEY);

    final boolean result =
        gateway.onEgressMessage(SESSION_KEY, ExecutionReportDecoder.TEMPLATE_ID, TIMESTAMP);

    assertTrue(result); // ACK — session is gone, clear in-flight
    assertEquals(0, fakeSession.sentEncoders.size()); // nothing sent
  }

  // ===========================================================================
  // onEgressMessage — session exists but not connected → remove + ACK
  // ===========================================================================

  @Test
  void onEgressMessageForNotConnectedSessionRemovesAndReturnsTrue() {
    fakeSession.setConnected(false);

    final boolean result =
        gateway.onEgressMessage(SESSION_KEY, ExecutionReportDecoder.TEMPLATE_ID, TIMESTAMP);

    assertTrue(result);
    assertNull(registry.findSession(SESSION_KEY)); // removed from registry
  }

  // ===========================================================================
  // onEgressMessage — unknown templateId → ACK (true)
  // ===========================================================================

  @Test
  void onEgressMessageForUnknownTemplateIdReturnsTrue() {
    final boolean result = gateway.onEgressMessage(SESSION_KEY, 9999, TIMESTAMP);

    assertTrue(result);
    assertEquals(0, fakeSession.sentEncoders.size());
  }

  // ===========================================================================
  // onEgressMessage — trySend back-pressured → returns false
  // ===========================================================================

  @Test
  void onEgressMessageReturnsFalseOnBackPressure() {
    fakeSession.setTrySendResult(-2L); // simulate back-pressure

    final String clOrdId = "ORD-BP-001";
    final int sbeLen = encodeSbeExecutionReport(clOrdId);

    // Register correlation
    final byte[] clOrdIdBytes = clOrdId.getBytes(StandardCharsets.US_ASCII);
    registry.registerCorrelation(
        clOrdIdBytes, 0, clOrdIdBytes.length, SESSION_KEY, clock.nanoTime());

    // Drive through egress listener
    final Action action = egressListener.onMessage(1L, TIMESTAMP, sbeBuffer, 0, sbeLen, null);

    // Back-pressure → ABORT (message will be re-delivered)
    assertEquals(Action.ABORT, action);
    // Correlation NOT removed (will be cleaned on successful re-delivery)
    assertEquals(SESSION_KEY, registry.findByCorrelationId(clOrdIdBytes, 0, clOrdIdBytes.length));
  }

  // ===========================================================================
  // Session capacity enforcement
  // ===========================================================================

  @Test
  void registryEnforcesGlobalSessionLimit() {
    // Registry was created with maxSessions=100, already has 1 session
    // Fill to capacity
    for (int i = 1; i < 100; i++) {
      assertTrue(
          registry.tryRegisterSession(1000L + i, (long) i, new FakeGatewaySession(1000L + i)));
    }
    assertEquals(100, registry.sessionCount());

    // 101st should fail
    assertFalse(registry.tryRegisterSession(9999L, 9999L, new FakeGatewaySession(9999L)));
  }

  @Test
  void registryEnforcesPerCompIdLimit() {
    // Registry maxSessionsPerCompId=10, same compIdHash for all
    final long compIdHash = 42L;
    for (int i = 1; i <= 9; i++) {
      assertTrue(
          registry.tryRegisterSession(2000L + i, compIdHash, new FakeGatewaySession(2000L + i)));
    }
    // 10th from same CompID should succeed (limit is 10)
    assertTrue(registry.tryRegisterSession(2010L, compIdHash, new FakeGatewaySession(2010L)));
    // 11th should fail
    assertFalse(registry.tryRegisterSession(2011L, compIdHash, new FakeGatewaySession(2011L)));
  }
}
