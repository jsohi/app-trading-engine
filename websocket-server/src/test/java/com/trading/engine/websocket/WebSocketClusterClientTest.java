package com.trading.engine.websocket;

import static io.aeron.Publication.NOT_CONNECTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.testsupport.clock.ControllableNanoClock;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.logbuffer.Header;
import java.util.concurrent.TimeUnit;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link WebSocketClusterClient} — verifies builder validation, state machine
 * transitions, offer-when-disconnected behavior, and idempotent close.
 *
 * <p>No cluster connection is attempted. All tests operate on the builder and the constructed
 * client in its initial DISCONNECTED state. The {@code connect()} method requires a running Aeron
 * Media Driver and cluster, so only pre-connection logic is exercised here.
 */
final class WebSocketClusterClientTest {

  /** No-op egress listener for builder satisfaction. */
  private static final EgressListener NOOP_EGRESS_LISTENER =
      new EgressListener() {
        @Override
        public void onMessage(
            final long clusterSessionId,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header) {}

        @Override
        public void onSessionEvent(
            final long correlationId,
            final long clusterSessionId,
            final long leadershipTermId,
            final int leaderMemberId,
            final EventCode code,
            final String detail) {}

        @Override
        public void onNewLeader(
            final long clusterSessionId,
            final long leadershipTermId,
            final int leaderMemberId,
            final String ingressEndpoints) {}
      };

  private ControllableNanoClock clock;

  @BeforeEach
  void setUp() {
    clock = new ControllableNanoClock(1_000_000_000L);
  }

  /**
   * Build a fully configured {@link WebSocketClusterClient} with all required fields populated.
   *
   * @return a configured but unconnected cluster client
   */
  private WebSocketClusterClient buildValidClient() {
    return WebSocketClusterClient.builder()
        .aeronDirectoryName("/tmp/aeron-test")
        .ingressEndpoints("0=localhost:20000")
        .egressListener(NOOP_EGRESS_LISTENER)
        .errorHandler(Throwable::printStackTrace)
        .nanoClock(clock)
        .build();
  }

  @Test
  void builder_allRequiredFields_buildsSuccessfully() {
    final var client = buildValidClient();

    // Construction succeeded — verify the client is in the initial DISCONNECTED state
    assertEquals(WebSocketClusterClient.State.DISCONNECTED, client.state());

    client.close();
  }

  @Test
  void builder_missingAeronDir_throwsNullPointerException() {
    assertThrows(
        NullPointerException.class,
        () ->
            WebSocketClusterClient.builder()
                .ingressEndpoints("0=localhost:20000")
                .egressListener(NOOP_EGRESS_LISTENER)
                .errorHandler(Throwable::printStackTrace)
                .nanoClock(clock)
                .build());
  }

  @Test
  void builder_missingIngressEndpoints_throwsNullPointerException() {
    assertThrows(
        NullPointerException.class,
        () ->
            WebSocketClusterClient.builder()
                .aeronDirectoryName("/tmp/aeron-test")
                .egressListener(NOOP_EGRESS_LISTENER)
                .errorHandler(Throwable::printStackTrace)
                .nanoClock(clock)
                .build());
  }

  @Test
  void builder_missingEgressListener_throwsNullPointerException() {
    assertThrows(
        NullPointerException.class,
        () ->
            WebSocketClusterClient.builder()
                .aeronDirectoryName("/tmp/aeron-test")
                .ingressEndpoints("0=localhost:20000")
                .errorHandler(Throwable::printStackTrace)
                .nanoClock(clock)
                .build());
  }

  @Test
  void builder_missingErrorHandler_throwsNullPointerException() {
    assertThrows(
        NullPointerException.class,
        () ->
            WebSocketClusterClient.builder()
                .aeronDirectoryName("/tmp/aeron-test")
                .ingressEndpoints("0=localhost:20000")
                .egressListener(NOOP_EGRESS_LISTENER)
                .nanoClock(clock)
                .build());
  }

  @Test
  void builder_invalidReconnectDelay_throwsIllegalArgument() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            WebSocketClusterClient.builder()
                .aeronDirectoryName("/tmp/aeron-test")
                .ingressEndpoints("0=localhost:20000")
                .egressListener(NOOP_EGRESS_LISTENER)
                .errorHandler(Throwable::printStackTrace)
                .nanoClock(clock)
                .reconnectBaseDelayNs(0)
                .build());
  }

  @Test
  void builder_maxDelayLessThanBase_throwsIllegalArgument() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            WebSocketClusterClient.builder()
                .aeronDirectoryName("/tmp/aeron-test")
                .ingressEndpoints("0=localhost:20000")
                .egressListener(NOOP_EGRESS_LISTENER)
                .errorHandler(Throwable::printStackTrace)
                .nanoClock(clock)
                .reconnectBaseDelayNs(TimeUnit.SECONDS.toNanos(5))
                .reconnectMaxDelayNs(TimeUnit.SECONDS.toNanos(1))
                .build());
  }

  @Test
  void state_afterConstruction_isDisconnected() {
    final var client = buildValidClient();

    assertEquals(
        WebSocketClusterClient.State.DISCONNECTED,
        client.state(),
        "Freshly built client must be in DISCONNECTED state");

    client.close();
  }

  @Test
  void isConnected_whenDisconnected_returnsFalse() {
    final var client = buildValidClient();

    assertFalse(client.isConnected(), "Unconnected client must return false for isConnected()");

    client.close();
  }

  @Test
  void isClosed_whenDisconnected_returnsFalse() {
    final var client = buildValidClient();

    assertFalse(client.isClosed(), "Unclosed client must return false for isClosed()");

    client.close();
  }

  @Test
  void offer_whenDisconnected_returnsNotConnected() {
    final var client = buildValidClient();
    final var buffer = new ExpandableArrayBuffer(64);
    buffer.putByte(0, (byte) 0x01);

    final long result = client.offer(buffer, 0, 16);

    assertEquals(
        NOT_CONNECTED,
        result,
        "offer() on a disconnected client must return Publication.NOT_CONNECTED");

    client.close();
  }

  @Test
  void close_whenDisconnected_transitionsToClosed() {
    final var client = buildValidClient();

    client.close();

    assertEquals(
        WebSocketClusterClient.State.CLOSED,
        client.state(),
        "State must transition to CLOSED after close()");
    assertTrue(client.isClosed(), "isClosed() must return true after close()");
  }

  @Test
  void close_calledTwice_isIdempotent() {
    final var client = buildValidClient();

    client.close();
    client.close(); // second close must not throw

    assertEquals(
        WebSocketClusterClient.State.CLOSED,
        client.state(),
        "State must remain CLOSED after second close()");
    assertTrue(client.isClosed(), "isClosed() must remain true after second close()");
  }

  @Test
  void roleName_returnsWsClusterClient() {
    final var client = buildValidClient();

    assertEquals(
        "ws-cluster-client",
        client.roleName(),
        "roleName() must return the expected agent role name");

    client.close();
  }
}
