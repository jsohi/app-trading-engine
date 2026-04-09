package com.trading.engine.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.Publication;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.NanoClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ClusterClient}. These test the state machine, reconnection logic, and
 * in-flight tracking without requiring a live Aeron cluster. Connection is tested by verifying
 * state transitions; actual AeronCluster connectivity is covered by integration tests (APP-16).
 */
class ClusterClientTest {

  private long currentNanos = 1_000_000_000L;
  private final NanoClock clock = () -> currentNanos;
  private final AtomicReference<Throwable> lastError = new AtomicReference<>();
  private final ErrorHandler errorHandler = lastError::set;

  private final SbeToFixTranslator translator = new SbeToFixTranslator();
  private final InFlightTracker inFlightTracker =
      new InFlightTracker(16, TimeUnit.SECONDS.toNanos(5));
  private final SessionLookup sessionLookup =
      (clOrdId, offset, length) -> SessionLookup.NULL_SESSION;

  private ClusterEgressListener egressListener;

  @BeforeEach
  void setUp() {
    egressListener =
        new ClusterEgressListener(
            translator,
            sessionLookup,
            inFlightTracker,
            (sessionKey, templateId, timestamp) -> true);
  }

  private ClusterClient.Builder defaultBuilder() {
    return ClusterClient.builder()
        .aeronDirectoryName("/tmp/aeron-test")
        .ingressEndpoints("0=localhost:20110,1=localhost:20111,2=localhost:20112")
        .egressChannel("aeron:udp")
        .egressListener(egressListener)
        .messageTimeoutNs(TimeUnit.SECONDS.toNanos(5))
        .keepAliveIntervalNs(TimeUnit.SECONDS.toNanos(1))
        .reconnectBaseDelayNs(TimeUnit.MILLISECONDS.toNanos(100))
        .reconnectMaxDelayNs(TimeUnit.SECONDS.toNanos(10))
        .maxReconnectAttempts(3)
        .errorHandler(errorHandler)
        .nanoClock(clock)
        .inFlightTracker(inFlightTracker);
  }

  // ===========================================================================
  // Builder validation
  // ===========================================================================

  @Test
  void builder_missingAeronDirectoryName_throws() {
    assertThrows(
        NullPointerException.class, () -> defaultBuilder().aeronDirectoryName(null).build());
  }

  @Test
  void builder_missingIngressEndpoints_throws() {
    assertThrows(NullPointerException.class, () -> defaultBuilder().ingressEndpoints(null).build());
  }

  @Test
  void builder_missingEgressListener_throws() {
    assertThrows(NullPointerException.class, () -> defaultBuilder().egressListener(null).build());
  }

  @Test
  void builder_missingErrorHandler_throws() {
    assertThrows(NullPointerException.class, () -> defaultBuilder().errorHandler(null).build());
  }

  @Test
  void builder_missingInFlightTracker_throws() {
    assertThrows(NullPointerException.class, () -> defaultBuilder().inFlightTracker(null).build());
  }

  // ===========================================================================
  // Initial state
  // ===========================================================================

  @Test
  void newClient_isDisconnected() {
    final var client = defaultBuilder().build();
    assertFalse(client.isConnected());
    assertFalse(client.isClosed());
    assertEquals(-1L, client.clusterSessionId());
    assertEquals(-1, client.leaderMemberId());
  }

  // ===========================================================================
  // Offer when disconnected
  // ===========================================================================

  @Test
  void offer_disconnected_returnsNotConnected() {
    final var client = defaultBuilder().build();
    final var buf = new org.agrona.concurrent.UnsafeBuffer(new byte[64]);
    assertEquals(Publication.NOT_CONNECTED, client.offer(buf, 0, 8));
  }

  // ===========================================================================
  // Close
  // ===========================================================================

  @Test
  void close_idempotent() {
    final var client = defaultBuilder().build();
    client.close();
    assertTrue(client.isClosed());
    client.close(); // should not throw
    assertTrue(client.isClosed());
  }

  @Test
  void close_resetsInFlightTracker() {
    final var client = defaultBuilder().build();
    final byte[] id = "ORD-001".getBytes(StandardCharsets.US_ASCII);
    inFlightTracker.onCommandSent(id, 0, id.length, 1_000L);
    assertEquals(1, inFlightTracker.size());

    client.close();
    assertEquals(0, inFlightTracker.size());
  }

  @Test
  void offer_afterClose_returnsNotConnected() {
    final var client = defaultBuilder().build();
    client.close();
    final var buf = new org.agrona.concurrent.UnsafeBuffer(new byte[64]);
    assertEquals(Publication.NOT_CONNECTED, client.offer(buf, 0, 8));
  }

  // ===========================================================================
  // Reconnection state machine
  // ===========================================================================

  @Test
  void onStart_connectionFailure_schedulesReconnect() {
    // onStart() calls connect() which will fail (no real cluster).
    // The client should enter RECONNECTING state, not CLOSED.
    final var client = defaultBuilder().build();
    client.onStart();

    assertEquals(ClusterClient.State.RECONNECTING, client.state());
    assertFalse(client.isConnected());
  }

  @Test
  void doWork_reconnecting_waitsForBackoff() {
    final var client = defaultBuilder().build();
    client.onStart(); // fails, enters RECONNECTING

    // doWork before deadline should do nothing.
    final int work = client.doWork();
    assertEquals(0, work);
    assertEquals(ClusterClient.State.RECONNECTING, client.state());
  }

  @Test
  void doWork_reconnecting_attemptsAfterDeadline() {
    final var client =
        defaultBuilder().reconnectBaseDelayNs(TimeUnit.MILLISECONDS.toNanos(100)).build();
    client.onStart(); // fails, enters RECONNECTING

    // Advance clock past the backoff deadline.
    currentNanos += TimeUnit.MILLISECONDS.toNanos(200);
    client.doWork(); // attempts reconnect — fails again, schedules next

    // Should still be reconnecting (second attempt).
    assertEquals(ClusterClient.State.RECONNECTING, client.state());
  }

  @Test
  void reconnect_maxAttemptsExceeded_closes() {
    final var client =
        defaultBuilder()
            .maxReconnectAttempts(1)
            .reconnectBaseDelayNs(TimeUnit.MILLISECONDS.toNanos(10))
            .build();

    // onStart() → connect() fails → scheduleReconnect: attempts 0 → 1
    client.onStart();
    assertEquals(ClusterClient.State.RECONNECTING, client.state());

    // doWork() past deadline → connect() fails → scheduleReconnect: attempts 1 >= max(1) → CLOSED
    currentNanos += TimeUnit.MILLISECONDS.toNanos(100);
    client.doWork();

    assertTrue(client.isClosed());
    assertNotNull(lastError.get());
    assertTrue(lastError.get() instanceof IllegalStateException);
  }

  @Test
  void doWork_closed_returnsZero() {
    final var client = defaultBuilder().build();
    client.close();
    assertEquals(0, client.doWork());
  }

  // ===========================================================================
  // In-flight timeout integration
  // ===========================================================================

  @Test
  void inFlightTracker_expiresTimedOutEntries() {
    // Connected-state doWork() testing requires a live cluster (APP-16 integration tests).
    // Here we verify the InFlightTracker timeout mechanism independently.
    final byte[] id = "ORD-001".getBytes(StandardCharsets.US_ASCII);
    inFlightTracker.onCommandSent(id, 0, id.length, 1_000L);
    assertEquals(1, inFlightTracker.size());

    final AtomicInteger expired = new AtomicInteger();
    final long nowNs = 1_000L + TimeUnit.SECONDS.toNanos(5);
    inFlightTracker.checkTimeouts(nowNs, (hash, sentNs) -> expired.incrementAndGet());

    assertEquals(1, expired.get());
    assertEquals(0, inFlightTracker.size());
  }

  // ===========================================================================
  // Role name
  // ===========================================================================

  @Test
  void roleName_returnsClusterClient() {
    final var client = defaultBuilder().build();
    assertEquals("cluster-client", client.roleName());
  }
}
