package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ResourceLeakDetector;
import java.util.Set;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link WebSocketSession} — verifies session identity, sequence numbering, heartbeat
 * tracking, disconnect grace period, and replay-in-progress flag.
 *
 * <p>Uses {@link EmbeddedChannel} as a lightweight Netty channel for the constructor.
 */
final class WebSocketSessionTest {

  @BeforeAll
  static void setLeakDetection() {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
  }

  @Test
  void constructor_newSession_generatesUniqueSessionId() {
    final var channel1 = new EmbeddedChannel();
    final var channel2 = new EmbeddedChannel();
    final long nowNs = 1_000_000_000L;

    final var session1 = new WebSocketSession(channel1, nowNs, "127.0.0.1");
    final var session2 = new WebSocketSession(channel2, nowNs, "127.0.0.2");

    assertNotNull(session1.sessionId(), "Session ID must not be null");
    assertNotNull(session2.sessionId(), "Session ID must not be null");
    assertNotEquals(
        session1.sessionId(), session2.sessionId(), "Two sessions must have distinct UUIDs");

    channel1.finishAndReleaseAll();
    channel2.finishAndReleaseAll();
  }

  @Test
  void nextReliableSeqNo_consecutiveCalls_incrementsMonotonically() {
    final var channel = new EmbeddedChannel();
    final var session = new WebSocketSession(channel, 0L, "127.0.0.1");

    final long first = session.nextReliableSeqNo();
    final long second = session.nextReliableSeqNo();
    final long third = session.nextReliableSeqNo();

    assertEquals(1L, first, "First sequence number must be 1");
    assertEquals(2L, second, "Second sequence number must be 2");
    assertEquals(3L, third, "Third sequence number must be 3");

    channel.finishAndReleaseAll();
  }

  @Test
  void userId_setAndGet_returnsSetValue() {
    final var channel = new EmbeddedChannel();
    final var session = new WebSocketSession(channel, 0L, "127.0.0.1");

    session.userId("trader-42");

    assertEquals("trader-42", session.userId(), "userId must return the value that was set");

    channel.finishAndReleaseAll();
  }

  @Test
  void jti_setAndGet_returnsSetValue() {
    final var channel = new EmbeddedChannel();
    final var session = new WebSocketSession(channel, 0L, "127.0.0.1");

    session.jti("test-jti-abc-123");

    assertEquals("test-jti-abc-123", session.jti(), "jti must return the value that was set");

    channel.finishAndReleaseAll();
  }

  @Test
  void updateHeartbeat_newTimestamp_updatesLastHeartbeat() {
    final var channel = new EmbeddedChannel();
    final long initialNs = 1_000_000_000L;
    final var session = new WebSocketSession(channel, initialNs, "127.0.0.1");

    assertEquals(
        initialNs,
        session.lastClientHeartbeatNs(),
        "Initial heartbeat must equal the constructor timestamp");

    final long updatedNs = 5_000_000_000L;
    session.updateHeartbeat(updatedNs);

    assertEquals(
        updatedNs, session.lastClientHeartbeatNs(), "Heartbeat must reflect the updated timestamp");

    channel.finishAndReleaseAll();
  }

  @Test
  void markDisconnected_setsDisconnectedAndGracePeriod() {
    final var channel = new EmbeddedChannel();
    final var session = new WebSocketSession(channel, 0L, "127.0.0.1");

    assertFalse(session.isDisconnected(), "New session must not be disconnected");
    assertEquals(0L, session.gracePeriodStartNs(), "Grace period must be 0 before disconnect");

    final long disconnectNs = 10_000_000_000L;
    session.markDisconnected(disconnectNs);

    assertTrue(session.isDisconnected(), "Session must be disconnected after markDisconnected");
    assertEquals(
        disconnectNs,
        session.gracePeriodStartNs(),
        "Grace period start must equal the disconnect timestamp");

    channel.finishAndReleaseAll();
  }

  @Test
  void initSubscriptionFilter_afterInit_returnsNonNullFilter() {
    final var channel = new EmbeddedChannel();
    final var session = new WebSocketSession(channel, 0L, "127.0.0.1");

    assertNull(session.subscriptionFilter(), "Filter must be null before init");
    session.initSubscriptionFilter(100, WebSocketMetrics.createWithDefaults());
    assertNotNull(session.subscriptionFilter(), "Filter must be non-null after init");
    assertEquals(0, session.subscriptionFilter().subscriptionCount());

    channel.finishAndReleaseAll();
  }

  @Test
  void entitledAccounts_setAndGet_returnsSetValue() {
    final var channel = new EmbeddedChannel();
    final var session = new WebSocketSession(channel, 0L, "127.0.0.1");

    assertEquals(Set.of(), session.entitledAccounts(), "Default must be empty set");
    session.entitledAccounts(Set.of("ACC1", "ACC2"));
    assertEquals(Set.of("ACC1", "ACC2"), session.entitledAccounts());

    channel.finishAndReleaseAll();
  }

  @Test
  void markDisconnected_withActiveSubscriptions_clearsSubscriptionFilterAndEntitlements() {
    final var channel = new EmbeddedChannel();
    final var session = new WebSocketSession(channel, 0L, "127.0.0.1");

    session.initSubscriptionFilter(100, WebSocketMetrics.createWithDefaults());
    session.subscriptionFilter().addSubscription(0x4555525553440000L, 0x01);
    session.entitledAccounts(Set.of("ACC1"));

    assertEquals(1, session.subscriptionFilter().subscriptionCount());
    assertFalse(session.entitledAccounts().isEmpty());

    session.markDisconnected(10_000_000_000L);

    assertEquals(0, session.subscriptionFilter().subscriptionCount());
    assertTrue(session.subscriptionFilter().isEmpty());
    assertTrue(
        session.entitledAccounts().isEmpty(), "Entitled accounts must be cleared on disconnect");

    channel.finishAndReleaseAll();
  }

  @Test
  void replayInProgress_setTrue_returnsTrue() {
    final var channel = new EmbeddedChannel();
    final var session = new WebSocketSession(channel, 0L, "127.0.0.1");

    assertFalse(session.isReplayInProgress(), "Replay must be false by default");

    session.replayInProgress(true);

    assertTrue(session.isReplayInProgress(), "Replay must be true after setting to true");

    channel.finishAndReleaseAll();
  }
}
