package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.testsupport.clock.ControllableNanoClock;
import io.netty.channel.DefaultChannelId;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ResourceLeakDetector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link WebSocketSessionManager} — verifies session registration, global/per-IP/per-user
 * capacity enforcement, session removal, and lookup.
 *
 * <p>Uses tight limits ({@code maxConcurrentSessions=2, maxConnectionsPerIp=1,
 * maxConnectionsPerUser=1}) to exercise capacity enforcement in a small number of operations.
 *
 * <p><b>EmbeddedChannel note.</b> Default {@link EmbeddedChannel} instances share the same internal
 * {@code EmbeddedChannelId} with identical {@code hashCode()} (0), causing key collisions in the
 * session manager's {@code Long2ObjectHashMap}. Tests that require distinct channels must use the
 * {@code EmbeddedChannel(ChannelId, ChannelHandler...)} constructor with {@link
 * DefaultChannelId#newInstance()} to get unique IDs. Similarly, {@code remoteAddress()} returns an
 * {@code EmbeddedSocketAddress} whose {@code toString()} is {@code "embedded"} for all channels, so
 * all channels share the same per-IP bucket.
 */
final class WebSocketSessionManagerTest {

  private final java.util.List<EmbeddedChannel> openChannels = new java.util.ArrayList<>();
  private WebSocketServerConfig config;
  private WebSocketMetrics metrics;
  private ControllableNanoClock clock;
  private WebSocketSessionManager manager;

  @BeforeAll
  static void setLeakDetection() {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
  }

  @BeforeEach
  void setUp() {
    config =
        WebSocketServerConfig.builder()
            .maxConcurrentSessions(2)
            .maxConnectionsPerIp(1)
            .maxConnectionsPerUser(1)
            .build();
    metrics = WebSocketMetrics.createWithDefaults();
    clock = new ControllableNanoClock(1_000_000_000L);
    manager = new WebSocketSessionManager(config, metrics, clock);
  }

  @AfterEach
  void tearDown() {
    for (final var ch : openChannels) {
      ch.finishAndReleaseAll();
    }
    openChannels.clear();
  }

  /**
   * Create an {@link EmbeddedChannel} with a unique {@link DefaultChannelId} so that each channel
   * has a distinct {@code channel.id().hashCode()} for the session manager's keyed map.
   *
   * @return a new EmbeddedChannel with a globally unique channel ID
   */
  private EmbeddedChannel newUniqueChannel() {
    final var ch = new EmbeddedChannel(DefaultChannelId.newInstance());
    openChannels.add(ch);
    return ch;
  }

  @Test
  void tryRegister_firstSession_returnsNonNull() {
    final var channel = newUniqueChannel();

    final var session = manager.tryRegister(channel);

    assertNotNull(session, "First registration must succeed and return a non-null session");
    assertEquals(
        1, manager.activeSessionCount(), "Active session count must be 1 after first registration");
  }

  @Test
  void tryRegister_exceedsGlobalLimit_returnsNull() {
    // EmbeddedChannel.remoteAddress() returns EmbeddedSocketAddress("embedded") for all channels,
    // so all share the same per-IP bucket. Raise per-IP limit so the global limit is hit first.
    final var globalConfig =
        WebSocketServerConfig.builder()
            .maxConcurrentSessions(2)
            .maxConnectionsPerIp(10)
            .maxConnectionsPerUser(10)
            .build();
    final var globalManager = new WebSocketSessionManager(globalConfig, metrics, clock);

    final var channel1 = newUniqueChannel();
    final var channel2 = newUniqueChannel();
    final var channel3 = newUniqueChannel();

    assertNotNull(globalManager.tryRegister(channel1), "First registration must succeed");
    assertNotNull(globalManager.tryRegister(channel2), "Second registration must succeed");

    final var third = globalManager.tryRegister(channel3);

    assertNull(third, "Third registration must return null when global limit (2) is reached");
    assertEquals(
        2,
        globalManager.activeSessionCount(),
        "Active session count must remain at the global limit");
  }

  @Test
  void tryRegister_exceedsPerIpLimit_returnsNull() {
    // Per-IP limit is 1; all EmbeddedChannels resolve to the same "embedded" IP bucket.
    // Raise global limit so per-IP is hit first.
    final var ipConfig =
        WebSocketServerConfig.builder()
            .maxConcurrentSessions(10)
            .maxConnectionsPerIp(1)
            .maxConnectionsPerUser(10)
            .build();
    final var ipManager = new WebSocketSessionManager(ipConfig, metrics, clock);

    final var channel1 = newUniqueChannel();
    final var channel2 = newUniqueChannel();

    assertNotNull(ipManager.tryRegister(channel1), "First registration from same IP must succeed");

    final var second = ipManager.tryRegister(channel2);

    assertNull(second, "Second registration from same IP must return null (per-IP limit=1)");
  }

  @Test
  void setUserId_exceedsPerUserLimit_returnsFalse() {
    // Raise per-IP limit so both sessions can register from the shared "embedded" IP.
    final var userConfig =
        WebSocketServerConfig.builder()
            .maxConcurrentSessions(10)
            .maxConnectionsPerIp(10)
            .maxConnectionsPerUser(1)
            .build();
    final var userManager = new WebSocketSessionManager(userConfig, metrics, clock);

    final var channel1 = newUniqueChannel();
    final var channel2 = newUniqueChannel();
    final var session1 = userManager.tryRegister(channel1);
    final var session2 = userManager.tryRegister(channel2);

    assertNotNull(session1);
    assertNotNull(session2);

    final boolean firstSet = userManager.setUserId(session1, "trader-1");
    final boolean secondSet = userManager.setUserId(session2, "trader-1");

    assertTrue(firstSet, "First setUserId for a user must succeed");
    assertFalse(secondSet, "Second setUserId for same user must fail (per-user limit=1)");
  }

  @Test
  void removeSession_existingSession_decrementsCount() {
    final var channel = newUniqueChannel();
    final var session = manager.tryRegister(channel);

    assertNotNull(session);
    assertEquals(1, manager.activeSessionCount());

    manager.removeSession(channel);

    assertEquals(
        0,
        manager.activeSessionCount(),
        "Active session count must be 0 after removing the only session");
  }

  @Test
  void findSession_existingChannel_returnsSession() {
    final var channel = newUniqueChannel();
    final var registered = manager.tryRegister(channel);

    final var found = manager.findSession(channel);

    assertNotNull(found, "findSession must return the registered session");
    assertEquals(
        registered.sessionId(),
        found.sessionId(),
        "Found session must have the same session ID as the registered one");
  }

  @Test
  void findSession_unknownChannel_returnsNull() {
    final var unknownChannel = newUniqueChannel();

    final var found = manager.findSession(unknownChannel);

    assertNull(found, "findSession must return null for an unregistered channel");
  }

  @Test
  void activeSessionCount_afterRegistrations_returnsCorrectCount() {
    // Raise all limits so all 3 registrations succeed.
    final var countConfig =
        WebSocketServerConfig.builder()
            .maxConcurrentSessions(10)
            .maxConnectionsPerIp(10)
            .maxConnectionsPerUser(10)
            .build();
    final var countManager = new WebSocketSessionManager(countConfig, metrics, clock);

    assertEquals(0, countManager.activeSessionCount(), "Count must be 0 before any registration");

    final var ch1 = newUniqueChannel();
    final var ch2 = newUniqueChannel();
    final var ch3 = newUniqueChannel();

    countManager.tryRegister(ch1);
    countManager.tryRegister(ch2);
    countManager.tryRegister(ch3);

    assertEquals(
        3,
        countManager.activeSessionCount(),
        "Active session count must equal the number of registered sessions");
  }
}
