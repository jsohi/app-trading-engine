package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.testsupport.clock.ControllableNanoClock;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ResourceLeakDetector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ConnectionRateLimiter} — verifies global and per-IP token bucket rate limiting
 * with refill after one-second intervals.
 *
 * <p>Uses tight limits ({@code perIpNewConnectionsPerSec=2, globalNewConnectionsPerSec=3}) and a
 * {@link ControllableNanoClock} to deterministically test bucket exhaustion and refill.
 */
final class ConnectionRateLimiterTest {

  private final java.util.List<EmbeddedChannel> openChannels = new java.util.ArrayList<>();
  private ControllableNanoClock clock;
  private ConnectionRateLimiter.RateLimiterState state;

  @BeforeAll
  static void setLeakDetection() {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
  }

  @BeforeEach
  void setUp() {
    clock = new ControllableNanoClock(1_000_000_000L);
    final var config =
        WebSocketServerConfig.builder()
            .perIpNewConnectionsPerSec(2)
            .globalNewConnectionsPerSec(3)
            .build();
    state = new ConnectionRateLimiter.RateLimiterState(config, clock);
  }

  @AfterEach
  void tearDown() {
    for (final var ch : openChannels) {
      ch.finishAndReleaseAll();
    }
    openChannels.clear();
  }

  private EmbeddedChannel trackChannel(final EmbeddedChannel ch) {
    openChannels.add(ch);
    return ch;
  }

  /** Create a new per-channel ConnectionRateLimiter backed by the shared state. */
  private ConnectionRateLimiter newLimiter() {
    return new ConnectionRateLimiter(state);
  }

  /**
   * Create a new per-channel ConnectionRateLimiter backed by a given state.
   *
   * @param sharedState the shared rate limiter state
   * @return a new handler instance
   */
  private static ConnectionRateLimiter newLimiter(
      final ConnectionRateLimiter.RateLimiterState sharedState) {
    return new ConnectionRateLimiter(sharedState);
  }

  @Test
  void channelActive_withinGlobalLimit_passesThrough() {
    final var channel = trackChannel(new EmbeddedChannel(newLimiter()));

    assertTrue(
        channel.isActive(),
        "Channel must remain active when connection is within the global rate limit");
  }

  @Test
  void channelActive_exceedsGlobalLimit_closesChannel() {
    // Global limit = 3. EmbeddedChannel.remoteAddress() returns null -> all share "unknown" IP,
    // but per-IP limit is 2, so the third connection hits global limit (3) before per-IP (2)
    // only if we consume tokens across multiple IPs. Since all channels share "unknown" IP and
    // per-IP limit is 2, the third connection is blocked by per-IP first.
    //
    // To test global limit specifically, we exhaust global tokens (3) with per-IP limit (2) by
    // making the per-IP limit higher. Reconstruct with higher per-IP.
    final var globalConfig =
        WebSocketServerConfig.builder()
            .perIpNewConnectionsPerSec(3)
            .globalNewConnectionsPerSec(3)
            .build();
    final var globalClock = new ControllableNanoClock(1_000_000_000L);
    final var globalState = new ConnectionRateLimiter.RateLimiterState(globalConfig, globalClock);

    // Consume all 3 global tokens (each channel gets its own handler instance)
    trackChannel(new EmbeddedChannel(newLimiter(globalState)));
    trackChannel(new EmbeddedChannel(newLimiter(globalState)));
    trackChannel(new EmbeddedChannel(newLimiter(globalState)));

    // Fourth channel should be closed
    final var excess = trackChannel(new EmbeddedChannel(newLimiter(globalState)));

    assertFalse(
        excess.isActive(),
        "Channel must be closed when the global connection rate limit is exceeded");
  }

  @Test
  void channelActive_afterOneSecondRefill_allowsNewConnections() {
    // Use a config where per-IP and global both allow 2 per second.
    final var refillConfig =
        WebSocketServerConfig.builder()
            .perIpNewConnectionsPerSec(2)
            .globalNewConnectionsPerSec(2)
            .build();
    final var refillClock = new ControllableNanoClock(1_000_000_000L);
    final var refillState = new ConnectionRateLimiter.RateLimiterState(refillConfig, refillClock);

    // Exhaust all 2 tokens
    trackChannel(new EmbeddedChannel(newLimiter(refillState)));
    trackChannel(new EmbeddedChannel(newLimiter(refillState)));

    // Third connection should be rejected (tokens exhausted)
    final var rejected = trackChannel(new EmbeddedChannel(newLimiter(refillState)));
    assertFalse(rejected.isActive(), "Channel must be closed when tokens are exhausted");

    // Advance clock by 1 second to trigger refill
    refillClock.advanceSeconds(1);

    // After refill, a new connection should succeed
    final var afterRefill = trackChannel(new EmbeddedChannel(newLimiter(refillState)));

    assertTrue(
        afterRefill.isActive(),
        "Channel must remain active after token bucket refill (1 second elapsed)");
  }
}
