package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link TransportDetector} — verifies runtime transport detection produces valid event
 * loop groups and a known transport name.
 *
 * <p>Event loop groups are shut down in {@link #tearDown()} to prevent thread leaks.
 */
final class TransportDetectorTest {

  private TransportDetector.Result result;

  @AfterEach
  void tearDown() {
    if (result != null) {
      result.bossGroup().shutdownGracefully().syncUninterruptibly();
      result.workerGroup().shutdownGracefully().syncUninterruptibly();
    }
  }

  @Test
  void detect_onCurrentPlatform_returnsNonNullResult() {
    result = TransportDetector.detect();

    assertNotNull(result);
    assertNotNull(result.bossGroup());
    assertNotNull(result.workerGroup());
    assertNotNull(result.channelClass());
    assertNotNull(result.transportName());
  }

  @Test
  void detect_workerThreadCount_isAtLeastTwo() {
    result = TransportDetector.detect();

    // Count event loops by iterating (EventLoopGroup is Iterable<EventExecutor>)
    // TransportDetector guarantees max(2, availableProcessors / 2)
    int workerCount = 0;
    for (final var ignored : result.workerGroup()) {
      workerCount++;
    }
    assertTrue(workerCount >= 2, "Worker thread count must be >= 2, was: " + workerCount);
  }

  @Test
  void detect_transportName_isKnownValue() {
    result = TransportDetector.detect();

    final var knownTransports = Set.of("epoll", "kqueue", "nio");
    assertTrue(
        knownTransports.contains(result.transportName()),
        "Transport name must be one of " + knownTransports + ", was: " + result.transportName());
  }
}
