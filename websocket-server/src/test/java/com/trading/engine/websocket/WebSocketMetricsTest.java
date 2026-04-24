package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link WebSocketMetrics} — verifies gauge increments, counter increments, timer
 * registration, and null-safety of the constructor.
 *
 * <p>Uses {@link SimpleMeterRegistry} (in-memory) so tests run without Prometheus dependencies.
 */
final class WebSocketMetricsTest {

  private SimpleMeterRegistry registry;
  private WebSocketMetrics metrics;

  @BeforeEach
  void setUp() {
    registry = new SimpleMeterRegistry();
    metrics = new WebSocketMetrics(registry);
  }

  @Test
  void connectionOpened_singleCall_incrementsGauge() {
    metrics.connectionOpened();

    assertEquals(
        1.0,
        registry.get("websocket.connections.active").gauge().value(),
        "Active connections gauge should be 1 after a single connectionOpened call");
  }

  @Test
  void connectionClosed_afterOpen_decrementsGauge() {
    metrics.connectionOpened();
    metrics.connectionClosed();

    assertEquals(
        0.0,
        registry.get("websocket.connections.active").gauge().value(),
        "Active connections gauge should be 0 after open then close");
  }

  @Test
  void connectionClosed_whenZero_doesNotGoNegative() {
    metrics.connectionClosed();

    assertEquals(
        0.0,
        registry.get("websocket.connections.active").gauge().value(),
        "Active connections gauge must not go negative");
  }

  @Test
  void updateQueueDepth_positiveValue_updatesGauge() {
    metrics.updateQueueDepth(42);

    assertEquals(
        42.0,
        registry.get("websocket.queue.depth").gauge().value(),
        "Queue depth gauge should reflect the set value");
  }

  @Test
  void updateMaxClientLag_positiveValue_updatesGauge() {
    metrics.updateMaxClientLag(128);

    assertEquals(
        128.0,
        registry.get("websocket.client.lag").gauge().value(),
        "Client lag gauge should reflect the set value");
  }

  @Test
  void messageDropped_singleCall_incrementsCounter() {
    metrics.messageDropped();

    assertEquals(
        1.0,
        registry.get("websocket.messages.dropped.backpressure").counter().count(),
        "Messages dropped counter should be 1 after a single call");
  }

  @Test
  void authSucceeded_singleCall_incrementsCounter() {
    metrics.authSucceeded();

    assertEquals(
        1.0,
        registry.get("websocket.auth.success").counter().count(),
        "Auth success counter should be 1 after a single call");
  }

  @Test
  void authFailed_singleCall_incrementsCounter() {
    metrics.authFailed();

    assertEquals(
        1.0,
        registry.get("websocket.auth.failure").counter().count(),
        "Auth failure counter should be 1 after a single call");
  }

  @Test
  void commandRateLimited_singleCall_incrementsCounter() {
    metrics.commandRateLimited();

    assertEquals(
        1.0,
        registry.get("websocket.rate.limited").counter().count(),
        "Rate limited counter should be 1 after a single call");
  }

  @Test
  void replayEviction_singleCall_incrementsCounter() {
    metrics.replayEviction();

    assertEquals(
        1.0,
        registry.get("websocket.replay.eviction").counter().count(),
        "Replay eviction counter should be 1 after a single call");
  }

  @Test
  void aeronPollLatency_returnsNonNullTimer() {
    assertNotNull(
        metrics.aeronPollLatency(), "aeronPollLatency() must return a non-null Timer instance");
  }

  @Test
  void constructor_nullRegistry_throwsNullPointerException() {
    assertThrows(NullPointerException.class, () -> new WebSocketMetrics(null));
  }
}
