package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.testsupport.clock.ControllableNanoClock;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link SlowConsumerHandler}. */
final class SlowConsumerHandlerTest {

  private WebSocketServerConfig config;
  private WebSocketSessionManager sessionManager;
  private WebSocketMetrics metrics;
  private ControllableNanoClock clock;
  private SlowConsumerHandler handler;
  private EmbeddedChannel channel;
  private WebSocketSession session;
  private AtomicLong pendingBytes;

  @BeforeEach
  void setUp() {
    config =
        WebSocketServerConfig.builder()
            .slowConsumerLevel1Bytes(100)
            .slowConsumerLevel2Bytes(200)
            .slowConsumerLevel3Bytes(300)
            .slowConsumerLevel4Bytes(400)
            .writeBufferLowWaterMark(1)
            .writeBufferHighWaterMark(400)
            .slowConsumerDisconnectMs(1000)
            .build();
    metrics = WebSocketMetrics.createWithDefaults();
    clock = new ControllableNanoClock();
    sessionManager = new WebSocketSessionManager(config, metrics, clock);
    handler = new SlowConsumerHandler(sessionManager, config, metrics, clock);
    channel = new EmbeddedChannel();
    session = sessionManager.tryRegister(channel);
    pendingBytes = new AtomicLong(0);
    session.pendingBytesRef(pendingBytes);
  }

  @AfterEach
  void tearDown() {
    if (channel.isOpen()) {
      channel.close();
    }
    channel.finishAndReleaseAll();
  }

  @Test
  void scan_belowLevel1_recordsZero() {
    pendingBytes.set(50);
    handler.scan();
    assertEquals(0, session.lastLagLevel());
  }

  @Test
  void scan_atLevel1_recordsLevel1AndDoesNotDropBestEffort() {
    pendingBytes.set(150);
    handler.scan();
    assertEquals(1, session.lastLagLevel());
    assertFalse(session.isDropBestEffort());
  }

  @Test
  void scan_atLevel2_setsDropBestEffort() {
    pendingBytes.set(250);
    handler.scan();
    assertEquals(2, session.lastLagLevel());
    assertTrue(session.isDropBestEffort());
  }

  @Test
  void scan_returnToBelowLevel2_clearsDropBestEffort() {
    pendingBytes.set(250);
    handler.scan();
    assertTrue(session.isDropBestEffort());

    pendingBytes.set(50);
    handler.scan();
    assertEquals(0, session.lastLagLevel());
    assertFalse(session.isDropBestEffort());
  }

  @Test
  void scan_atLevel4_replayInProgress_suppressesDisconnect() {
    pendingBytes.set(450);
    session.replayInProgress(true);
    handler.scan(); // enter L4
    assertEquals(4, session.lastLagLevel());
    // Advance past disconnect timeout but replayInProgress suppresses disconnect.
    clock.advanceMillis(config.slowConsumerDisconnectMs() + 100);
    handler.scan();
    assertTrue(channel.isOpen(), "channel must remain open during replay");
  }

  @Test
  void scan_atLevel4_sustainedDwell_disconnects() {
    pendingBytes.set(450);
    handler.scan(); // enter L4
    assertEquals(4, session.lastLagLevel());
    // Dwell past disconnect window.
    clock.advanceNanos(TimeUnit.MILLISECONDS.toNanos(config.slowConsumerDisconnectMs()) + 1L);
    handler.scan();
    assertFalse(channel.isOpen(), "channel must be closed after sustained L4");
  }

  @Test
  void scan_hysteresis_levelStaysSticky_untilDownward() {
    // Crossing from 250 (L2) up to 350 (L3) increments only L3 once.
    pendingBytes.set(250);
    handler.scan();
    assertEquals(2, session.lastLagLevel());

    pendingBytes.set(350);
    handler.scan();
    assertEquals(3, session.lastLagLevel());
    // Stay at 350 — repeated scans should not re-fire (level stays same).
    handler.scan();
    handler.scan();
    assertEquals(3, session.lastLagLevel());
  }
}
