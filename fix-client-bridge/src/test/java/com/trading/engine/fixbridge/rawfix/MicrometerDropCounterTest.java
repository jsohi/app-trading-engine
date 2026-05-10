package com.trading.engine.fixbridge.rawfix;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.trading.engine.fixbridge.quote.SessionId;
import com.trading.engine.fixbridge.ratelimit.PerTypeRateLimiter;
import com.trading.engine.fixbridge.transport.BridgeSession;
import com.trading.engine.fixbridge.transport.OutboundQueue;
import com.trading.engine.websocket.JwtValidator.ValidatedClaims;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.net.InetAddress;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link MicrometerDropCounter} — verifies counter registration and per-(session,
 * reason) increment behaviour against a {@link SimpleMeterRegistry} (in-memory, no Prometheus).
 */
final class MicrometerDropCounterTest {

  private SimpleMeterRegistry registry;
  private MicrometerDropCounter counter;

  @BeforeEach
  void setUp() {
    registry = new SimpleMeterRegistry();
    counter = new MicrometerDropCounter(registry);
  }

  @Test
  void ctor_nullRegistry_throwsNPE() {
    assertThrows(NullPointerException.class, () -> new MicrometerDropCounter(null));
  }

  @Test
  void incrementDrop_singleSessionDisabled_registersAndIncrements() {
    final var session = session("S-1");
    counter.incrementDrop(session, RawFixTap.DropReason.DISABLED);

    final Counter c = findCounter("S-1", "disabled");
    assertNotNull(c, "counter must be registered for (S-1, disabled)");
    assertEquals(1.0, c.count(), "counter must read 1 after one increment");
  }

  @Test
  void incrementDrop_repeatedSamePair_increments() {
    final var session = session("S-1");
    counter.incrementDrop(session, RawFixTap.DropReason.RATE_LIMIT);
    counter.incrementDrop(session, RawFixTap.DropReason.RATE_LIMIT);
    counter.incrementDrop(session, RawFixTap.DropReason.RATE_LIMIT);

    final Counter c = findCounter("S-1", "rate_limit");
    assertEquals(3.0, c.count(), "three increments must yield count = 3");
  }

  @Test
  void incrementDrop_differentReasons_separateCounters() {
    final var session = session("S-1");
    counter.incrementDrop(session, RawFixTap.DropReason.DISABLED);
    counter.incrementDrop(session, RawFixTap.DropReason.RATE_LIMIT);
    counter.incrementDrop(session, RawFixTap.DropReason.OUTBOUND_QUEUE_FULL);

    assertEquals(1.0, findCounter("S-1", "disabled").count());
    assertEquals(1.0, findCounter("S-1", "rate_limit").count());
    assertEquals(1.0, findCounter("S-1", "outbound_queue_full").count());
  }

  @Test
  void incrementDrop_differentSessions_separateCounters() {
    counter.incrementDrop(session("S-A"), RawFixTap.DropReason.DISABLED);
    counter.incrementDrop(session("S-A"), RawFixTap.DropReason.DISABLED);
    counter.incrementDrop(session("S-B"), RawFixTap.DropReason.DISABLED);

    assertEquals(2.0, findCounter("S-A", "disabled").count());
    assertEquals(1.0, findCounter("S-B", "disabled").count());
  }

  // --- helpers -----------------------------------------------------------

  private Counter findCounter(final String session, final String reason) {
    return registry
        .find(MicrometerDropCounter.METRIC_NAME)
        .tag("session", session)
        .tag("reason", reason)
        .counter();
  }

  private static BridgeSession session(final String id) {
    final var claims =
        new ValidatedClaims("user", "jti", List.of(), Long.MAX_VALUE, true, List.of(), null);
    return new BridgeSession(
        new SessionId(id),
        claims,
        InetAddress.getLoopbackAddress(),
        new OutboundQueue(8),
        new PerTypeRateLimiter(0L));
  }
}
