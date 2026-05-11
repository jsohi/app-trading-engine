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
 * Unit tests for {@link MicrometerDropCounter} — verifies counter pre-registration and per-reason
 * increment behaviour against a {@link SimpleMeterRegistry} (in-memory, no Prometheus). Counters
 * are NOT tagged by session id (PR #71 R2 cardinality fix); per-session forensics live in the audit
 * log instead.
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
  void ctor_preRegistersAllThreeCounters() {
    // Construction alone must register one counter per DropReason — three series total.
    assertNotNull(findCounter("disabled"), "DISABLED counter must be pre-registered");
    assertNotNull(findCounter("rate_limit"), "RATE_LIMIT counter must be pre-registered");
    assertNotNull(
        findCounter("outbound_queue_full"), "OUTBOUND_QUEUE_FULL counter must be pre-registered");
    assertEquals(0.0, findCounter("disabled").count(), "fresh counter starts at 0");
    assertEquals(0.0, findCounter("rate_limit").count());
    assertEquals(0.0, findCounter("outbound_queue_full").count());
  }

  @Test
  void incrementDrop_disabled_increments() {
    final var session = session("S-1");
    counter.incrementDrop(session, RawFixTap.DropReason.DISABLED);

    final var c = findCounter("disabled");
    assertEquals(1.0, c.count(), "counter must read 1 after one increment");
  }

  @Test
  void incrementDrop_repeatedSameReason_increments() {
    final var session = session("S-1");
    counter.incrementDrop(session, RawFixTap.DropReason.RATE_LIMIT);
    counter.incrementDrop(session, RawFixTap.DropReason.RATE_LIMIT);
    counter.incrementDrop(session, RawFixTap.DropReason.RATE_LIMIT);

    final var c = findCounter("rate_limit");
    assertEquals(3.0, c.count(), "three increments must yield count = 3");
  }

  @Test
  void incrementDrop_differentReasons_separateCounters() {
    final var session = session("S-1");
    counter.incrementDrop(session, RawFixTap.DropReason.DISABLED);
    counter.incrementDrop(session, RawFixTap.DropReason.RATE_LIMIT);
    counter.incrementDrop(session, RawFixTap.DropReason.OUTBOUND_QUEUE_FULL);

    assertEquals(1.0, findCounter("disabled").count());
    assertEquals(1.0, findCounter("rate_limit").count());
    assertEquals(1.0, findCounter("outbound_queue_full").count());
  }

  @Test
  void incrementDrop_differentSessions_aggregateUnderSameReason() {
    // Per-session granularity intentionally NOT a metric tag (cardinality fix). Drops from
    // different sessions for the same reason aggregate into the single per-reason counter.
    counter.incrementDrop(session("S-A"), RawFixTap.DropReason.DISABLED);
    counter.incrementDrop(session("S-A"), RawFixTap.DropReason.DISABLED);
    counter.incrementDrop(session("S-B"), RawFixTap.DropReason.DISABLED);

    assertEquals(3.0, findCounter("disabled").count(), "drops from S-A + S-B aggregate");
  }

  // --- helpers -----------------------------------------------------------

  private Counter findCounter(final String reason) {
    return registry.find(MicrometerDropCounter.METRIC_NAME).tag("reason", reason).counter();
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
