package com.trading.engine.projections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import java.util.Map;
import java.util.Set;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

class ProjectionRegistryTest {

  /** Projection whose lastProcessedSequence() returns whatever the test sets. */
  private static final class FixedLagProjection implements Projection {
    long lastSeq;

    FixedLagProjection(final long lastSeq) {
      this.lastSeq = lastSeq;
    }

    @Override
    public void onEvent(
        final long seqNo,
        final int eventType,
        final DirectBuffer buffer,
        final int offset,
        final int length) {
      this.lastSeq = seqNo;
    }

    @Override
    public long lastProcessedSequence() {
      return lastSeq;
    }

    @Override
    public void reset() {
      this.lastSeq = 0L;
    }
  }

  /**
   * Feed {@code count} fragments with {@code templateId} into the consumer. Any projection
   * registered for that templateId advances by {@code count}; projections registered for other
   * types stay at their current tracked seqNo. The consumer's ingress counter advances by {@code
   * count} for each dispatched fragment.
   */
  private static void feed(final EventConsumer consumer, final int templateId, final int count) {
    final UnsafeBuffer buf = new UnsafeBuffer(new byte[32]);
    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    header.wrap(buf, 0).blockLength(0).templateId(templateId).schemaId(1).version(1);
    for (int i = 0; i < count; i++) {
      consumer.onFragment(buf, 0, 32, null);
    }
  }

  // ---------------------------------------------------------------------------
  // Construction
  // ---------------------------------------------------------------------------

  @Test
  void constructorRejectsNullConsumer() {
    assertThrows(NullPointerException.class, () -> new ProjectionRegistry(null, 100L));
  }

  @Test
  void constructorRejectsNegativeLagThreshold() {
    final EventConsumer c = new EventConsumer();
    assertThrows(IllegalArgumentException.class, () -> new ProjectionRegistry(c, -1L));
  }

  @Test
  void lagThresholdAccessor() {
    final EventConsumer c = new EventConsumer();
    final ProjectionRegistry r = new ProjectionRegistry(c, 50L);
    assertEquals(50L, r.lagThreshold());
  }

  // ---------------------------------------------------------------------------
  // Registration
  // ---------------------------------------------------------------------------

  @Test
  void registerAddsByName() {
    final EventConsumer c = new EventConsumer();
    final ProjectionRegistry r = new ProjectionRegistry(c, 100L);
    r.register("orders", new FixedLagProjection(0L));
    r.register("positions", new FixedLagProjection(0L));

    assertEquals(2, r.size());
    assertEquals(Set.of("orders", "positions"), r.names());
  }

  @Test
  void registerRejectsDuplicateName() {
    final EventConsumer c = new EventConsumer();
    final ProjectionRegistry r = new ProjectionRegistry(c, 100L);
    r.register("orders", new FixedLagProjection(0L));
    assertThrows(
        IllegalArgumentException.class, () -> r.register("orders", new FixedLagProjection(0L)));
  }

  @Test
  void registerRejectsNulls() {
    final EventConsumer c = new EventConsumer();
    final ProjectionRegistry r = new ProjectionRegistry(c, 100L);
    assertThrows(NullPointerException.class, () -> r.register(null, new FixedLagProjection(0L)));
    assertThrows(NullPointerException.class, () -> r.register("x", null));
  }

  // ---------------------------------------------------------------------------
  // Lag + health
  // ---------------------------------------------------------------------------

  @Test
  void getLagComputedRelativeToConsumerHead() {
    // Register two projections with the consumer on different templateIds. Feed 7 fragments of
    // type 100 (fast projection ticks to 7) then 3 fragments of type 200 (slow projection
    // ticks to 10, but fast stays at 7). Consumer head = 10.
    final EventConsumer c = new EventConsumer();
    final FixedLagProjection fast = new FixedLagProjection(0L);
    final FixedLagProjection slow = new FixedLagProjection(0L);
    c.registerProjection(fast, 100);
    c.registerProjection(slow, 200);
    feed(c, 100, 7);
    feed(c, 200, 3);
    assertEquals(10L, c.lastProcessedSequence());
    assertEquals(7L, c.lastProcessedSequence(fast));
    assertEquals(10L, c.lastProcessedSequence(slow));

    final ProjectionRegistry r = new ProjectionRegistry(c, 100L);
    r.register("fast", fast);
    r.register("slow", slow);

    final Map<String, Long> lag = r.getLag();
    assertEquals(3L, lag.get("fast")); // head 10 - fast 7
    assertEquals(0L, lag.get("slow")); // head 10 - slow 10
  }

  @Test
  void isHealthyTrueWhenAllWithinThreshold() {
    final EventConsumer c = new EventConsumer();
    final FixedLagProjection a = new FixedLagProjection(0L);
    final FixedLagProjection b = new FixedLagProjection(0L);
    c.registerProjection(a, 100);
    c.registerProjection(b, 200);
    feed(c, 100, 95); // a at 95
    feed(c, 200, 5); // b at 100 (head=100), a still at 95 → lag 5
    // Both projections within threshold 10.
    final ProjectionRegistry r = new ProjectionRegistry(c, 10L);
    r.register("a", a);
    r.register("b", b);
    assertTrue(r.isHealthy());
  }

  @Test
  void isHealthyFalseWhenAnyExceedsThreshold() {
    final EventConsumer c = new EventConsumer();
    final FixedLagProjection fast = new FixedLagProjection(0L);
    final FixedLagProjection stale = new FixedLagProjection(0L);
    c.registerProjection(fast, 100);
    c.registerProjection(stale, 200);
    feed(c, 200, 1); // stale at 1 (head=1)
    feed(c, 100, 30); // fast at 31 (head=31), stale still at 1 → lag 30
    final ProjectionRegistry r = new ProjectionRegistry(c, 10L);
    r.register("fast", fast);
    r.register("stale", stale);
    assertFalse(r.isHealthy());
  }

  @Test
  void getLagForUnregisteredProjectionIsConsumerHead() {
    // A projection that the registry knows about but that was NEVER registered with the
    // consumer should report lag = consumer head (consumer tracking returns 0 for unknowns).
    final EventConsumer c = new EventConsumer();
    final FixedLagProjection tracked = new FixedLagProjection(0L);
    c.registerProjection(tracked, 100);
    feed(c, 100, 5);

    final ProjectionRegistry r = new ProjectionRegistry(c, 100L);
    r.register("known", tracked);
    r.register("unknown-to-consumer", new FixedLagProjection(0L));

    final Map<String, Long> lag = r.getLag();
    assertEquals(0L, lag.get("known"));
    assertEquals(5L, lag.get("unknown-to-consumer"));
  }

  @Test
  void isHealthyEmptyRegistryVacuouslyTrue() {
    final EventConsumer c = new EventConsumer();
    final ProjectionRegistry r = new ProjectionRegistry(c, 10L);
    assertTrue(r.isHealthy());
    assertEquals(0, r.size());
  }

  @Test
  void namesReturnsDefensiveCopy() {
    final EventConsumer c = new EventConsumer();
    final ProjectionRegistry r = new ProjectionRegistry(c, 100L);
    r.register("orders", new FixedLagProjection(0L));
    final Set<String> snapshot = r.names();
    r.register("positions", new FixedLagProjection(0L));
    // The earlier snapshot must not reflect the later registration.
    assertEquals(1, snapshot.size());
    assertEquals(2, r.size());
  }
}
