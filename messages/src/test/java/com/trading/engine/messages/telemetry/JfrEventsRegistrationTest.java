package com.trading.engine.messages.telemetry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import jdk.jfr.EventType;
import org.junit.jupiter.api.Test;

/**
 * Verifies that the three trading-engine JFR custom event classes are correctly registered with the
 * JFR runtime and carry the expected {@code @Name} annotation values.
 *
 * <p><b>Purpose.</b> Guards against class-loading failures ({@link NoClassDefFoundError}), missing
 * JFR annotations, and typos in the {@code trading.MarketData*} event-name namespace that would
 * cause Mission Control queries to return empty results. Runs in the {@code :messages} module where
 * the event classes reside, with no external process or Aeron infrastructure required.
 *
 * <p><b>Design rationale.</b> {@link EventType#getEventType(Class)} performs a lightweight
 * registration check: it loads the class, validates the {@link jdk.jfr.Name} annotation, and
 * returns the live {@code EventType} descriptor. A non-null return proves the class is on the
 * classpath and has a well-formed JFR annotation. The event-name string match proves the
 * {@code @Name} value has not been silently changed during refactoring.
 *
 * <p><b>Threading model.</b> Single-threaded JUnit test runner. JFR registration is thread-safe
 * internally.
 *
 * <p><b>Allocation.</b> Test-only; no allocation constraints apply.
 *
 * <p><b>Dependencies.</b> {@link MarketDataTickPublished}, {@link MarketDataTickRejected}, {@link
 * MarketDataFeedStateTransition}; {@link jdk.jfr.EventType}; JUnit Jupiter.
 */
final class JfrEventsRegistrationTest {

  /**
   * Verifies {@link MarketDataTickPublished} registers with JFR and carries the expected event name
   * in the {@code trading.MarketData*} namespace.
   */
  @Test
  void marketDataTickPublished_registersWithJfr_nameMatchesPlan() {
    final var eventType = EventType.getEventType(MarketDataTickPublished.class);

    assertNotNull(
        eventType,
        "MarketDataTickPublished must be recognised by EventType.getEventType() — "
            + "check that the class extends jdk.jfr.Event and carries @Name");
    assertEquals(
        "trading.MarketDataTickPublished",
        eventType.getName(),
        "MarketDataTickPublished @Name must be 'trading.MarketDataTickPublished'");
  }

  /**
   * Verifies {@link MarketDataTickRejected} registers with JFR and carries the expected event name
   * in the {@code trading.MarketData*} namespace.
   */
  @Test
  void marketDataTickRejected_registersWithJfr_nameMatchesPlan() {
    final var eventType = EventType.getEventType(MarketDataTickRejected.class);

    assertNotNull(
        eventType,
        "MarketDataTickRejected must be recognised by EventType.getEventType() — "
            + "check that the class extends jdk.jfr.Event and carries @Name");
    assertEquals(
        "trading.MarketDataTickRejected",
        eventType.getName(),
        "MarketDataTickRejected @Name must be 'trading.MarketDataTickRejected'");
  }

  /**
   * Verifies {@link MarketDataFeedStateTransition} registers with JFR and carries the expected
   * event name in the {@code trading.MarketData*} namespace.
   */
  @Test
  void marketDataFeedStateTransition_registersWithJfr_nameMatchesPlan() {
    final var eventType = EventType.getEventType(MarketDataFeedStateTransition.class);

    assertNotNull(
        eventType,
        "MarketDataFeedStateTransition must be recognised by EventType.getEventType() — "
            + "check that the class extends jdk.jfr.Event and carries @Name");
    assertEquals(
        "trading.MarketDataFeedStateTransition",
        eventType.getName(),
        "MarketDataFeedStateTransition @Name must be 'trading.MarketDataFeedStateTransition'");
  }

  /**
   * Verifies all three event names share the same {@code trading.} namespace prefix, guarding
   * against a partial rename that would scatter events across inconsistent JFR namespaces.
   */
  @Test
  void allEvents_shareTradeNamespace_prefixConsistent() {
    final var publishedType = EventType.getEventType(MarketDataTickPublished.class);
    final var rejectedType = EventType.getEventType(MarketDataTickRejected.class);
    final var transitionType = EventType.getEventType(MarketDataFeedStateTransition.class);

    assertNotNull(publishedType);
    assertNotNull(rejectedType);
    assertNotNull(transitionType);

    assertEquals(
        "trading",
        publishedType.getName().split("\\.")[0],
        "MarketDataTickPublished must be in the 'trading' namespace");
    assertEquals(
        "trading",
        rejectedType.getName().split("\\.")[0],
        "MarketDataTickRejected must be in the 'trading' namespace");
    assertEquals(
        "trading",
        transitionType.getName().split("\\.")[0],
        "MarketDataFeedStateTransition must be in the 'trading' namespace");
  }
}
