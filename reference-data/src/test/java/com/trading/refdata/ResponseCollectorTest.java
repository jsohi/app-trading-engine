package com.trading.refdata;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

final class ResponseCollectorTest {

  @Test
  void tracksLoadedResponses() {
    final var collector = new ResponseCollector();
    collector.expectResponses(3);

    assertFalse(collector.isComplete());
    assertFalse(collector.hasRejections());

    collector.onLoaded();
    collector.onLoaded();
    assertFalse(collector.isComplete());

    collector.onLoaded();
    assertTrue(collector.isComplete());
    assertFalse(collector.hasRejections());
    assertEquals(3, collector.loadedCount());
    assertEquals(0, collector.rejectedCount());
  }

  @Test
  void tracksRejectedResponses() {
    final var collector = new ResponseCollector();
    collector.expectResponses(2);

    collector.onLoaded();
    collector.onRejected("bad account");

    assertTrue(collector.isComplete());
    assertTrue(collector.hasRejections());
    assertEquals(1, collector.loadedCount());
    assertEquals(1, collector.rejectedCount());
    assertEquals("bad account", collector.rejectionReasons().get(0));
  }

  @Test
  void expectResponsesResetsState() {
    final var collector = new ResponseCollector();
    collector.expectResponses(1);
    collector.onLoaded();
    assertTrue(collector.isComplete());

    collector.expectResponses(2);
    assertFalse(collector.isComplete());
    assertEquals(0, collector.loadedCount());
    assertEquals(0, collector.rejectedCount());
  }

  @Test
  void isCompleteReturnsFalseBeforeInitialization() {
    final var collector = new ResponseCollector();
    assertFalse(collector.isComplete());
  }

  @Test
  void rejectionReasonsAreImmutableCopy() {
    final var collector = new ResponseCollector();
    collector.expectResponses(1);
    collector.onRejected("reason");

    final var reasons = collector.rejectionReasons();
    assertThrows(UnsupportedOperationException.class, () -> reasons.add("another"));
  }
}
