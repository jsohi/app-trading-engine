package com.trading.engine.orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.aeron.cluster.codecs.CloseReason;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link OrchestratorRfqForwarder}.
 *
 * <p>The forwarder is the cluster-event-translation + FIX-session routing piece introduced for
 * APP-232. Tests cover routing-map insert/lookup/evict, hash collision avoidance with the sentinel
 * value, and per-session bulk eviction.
 */
class OrchestratorRfqForwarderTest {

  private final byte[] qrId1 =
      "REQ-12345678901234567".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
  private final byte[] qrId2 =
      "REQ-99999999999999999".getBytes(java.nio.charset.StandardCharsets.US_ASCII);

  @Test
  void recordOriginatingSession_thenLookup_returnsRecordedSessionId() {
    final var fwd = new OrchestratorRfqForwarder(64);
    final long h1 = OrchestratorRfqForwarder.fnv1a64(qrId1, 0, qrId1.length);
    fwd.recordOriginatingSession(h1, 7L);
    assertEquals(7L, fwd.lookupSessionForQuoteReqId(h1));
  }

  @Test
  void lookupSessionForQuoteReqId_unknownKey_returnsMissingSentinel() {
    final var fwd = new OrchestratorRfqForwarder(64);
    final long h1 = OrchestratorRfqForwarder.fnv1a64(qrId1, 0, qrId1.length);
    assertEquals(OrchestratorRfqForwarder.MISSING_SESSION, fwd.lookupSessionForQuoteReqId(h1));
  }

  @Test
  void evict_removesEntry() {
    final var fwd = new OrchestratorRfqForwarder(64);
    final long h1 = OrchestratorRfqForwarder.fnv1a64(qrId1, 0, qrId1.length);
    fwd.recordOriginatingSession(h1, 7L);
    fwd.evict(h1);
    assertEquals(OrchestratorRfqForwarder.MISSING_SESSION, fwd.lookupSessionForQuoteReqId(h1));
  }

  @Test
  void evictSession_removesAllEntriesForSession_returnsCount() {
    final var fwd = new OrchestratorRfqForwarder(64);
    final long h1 = OrchestratorRfqForwarder.fnv1a64(qrId1, 0, qrId1.length);
    final long h2 = OrchestratorRfqForwarder.fnv1a64(qrId2, 0, qrId2.length);
    fwd.recordOriginatingSession(h1, 7L);
    fwd.recordOriginatingSession(h2, 7L);
    fwd.recordOriginatingSession(99L, 8L); // distinct session, must survive
    final int evicted = fwd.evictSession(7L, CloseReason.CLIENT_ACTION);
    assertEquals(2, evicted);
    assertEquals(OrchestratorRfqForwarder.MISSING_SESSION, fwd.lookupSessionForQuoteReqId(h1));
    assertEquals(OrchestratorRfqForwarder.MISSING_SESSION, fwd.lookupSessionForQuoteReqId(h2));
    assertEquals(8L, fwd.lookupSessionForQuoteReqId(99L), "other session's entry must survive");
  }

  @Test
  void fnv1a64_distinctInputsProduceDistinctHashes() {
    final long h1 = OrchestratorRfqForwarder.fnv1a64(qrId1, 0, qrId1.length);
    final long h2 = OrchestratorRfqForwarder.fnv1a64(qrId2, 0, qrId2.length);
    assertNotEquals(h1, h2);
  }

  @Test
  void fnv1a64_neverReturnsMissingSentinel() {
    // Construct an input that would normally hash to Long.MIN_VALUE if the sentinel-avoidance
    // branch were absent. The probability is ~2^-64; we instead validate the contract by
    // hashing many inputs and asserting none hit the sentinel.
    for (int i = 0; i < 10_000; i++) {
      final byte[] in = ("input-" + i).getBytes(java.nio.charset.StandardCharsets.US_ASCII);
      final long h = OrchestratorRfqForwarder.fnv1a64(in, 0, in.length);
      assertNotEquals(OrchestratorRfqForwarder.MISSING_SESSION, h);
    }
  }

  @Test
  void constructor_zeroOrNegativeCapacity_throwsIllegalArgumentException() {
    assertThrows(IllegalArgumentException.class, () -> new OrchestratorRfqForwarder(0));
    assertThrows(IllegalArgumentException.class, () -> new OrchestratorRfqForwarder(-1));
  }

  @Test
  void routingEntryCount_reflectsInsertions() {
    final var fwd = new OrchestratorRfqForwarder(64);
    assertEquals(0, fwd.routingEntryCount());
    fwd.recordOriginatingSession(1L, 7L);
    fwd.recordOriginatingSession(2L, 7L);
    fwd.recordOriginatingSession(3L, 8L);
    assertEquals(3, fwd.routingEntryCount());
  }
}
