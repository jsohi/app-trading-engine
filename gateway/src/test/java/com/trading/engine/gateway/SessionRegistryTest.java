package com.trading.engine.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.testsupport.clock.ControllableNanoClock;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SessionRegistryTest {

  private static final int MAX_SESSIONS = 3;
  private static final int MAX_PER_COMP_ID = 2;

  /** Minimal GatewaySession stub for testing SessionRegistry without Artio infrastructure. */
  private static final GatewaySession FAKE_SESSION = new FakeGatewaySession(1L);

  private static GatewaySession fakeSession(final long id) {
    return new FakeGatewaySession(id);
  }

  private final ControllableNanoClock clock = new ControllableNanoClock(1_000_000_000L);
  private SessionRegistry registry;

  @BeforeEach
  void setUp() {
    registry = new SessionRegistry(MAX_SESSIONS, MAX_PER_COMP_ID, 64);
  }

  // ===========================================================================
  // Correlation round-trip
  // ===========================================================================

  @Test
  void registerAndFindCorrelation() {
    final byte[] clOrdId = "ORD-001".getBytes(StandardCharsets.US_ASCII);
    registry.registerCorrelation(clOrdId, 0, clOrdId.length, 42L, clock.nanoTime());

    assertEquals(42L, registry.findByCorrelationId(clOrdId, 0, clOrdId.length));
  }

  @Test
  void unknownCorrelationReturnsNullSession() {
    final byte[] unknown = "UNKNOWN".getBytes(StandardCharsets.US_ASCII);
    assertEquals(
        SessionLookup.NULL_SESSION, registry.findByCorrelationId(unknown, 0, unknown.length));
  }

  @Test
  void removeCorrelationClearsEntry() {
    final byte[] clOrdId = "ORD-002".getBytes(StandardCharsets.US_ASCII);
    registry.registerCorrelation(clOrdId, 0, clOrdId.length, 99L, clock.nanoTime());
    assertEquals(99L, registry.findByCorrelationId(clOrdId, 0, clOrdId.length));

    registry.removeCorrelation(clOrdId, 0, clOrdId.length);
    assertEquals(
        SessionLookup.NULL_SESSION, registry.findByCorrelationId(clOrdId, 0, clOrdId.length));
  }

  @Test
  void multipleCorrelationsMappedToSameSession() {
    final byte[] ord1 = "ORD-A".getBytes(StandardCharsets.US_ASCII);
    final byte[] ord2 = "ORD-B".getBytes(StandardCharsets.US_ASCII);
    registry.registerCorrelation(ord1, 0, ord1.length, 10L, clock.nanoTime());
    registry.registerCorrelation(ord2, 0, ord2.length, 10L, clock.nanoTime());

    assertEquals(10L, registry.findByCorrelationId(ord1, 0, ord1.length));
    assertEquals(10L, registry.findByCorrelationId(ord2, 0, ord2.length));
  }

  @Test
  void correlationsForDifferentSessions() {
    final byte[] ord1 = "ORD-X".getBytes(StandardCharsets.US_ASCII);
    final byte[] ord2 = "ORD-Y".getBytes(StandardCharsets.US_ASCII);
    registry.registerCorrelation(ord1, 0, ord1.length, 100L, clock.nanoTime());
    registry.registerCorrelation(ord2, 0, ord2.length, 200L, clock.nanoTime());

    assertEquals(100L, registry.findByCorrelationId(ord1, 0, ord1.length));
    assertEquals(200L, registry.findByCorrelationId(ord2, 0, ord2.length));
  }

  // ===========================================================================
  // Session management
  // ===========================================================================

  @Test
  void registerAndFindSession() {
    final GatewaySession mySession = fakeSession(1L);
    final boolean registered = registry.tryRegisterSession(1L, 100L, mySession);
    assertTrue(registered);
    assertEquals(1, registry.sessionCount());
    assertSame(mySession, registry.findSession(1L));
  }

  @Test
  void removeSessionClearsEntry() {
    registry.tryRegisterSession(1L, 100L, FAKE_SESSION);
    registry.removeSession(1L);
    assertNull(registry.findSession(1L));
    assertEquals(0, registry.sessionCount());
  }

  @Test
  void findSessionReturnsNullForUnknown() {
    assertNull(registry.findSession(999L));
  }

  // ===========================================================================
  // Session capacity limits
  // ===========================================================================

  @Test
  void globalMaxSessionsEnforced() {
    assertTrue(registry.tryRegisterSession(1L, 100L, FAKE_SESSION));
    assertTrue(registry.tryRegisterSession(2L, 200L, FAKE_SESSION));
    assertTrue(registry.tryRegisterSession(3L, 300L, FAKE_SESSION));

    // 4th session exceeds global max (3)
    assertFalse(registry.tryRegisterSession(4L, 400L, FAKE_SESSION));
    assertEquals(3, registry.sessionCount());
  }

  @Test
  void perCompIdMaxEnforced() {
    // Same CompID hash for both sessions
    assertTrue(registry.tryRegisterSession(1L, 100L, FAKE_SESSION));
    assertTrue(registry.tryRegisterSession(2L, 100L, FAKE_SESSION));

    // 3rd session from same CompID exceeds per-CompID max (2)
    assertFalse(registry.tryRegisterSession(3L, 100L, FAKE_SESSION));
    assertEquals(2, registry.sessionCount());
  }

  @Test
  void removeSessionDecrementsCompIdCounter() {
    assertTrue(registry.tryRegisterSession(1L, 100L, FAKE_SESSION));
    assertTrue(registry.tryRegisterSession(2L, 100L, FAKE_SESSION));

    // At per-CompID limit — can't add another
    assertFalse(registry.tryRegisterSession(3L, 100L, FAKE_SESSION));

    // Remove one session from the same CompID
    registry.removeSession(1L);

    // Now we can add another
    assertTrue(registry.tryRegisterSession(3L, 100L, FAKE_SESSION));
    assertEquals(2, registry.sessionCount());
  }

  // ===========================================================================
  // Stale correlation sweep
  // ===========================================================================

  @Test
  void sweepRemovesOrphanCorrelations() {
    final byte[] ord1 = "ORD-SWEEP1".getBytes(StandardCharsets.US_ASCII);
    final byte[] ord2 = "ORD-SWEEP2".getBytes(StandardCharsets.US_ASCII);

    // Register correlations for session 10 and 20
    registry.registerCorrelation(ord1, 0, ord1.length, 10L, clock.nanoTime());
    registry.registerCorrelation(ord2, 0, ord2.length, 20L, clock.nanoTime());

    // Only session 10 is actually registered
    registry.tryRegisterSession(10L, 100L, FAKE_SESSION);

    // Sweep should remove the orphan correlation for session 20
    final int removed = registry.sweepStaleCorrelations();
    assertEquals(1, removed);

    // Session 10's correlation still works
    assertEquals(10L, registry.findByCorrelationId(ord1, 0, ord1.length));

    // Session 20's correlation is gone
    assertEquals(SessionLookup.NULL_SESSION, registry.findByCorrelationId(ord2, 0, ord2.length));
  }

  @Test
  void sweepAfterSessionRemovalCleansOrphanCorrelations() {
    final byte[] ord1 = "ORD-REM1".getBytes(StandardCharsets.US_ASCII);
    final byte[] ord2 = "ORD-REM2".getBytes(StandardCharsets.US_ASCII);

    // Register session, add correlations, then disconnect
    registry.tryRegisterSession(10L, 100L, FAKE_SESSION);
    registry.registerCorrelation(ord1, 0, ord1.length, 10L, clock.nanoTime());
    registry.registerCorrelation(ord2, 0, ord2.length, 10L, clock.nanoTime());
    assertEquals(2, registry.correlationCount());

    // Session disconnects
    registry.removeSession(10L);

    // Sweep should find both correlations orphaned
    assertEquals(2, registry.sweepStaleCorrelations());
    assertEquals(0, registry.correlationCount());
  }

  @Test
  void sweepWithNoOrphansRemovesNothing() {
    final byte[] ord = "ORD-OK".getBytes(StandardCharsets.US_ASCII);
    registry.tryRegisterSession(1L, 100L, FAKE_SESSION);
    registry.registerCorrelation(ord, 0, ord.length, 1L, clock.nanoTime());

    assertEquals(0, registry.sweepStaleCorrelations());
    assertEquals(1, registry.correlationCount());
  }

  // ===========================================================================
  // TTL-based correlation expiry
  // ===========================================================================

  private static final long TTL_NS = 45_000_000_000L; // 45 seconds

  @Test
  void sweepExpiredCorrelations_removesEntriesOlderThanTtl() {
    final byte[] ord1 = "ORD-TTL1".getBytes(StandardCharsets.US_ASCII);
    final byte[] ord2 = "ORD-TTL2".getBytes(StandardCharsets.US_ASCII);

    // Register both at t=1s
    registry.registerCorrelation(ord1, 0, ord1.length, 10L, clock.nanoTime());
    registry.registerCorrelation(ord2, 0, ord2.length, 20L, clock.nanoTime());
    assertEquals(2, registry.correlationCount());

    // Advance past TTL
    clock.advanceNanos(TTL_NS + 1);

    // Both should be expired
    final int expired = registry.sweepExpiredCorrelations(clock.nanoTime(), TTL_NS);
    assertEquals(2, expired);
    assertEquals(0, registry.correlationCount());

    // Verify they are actually gone from correlation map
    assertEquals(SessionLookup.NULL_SESSION, registry.findByCorrelationId(ord1, 0, ord1.length));
    assertEquals(SessionLookup.NULL_SESSION, registry.findByCorrelationId(ord2, 0, ord2.length));
  }

  @Test
  void sweepExpiredCorrelations_retainsRecentEntries() {
    final byte[] old = "ORD-OLD".getBytes(StandardCharsets.US_ASCII);
    final byte[] fresh = "ORD-FRESH".getBytes(StandardCharsets.US_ASCII);

    // Register old entry at t=1s
    registry.registerCorrelation(old, 0, old.length, 10L, clock.nanoTime());

    // Advance 30s, then register fresh entry
    clock.advanceSeconds(30);
    registry.registerCorrelation(fresh, 0, fresh.length, 20L, clock.nanoTime());

    // Advance another 20s (old = 50s old, fresh = 20s old, TTL = 45s)
    clock.advanceSeconds(20);

    final int expired = registry.sweepExpiredCorrelations(clock.nanoTime(), TTL_NS);
    assertEquals(1, expired); // only old entry expired

    // Fresh entry still findable
    assertEquals(20L, registry.findByCorrelationId(fresh, 0, fresh.length));
    // Old entry gone
    assertEquals(SessionLookup.NULL_SESSION, registry.findByCorrelationId(old, 0, old.length));
  }

  @Test
  void sweepExpiredCorrelations_incrementsTtlExpiredCount() {
    final byte[] ord = "ORD-COUNT".getBytes(StandardCharsets.US_ASCII);
    registry.registerCorrelation(ord, 0, ord.length, 10L, clock.nanoTime());

    assertEquals(0, registry.ttlExpiredCount());

    clock.advanceNanos(TTL_NS + 1);
    registry.sweepExpiredCorrelations(clock.nanoTime(), TTL_NS);

    assertEquals(1, registry.ttlExpiredCount());

    // Register + expire another
    registry.registerCorrelation(ord, 0, ord.length, 10L, clock.nanoTime());
    clock.advanceNanos(TTL_NS + 1);
    registry.sweepExpiredCorrelations(clock.nanoTime(), TTL_NS);

    assertEquals(2, registry.ttlExpiredCount()); // cumulative
  }

  @Test
  void sweepExpiredCorrelations_noExpiredEntriesReturnsZero() {
    final byte[] ord = "ORD-NOEXPIRY".getBytes(StandardCharsets.US_ASCII);
    registry.registerCorrelation(ord, 0, ord.length, 10L, clock.nanoTime());

    // Don't advance clock — entry is fresh
    assertEquals(0, registry.sweepExpiredCorrelations(clock.nanoTime(), TTL_NS));
    assertEquals(1, registry.correlationCount());
  }

  @Test
  void removeCorrelation_alsoRemovesTimestamp() {
    final byte[] ord = "ORD-RM-TS".getBytes(StandardCharsets.US_ASCII);
    registry.registerCorrelation(ord, 0, ord.length, 10L, clock.nanoTime());

    // Remove the correlation
    registry.removeCorrelation(ord, 0, ord.length);

    // Advance past TTL and sweep — should find nothing (timestamp was already removed)
    clock.advanceNanos(TTL_NS + 1);
    assertEquals(0, registry.sweepExpiredCorrelations(clock.nanoTime(), TTL_NS));
  }

  @Test
  void sweepStaleCorrelations_alsoRemovesTimestamps() {
    final byte[] ord = "ORD-STALE-TS".getBytes(StandardCharsets.US_ASCII);
    // Register correlation for a session that doesn't exist (orphan)
    registry.registerCorrelation(ord, 0, ord.length, 999L, clock.nanoTime());

    // Stale sweep removes the orphan
    assertEquals(1, registry.sweepStaleCorrelations());

    // Advance past TTL and sweep — should find nothing (timestamp was cleaned by stale sweep)
    clock.advanceNanos(TTL_NS + 1);
    assertEquals(0, registry.sweepExpiredCorrelations(clock.nanoTime(), TTL_NS));
  }

  // ===========================================================================
  // FNV-1a 64-bit hash (moved from InFlightTracker — APP-161)
  // ===========================================================================

  @Test
  void fnv1aHash_deterministic() {
    final byte[] a = "ORD-00000000001".getBytes(StandardCharsets.US_ASCII);
    final byte[] b = "ORD-00000000001".getBytes(StandardCharsets.US_ASCII);
    assertEquals(
        SessionRegistry.fnv1aHash(a, 0, a.length), SessionRegistry.fnv1aHash(b, 0, b.length));
  }

  @Test
  void fnv1aHash_differentInputs_differentHashes() {
    final byte[] a = "ORD-001".getBytes(StandardCharsets.US_ASCII);
    final byte[] b = "ORD-002".getBytes(StandardCharsets.US_ASCII);
    assertTrue(
        SessionRegistry.fnv1aHash(a, 0, a.length) != SessionRegistry.fnv1aHash(b, 0, b.length));
  }

  @Test
  void fnv1aHash_respectsOffsetAndLength() {
    final byte[] buf = "xxORD-001yy".getBytes(StandardCharsets.US_ASCII);
    final byte[] exact = "ORD-001".getBytes(StandardCharsets.US_ASCII);
    assertEquals(
        SessionRegistry.fnv1aHash(exact, 0, exact.length), SessionRegistry.fnv1aHash(buf, 2, 7));
  }
}
