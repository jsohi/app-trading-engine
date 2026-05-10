package com.trading.engine.fixbridge.quote;

import static com.trading.engine.fixbridge.quote.SessionQuoteIndex.DUPLICATE_REQID_WINDOW_NANOS;
import static com.trading.engine.fixbridge.quote.SessionQuoteIndex.QUOTE_EMITTED_TTL_NANOS;
import static com.trading.engine.fixbridge.quote.SessionQuoteIndex.REQ_ID_TTL_NANOS;
import static com.trading.engine.fixbridge.quote.SessionQuoteIndex.QuoteRequestRegistration.ACCEPTED;
import static com.trading.engine.fixbridge.quote.SessionQuoteIndex.QuoteRequestRegistration.DUPLICATE_REQID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.agrona.collections.ObjectHashSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Comprehensive unit tests for {@link SessionQuoteIndex} — covers session lifecycle, QuoteRequest
 * registration, Quote emission correlation, ownership validation, TTL sweep, sub-bucket fan-out,
 * and the combined orphan-routing scenario described in plan §3.2.
 *
 * <p><b>Time model.</b> All timestamps are nanoseconds from an arbitrary epoch base
 * ({@code T0 = 1_000_000_000_000L}). Offsets are expressed as multiples of named constants so
 * the intent is clear at each callsite.
 *
 * <p><b>Threading.</b> Single-threaded test execution; {@link SessionQuoteIndex} is not
 * thread-safe by design.
 */
final class SessionQuoteIndexTest {

  /** Arbitrary epoch base so timestamps are clearly non-zero. */
  private static final long T0 = 1_000_000_000_000L;

  /** Convenience: 1 second in nanoseconds. */
  private static final long ONE_SEC = 1_000_000_000L;

  private SessionQuoteIndex index;

  @BeforeEach
  void setUp() {
    index = new SessionQuoteIndex();
  }

  // ===========================================================================
  // Session lifecycle
  // ===========================================================================

  @Test
  void onSessionAuthenticated_freshSession_registersInBothMaps() {
    final var s = new SessionId("S-1");
    index.onSessionAuthenticated(s, "alice");

    assertEquals("alice", index.subFor(s));
    final ObjectHashSet<SessionId> bucket = index.sessionsForSub("alice");
    assertNotNull(bucket);
    assertTrue(bucket.contains(s));
  }

  @Test
  void onSessionAuthenticated_sameSessionSameSub_isIdempotent() {
    final var s = new SessionId("S-1");
    index.onSessionAuthenticated(s, "alice");
    // second call must not throw
    index.onSessionAuthenticated(s, "alice");

    assertEquals("alice", index.subFor(s));
    assertEquals(1, index.sessionsForSub("alice").size());
  }

  @Test
  void onSessionAuthenticated_sameSessionDifferentSub_throwsIllegalStateException() {
    final var s = new SessionId("S-1");
    index.onSessionAuthenticated(s, "alice");

    assertThrows(IllegalStateException.class, () -> index.onSessionAuthenticated(s, "bob"));
  }

  @Test
  void onSessionAuthenticated_nullSessionId_throwsNPE() {
    assertThrows(NullPointerException.class, () -> index.onSessionAuthenticated(null, "alice"));
  }

  @Test
  void onSessionAuthenticated_nullSub_throwsNPE() {
    final var s = new SessionId("S-1");
    assertThrows(NullPointerException.class, () -> index.onSessionAuthenticated(s, null));
  }

  @Test
  void onSessionClosed_authenticatedSession_returnsSubAndEvictsBothMaps() {
    final var s = new SessionId("S-1");
    index.onSessionAuthenticated(s, "alice");

    final String sub = index.onSessionClosed(s);

    assertEquals("alice", sub);
    assertNull(index.subFor(s));
    // bucket removed because it was the last session for "alice"
    assertNull(index.sessionsForSub("alice"));
  }

  @Test
  void onSessionClosed_lastSessionInBucket_removesSubBucket() {
    final var s = new SessionId("S-1");
    index.onSessionAuthenticated(s, "alice");
    assertEquals(1, index.subCount());

    index.onSessionClosed(s);

    assertEquals(0, index.subCount());
  }

  @Test
  void onSessionClosed_unauthenticatedSession_returnsNull() {
    final var s = new SessionId("S-never-authed");
    final String sub = index.onSessionClosed(s);
    assertNull(sub);
  }

  @Test
  void onSessionClosed_evictsAllReqIdsForThatSession() {
    final var s = new SessionId("S-1");
    final var other = new SessionId("S-2");
    index.onSessionAuthenticated(s, "alice");
    index.onSessionAuthenticated(other, "bob");

    // Register 3 reqIds for s: two plain, one with Quote emitted
    index.onQuoteRequest("R1", s, T0);
    index.onQuoteRequest("R2", s, T0);
    index.onQuoteRequest("R3", s, T0);
    index.onQuoteEmitted("R3", "Q3", T0 + ONE_SEC);

    // Register 1 reqId for the other session — must survive
    index.onQuoteRequest("R-OTHER", other, T0);

    assertEquals(4, index.reqIdCount());
    assertEquals(1, index.quoteIdCount());

    index.onSessionClosed(s);

    // s's 3 reqIds and 1 quoteId must be gone; other session's entry must remain
    assertEquals(1, index.reqIdCount());
    assertEquals(0, index.quoteIdCount());
    assertFalse(index.isOwnedBy("Q3", s));
  }

  // ===========================================================================
  // QuoteRequest registration
  // ===========================================================================

  @Test
  void onQuoteRequest_freshReqId_returnsAccepted() {
    final var s = new SessionId("S-1");
    assertEquals(0, index.reqIdCount());

    final var result = index.onQuoteRequest("R1", s, T0);

    assertEquals(ACCEPTED, result);
    assertEquals(1, index.reqIdCount());
  }

  @Test
  void onQuoteRequest_sameReqIdSameSessionWithinWindow_returnsDuplicateReqid() {
    final var s = new SessionId("S-1");
    index.onQuoteRequest("R1", s, T0);

    // Re-register 30 seconds later — within the 60s dedupe window
    final long reregisterAt = T0 + 30L * ONE_SEC;
    final var result = index.onQuoteRequest("R1", s, reregisterAt);

    assertEquals(DUPLICATE_REQID, result);
  }

  @Test
  void onQuoteRequest_sameReqIdSameSessionAfterWindow_returnsAccepted() {
    final var s = new SessionId("S-1");
    index.onQuoteRequest("R1", s, T0);

    // Re-register 70 seconds later — outside the 60s dedupe window, entry replaced
    final long reregisterAt = T0 + 70L * ONE_SEC;
    final var result = index.onQuoteRequest("R1", s, reregisterAt);

    assertEquals(ACCEPTED, result);
    // entry count stays at 1 — replaced, not duplicated
    assertEquals(1, index.reqIdCount());
  }

  @Test
  void onQuoteRequest_sameReqIdDifferentSession_returnsAccepted() {
    final var s1 = new SessionId("S-1");
    final var s2 = new SessionId("S-2");
    index.onQuoteRequest("R1", s1, T0);

    // Different session re-using the same reqId is a different RFQ flow — must be accepted
    final var result = index.onQuoteRequest("R1", s2, T0 + ONE_SEC);

    assertEquals(ACCEPTED, result);
  }

  @Test
  void onQuoteRequest_nullReqId_throwsNPE() {
    final var s = new SessionId("S-1");
    assertThrows(NullPointerException.class, () -> index.onQuoteRequest(null, s, T0));
  }

  @Test
  void onQuoteRequest_nullSessionId_throwsNPE() {
    assertThrows(NullPointerException.class, () -> index.onQuoteRequest("R1", null, T0));
  }

  // ===========================================================================
  // Quote emission
  // ===========================================================================

  @Test
  void onQuoteEmitted_liveReqId_returnsOwningSession() {
    final var s = new SessionId("S-1");
    index.onQuoteRequest("R1", s, T0);

    final SessionId owner = index.onQuoteEmitted("R1", "Q1", T0 + 5L * ONE_SEC);

    assertEquals(s, owner);
    assertEquals(1, index.quoteIdCount());
    assertTrue(index.isOwnedBy("Q1", s));
  }

  @Test
  void onQuoteEmitted_unknownReqId_returnsNull() {
    final SessionId owner = index.onQuoteEmitted("UNKNOWN", "Q9", T0);

    assertNull(owner);
    assertEquals(0, index.quoteIdCount());
  }

  @Test
  void onQuoteEmitted_reqIdAlreadyClosedSession_returnsNull() {
    final var s = new SessionId("S-1");
    index.onSessionAuthenticated(s, "alice");
    index.onQuoteRequest("R1", s, T0);
    index.onSessionClosed(s); // eagerly evicts R1

    final SessionId owner = index.onQuoteEmitted("R1", "Q1", T0 + ONE_SEC);

    assertNull(owner);
    assertEquals(0, index.quoteIdCount());
  }

  // ===========================================================================
  // Quote ownership validation
  // ===========================================================================

  @Test
  void isOwnedBy_correctSession_returnsTrue() {
    final var s = new SessionId("S-1");
    index.onQuoteRequest("R1", s, T0);
    index.onQuoteEmitted("R1", "Q1", T0 + ONE_SEC);

    assertTrue(index.isOwnedBy("Q1", s));
  }

  @Test
  void isOwnedBy_differentSession_returnsFalse() {
    final var s1 = new SessionId("S-1");
    final var s2 = new SessionId("S-2");
    index.onQuoteRequest("R1", s1, T0);
    index.onQuoteEmitted("R1", "Q1", T0 + ONE_SEC);

    assertFalse(index.isOwnedBy("Q1", s2));
  }

  @Test
  void isOwnedBy_unknownQuoteId_returnsFalse() {
    assertFalse(index.isOwnedBy("UNKNOWN-QUOTE", new SessionId("S-1")));
  }

  @Test
  void isOwnedBy_bothNull_returnsFalse() {
    assertFalse(index.isOwnedBy(null, null));
  }

  @Test
  void isOwnedBy_nullQuoteId_returnsFalse() {
    final var s = new SessionId("S-1");
    assertFalse(index.isOwnedBy(null, s));
  }

  @Test
  void isOwnedBy_nullSessionId_returnsFalse() {
    final var s = new SessionId("S-1");
    index.onQuoteRequest("R1", s, T0);
    index.onQuoteEmitted("R1", "Q1", T0 + ONE_SEC);

    assertFalse(index.isOwnedBy("Q1", null));
  }

  // ===========================================================================
  // TTL sweep
  // ===========================================================================

  @Test
  void sweepExpired_unemittedReqIdAfter120s_isEvicted() {
    final var s = new SessionId("S-1");
    index.onQuoteRequest("R1", s, T0);

    // 1 ns before TTL boundary — must NOT be evicted
    final int evictedBefore = index.sweepExpired(T0 + REQ_ID_TTL_NANOS - 1L);
    assertEquals(0, evictedBefore);
    assertEquals(1, index.reqIdCount());

    // Exactly at TTL boundary (>=) — must be evicted
    final int evictedAt = index.sweepExpired(T0 + REQ_ID_TTL_NANOS);
    assertEquals(1, evictedAt);
    assertEquals(0, index.reqIdCount());
  }

  @Test
  void sweepExpired_emittedReqIdAfter60sFromEmission_isEvicted() {
    final var s = new SessionId("S-1");
    final long emitAt = T0 + 50L * ONE_SEC;
    index.onQuoteRequest("R1", s, T0);
    index.onQuoteEmitted("R1", "Q1", emitAt);

    // 1 ns before emission TTL boundary — must NOT be evicted
    final int evictedBefore = index.sweepExpired(emitAt + QUOTE_EMITTED_TTL_NANOS - 1L);
    assertEquals(0, evictedBefore);
    assertEquals(1, index.reqIdCount());
    assertEquals(1, index.quoteIdCount());

    // Exactly at emission TTL boundary (>=) — must be evicted
    final int evictedAt = index.sweepExpired(emitAt + QUOTE_EMITTED_TTL_NANOS);
    assertEquals(1, evictedAt);
    assertEquals(0, index.reqIdCount());
    assertEquals(0, index.quoteIdCount());
  }

  @Test
  void sweepExpired_returnsCount() {
    final var s = new SessionId("S-1");
    index.onQuoteRequest("R1", s, T0);
    index.onQuoteRequest("R2", s, T0);
    index.onQuoteRequest("R3", s, T0);

    final int evicted = index.sweepExpired(T0 + REQ_ID_TTL_NANOS);

    assertEquals(3, evicted);
    assertEquals(0, index.reqIdCount());
  }

  @Test
  void sweepExpired_emptyIndex_returnsZero() {
    assertEquals(0, index.sweepExpired(T0 + REQ_ID_TTL_NANOS));
  }

  @Test
  void sweepExpired_unrelatedEntries_unaffected() {
    final var s = new SessionId("S-1");
    // R1 registered at T0 → expires at T0 + 120s
    index.onQuoteRequest("R1", s, T0);
    // R2 registered 90s later → expires at T0 + 90s + 120s = T0 + 210s
    index.onQuoteRequest("R2", s, T0 + 90L * ONE_SEC);

    // Sweep at T0 + 120s: only R1 should expire
    final int evicted = index.sweepExpired(T0 + REQ_ID_TTL_NANOS);

    assertEquals(1, evicted);
    assertEquals(1, index.reqIdCount());
  }

  // ===========================================================================
  // Sub bucket / sessions for sub
  // ===========================================================================

  @Test
  void sessionsForSub_multipleAuthsForSameSub_allInBucket() {
    final var s1 = new SessionId("S-1");
    final var s2 = new SessionId("S-2");
    index.onSessionAuthenticated(s1, "alice");
    index.onSessionAuthenticated(s2, "alice");

    final ObjectHashSet<SessionId> bucket = index.sessionsForSub("alice");

    assertNotNull(bucket);
    assertEquals(2, bucket.size());
    assertTrue(bucket.contains(s1));
    assertTrue(bucket.contains(s2));
  }

  @Test
  void sessionsForSub_unknownSub_returnsNull() {
    assertNull(index.sessionsForSub("nobody"));
  }

  @Test
  void sessionsForSub_nullSub_returnsNull() {
    assertNull(index.sessionsForSub(null));
  }

  @Test
  void onSessionClosed_oneOfTwoSessions_bucketRetainsOther() {
    final var s1 = new SessionId("S-1");
    final var s2 = new SessionId("S-2");
    index.onSessionAuthenticated(s1, "alice");
    index.onSessionAuthenticated(s2, "alice");

    index.onSessionClosed(s1);

    final ObjectHashSet<SessionId> bucket = index.sessionsForSub("alice");
    assertNotNull(bucket);
    assertEquals(1, bucket.size());
    assertTrue(bucket.contains(s2));
    assertEquals(1, index.subCount());
  }

  @Test
  void onSessionClosed_bothSessions_subBucketRemovedAndSubCountDrops() {
    final var s1 = new SessionId("S-1");
    final var s2 = new SessionId("S-2");
    index.onSessionAuthenticated(s1, "alice");
    index.onSessionAuthenticated(s2, "alice");
    assertEquals(1, index.subCount());

    index.onSessionClosed(s1);
    assertEquals(1, index.subCount());

    index.onSessionClosed(s2);
    assertEquals(0, index.subCount());
    assertNull(index.sessionsForSub("alice"));
  }

  // ===========================================================================
  // Combined orphan-routing scenario (end-to-end)
  // ===========================================================================

  @Test
  void orphanRouting_sessionClosedBeforeQuoteEmitted_quoteEmittedReturnsNullAndSubBucketGone() {
    // Session A authenticates, sends a QuoteRequest, then disconnects before the Quote arrives.
    final var sessionA = new SessionId("S-A");
    index.onSessionAuthenticated(sessionA, "carol");
    index.onQuoteRequest("REQ-ORPHAN", sessionA, T0);

    // Session A disconnects — eager eviction of REQ-ORPHAN
    final String sub = index.onSessionClosed(sessionA);
    assertEquals("carol", sub);

    // Quote arrives for REQ-ORPHAN → must return null (orphan path)
    final SessionId owner = index.onQuoteEmitted("REQ-ORPHAN", "Q-ORPHAN", T0 + 5L * ONE_SEC);
    assertNull(owner);

    // Single-session sub → bucket gone
    assertNull(index.sessionsForSub("carol"));
    assertEquals(0, index.subCount());
    assertEquals(0, index.reqIdCount());
    assertEquals(0, index.quoteIdCount());
  }
}
