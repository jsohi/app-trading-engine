package com.trading.engine.fixbridge.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.trading.engine.fixbridge.json.MutableParsedMessage;
import com.trading.engine.fixbridge.quote.SessionId;
import com.trading.engine.fixbridge.quote.SessionQuoteIndex;
import com.trading.engine.fixbridge.quote.SessionQuoteIndex.QuoteRequestRegistration;
import com.trading.engine.fixbridge.translator.QuoteSnapshot;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link IndexBackedQuoteSnapshotCache}.
 *
 * <p>Verifies the per-session cache:
 *
 * <ul>
 *   <li>Stash &rarr; lookup returns the same snapshot reference.
 *   <li>Lookup miss returns {@code null}.
 *   <li>Evict removes the slot; subsequent lookup returns {@code null}.
 *   <li>Stash &rarr; lookup &rarr; evict round-trip mirrors the AcceptQuote production path.
 *   <li>Defence-in-depth: lookup returns {@code null} and evicts the stale entry when {@link
 *       SessionQuoteIndex} no longer reports the quoteId as owned by this cache's session (race
 *       with session-close).
 *   <li>Constructor null/invalid argument validation.
 * </ul>
 *
 * <p><b>Threading.</b> Single-threaded — test-only.
 *
 * <p><b>Allocation.</b> Test-only — allocation is acceptable.
 */
final class IndexBackedQuoteSnapshotCacheTest {

  private static final SessionId OWNER = new SessionId("session-cache-owner");
  private static final SessionId OTHER = new SessionId("session-cache-other");
  private static final String SUB = "user-cache-test";

  private SessionQuoteIndex index;
  private IndexBackedQuoteSnapshotCache cache;

  @BeforeEach
  void setUp() {
    index = new SessionQuoteIndex();
    index.onSessionAuthenticated(OWNER, SUB);
    cache = new IndexBackedQuoteSnapshotCache(index, OWNER);
  }

  // ---------------------------------------------------------------------------
  // Helpers.
  // ---------------------------------------------------------------------------

  /** Build a bound {@link QuoteSnapshot} with canned EURUSD values. */
  private static QuoteSnapshot snapshot() {
    final var snap = new QuoteSnapshot();
    final byte[] sym = "EURUSD".getBytes(StandardCharsets.US_ASCII);
    snap.bind(
        sym,
        0,
        sym.length,
        MutableParsedMessage.SIDE_BUY,
        /* qtyInt64 */ 100_000_000L,
        /* bidValue */ 110_000_000L,
        /* bidScale */ 8,
        /* askValue */ 110_100_000L,
        /* askScale */ 8,
        /* expiryNs */ Long.MAX_VALUE);
    return snap;
  }

  /**
   * Register {@code quoteId} as owned by {@link #OWNER} in the {@link SessionQuoteIndex}. Mirrors
   * the production {@code QuoteRequest -> Quote} flow that binds (reqId, quoteId, sessionId).
   */
  private void registerOwnership(final String reqId, final String quoteId) {
    final var registration = index.onQuoteRequest(reqId, OWNER, /* nowNs */ 1_000L);
    assertEquals(QuoteRequestRegistration.ACCEPTED, registration);
    final var owner = index.onQuoteEmitted(reqId, quoteId, /* nowNs */ 2_000L);
    assertEquals(OWNER, owner, "test setup must establish OWNER as owner of quoteId");
  }

  /** Convert a quoteId String into the byte-slice form expected by the cache API. */
  private static byte[] bytes(final String s) {
    return s.getBytes(StandardCharsets.US_ASCII);
  }

  // ---------------------------------------------------------------------------
  // Stash + lookup hit.
  // ---------------------------------------------------------------------------

  @Test
  void stashThenLookup_returnsSameSnapshotReference() {
    registerOwnership("R-1", "Q-1");
    final var snap = snapshot();
    cache.stash("Q-1", snap);

    final byte[] key = bytes("Q-1");
    final var hit = cache.lookup(key, 0, key.length);

    assertSame(snap, hit, "lookup must return the same snapshot reference that was stashed");
    assertEquals(1, cache.size(), "cache size reflects the single stash");
  }

  @Test
  void stashThenLookup_byteSliceWithOffset_resolvesCorrectKey() {
    // Lookup must respect the (off, len) window — common production case where the quoteId
    // bytes live inside a larger scratch buffer (e.g. MutableParsedMessage.scratch).
    registerOwnership("R-2", "Q-2");
    final var snap = snapshot();
    cache.stash("Q-2", snap);

    final byte[] padded = "PREFIX:Q-2:SUFFIX".getBytes(StandardCharsets.US_ASCII);
    final var hit = cache.lookup(padded, 7, 3); // "Q-2" sits at off=7 len=3

    assertSame(snap, hit, "lookup must materialise the key from (off, len) slice");
  }

  // ---------------------------------------------------------------------------
  // Lookup miss.
  // ---------------------------------------------------------------------------

  @Test
  void lookup_unknownQuoteId_returnsNull() {
    final byte[] key = bytes("NOT-PRESENT");
    assertNull(cache.lookup(key, 0, key.length), "miss must return null");
    assertEquals(0, cache.size(), "miss must not mutate cache size");
  }

  @Test
  void lookup_nullBuffer_returnsNullSafely() {
    assertNull(cache.lookup(null, 0, 0));
  }

  @Test
  void lookup_zeroLength_returnsNullSafely() {
    assertNull(cache.lookup(new byte[0], 0, 0));
  }

  // ---------------------------------------------------------------------------
  // Evict.
  // ---------------------------------------------------------------------------

  @Test
  void evict_existingEntry_removesSlot() {
    registerOwnership("R-3", "Q-3");
    cache.stash("Q-3", snapshot());
    assertEquals(1, cache.size());

    final byte[] key = bytes("Q-3");
    cache.evict(key, 0, key.length);

    assertEquals(0, cache.size(), "evict must remove the slot");
    assertNull(cache.lookup(key, 0, key.length), "subsequent lookup must miss");
  }

  @Test
  void evict_absentEntry_noOpSafe() {
    final byte[] key = bytes("NEVER-STASHED");
    cache.evict(key, 0, key.length); // must not throw
    assertEquals(0, cache.size());
  }

  @Test
  void evict_nullBuffer_noOpSafe() {
    cache.evict(null, 0, 0); // must not throw
  }

  // ---------------------------------------------------------------------------
  // Round-trip — mirrors the production AcceptQuote two-phase commit path.
  // ---------------------------------------------------------------------------

  @Test
  void stashLookupEvictRoundTrip_mirrorsProductionAcceptQuoteFlow() {
    registerOwnership("R-4", "Q-4");
    final var snap = snapshot();

    // 1) Push path: orchestrator side stashes after Quote arrives.
    cache.stash("Q-4", snap);
    assertEquals(1, cache.size());

    // 2) Pull path: AcceptQuote arrives — lookup retrieves the snapshot.
    final byte[] key = bytes("Q-4");
    final var pulled = cache.lookup(key, 0, key.length);
    assertSame(snap, pulled, "pull must see the pushed snapshot");

    // 3) After successful Session.trySend the bridge evicts the slot.
    cache.evict(key, 0, key.length);
    assertEquals(0, cache.size());
    assertNull(cache.lookup(key, 0, key.length), "post-evict lookup must miss");
  }

  // ---------------------------------------------------------------------------
  // Defence-in-depth: ownership revoked between stash and lookup.
  // ---------------------------------------------------------------------------

  @Test
  void lookup_ownershipRevokedAfterStash_evictsAndReturnsNull() {
    // Set up: OWNER owns Q-5; stash a snapshot.
    registerOwnership("R-5", "Q-5");
    cache.stash("Q-5", snapshot());

    // Race: OWNER session closes; SessionQuoteIndex revokes ownership of Q-5.
    index.onSessionClosed(OWNER);

    // Defence-in-depth: cache lookup must not return a snapshot whose ownership has been revoked.
    final byte[] key = bytes("Q-5");
    final var hit = cache.lookup(key, 0, key.length);

    assertNull(hit, "lookup must return null when ownership has been revoked");
    assertEquals(0, cache.size(), "lookup must eagerly evict the stale entry");
  }

  @Test
  void lookup_quoteOwnedByDifferentSession_returnsNull() {
    // Set up: OTHER session owns Q-6 in the index; OWNER's cache stashes a snapshot for Q-6
    // (a hypothetical mis-routing). Defence-in-depth must catch the cross-session leak.
    index.onSessionAuthenticated(OTHER, SUB);
    final var registration = index.onQuoteRequest("R-6", OTHER, /* nowNs */ 1_000L);
    assertEquals(QuoteRequestRegistration.ACCEPTED, registration);
    final var owner = index.onQuoteEmitted("R-6", "Q-6", /* nowNs */ 2_000L);
    assertEquals(OTHER, owner);

    cache.stash("Q-6", snapshot()); // OWNER's cache mistakenly stashes Q-6
    final byte[] key = bytes("Q-6");
    final var hit = cache.lookup(key, 0, key.length);

    assertNull(hit, "cross-session leak must not be served");
    assertEquals(0, cache.size(), "stale entry must be evicted");
  }

  // ---------------------------------------------------------------------------
  // Stash idempotency / overwrite semantics.
  // ---------------------------------------------------------------------------

  @Test
  void stash_sameQuoteIdTwice_lastWriteWins() {
    registerOwnership("R-7", "Q-7");
    final var first = snapshot();
    final var second = snapshot();

    cache.stash("Q-7", first);
    cache.stash("Q-7", second);

    final byte[] key = bytes("Q-7");
    assertSame(second, cache.lookup(key, 0, key.length), "second stash must overwrite first");
    assertEquals(1, cache.size(), "duplicate stash must not double-count");
  }

  // ---------------------------------------------------------------------------
  // Constructor argument validation.
  // ---------------------------------------------------------------------------

  @Test
  void constructor_nullIndex_throwsNpe() {
    final var npe =
        assertThrows(
            NullPointerException.class, () -> new IndexBackedQuoteSnapshotCache(null, OWNER));
    assertNotNull(npe.getMessage());
  }

  @Test
  void constructor_nullSession_throwsNpe() {
    final var npe =
        assertThrows(
            NullPointerException.class, () -> new IndexBackedQuoteSnapshotCache(index, null));
    assertNotNull(npe.getMessage());
  }

  @Test
  void constructor_zeroCapacity_throwsIae() {
    assertThrows(
        IllegalArgumentException.class, () -> new IndexBackedQuoteSnapshotCache(index, OWNER, 0));
  }

  @Test
  void constructor_negativeCapacity_throwsIae() {
    assertThrows(
        IllegalArgumentException.class, () -> new IndexBackedQuoteSnapshotCache(index, OWNER, -1));
  }

  @Test
  void stash_nullQuoteId_throwsNpe() {
    assertThrows(NullPointerException.class, () -> cache.stash(null, snapshot()));
  }

  @Test
  void stash_nullSnapshot_throwsNpe() {
    assertThrows(NullPointerException.class, () -> cache.stash("Q-X", null));
  }

  // ---------------------------------------------------------------------------
  // Diagnostics.
  // ---------------------------------------------------------------------------

  @Test
  void owningSession_returnsConstructorArgument() {
    assertEquals(OWNER, cache.owningSession());
  }
}
