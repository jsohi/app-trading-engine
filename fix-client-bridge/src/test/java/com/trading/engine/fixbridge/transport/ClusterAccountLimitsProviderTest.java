package com.trading.engine.fixbridge.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.trading.engine.fixbridge.json.BrowserEvent;
import com.trading.engine.fixbridge.transport.ClusterAccountLimitsProvider.AccountLimitsLookup;
import java.util.HashMap;
import java.util.Map;
import org.agrona.concurrent.NanoClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ClusterAccountLimitsProvider}.
 *
 * <p>Verifies:
 *
 * <ul>
 *   <li>Known-account lookup returns the limits supplied by the underlying delegate.
 *   <li>Unknown-account lookup returns {@code null} (cluster says "not provisioned").
 *   <li>Repeat lookup within the TTL window is served from the cache (delegate not re-invoked).
 *   <li>Repeat lookup after TTL expiry re-queries the delegate.
 *   <li>Negative results are also cached so unknown accounts don't spam the cluster.
 *   <li>Concurrent constructor null-checks and TTL validation.
 *   <li>{@link ClusterAccountLimitsProvider#invalidateAll()} forces a re-query.
 * </ul>
 *
 * <p><b>Threading.</b> Single-threaded — clock and delegate are deterministic test fixtures.
 *
 * <p><b>Allocation.</b> Test-only — allocation acceptable.
 */
final class ClusterAccountLimitsProviderTest {

  // ---------------------------------------------------------------------------
  // Test doubles.
  // ---------------------------------------------------------------------------

  /**
   * Counting {@link AccountLimitsLookup} that returns a fixed map of canned limits and tracks the
   * per-account call count. Returns {@code null} for accounts not in the map.
   */
  private static final class CountingLookup implements AccountLimitsLookup {

    final Map<String, BrowserEvent.AccountLimits> table = new HashMap<>();
    final Map<String, Integer> callCount = new HashMap<>();

    void put(final String account, final BrowserEvent.AccountLimits limits) {
      table.put(account, limits);
    }

    int callsFor(final String account) {
      return callCount.getOrDefault(account, 0);
    }

    @Override
    public BrowserEvent.AccountLimits lookup(final String account) {
      callCount.merge(account, 1, Integer::sum);
      return table.get(account);
    }
  }

  /**
   * Mutable {@link NanoClock} controlled by the test. Returns whatever value {@link #set(long)} or
   * {@link #advance(long)} most recently produced.
   */
  private static final class TestClock implements NanoClock {

    private long now;

    void set(final long nanos) {
      this.now = nanos;
    }

    void advance(final long nanos) {
      this.now += nanos;
    }

    @Override
    public long nanoTime() {
      return now;
    }
  }

  // ---------------------------------------------------------------------------
  // Fixtures.
  // ---------------------------------------------------------------------------

  private static final long TTL_NANOS = 30L * 1_000_000_000L; // 30s — matches DEFAULT.
  private static final long T0 = 1_000_000_000L; // arbitrary non-zero baseline.

  private CountingLookup lookup;
  private TestClock clock;
  private ClusterAccountLimitsProvider provider;

  @BeforeEach
  void setUp() {
    lookup = new CountingLookup();
    clock = new TestClock();
    clock.set(T0);
    provider = new ClusterAccountLimitsProvider(lookup, clock, TTL_NANOS);
  }

  /** Returns a representative limits frame distinct from the pessimistic-defaults pattern. */
  private static BrowserEvent.AccountLimits sampleLimits(final String account) {
    return new BrowserEvent.AccountLimits(
        account, 500L * 100_000_000L, 5_000_000L * 100_000_000L, 25, 20);
  }

  // ---------------------------------------------------------------------------
  // Known-account lookup returns delegate value.
  // ---------------------------------------------------------------------------

  @Test
  void lookup_knownAccount_returnsDelegateValue() {
    final var limits = sampleLimits("ACME-001");
    lookup.put("ACME-001", limits);

    final var result = provider.lookup("ACME-001");

    assertNotNull(result);
    assertEquals("ACME-001", result.account());
    assertEquals(limits.maxQtyInt64(), result.maxQtyInt64());
    assertEquals(limits.maxNotionalInt64(), result.maxNotionalInt64());
    assertEquals(limits.priceDeviationBps(), result.priceDeviationBps());
    assertEquals(limits.maxOrdersPerSecond(), result.maxOrdersPerSecond());
    assertEquals(1, lookup.callsFor("ACME-001"));
  }

  // ---------------------------------------------------------------------------
  // Unknown account: provider returns null.
  // ---------------------------------------------------------------------------

  @Test
  void lookup_unknownAccount_returnsNull() {
    final var result = provider.lookup("UNKNOWN-001");

    assertNull(result, "unknown account must return null so caller can pick pessimistic defaults");
    assertEquals(1, lookup.callsFor("UNKNOWN-001"));
  }

  // ---------------------------------------------------------------------------
  // TTL cache: second lookup within window served from cache.
  // ---------------------------------------------------------------------------

  @Test
  void lookup_secondCallWithinTtl_servedFromCacheWithoutRequery() {
    final var limits = sampleLimits("ACME-001");
    lookup.put("ACME-001", limits);

    final var first = provider.lookup("ACME-001");
    // Advance just under the TTL window — entry must still be hot.
    clock.advance(TTL_NANOS - 1L);
    final var second = provider.lookup("ACME-001");

    assertSame(first, second, "cached lookup must return the exact same instance");
    assertEquals(1, lookup.callsFor("ACME-001"), "delegate must NOT be re-invoked within TTL");
  }

  @Test
  void lookup_negativeResult_alsoCachedWithinTtl() {
    final var first = provider.lookup("UNKNOWN-001");
    clock.advance(TTL_NANOS / 2);
    final var second = provider.lookup("UNKNOWN-001");

    assertNull(first);
    assertNull(second);
    assertEquals(
        1,
        lookup.callsFor("UNKNOWN-001"),
        "negative cache must prevent repeat cluster queries for unknown accounts within TTL");
  }

  // ---------------------------------------------------------------------------
  // TTL expiry: lookup after window re-queries.
  // ---------------------------------------------------------------------------

  @Test
  void lookup_afterTtlExpiry_requeriesDelegate() {
    final var first = sampleLimits("ACME-001");
    lookup.put("ACME-001", first);

    final var initial = provider.lookup("ACME-001");
    assertSame(first, initial);

    // Step the clock past the TTL boundary — entry must be stale and evicted on next access.
    clock.advance(TTL_NANOS + 1L);

    // Swap the underlying value to verify the second call really did re-query (not just return
    // the stale cached instance).
    final var refreshed = new BrowserEvent.AccountLimits("ACME-001", 999L, 999L * 100L, 99, 99);
    lookup.put("ACME-001", refreshed);

    final var second = provider.lookup("ACME-001");

    assertSame(refreshed, second, "expired cache entry must be re-fetched from the delegate");
    assertEquals(
        2,
        lookup.callsFor("ACME-001"),
        "delegate must be re-invoked exactly once after TTL expiry");
  }

  @Test
  void lookup_exactlyAtTtlBoundary_treatedAsExpired() {
    // The provider's contract: an entry expires when now >= expiresAtNanos. At now == expiresAt
    // the cached entry is no longer valid and the delegate must be re-invoked.
    final var first = sampleLimits("ACME-001");
    lookup.put("ACME-001", first);

    provider.lookup("ACME-001");
    clock.advance(TTL_NANOS);

    provider.lookup("ACME-001");

    assertEquals(2, lookup.callsFor("ACME-001"), "boundary at TTL must count as expired");
  }

  // ---------------------------------------------------------------------------
  // Cache eviction: stale entry removed on access (cache stays bounded).
  // ---------------------------------------------------------------------------

  @Test
  void lookup_staleEntry_evictedOnAccessSoCacheStaysBounded() {
    lookup.put("ACME-001", sampleLimits("ACME-001"));

    provider.lookup("ACME-001");
    assertEquals(1, provider.cacheSize());

    clock.advance(TTL_NANOS + 1L);
    // Drop the table entry so the next lookup returns null (negative); we still expect the
    // stale positive entry to be removed and replaced rather than retained.
    lookup.table.remove("ACME-001");
    provider.lookup("ACME-001");

    // After eviction + re-fetch the cache should hold exactly one entry (the new negative one).
    assertEquals(1, provider.cacheSize());
  }

  // ---------------------------------------------------------------------------
  // invalidateAll forces a re-query.
  // ---------------------------------------------------------------------------

  @Test
  void invalidateAll_clearsCacheAndForcesRequery() {
    lookup.put("ACME-001", sampleLimits("ACME-001"));

    provider.lookup("ACME-001");
    provider.invalidateAll();
    provider.lookup("ACME-001");

    assertEquals(2, lookup.callsFor("ACME-001"));
  }

  // ---------------------------------------------------------------------------
  // Constructor validation.
  // ---------------------------------------------------------------------------

  @Test
  void constructor_nullDelegate_throws() {
    assertThrows(NullPointerException.class, () -> new ClusterAccountLimitsProvider(null));
  }

  @Test
  void constructor_nullClock_throws() {
    assertThrows(
        NullPointerException.class,
        () -> new ClusterAccountLimitsProvider(lookup, null, TTL_NANOS));
  }

  @Test
  void constructor_nonPositiveTtl_throws() {
    assertThrows(
        IllegalArgumentException.class, () -> new ClusterAccountLimitsProvider(lookup, clock, 0L));
    assertThrows(
        IllegalArgumentException.class, () -> new ClusterAccountLimitsProvider(lookup, clock, -1L));
  }

  @Test
  void lookup_nullAccount_throws() {
    assertThrows(NullPointerException.class, () -> provider.lookup(null));
  }

  // ---------------------------------------------------------------------------
  // Multiple accounts coexist in cache without cross-talk.
  // ---------------------------------------------------------------------------

  @Test
  void lookup_multipleAccounts_eachCachedIndependently() {
    final var a = sampleLimits("ACME-001");
    final var b = sampleLimits("ACME-002");
    lookup.put("ACME-001", a);
    lookup.put("ACME-002", b);

    final var firstA = provider.lookup("ACME-001");
    final var firstB = provider.lookup("ACME-002");
    final var secondA = provider.lookup("ACME-001");
    final var secondB = provider.lookup("ACME-002");

    assertSame(a, firstA);
    assertSame(b, firstB);
    assertSame(a, secondA);
    assertSame(b, secondB);
    assertEquals(1, lookup.callsFor("ACME-001"));
    assertEquals(1, lookup.callsFor("ACME-002"));
  }
}
