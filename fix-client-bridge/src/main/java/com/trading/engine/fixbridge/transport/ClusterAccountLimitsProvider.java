package com.trading.engine.fixbridge.transport;

import com.trading.engine.fixbridge.json.BrowserEvent;
import java.util.Objects;
import org.agrona.collections.Object2ObjectHashMap;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SystemNanoClock;

/**
 * Production {@link AccountLimitsProvider} backed by a cluster-side per-account limits lookup
 * (typically a CQRS read-model projection over {@code RiskLimitLoadedEvent} (110/115) reached via
 * the {@code :query-service} module). Adds a short TTL cache to coalesce repeat lookups during
 * authentication storms (multi-tab logins, browser refresh loops) so a single hot account doesn't
 * generate one cluster round-trip per AUTH_SUCCESS.
 *
 * <h2>Cluster query path — present state</h2>
 *
 * <p>The cluster has the data ({@code RiskLimitLoadedEvent} template 115 + {@code
 * RiskLimitChangedEvent} template 119 in {@code messages/src/main/resources/trading-schema.xml})
 * and as of APP-62 R11 the read-model {@code RiskLimitProjection} ({@code
 * projections/src/main/java/com/trading/engine/projections/risklimits/RiskLimitProjection.java})
 * consumes templates 115/119 and exposes per-account limits via {@link
 * com.trading.engine.queryservice.QueryService#getAccountLimits(String)} returning {@code
 * RiskLimitRecordView}.
 *
 * <ul>
 *   <li>The {@code RiskLimitRecordView} surface carries {@code maxOrderSize}, {@code
 *       maxOrderNotional}, {@code priceDeviationBps} and {@code maxOrdersPerSecond} — all four of
 *       the {@link BrowserEvent.AccountLimits} fields. The {@link RiskLimitToBrowserAdapter}
 *       performs the per-field narrowing from cluster-side {@code long} to browser-side {@code int}
 *       (clamping at {@link Integer#MAX_VALUE}).
 *   <li>The remaining gap is launcher-binding: a top-level process is responsible for wiring {@code
 *       QueryService::getAccountLimits} into this provider's {@link AccountLimitsLookup} SAM. Until
 *       that wiring lands the bridge's runtime path falls through to the in-memory default provider
 *       (see {@link BoundedAccountLimitsSource}).
 * </ul>
 *
 * <p>This provider is implemented against the minimal {@link AccountLimitsLookup} SAM so the
 * binding swap is a single launcher edit with no module-graph changes to {@code
 * :fix-client-bridge}. See TODO(APP-62) immediately below for the exact binding contract.
 *
 * <h2>Threading</h2>
 *
 * <p>Thread-safe. {@link #lookup(String)} may be called concurrently from any Netty event loop (the
 * auth handler invokes it on its per-channel worker). All cache access is guarded by the monitor on
 * {@code this}; the synchronized region is short (one map probe + at most one map put) and no I/O
 * occurs while holding the monitor — the underlying {@link AccountLimitsLookup} is called outside
 * the monitor on a cache miss to avoid serializing cluster round-trips.
 *
 * <p>The threading model of the underlying {@link AccountLimitsLookup} is the launcher's concern.
 * If it dispatches onto an Aeron client thread it must do so with its own thread-safety guarantees;
 * this class makes no assumptions beyond "{@code lookup} returns a value or {@code null} without
 * retaining the caller thread".
 *
 * <h2>Allocation</h2>
 *
 * <p>Cold path — invoked once per {@code AUTH_SUCCESS} per account. Per-call allocation is
 * acceptable. Steady-state allocations are bounded:
 *
 * <ul>
 *   <li>One {@link CachedLimits} record per unique account in the TTL window (held via {@link
 *       Object2ObjectHashMap}, no boxing on the value side).
 *   <li>Zero allocation on a cache hit path beyond the {@link CachedLimits} dereference.
 * </ul>
 *
 * <h2>Caching policy</h2>
 *
 * <p>30-second TTL by default ({@link #DEFAULT_TTL_NANOS}). Both positive (limits found) and
 * negative ({@code null} — account not provisioned) lookups are cached. Negative caching prevents a
 * misconfigured or malicious client from hammering the cluster query path with unknown account
 * codes; positive caching coalesces repeat AUTHs for the same account during the TTL window.
 *
 * <p>The cache uses lazy eviction: stale entries are not proactively removed on a timer (avoids an
 * extra thread / scheduling complexity for a cold-path cache). Instead, each {@link #lookup} checks
 * the entry's {@link CachedLimits#expiresAtNanos} and evicts on access if expired. Worst- case
 * memory growth is bounded by the cardinality of accounts looked up within any 30-second window —
 * typically dozens, never thousands, since each request maps to an entitled-account list of a
 * single AUTH success.
 *
 * <h2>Lifecycle</h2>
 *
 * <p>Per-process singleton. Constructed at boot by the launcher; never closed. The cache holds only
 * immutable record references and is GC-friendly on process shutdown.
 *
 * @see AccountLimitsProvider
 * @see AccountLimitsLookup
 * @see BoundedAccountLimitsSource
 */
// TODO(APP-62): wire the production binding in the top-level launcher:
//     new ClusterAccountLimitsProvider(
//         accountCode -> {
//           final var view = queryService.getAccountLimits(accountCode);
//           if (view == null) return null;
//           return RiskLimitToBrowserAdapter.toBrowserLimits(
//               accountCode,
//               view.maxOrderSize(),
//               view.maxOrderNotional(),
//               view.priceDeviationBps(),
//               view.maxOrdersPerSecond());
//         });
//   Until this wiring lands, the JWT cold-path auth handler resolves to the in-memory
//   BoundedAccountLimitsSource. Tracked as the same gap as RiskLimitToBrowserAdapter (APP-62 R11
//   MEDIUM Agent B #1) — the adapter, provider and projection all exist; only the launcher
//   binding is outstanding.
public final class ClusterAccountLimitsProvider implements AccountLimitsProvider {

  /**
   * Default TTL for cached lookups: 30 seconds. Long enough to coalesce a multi-tab AUTH burst,
   * short enough that an operator-driven limits change propagates within a minute.
   */
  public static final long DEFAULT_TTL_NANOS = 30L * 1_000_000_000L;

  /**
   * Minimal SAM for cluster-side per-account limits lookup. The launcher provides the impl —
   * typically backed by {@code QueryService.getAccountLimits(String)} (now exposed as of APP-62
   * R11; see class-level Javadoc and the TODO(APP-62) binding sketch). Returning {@code null}
   * indicates the account is not provisioned in the cluster's read-model.
   *
   * <p>The SAM lives next to the provider rather than in {@code :query-service} to keep the
   * bridge's transport package self-contained — adding a {@code :query-service} dependency to
   * {@code :fix-client-bridge} is a larger module-graph change which we defer until the real query
   * surface lands.
   */
  @FunctionalInterface
  public interface AccountLimitsLookup {

    /**
     * Look up the current pre-trade limits for the named account.
     *
     * @param account FIX {@code Account (1)} identifier; never {@code null}
     * @return populated {@link BrowserEvent.AccountLimits}, or {@code null} when the account is not
     *     provisioned
     */
    BrowserEvent.AccountLimits lookup(String account);
  }

  /**
   * Cache entry pairing the looked-up value with its expiry timestamp. Both positive and negative
   * ({@code value == null}) results are cached so repeated lookups for unknown accounts also hit
   * the cache.
   *
   * @param value the cached {@link BrowserEvent.AccountLimits}, or {@code null} for a negative
   *     cache entry (account not provisioned)
   * @param expiresAtNanos monotonic nanos at which this entry becomes stale
   */
  private record CachedLimits(BrowserEvent.AccountLimits value, long expiresAtNanos) {}

  private final AccountLimitsLookup delegate;
  private final NanoClock clock;
  private final long ttlNanos;
  private final Object2ObjectHashMap<String, CachedLimits> cache = new Object2ObjectHashMap<>();

  /**
   * Constructs a provider with the {@link #DEFAULT_TTL_NANOS default 30-second TTL} and the shared
   * system monotonic clock.
   *
   * @param delegate cluster-side limits lookup; never {@code null}
   * @throws NullPointerException if {@code delegate} is {@code null}
   */
  public ClusterAccountLimitsProvider(final AccountLimitsLookup delegate) {
    this(delegate, SystemNanoClock.INSTANCE, DEFAULT_TTL_NANOS);
  }

  /**
   * Constructs a provider with explicit clock and TTL — used by tests to advance time
   * deterministically and to validate cache behavior at a sub-second resolution.
   *
   * @param delegate cluster-side limits lookup; never {@code null}
   * @param clock monotonic nano clock used for TTL accounting; never {@code null}
   * @param ttlNanos cache TTL in nanoseconds; must be {@code > 0}
   * @throws NullPointerException if {@code delegate} or {@code clock} is {@code null}
   * @throws IllegalArgumentException if {@code ttlNanos <= 0}
   */
  public ClusterAccountLimitsProvider(
      final AccountLimitsLookup delegate, final NanoClock clock, final long ttlNanos) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.clock = Objects.requireNonNull(clock, "clock");
    if (ttlNanos <= 0L) {
      throw new IllegalArgumentException("ttlNanos must be positive: " + ttlNanos);
    }
    this.ttlNanos = ttlNanos;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Cache-first lookup. On hit (entry present and not expired), returns the cached value without
   * consulting the delegate. On miss (entry absent or expired), invokes the delegate outside the
   * cache monitor, then stores the result with a fresh expiry. Both positive and negative results
   * are cached.
   *
   * @param account FIX {@code Account (1)} identifier; never {@code null}
   * @return cached or freshly-fetched {@link BrowserEvent.AccountLimits}, or {@code null} when the
   *     account is not provisioned
   * @throws NullPointerException if {@code account} is {@code null}
   */
  @Override
  public BrowserEvent.AccountLimits lookup(final String account) {
    Objects.requireNonNull(account, "account");

    final long now = clock.nanoTime();

    // Fast path: cache hit under the monitor.
    synchronized (this) {
      final var cached = cache.get(account);
      if (cached != null) {
        if (cached.expiresAtNanos > now) {
          return cached.value;
        }
        // Lazy eviction of stale entry — keeps the map bounded without a separate sweeper.
        cache.remove(account);
      }
    }

    // Miss: call the delegate outside the monitor so a slow cluster round-trip does not
    // serialize parallel lookups for different accounts. A small race here is acceptable: two
    // concurrent misses for the same account may both hit the delegate; the second put just
    // overwrites the first with an equivalent value.
    final var fresh = delegate.lookup(account);
    final long expiresAt = now + ttlNanos;

    synchronized (this) {
      cache.put(account, new CachedLimits(fresh, expiresAt));
    }
    return fresh;
  }

  /**
   * Returns the current cache size. Test-visibility hook for verifying cache eviction; not part of
   * the {@link AccountLimitsProvider} contract.
   *
   * @return number of entries currently held (including stale-but-not-yet-evicted ones)
   */
  synchronized int cacheSize() {
    return cache.size();
  }

  /**
   * Clears the cache. Intended for tests and for operator-driven invalidation hooks (e.g., on
   * receipt of a {@code RiskLimitLoadedEvent} broadcast). Not on the hot path.
   */
  public synchronized void invalidateAll() {
    cache.clear();
  }
}
