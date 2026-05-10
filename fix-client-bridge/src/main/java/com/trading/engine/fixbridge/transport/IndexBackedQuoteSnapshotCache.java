package com.trading.engine.fixbridge.transport;

import com.trading.engine.fixbridge.quote.SessionId;
import com.trading.engine.fixbridge.quote.SessionQuoteIndex;
import com.trading.engine.fixbridge.translator.QuoteSnapshot;
import java.nio.charset.StandardCharsets;
import org.agrona.collections.Object2ObjectHashMap;

/**
 * Production {@link QuoteSnapshotCache} impl backed by a per-session map of {@code quoteId} &rarr;
 * {@link QuoteSnapshot}. Bridges the orchestrator-side push path (a dealer {@code Quote} arrives
 * from the cluster &rarr; the bridge calls {@link #stash(String, QuoteSnapshot)}) to the
 * browser-side pull path ({@code AcceptQuote} arrives from the WebSocket &rarr; {@link
 * ArtioFixCommandSink#sendAcceptQuote} calls {@link #lookup(byte[], int, int)} to retrieve the
 * snapshot for FIX {@code NewOrderSingle (35=D)} translation, then {@link #evict(byte[], int, int)}
 * after a successful send).
 *
 * <p><b>Per-session ownership.</b> Each authenticated browser session gets its own instance, paired
 * 1:1 with the {@link SessionId} captured at JWT-auth-success time. The orchestrator-side push path
 * resolves the target session via {@link SessionQuoteIndex#onQuoteEmitted} (which returns the
 * owning {@link SessionId}); the launcher then routes the {@code stash} call to the cache instance
 * bound to that session. The {@link SessionQuoteIndex} reference held by this cache is used solely
 * as defence-in-depth to assert that the cache's owning session still owns the quoteId at lookup
 * time — the upstream dispatcher has already validated ownership via {@link
 * SessionQuoteIndex#isOwnedBy(String, SessionId)}, but a redundant check here closes the race where
 * ownership is revoked (e.g. session closed by another thread of orchestration) between the
 * dispatcher's check and the lookup.
 *
 * <p><b>Snapshot type.</b> The {@link QuoteSnapshotCache} interface contract returns the strongly
 * typed {@link QuoteSnapshot} (not raw {@code byte[]}) — the snapshot carries pre-parsed structural
 * fields ({@code symbol}, {@code side}, {@code qty}, {@code bid}, {@code ask}, {@code expiryNs})
 * needed by {@link com.trading.engine.fixbridge.translator.JsonToFixTranslator JsonToFixTranslator}
 * to build the FIX {@code NewOrderSingle}. Storing the typed snapshot avoids a re-parse on the
 * AcceptQuote hot path.
 *
 * <p><b>Threading.</b> NOT thread-safe. Owned exclusively by the per-session Netty channel event
 * loop. The orchestrator-side push code that calls {@link #stash} MUST hop onto the owning
 * channel's event loop before invoking this cache (this is a launcher-level invariant — the cache
 * itself does no thread-confinement enforcement to keep the hot path branch-free).
 *
 * <p><b>Allocation.</b>
 *
 * <ul>
 *   <li>{@link #lookup(byte[], int, int)} allocates one {@code String} on every call (key
 *       materialisation from the byte slice). The Agrona {@link Object2ObjectHashMap} cannot look
 *       up by byte-slice without a key object. This allocation is acceptable on the AcceptQuote
 *       path because AcceptQuote is rate-limited at the dispatcher (low-frequency human-driven
 *       action) and the alternative — a custom {@code byte[]}-keyed open-addressing map — would add
 *       ~200 LOC and a maintenance burden disproportionate to a couple-of-Hz call rate. Rationale
 *       documented for future profile-driven optimisation: see {@code AGENTS.md} (TODO when filed).
 *   <li>{@link #evict(byte[], int, int)} allocates one {@code String} per call for the same reason.
 *       Same rate-limit rationale applies.
 *   <li>{@link #stash(String, QuoteSnapshot)} allocates zero — the caller already holds the {@code
 *       String} {@code quoteId} (deserialised from the {@code Quote} event) and the {@link
 *       QuoteSnapshot} reference (pooled by the orchestrator-side push path). The map insert reuses
 *       Agrona's open-addressing slots without per-entry node allocation.
 * </ul>
 *
 * <p><b>Lifecycle.</b> One instance per authenticated session. Allocated by the launcher at
 * auth-success; released to GC when the channel closes. Internal map capacity is sized to the
 * typical concurrent-quotes-per-session ceiling ({@link #DEFAULT_INITIAL_CAPACITY}); the map grows
 * beyond that if needed (Agrona handles resize) — an unusual path that warrants a metrics counter
 * in production (TODO: APP-40 metrics phase).
 *
 * <p><b>Dependencies.</b> {@link SessionQuoteIndex}, {@link SessionId}, {@link QuoteSnapshot},
 * Agrona {@link Object2ObjectHashMap}.
 *
 * @see QuoteSnapshotCache
 * @see SessionQuoteIndex
 * @see ArtioFixCommandSink
 */
public final class IndexBackedQuoteSnapshotCache implements QuoteSnapshotCache {

  /**
   * Initial bucket count for the per-session quote map. Sized to the expected steady-state
   * concurrent-quotes-per-session ceiling (a single user typically has &le;16 in-flight RFQs at
   * once; 64 leaves comfortable headroom before Agrona's first resize). Powers-of-two are preferred
   * for open-addressing implementations.
   */
  public static final int DEFAULT_INITIAL_CAPACITY = 64;

  /** Global correlation index — used for defence-in-depth ownership validation at lookup time. */
  private final SessionQuoteIndex sessionQuoteIndex;

  /** This cache's owning session — captured at construction; never mutated. */
  private final SessionId owningSession;

  /**
   * Per-session quoteId &rarr; snapshot map. Open-addressing (no per-entry node allocation).
   * Mutated by {@link #stash}, {@link #evict}, and (indirectly) by {@link #lookup}'s defensive
   * eviction-on-stale-ownership path.
   */
  private final Object2ObjectHashMap<String, QuoteSnapshot> quoteToSnapshot;

  /**
   * Constructs a per-session snapshot cache with {@link #DEFAULT_INITIAL_CAPACITY} initial
   * capacity.
   *
   * @param sessionQuoteIndex global correlation index; must be non-null. Used for defence-in- depth
   *     ownership validation at lookup time.
   * @param owningSession the session that owns this cache; must be non-null
   * @throws NullPointerException if either argument is null
   */
  public IndexBackedQuoteSnapshotCache(
      final SessionQuoteIndex sessionQuoteIndex, final SessionId owningSession) {
    this(sessionQuoteIndex, owningSession, DEFAULT_INITIAL_CAPACITY);
  }

  /**
   * Constructs a per-session snapshot cache with an explicit initial capacity. Used by tests that
   * want to assert resize-free behaviour at a specific size.
   *
   * @param sessionQuoteIndex global correlation index; must be non-null
   * @param owningSession the session that owns this cache; must be non-null
   * @param initialCapacity initial map capacity; must be positive
   * @throws NullPointerException if a reference argument is null
   * @throws IllegalArgumentException if {@code initialCapacity <= 0}
   */
  public IndexBackedQuoteSnapshotCache(
      final SessionQuoteIndex sessionQuoteIndex,
      final SessionId owningSession,
      final int initialCapacity) {
    if (sessionQuoteIndex == null) {
      throw new NullPointerException("sessionQuoteIndex must not be null");
    }
    if (owningSession == null) {
      throw new NullPointerException("owningSession must not be null");
    }
    if (initialCapacity <= 0) {
      throw new IllegalArgumentException("initialCapacity must be positive: " + initialCapacity);
    }
    this.sessionQuoteIndex = sessionQuoteIndex;
    this.owningSession = owningSession;
    this.quoteToSnapshot = new Object2ObjectHashMap<>(initialCapacity, 0.65f);
  }

  /**
   * Stash a quote snapshot for later {@link #lookup} by the AcceptQuote path. Called by the
   * orchestrator-side push code when a dealer {@code Quote} has been routed to this session.
   *
   * <p>Idempotent: re-stashing the same {@code quoteId} replaces the prior snapshot. Callers should
   * not normally re-stash — quoteIds are globally unique per {@code OrchestratorIdGenerator}'s
   * restart-safe counter — but the map's last-write-wins semantics keep the cache safe under
   * accidental dual-stash.
   *
   * @param quoteId server-allocated quote id (globally unique); must be non-null
   * @param snapshot the bound {@link QuoteSnapshot}; must be non-null. The cache stores the
   *     reference verbatim — callers must not mutate the snapshot after the call returns and before
   *     the matching {@link #lookup} / {@link #evict}. Snapshots are typically managed by a
   *     per-session pool and the stash/evict pair governs the borrow window.
   * @throws NullPointerException if either argument is null
   */
  public void stash(final String quoteId, final QuoteSnapshot snapshot) {
    if (quoteId == null) {
      throw new NullPointerException("quoteId must not be null");
    }
    if (snapshot == null) {
      throw new NullPointerException("snapshot must not be null");
    }
    quoteToSnapshot.put(quoteId, snapshot);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Materialises the byte-slice into a {@code String} key (one allocation), looks up the stashed
   * snapshot, and — when found — verifies via {@link SessionQuoteIndex#isOwnedBy} that the cache's
   * {@link #owningSession} still owns the quoteId. A defence-in-depth ownership mismatch (the
   * upstream dispatcher should have caught this) results in eager eviction of the stale entry and a
   * {@code null} return — the AcceptQuote path treats this as a cache miss and emits {@code
   * OrderReject{quote-not-owned}} (handled upstream).
   *
   * @return the bound {@link QuoteSnapshot} on a hit owned by this session; {@code null} on miss or
   *     stale-ownership eviction
   */
  @Override
  public QuoteSnapshot lookup(final byte[] buf, final int off, final int len) {
    if (buf == null || len <= 0) {
      return null;
    }
    final var quoteId = new String(buf, off, len, StandardCharsets.US_ASCII);
    final var snapshot = quoteToSnapshot.get(quoteId);
    if (snapshot == null) {
      return null;
    }
    // Defence-in-depth: confirm the cache's owning session still owns the quoteId. If
    // SessionQuoteIndex has revoked ownership (session closed, TTL expired) between the
    // dispatcher's prior check and now, evict the stale entry and treat as a miss.
    if (!sessionQuoteIndex.isOwnedBy(quoteId, owningSession)) {
      quoteToSnapshot.remove(quoteId);
      return null;
    }
    return snapshot;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Materialises the byte-slice into a {@code String} key (one allocation) and removes the entry
   * from the per-session map. Safe to call when the entry is absent (no-op).
   */
  @Override
  public void evict(final byte[] buf, final int off, final int len) {
    if (buf == null || len <= 0) {
      return;
    }
    final var quoteId = new String(buf, off, len, StandardCharsets.US_ASCII);
    quoteToSnapshot.remove(quoteId);
  }

  // ---------------------------------------------------------------------------
  // Diagnostics — used by tests and metrics.
  // ---------------------------------------------------------------------------

  /**
   * Number of entries currently stashed. Used for metrics and to assert eager-eviction invariants
   * in tests.
   *
   * @return current entry count
   */
  public int size() {
    return quoteToSnapshot.size();
  }

  /**
   * The session that owns this cache; never null after construction.
   *
   * @return the owning session
   */
  public SessionId owningSession() {
    return owningSession;
  }
}
