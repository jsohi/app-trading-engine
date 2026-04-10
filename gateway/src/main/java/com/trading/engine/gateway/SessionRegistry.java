package com.trading.engine.gateway;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import uk.co.real_logic.artio.session.Session;

/**
 * Central session registry that maps FIX correlation identifiers (ClOrdID, QuoteReqID) to gateway
 * session keys and session keys to Artio {@link Session} objects. Implements {@link SessionLookup}
 * for use by {@link ClusterEgressListener}.
 *
 * <p><b>Two maps.</b> The egress pipeline needs two lookups:
 *
 * <ol>
 *   <li>{@code correlationHash → sessionKey} — used by {@link ClusterEgressListener} via {@link
 *       SessionLookup#findByCorrelationId} to route cluster responses.
 *   <li>{@code sessionKey → Session} — used by the egress callback to call {@link Session#trySend}
 *       on the correct Artio session.
 * </ol>
 *
 * <p><b>Session capacity.</b> Enforces a global maximum session count and a per-CompID maximum to
 * prevent resource exhaustion from rogue or misconfigured clients.
 *
 * <p><b>Correlation lifecycle.</b> Entries are registered on each inbound command and removed when
 * the corresponding cluster response is received (via {@link #removeCorrelation}). A periodic
 * {@link #sweepStaleCorrelations()} removes orphan entries whose session has disconnected.
 *
 * <p><b>Allocation.</b> Zero allocation after construction. Uses Agrona primitive maps throughout.
 *
 * <p><b>Threading.</b> Not thread-safe — single-threaded gateway duty-cycle thread only.
 */
public final class SessionRegistry implements SessionLookup {

  private static final Log LOG = LogFactory.getLog(SessionRegistry.class);

  /** Sentinel for missing entries in Agrona Long2LongHashMap. */
  private static final long CORRELATION_MISSING = Long.MIN_VALUE;

  /** Sentinel for missing entries in the compId session count map. */
  private static final long COMP_ID_COUNT_MISSING = Long.MIN_VALUE;

  private final Long2LongHashMap correlationMap;
  private final Long2ObjectHashMap<Object> sessionsByKey;
  private final Long2LongHashMap sessionCompIdMap;
  private final Long2LongHashMap compIdSessionCount;

  private final int maxSessions;
  private final int maxSessionsPerCompId;

  /**
   * @param maxSessions global maximum number of concurrent FIX sessions
   * @param maxSessionsPerCompId maximum sessions per SenderCompID (by hash)
   * @param correlationCapacity initial capacity for the correlation map
   */
  public SessionRegistry(
      final int maxSessions, final int maxSessionsPerCompId, final int correlationCapacity) {
    if (maxSessions <= 0) {
      throw new IllegalArgumentException("maxSessions must be positive: " + maxSessions);
    }
    if (maxSessionsPerCompId <= 0) {
      throw new IllegalArgumentException(
          "maxSessionsPerCompId must be positive: " + maxSessionsPerCompId);
    }
    this.maxSessions = maxSessions;
    this.maxSessionsPerCompId = maxSessionsPerCompId;
    this.correlationMap = new Long2LongHashMap(correlationCapacity, 0.65f, CORRELATION_MISSING);
    this.sessionsByKey = new Long2ObjectHashMap<Object>(maxSessions, 0.65f);
    this.sessionCompIdMap = new Long2LongHashMap(maxSessions, 0.65f, CORRELATION_MISSING);
    this.compIdSessionCount = new Long2LongHashMap(maxSessions, 0.65f, COMP_ID_COUNT_MISSING);
  }

  // ===========================================================================
  // SessionLookup
  // ===========================================================================

  @Override
  public long findByCorrelationId(final byte[] correlationId, final int offset, final int length) {
    final long hash = InFlightTracker.fnv1aHash(correlationId, offset, length);
    final long sessionKey = correlationMap.get(hash);
    return sessionKey == CORRELATION_MISSING ? NULL_SESSION : sessionKey;
  }

  // ===========================================================================
  // Correlation management
  // ===========================================================================

  /**
   * Register a mapping from a correlation identifier to a session key. Called by {@link
   * FixSessionHandler} on each inbound command.
   */
  public void registerCorrelation(
      final byte[] correlationId, final int offset, final int length, final long sessionKey) {
    final long hash = InFlightTracker.fnv1aHash(correlationId, offset, length);
    correlationMap.put(hash, sessionKey);
  }

  /**
   * Remove a correlation entry. Called when a cluster response is received or the in-flight entry
   * times out.
   */
  public void removeCorrelation(final byte[] correlationId, final int offset, final int length) {
    final long hash = InFlightTracker.fnv1aHash(correlationId, offset, length);
    correlationMap.remove(hash);
  }

  /**
   * Remove orphan correlation entries whose session key is no longer in {@link #sessionsByKey}.
   * Called periodically from the gateway duty cycle to prevent unbounded map growth.
   *
   * <p><b>Iterator compaction.</b> Agrona's {@code Long2LongHashMap} uses open-addressing with
   * compaction on remove, which may cause the iterator to skip entries in a single pass. Any missed
   * entries will be caught on the next sweep. This is acceptable given the sweep interval (60s).
   *
   * @return number of stale entries removed
   */
  public int sweepStaleCorrelations() {
    int removed = 0;
    final Long2LongHashMap.EntryIterator it = correlationMap.entrySet().iterator();
    while (it.hasNext()) {
      it.next();
      final long sessionKey = it.getLongValue();
      if (!sessionsByKey.containsKey(sessionKey)) {
        it.remove();
        removed++;
      }
    }
    return removed;
  }

  // ===========================================================================
  // Session management
  // ===========================================================================

  /**
   * Try to register a new FIX session. Enforces global and per-CompID capacity limits.
   *
   * @param sessionKey Artio session ID ({@code session.id()})
   * @param compIdHash FNV-1a hash of the SenderCompID
   * @param session Artio session object
   * @return {@code true} if registered, {@code false} if capacity exceeded
   */
  public boolean tryRegisterSession(
      final long sessionKey, final long compIdHash, final Object session) {
    if (sessionsByKey.size() >= maxSessions) {
      LOG.warn()
          .append("Session rejected: global limit reached (")
          .append(maxSessions)
          .append(")")
          .commit();
      return false;
    }

    final long currentCount = compIdSessionCount.get(compIdHash);
    final long count = currentCount == COMP_ID_COUNT_MISSING ? 0L : currentCount;
    if (count >= maxSessionsPerCompId) {
      LOG.warn()
          .append("Session rejected: per-CompID limit reached (")
          .append(maxSessionsPerCompId)
          .append(") for compIdHash=")
          .append(compIdHash)
          .commit();
      return false;
    }

    sessionsByKey.put(sessionKey, session);
    sessionCompIdMap.put(sessionKey, compIdHash);
    compIdSessionCount.put(compIdHash, count + 1);
    return true;
  }

  /**
   * Look up the session object for the given key. Returns the raw Object — callers in production
   * code should cast to {@link Session}. Using Object allows test doubles without requiring Artio
   * session infrastructure.
   *
   * @return the session object, or {@code null} if the session has disconnected
   */
  @SuppressWarnings("unchecked")
  public <T> T findSession(final long sessionKey) {
    return (T) sessionsByKey.get(sessionKey);
  }

  /**
   * Remove a session on disconnect. Decrements the per-CompID counter so new sessions from the same
   * counterparty can connect.
   */
  public void removeSession(final long sessionKey) {
    sessionsByKey.remove(sessionKey);
    final long compIdHash = sessionCompIdMap.remove(sessionKey);
    if (compIdHash != CORRELATION_MISSING) {
      final long currentCount = compIdSessionCount.get(compIdHash);
      if (currentCount != COMP_ID_COUNT_MISSING && currentCount > 0) {
        if (currentCount == 1L) {
          compIdSessionCount.remove(compIdHash);
        } else {
          compIdSessionCount.put(compIdHash, currentCount - 1);
        }
      }
    }
  }

  /**
   * Returns all registered sessions for iteration (e.g., graceful shutdown logout). The returned
   * iterator is a reusable Agrona flyweight — callers must consume it immediately and not store it.
   */
  public Long2ObjectHashMap<Object>.ValueIterator allSessions() {
    return sessionsByKey.values().iterator();
  }

  /** Number of active sessions. */
  public int sessionCount() {
    return sessionsByKey.size();
  }

  /** Number of active correlation entries. */
  public int correlationCount() {
    return correlationMap.size();
  }
}
