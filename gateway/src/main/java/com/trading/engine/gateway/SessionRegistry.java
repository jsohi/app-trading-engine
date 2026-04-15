package com.trading.engine.gateway;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.Long2ObjectHashMap;

/**
 * Central session registry that maps FIX correlation identifiers (ClOrdID, QuoteReqID) to gateway
 * session keys and session keys to {@link GatewaySession} objects. Implements {@link SessionLookup}
 * for use by {@link ClusterEgressListener}.
 *
 * <p><b>Two maps.</b> The egress pipeline needs two lookups:
 *
 * <ol>
 *   <li>{@code correlationHash → sessionKey} — used by {@link ClusterEgressListener} via {@link
 *       SessionLookup#findByCorrelationId} to route cluster responses.
 *   <li>{@code sessionKey → GatewaySession} — used by the egress callback to call {@link
 *       GatewaySession#trySend} on the correct Artio session.
 * </ol>
 *
 * <p><b>Session capacity.</b> Enforces a global maximum session count and a per-CompID maximum to
 * prevent resource exhaustion from rogue or misconfigured clients.
 *
 * <p><b>Correlation lifecycle.</b> Entries are registered on each inbound command (with a monotonic
 * timestamp) and removed when the corresponding response is received (via {@link
 * #removeCorrelation}). Two periodic sweeps run from the gateway duty cycle:
 *
 * <ul>
 *   <li>{@link #sweepStaleCorrelations()} — removes orphan entries whose session has disconnected.
 *   <li>{@link #sweepExpiredCorrelations(long, long)} — removes entries older than a configurable
 *       TTL, regardless of session state. This covers orchestrator crash/stall scenarios where the
 *       session is still connected but no response will ever arrive.
 * </ul>
 *
 * <p><b>Allocation.</b> Zero allocation after construction. Uses Agrona primitive maps throughout.
 *
 * <p><b>Threading.</b> Not thread-safe — single-threaded gateway duty-cycle thread only.
 */
public final class SessionRegistry implements SessionLookup {

  private static final Log LOG = LogFactory.getLog(SessionRegistry.class);

  /** Sentinel for missing entries in all Agrona {@link Long2LongHashMap} instances. */
  private static final long MISSING_VALUE = Long.MIN_VALUE;

  /**
   * Remap a hash value that collides with Agrona's {@link Long2LongHashMap} {@code MISSING_VALUE}
   * sentinel. If an FNV-1a hash produces {@code Long.MIN_VALUE}, the map silently treats it as "not
   * present" — breaking correlation lookups and per-CompID counting. Remapping to {@code
   * Long.MIN_VALUE + 1} is safe: it shifts one value in a 2^64 space and preserves the uniformity
   * guarantee of the hash.
   */
  static long remapSentinel(final long hash) {
    return hash == MISSING_VALUE ? MISSING_VALUE + 1 : hash;
  }

  private final Long2LongHashMap correlationMap;
  private final Long2LongHashMap correlationTimestamps;
  private final Long2ObjectHashMap<GatewaySession> sessionsByKey;
  private final Long2LongHashMap sessionCompIdMap;
  private final Long2LongHashMap compIdSessionCount;

  /** Number of correlations expired by TTL sweep since startup. */
  private long ttlExpired;

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
    this.correlationMap = new Long2LongHashMap(correlationCapacity, 0.65f, MISSING_VALUE);
    this.correlationTimestamps = new Long2LongHashMap(correlationCapacity, 0.65f, MISSING_VALUE);
    this.sessionsByKey = new Long2ObjectHashMap<GatewaySession>(maxSessions, 0.65f);
    this.sessionCompIdMap = new Long2LongHashMap(maxSessions, 0.65f, MISSING_VALUE);
    this.compIdSessionCount = new Long2LongHashMap(maxSessions, 0.65f, MISSING_VALUE);
  }

  // ===========================================================================
  // SessionLookup
  // ===========================================================================

  @Override
  public long findByCorrelationId(final byte[] correlationId, final int offset, final int length) {
    final long hash = remapSentinel(InFlightTracker.fnv1aHash(correlationId, offset, length));
    final long sessionKey = correlationMap.get(hash);
    return sessionKey == MISSING_VALUE ? NULL_SESSION : sessionKey;
  }

  // ===========================================================================
  // Correlation management
  // ===========================================================================

  /**
   * Register a mapping from a correlation identifier to a session key, with a monotonic timestamp
   * for TTL-based expiry. Called by {@link FixSessionHandler} on each inbound command.
   *
   * @param correlationId the ClOrdID or QuoteReqID bytes
   * @param offset start offset within the correlation byte array
   * @param length number of significant bytes
   * @param sessionKey Artio session ID for response routing
   * @param nowNs monotonic nanosecond timestamp (from {@code NanoClock.nanoTime()}) for TTL expiry
   */
  public void registerCorrelation(
      final byte[] correlationId,
      final int offset,
      final int length,
      final long sessionKey,
      final long nowNs) {
    final long hash = remapSentinel(InFlightTracker.fnv1aHash(correlationId, offset, length));
    final long existing = correlationMap.put(hash, sessionKey);
    correlationTimestamps.put(hash, nowNs);
    if (existing != MISSING_VALUE && existing != sessionKey) {
      // Hash collision or ClOrdID reuse across sessions — log for observability.
      LOG.warn()
          .append("Correlation overwrite: hash=")
          .append(hash)
          .append(" prevSession=")
          .append(existing)
          .append(" newSession=")
          .append(sessionKey)
          .commit();
    }
  }

  /**
   * Remove a correlation entry and its associated timestamp. Called when a response is received
   * (from cluster or orchestrator) or when the in-flight entry times out.
   *
   * @param correlationId the ClOrdID or QuoteReqID bytes
   * @param offset start offset within the correlation byte array
   * @param length number of significant bytes
   */
  public void removeCorrelation(final byte[] correlationId, final int offset, final int length) {
    final long hash = remapSentinel(InFlightTracker.fnv1aHash(correlationId, offset, length));
    correlationMap.remove(hash);
    correlationTimestamps.remove(hash);
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
        correlationTimestamps.remove(it.getLongKey());
        it.remove();
        removed++;
      }
    }
    return removed;
  }

  /**
   * Remove correlation entries whose registration timestamp is older than the given TTL. This
   * catches orphaned correlations from orchestrator crashes, stalls, or message loss on Aeron IPC —
   * scenarios where the session is still connected but no response will ever arrive.
   *
   * <p><b>Iterator compaction.</b> Same Agrona caveat as {@link #sweepStaleCorrelations()} — missed
   * entries are caught on the next sweep.
   *
   * @param nowNs current monotonic nanosecond timestamp
   * @param ttlNs maximum age in nanoseconds; entries older than this are expired
   * @return number of expired entries removed
   */
  public int sweepExpiredCorrelations(final long nowNs, final long ttlNs) {
    int expired = 0;
    final Long2LongHashMap.EntryIterator it = correlationTimestamps.entrySet().iterator();
    while (it.hasNext()) {
      it.next();
      if (nowNs - it.getLongValue() >= ttlNs) {
        final long hash = it.getLongKey();
        correlationMap.remove(hash);
        it.remove();
        expired++;
      }
    }
    ttlExpired += expired;
    return expired;
  }

  /** Cumulative number of correlations expired by TTL sweep since startup. */
  public long ttlExpiredCount() {
    return ttlExpired;
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
      final long sessionKey, final long compIdHash, final GatewaySession session) {
    if (sessionsByKey.size() >= maxSessions) {
      LOG.warn()
          .append("Session rejected: global limit reached (")
          .append(maxSessions)
          .append(")")
          .commit();
      return false;
    }

    final long currentCount = compIdSessionCount.get(compIdHash);
    final long count = currentCount == MISSING_VALUE ? 0L : currentCount;
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
   * Look up the {@link GatewaySession} for the given session key. Used by the egress callback to
   * call {@link GatewaySession#trySend}.
   *
   * @return the session, or {@code null} if the session has disconnected
   */
  public GatewaySession findSession(final long sessionKey) {
    return sessionsByKey.get(sessionKey);
  }

  /**
   * Remove a session on disconnect. Decrements the per-CompID counter so new sessions from the same
   * counterparty can connect.
   */
  public void removeSession(final long sessionKey) {
    sessionsByKey.remove(sessionKey);
    final long compIdHash = sessionCompIdMap.remove(sessionKey);
    if (compIdHash != MISSING_VALUE) {
      final long currentCount = compIdSessionCount.get(compIdHash);
      if (currentCount != MISSING_VALUE && currentCount > 0) {
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
  public Long2ObjectHashMap<GatewaySession>.ValueIterator allSessions() {
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
