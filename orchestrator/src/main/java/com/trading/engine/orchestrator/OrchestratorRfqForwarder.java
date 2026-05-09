package com.trading.engine.orchestrator;

import io.aeron.cluster.codecs.CloseReason;
import org.agrona.collections.Long2LongHashMap;

/**
 * Cluster-event egress translator and FIX-session routing map for the post-APP-232 architecture.
 *
 * <p>After APP-232 the cluster is authoritative for RFQ state (slot pool, quoteId minting, snapshot
 * 203, TTL timers). The orchestrator forwards FIX-side commands (QuoteRequest, PriceResponse) to
 * the cluster and, on the egress side, consumes cluster events 105 / 106 / 107 and translates them
 * into the appropriate FIX outbound messages:
 *
 * <ul>
 *   <li>{@code QuoteCreatedEvent} (105) → FIX {@code 35=S Quote}
 *   <li>{@code QuoteRejectedEvent} (106) → FIX {@code 35=AG QuoteRequestReject}
 *   <li>{@code QuoteExpiredEvent} (107) → FIX {@code 35=AI QuoteStatusReport} with QuoteStatus (tag
 *       297) = 6 (Removed)
 * </ul>
 *
 * <p>Because cluster events do not carry the originating Artio FIX session id (only the cluster
 * session id), this class maintains a routing map keyed off a hash of the {@code quoteReqId}
 * populated during the FIX→cluster forwarding step. On the cluster→FIX egress side, the map is
 * consulted to find the correct Artio session for outbound delivery. Map entries are evicted on
 * session close or after a configurable retention window.
 *
 * <p><b>Threading.</b> Single-threaded — orchestrator agent thread only.
 *
 * <p><b>Allocation.</b> Zero allocation per command after construction. The Agrona {@link
 * Long2LongHashMap} pre-sizes the bucket array; the long key is a hash of the 20-byte quoteReqId.
 *
 * <p><b>Non-deterministic.</b> The orchestrator runs outside Raft consensus; the routing map is
 * in-memory only and is intentionally lost on orchestrator restart. Clients re-issue stale requests
 * — the cluster's recovery sweep handles in-flight RFQs that survive orchestrator restart via the
 * snapshot path.
 *
 * <p><b>Coexists with</b> {@link RfqStateMachine}, which tracks orchestrator-internal concerns
 * (pricing-flow async coordination, NOS validation, re-delivery dedup). The orchestrator-side state
 * machine is distinct from the cluster's slot pool and remains the source of truth for
 * pricing-pending state.
 *
 * @see RfqStateMachine
 * @see OrchestratorService
 */
public final class OrchestratorRfqForwarder {

  /**
   * {@code Long2LongHashMap} sentinel for "no routing entry" — Long.MIN_VALUE is unused as a
   * session id.
   */
  public static final long MISSING_SESSION = Long.MIN_VALUE;

  /**
   * Routing map: hash(quoteReqId) → originating Artio session id. Populated when the orchestrator
   * forwards a {@code QuoteRequest} from FIX to the cluster; consulted when the cluster emits a 105
   * / 106 / 107 event so the response is delivered to the correct FIX client. Pre-sized for the
   * typical concurrent-RFQ ceiling.
   */
  private final Long2LongHashMap quoteReqIdToSession;

  /**
   * Constructs a forwarder with the given expected concurrent-RFQ capacity.
   *
   * @param expectedCapacity initial map size; map auto-grows beyond this but the initial alloc
   *     should cover the steady-state working set
   */
  public OrchestratorRfqForwarder(final int expectedCapacity) {
    if (expectedCapacity <= 0) {
      throw new IllegalArgumentException("expectedCapacity must be > 0, got " + expectedCapacity);
    }
    this.quoteReqIdToSession = new Long2LongHashMap(expectedCapacity, 0.55f, MISSING_SESSION);
  }

  /**
   * Records the FIX session that originated a QuoteRequest, keyed off a hash of the 20-byte
   * quoteReqId. Called from the gateway-side {@code QuoteRequest} forwarding path.
   *
   * @param quoteReqIdHash hash of the QuoteReqID (FIX tag 131) — caller computes via FNV-1a or
   *     similar over the 20-byte field
   * @param artioSessionId Artio FIX session id from the inbound FIX message
   */
  public void recordOriginatingSession(final long quoteReqIdHash, final long artioSessionId) {
    quoteReqIdToSession.put(quoteReqIdHash, artioSessionId);
  }

  /**
   * Looks up the originating Artio FIX session for a quoteReqId. Returns {@link #MISSING_SESSION}
   * if no entry — the caller should route to the {@code default-session} fallback or log a {@code
   * rfqRouteMissing} counter increment.
   *
   * @param quoteReqIdHash hash of the QuoteReqID
   * @return the Artio session id, or {@link #MISSING_SESSION}
   */
  public long lookupSessionForQuoteReqId(final long quoteReqIdHash) {
    return quoteReqIdToSession.get(quoteReqIdHash);
  }

  /**
   * Removes the routing entry for a quoteReqId, e.g. after the cluster emits the terminal 106 / 107
   * event so the client can no longer be routed via this quoteReqId. Idempotent.
   *
   * @param quoteReqIdHash hash of the QuoteReqID to evict
   */
  public void evict(final long quoteReqIdHash) {
    quoteReqIdToSession.remove(quoteReqIdHash);
  }

  /**
   * Removes all entries originating from a closing FIX session. Called from the orchestrator's
   * {@code onSessionClose} hook so stale routing entries don't accumulate indefinitely. O(N) over
   * the map; sessions close infrequently so this is acceptable cost.
   *
   * @param artioSessionId the closing session id
   * @param closeReason the close reason (informational; not currently used to vary behaviour)
   * @return the number of routing entries evicted
   */
  public int evictSession(final long artioSessionId, final CloseReason closeReason) {
    // Iterate the long key set primitive-iterator-style (no boxing). Build a removal list
    // first because the map iterator does not support concurrent removal of the current entry
    // safely across all Agrona versions. Removal pass runs on session close (cold path); the
    // small array allocation is acceptable.
    final long[] keysToEvict = new long[quoteReqIdToSession.size()];
    int idx = 0;
    final var keyIter = quoteReqIdToSession.keySet().iterator();
    while (keyIter.hasNext()) {
      final long key = keyIter.nextValue();
      if (quoteReqIdToSession.get(key) == artioSessionId) {
        keysToEvict[idx++] = key;
      }
    }
    for (int i = 0; i < idx; i++) {
      quoteReqIdToSession.remove(keysToEvict[i]);
    }
    return idx;
  }

  /**
   * Returns the number of currently-tracked routing entries. For diagnostics / metrics only.
   *
   * @return current map size
   */
  public int routingEntryCount() {
    return quoteReqIdToSession.size();
  }

  /**
   * Computes a stable 64-bit FNV-1a hash of the first {@code length} bytes of {@code src} starting
   * at {@code offset}. Used to convert a 20-byte QuoteReqID into a primitive {@code long} key for
   * the routing map. Two distinct quoteReqIds collide with probability ~2^-64.
   *
   * <p>Zero allocation.
   *
   * @param src source byte array
   * @param offset starting offset
   * @param length number of bytes to hash
   * @return 64-bit FNV-1a hash
   */
  public static long fnv1a64(final byte[] src, final int offset, final int length) {
    long hash = 0xCBF29CE484222325L; // FNV-1a 64-bit offset basis
    final long prime = 0x100000001B3L;
    for (int i = 0; i < length; i++) {
      hash ^= (src[offset + i] & 0xFFL);
      hash *= prime;
    }
    return hash == MISSING_SESSION ? 1L : hash; // avoid sentinel collision on legitimate keys
  }
}
