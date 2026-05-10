package com.trading.engine.fixbridge.quote;

import java.util.LinkedHashSet;
import java.util.Set;
import org.agrona.collections.Object2ObjectHashMap;
import org.agrona.collections.ObjectHashSet;

/**
 * Per-session quote correlation indexes for the bridge dispatcher.
 *
 * <p><b>Purpose (locked plan §3.2).</b> Routes outbound {@code Quote} events back to the session
 * that originated the {@code QuoteRequest}, validates inbound {@code AcceptQuote}/{@code
 * RejectQuote} ownership, fans out {@code SessionTerminated} to other sessions of the same {@code
 * sub} on logout, and routes {@code Error{quote-orphaned}} to the oldest-connected surviving
 * session of a {@code sub} when a Quote arrives for a session that has gone away.
 *
 * <p>Fixes the round-1 BLOCKER "Quote broadcast to all authenticated sessions" by enforcing strict
 * per-session correlation. {@code quoteId} global uniqueness is now guaranteed by the APP-40a edit
 * to {@code OrchestratorIdGenerator} (clock-injected boot seed) — see plan §3.2.
 *
 * <p><b>Three indexes</b> maintained:
 *
 * <ul>
 *   <li>{@code reqIdToOwner: reqId -> ReqIdEntry{sessionId, registeredAtNs, quoteEmittedAtNs}}
 *       populated on {@link #onQuoteRequest}; eagerly evicted on session close; also TTL-evicted at
 *       {@code 120s} from registration (or {@code 60s} after Quote emission, whichever is later,
 *       per §B-r2-24 / §3.2 lifecycle).
 *   <li>{@code quoteIdToSessionId: quoteId -> sessionId} populated on {@link #onQuoteEmitted}; same
 *       TTL semantics as the parent {@code reqIdToOwner} entry.
 *   <li>{@code subToSessions: sub -> ordered-set-of-sessionId} maintained on session authentication
 *       and close; used for orphan-routing and {@code SessionTerminated} fan-out. Insertion order
 *       is preserved so "oldest-connected surviving session" lookup is O(1) on the iteration order.
 * </ul>
 *
 * <p><b>Threading.</b> NOT thread-safe by design. The bridge owns this index on its Netty boss-loop
 * thread (consistent with {@code :websocket-server} pattern). Mutation from any other thread is
 * forbidden. The single-event-loop invariant is enforced by inspection at PR review time and by the
 * dispatcher contract — there is no internal lock.
 *
 * <p><b>Allocation.</b> Map insertion allocates the entry node (Agrona's {@link
 * Object2ObjectHashMap} uses open-addressing so no per-entry node allocation), plus a {@link
 * ReqIdEntry} per QuoteRequest and a {@link LinkedHashSet} per first-session-for-a-sub. The inner
 * {@code LinkedHashSet} is chosen over Agrona's {@code ObjectHashSet} because §3.2 orphan-routing
 * relies on insertion-order iteration ("oldest-connected surviving session"), and Agrona's
 * open-addressing hash set iterates in slot-probe order — which would break the routing contract.
 * Session authentication is a cold path (once per WebSocket handshake), so the JDK collection
 * allocation is acceptable. Removal is zero-alloc. Lookups are zero-alloc (the inner set is
 * returned by reference, not copied).
 *
 * <p><b>Lifecycle.</b> One instance per bridge process (singleton owned by the dispatcher). Live
 * for the bridge JVM's lifetime.
 *
 * <p><b>Dependencies.</b> Agrona collections only.
 */
public final class SessionQuoteIndex {

  /**
   * Default TTL for an unused {@code reqId} entry — 120 seconds in nanoseconds. Per §B-r2-24:
   * registration TTL is 120s; if a Quote is emitted, the entry retains for an additional 60s (total
   * 120s+60s=180s) so late retransmissions still find a route. The §B-r2-24 wording says
   * "additional 60s (total 120s)"; we model that as {@code REQ_ID_TTL_NANOS} from registration AND
   * {@code QUOTE_EMITTED_TTL_NANOS} from emission, taking the later expiry.
   */
  static final long REQ_ID_TTL_NANOS = 120_000_000_000L;

  /**
   * Additional TTL after Quote emission. Per §3.2: "On Quote emission the entry retains for an
   * additional 60s (total 120s) to handle late retransmissions."
   */
  static final long QUOTE_EMITTED_TTL_NANOS = 60_000_000_000L;

  /**
   * Window within which a duplicate {@code reqId} from the same session is rejected with {@code
   * OrderReject{reason:"duplicate-reqId"}} per §3.2. 60 seconds in nanoseconds.
   */
  static final long DUPLICATE_REQID_WINDOW_NANOS = 60_000_000_000L;

  // ---------------------------------------------------------------------------
  // Index state.
  // ---------------------------------------------------------------------------

  /** {@code reqId -> ReqIdEntry}. Mutated on QuoteRequest, Quote emission, and TTL sweep. */
  private final Object2ObjectHashMap<String, ReqIdEntry> reqIdToOwner =
      new Object2ObjectHashMap<>();

  /** {@code quoteId -> sessionId}. Mutated on Quote emission and TTL sweep. */
  private final Object2ObjectHashMap<String, SessionId> quoteIdToSessionId =
      new Object2ObjectHashMap<>();

  /**
   * {@code sub -> ordered-set-of-sessionId}. Insertion order preserved for "oldest-connected
   * surviving session" fan-out. Mutated on session authentication and on session close. The inner
   * value is a JDK {@link LinkedHashSet} (NOT Agrona's {@code ObjectHashSet} — that one iterates in
   * hash-probe order and would silently break the §3.2 routing contract).
   */
  private final Object2ObjectHashMap<String, LinkedHashSet<SessionId>> subToSessions =
      new Object2ObjectHashMap<>();

  /**
   * Reverse map: {@code sessionId -> sub} so eager eviction on session close can locate the {@code
   * sub} bucket without iterating {@link #subToSessions}.
   */
  private final Object2ObjectHashMap<SessionId, String> sessionToSub = new Object2ObjectHashMap<>();

  /**
   * Reverse map: {@code sessionId -> set-of-reqIds-owned-by-session}. Lets {@link #onSessionClosed}
   * evict in O(K) where K is the per-session reqId count instead of O(N) over the global {@link
   * #reqIdToOwner} map (Gemini medium-priority finding on PR #70). The Agrona {@code ObjectHashSet}
   * is preferred over a JDK collection here because the per-session count can be large (a user can
   * have hundreds of in-flight QuoteRequests) and Agrona's open-addressing implementation has
   * tighter memory locality for the typical small-set case.
   */
  private final Object2ObjectHashMap<SessionId, ObjectHashSet<String>> sessionToReqIds =
      new Object2ObjectHashMap<>();

  // ---------------------------------------------------------------------------
  // Session-lifecycle hooks.
  // ---------------------------------------------------------------------------

  /**
   * Record a new authenticated session. Caller MUST invoke exactly once per session, immediately
   * after {@code Auth} validation succeeds. Idempotent: re-registering the same {@code (sessionId,
   * sub)} pair is a no-op.
   *
   * @param sessionId fresh, never-reused session id minted at handshake
   * @param sub JWT {@code sub} claim of the authenticated user
   * @throws NullPointerException if either argument is null
   * @throws IllegalStateException if {@code sessionId} is already registered with a different
   *     {@code sub} (caller bug — session ids are not reusable)
   */
  public void onSessionAuthenticated(final SessionId sessionId, final String sub) {
    if (sessionId == null) {
      throw new NullPointerException("sessionId must not be null");
    }
    if (sub == null) {
      throw new NullPointerException("sub must not be null");
    }
    final var existing = sessionToSub.get(sessionId);
    if (existing != null) {
      if (existing.equals(sub)) {
        return; // idempotent re-registration
      }
      throw new IllegalStateException(
          "sessionId " + sessionId + " already registered with sub=" + existing);
    }
    sessionToSub.put(sessionId, sub);
    // computeIfAbsent keeps the bucket-ref local final per CLAUDE.md (was a mutable `var bucket`
    // pre-fix per CodeRabbit). The lambda allocation is once-per-session-auth (cold path).
    // A small initial capacity (4) keeps the typical-case (1-2 sessions per user) tight.
    // LinkedHashSet preserves insertion order — required by §3.2 orphan-routing per the field
    // declaration above.
    final var bucket = subToSessions.computeIfAbsent(sub, k -> new LinkedHashSet<>(4));
    bucket.add(sessionId);
  }

  /**
   * Eagerly remove all index entries owned by {@code sessionId}. Caller MUST invoke from the Netty
   * {@code channelInactive} hook regardless of whether the close was normal, abnormal, or forced
   * (close codes 1001/1006/4001/4002/4008).
   *
   * <p>Per §3.2: "On WS close, all entries for that session are eagerly evicted regardless of TTL."
   *
   * @param sessionId the session that just closed
   * @return the {@code sub} that owned the session, or {@code null} if the session was never
   *     authenticated. Callers needing to fan out {@code SessionTerminated} use this to locate the
   *     surviving siblings via {@link #sessionsForSub(String)} BEFORE invoking this method (which
   *     removes the gone session from the bucket).
   */
  public String onSessionClosed(final SessionId sessionId) {
    if (sessionId == null) {
      throw new NullPointerException("sessionId must not be null");
    }
    final var sub = sessionToSub.remove(sessionId);
    if (sub != null) {
      final var bucket = subToSessions.get(sub);
      if (bucket != null) {
        bucket.remove(sessionId);
        if (bucket.isEmpty()) {
          subToSessions.remove(sub);
        }
      }
    }
    // O(K) eviction per Gemini medium-priority finding on PR #70 — was O(N) over the global
    // reqIdToOwner map (full entrySet scan). The sessionToReqIds reverse-index lets us touch
    // only the reqIds owned by THIS session.
    final var ownedReqIds = sessionToReqIds.remove(sessionId);
    if (ownedReqIds != null) {
      for (final var reqId : ownedReqIds) {
        final var entry = reqIdToOwner.remove(reqId);
        if (entry != null) {
          // Also remove the corresponding quoteId mapping if Quote was emitted.
          final var quoteId = entry.quoteId;
          if (quoteId != null) {
            quoteIdToSessionId.remove(quoteId);
          }
        }
      }
    }
    return sub;
  }

  // ---------------------------------------------------------------------------
  // QuoteRequest / Quote / Accept-or-Reject correlation.
  // ---------------------------------------------------------------------------

  /**
   * Result of {@link #onQuoteRequest(String, SessionId, long)}. Either the registration succeeded
   * ({@link #ACCEPTED}) or it was rejected as a duplicate within the §3.2 60-second window ({@link
   * #DUPLICATE_REQID}).
   */
  public enum QuoteRequestRegistration {
    /** {@code reqId} registered; the dispatcher should forward to the orchestrator. */
    ACCEPTED,
    /**
     * {@code reqId} already registered for this same session within the 60s dedupe window. The
     * dispatcher MUST emit {@code OrderReject{reason:"duplicate-reqId"}} (per §3.2).
     */
    DUPLICATE_REQID
  }

  /**
   * Register a {@code QuoteRequest}. Returns whether the registration is accepted or rejected as a
   * duplicate.
   *
   * @param reqId client-supplied request id from the inbound {@code QuoteRequest}
   * @param sessionId the originating session
   * @param nowNs current epoch-nanosecond timestamp from the bridge clock
   * @return {@link QuoteRequestRegistration#ACCEPTED} if registered, {@link
   *     QuoteRequestRegistration#DUPLICATE_REQID} if the same {@code (reqId, sessionId)} appeared
   *     within {@link #DUPLICATE_REQID_WINDOW_NANOS} of a still-live registration
   */
  public QuoteRequestRegistration onQuoteRequest(
      final String reqId, final SessionId sessionId, final long nowNs) {
    if (reqId == null) {
      throw new NullPointerException("reqId must not be null");
    }
    if (sessionId == null) {
      throw new NullPointerException("sessionId must not be null");
    }
    final var existing = reqIdToOwner.get(reqId);
    if (existing != null
        && sessionId.equals(existing.sessionId)
        && (nowNs - existing.registeredAtNs) < DUPLICATE_REQID_WINDOW_NANOS) {
      return QuoteRequestRegistration.DUPLICATE_REQID;
    }
    // Either no prior entry, an entry from a different session (allowed — different RFQ flows),
    // or a same-session entry outside the dedupe window (allowed — old entry will be TTL-evicted).
    reqIdToOwner.put(reqId, new ReqIdEntry(sessionId, nowNs));
    // Maintain the reverse index for O(K) session-close eviction (Gemini PR #70 fix).
    sessionToReqIds.computeIfAbsent(sessionId, k -> new ObjectHashSet<>()).add(reqId);
    return QuoteRequestRegistration.ACCEPTED;
  }

  /**
   * Record that a server-issued {@code Quote} has been emitted for {@code reqId}, binding {@code
   * quoteId} to the same session for downstream {@code AcceptQuote}/{@code RejectQuote} validation.
   *
   * <p>If the {@code reqId} entry has been evicted (session closed, TTL elapsed, or unknown reqId),
   * this method returns {@code null} and emits no side effect — the caller (orphan-routing path) is
   * expected to consult {@link #sessionsForSub(String)} to find a surviving sibling, or drop
   * silently.
   *
   * @param reqId the originating request id
   * @param quoteId the server-allocated quote id (globally unique per §3.2)
   * @param nowNs current epoch-nanosecond timestamp
   * @return the owning {@link SessionId} if the route is live; {@code null} if the originating
   *     session is gone (orphan path)
   */
  public SessionId onQuoteEmitted(final String reqId, final String quoteId, final long nowNs) {
    if (reqId == null) {
      throw new NullPointerException("reqId must not be null");
    }
    if (quoteId == null) {
      throw new NullPointerException("quoteId must not be null");
    }
    final var entry = reqIdToOwner.get(reqId);
    if (entry == null) {
      return null; // session gone or TTL-evicted before Quote arrived
    }
    entry.quoteId = quoteId;
    entry.quoteEmittedAtNs = nowNs;
    quoteIdToSessionId.put(quoteId, entry.sessionId);
    return entry.sessionId;
  }

  /**
   * Validate that {@code currentSessionId} owns {@code quoteId} for an inbound {@code AcceptQuote}
   * / {@code RejectQuote}. Mismatch must be surfaced as {@code OrderReject{reason:"quote-not-
   * owned"}} per §3.2.
   *
   * @param quoteId quote id from the inbound message
   * @param currentSessionId session that sent the Accept/Reject
   * @return {@code true} if {@code quoteId} is owned by {@code currentSessionId}, {@code false} if
   *     unknown or owned by a different session
   */
  public boolean isOwnedBy(final String quoteId, final SessionId currentSessionId) {
    if (quoteId == null || currentSessionId == null) {
      return false;
    }
    final var owner = quoteIdToSessionId.get(quoteId);
    return currentSessionId.equals(owner);
  }

  /**
   * Look up the surviving sessions of a {@code sub}. Used by the orphan-routing path (route a
   * stranded Quote to the oldest-connected surviving session of the same user) and by the {@code
   * SessionTerminated} fan-out path (close every other session of the user when one signs out).
   *
   * <p>Iteration order is insertion order — guaranteed by the inner {@link LinkedHashSet}. The
   * caller iterates from oldest-to-newest. (The previous implementation backed this with Agrona's
   * {@code ObjectHashSet}, which iterates in slot-probe order — silently violating the §3.2
   * orphan-routing contract. Fixed in iteration 1 of /orchestrate.)
   *
   * @param sub JWT {@code sub} claim
   * @return the live session set for this sub, or {@code null} if the sub has no live sessions.
   *     Caller MUST NOT mutate the returned set; this is a live view. Declared {@link
   *     LinkedHashSet} (not {@link Set}) so the insertion-order contract is visible at the API
   *     boundary.
   */
  public LinkedHashSet<SessionId> sessionsForSub(final String sub) {
    if (sub == null) {
      return null;
    }
    return subToSessions.get(sub);
  }

  /**
   * Return the {@code sub} owning a {@link SessionId}, or {@code null} if the session was never
   * authenticated. Used by the audit logger and by the {@code SessionTerminated} fan-out path.
   *
   * @param sessionId session id
   * @return the sub or null
   */
  public String subFor(final SessionId sessionId) {
    if (sessionId == null) {
      return null;
    }
    return sessionToSub.get(sessionId);
  }

  // ---------------------------------------------------------------------------
  // TTL sweep.
  // ---------------------------------------------------------------------------

  /**
   * Evict every {@code reqId} entry that has expired per the TTL rules in §3.2. Caller invokes
   * periodically from the bridge duty cycle (e.g. once per second). Returns the number of entries
   * evicted (for metrics).
   *
   * @param nowNs current epoch-nanosecond timestamp
   * @return count of entries evicted in this sweep
   */
  public int sweepExpired(final long nowNs) {
    int evicted = 0;
    final var iter = reqIdToOwner.entrySet().iterator();
    while (iter.hasNext()) {
      final var entry = iter.next();
      if (isExpired(entry.getValue(), nowNs)) {
        final var quoteId = entry.getValue().quoteId;
        if (quoteId != null) {
          quoteIdToSessionId.remove(quoteId);
        }
        // Keep sessionToReqIds in sync — drop the reqId from its owning session's bucket so
        // a later session-close doesn't try to re-evict it (and so the reverse-index size
        // remains bounded by live reqIds, not lifetime reqIds).
        final var ownerSession = entry.getValue().sessionId;
        final var ownerBucket = sessionToReqIds.get(ownerSession);
        if (ownerBucket != null) {
          ownerBucket.remove(entry.getKey());
          if (ownerBucket.isEmpty()) {
            sessionToReqIds.remove(ownerSession);
          }
        }
        iter.remove();
        evicted++;
      }
    }
    return evicted;
  }

  private static boolean isExpired(final ReqIdEntry e, final long nowNs) {
    if (e.quoteEmittedAtNs > 0L) {
      return (nowNs - e.quoteEmittedAtNs) >= QUOTE_EMITTED_TTL_NANOS;
    }
    return (nowNs - e.registeredAtNs) >= REQ_ID_TTL_NANOS;
  }

  // ---------------------------------------------------------------------------
  // Diagnostics — used by tests + metrics.
  // ---------------------------------------------------------------------------

  /**
   * Current size of the {@code reqId} index. Useful for metrics and for asserting the eager-
   * eviction invariant in tests.
   *
   * @return number of live {@code reqId} entries
   */
  public int reqIdCount() {
    return reqIdToOwner.size();
  }

  /**
   * Current size of the {@code quoteId} index.
   *
   * @return number of live {@code quoteId} entries
   */
  public int quoteIdCount() {
    return quoteIdToSessionId.size();
  }

  /**
   * Current number of distinct {@code sub} buckets with at least one live session.
   *
   * @return number of live subs
   */
  public int subCount() {
    return subToSessions.size();
  }

  // ---------------------------------------------------------------------------
  // Internal entry type.
  // ---------------------------------------------------------------------------

  /**
   * One node in the {@code reqIdToOwner} map. Mutable: the {@code quoteId}/{@code quoteEmittedAtNs}
   * fields flip from null/{@code 0L} to populated when {@code onQuoteEmitted} is called for this
   * {@code reqId}.
   */
  static final class ReqIdEntry {

    final SessionId sessionId;
    final long registeredAtNs;
    String quoteId; // null until onQuoteEmitted populates it
    long quoteEmittedAtNs; // 0L until onQuoteEmitted populates it

    ReqIdEntry(final SessionId sessionId, final long registeredAtNs) {
      this.sessionId = sessionId;
      this.registeredAtNs = registeredAtNs;
    }
  }
}
