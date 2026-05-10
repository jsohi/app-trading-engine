package com.trading.engine.fixbridge.auth;

/**
 * Short-window replay cache for DPoP proof {@code jti} (JWT id) claims (RFC 9449 §11.1 / §B-r2-7).
 *
 * <p><b>Distinct from {@link JtiRevocationCache}.</b> The bridge has two JTI caches that
 * intentionally do not share state:
 *
 * <ul>
 *   <li>{@link JtiRevocationCache} — tracks <i>bearer-JWT</i> JTIs revoked by user sign-out. TTL =
 *       remaining JWT {@code exp} (typically 15 min). Cap 10k entries.
 *   <li>{@link JtiReplayCache} (this) — tracks <i>DPoP proof</i> JTIs to prevent replay of the
 *       proof JWT itself. TTL is short (typically 60s — long enough to absorb clock skew without
 *       admitting replay, short enough to keep the cache small). Per-JWK rather than per-user,
 *       though impls MAY collapse to a single global cache when the JTI namespace is sufficiently
 *       random (UUIDv4) for the cardinality not to matter in practice.
 * </ul>
 *
 * <p><b>Contract.</b> A single SAM method: {@link #checkAndAdd(String, long)} returns {@code true}
 * iff the {@code jti} was NOT previously seen (i.e., this is a fresh proof). On {@code false} the
 * caller MUST treat the DPoP proof as a replay and reject the auth attempt with {@link
 * DpopValidator.Result#INVALID}.
 *
 * <p><b>Atomicity.</b> The check and insertion MUST be atomic with respect to concurrent calls —
 * two threads racing the same {@code jti} must see exactly one {@code true} return. Implementations
 * are responsible for the appropriate synchronization (the bridge's auth handler runs single-
 * threaded on a Netty event loop today, but the SAM must not assume that since the launcher may
 * choose to share a single cache across multiple bridge processes via a future Aeron-backed impl).
 *
 * <p><b>Lifecycle.</b> Bound at bridge startup; shared across all auth attempts for the lifetime of
 * the JVM.
 *
 * <p><b>Allocation.</b> Cold path (per-auth, not per-message); impls MAY allocate.
 */
@FunctionalInterface
public interface JtiReplayCache {

  /**
   * Atomically check-and-insert a DPoP proof JTI.
   *
   * @param jti the {@code jti} claim from the DPoP proof JWT (non-null, non-empty per RFC 9449)
   * @param expireAtNs absolute monotonic-nanosecond deadline at which this entry MAY be evicted by
   *     the cache. Impls SHOULD treat values strictly less than the current clock as already-
   *     expired (i.e., MUST NOT keep them resident). Caller computes via {@code clock.nanoTime() +
   *     ttlNs}.
   * @return {@code true} if {@code jti} was not previously cached (fresh proof — auth proceeds);
   *     {@code false} if a non-expired entry for the same {@code jti} already existed (replay —
   *     auth MUST be rejected)
   */
  boolean checkAndAdd(String jti, long expireAtNs);
}
