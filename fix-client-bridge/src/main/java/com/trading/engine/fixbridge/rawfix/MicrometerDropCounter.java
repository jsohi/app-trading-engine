package com.trading.engine.fixbridge.rawfix;

import com.trading.engine.fixbridge.transport.BridgeSession;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Production {@link RawFixTap.DropCounter} backed by Micrometer.
 *
 * <p><b>Purpose.</b> Replaces {@link RawFixTap.DropCounter#NOOP} for production use. Increments a
 * pre-registered counter per {@link RawFixTap.DropReason} so the operator's Prometheus dashboard
 * can graph {@code fixbridge_rawfix_dropped_total{session,reason}} per §3.5 observability
 * requirements. The launcher binds this once at boot.
 *
 * <p><b>Counter naming.</b> One Micrometer counter per {@link RawFixTap.DropReason}, all sharing
 * the metric name {@code fixbridge_rawfix_dropped_total}. The {@code reason} tag distinguishes the
 * three drop classes ({@code disabled}, {@code rate_limit}, {@code outbound_queue_full}). The
 * {@code session} tag is added per-session at increment time — bounded by the per-process
 * concurrent-session ceiling (~256) so cardinality stays sane.
 *
 * <p><b>Threading.</b> Thread-safe. The outer {@link ConcurrentHashMap} is concurrent; Micrometer
 * counters are themselves lock-free (atomic-double-backed). Multiple Netty event loops may
 * concurrently increment without contention on the steady-state path.
 *
 * <p><b>Allocation.</b> Per-increment on a (session, reason) pair that has been seen before: one
 * {@link ConcurrentHashMap#get} + one {@link EnumMap#get} + one {@link Counter#increment} — all
 * zero-alloc on the JIT-compiled path. First-touch for a session allocates one {@link EnumMap};
 * first-touch for a (session, reason) pair allocates the underlying Counter via {@link
 * Counter.Builder#register} — bounded to {@code 3 × max_concurrent_sessions} per process lifetime.
 *
 * <p><b>Lifecycle.</b> One instance per bridge process, injected at launcher boot. Counters live
 * for the JVM lifetime (no eviction on session close — operators may want to graph the cumulative
 * drop count for retired sessions to detect post-mortem patterns).
 *
 * <p><b>Dependencies.</b> {@link io.micrometer.core.instrument.MeterRegistry} (any impl —
 * production binds Prometheus).
 */
public final class MicrometerDropCounter implements RawFixTap.DropCounter {

  /** Micrometer counter name for RawFix drops. */
  public static final String METRIC_NAME = "fixbridge_rawfix_dropped_total";

  /** Tag key for the per-session label. */
  private static final String TAG_SESSION = "session";

  /** Tag key for the drop-reason label. */
  private static final String TAG_REASON = "reason";

  private final MeterRegistry registry;

  /**
   * Per-session map of {@link RawFixTap.DropReason} → Counter. Outer map is a {@link
   * ConcurrentHashMap} for lock-free reads on the steady-state hot path; inner is an {@link
   * EnumMap} (zero-overhead, array-backed for enum keys) populated lazily per reason via {@link
   * Map#computeIfAbsent}. Avoids any String concatenation on the increment path (the prior
   * implementation built a {@code "session:reason"} String per call AND took a monitor on every
   * increment — both eliminated here).
   */
  private final ConcurrentHashMap<String, EnumMap<RawFixTap.DropReason, Counter>> counterCache =
      new ConcurrentHashMap<>();

  /**
   * Construct the counter sink.
   *
   * @param registry the Micrometer registry to which counters are registered (typically the
   *     launcher's process-wide PrometheusMeterRegistry)
   * @throws NullPointerException if {@code registry} is null
   */
  public MicrometerDropCounter(final MeterRegistry registry) {
    this.registry = Objects.requireNonNull(registry, "registry");
  }

  @Override
  public void incrementDrop(final BridgeSession session, final RawFixTap.DropReason reason) {
    final var sessionId = session.sessionId().value();
    // computeIfAbsent on ConcurrentHashMap is lock-free for the lookup-hit path (just a volatile
    // read); only first-touch per session pays the per-bin lock. EnumMap.computeIfAbsent is
    // array-indexed (one ordinal lookup) — zero-alloc on hits.
    final var perSession =
        counterCache.computeIfAbsent(sessionId, k -> new EnumMap<>(RawFixTap.DropReason.class));
    // Fast path: array-indexed EnumMap lookup. EnumMap reference reads are atomic per JLS §17.7
    // so the unsynchronised read is safe — worst case observe null and fall through to the slow
    // path which double-checks under the per-session lock.
    final var fastHit = perSession.get(reason);
    final var counter = fastHit != null ? fastHit : registerCounter(perSession, sessionId, reason);
    counter.increment();
  }

  /**
   * First-touch slow path: register a new {@link Counter} for {@code (sessionId, reason)} under the
   * per-session monitor. Double-checks the EnumMap inside the synchronized block so two threads
   * racing the same first-touch produce exactly one Counter registration.
   *
   * @param perSession the per-session reason→Counter map (also serves as the lock object)
   * @param sessionId the session id used as the {@code session} metric tag
   * @param reason the drop reason used as the {@code reason} metric tag
   * @return the {@link Counter} that should be incremented (never null)
   */
  private Counter registerCounter(
      final EnumMap<RawFixTap.DropReason, Counter> perSession,
      final String sessionId,
      final RawFixTap.DropReason reason) {
    synchronized (perSession) {
      final var existing = perSession.get(reason);
      if (existing != null) {
        return existing;
      }
      final var counter =
          Counter.builder(METRIC_NAME)
              .description("Count of RawFix events dropped, broken down by session and reason")
              .tags(Tags.of(Tag.of(TAG_SESSION, sessionId), Tag.of(TAG_REASON, reasonTag(reason))))
              .register(registry);
      perSession.put(reason, counter);
      return counter;
    }
  }

  /**
   * Map the {@link RawFixTap.DropReason} enum to its on-wire metric tag value (lowercase snake-case
   * to match Prometheus conventions).
   */
  private static String reasonTag(final RawFixTap.DropReason reason) {
    return switch (reason) {
      case DISABLED -> "disabled";
      case RATE_LIMIT -> "rate_limit";
      case OUTBOUND_QUEUE_FULL -> "outbound_queue_full";
    };
  }
}
