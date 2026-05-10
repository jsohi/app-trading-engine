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
    // EnumMap is NOT thread-safe so we synchronise on the per-session map for first-touch only.
    // The synchronisation block is short (one EnumMap probe + at most one Counter.register call)
    // and contention is per-session — different sessions have different per-session locks.
    Counter counter = perSession.get(reason);
    if (counter == null) {
      synchronized (perSession) {
        counter = perSession.get(reason);
        if (counter == null) {
          counter =
              Counter.builder(METRIC_NAME)
                  .description("Count of RawFix events dropped, broken down by session and reason")
                  .tags(
                      Tags.of(
                          Tag.of(TAG_SESSION, sessionId), Tag.of(TAG_REASON, reasonTag(reason))))
                  .register(registry);
          perSession.put(reason, counter);
        }
      }
    }
    counter.increment();
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
