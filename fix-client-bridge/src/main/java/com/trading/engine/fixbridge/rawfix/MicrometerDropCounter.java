package com.trading.engine.fixbridge.rawfix;

import com.trading.engine.fixbridge.transport.BridgeSession;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import java.util.EnumMap;
import java.util.Objects;

/**
 * Production {@link RawFixTap.DropCounter} backed by Micrometer.
 *
 * <p><b>Purpose.</b> Replaces {@link RawFixTap.DropCounter#NOOP} for production use. Increments a
 * pre-registered counter per {@link RawFixTap.DropReason} so the operator's Prometheus dashboard
 * can graph {@code fixbridge_rawfix_dropped_total{reason}} per §3.5 observability requirements. The
 * launcher binds this once at boot.
 *
 * <p><b>Counter naming.</b> One Micrometer counter per {@link RawFixTap.DropReason} — three series
 * total ({@code disabled}, {@code rate_limit}, {@code outbound_queue_full}). The {@code reason} tag
 * distinguishes them.
 *
 * <p><b>Why not per-session?</b> An earlier design (PR #71 R1) tagged counters by session id (UUID
 * per browser connection) which was a textbook Prometheus-cardinality footgun: every connection
 * added 3 fresh series that never evicted, ballooning the registry over the bridge process lifetime
 * (Gemini high-priority finding on PR #71 R2). For per-session forensic granularity, operators
 * consult the audit log (which carries sessionId on every entry) rather than the metrics dashboard.
 *
 * <p><b>Threading.</b> Thread-safe AND lock-free on the hot path. The {@link EnumMap} is built once
 * at construction (immutable post-construction); subsequent {@link #incrementDrop} calls are
 * pure-reader so no synchronisation is required. Micrometer counters themselves are lock-free
 * (atomic-double-backed). Multiple Netty event loops may concurrently increment.
 *
 * <p><b>Allocation.</b> Per-increment: one {@link EnumMap#get} (array-indexed, zero-alloc) + one
 * {@link Counter#increment} (atomic-double add, zero-alloc). True zero-alloc on the hot path.
 * Construction allocates exactly 3 Counters + the EnumMap backing array.
 *
 * <p><b>Lifecycle.</b> One instance per bridge process, injected at launcher boot.
 *
 * <p><b>Dependencies.</b> {@link io.micrometer.core.instrument.MeterRegistry} (any impl —
 * production binds Prometheus).
 */
public final class MicrometerDropCounter implements RawFixTap.DropCounter {

  /** Micrometer counter name for RawFix drops. */
  public static final String METRIC_NAME = "fixbridge_rawfix_dropped_total";

  /** Tag key for the drop-reason label. */
  private static final String TAG_REASON = "reason";

  /**
   * Pre-registered counter per drop reason. Built once at construction and never mutated, so the
   * hot-path {@link #incrementDrop} is lock-free and zero-alloc. The {@link EnumMap} backing array
   * is constant-sized (one slot per enum constant) and indexed by ordinal — faster and smaller than
   * {@link java.util.HashMap} for enum keys.
   */
  private final EnumMap<RawFixTap.DropReason, Counter> counters;

  /**
   * Construct the counter sink.
   *
   * @param registry the Micrometer registry to which counters are registered (typically the
   *     launcher's process-wide PrometheusMeterRegistry)
   * @throws NullPointerException if {@code registry} is null
   */
  public MicrometerDropCounter(final MeterRegistry registry) {
    Objects.requireNonNull(registry, "registry");
    this.counters = new EnumMap<>(RawFixTap.DropReason.class);
    // Pre-register one counter per drop reason — three series total.
    for (final var reason : RawFixTap.DropReason.values()) {
      final var counter =
          Counter.builder(METRIC_NAME)
              .description("Count of RawFix events dropped, broken down by reason")
              .tags(Tags.of(Tag.of(TAG_REASON, reasonTag(reason))))
              .register(registry);
      counters.put(reason, counter);
    }
  }

  /**
   * Increment the drop counter for the given reason. The {@code session} parameter is part of the
   * {@link RawFixTap.DropCounter} SAM contract but is intentionally not surfaced as a Micrometer
   * tag (see class Javadoc for the cardinality rationale); operators correlate drops to sessions
   * via the audit log.
   *
   * <p><b>Allocation.</b> Zero-alloc — a single {@link EnumMap#get} (array-indexed) plus one {@link
   * Counter#increment} (atomic-double add).
   *
   * @param session the bridge session whose RawFix tap dropped a frame (unused here; required by
   *     the SAM contract for forensic correlation via audit log)
   * @param reason the drop reason; must be one of the {@link RawFixTap.DropReason} enum values
   */
  @Override
  public void incrementDrop(final BridgeSession session, final RawFixTap.DropReason reason) {
    counters.get(reason).increment();
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
