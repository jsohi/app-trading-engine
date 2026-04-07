package com.trading.engine.projections;

import java.util.Map;
import java.util.Set;
import org.agrona.collections.Object2ObjectHashMap;

/**
 * Names and tracks the projections running alongside an {@link EventConsumer}, and reports their
 * lag for health checks and monitoring.
 *
 * <p>The registry is a <b>diagnostic</b> layer: it does NOT wire projections into the consumer's
 * dispatch table. Callers register each projection with the {@link EventConsumer} (which decides
 * the dispatch routing) AND with the {@link ProjectionRegistry} (which gives it a name for
 * reporting). Keeping the two concerns separate makes them independently testable and lets the
 * registry cover projections that are not in this consumer's dispatch path (though those will
 * always show zero progress).
 *
 * <p><b>Lag computation:</b> for each projection, {@code lag = consumer.lastProcessedSequence() -
 * consumer.lastProcessedSequence(projection)}. The registry uses the consumer-owned per-projection
 * tracking (not {@link Projection#lastProcessedSequence()}) as the authoritative source so a
 * projection that forgets to update its own internal tracking cannot produce a misleading lag
 * report.
 *
 * <p><b>Health:</b> {@link #isHealthy()} returns {@code true} iff every registered projection's lag
 * is {@code <= lagThreshold}. An empty registry is vacuously healthy. The threshold is fixed at
 * construction; a richer model (per-projection thresholds, error-state tracking, replay flag) is
 * deferred to the metrics PR (APP-41 / APP-49).
 *
 * <p><b>Thread-safety:</b> not thread-safe. Construction and {@link #register} must happen on the
 * setup thread before the consumer's poll loop starts. {@link #getLag} and {@link #isHealthy} are
 * intended to be called from the poll thread for consistent reads of the consumer's counters.
 * Cross-thread monitoring reads are possible but may see momentarily inconsistent values; the lag
 * value is defensively clamped to {@code >= 0} so cross-thread races can never produce a negative
 * diagnostic.
 */
public final class ProjectionRegistry {

  private final EventConsumer consumer;
  private final long lagThreshold;
  private final Object2ObjectHashMap<String, Projection> byName = new Object2ObjectHashMap<>();

  /**
   * @param consumer the consumer whose ingress counter and per-projection tracking define the lag
   *     math. Must not be null.
   * @param lagThreshold maximum acceptable lag (in messages) for a projection to count as healthy.
   *     Must be non-negative.
   * @throws NullPointerException if {@code consumer} is null
   * @throws IllegalArgumentException if {@code lagThreshold} is negative
   */
  public ProjectionRegistry(final EventConsumer consumer, final long lagThreshold) {
    if (consumer == null) {
      throw new NullPointerException("consumer must not be null");
    }
    if (lagThreshold < 0L) {
      throw new IllegalArgumentException("lagThreshold must be >= 0, was " + lagThreshold);
    }
    this.consumer = consumer;
    this.lagThreshold = lagThreshold;
  }

  /**
   * Register a projection under the given name. Names must be unique — re-registering the same name
   * is rejected for loud failure (rather than silently replacing). The same projection instance can
   * be registered under multiple names if desired, though that is unusual.
   *
   * @throws NullPointerException if {@code name} or {@code projection} is null
   * @throws IllegalArgumentException if {@code name} is already registered
   */
  public void register(final String name, final Projection projection) {
    if (name == null) {
      throw new NullPointerException("name must not be null");
    }
    if (projection == null) {
      throw new NullPointerException("projection must not be null");
    }
    if (byName.containsKey(name)) {
      throw new IllegalArgumentException("projection name already registered: " + name);
    }
    byName.put(name, projection);
  }

  /**
   * A fresh snapshot of per-projection lag: {@code name → max(0, consumer head -
   * consumer.lastProcessedSequence(projection))}. Allocates a new map (and boxes each {@code Long}
   * value) on every call — diagnostic API, allocation is acceptable. The returned map is an Agrona
   * {@link Object2ObjectHashMap} behind the {@link Map} interface; no {@code java.util.HashMap}
   * instances leak from this method.
   */
  public Map<String, Long> getLag() {
    final Object2ObjectHashMap<String, Long> snapshot = new Object2ObjectHashMap<>();
    final long head = consumer.lastProcessedSequence();
    for (final Map.Entry<String, Projection> entry : byName.entrySet()) {
      final long lag = head - consumer.lastProcessedSequence(entry.getValue());
      snapshot.put(entry.getKey(), lag < 0L ? 0L : lag);
    }
    return snapshot;
  }

  /**
   * {@code true} iff every registered projection's lag is {@code <= lagThreshold}. Empty registry
   * is vacuously healthy. Zero allocation.
   */
  public boolean isHealthy() {
    final long head = consumer.lastProcessedSequence();
    for (final Projection projection : byName.values()) {
      final long lag = head - consumer.lastProcessedSequence(projection);
      if (lag > lagThreshold) {
        return false;
      }
    }
    return true;
  }

  public int size() {
    return byName.size();
  }

  public long lagThreshold() {
    return lagThreshold;
  }

  /**
   * The set of registered projection names. Snapshot — modifying the returned set does not affect
   * the registry. For diagnostics only.
   */
  public Set<String> names() {
    return Set.copyOf(byName.keySet());
  }
}
