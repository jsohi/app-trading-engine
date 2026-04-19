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
 * is {@code <= lagThreshold} AND the consumer is not closed. An empty registry is vacuously
 * healthy. The threshold is fixed at construction; a richer model (per-projection thresholds,
 * error-state tracking, replay flag) is deferred to the metrics PR (APP-41 / APP-49).
 *
 * <p><b>Thread-safety:</b> construction and {@link #register} must happen on the setup thread
 * before the consumer's poll loop starts. {@link #getLagSnapshot}, {@link #fillLag}, and {@link
 * #isHealthy} are safe to call from any thread after the consumer has been started — they read the
 * consumer's release/acquire-backed counters for cross-thread visibility without locks. Lag values
 * represent an approximate point-in-time snapshot; slight staleness is expected (the monitoring
 * thread may transiently over- or under-report lag by a small number of messages) but torn reads
 * are impossible. Lag is defensively clamped to {@code >= 0}. A closed consumer reports lag = 0 and
 * {@link #isHealthy} returns {@code false} — callers should check {@code consumer.isClosed()} if
 * they need to distinguish "healthy" from "shut down".
 */
public final class ProjectionRegistry {

  private final EventConsumer consumer;
  private final long lagThreshold;
  private final Object2ObjectHashMap<String, Projection> byName = new Object2ObjectHashMap<>();

  /**
   * Creates a projection registry backed by the given consumer.
   *
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
   * @param name the projection name for diagnostic reporting; must not be null
   * @param projection the projection to register; must not be null
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
   * Allocating snapshot of per-projection lag: {@code name → max(0, consumer head -
   * consumer.lastProcessedSequence(projection))}. Allocates a new map (and boxes each {@code Long}
   * value) on every call — convenience for HTTP diagnostic endpoints where allocation is
   * acceptable. For zero-allocation monitoring, use {@link #fillLag(long[], String[])}.
   *
   * <p>The returned map is an Agrona {@link Object2ObjectHashMap} behind the {@link Map} interface;
   * no {@code java.util.HashMap} instances leak from this method.
   *
   * @return fresh map of projection name → lag (always non-negative)
   */
  public Map<String, Long> getLagSnapshot() {
    final Object2ObjectHashMap<String, Long> snapshot = new Object2ObjectHashMap<>();
    final long head = consumer.lastProcessedSequence();
    for (final Map.Entry<String, Projection> entry : byName.entrySet()) {
      final long lag = head - consumer.lastProcessedSequence(entry.getValue());
      snapshot.put(entry.getKey(), lag < 0L ? 0L : lag);
    }
    return snapshot;
  }

  /**
   * Fill the caller-provided arrays with per-projection lag values and names. Both arrays are
   * written in the same iteration order, so {@code lagOut[i]} corresponds to {@code namesOut[i]}.
   * Zero-allocation after the initial array creation by the caller. Use {@link #size()} to
   * determine the required array lengths.
   *
   * <p>Note: {@link Object2ObjectHashMap} does not guarantee insertion order, so the iteration
   * order may differ from registration order. It is however stable across calls as long as no new
   * projections are registered (which is the case after startup).
   *
   * @param lagOut array to fill with lag values; must have length {@code >= size()}
   * @param namesOut array to fill with projection names; must have length {@code >= size()}
   * @return the number of projections written (equal to {@link #size()})
   * @throws NullPointerException if either array is null
   * @throws IllegalArgumentException if either array is undersized
   */
  public int fillLag(final long[] lagOut, final String[] namesOut) {
    if (lagOut == null) {
      throw new NullPointerException("lagOut must not be null");
    }
    if (namesOut == null) {
      throw new NullPointerException("namesOut must not be null");
    }
    final int count = byName.size();
    if (lagOut.length < count) {
      throw new IllegalArgumentException(
          "lagOut.length " + lagOut.length + " < projection count " + count);
    }
    if (namesOut.length < count) {
      throw new IllegalArgumentException(
          "namesOut.length " + namesOut.length + " < projection count " + count);
    }
    final long head = consumer.lastProcessedSequence();
    int i = 0;
    for (final Map.Entry<String, Projection> entry : byName.entrySet()) {
      final long lag = head - consumer.lastProcessedSequence(entry.getValue());
      namesOut[i] = entry.getKey();
      lagOut[i] = lag < 0L ? 0L : lag;
      i++;
    }
    return count;
  }

  /**
   * Returns {@code true} iff every registered projection's lag is within the threshold AND the
   * consumer is not closed. A closed consumer is unhealthy by definition — it cannot make progress.
   * An empty registry is vacuously healthy (assuming the consumer is alive).
   *
   * <p>Iterating {@link Object2ObjectHashMap#values()} uses Agrona's cached flyweight iterator —
   * zero allocation after the first call. Not on the dispatch hot path.
   *
   * @return {@code true} if all projections are within lag threshold and consumer is alive
   */
  public boolean isHealthy() {
    if (consumer.isClosed()) {
      return false;
    }
    final long head = consumer.lastProcessedSequence();
    for (final Projection projection : byName.values()) {
      final long lag = head - consumer.lastProcessedSequence(projection);
      if (lag > lagThreshold) {
        return false;
      }
    }
    return true;
  }

  /**
   * The number of registered projections.
   *
   * @return the projection count
   */
  public int size() {
    return byName.size();
  }

  /**
   * The configured lag threshold.
   *
   * @return the maximum acceptable lag in messages
   */
  public long lagThreshold() {
    return lagThreshold;
  }

  /**
   * The set of registered projection names. Snapshot — modifying the returned set does not affect
   * the registry. For diagnostics only.
   *
   * @return an unmodifiable copy of the projection names
   */
  public Set<String> names() {
    return Set.copyOf(byName.keySet());
  }
}
