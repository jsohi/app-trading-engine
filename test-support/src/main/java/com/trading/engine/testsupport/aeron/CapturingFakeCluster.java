package com.trading.engine.testsupport.aeron;

import io.aeron.DirectBufferVector;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.logbuffer.BufferClaim;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.concurrent.IdleStrategy;

/**
 * Aeron {@link Cluster} test double that captures every {@code scheduleTimer(correlationId,
 * deadline)} call and supports explicit client-session registration for {@code
 * EventSink.emit}-style broadcast tests.
 *
 * <p><b>Purpose.</b> Tests that need to observe timer-scheduling side effects (e.g. RFQ TTL,
 * pricing-service request timeout, rate-limit reset windows) AND also need a real {@code
 * forEachClientSession} fold (post-Phase-3-Commit-2 broadcast invariant) cannot use {@link
 * FakeCluster} directly because that double does not record timer calls. Conversely the {@code
 * scheduledTimers} primitive-keyed {@code Long2LongHashMap} would be wasted state on the non-timer
 * paths covered by {@code FakeCluster}. {@code CapturingFakeCluster} extends the contract by
 * reimplementing the full {@link Cluster} interface without subclassing (the parent is {@code
 * final}).
 *
 * <p><b>Threading.</b> Not thread-safe — intended for single-threaded cluster-service tests
 * (matching the cluster duty-cycle invariant).
 *
 * <p><b>Allocation.</b> Allocates the {@link Long2LongHashMap}, the registered-sessions list, and
 * the {@link IdleStrategy} wrapper at construction. All {@code Cluster} interface methods are
 * allocation-free at steady state.
 *
 * <p><b>Design rationale.</b> Promoted to {@code test-support} in Phase 3 Commit 2 review-fix pass
 * to eliminate ~120-line duplication between {@code RfqStateMachineTest} and {@code
 * PriceResponseHandlerTest}; both tests previously declared structurally-identical static-nested
 * copies. A single canonical implementation here means future contract extensions (e.g. {@code
 * closeClientSession} side effects) touch one place.
 *
 * <p><b>Dependencies.</b> Aeron Cluster service API + Agrona primitive collections.
 *
 * @see FakeCluster
 * @see FakeClientSession
 */
public final class CapturingFakeCluster implements Cluster {

  /**
   * All {@code (correlationId → deadline)} pairs recorded from {@link #scheduleTimer}. Public for
   * direct read access from test assertions. Primitive-keyed to avoid Long boxing inside the
   * timer-scheduling hot path during burst tests.
   */
  public final Long2LongHashMap scheduledTimers = new Long2LongHashMap(Long.MIN_VALUE);

  /**
   * Controls the return value of {@link #scheduleTimer}. Default {@code true}. Set to {@code false}
   * from a test to simulate timer-pool exhaustion.
   */
  public boolean scheduleTimerResult = true;

  /** Registered sessions iterated by {@link #forEachClientSession}. */
  private final List<ClientSession> registeredSessions = new ArrayList<>();

  private final IdleStrategy idle =
      new IdleStrategy() {
        @Override
        public void idle(final int workCount) {}

        @Override
        public void idle() {}

        @Override
        public void reset() {}
      };

  /** Creates a fresh CapturingFakeCluster with no registered sessions and no scheduled timers. */
  public CapturingFakeCluster() {}

  /**
   * Registers a client session so it is reached during {@link #forEachClientSession} broadcast.
   * Mirror of {@link FakeCluster#addClientSession(ClientSession)}.
   *
   * @param session the session to register (must not be null)
   */
  public void addClientSession(final ClientSession session) {
    registeredSessions.add(java.util.Objects.requireNonNull(session, "session"));
  }

  /** {@inheritDoc} */
  @Override
  public boolean scheduleTimer(final long correlationId, final long deadline) {
    scheduledTimers.put(correlationId, deadline);
    return scheduleTimerResult;
  }

  /** {@inheritDoc} */
  @Override
  public boolean cancelTimer(final long correlationId) {
    return true;
  }

  /** {@inheritDoc} */
  @Override
  public int memberId() {
    return 0;
  }

  /** {@inheritDoc} */
  @Override
  public Role role() {
    return Role.LEADER;
  }

  /** {@inheritDoc} */
  @Override
  public long logPosition() {
    return 0L;
  }

  /** {@inheritDoc} */
  @Override
  public io.aeron.Aeron aeron() {
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public ClusteredServiceContainer.Context context() {
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public ClientSession getClientSession(final long clusterSessionId) {
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public Collection<ClientSession> clientSessions() {
    return registeredSessions;
  }

  /** {@inheritDoc} */
  @Override
  public void forEachClientSession(final Consumer<? super ClientSession> action) {
    java.util.Objects.requireNonNull(action, "action must not be null");
    // Index-based iteration — mirrors FakeCluster's no-Iterator contract so the broadcast loop
    // in production EventSink.emit() observes the same allocation profile in tests as in
    // production (no silent zero-alloc regression masking).
    for (int i = 0, n = registeredSessions.size(); i < n; i++) {
      action.accept(registeredSessions.get(i));
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean closeClientSession(final long clusterSessionId) {
    return false;
  }

  /** {@inheritDoc} */
  @Override
  public long time() {
    return 0L;
  }

  /** {@inheritDoc} */
  @Override
  public TimeUnit timeUnit() {
    return TimeUnit.NANOSECONDS;
  }

  /** {@inheritDoc} */
  @Override
  public long offer(final DirectBuffer buffer, final int offset, final int length) {
    return 0L;
  }

  /** {@inheritDoc} */
  @Override
  public long offer(final DirectBufferVector[] vectors) {
    return 0L;
  }

  /** {@inheritDoc} */
  @Override
  public long tryClaim(final int length, final BufferClaim bufferClaim) {
    return 0L;
  }

  /** {@inheritDoc} */
  @Override
  public IdleStrategy idleStrategy() {
    return idle;
  }
}
