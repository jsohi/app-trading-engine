package com.trading.engine.testsupport.aeron;

import io.aeron.DirectBufferVector;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.logbuffer.BufferClaim;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.IdleStrategy;

/**
 * Minimal test double for Aeron {@link Cluster} that returns a fixed timestamp and tracks
 * idle-strategy invocations.
 *
 * <p>Suitable for unit-testing {@link io.aeron.cluster.service.ClusteredService} implementations
 * without spinning up a real Aeron cluster.
 *
 * <p>Not thread-safe — intended for single-threaded cluster service tests.
 *
 * <p><b>Field visibility:</b> {@code idleCount} is {@code public} to preserve direct-field-access
 * from existing test call sites. The error handler is exposed via {@link
 * #setErrorHandler(ErrorHandler)}.
 *
 * @see FakeClientSession
 */
public final class FakeCluster implements Cluster {

  private final long time;
  private final IdleStrategy idleStrategy;

  /** Tracks the number of idle() invocations via the wrapped IdleStrategy. */
  public int idleCount;

  /**
   * When non-null, {@link #context()} returns a cached {@link ClusteredServiceContainer.Context}
   * wired with this error handler. Used by the warning-threshold snapshot test (APP-150). Setting
   * this field invalidates any previously cached context.
   */
  private ErrorHandler errorHandler;

  private ClusteredServiceContainer.Context cachedContext;

  /**
   * Creates a fake cluster with a fixed timestamp.
   *
   * @param time the fixed timestamp returned by {@link #time()}, in nanoseconds
   */
  public FakeCluster(final long time) {
    this.time = time;
    this.idleStrategy =
        new IdleStrategy() {
          @Override
          public void idle(final int workCount) {
            idleCount++;
          }

          @Override
          public void idle() {
            idleCount++;
          }

          @Override
          public void reset() {}
        };
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

  /**
   * Sets the error handler for {@link #context()}. Invalidates any cached context.
   *
   * @param errorHandler the error handler, or {@code null} to clear
   */
  public void setErrorHandler(final ErrorHandler errorHandler) {
    this.errorHandler = errorHandler;
    this.cachedContext = null;
  }

  /**
   * Returns a cached {@link ClusteredServiceContainer.Context} with the configured error handler,
   * or {@code null} if no error handler is set. The same instance is returned on repeated calls
   * (matching real Aeron behavior).
   *
   * @return context wired with error handler, or {@code null}
   */
  @Override
  public ClusteredServiceContainer.Context context() {
    if (errorHandler == null) {
      return null;
    }
    if (cachedContext == null) {
      cachedContext = new ClusteredServiceContainer.Context().errorHandler(errorHandler);
    }
    return cachedContext;
  }

  /** {@inheritDoc} */
  @Override
  public ClientSession getClientSession(final long clusterSessionId) {
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public Collection<ClientSession> clientSessions() {
    return List.of();
  }

  /** {@inheritDoc} */
  @Override
  public void forEachClientSession(final Consumer<? super ClientSession> action) {}

  /** {@inheritDoc} */
  @Override
  public boolean closeClientSession(final long clusterSessionId) {
    return false;
  }

  /**
   * Returns the fixed timestamp provided at construction.
   *
   * @return fixed cluster time in nanoseconds
   */
  @Override
  public long time() {
    return time;
  }

  /** {@inheritDoc} */
  @Override
  public TimeUnit timeUnit() {
    return TimeUnit.NANOSECONDS;
  }

  /** {@inheritDoc} */
  @Override
  public boolean scheduleTimer(final long correlationId, final long deadline) {
    return false;
  }

  /** {@inheritDoc} */
  @Override
  public boolean cancelTimer(final long correlationId) {
    return false;
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
    return idleStrategy;
  }
}
