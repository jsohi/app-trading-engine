package com.trading.engine.projections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

// TODO(APP-228): add JCStress @JCStressTest for release/acquire contract validation.
// Thread + CountDownLatch stress tests are primarily crash-freedom and visibility smoke tests.
// On x86-64 (TSO), monotonicity violations from plain reads are practically impossible to
// trigger. True memory ordering validation requires JCStress under systematic scheduling
// perturbation.

/**
 * Concurrent stress tests for {@link EventConsumer} cross-thread diagnostic reads. Validates that
 * monitoring threads calling {@link EventConsumer#lastProcessedSequence()}, {@link
 * EventConsumer#lastProcessedSequence(Projection)}, {@link ProjectionRegistry#getLagSnapshot()},
 * and {@link ProjectionRegistry#isHealthy()} see consistent, non-corrupt values while the poll
 * thread dispatches fragments.
 *
 * <p>These tests target <b>cross-thread visibility</b> (stale reads, ordering violations), not torn
 * reads — 64-bit aligned long reads are hardware-atomic on x86-64 / AArch64 JDK 25.
 *
 * <p>All tests are capped at 15 seconds via {@link Timeout} to prevent CI hangs if a thread
 * deadlocks or enters an infinite loop.
 *
 * @see <a href="https://linear.app/app-trade-engine/issue/APP-162">APP-162</a>
 */
@Timeout(value = 15, unit = TimeUnit.SECONDS)
class EventConsumerConcurrentReadTest {

  private static final int TEMPLATE_ID = 100;
  private static final int ITERATIONS = 500_000;

  /** Minimal projection that does nothing — just a dispatch target. */
  private static final class NoOpProjection implements Projection {
    @Override
    public void onEvent(
        final long seqNo,
        final int eventType,
        final DirectBuffer buffer,
        final int offset,
        final int length) {
      // Consumer tracks seqNo independently — projection's own tracking is unused in these tests
    }

    @Override
    public long lastProcessedSequence() {
      return 0L;
    }

    @Override
    public void reset() {
      // intentionally empty
    }
  }

  private static UnsafeBuffer makeBuffer() {
    final UnsafeBuffer buf = new UnsafeBuffer(new byte[32]);
    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    header.wrap(buf, 0).blockLength(0).templateId(TEMPLATE_ID).schemaId(1).version(1);
    return buf;
  }

  // ---------------------------------------------------------------------------
  // Concurrent dispatch + read — monotonicity
  // ---------------------------------------------------------------------------

  @Test
  void lastProcessedSequence_concurrentDispatchAndRead_valuesMonotonicallyNonDecreasing()
      throws Exception {
    final EventConsumer c = new EventConsumer();
    final NoOpProjection p = new NoOpProjection();
    c.registerProjection(p, TEMPLATE_ID);
    c.markStartedForTest();

    final UnsafeBuffer buf = makeBuffer();
    final CountDownLatch startLatch = new CountDownLatch(1);
    final AtomicBoolean writerDone = new AtomicBoolean(false);
    final AtomicBoolean readerFailed = new AtomicBoolean(false);
    final StringBuilder readerError = new StringBuilder();

    final Thread writer =
        new Thread(
            () -> {
              try {
                startLatch.await();
                for (int i = 0; i < ITERATIONS; i++) {
                  c.onFragment(buf, 0, 32, null);
                }
              } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
              } finally {
                writerDone.set(true);
              }
            },
            "writer");

    final Thread reader =
        new Thread(
            () -> {
              try {
                startLatch.await();
              } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
              }
              long prevGlobal = 0L;
              long prevProjection = 0L;
              while (!writerDone.get() || prevGlobal < ITERATIONS) {
                final long global = c.lastProcessedSequence();
                final long projection = c.lastProcessedSequence(p);

                if (global < 0L || projection < 0L) {
                  readerFailed.set(true);
                  readerError
                      .append("negative value: global=")
                      .append(global)
                      .append(" projection=")
                      .append(projection);
                  return;
                }
                if (global < prevGlobal) {
                  readerFailed.set(true);
                  readerError
                      .append("global monotonicity violated: prev=")
                      .append(prevGlobal)
                      .append(" current=")
                      .append(global);
                  return;
                }
                if (projection < prevProjection) {
                  readerFailed.set(true);
                  readerError
                      .append("projection monotonicity violated: prev=")
                      .append(prevProjection)
                      .append(" current=")
                      .append(projection);
                  return;
                }
                prevGlobal = global;
                prevProjection = projection;
                Thread.onSpinWait();
              }
            },
            "reader");

    writer.start();
    reader.start();
    startLatch.countDown();

    writer.join(10_000);
    reader.join(10_000);

    assertFalse(writer.isAlive(), "writer did not finish in time");
    assertFalse(reader.isAlive(), "reader did not finish in time");
    assertFalse(readerFailed.get(), readerError.toString());
    assertEquals(ITERATIONS, c.lastProcessedSequence());
    assertEquals(ITERATIONS, c.lastProcessedSequence(p));
  }

  // ---------------------------------------------------------------------------
  // Concurrent dispatch + getLagSnapshot / isHealthy
  // ---------------------------------------------------------------------------

  @Test
  void getLagSnapshot_concurrentDispatchAndRead_allLagValuesNonNegative() throws Exception {
    final EventConsumer c = new EventConsumer();
    final NoOpProjection p = new NoOpProjection();
    c.registerProjection(p, TEMPLATE_ID);
    c.markStartedForTest();

    final ProjectionRegistry registry = new ProjectionRegistry(c, 1_000_000L);
    registry.register("p", p);

    final UnsafeBuffer buf = makeBuffer();
    final CountDownLatch startLatch = new CountDownLatch(1);
    final AtomicBoolean writerDone = new AtomicBoolean(false);
    final AtomicBoolean readerFailed = new AtomicBoolean(false);
    final StringBuilder readerError = new StringBuilder();

    final Thread writer =
        new Thread(
            () -> {
              try {
                startLatch.await();
                for (int i = 0; i < ITERATIONS; i++) {
                  c.onFragment(buf, 0, 32, null);
                }
              } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
              } finally {
                writerDone.set(true);
              }
            },
            "writer");

    final Thread reader =
        new Thread(
            () -> {
              try {
                startLatch.await();
              } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
              }
              while (!writerDone.get()) {
                final Map<String, Long> lag = registry.getLagSnapshot();
                for (final Map.Entry<String, Long> entry : lag.entrySet()) {
                  if (entry.getValue() < 0L) {
                    readerFailed.set(true);
                    readerError
                        .append("negative lag for ")
                        .append(entry.getKey())
                        .append(": ")
                        .append(entry.getValue());
                    return;
                  }
                }
                // isHealthy should not throw
                registry.isHealthy();
                Thread.onSpinWait();
              }
            },
            "reader");

    writer.start();
    reader.start();
    startLatch.countDown();

    writer.join(10_000);
    reader.join(10_000);

    assertFalse(writer.isAlive(), "writer did not finish in time");
    assertFalse(reader.isAlive(), "reader did not finish in time");
    assertFalse(readerFailed.get(), readerError.toString());
  }

  // ---------------------------------------------------------------------------
  // Concurrent dispatch + close — crash freedom
  // ---------------------------------------------------------------------------

  @Test
  void lastProcessedSequence_concurrentDispatchAndClose_noException() throws Exception {
    final EventConsumer c = new EventConsumer();
    final NoOpProjection p = new NoOpProjection();
    c.registerProjection(p, TEMPLATE_ID);
    c.markStartedForTest();

    final UnsafeBuffer buf = makeBuffer();
    final CountDownLatch startLatch = new CountDownLatch(1);
    final AtomicBoolean done = new AtomicBoolean(false);
    final AtomicBoolean readerFailed = new AtomicBoolean(false);
    final StringBuilder readerError = new StringBuilder();

    final Thread writer =
        new Thread(
            () -> {
              try {
                startLatch.await();
                for (int i = 0; i < 50_000; i++) {
                  c.onFragment(buf, 0, 32, null);
                }
                c.close();
              } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
              } finally {
                done.set(true);
              }
            },
            "writer");

    final Thread reader =
        new Thread(
            () -> {
              try {
                startLatch.await();
              } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
              }
              while (!done.get()) {
                try {
                  final long global = c.lastProcessedSequence();
                  final long projection = c.lastProcessedSequence(p);
                  if (global < 0L || projection < 0L) {
                    readerFailed.set(true);
                    readerError
                        .append("negative value: global=")
                        .append(global)
                        .append(" projection=")
                        .append(projection);
                    return;
                  }
                } catch (final Exception e) {
                  readerFailed.set(true);
                  readerError.append("exception: ").append(e);
                  return;
                }
                Thread.onSpinWait();
              }
            },
            "reader");

    writer.start();
    reader.start();
    startLatch.countDown();

    writer.join(10_000);
    reader.join(10_000);

    assertFalse(writer.isAlive(), "writer did not finish in time");
    assertFalse(reader.isAlive(), "reader did not finish in time");
    assertFalse(readerFailed.get(), readerError.toString());
    assertTrue(c.isClosed());
  }

  // ---------------------------------------------------------------------------
  // Concurrent isHealthy + close — crash freedom
  // ---------------------------------------------------------------------------

  @Test
  void isHealthy_concurrentDispatchAndClose_noException() throws Exception {
    final EventConsumer c = new EventConsumer();
    final NoOpProjection p = new NoOpProjection();
    c.registerProjection(p, TEMPLATE_ID);
    c.markStartedForTest();

    final ProjectionRegistry registry = new ProjectionRegistry(c, 1_000_000L);
    registry.register("p", p);

    final UnsafeBuffer buf = makeBuffer();
    final CountDownLatch startLatch = new CountDownLatch(1);
    final AtomicBoolean done = new AtomicBoolean(false);
    final AtomicBoolean readerFailed = new AtomicBoolean(false);
    final StringBuilder readerError = new StringBuilder();

    final Thread writer =
        new Thread(
            () -> {
              try {
                startLatch.await();
                for (int i = 0; i < 50_000; i++) {
                  c.onFragment(buf, 0, 32, null);
                }
                c.close();
              } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
              } finally {
                done.set(true);
              }
            },
            "writer");

    final Thread reader =
        new Thread(
            () -> {
              try {
                startLatch.await();
              } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
              }
              while (!done.get()) {
                try {
                  registry.isHealthy();
                } catch (final Exception e) {
                  readerFailed.set(true);
                  readerError.append("exception: ").append(e);
                  return;
                }
                Thread.onSpinWait();
              }
              // Final check after close
              if (c.isClosed()) {
                assertFalse(registry.isHealthy());
              }
            },
            "reader");

    writer.start();
    reader.start();
    startLatch.countDown();

    writer.join(10_000);
    reader.join(10_000);

    assertFalse(writer.isAlive(), "writer did not finish in time");
    assertFalse(reader.isAlive(), "reader did not finish in time");
    assertFalse(readerFailed.get(), readerError.toString());
    assertTrue(c.isClosed());
    assertFalse(registry.isHealthy());
  }
}
