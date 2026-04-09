package com.trading.refdata;

import com.trading.refdata.spi.ReferenceDataEncoder;
import com.trading.refdata.spi.ReferenceDataLoader;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.NanoClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Orchestrates reference data loading at startup: load records from source, encode into SBE batch
 * commands, send to cluster, and await acknowledgements.
 *
 * <p>This class is generic — it works with any {@code (loader, encoder)} pair.
 */
public final class ReferenceDataOrchestrator {

  private static final Logger LOG = LoggerFactory.getLogger(ReferenceDataOrchestrator.class);
  private static final long ACK_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(10);
  private static final long POLL_INTERVAL_NS = TimeUnit.MILLISECONDS.toNanos(1);
  private static final long SEND_RETRY_INITIAL_NS = TimeUnit.MICROSECONDS.toNanos(100);
  private static final long SEND_RETRY_MAX_NS = TimeUnit.MILLISECONDS.toNanos(10);
  private static final int BUFFER_INITIAL_CAPACITY = 64 * 1024;

  private final NanoClock nanoClock;

  public ReferenceDataOrchestrator(final NanoClock nanoClock) {
    this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
  }

  /**
   * Load all records from the given source, encode them as SBE batch commands, send to the cluster,
   * and await acknowledgements.
   *
   * @param loader reads records from external source
   * @param encoder encodes records into SBE batch commands
   * @param sender sends encoded command to cluster ingress
   * @param pollEgress polls the cluster egress for responses (drives the response listener)
   * @param collector accumulates ack/reject responses from the egress listener
   * @throws ReferenceDataLoadException if any record is rejected or a timeout occurs
   */
  public <T> void load(
      final ReferenceDataLoader<T> loader,
      final ReferenceDataEncoder<T> encoder,
      final ClusterCommandSender sender,
      final Runnable pollEgress,
      final ResponseCollector collector)
      throws ReferenceDataLoadException {

    final long startNs = nanoClock.nanoTime();
    final List<T> records = loader.load();

    if (records.isEmpty()) {
      LOG.info("No records to load from {}", loader.sourceName());
      return;
    }

    final int maxBatch = encoder.maxBatchSize();
    if (maxBatch <= 0) {
      throw new ReferenceDataLoadException(
          encoder.entityType(), "encoder.maxBatchSize() must be > 0, got " + maxBatch);
    }

    final MutableDirectBuffer buffer = new ExpandableArrayBuffer(BUFFER_INITIAL_CAPACITY);
    int totalLoaded = 0;

    for (int from = 0; from < records.size(); from += maxBatch) {
      final int to = Math.min(from + maxBatch, records.size());
      final int batchSize = to - from;

      final int encodedLength = encoder.encodeBatch(records, from, to, buffer, 0);
      if (encodedLength <= 0) {
        throw new ReferenceDataLoadException(
            encoder.entityType(),
            "encoder produced invalid length "
                + encodedLength
                + " for batch ["
                + from
                + ", "
                + to
                + ")");
      }

      sendWithRetry(sender, buffer, encodedLength, encoder.entityType());

      collector.expectResponses(batchSize);
      awaitResponses(pollEgress, collector, encoder.entityType(), loader.sourceName());

      totalLoaded += collector.loadedCount();
    }

    final long elapsedMs = TimeUnit.NANOSECONDS.toMillis(nanoClock.nanoTime() - startNs);
    LOG.info("Loaded {} records from {} in {}ms", totalLoaded, loader.sourceName(), elapsedMs);
  }

  private void sendWithRetry(
      final ClusterCommandSender sender,
      final MutableDirectBuffer buf,
      final int length,
      final String entityType)
      throws ReferenceDataLoadException {

    final long deadlineNs = nanoClock.nanoTime() + ACK_TIMEOUT_NS;
    long backoffNs = SEND_RETRY_INITIAL_NS;

    while (true) {
      final long result = sender.send(buf, 0, length);
      if (result >= 0) {
        return;
      }
      if (nanoClock.nanoTime() >= deadlineNs) {
        throw new ReferenceDataLoadException(
            entityType, "timed out sending command to cluster (back-pressure)");
      }
      LockSupport.parkNanos(backoffNs);
      backoffNs = Math.min(backoffNs * 2, SEND_RETRY_MAX_NS);
    }
  }

  private void awaitResponses(
      final Runnable pollEgress,
      final ResponseCollector collector,
      final String entityType,
      final String sourceName)
      throws ReferenceDataLoadException {

    final long deadlineNs = nanoClock.nanoTime() + ACK_TIMEOUT_NS;

    while (!collector.isComplete()) {
      pollEgress.run();

      if (collector.hasRejections()) {
        throw new ReferenceDataLoadException(
            entityType,
            collector.rejectedCount()
                + " record(s) rejected from "
                + sourceName
                + ": "
                + collector.rejectionReasons());
      }

      if (nanoClock.nanoTime() >= deadlineNs) {
        throw new ReferenceDataLoadException(
            entityType,
            "timed out waiting for cluster acks from "
                + sourceName
                + " (received "
                + collector.loadedCount()
                + " of "
                + collector.expectedCount()
                + ")");
      }

      LockSupport.parkNanos(POLL_INTERVAL_NS);
    }
  }
}
