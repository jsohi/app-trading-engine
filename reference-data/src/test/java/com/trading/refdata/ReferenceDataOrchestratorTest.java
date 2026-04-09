package com.trading.refdata;

import static org.junit.jupiter.api.Assertions.*;

import com.trading.refdata.spi.ReferenceDataEncoder;
import com.trading.refdata.spi.ReferenceDataLoader;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SystemNanoClock;
import org.junit.jupiter.api.Test;

final class ReferenceDataOrchestratorTest {

  private static final long ACK_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(10);

  private final ReferenceDataOrchestrator orchestrator =
      new ReferenceDataOrchestrator(SystemNanoClock.INSTANCE);

  @Test
  void happyPathLoadsAllRecords() throws Exception {
    final List<String> records = List.of("rec1", "rec2", "rec3");
    final ReferenceDataLoader<String> loader = stubLoader(records, "test.yaml");
    final ReferenceDataEncoder<String> encoder = stubEncoder(10);
    final ResponseCollector collector = new ResponseCollector();

    // Simulate: sender accepts, egress delivers acks immediately
    final ClusterCommandSender sender = (buf, off, len) -> 1L;
    final Runnable pollEgress =
        () -> {
          for (int i = 0; i < collector.expectedCount(); i++) {
            collector.onLoaded();
          }
        };

    orchestrator.load(loader, encoder, sender, pollEgress, collector);

    assertEquals(3, collector.loadedCount());
    assertEquals(0, collector.rejectedCount());
  }

  @Test
  void emptyRecordsIsNoOp() throws Exception {
    final ReferenceDataLoader<String> loader = stubLoader(List.of(), "empty.yaml");
    final ReferenceDataEncoder<String> encoder = stubEncoder(10);
    final ResponseCollector collector = new ResponseCollector();

    orchestrator.load(loader, encoder, (buf, off, len) -> 1L, () -> {}, collector);
    // No exception — no records to load
  }

  @Test
  void rejectionThrows() {
    final List<String> records = List.of("rec1");
    final ReferenceDataLoader<String> loader = stubLoader(records, "reject.yaml");
    final ReferenceDataEncoder<String> encoder = stubEncoder(10);
    final ResponseCollector collector = new ResponseCollector();

    final ClusterCommandSender sender = (buf, off, len) -> 1L;
    final Runnable pollEgress = () -> collector.onRejected("invalid account");

    final ReferenceDataLoadException ex =
        assertThrows(
            ReferenceDataLoadException.class,
            () -> orchestrator.load(loader, encoder, sender, pollEgress, collector));
    assertTrue(ex.getMessage().contains("rejected"));
    assertTrue(ex.getMessage().contains("invalid account"));
  }

  @Test
  void multipleBatchesLoadCorrectly() throws Exception {
    final List<String> records = List.of("r1", "r2", "r3", "r4", "r5");
    final ReferenceDataLoader<String> loader = stubLoader(records, "multi.yaml");
    final ReferenceDataEncoder<String> encoder =
        stubEncoder(2); // max batch size = 2, so 3 batches: 2+2+1
    final ResponseCollector collector = new ResponseCollector();

    final int[] sendCount = {0};
    final ClusterCommandSender sender =
        (buf, off, len) -> {
          sendCount[0]++;
          return 1L;
        };
    final Runnable pollEgress =
        () -> {
          for (int i = 0; i < collector.expectedCount(); i++) {
            collector.onLoaded();
          }
        };

    orchestrator.load(loader, encoder, sender, pollEgress, collector);

    assertEquals(3, sendCount[0], "5 records with batch size 2 should produce 3 sends");
  }

  @Test
  void loaderExceptionPropagates() {
    final ReferenceDataLoader<String> loader =
        new ReferenceDataLoader<String>() {
          @Override
          public List<String> load() throws ReferenceDataLoadException {
            throw new ReferenceDataLoadException("Account", "file not found");
          }

          @Override
          public String sourceName() {
            return "bad.yaml";
          }
        };
    final ReferenceDataEncoder<String> encoder = stubEncoder(10);
    final ResponseCollector collector = new ResponseCollector();

    final ReferenceDataLoadException ex =
        assertThrows(
            ReferenceDataLoadException.class,
            () -> orchestrator.load(loader, encoder, (b, o, l) -> 1L, () -> {}, collector));
    assertTrue(ex.getMessage().contains("file not found"));
  }

  @Test
  void sendWithRetryTimesOutOnPersistentBackPressure() {
    final long[] nowNs = {0L};
    final NanoClock fakeClock = () -> nowNs[0];
    final ReferenceDataOrchestrator timedOrchestrator = new ReferenceDataOrchestrator(fakeClock);

    final List<String> records = List.of("rec1");
    final ReferenceDataLoader<String> loader = stubLoader(records, "timeout.yaml");
    final ReferenceDataEncoder<String> encoder = stubEncoder(10);
    final ResponseCollector collector = new ResponseCollector();

    // Sender always returns back-pressure; clock jumps past the 10s deadline on second call
    final ClusterCommandSender sender =
        (buf, off, len) -> {
          nowNs[0] += ACK_TIMEOUT_NS + 1;
          return -1L;
        };

    final ReferenceDataLoadException ex =
        assertThrows(
            ReferenceDataLoadException.class,
            () -> timedOrchestrator.load(loader, encoder, sender, () -> {}, collector));
    assertTrue(ex.getMessage().contains("timed out sending command"));
    assertTrue(ex.getMessage().contains("back-pressure"));
  }

  @Test
  void awaitResponsesTimesOutWhenClusterSilent() {
    final long[] nowNs = {0L};
    final NanoClock fakeClock = () -> nowNs[0];
    final ReferenceDataOrchestrator timedOrchestrator = new ReferenceDataOrchestrator(fakeClock);

    final List<String> records = List.of("rec1");
    final ReferenceDataLoader<String> loader = stubLoader(records, "silent.yaml");
    final ReferenceDataEncoder<String> encoder = stubEncoder(10);
    final ResponseCollector collector = new ResponseCollector();

    // Sender succeeds, but egress never delivers acks — clock advances past deadline
    final ClusterCommandSender sender = (buf, off, len) -> 1L;
    final Runnable pollEgress = () -> nowNs[0] += ACK_TIMEOUT_NS + 1;

    final ReferenceDataLoadException ex =
        assertThrows(
            ReferenceDataLoadException.class,
            () -> timedOrchestrator.load(loader, encoder, sender, pollEgress, collector));
    assertTrue(ex.getMessage().contains("timed out waiting for cluster acks"));
  }

  private static <T> ReferenceDataLoader<T> stubLoader(final List<T> records, final String name) {
    return new ReferenceDataLoader<>() {
      @Override
      public List<T> load() {
        return records;
      }

      @Override
      public String sourceName() {
        return name;
      }
    };
  }

  private static <T> ReferenceDataEncoder<T> stubEncoder(final int maxBatch) {
    return new ReferenceDataEncoder<>() {
      @Override
      public int encodeBatch(
          final List<T> records,
          final int fromIndex,
          final int toIndex,
          final MutableDirectBuffer buffer,
          final int offset) {
        // Write a minimal header so the send has something
        return 8;
      }

      @Override
      public int templateId() {
        return 12;
      }

      @Override
      public int maxBatchSize() {
        return maxBatch;
      }

      @Override
      public String entityType() {
        return "TestEntity";
      }
    };
  }
}
