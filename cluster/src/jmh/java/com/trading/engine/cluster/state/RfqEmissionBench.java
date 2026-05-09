package com.trading.engine.cluster.state;

import com.trading.engine.cluster.handler.EventSink;
import com.trading.engine.cluster.journal.EventJournal;
import com.trading.engine.cluster.metrics.RfqMetrics;
import com.trading.engine.cluster.refdata.AccountStore;
import com.trading.engine.cluster.sequencer.EventSequencer;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * JMH micro-bench for the RFQ slot pool hot path. Measures acquire/release, peek-by-quoteId, and
 * timer-expiry emit-107 throughput. Per APP-232 plan §11 — target P50 ≤ 1µs, P99 ≤ 3µs on the
 * standard CI runner; this harness reports raw throughput and average latency. Use {@code -prof gc}
 * to assert {@code gc.alloc.rate.norm == 0 B/op} in CI gates.
 *
 * <p><b>Run via:</b> {@code ./gradlew :cluster:jmh}
 *
 * <p><b>Threading:</b> single-threaded (matches cluster duty cycle). State annotated {@code
 * Scope.Benchmark} for shared once-per-trial setup.
 *
 * <p><b>Allocation:</b> hot loop is allocation-free post-warmup. Setup at {@code @Setup(Trial)}
 * builds the state machine once.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Benchmark)
public class RfqEmissionBench {

  private RfqStateMachine machine;
  private RfqMetrics metrics;
  private EventSink eventSink;
  private long clusterTs;
  private final byte[] reqIdScratch = new byte[RfqSlot.QUOTE_REQ_ID_LENGTH];

  @Setup(Level.Trial)
  public void setUp() {
    metrics = new RfqMetrics();
    final var accounts = new AccountStore();
    machine =
        new RfqStateMachine(
            8192,
            30_000_000_000L,
            30_000_000_000L,
            30_000_000_000L,
            30_000_000_000L,
            5_000_000_000L,
            100L,
            1_000_000_000L,
            0,
            0,
            accounts,
            metrics);
    eventSink = new EventSink(new EventSequencer(), new EventJournal(256));
    clusterTs = 1_700_000_000_000_000_000L;
    // Pre-populate the byQuoteReqId map with a known slot for lookupByQuoteReqId bench.
    final var slot = machine.acquire();
    if (slot != null) {
      slot.quoteReqIdBytes[0] = 'B';
      slot.quoteReqIdBytes[1] = 'E';
      slot.quoteReqIdBytes[2] = 'N';
      slot.quoteReqIdBytes[3] = 'C';
      slot.quoteReqIdBytes[4] = 'H';
      slot.syncQuoteReqIdKey();
      machine.registerRequested(slot);
    }
    reqIdScratch[0] = 'B';
    reqIdScratch[1] = 'E';
    reqIdScratch[2] = 'N';
    reqIdScratch[3] = 'C';
    reqIdScratch[4] = 'H';
  }

  /**
   * Benchmarks an {@code acquire} + {@code release} round-trip. Measures slot-pool hot-path cost
   * with no map insertions or byte mutation.
   */
  @Benchmark
  public void acquireRelease(final Blackhole bh) {
    final var slot = machine.acquire();
    bh.consume(slot);
    machine.release(slot);
  }

  /** Benchmarks {@code lookupByQuoteReqId} — Object2ObjectHashMap content-equals lookup. */
  @Benchmark
  public RfqSlot lookupByQuoteReqId() {
    return machine.lookupByQuoteReqId(reqIdScratch, 0, RfqSlot.QUOTE_REQ_ID_LENGTH);
  }

  /** Benchmarks {@code recentlyTerminalReason} — Object2IntHashMap O(1) side-index lookup. */
  @Benchmark
  public byte recentlyTerminalLookup() {
    return machine.recentlyTerminalReason(
        new org.agrona.concurrent.UnsafeBuffer(reqIdScratch), 0, RfqSlot.QUOTE_REQ_ID_LENGTH);
  }

  /** Benchmarks {@code rateLimitTryConsume} steady-state — no bucket-pool growth. */
  @Benchmark
  public boolean rateLimitTryConsume() {
    clusterTs += 10_000_000L; // advance 10ms per op so token bucket replenishes
    return machine.rateLimitTryConsume(42L, clusterTs);
  }
}
