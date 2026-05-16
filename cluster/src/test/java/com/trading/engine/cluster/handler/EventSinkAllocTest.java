package com.trading.engine.cluster.handler;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.cluster.journal.EventJournal;
import com.trading.engine.cluster.sequencer.EventSequencer;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.OrderCreatedEventEncoder;
import com.trading.engine.testsupport.aeron.FakeCluster;
import io.aeron.DirectBufferVector;
import io.aeron.cluster.service.ClientSession;
import io.aeron.logbuffer.BufferClaim;
import java.lang.management.ManagementFactory;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Zero-allocation regression test for the {@link EventSink#emit} broadcast path.
 *
 * <p>Asserts that {@link EventSink#emit} allocates ≤ 100 bytes/call (≈ zero) after the warmup phase
 * (20 000 emits) by measuring per-thread TLAB bytes via {@link
 * com.sun.management.ThreadMXBean#getThreadAllocatedBytes} before and after 100 000 measurement
 * emits. The 100 bytes/call threshold absorbs TLAB-sample granularity noise while reliably
 * rejecting any real object allocation.
 *
 * <p><b>Measurement technique:</b> {@link com.sun.management.ThreadMXBean#getThreadAllocatedBytes}
 * provides TLAB-sample-based per-thread byte accounting. Two sessions are registered so the {@code
 * forEachClientSession} loop body executes twice per emit; both sessions use {@link
 * NonCapturingSession} which returns {@code 1L} from {@code offer} without allocating.
 *
 * <p><b>Pre-warm discipline:</b>
 *
 * <ol>
 *   <li>Journal capacity = 2^13 = 8 192 (must be power-of-two per {@link EventJournal}). The ring
 *       is chosen SMALLER than the warmup count (20 000) so the journal wraps fully during warmup —
 *       all 8 192 ring slots have their {@link com.trading.engine.cluster.journal.EventEntry}
 *       payload buffers expanded before the measurement loop starts. Measurement emits (100 000)
 *       therefore only overwrite already-expanded slots, producing zero allocation from the journal
 *       path.
 *   <li>20 000 warmup emits prime the JIT, the TLAB, and the journal ring before measurement.
 *   <li>The {@code EventSequencer}'s internal counter has no lazy state.
 * </ol>
 *
 * <p><b>Threading:</b> single-threaded — all emits run on the test thread, matching the cluster
 * duty-cycle invariant.
 *
 * <p><b>Allocation:</b> zero — this test asserts the production {@code EventSink.emit} broadcast
 * path allocates zero bytes per emit at steady state.
 */
final class EventSinkAllocTest {

  // -------------------------------------------------------------------------
  // Non-capturing session — returns 1L from offer(), no allocation
  // -------------------------------------------------------------------------

  /**
   * Minimal {@link ClientSession} test double that returns {@code 1L} (success) from {@link
   * #offer(DirectBuffer, int, int)} without allocating any objects. All other methods throw {@link
   * UnsupportedOperationException} via a static constant message (no {@code new String()} in the
   * hot path).
   *
   * <p>Used exclusively by {@link EventSinkAllocTest} to keep the broadcast loop allocation-free.
   * Tests that need message capture should use {@link
   * com.trading.engine.testsupport.aeron.FakeClientSession} instead.
   */
  private static final class NonCapturingSession implements ClientSession {

    private static final String UNSUPPORTED = "not exercised by EventSink";

    private final long sessionId;

    NonCapturingSession(final long sessionId) {
      this.sessionId = sessionId;
    }

    @Override
    public long id() {
      return sessionId;
    }

    @Override
    public int responseStreamId() {
      return 0;
    }

    @Override
    public String responseChannel() {
      throw new UnsupportedOperationException(UNSUPPORTED);
    }

    @Override
    public byte[] encodedPrincipal() {
      throw new UnsupportedOperationException(UNSUPPORTED);
    }

    @Override
    public void close() {
      throw new UnsupportedOperationException(UNSUPPORTED);
    }

    @Override
    public boolean isClosing() {
      return false;
    }

    /**
     * Returns {@code 1L} to signal success without allocating. EventSink interprets any result
     * {@code >= 0} or {@code == MOCKED_OFFER} as success and breaks the retry loop immediately.
     *
     * @param buffer the broadcast buffer (not read or copied)
     * @param offset ignored
     * @param length ignored
     * @return 1L (success sentinel)
     */
    @Override
    public long offer(final DirectBuffer buffer, final int offset, final int length) {
      return 1L;
    }

    @Override
    public long offer(final DirectBufferVector[] vectors) {
      throw new UnsupportedOperationException(UNSUPPORTED);
    }

    @Override
    public long tryClaim(final int length, final BufferClaim bufferClaim) {
      throw new UnsupportedOperationException(UNSUPPORTED);
    }
  }

  // -------------------------------------------------------------------------
  // Constants
  // -------------------------------------------------------------------------

  /**
   * Warmup iterations. Must be at least twice the ring capacity (8 192) so the journal ring wraps
   * fully and all measurement slots hit already-expanded {@link
   * com.trading.engine.cluster.journal.EventEntry} buffers.
   */
  private static final int WARMUP = 20_000;

  /** Measurement iterations. */
  private static final int MEASURE = 100_000;

  /**
   * Maximum bytes-per-call threshold. Absorbs TLAB-sample noise (~1–2 kB / 100k = ~10–20
   * bytes/call) while rejecting any real allocation (one Java object ≥ 16 bytes → 16 * 100k = 1.6
   * MB total, far above this threshold). Matches the pattern used in {@link
   * com.trading.engine.cluster.state.RfqStateMachineAllocationTest}.
   */
  private static final long MAX_BYTES_PER_CALL = 100L;

  private static final long CLUSTER_TS = 1_700_000_000_000_000_000L;
  private static final int HDR_LEN = MessageHeaderEncoder.ENCODED_LENGTH;

  // -------------------------------------------------------------------------
  // Fixtures
  // -------------------------------------------------------------------------

  private EventSink sink;
  private UnsafeBuffer buf;
  private int totalLen;
  private com.sun.management.ThreadMXBean threadBean;
  private long tid;

  @BeforeEach
  void setUp() {
    final var sequencer = new EventSequencer();
    // Ring capacity = 8 192 (2^13), well below WARMUP (20 000). After the first 8 192 warmup
    // emits the ring is full and all subsequent emits overwrite already-expanded EventEntry
    // payload buffers. The measurement loop (100 000 emits) therefore never encounters a
    // fresh slot whose ExpandableArrayBuffer needs to grow — zero allocation is guaranteed.
    final var journal = new EventJournal(1 << 13);
    sink = new EventSink(sequencer, journal);

    final var cluster = new FakeCluster(0L);
    cluster.addClientSession(new NonCapturingSession(1L));
    cluster.addClientSession(new NonCapturingSession(2L));
    sink.setCluster(cluster);

    // Pre-encode one OrderCreatedEvent into a reusable buffer (one-shot; outside the loop).
    buf = new UnsafeBuffer(new byte[512]);
    final var hdr = new MessageHeaderEncoder();
    final var enc = new OrderCreatedEventEncoder();
    enc.wrapAndApplyHeader(buf, 0, hdr);
    enc.sequenceNumber(0L);
    enc.timestamp(0L);
    totalLen = HDR_LEN + enc.encodedLength();

    threadBean = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
    tid = Thread.currentThread().getId();
  }

  // =========================================================================
  // §1 — emit allocates zero bytes at steady state (two non-capturing sessions)
  // =========================================================================

  /**
   * Asserts that {@link EventSink#emit} allocates at most {@link #MAX_BYTES_PER_CALL} bytes per
   * call at steady state. Two {@link NonCapturingSession}s are registered so the {@code
   * forEachClientSession} loop body executes twice per emit without any allocation.
   *
   * <p>The threshold absorbs TLAB-sample granularity noise (~10–20 bytes/call) while rejecting any
   * real object allocation (one Java object ≥ 16 bytes → 16 * 100k = 1.6 MB, far above the
   * threshold).
   *
   * <p>Warmup: 20 000 emits (twice the ring capacity) — primes JIT, TLAB, and expands all
   * EventEntry payload buffers so measurement sees zero buffer-growth allocation. Measurement: 100
   * 000 emits; bytes-per-call must be ≤ {@link #MAX_BYTES_PER_CALL}.
   */
  @Test
  void emit_zeroAllocationAtSteadyState() {
    // Warmup: prime JIT inline caches and expand all EventEntry payload buffers.
    for (int i = 0; i < WARMUP; i++) {
      sink.emit(CLUSTER_TS + i, buf, 0, totalLen);
    }

    // Measurement.
    final long bytesBefore = threadBean.getThreadAllocatedBytes(tid);
    for (int i = 0; i < MEASURE; i++) {
      sink.emit(CLUSTER_TS + WARMUP + i, buf, 0, totalLen);
    }
    final long bytesAfter = threadBean.getThreadAllocatedBytes(tid);

    final long delta = bytesAfter - bytesBefore;
    final long perCall = delta / MEASURE;
    assertTrue(
        perCall <= MAX_BYTES_PER_CALL,
        "EventSink.emit broadcast path must allocate <= "
            + MAX_BYTES_PER_CALL
            + " bytes/call at steady state; actual "
            + perCall
            + " bytes/call (total delta = "
            + delta
            + " bytes over "
            + MEASURE
            + " iterations)");
  }
}
