package com.trading.engine.cluster.handler;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.trading.engine.cluster.journal.EventJournal;
import com.trading.engine.cluster.sequencer.EventSequencer;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.OrderCreatedEventEncoder;
import com.trading.engine.testsupport.aeron.FakeClientSession;
import com.trading.engine.testsupport.aeron.FakeCluster;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/**
 * Verifies the synchronous-broadcast contract of {@link EventSink#emit}: every registered cluster
 * client session must receive byte-identical copies of each emitted event before {@code emit()}
 * returns, the event journal must record exactly one entry per emission, and sequence numbers must
 * be strictly monotonic across successive emits.
 *
 * <p>Also verifies the partial-deregistration path: after removing a session from the cluster, only
 * the remaining registered session receives subsequent emits.
 *
 * <p><b>Threading:</b> single-threaded — matches the cluster duty-cycle invariant. {@link
 * EventSink#emit} relies on {@link io.aeron.cluster.service.Cluster#forEachClientSession} being a
 * synchronous fold; this test is the regression guard for that assumption (if Aeron ever defers
 * iteration, the assertions below would fail immediately).
 *
 * <p><b>Allocation:</b> not asserting zero-alloc here — {@link FakeClientSession#offer} allocates a
 * {@code byte[]} per call (capture semantics). The zero-alloc path is covered by {@link
 * EventSinkAllocTest}.
 */
final class EventSinkBroadcastTest {

  private static final long CLUSTER_TS = 1_700_000_000_000_000_000L;
  private static final int HDR_LEN = MessageHeaderEncoder.ENCODED_LENGTH;

  /**
   * Encodes a minimal {@link OrderCreatedEventEncoder} into a 512-byte {@link UnsafeBuffer} and
   * returns both the buffer and the total encoded length.
   *
   * @param buf pre-allocated destination buffer (must be at least 512 bytes)
   * @return total encoded length (header + body)
   */
  private static int encodeOrderCreated(final UnsafeBuffer buf) {
    final var hdr = new MessageHeaderEncoder();
    final var enc = new OrderCreatedEventEncoder();
    enc.wrapAndApplyHeader(buf, 0, hdr);
    enc.sequenceNumber(0L); // placeholder — EventSink will overwrite
    enc.timestamp(0L); // placeholder — EventSink will overwrite
    return HDR_LEN + enc.encodedLength();
  }

  // =========================================================================
  // §1 — Two sessions both receive byte-identical frames from a single emit
  // =========================================================================

  /**
   * Two {@link FakeClientSession}s registered via {@link FakeCluster#addClientSession} must each
   * receive exactly one captured frame after a single {@link EventSink#emit}, and those frames must
   * be byte-identical.
   */
  @Test
  void emit_twoRegisteredSessions_bothReceiveByteIdenticalFrames() {
    final var sequencer = new EventSequencer();
    final var journal = new EventJournal(16);
    final var sink = new EventSink(sequencer, journal);
    final var cluster = new FakeCluster(0L);
    sink.setCluster(cluster);

    final var s1 = new FakeClientSession(101L);
    final var s2 = new FakeClientSession(202L);
    cluster.addClientSession(s1);
    cluster.addClientSession(s2);

    final var buf = new UnsafeBuffer(new byte[512]);
    final int totalLen = encodeOrderCreated(buf);

    final long seq = sink.emit(CLUSTER_TS, buf, 0, totalLen);

    assertEquals(1L, seq, "first emit must return sequence number 1");
    assertEquals(1, s1.messages.size(), "s1 must receive exactly one frame");
    assertEquals(1, s2.messages.size(), "s2 must receive exactly one frame");
    assertArrayEquals(
        s1.messages.get(0),
        s2.messages.get(0),
        "both sessions must receive byte-identical frame content");
  }

  // =========================================================================
  // §2 — Journal receives exactly one entry per emit
  // =========================================================================

  /**
   * After one {@link EventSink#emit}, the journal's {@code highestSequence()} must equal 1. After a
   * second emit, it must equal 2. This verifies the journal-side contract is not broken when
   * multiple sessions are wired.
   */
  @Test
  void emit_journalReceivesExactlyOneEntryPerEmit() {
    final var sequencer = new EventSequencer();
    final var journal = new EventJournal(16);
    final var sink = new EventSink(sequencer, journal);
    final var cluster = new FakeCluster(0L);
    sink.setCluster(cluster);

    final var s1 = new FakeClientSession(101L);
    final var s2 = new FakeClientSession(202L);
    cluster.addClientSession(s1);
    cluster.addClientSession(s2);

    final var buf = new UnsafeBuffer(new byte[512]);
    final int totalLen = encodeOrderCreated(buf);

    sink.emit(CLUSTER_TS, buf, 0, totalLen);
    assertEquals(1L, journal.highestSequence(), "journal must record sequence 1 after first emit");

    sink.emit(CLUSTER_TS + 1L, buf, 0, totalLen);
    assertEquals(2L, journal.highestSequence(), "journal must record sequence 2 after second emit");
  }

  // =========================================================================
  // §3 — Sequence numbers are strictly monotonic across successive emits
  // =========================================================================

  /**
   * Successive {@link EventSink#emit} calls must return strictly increasing sequence numbers (1, 2,
   * 3, …). The sequence number embedded in the buffer at body offset 0 must match the returned
   * value.
   */
  @Test
  void emit_sequenceNumberStrictlyMonotonicAcrossEmits() {
    final var sequencer = new EventSequencer();
    final var journal = new EventJournal(16);
    final var sink = new EventSink(sequencer, journal);
    final var cluster = new FakeCluster(0L);
    sink.setCluster(cluster);

    final var s1 = new FakeClientSession(101L);
    cluster.addClientSession(s1);

    final var buf = new UnsafeBuffer(new byte[512]);
    final int totalLen = encodeOrderCreated(buf);

    final long seq1 = sink.emit(CLUSTER_TS, buf, 0, totalLen);
    final long seq2 = sink.emit(CLUSTER_TS + 1L, buf, 0, totalLen);
    final long seq3 = sink.emit(CLUSTER_TS + 2L, buf, 0, totalLen);

    assertEquals(1L, seq1);
    assertEquals(2L, seq2);
    assertEquals(3L, seq3);
    assertEquals(
        2,
        s1.messages.size() - 1,
        "s1 should have received 3 frames total (2+1 = index 2 is last)");
    assertEquals(3, s1.messages.size());
  }

  // =========================================================================
  // §4 — Deregistered session no longer receives emits
  // =========================================================================

  /**
   * After removing a session from the cluster, subsequent emits must only reach the remaining
   * registered session. This exercises the partial-deregistration path by building a second {@link
   * FakeCluster} with only one of the original two sessions and re-wiring {@code sink}.
   *
   * <p>This simulates the production scenario where a client disconnects mid-session and the
   * cluster framework removes it from the active set before the next duty-cycle iteration.
   */
  @Test
  void emit_afterSessionDeregistered_onlyRemainingSessionReceives() {
    final var sequencer = new EventSequencer();
    final var journal = new EventJournal(16);
    final var sink = new EventSink(sequencer, journal);

    // Initial cluster: both sessions registered.
    final var clusterBoth = new FakeCluster(0L);
    final var s1 = new FakeClientSession(101L);
    final var s2 = new FakeClientSession(202L);
    clusterBoth.addClientSession(s1);
    clusterBoth.addClientSession(s2);
    sink.setCluster(clusterBoth);

    final var buf = new UnsafeBuffer(new byte[512]);
    final int totalLen = encodeOrderCreated(buf);

    // First emit — both sessions receive.
    sink.emit(CLUSTER_TS, buf, 0, totalLen);
    assertEquals(1, s1.messages.size(), "s1 must receive emit 1");
    assertEquals(1, s2.messages.size(), "s2 must receive emit 1");

    // Simulate s2 disconnection: re-wire sink to a cluster with only s1.
    final var clusterS1Only = new FakeCluster(0L);
    clusterS1Only.addClientSession(s1);
    sink.setCluster(clusterS1Only);

    // Second emit — only s1 should receive.
    sink.emit(CLUSTER_TS + 1L, buf, 0, totalLen);
    assertEquals(2, s1.messages.size(), "s1 must receive emit 2 after s2 deregistered");
    assertEquals(1, s2.messages.size(), "s2 must NOT receive emit 2 after deregistration");
  }
}
