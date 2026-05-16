package com.trading.engine.pricing.market;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.MarketDataTickDecoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.testsupport.clock.ControllableNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/**
 * Snapshot-burst tests for {@link MarketDataPublisher#snapshotForSymbol}.
 *
 * <p><b>Purpose.</b> Verifies that 1 000 successive calls to {@link
 * MarketDataPublisher#snapshotForSymbol} for the same symbol each produce exactly one wire publish
 * with {@code symbolSeq=0} (the snapshot sentinel). No burst should be dropped, skipped, or
 * coalesced — each snapshot request is a distinct demand for a gap-reset tick from a browser client
 * performing reconnect recovery.
 *
 * <p>The test also verifies that the publisher's live {@code symbolSeq} is unchanged after all 1
 * 000 snapshots, confirming the stash-restore pattern in {@link
 * MarketDataPublisher#snapshotForSymbol} is idempotent across repeated calls.
 *
 * <p><b>Threading model.</b> Single-threaded — all calls run on the JUnit test thread after {@code
 * onStart} binds the agent-thread guard. The snapshot path is synchronous (no background thread
 * involved).
 *
 * <p><b>Allocation.</b> Not asserting zero-alloc — the burst test exercises the snapshot API
 * correctness, not the allocation budget. {@link FakeBroadcastPublisher} allocates {@code byte[]}
 * per offer (acceptable for test-only usage).
 *
 * <p><b>Dependencies.</b> {@link ControllableNanoClock}, {@link MarketDataTickDecoder}, {@link
 * FakeBroadcastPublisher}.
 */
final class MarketDataSnapshotBurstTest {

  // ─── Constants ────────────────────────────────────────────────────────────
  private static final int BURST_SIZE = 1_000;
  private static final long BID = 118_500_000_000L;
  private static final long ASK = 118_510_000_000L;
  private static final long SIZE = 1_000_000L * 100_000_000L;
  private static final long INGRESS = 1_700_000_000_000_000_000L;
  private static final long CADENCE_MICROS = 5_000L;
  private static final long HEARTBEAT_BASE_MS = 1_000L;

  // ─── Helpers ─────────────────────────────────────────────────────────────

  private static long pack(final String s) {
    long packed = 0L;
    for (int i = 0; i < 8; i++) {
      final long b = i < s.length() ? (byte) s.charAt(i) : (byte) ' ';
      packed |= (b & 0xFFL) << (i * 8);
    }
    return packed;
  }

  /**
   * Constructs a {@link MarketDataPublisher} backed by the given {@link FakeBroadcastPublisher} and
   * clock. The snapshot-request subscription is {@code null} — the burst test exercises the direct
   * {@code snapshotForSymbol} API, not the Aeron-subscription polling path.
   *
   * @param fake the capturing fake publication.
   * @param clock the controllable clock.
   * @return a publisher ready for {@code onStart()}.
   */
  private static MarketDataPublisher buildPublisher(
      final FakeBroadcastPublisher fake, final ControllableNanoClock clock) {
    final var config =
        new MarketDataPublisherConfig(
            MarketDataPublisherConfig.AdapterKind.DETERMINISTIC, CADENCE_MICROS, HEARTBEAT_BASE_MS);
    return new MarketDataPublisher(fake, null, clock, clock, config);
  }

  // =========================================================================
  // §1 — 1 000 snapshotForSymbol calls → 1 000 wire publishes each with symbolSeq=0
  // =========================================================================

  /**
   * 1 000 successive {@link MarketDataPublisher#snapshotForSymbol} calls for the same symbol must
   * each produce exactly one wire publish with {@code symbolSeq=0}. The total offer count must be 1
   * 001 (one initial drain + 1 000 snapshots). After all snapshots the live {@code symbolSeq} as
   * reported by subsequent drains must continue from where it left off before the burst (seq=2 on
   * the next normal drain, since the live seq after the first drain was 1).
   */
  @Test
  void snapshotForSymbol_burst1000SameSymbol_each1WirePublishWithSeq0() {
    final var fake = new FakeBroadcastPublisher();
    final var clock = new ControllableNanoClock(1_000_000_000L);
    final var publisher = buildPublisher(fake, clock);
    publisher.onStart();

    final long packed = pack("EURUSD  ");

    // Establish a live slot with one normal tick + drain so the slot exists and symbolSeq=1.
    publisher.onTick(packed, BID, ASK, SIZE, SIZE, INGRESS);
    clock.advanceMillis(10L);
    publisher.doWork();
    assertEquals(1, fake.offerCount(), "initial drain must produce 1 publish");

    // Burst of 1 000 snapshot calls.
    for (int i = 0; i < BURST_SIZE; i++) {
      final boolean snapped = publisher.snapshotForSymbol(packed);
      assertTrue(
          snapped, "snapshotForSymbol must return true for a known symbol (iteration " + i + ")");
    }

    final int expectedTotal = 1 + BURST_SIZE;
    assertEquals(
        expectedTotal,
        fake.offerCount(),
        "total offer count must be 1 (initial drain) + 1000 (snapshots) = 1001");

    // Verify every snapshot carries symbolSeq=0.
    final var hdrDecoder = new MessageHeaderDecoder();
    final var tickDecoder = new MarketDataTickDecoder();
    for (int i = 1; i <= BURST_SIZE; i++) {
      final var buf = new UnsafeBuffer(fake.capturedBytes(i));
      tickDecoder.wrapAndApplyHeader(buf, 0, hdrDecoder);
      assertEquals(
          0L, tickDecoder.symbolSeq(), "snapshot at index " + i + " must carry symbolSeq=0");
    }

    // After burst: a normal tick + drain must resume from seq=2 (live seq was 1 before burst).
    publisher.onTick(packed, BID + 1L, ASK + 1L, SIZE, SIZE, INGRESS + 1L);
    clock.advanceMillis(10L);
    publisher.doWork();

    final int totalAfterLive = expectedTotal + 1;
    assertEquals(
        totalAfterLive,
        fake.offerCount(),
        "one more publish expected after the post-burst live tick drain");

    final var liveBuf = new UnsafeBuffer(fake.capturedBytes(totalAfterLive - 1));
    final var hdr2 = new MessageHeaderDecoder();
    final var tick2 = new MarketDataTickDecoder();
    tick2.wrapAndApplyHeader(liveBuf, 0, hdr2);
    assertEquals(
        2L,
        tick2.symbolSeq(),
        "post-burst live drain must carry symbolSeq=2 (prior live seq 1 + 1)");
  }
}
