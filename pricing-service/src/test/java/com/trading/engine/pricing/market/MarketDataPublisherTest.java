package com.trading.engine.pricing.market;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.MarketDataHeartbeatDecoder;
import com.trading.engine.messages.sbe.MarketDataTickDecoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.testsupport.clock.ControllableNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/**
 * Core-behaviour unit tests for {@link MarketDataPublisher}.
 *
 * <p><b>Purpose.</b> Verifies the five primary publisher contracts in isolation:
 *
 * <ol>
 *   <li>Conflation: 1 000 rapid {@code onTick} calls for one symbol before any drain → exactly one
 *       wire publish on the first drain; {@code symbolSeq} increments by 1.
 *   <li>Multi-symbol: N=4 symbols one tick each → exactly N wire publishes per drain.
 *   <li>Sanity rejects: crossed ({@code bid >= ask}) and non-positive ({@code bid <= 0 || ask <=
 *       0}) ticks → no publish + correct {@link RejectReason} counter increments.
 *   <li>Heartbeat: controllable {@link ControllableNanoClock} advanced past heartbeat interval →
 *       exactly one heartbeat publish on the subsequent drain.
 *   <li>Snapshot: {@link MarketDataPublisher#snapshotForSymbol} re-emits with {@code symbolSeq=0}
 *       on the wire; next live tick resumes from prior live seq+1.
 * </ol>
 *
 * <p><b>Threading model.</b> All publisher entry points ({@code onStart}, {@code onTick}, {@code
 * doWork}, {@code snapshotForSymbol}) are called from the JUnit test thread. The publisher's
 * single-writer guard records the agent thread on {@code onStart}; tests call {@code onStart} first
 * so the guard binds to the test thread.
 *
 * <p><b>Allocation.</b> Not asserting zero-alloc here — covered by {@link
 * MarketDataPublisherAllocTest}. {@link FakeBroadcastPublisher} captures via {@code byte[]} copies,
 * which is acceptable for correctness-only tests.
 *
 * <p><b>Dependencies.</b> {@link ControllableNanoClock} (test-support), {@link
 * MarketDataTickDecoder}, {@link MarketDataHeartbeatDecoder} (generated SBE codecs), {@link
 * FakeBroadcastPublisher}.
 */
final class MarketDataPublisherTest {

  // ─── SBE message-header length ────────────────────────────────────────────
  private static final int HDR_LEN = MessageHeaderDecoder.ENCODED_LENGTH;

  // ─── Fixed-point 10^-8 test prices ───────────────────────────────────────
  private static final long BID = 118_500_000_000L; // 1.185 × 10^8 (EURUSD region)
  private static final long ASK = 118_510_000_000L; // 1.18510 × 10^8

  private static final long BID_SIZE = 1_000_000L * 100_000_000L; // 1M units FP
  private static final long ASK_SIZE = 1_000_000L * 100_000_000L;
  private static final long INGRESS_NANOS = 1_700_000_000_000_000_000L;

  /**
   * Cadence set to 5 ms (production default) — tests control the clock so drain is gated by {@link
   * ControllableNanoClock} advancing past {@code cadenceNanos}.
   */
  private static final long CADENCE_MICROS = 5_000L;

  /**
   * Heartbeat base set to 1 000 ms (production default). Tests advance the clock by 1 100 ms to
   * fire the heartbeat without being inside the ±10% jitter band.
   */
  private static final long HEARTBEAT_BASE_MS = 1_000L;

  // ─── Packed symbol helpers ────────────────────────────────────────────────

  /** Pack an ASCII symbol into a {@code long} (little-endian, space-padded to 8 bytes). */
  private static long pack(final String symbol) {
    long packed = 0L;
    for (int i = 0; i < 8; i++) {
      final long b = i < symbol.length() ? (byte) symbol.charAt(i) : (byte) ' ';
      packed |= (b & 0xFFL) << (i * 8);
    }
    return packed;
  }

  // ─── Test fixtures ────────────────────────────────────────────────────────

  /**
   * Constructs a {@link MarketDataPublisher} backed by the given {@link FakeBroadcastPublisher} and
   * clock. The snapshot-request subscription is {@code null} — snapshot-subscription polling is not
   * exercised in this test class.
   *
   * @param fake the capturing fake publication.
   * @param clock the controllable clock, used for both epoch and monotonic time.
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
  // §1 — conflation: 1 000 rapid onTick → exactly one publish per drain
  // =========================================================================

  /**
   * 1 000 rapid {@code onTick} calls for the same symbol before any drain must produce exactly one
   * wire publish on the first drain — the latest top-of-book. {@code symbolSeq} on the wire must
   * equal 1 (first drain after conflation).
   */
  @Test
  void onTick_conflation_1000TicksOneSymbol_exactlyOnePublishPerDrain() {
    final var fake = new FakeBroadcastPublisher();
    final var clock = new ControllableNanoClock(1_000_000_000L);
    final var publisher = buildPublisher(fake, clock);
    publisher.onStart();

    final long packed = pack("EURUSD  ");
    for (int i = 0; i < 1_000; i++) {
      publisher.onTick(packed, BID + i, ASK + i, BID_SIZE, ASK_SIZE, INGRESS_NANOS + i);
    }

    // Before drain — no publishes yet.
    assertEquals(0, fake.offerCount(), "no publishes before drain");

    // Advance past the 5 ms drain cadence and call doWork.
    clock.advanceMillis(10L);
    publisher.doWork();

    assertEquals(
        1, fake.offerCount(), "exactly one publish per drain even after 1 000 conflated ticks");

    // Decode and verify symbolSeq = 1.
    final var captured = fake.capturedBytes(0);
    final var buf = new UnsafeBuffer(captured);
    final var hdrDecoder = new MessageHeaderDecoder();
    final var tickDecoder = new MarketDataTickDecoder();
    tickDecoder.wrapAndApplyHeader(buf, 0, hdrDecoder);

    assertEquals(1L, tickDecoder.symbolSeq(), "symbolSeq must be 1 after first drain");
  }

  // =========================================================================
  // §2 — multi-symbol: 4 symbols one tick each → 4 publishes
  // =========================================================================

  /**
   * N=4 symbols ({@code EURUSD}, {@code GBPUSD}, {@code USDJPY}, {@code AUDUSD}), each with one
   * tick, must produce exactly 4 wire publishes on the first drain.
   */
  @Test
  void onTick_multiSymbol_4symbols_4publishesPerDrain() {
    final var fake = new FakeBroadcastPublisher();
    final var clock = new ControllableNanoClock(1_000_000_000L);
    final var publisher = buildPublisher(fake, clock);
    publisher.onStart();

    final long[] symbols = {pack("EURUSD  "), pack("GBPUSD  "), pack("USDJPY  "), pack("AUDUSD  ")};
    for (final long sym : symbols) {
      publisher.onTick(sym, BID, ASK, BID_SIZE, ASK_SIZE, INGRESS_NANOS);
    }

    clock.advanceMillis(10L);
    publisher.doWork();

    assertEquals(4, fake.offerCount(), "exactly 4 publishes for 4 distinct symbols on first drain");
  }

  // =========================================================================
  // §3 — sanity rejects: crossed and non-positive
  // =========================================================================

  /**
   * A tick with {@code bid >= ask} (crossed market) must not produce a wire publish and must
   * increment {@link RejectReason#CROSSED} drop counter.
   */
  @Test
  void onTick_crossedBidAsk_noPublish_crossedCounterIncrements() {
    final var fake = new FakeBroadcastPublisher();
    final var clock = new ControllableNanoClock(1_000_000_000L);
    final var publisher = buildPublisher(fake, clock);
    publisher.onStart();

    final long packed = pack("EURUSD  ");
    // bid == ask → crossed
    publisher.onTick(packed, ASK, ASK, BID_SIZE, ASK_SIZE, INGRESS_NANOS);
    // bid > ask → also crossed
    publisher.onTick(packed, ASK + 1L, ASK, BID_SIZE, ASK_SIZE, INGRESS_NANOS);

    clock.advanceMillis(10L);
    publisher.doWork();

    assertEquals(0, fake.offerCount(), "no publish for crossed ticks");
    assertEquals(2L, publisher.droppedCount(RejectReason.CROSSED), "CROSSED counter must be 2");
  }

  /**
   * A tick with {@code bid <= 0} or {@code ask <= 0} must not produce a wire publish and must
   * increment {@link RejectReason#NON_POSITIVE} drop counter.
   */
  @Test
  void onTick_nonPositivePrice_noPublish_nonPositiveCounterIncrements() {
    final var fake = new FakeBroadcastPublisher();
    final var clock = new ControllableNanoClock(1_000_000_000L);
    final var publisher = buildPublisher(fake, clock);
    publisher.onStart();

    final long packed = pack("EURUSD  ");
    publisher.onTick(packed, 0L, ASK, BID_SIZE, ASK_SIZE, INGRESS_NANOS); // bid = 0
    publisher.onTick(packed, -1L, ASK, BID_SIZE, ASK_SIZE, INGRESS_NANOS); // bid negative
    publisher.onTick(packed, BID, 0L, BID_SIZE, ASK_SIZE, INGRESS_NANOS); // ask = 0
    publisher.onTick(packed, BID, -1L, BID_SIZE, ASK_SIZE, INGRESS_NANOS); // ask negative

    clock.advanceMillis(10L);
    publisher.doWork();

    assertEquals(0, fake.offerCount(), "no publish for non-positive ticks");
    assertEquals(
        4L, publisher.droppedCount(RejectReason.NON_POSITIVE), "NON_POSITIVE counter must be 4");
  }

  // =========================================================================
  // §4 — heartbeat: no ticks for > heartbeat interval → heartbeat publish
  // =========================================================================

  /**
   * When no ticks arrive for longer than the heartbeat interval (1 100 ms &gt; 1 000 ms base × any
   * ±10% jitter outcome), {@code doWork} must emit exactly one heartbeat publish. The wire message
   * must decode as template 55 ({@link MarketDataHeartbeatDecoder#TEMPLATE_ID}).
   */
  @Test
  void doWork_noTicksForHeartbeatInterval_exactlyOneHeartbeatPublish() {
    final var fake = new FakeBroadcastPublisher();
    final var clock = new ControllableNanoClock(1_000_000_000L);
    final var publisher = buildPublisher(fake, clock);
    publisher.onStart();

    // Advance past drain cadence without any ticks → touchedSinceLastDrain remains 0.
    // Advance by 1 100 ms to exceed any ±10% heartbeat interval (base 1 000 ms, max 1 100 ms).
    clock.advanceMillis(10L); // past cadence; no ticks → lastDrainNanos updated
    publisher.doWork();
    assertEquals(0, fake.offerCount(), "no publish for idle drain with no ticks");

    // Now advance another 1 100 ms past the heartbeat interval.
    clock.advanceMillis(1_100L);
    publisher.doWork();

    assertEquals(1, fake.offerCount(), "exactly one heartbeat publish");

    // Decode and verify template ID = 55.
    final var captured = fake.capturedBytes(0);
    final var buf = new UnsafeBuffer(captured);
    final var hdrDecoder = new MessageHeaderDecoder();
    hdrDecoder.wrap(buf, 0);
    assertEquals(
        MarketDataHeartbeatDecoder.TEMPLATE_ID,
        hdrDecoder.templateId(),
        "heartbeat message must carry template ID 55");
  }

  // =========================================================================
  // §5 — snapshot: snapshotForSymbol emits symbolSeq=0; next live tick resumes from prior+1
  // =========================================================================

  /**
   * After a normal tick drains (symbolSeq=1), calling {@link MarketDataPublisher#snapshotForSymbol}
   * must emit a wire publish with {@code symbolSeq=0} (the snapshot sentinel). The slot's live
   * {@code symbolSeq} must be restored so the next normal drain publishes {@code symbolSeq=2}
   * (prior live + 1).
   */
  @Test
  void snapshotForSymbol_afterNormalTick_emitsSeq0_nextLiveTickResumesFromPriorPlusOne() {
    final var fake = new FakeBroadcastPublisher();
    final var clock = new ControllableNanoClock(1_000_000_000L);
    final var publisher = buildPublisher(fake, clock);
    publisher.onStart();

    final long packed = pack("EURUSD  ");

    // First drain → symbolSeq=1 on the wire.
    publisher.onTick(packed, BID, ASK, BID_SIZE, ASK_SIZE, INGRESS_NANOS);
    clock.advanceMillis(10L);
    publisher.doWork();
    assertEquals(1, fake.offerCount(), "first drain must produce 1 publish");

    // Snapshot → symbolSeq=0 on the wire.
    final boolean snapped = publisher.snapshotForSymbol(packed);
    assertTrue(snapped, "snapshotForSymbol must return true for a known symbol");
    assertEquals(2, fake.offerCount(), "snapshot must add exactly 1 more offer");

    final var snapBuf = new UnsafeBuffer(fake.capturedBytes(1));
    final var hdrDecoder = new MessageHeaderDecoder();
    final var tickDecoder = new MarketDataTickDecoder();
    tickDecoder.wrapAndApplyHeader(snapBuf, 0, hdrDecoder);
    assertEquals(0L, tickDecoder.symbolSeq(), "snapshot publish must carry symbolSeq=0");

    // Second drain → symbolSeq must be 2 (prior live seq 1 + 1).
    publisher.onTick(packed, BID + 1L, ASK + 1L, BID_SIZE, ASK_SIZE, INGRESS_NANOS + 1L);
    clock.advanceMillis(10L);
    publisher.doWork();
    assertEquals(3, fake.offerCount(), "second drain must add 1 more publish");

    final var liveBuf = new UnsafeBuffer(fake.capturedBytes(2));
    final var hdr2 = new MessageHeaderDecoder();
    final var tick2 = new MarketDataTickDecoder();
    tick2.wrapAndApplyHeader(liveBuf, 0, hdr2);
    assertEquals(2L, tick2.symbolSeq(), "second live drain must carry symbolSeq=2");
  }

  // =========================================================================
  // §6 — onTick counter is total (including rejects)
  // =========================================================================

  /** {@link MarketDataPublisher#onTickCount()} must count ALL invocations, including rejects. */
  @Test
  void onTickCount_includesRejectedTicks() {
    final var fake = new FakeBroadcastPublisher();
    final var clock = new ControllableNanoClock(1_000_000_000L);
    final var publisher = buildPublisher(fake, clock);
    publisher.onStart();

    final long packed = pack("EURUSD  ");
    publisher.onTick(packed, BID, ASK, BID_SIZE, ASK_SIZE, INGRESS_NANOS); // valid
    publisher.onTick(packed, ASK, ASK, BID_SIZE, ASK_SIZE, INGRESS_NANOS); // crossed
    publisher.onTick(packed, 0L, ASK, BID_SIZE, ASK_SIZE, INGRESS_NANOS); // non-positive

    assertEquals(3L, publisher.onTickCount(), "onTickCount must include all 3 calls");
  }
}
