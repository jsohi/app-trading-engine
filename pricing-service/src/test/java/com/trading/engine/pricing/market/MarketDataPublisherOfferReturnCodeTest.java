package com.trading.engine.pricing.market;

import static io.aeron.Publication.ADMIN_ACTION;
import static io.aeron.Publication.BACK_PRESSURED;
import static io.aeron.Publication.CLOSED;
import static io.aeron.Publication.MAX_POSITION_EXCEEDED;
import static io.aeron.Publication.NOT_CONNECTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.trading.engine.testsupport.clock.ControllableNanoClock;
import org.junit.jupiter.api.Test;

/**
 * Aeron offer-return-code handling tests for {@link MarketDataPublisher}.
 *
 * <p><b>Purpose.</b> Verifies the five-case offer-return-code contract specified in the {@link
 * MarketDataPublisher} class-level Javadoc:
 *
 * <ul>
 *   <li>{@code BACK_PRESSURED} → retry once; if retry also fails, drop + {@link
 *       RejectReason#BACK_PRESSURED} counter increments.
 *   <li>{@code NOT_CONNECTED} → drop + {@link RejectReason#NOT_CONNECTED} counter increments.
 *   <li>{@code ADMIN_ACTION} → drop + {@link RejectReason#ADMIN_ACTION} counter increments.
 *   <li>{@code MAX_POSITION_EXCEEDED} → drop + {@link RejectReason#MAX_POSITION_EXCEEDED} counter
 *       increments.
 *   <li>{@code CLOSED} → {@link IllegalStateException} thrown by {@code doWork}.
 * </ul>
 *
 * <p>Each case uses a {@link FakeBroadcastPublisher} configured to return the target error code on
 * all {@code offer} calls so the publisher's error-handling branch is exercised deterministically.
 *
 * <p><b>Threading model.</b> Single-threaded — all calls run on the JUnit test thread after {@code
 * onStart} binds the agent-thread guard.
 *
 * <p><b>Allocation.</b> Not asserting zero-alloc — covered by {@link MarketDataPublisherAllocTest}.
 *
 * <p><b>Dependencies.</b> {@link ControllableNanoClock}, {@link RejectReason}, {@link
 * io.aeron.Publication} constants, {@link FakeBroadcastPublisher}.
 */
final class MarketDataPublisherOfferReturnCodeTest {

  // ─── Constants ────────────────────────────────────────────────────────────
  private static final long BID = 118_500_000_000L;
  private static final long ASK = 118_510_000_000L;
  private static final long SIZE = 1_000_000L * 100_000_000L;
  private static final long INGRESS = 1_700_000_000_000_000_000L;
  private static final long CADENCE_MICROS = 5_000L;
  private static final long HEARTBEAT_BASE_MS = 1_000L;

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
   * a fresh {@link ControllableNanoClock}. The clock is returned via a two-element array so the
   * caller can advance time after construction.
   *
   * @param fake the fake publication with a pre-configured return code.
   * @param clock the controllable clock to inject.
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
  // §1 — BACK_PRESSURED → retry once; on second failure drop + counter
  // =========================================================================

  /**
   * When the publication always returns {@code BACK_PRESSURED}, the publisher must retry once (two
   * total offer calls per slot), drop, and increment {@link RejectReason#BACK_PRESSURED}.
   */
  @Test
  void doWork_backPressured_retryOnce_dropOnSecondFailure() {
    final var fake = new FakeBroadcastPublisher();
    fake.setNextResult(BACK_PRESSURED);
    final var clock = new ControllableNanoClock(1_000_000_000L);
    final var publisher = buildPublisher(fake, clock);
    publisher.onStart();

    final long packed = pack("EURUSD  ");
    publisher.onTick(packed, BID, ASK, SIZE, SIZE, INGRESS);

    clock.advanceMillis(10L);
    publisher.doWork();

    // Back-pressure retry budget = 1 → 2 offer calls per slot.
    assertEquals(2, fake.offerCount(), "must retry once (2 offer calls total)");
    assertEquals(
        1L,
        publisher.droppedCount(RejectReason.BACK_PRESSURED),
        "BACK_PRESSURED counter must be 1");
    assertEquals(0L, publisher.ticksPublished(), "no successful publications");
  }

  // =========================================================================
  // §2 — NOT_CONNECTED → drop + counter
  // =========================================================================

  /**
   * When the publication returns {@code NOT_CONNECTED}, the publisher must drop the tick and
   * increment {@link RejectReason#NOT_CONNECTED}.
   */
  @Test
  void doWork_notConnected_drop_notConnectedCounterIncrements() {
    final var fake = new FakeBroadcastPublisher();
    fake.setNextResult(NOT_CONNECTED);
    final var clock = new ControllableNanoClock(1_000_000_000L);
    final var publisher = buildPublisher(fake, clock);
    publisher.onStart();

    publisher.onTick(pack("EURUSD  "), BID, ASK, SIZE, SIZE, INGRESS);
    clock.advanceMillis(10L);
    publisher.doWork();

    assertEquals(
        1L, publisher.droppedCount(RejectReason.NOT_CONNECTED), "NOT_CONNECTED counter must be 1");
    assertEquals(0L, publisher.ticksPublished(), "no successful publications");
  }

  // =========================================================================
  // §3 — ADMIN_ACTION → drop + counter
  // =========================================================================

  /**
   * When the publication returns {@code ADMIN_ACTION}, the publisher must drop and increment {@link
   * RejectReason#ADMIN_ACTION}.
   */
  @Test
  void doWork_adminAction_drop_adminActionCounterIncrements() {
    final var fake = new FakeBroadcastPublisher();
    fake.setNextResult(ADMIN_ACTION);
    final var clock = new ControllableNanoClock(1_000_000_000L);
    final var publisher = buildPublisher(fake, clock);
    publisher.onStart();

    publisher.onTick(pack("EURUSD  "), BID, ASK, SIZE, SIZE, INGRESS);
    clock.advanceMillis(10L);
    publisher.doWork();

    assertEquals(
        1L, publisher.droppedCount(RejectReason.ADMIN_ACTION), "ADMIN_ACTION counter must be 1");
    assertEquals(0L, publisher.ticksPublished(), "no successful publications");
  }

  // =========================================================================
  // §4 — MAX_POSITION_EXCEEDED → drop + counter
  // =========================================================================

  /**
   * When the publication returns {@code MAX_POSITION_EXCEEDED}, the publisher must drop and
   * increment {@link RejectReason#MAX_POSITION_EXCEEDED}.
   */
  @Test
  void doWork_maxPositionExceeded_drop_maxPositionExceededCounterIncrements() {
    final var fake = new FakeBroadcastPublisher();
    fake.setNextResult(MAX_POSITION_EXCEEDED);
    final var clock = new ControllableNanoClock(1_000_000_000L);
    final var publisher = buildPublisher(fake, clock);
    publisher.onStart();

    publisher.onTick(pack("EURUSD  "), BID, ASK, SIZE, SIZE, INGRESS);
    clock.advanceMillis(10L);
    publisher.doWork();

    assertEquals(
        1L,
        publisher.droppedCount(RejectReason.MAX_POSITION_EXCEEDED),
        "MAX_POSITION_EXCEEDED counter must be 1");
    assertEquals(0L, publisher.ticksPublished(), "no successful publications");
  }

  // =========================================================================
  // §5 — CLOSED → IllegalStateException from doWork (fatal)
  // =========================================================================

  /**
   * When the publication returns {@code CLOSED}, {@code doWork} must throw {@link
   * IllegalStateException} (fatal — agent must terminate). The exception must propagate to the
   * caller without being swallowed.
   */
  @Test
  void doWork_closed_throwsIllegalStateException() {
    final var fake = new FakeBroadcastPublisher();
    fake.setNextResult(CLOSED);
    final var clock = new ControllableNanoClock(1_000_000_000L);
    final var publisher = buildPublisher(fake, clock);
    publisher.onStart();

    publisher.onTick(pack("EURUSD  "), BID, ASK, SIZE, SIZE, INGRESS);
    clock.advanceMillis(10L);

    assertThrows(
        IllegalStateException.class,
        publisher::doWork,
        "CLOSED publication must throw IllegalStateException from doWork");
  }
}
