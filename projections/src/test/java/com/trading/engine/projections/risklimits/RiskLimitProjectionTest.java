package com.trading.engine.projections.risklimits;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.RiskLimitChangedEventDecoder;
import com.trading.engine.messages.sbe.RiskLimitChangedEventEncoder;
import com.trading.engine.messages.sbe.RiskLimitLoadedEventDecoder;
import com.trading.engine.messages.sbe.RiskLimitLoadedEventEncoder;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link RiskLimitProjection} — APP-62 §A read-model over per-account risk limits.
 *
 * <p>Covers: upsert on {@code RiskLimitLoadedEvent}, last-write-wins on repeat load, change-event
 * counter increment on {@code RiskLimitChangedEvent}, and replay determinism (reset → re-deliver
 * sequence reproduces the same final state).
 */
class RiskLimitProjectionTest {

  private static final int HDR_LEN = MessageHeaderEncoder.ENCODED_LENGTH;

  private RiskLimitProjection projection;
  private MutableDirectBuffer buf;

  @BeforeEach
  void setUp() {
    projection = new RiskLimitProjection(64);
    buf = new ExpandableArrayBuffer(512);
  }

  // ---------------------------------------------------------------------------
  // Encoding helpers (event-thread compatible, single-buffer reuse)
  // ---------------------------------------------------------------------------

  /**
   * Encode a RiskLimitLoadedEvent with the full APP-62 §4 / §5 / §B knob set.
   *
   * @return total encoded length including SBE header
   */
  private int encodeLoaded(
      final long accountId,
      final long maxOrderSize,
      final long maxOrderNotional,
      final long maxLongPosition,
      final long maxShortPosition,
      final boolean positionLimitEnabled,
      final long priceDeviationBps,
      final boolean fatFingerEnabled,
      final boolean fatFingerFailClosed,
      final long idleSessionTimeoutNanos,
      final long transactTime) {
    final var header = new MessageHeaderEncoder();
    final var enc = new RiskLimitLoadedEventEncoder();
    enc.wrapAndApplyHeader(buf, 0, header);
    enc.sequenceNumber(1L)
        .timestamp(transactTime)
        .accountId(accountId)
        .maxOrderSize(maxOrderSize)
        .maxOrderNotional(maxOrderNotional)
        .maxDailyVolume(0L)
        .maxOrdersPerSecond(0L)
        .maxLongPosition(maxLongPosition)
        .maxShortPosition(maxShortPosition)
        .positionLimitEnabled((short) (positionLimitEnabled ? 1 : 0))
        .priceDeviationBps(priceDeviationBps)
        .fatFingerEnabled((short) (fatFingerEnabled ? 1 : 0))
        .fatFingerFailClosed((short) (fatFingerFailClosed ? 1 : 0))
        .idleSessionTimeoutNanos(idleSessionTimeoutNanos)
        .status(AccountStatusEnum.Active)
        .transactTime(transactTime);
    return HDR_LEN + enc.encodedLength();
  }

  /** Encode a minimal RiskLimitChangedEvent for the counter-bump test. */
  private int encodeChanged(final long accountId) {
    final var header = new MessageHeaderEncoder();
    final var enc = new RiskLimitChangedEventEncoder();
    enc.wrapAndApplyHeader(buf, 0, header);
    enc.sequenceNumber(2L)
        .timestamp(0L)
        .accountId(accountId)
        .putProposerId(new byte[16], 0)
        .putApproverId(new byte[16], 0)
        .oldRecordCount(0);
    return HDR_LEN + enc.encodedLength();
  }

  // ---------------------------------------------------------------------------
  // Tests
  // ---------------------------------------------------------------------------

  @Test
  void riskLimitLoadedEvent_populatesRecord() {
    final int len = encodeLoaded(42L, 100L, 1_000L, 500L, 600L, true, 200L, true, true, 0L, 100L);

    projection.onEvent(1L, RiskLimitLoadedEventDecoder.TEMPLATE_ID, buf, HDR_LEN, len - HDR_LEN);

    final var record = projection.getByAccountId(42L);
    assertNotNull(record);
    assertEquals(42L, record.accountId());
    assertEquals(100L, record.maxOrderSize());
    assertEquals(1_000L, record.maxOrderNotional());
    assertEquals(500L, record.maxLongPosition());
    assertEquals(600L, record.maxShortPosition());
    assertTrue(record.positionLimitEnabled());
    assertEquals(200L, record.priceDeviationBps());
    assertTrue(record.fatFingerEnabled());
    assertTrue(record.fatFingerFailClosed());
    assertEquals(0L, record.idleSessionTimeoutNanos());
    assertEquals(100L, record.transactTime());
    assertEquals(1L, projection.eventsProcessed());
    assertEquals(0L, projection.changedEventCount());
  }

  @Test
  void multipleLoads_latestWins() {
    int len = encodeLoaded(7L, 100L, 0L, 0L, 0L, false, 50L, true, false, 0L, 0L);
    projection.onEvent(1L, RiskLimitLoadedEventDecoder.TEMPLATE_ID, buf, HDR_LEN, len - HDR_LEN);

    // Second load with tighter knobs overwrites the first.
    len = encodeLoaded(7L, 500L, 9_999L, 0L, 0L, false, 75L, true, true, 30_000_000L, 50L);
    projection.onEvent(2L, RiskLimitLoadedEventDecoder.TEMPLATE_ID, buf, HDR_LEN, len - HDR_LEN);

    final var record = projection.getByAccountId(7L);
    assertNotNull(record);
    assertEquals(500L, record.maxOrderSize());
    assertEquals(9_999L, record.maxOrderNotional());
    assertEquals(75L, record.priceDeviationBps());
    assertEquals(30_000_000L, record.idleSessionTimeoutNanos());
    assertTrue(record.fatFingerFailClosed());
    assertEquals(50L, record.transactTime());
    assertEquals(2L, record.sequenceNumber());
    assertEquals(1, projection.size());
  }

  @Test
  void riskLimitChangedEvent_bumpsCounterWithoutMutatingState() {
    int len = encodeLoaded(9L, 100L, 0L, 0L, 0L, false, 50L, true, false, 0L, 0L);
    projection.onEvent(1L, RiskLimitLoadedEventDecoder.TEMPLATE_ID, buf, HDR_LEN, len - HDR_LEN);
    final var before = projection.getByAccountId(9L);
    assertNotNull(before);

    len = encodeChanged(9L);
    projection.onEvent(2L, RiskLimitChangedEventDecoder.TEMPLATE_ID, buf, HDR_LEN, len - HDR_LEN);

    assertEquals(1L, projection.changedEventCount());
    // State unchanged — RiskLimitChangedEvent is an audit-trail-only event for the projection.
    assertEquals(before.maxOrderSize(), projection.getByAccountId(9L).maxOrderSize());
  }

  @Test
  void eventReplay_deterministic() {
    int len1 = encodeLoaded(1L, 100L, 0L, 0L, 0L, false, 50L, true, false, 0L, 0L);
    projection.onEvent(1L, RiskLimitLoadedEventDecoder.TEMPLATE_ID, buf, HDR_LEN, len1 - HDR_LEN);
    int len2 = encodeLoaded(2L, 200L, 0L, 0L, 0L, false, 60L, true, false, 0L, 0L);
    projection.onEvent(2L, RiskLimitLoadedEventDecoder.TEMPLATE_ID, buf, HDR_LEN, len2 - HDR_LEN);
    final var beforeReset1 = projection.getByAccountId(1L);
    final var beforeReset2 = projection.getByAccountId(2L);

    projection.reset();
    assertEquals(0, projection.size());
    assertEquals(0L, projection.eventsProcessed());

    // Replay
    len1 = encodeLoaded(1L, 100L, 0L, 0L, 0L, false, 50L, true, false, 0L, 0L);
    projection.onEvent(1L, RiskLimitLoadedEventDecoder.TEMPLATE_ID, buf, HDR_LEN, len1 - HDR_LEN);
    len2 = encodeLoaded(2L, 200L, 0L, 0L, 0L, false, 60L, true, false, 0L, 0L);
    projection.onEvent(2L, RiskLimitLoadedEventDecoder.TEMPLATE_ID, buf, HDR_LEN, len2 - HDR_LEN);

    final var afterReplay1 = projection.getByAccountId(1L);
    final var afterReplay2 = projection.getByAccountId(2L);
    assertEquals(beforeReset1.maxOrderSize(), afterReplay1.maxOrderSize());
    assertEquals(beforeReset2.maxOrderSize(), afterReplay2.maxOrderSize());
    assertEquals(beforeReset1.priceDeviationBps(), afterReplay1.priceDeviationBps());
  }

  @Test
  void getByAccountId_unknownAccount_returnsNull() {
    assertNull(projection.getByAccountId(999L));
  }

  @Test
  void onEvent_unregisteredTemplate_skipsWithoutBumpingCounters() {
    // Send a template the projection doesn't handle (e.g., 9999) — should be silent skip.
    projection.onEvent(1L, 9999, buf, 0, 8);
    assertEquals(0L, projection.eventsProcessed());
    assertEquals(0L, projection.errorCount());
  }

  @Test
  void reset_clearsStateAndCounters() {
    int len = encodeLoaded(5L, 100L, 0L, 0L, 0L, false, 50L, true, false, 0L, 0L);
    projection.onEvent(1L, RiskLimitLoadedEventDecoder.TEMPLATE_ID, buf, HDR_LEN, len - HDR_LEN);
    assertEquals(1, projection.size());

    projection.reset();
    assertEquals(0, projection.size());
    assertEquals(0L, projection.lastProcessedSequence());
    assertEquals(0L, projection.eventsProcessed());
    assertFalse(projection.getByAccountId(5L) != null);
  }
}
