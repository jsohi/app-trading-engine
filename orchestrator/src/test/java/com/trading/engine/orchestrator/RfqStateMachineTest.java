package com.trading.engine.orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.PriceResponseDecoder;
import com.trading.engine.messages.sbe.QuoteRequestDecoder;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.testsupport.buffer.SbeFieldUtil;
import com.trading.engine.testsupport.sbe.SbeTestEncoder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Comprehensive unit tests for {@link RfqStateMachine}. Covers constructor validation, happy-path
 * state transitions, exhaustive invalid-transition matrix, lookup, timeout reaping, pool
 * management, map cleanup, and duplicate quoteReqId behavior.
 */
class RfqStateMachineTest {

  /** Small pool size — exhaustion is reachable in 4 iterations. */
  private static final int POOL_SIZE = 4;

  /** Time budget (5s in nanos) before a {@code PENDING_PRICE} RFQ is reaped as expired. */
  private static final long PENDING_PRICE_TIMEOUT = 5_000_000_000L;

  /** Time budget (30s in nanos) before a {@code QUOTED} RFQ is reaped as expired. */
  private static final long QUOTED_TIMEOUT = 30_000_000_000L;

  /** Time budget (5s in nanos) before a {@code PENDING_VALIDATION} RFQ is reaped as expired. */
  private static final long PENDING_VALIDATION_TIMEOUT = 5_000_000_000L;

  /** Arbitrary base epoch-nanos timestamp; non-zero to surface "uninitialised long" bugs. */
  private static final long NOW = 1_000_000_000L;

  private static final String QUOTE_REQ_ID = "QR-000000000001";
  private static final String QUOTE_ID = "QTE-00000000001";
  private static final String SYMBOL = "EURUSD";

  /** QuoteReqID lookup bytes pre-padded to the SBE field length (20, null-padded). */
  private static final byte[] QRID_BYTES =
      SbeFieldUtil.zeroPad(QUOTE_REQ_ID, RfqState.QUOTE_REQ_ID_LENGTH);

  /** QuoteID lookup bytes pre-padded to the SBE field length (20, null-padded). */
  private static final byte[] QID_BYTES = SbeFieldUtil.zeroPad(QUOTE_ID, RfqState.QUOTE_ID_LENGTH);

  private final MutableDirectBuffer buf = new ExpandableArrayBuffer(512);
  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final QuoteRequestDecoder quoteReqDecoder = new QuoteRequestDecoder();
  private final PriceResponseDecoder priceRespDecoder = new PriceResponseDecoder();

  private RfqStateMachine sm;

  @BeforeEach
  void setUp() {
    sm =
        new RfqStateMachine(
            POOL_SIZE, PENDING_PRICE_TIMEOUT, QUOTED_TIMEOUT, PENDING_VALIDATION_TIMEOUT);
  }

  // ===========================================================================
  // Constructor validation
  // ===========================================================================

  @Test
  void constructor_zeroMaxActive_throwsIae() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new RfqStateMachine(
                0, PENDING_PRICE_TIMEOUT, QUOTED_TIMEOUT, PENDING_VALIDATION_TIMEOUT));
  }

  @Test
  void constructor_negativePendingPriceTimeout_throwsIae() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new RfqStateMachine(POOL_SIZE, -1, QUOTED_TIMEOUT, PENDING_VALIDATION_TIMEOUT));
  }

  @Test
  void constructor_negativeQuotedTimeout_throwsIae() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new RfqStateMachine(POOL_SIZE, PENDING_PRICE_TIMEOUT, -1, PENDING_VALIDATION_TIMEOUT));
  }

  @Test
  void constructor_negativePendingValidationTimeout_throwsIae() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new RfqStateMachine(POOL_SIZE, PENDING_PRICE_TIMEOUT, QUOTED_TIMEOUT, -1));
  }

  // ===========================================================================
  // Happy-path state transitions
  // ===========================================================================

  @Test
  void onQuoteRequest_acquiresSlotAndTransitionsToPendingPrice() {
    final var rfq = acquireSlot(QUOTE_REQ_ID);
    assertNotNull(rfq);
    assertEquals(RfqState.State.PENDING_PRICE, rfq.state());
    assertEquals(1, sm.activeCount());
  }

  @Test
  void onQuoteRequest_setsExpiryFromPendingPriceTimeout() {
    final var rfq = acquireSlot(QUOTE_REQ_ID);
    assertEquals(NOW + PENDING_PRICE_TIMEOUT, rfq.expiryNanos());
  }

  @Test
  void onQuoteRequest_poolExhausted_returnsNull() {
    for (int i = 0; i < POOL_SIZE; i++) {
      assertNotNull(acquireSlot("QR-" + String.format("%012d", i)));
    }
    assertNull(acquireSlot("QR-OVERFLOW00000"));
    assertEquals(POOL_SIZE, sm.activeCount());
  }

  @Test
  void onPriceResponseAccepted_transitionsToQuoted() {
    acquireSlot(QUOTE_REQ_ID);
    final var rfq = acceptPrice(QUOTE_REQ_ID, QUOTE_ID);
    assertNotNull(rfq);
    assertEquals(RfqState.State.QUOTED, rfq.state());
  }

  @Test
  void onPriceResponseAccepted_insertsQuoteIdIntoMap() {
    acquireSlot(QUOTE_REQ_ID);
    acceptPrice(QUOTE_REQ_ID, QUOTE_ID);

    final var qid = QID_BYTES;
    assertNotNull(sm.findByQuoteId(qid, 0, qid.length));
  }

  @Test
  void onPriceResponseRejected_transitionsToRejected_releasesSlot() {
    acquireSlot(QUOTE_REQ_ID);
    final var qrid = QRID_BYTES;

    sm.onPriceResponseRejected(qrid, 0, qrid.length);

    assertEquals(0, sm.activeCount());
    assertNull(sm.findByQuoteReqId(qrid, 0, qrid.length));
  }

  @Test
  void onNewOrderSingleWithQuote_transitionsToPendingValidation() {
    acquireSlot(QUOTE_REQ_ID);
    acceptPrice(QUOTE_REQ_ID, QUOTE_ID);

    final var qid = QID_BYTES;
    final var nosBytes = new UnsafeBuffer(new byte[64]);
    final var rfq = sm.onNewOrderSingleWithQuote(qid, 0, qid.length, nosBytes, 0, 64, NOW);

    assertNotNull(rfq);
    assertEquals(RfqState.State.PENDING_VALIDATION, rfq.state());
  }

  @Test
  void onNewOrderSingleWithQuote_stashesNosBytes() {
    acquireSlot(QUOTE_REQ_ID);
    acceptPrice(QUOTE_REQ_ID, QUOTE_ID);

    final var nosData = "NOS-PAYLOAD-DATA".getBytes(StandardCharsets.US_ASCII);
    final var nosBuffer = new UnsafeBuffer(nosData);
    final var qid = QID_BYTES;

    final var rfq =
        sm.onNewOrderSingleWithQuote(qid, 0, qid.length, nosBuffer, 0, nosData.length, NOW);

    assertNotNull(rfq);
    assertEquals(nosData.length, rfq.nosLength());
  }

  @Test
  void onValidationValid_transitionsToCompleted_releasesSlot() {
    acquireSlot(QUOTE_REQ_ID);
    acceptPrice(QUOTE_REQ_ID, QUOTE_ID);
    transitionToPendingValidation(QUOTE_ID);

    final var qid = QID_BYTES;
    sm.onValidationValid(qid, 0, qid.length);

    assertEquals(0, sm.activeCount());
  }

  @Test
  void onValidationInvalid_transitionsToRejected_releasesSlot() {
    acquireSlot(QUOTE_REQ_ID);
    acceptPrice(QUOTE_REQ_ID, QUOTE_ID);
    transitionToPendingValidation(QUOTE_ID);

    final var qid = QID_BYTES;
    sm.onValidationInvalid(qid, 0, qid.length);

    assertEquals(0, sm.activeCount());
  }

  @Test
  void rejectQuoted_transitionsToRejected_releasesSlot() {
    acquireSlot(QUOTE_REQ_ID);
    acceptPrice(QUOTE_REQ_ID, QUOTE_ID);

    final var qid = QID_BYTES;
    sm.rejectQuoted(qid, 0, qid.length);

    assertEquals(0, sm.activeCount());
  }

  // ===========================================================================
  // Invalid-transition matrix (not found / wrong state)
  // ===========================================================================

  @Test
  void onPriceResponseAccepted_notFound_returnsNull() {
    final var unknown = SbeFieldUtil.zeroPad("UNKNOWN-QR-ID000", RfqState.QUOTE_REQ_ID_LENGTH);
    assertNull(
        sm.onPriceResponseAccepted(
            unknown, 0, unknown.length, wrapPriceResponse(), new byte[20], 0, 15, NOW));
  }

  @Test
  void onPriceResponseAccepted_stateIsQuoted_returnsNull() {
    acquireSlot(QUOTE_REQ_ID);
    acceptPrice(QUOTE_REQ_ID, QUOTE_ID);
    // RFQ is now QUOTED, not PENDING_PRICE
    final var qrid = QRID_BYTES;
    assertNull(
        sm.onPriceResponseAccepted(
            qrid, 0, qrid.length, wrapPriceResponse(), new byte[20], 0, 15, NOW));
  }

  @Test
  void onPriceResponseRejected_notFound_returnsNull() {
    final var unknown = SbeFieldUtil.zeroPad("UNKNOWN-QR-ID000", RfqState.QUOTE_REQ_ID_LENGTH);
    assertNull(sm.onPriceResponseRejected(unknown, 0, unknown.length));
  }

  @Test
  void onPriceResponseRejected_stateIsQuoted_returnsNull() {
    acquireSlot(QUOTE_REQ_ID);
    acceptPrice(QUOTE_REQ_ID, QUOTE_ID);
    final var qrid = QRID_BYTES;
    assertNull(sm.onPriceResponseRejected(qrid, 0, qrid.length));
  }

  @Test
  void onNewOrderSingleWithQuote_notFound_returnsNull() {
    final var unknown = SbeFieldUtil.zeroPad("UNKNOWN-QUOTE-ID", RfqState.QUOTE_ID_LENGTH);
    assertNull(
        sm.onNewOrderSingleWithQuote(
            unknown, 0, unknown.length, new UnsafeBuffer(new byte[64]), 0, 64, NOW));
  }

  @Test
  void onNewOrderSingleWithQuote_stateIsPendingPrice_returnsNull() {
    acquireSlot(QUOTE_REQ_ID);
    // RFQ is PENDING_PRICE, not QUOTED — use quoteReqId as lookup (there's no quoteId yet)
    // But onNewOrderSingleWithQuote looks up by quoteId, so this will be not-found
    final var qid = SbeFieldUtil.zeroPad("NONEXISTENT-QID0", RfqState.QUOTE_ID_LENGTH);
    assertNull(
        sm.onNewOrderSingleWithQuote(
            qid, 0, qid.length, new UnsafeBuffer(new byte[64]), 0, 64, NOW));
  }

  @Test
  void onNewOrderSingleWithQuote_nosTooLarge_returnsNullSlotStaysQuoted() {
    acquireSlot(QUOTE_REQ_ID);
    acceptPrice(QUOTE_REQ_ID, QUOTE_ID);
    assertEquals(1, sm.activeCount());

    final var qid = QID_BYTES;
    final var qrid = QRID_BYTES;
    // NOS larger than NOS_STASH_BUFFER_SIZE (512)
    final var oversizedNos = new UnsafeBuffer(new byte[1024]);
    assertNull(sm.onNewOrderSingleWithQuote(qid, 0, qid.length, oversizedNos, 0, 1024, NOW));

    // Slot intentionally stays in QUOTED — the caller (OrchestratorService.onNewOrderSingle)
    // must read identity fields (symbol, currency, etc.) from the RfqState BEFORE the slot is
    // released to populate the rejection ExecutionReport. The caller invokes rejectQuoted()
    // after publishing the rejection. Releasing in-band here would zero the identity fields
    // and corrupt the rejection (Gemini PR #45 review G1).
    assertEquals(1, sm.activeCount());
    final var stillThere = sm.findByQuoteId(qid, 0, qid.length);
    assertNotNull(stillThere);
    assertEquals(RfqState.State.QUOTED, stillThere.state());
    assertNotNull(sm.findByQuoteReqId(qrid, 0, qrid.length));

    // Caller's responsibility: explicitly release after rejection encoded.
    sm.rejectQuoted(qid, 0, qid.length);
    assertEquals(0, sm.activeCount());
    assertNull(sm.findByQuoteId(qid, 0, qid.length));
    assertNull(sm.findByQuoteReqId(qrid, 0, qrid.length));
  }

  @Test
  void onValidationValid_notFound_returnsNull() {
    final var unknown = SbeFieldUtil.zeroPad("UNKNOWN-QUOTE-ID", RfqState.QUOTE_ID_LENGTH);
    assertNull(sm.onValidationValid(unknown, 0, unknown.length));
  }

  @Test
  void onValidationValid_stateIsQuoted_returnsNull() {
    acquireSlot(QUOTE_REQ_ID);
    acceptPrice(QUOTE_REQ_ID, QUOTE_ID);
    // RFQ is QUOTED, not PENDING_VALIDATION
    final var qid = QID_BYTES;
    assertNull(sm.onValidationValid(qid, 0, qid.length));
  }

  @Test
  void onValidationInvalid_notFound_returnsNull() {
    final var unknown = SbeFieldUtil.zeroPad("UNKNOWN-QUOTE-ID", RfqState.QUOTE_ID_LENGTH);
    assertNull(sm.onValidationInvalid(unknown, 0, unknown.length));
  }

  @Test
  void onValidationInvalid_stateIsQuoted_returnsNull() {
    acquireSlot(QUOTE_REQ_ID);
    acceptPrice(QUOTE_REQ_ID, QUOTE_ID);
    final var qid = QID_BYTES;
    assertNull(sm.onValidationInvalid(qid, 0, qid.length));
  }

  @Test
  void rejectQuoted_notFound_returnsNull() {
    final var unknown = SbeFieldUtil.zeroPad("UNKNOWN-QUOTE-ID", RfqState.QUOTE_ID_LENGTH);
    assertNull(sm.rejectQuoted(unknown, 0, unknown.length));
  }

  @Test
  void rejectQuoted_stateIsPendingPrice_returnsNull() {
    acquireSlot(QUOTE_REQ_ID);
    // RFQ is PENDING_PRICE — rejectQuoted expects QUOTED
    // But rejectQuoted looks up by quoteId (not quoteReqId), so this is not-found
    final var qid = SbeFieldUtil.zeroPad("NO-SUCH-QUOTE-ID", RfqState.QUOTE_ID_LENGTH);
    assertNull(sm.rejectQuoted(qid, 0, qid.length));
  }

  // ===========================================================================
  // Lookup
  // ===========================================================================

  @Test
  void findByQuoteReqId_existingEntry_returnsRfq() {
    acquireSlot(QUOTE_REQ_ID);
    final var qrid = QRID_BYTES;
    assertNotNull(sm.findByQuoteReqId(qrid, 0, qrid.length));
  }

  @Test
  void findByQuoteReqId_missingEntry_returnsNull() {
    final var unknown = SbeFieldUtil.zeroPad("UNKNOWN-QR-ID000", RfqState.QUOTE_REQ_ID_LENGTH);
    assertNull(sm.findByQuoteReqId(unknown, 0, unknown.length));
  }

  @Test
  void findByQuoteId_existingEntry_returnsRfq() {
    acquireSlot(QUOTE_REQ_ID);
    acceptPrice(QUOTE_REQ_ID, QUOTE_ID);
    final var qid = QID_BYTES;
    assertNotNull(sm.findByQuoteId(qid, 0, qid.length));
  }

  @Test
  void findByQuoteId_missingEntry_returnsNull() {
    final var unknown = SbeFieldUtil.zeroPad("UNKNOWN-QUOTE-ID", RfqState.QUOTE_ID_LENGTH);
    assertNull(sm.findByQuoteId(unknown, 0, unknown.length));
  }

  // ===========================================================================
  // Timeout reaping
  // ===========================================================================

  @Test
  void reapExpired_expiredRfq_transitionsToExpiredAndCallsBack() {
    acquireSlot(QUOTE_REQ_ID);
    final var counter = new AtomicInteger(0);
    final long expiredTime = NOW + PENDING_PRICE_TIMEOUT + 1;

    // acquireSlot advances reapCursor past the acquired slot. The incremental sweep may need
    // multiple passes to cover the full pool. Call reapExpired until the RFQ is found.
    int totalExpired = 0;
    // sm.capacity() is the worst-case loop bound regardless of incremental scanLimit
    for (int pass = 0; pass < sm.capacity() && counter.get() == 0; pass++) {
      totalExpired += sm.reapExpired(expiredTime, state -> counter.incrementAndGet());
    }
    assertEquals(1, totalExpired);
    assertEquals(1, counter.get());
    assertEquals(0, sm.activeCount());
  }

  @Test
  void reapExpired_activeButNotExpired_skips() {
    acquireSlot(QUOTE_REQ_ID);
    final var counter = new AtomicInteger(0);

    // Don't advance past timeout
    final int expired = sm.reapExpired(NOW + 1, state -> counter.incrementAndGet());
    assertEquals(0, expired);
    assertEquals(0, counter.get());
    assertEquals(1, sm.activeCount());
  }

  @Test
  void reapExpired_callbackThrows_slotStillReleased() {
    acquireSlot(QUOTE_REQ_ID);
    final long expiredTime = NOW + PENDING_PRICE_TIMEOUT + 1;

    // sm.capacity() is the worst-case loop bound regardless of incremental scanLimit
    for (int pass = 0; pass < sm.capacity() && sm.activeCount() > 0; pass++) {
      try {
        sm.reapExpired(
            expiredTime,
            state -> {
              throw new RuntimeException("callback failure");
            });
      } catch (final RuntimeException e) {
        // expected
      }
    }

    // Slot should still be released despite callback throw
    assertEquals(0, sm.activeCount());
  }

  @Test
  void reapAll_expiresAllActiveRfqs() {
    acquireSlot("QR-000000000001");
    acquireSlot("QR-000000000002");
    acquireSlot("QR-000000000003");
    assertEquals(3, sm.activeCount());

    final var counter = new AtomicInteger(0);
    final int expired =
        sm.reapAll(NOW + PENDING_PRICE_TIMEOUT + 1, state -> counter.incrementAndGet());

    assertEquals(3, expired);
    assertEquals(3, counter.get());
    assertEquals(0, sm.activeCount());
  }

  @Test
  void reapAll_emptyPool_returnsZero() {
    assertEquals(0, sm.reapAll(NOW, state -> {}));
  }

  // ===========================================================================
  // Pool management
  // ===========================================================================

  @Test
  void acquireSlot_recyclesToPreviouslyReleasedSlots() {
    // Fill pool
    for (int i = 0; i < POOL_SIZE; i++) {
      acquireSlot("QR-" + String.format("%012d", i));
    }
    assertEquals(POOL_SIZE, sm.activeCount());

    // Release one slot via rejection
    final var qrid = SbeFieldUtil.zeroPad("QR-000000000000", RfqState.QUOTE_REQ_ID_LENGTH);
    sm.onPriceResponseRejected(qrid, 0, qrid.length);
    assertEquals(POOL_SIZE - 1, sm.activeCount());

    // Should be able to acquire again
    final var recycled = acquireSlot("QR-RECYCLED00000");
    assertNotNull(recycled);
    assertEquals(POOL_SIZE, sm.activeCount());
  }

  @Test
  void activeCount_tracksNonTerminalEntries() {
    assertEquals(0, sm.activeCount());
    acquireSlot(QUOTE_REQ_ID);
    assertEquals(1, sm.activeCount());
  }

  @Test
  void capacity_returnsPoolSize() {
    assertEquals(POOL_SIZE, sm.capacity());
  }

  // ===========================================================================
  // Map cleanup
  // ===========================================================================

  @Test
  void onPriceResponseRejected_removesFromQuoteReqIdMap() {
    acquireSlot(QUOTE_REQ_ID);
    final var qrid = QRID_BYTES;

    sm.onPriceResponseRejected(qrid, 0, qrid.length);

    assertNull(sm.findByQuoteReqId(qrid, 0, qrid.length));
  }

  @Test
  void onValidationValid_removesFromBothMaps() {
    acquireSlot(QUOTE_REQ_ID);
    acceptPrice(QUOTE_REQ_ID, QUOTE_ID);
    transitionToPendingValidation(QUOTE_ID);

    final var qid = QID_BYTES;
    final var qrid = QRID_BYTES;
    sm.onValidationValid(qid, 0, qid.length);

    assertNull(sm.findByQuoteId(qid, 0, qid.length));
    assertNull(sm.findByQuoteReqId(qrid, 0, qrid.length));
  }

  @Test
  void reapExpired_removesFromMaps() {
    acquireSlot(QUOTE_REQ_ID);
    final var qrid = QRID_BYTES;
    final long expiredTime = NOW + PENDING_PRICE_TIMEOUT + 1;

    // sm.capacity() is the worst-case loop bound regardless of incremental scanLimit
    for (int pass = 0; pass < sm.capacity(); pass++) {
      sm.reapExpired(expiredTime, state -> {});
    }

    assertNull(sm.findByQuoteReqId(qrid, 0, qrid.length));
  }

  // ===========================================================================
  // Duplicate quoteReqId — in-band rejection (C.0a hardening)
  // ===========================================================================

  @Test
  void onQuoteRequest_duplicateActiveQuoteReqId_returnsNull() {
    // First acquire succeeds and registers the quoteReqId in the lookup map.
    final var first = acquireSlot(QUOTE_REQ_ID);
    assertNotNull(first);
    assertEquals(1, sm.activeCount());

    // Second acquire with the SAME quoteReqId is rejected in-band:
    // RfqStateMachine.onQuoteRequest now probes byQuoteReqId before acquiring a slot and returns
    // null if a non-terminal RFQ already holds that key. No second slot is consumed; activeCount
    // stays at 1; the map entry continues to point at the first slot.
    final var second = acquireSlot(QUOTE_REQ_ID);
    assertNull(second);
    assertEquals(1, sm.activeCount());

    final var qrid = QRID_BYTES;
    final var found = sm.findByQuoteReqId(qrid, 0, qrid.length);
    assertNotNull(found);
    assertEquals(first.poolIndex(), found.poolIndex());
  }

  @Test
  void onQuoteRequest_duplicateAfterTerminal_succeeds() {
    // After an RFQ reaches a terminal state (REJECTED here) and its slot is released, the same
    // quoteReqId can be re-acquired — the duplicate check only blocks active (non-terminal) RFQs.
    final var first = acquireSlot(QUOTE_REQ_ID);
    assertNotNull(first);
    sm.onPriceResponseRejected(QRID_BYTES, 0, QRID_BYTES.length);
    assertEquals(0, sm.activeCount());

    final var second = acquireSlot(QUOTE_REQ_ID);
    assertNotNull(second);
    assertEquals(1, sm.activeCount());
  }

  @Test
  void onQuoteRequest_duplicateWhileQuoted_returnsNull() {
    // Verify C.0a duplicate detection rejects a re-acquire while the existing RFQ is in QUOTED
    // (not just PENDING_PRICE). Covers the branch where probeByQuoteReqId finds a non-terminal
    // RFQ in any active state.
    acquireSlot(QUOTE_REQ_ID);
    acceptPrice(QUOTE_REQ_ID, QUOTE_ID);
    assertEquals(1, sm.activeCount());

    assertNull(acquireSlot(QUOTE_REQ_ID));
    assertEquals(1, sm.activeCount());
  }

  @Test
  void onQuoteRequest_duplicateWhilePendingValidation_returnsNull() {
    // Verify C.0a duplicate detection rejects a re-acquire while the existing RFQ is in
    // PENDING_VALIDATION (the third active state, not just PENDING_PRICE / QUOTED).
    acquireSlot(QUOTE_REQ_ID);
    acceptPrice(QUOTE_REQ_ID, QUOTE_ID);
    transitionToPendingValidation(QUOTE_ID);
    assertEquals(1, sm.activeCount());

    assertNull(acquireSlot(QUOTE_REQ_ID));
    assertEquals(1, sm.activeCount());
  }

  // ===========================================================================
  // Helpers
  // ===========================================================================

  /** Encodes a default QuoteRequest, wraps the decoder, and acquires a PENDING_PRICE slot. */
  private RfqState acquireSlot(final String quoteReqId) {
    SbeTestEncoder.encodeQuoteRequest(
        buf, 0, quoteReqId, SYMBOL, SideEnum.Buy, 100_000_000L, "ACCT001");
    headerDecoder.wrap(buf, 0);
    quoteReqDecoder.wrap(
        buf,
        MessageHeaderDecoder.ENCODED_LENGTH,
        headerDecoder.blockLength(),
        headerDecoder.version());
    return sm.onQuoteRequest(quoteReqDecoder, NOW);
  }

  /** Applies a price-accepted response, advancing the RFQ from PENDING_PRICE to QUOTED. */
  private RfqState acceptPrice(final String quoteReqId, final String quoteId) {
    final var qrid = SbeFieldUtil.zeroPad(quoteReqId, RfqState.QUOTE_REQ_ID_LENGTH);
    final var qid = SbeFieldUtil.zeroPad(quoteId, RfqState.QUOTE_ID_LENGTH);
    return sm.onPriceResponseAccepted(
        qrid, 0, RfqState.QUOTE_REQ_ID_LENGTH, wrapPriceResponse(), qid, 0, qid.length, NOW);
  }

  /**
   * Transitions the RFQ identified by {@code quoteId} from QUOTED to PENDING_VALIDATION. Asserts
   * the transition succeeded so callers fail fast if the state machine rejects the input.
   */
  private void transitionToPendingValidation(final String quoteId) {
    final var qid = SbeFieldUtil.zeroPad(quoteId, RfqState.QUOTE_ID_LENGTH);
    final var nosBuffer = new UnsafeBuffer(new byte[64]);
    final var transitioned =
        sm.onNewOrderSingleWithQuote(qid, 0, RfqState.QUOTE_ID_LENGTH, nosBuffer, 0, 64, NOW);
    assertNotNull(transitioned, "expected QUOTED→PENDING_VALIDATION to succeed for " + quoteId);
  }

  /** Encodes a default accepted PriceResponse and returns the wrapped decoder. */
  private PriceResponseDecoder wrapPriceResponse() {
    SbeTestEncoder.encodePriceResponse(
        buf, 0, QUOTE_REQ_ID, SYMBOL, true, 110_000_000L, 111_000_000L, NOW);
    headerDecoder.wrap(buf, 0);
    priceRespDecoder.wrap(
        buf,
        MessageHeaderDecoder.ENCODED_LENGTH,
        headerDecoder.blockLength(),
        headerDecoder.version());
    return priceRespDecoder;
  }
}
