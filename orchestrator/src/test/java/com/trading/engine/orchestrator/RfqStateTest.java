package com.trading.engine.orchestrator;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.PriceResponseDecoder;
import com.trading.engine.messages.sbe.QuoteRequestDecoder;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.testsupport.buffer.SbeFieldUtil;
import com.trading.engine.testsupport.sbe.SbeTestEncoder;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Dedicated unit tests for {@link RfqState} — exercises the flyweight in isolation, complementary
 * to {@link RfqStateMachineTest} (which tests state transitions end-to-end). Coverage targets:
 *
 * <ul>
 *   <li>Lifecycle helpers ({@code reset}, {@code isTerminal}, {@code isActive})
 *   <li>Identity field accessors after {@code populateFromQuoteRequest}
 *   <li>Pricing field accessors after {@code applyPriceResponse}
 *   <li>{@code putXxxInto(byte[], int)} byte-array overloads
 *   <li>{@code putXxxInto(MutableDirectBuffer, int)} buffer overloads
 *   <li>{@code stashNos} success and rejection paths
 * </ul>
 *
 * <p>This class focuses on the {@code RfqState} contract; behavior tests for state-machine
 * transitions live in {@link RfqStateMachineTest}.
 */
class RfqStateTest {

  private static final long PENDING_PRICE_TIMEOUT = 5_000_000_000L;
  private static final long QUOTED_TIMEOUT = 30_000_000_000L;
  private static final long NOW = 1_000_000_000L;

  private static final String QUOTE_REQ_ID = "QR-000000000001";
  private static final String QUOTE_ID = "QTE-00000000001";
  private static final String SYMBOL = "EURUSD";
  private static final String ACCOUNT = "ACCT001";
  private static final long ORDER_QTY = 100_000_000L;
  private static final long BID_PX = 110_000_000L;
  private static final long OFFER_PX = 111_000_000L;
  private static final long BID_SIZE = 100_000_000L;
  private static final long OFFER_SIZE = 100_000_000L;
  private static final long VALID_UNTIL = NOW + 30_000_000_000L;

  private final MutableDirectBuffer scratch = new ExpandableArrayBuffer(512);
  private final MessageHeaderDecoder hdrDec = new MessageHeaderDecoder();
  private final QuoteRequestDecoder qrDec = new QuoteRequestDecoder();
  private final PriceResponseDecoder prDec = new PriceResponseDecoder();

  private RfqState state;

  @BeforeEach
  void setUp() {
    state = new RfqState(/* poolIndex */ 0);
  }

  // ===========================================================================
  // Lifecycle helpers
  // ===========================================================================

  @Test
  void reset_afterPopulateAndApplyPricing_zerosEveryField() {
    // Populate identity + pricing + stash a NOS so EVERY field is non-default before reset.
    populate();
    applyPricing();
    final var nosBytes = "NOS-FOR-RESET".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
    assertTrue(state.stashNos(new UnsafeBuffer(nosBytes), 0, nosBytes.length));

    // Pre-condition: most fields non-default after the populate+pricing+stash sequence.
    assertEquals(RfqState.State.QUOTED, state.state());
    assertTrue(state.quoteReqIdLen() > 0);
    assertEquals(ORDER_QTY, state.orderQty());
    assertEquals(BID_PX, state.bidPx());
    assertEquals(nosBytes.length, state.nosLength());

    state.reset();

    // Lifecycle fields
    assertEquals(RfqState.State.FREE, state.state());
    assertEquals(0L, state.expiryNanos());
    // Identity / length fields
    assertEquals(0, state.quoteReqIdLen());
    assertEquals(0, state.quoteIdLen());
    // Quantity + raw enum bytes
    assertEquals(0L, state.orderQty());
    assertEquals((byte) 0, state.sideRaw());
    assertEquals((byte) 0, state.productTypeRaw());
    assertEquals((byte) 0, state.settlTypeRaw());
    assertEquals((byte) 0, state.tenorRaw());
    // Pricing fields
    assertEquals(0L, state.bidPx());
    assertEquals(0L, state.offerPx());
    assertEquals(0L, state.bidSize());
    assertEquals(0L, state.offerSize());
    assertEquals(0L, state.validUntil());
    assertEquals(0L, state.swapPoints());
    // Time + NOS
    assertEquals(0L, state.transactTime());
    assertEquals(0, state.nosLength());
  }

  @Test
  void isTerminal_completedRejectedExpired_returnsTrue_otherwiseFalse() {
    populate();
    assertFalse(state.isTerminal()); // PENDING_PRICE

    state.setState(RfqState.State.QUOTED);
    assertFalse(state.isTerminal());

    state.setState(RfqState.State.PENDING_VALIDATION);
    assertFalse(state.isTerminal());

    state.setState(RfqState.State.COMPLETED);
    assertTrue(state.isTerminal());

    state.setState(RfqState.State.REJECTED);
    assertTrue(state.isTerminal());

    state.setState(RfqState.State.EXPIRED);
    assertTrue(state.isTerminal());

    state.reset();
    assertFalse(state.isTerminal()); // FREE is not terminal
  }

  @Test
  void isActive_pendingAndQuotedStates_returnTrue_freeAndTerminalReturnFalse() {
    // FREE is the initial state — not active.
    assertFalse(state.isActive());

    populate(); // moves to PENDING_PRICE
    assertTrue(state.isActive());

    state.setState(RfqState.State.QUOTED);
    assertTrue(state.isActive());

    state.setState(RfqState.State.PENDING_VALIDATION);
    assertTrue(state.isActive());

    state.setState(RfqState.State.COMPLETED);
    assertFalse(state.isActive());

    state.setState(RfqState.State.REJECTED);
    assertFalse(state.isActive());

    state.setState(RfqState.State.EXPIRED);
    assertFalse(state.isActive());
  }

  // ===========================================================================
  // Identity / quantity accessors after populateFromQuoteRequest
  // ===========================================================================

  @Test
  void quoteReqIdLen_afterPopulate_matchesSbeFieldLength() {
    populate();
    assertEquals(RfqState.QUOTE_REQ_ID_LENGTH, state.quoteReqIdLen());
  }

  @Test
  void quoteIdLen_afterSetQuoteId_matchesProvidedLength() {
    populate();
    final var qid = SbeFieldUtil.zeroPad(QUOTE_ID, RfqState.QUOTE_ID_LENGTH);
    state.setQuoteId(qid, 0, qid.length);
    assertEquals(qid.length, state.quoteIdLen());
  }

  @Test
  void setQuoteId_partialLength_nullPadsRemainder() {
    // setQuoteId with length < QUOTE_ID_LENGTH triggers the null-padding branch in
    // RfqState.setQuoteId. Verify both the recorded length and that the remainder of the
    // QuoteID slot is zeroed (so subsequent putQuoteIdInto reads a clean buffer).
    populate();
    final var partial = "QTE-PART".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
    state.setQuoteId(partial, 0, partial.length);
    assertEquals(partial.length, state.quoteIdLen());

    final var dst = new byte[RfqState.QUOTE_ID_LENGTH];
    state.putQuoteIdInto(dst, 0);
    // First N bytes match the input; the rest must be zero (null-padded).
    for (int i = 0; i < partial.length; i++) {
      assertEquals(partial[i], dst[i], "byte " + i);
    }
    for (int i = partial.length; i < RfqState.QUOTE_ID_LENGTH; i++) {
      assertEquals((byte) 0, dst[i], "padding byte " + i);
    }
  }

  @Test
  void sideRaw_afterPopulate_returnsRawByteFromDecoder() {
    populate(); // SBE encoder uses SideEnum.Buy
    assertEquals((byte) SideEnum.Buy.value(), state.sideRaw());
  }

  // ===========================================================================
  // Pricing accessors after applyPriceResponse
  // ===========================================================================

  @Test
  void bidPx_afterApplyPriceResponse_returnsExactValue() {
    populate();
    applyPricing();
    assertEquals(BID_PX, state.bidPx());
  }

  @Test
  void offerPx_afterApplyPriceResponse_returnsExactValue() {
    populate();
    applyPricing();
    assertEquals(OFFER_PX, state.offerPx());
  }

  @Test
  void bidSize_afterApplyPriceResponse_returnsExactValue() {
    populate();
    applyPricing();
    assertEquals(BID_SIZE, state.bidSize());
  }

  @Test
  void offerSize_afterApplyPriceResponse_returnsExactValue() {
    populate();
    applyPricing();
    assertEquals(OFFER_SIZE, state.offerSize());
  }

  @Test
  void swapPoints_afterApplyPriceResponse_returnsNullValueForSpotResponse() {
    populate();
    applyPricing();
    // SbeTestEncoder.encodePriceResponse defaults swapPoints to NULL_VAL for FX spot.
    assertEquals(PriceResponseDecoder.swapPointsNullValue(), state.swapPoints());
  }

  // ===========================================================================
  // putXxxInto(byte[], int) — copies into pre-allocated byte arrays
  // ===========================================================================

  @Test
  void putQuoteReqIdInto_byteArray_copiesExactBytes() {
    populate();
    final var dst = new byte[RfqState.QUOTE_REQ_ID_LENGTH];
    final int written = state.putQuoteReqIdInto(dst, 0);
    assertEquals(RfqState.QUOTE_REQ_ID_LENGTH, written);
    assertArrayEquals(SbeFieldUtil.zeroPad(QUOTE_REQ_ID, RfqState.QUOTE_REQ_ID_LENGTH), dst);
  }

  @Test
  void putQuoteIdInto_byteArray_copiesExactBytes() {
    populate();
    final var qid = SbeFieldUtil.zeroPad(QUOTE_ID, RfqState.QUOTE_ID_LENGTH);
    state.setQuoteId(qid, 0, qid.length);

    final var dst = new byte[RfqState.QUOTE_ID_LENGTH];
    final int written = state.putQuoteIdInto(dst, 0);
    assertEquals(RfqState.QUOTE_ID_LENGTH, written);
    assertArrayEquals(qid, dst);
  }

  @Test
  void putSymbolInto_byteArray_copiesExactBytes() {
    populate();
    final var dst = new byte[RfqState.SYMBOL_LENGTH];
    final int written = state.putSymbolInto(dst, 0);
    assertEquals(RfqState.SYMBOL_LENGTH, written);
    assertArrayEquals(SbeFieldUtil.zeroPad(SYMBOL, RfqState.SYMBOL_LENGTH), dst);
  }

  @Test
  void putAccountCodeInto_byteArray_copiesExactBytes() {
    populate();
    final var dst = new byte[RfqState.ACCOUNT_CODE_LENGTH];
    final int written = state.putAccountCodeInto(dst, 0);
    assertEquals(RfqState.ACCOUNT_CODE_LENGTH, written);
    assertArrayEquals(SbeFieldUtil.zeroPad(ACCOUNT, RfqState.ACCOUNT_CODE_LENGTH), dst);
  }

  // ===========================================================================
  // putXxxInto(MutableDirectBuffer, int) — copies into off-heap-friendly buffers
  // ===========================================================================

  @Test
  void putQuoteReqIdInto_mutableBuffer_copiesExactBytes() {
    populate();
    final MutableDirectBuffer dst = new ExpandableArrayBuffer(64);
    final int written = state.putQuoteReqIdInto(dst, 7); // non-zero offset
    assertEquals(RfqState.QUOTE_REQ_ID_LENGTH, written);
    final var copy = new byte[RfqState.QUOTE_REQ_ID_LENGTH];
    dst.getBytes(7, copy);
    assertArrayEquals(SbeFieldUtil.zeroPad(QUOTE_REQ_ID, RfqState.QUOTE_REQ_ID_LENGTH), copy);
  }

  @Test
  void putSymbolInto_mutableBuffer_copiesExactBytes() {
    populate();
    final MutableDirectBuffer dst = new ExpandableArrayBuffer(64);
    final int written = state.putSymbolInto(dst, 0);
    assertEquals(RfqState.SYMBOL_LENGTH, written);
    final var copy = new byte[RfqState.SYMBOL_LENGTH];
    dst.getBytes(0, copy);
    assertArrayEquals(SbeFieldUtil.zeroPad(SYMBOL, RfqState.SYMBOL_LENGTH), copy);
  }

  @Test
  void putNosInto_mutableBuffer_copiesStashedBytesUpToLength() {
    populate();
    final var nosBytes =
        "NOS-PAYLOAD-FOR-TEST".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
    final var nosBuf = new UnsafeBuffer(nosBytes);
    assertTrue(state.stashNos(nosBuf, 0, nosBytes.length));
    assertEquals(nosBytes.length, state.nosLength());

    final MutableDirectBuffer dst = new ExpandableArrayBuffer(64);
    final int written = state.putNosInto(dst, 0);
    assertEquals(nosBytes.length, written);
    final var copy = new byte[nosBytes.length];
    dst.getBytes(0, copy);
    assertArrayEquals(nosBytes, copy);
  }

  // ===========================================================================
  // stashNos — success vs rejection
  // ===========================================================================

  @Test
  void stashNos_oversizedBuffer_returnsFalseWithoutMutating() {
    populate();
    final var oversizedNos =
        new UnsafeBuffer(new byte[OrchestratorConstants.NOS_STASH_BUFFER_SIZE + 1]);
    assertFalse(state.stashNos(oversizedNos, 0, OrchestratorConstants.NOS_STASH_BUFFER_SIZE + 1));
    assertEquals(0, state.nosLength()); // unchanged
  }

  // ===========================================================================
  // Helpers
  // ===========================================================================

  /** Populates the state from a default QuoteRequest decoder ({@code SideEnum.Buy}). */
  private void populate() {
    SbeTestEncoder.encodeQuoteRequest(
        scratch, 0, QUOTE_REQ_ID, SYMBOL, SideEnum.Buy, ORDER_QTY, ACCOUNT);
    hdrDec.wrap(scratch, 0);
    qrDec.wrap(
        scratch, MessageHeaderDecoder.ENCODED_LENGTH, hdrDec.blockLength(), hdrDec.version());
    state.populateFromQuoteRequest(qrDec, NOW, PENDING_PRICE_TIMEOUT);
  }

  /** Applies an accepted PriceResponse using the canonical bid/offer/sizes. */
  private void applyPricing() {
    SbeTestEncoder.encodePriceResponse(
        scratch, 0, QUOTE_REQ_ID, SYMBOL, /* accepted */ true, BID_PX, OFFER_PX, NOW);
    hdrDec.wrap(scratch, 0);
    prDec.wrap(
        scratch, MessageHeaderDecoder.ENCODED_LENGTH, hdrDec.blockLength(), hdrDec.version());
    state.applyPriceResponse(prDec, NOW, QUOTED_TIMEOUT);
  }
}
