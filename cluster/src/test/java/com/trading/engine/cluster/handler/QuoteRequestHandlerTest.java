package com.trading.engine.cluster.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.trading.engine.cluster.TradingClusteredServiceFactory;
import com.trading.engine.cluster.journal.EventJournal;
import com.trading.engine.cluster.metrics.RfqMetrics;
import com.trading.engine.cluster.refdata.AccountState;
import com.trading.engine.cluster.refdata.AccountStore;
import com.trading.engine.cluster.refdata.CurrencyState;
import com.trading.engine.cluster.refdata.CurrencyStore;
import com.trading.engine.cluster.sequencer.EventSequencer;
import com.trading.engine.cluster.state.RfqStateMachine;
import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.ProductTypeEnum;
import com.trading.engine.messages.sbe.QuoteRejectReasonEnum;
import com.trading.engine.messages.sbe.QuoteRejectedEventDecoder;
import com.trading.engine.messages.sbe.QuoteRequestDecoder;
import com.trading.engine.messages.sbe.QuoteRequestedEventDecoder;
import com.trading.engine.messages.sbe.SettlTypeEnum;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.messages.sbe.TenorEnum;
import com.trading.engine.testsupport.aeron.FakeClientSession;
import com.trading.engine.testsupport.aeron.FakeCluster;
import com.trading.engine.testsupport.sbe.SbeTestEncoder;
import java.nio.charset.StandardCharsets;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link QuoteRequestHandler}.
 *
 * <p>Each test seeds {@link AccountStore} and {@link CurrencyStore} as needed, dispatches a {@code
 * QuoteRequest} via {@link QuoteRequestHandler#onCommand}, then asserts on:
 *
 * <ul>
 *   <li>The emitted template ID ({@code 104} = QuoteRequestedEvent or {@code 106} =
 *       QuoteRejectedEvent) via the {@link FakeClientSession#messages} capture.
 *   <li>The decoded field values on the emitted event.
 *   <li>The {@link RfqMetrics} counters.
 * </ul>
 *
 * <p>The cluster reference is left {@code null} for all tests so the timer-scheduling path is
 * skipped (see production code §12: {@code if (cluster == null) { // Test path}}).
 *
 * <p><b>Threading:</b> single-threaded — cluster duty cycle invariant.
 */
class QuoteRequestHandlerTest {

  private static final long TIMESTAMP = 1_700_000_000_000_000_000L;
  private static final int HDR_LEN = MessageHeaderEncoder.ENCODED_LENGTH;

  // ---- Standard ref-data IDs ----
  private static final long ACTIVE_ACCOUNT_ID = 1L;
  private static final long NO_RFQ_ACCOUNT_ID = 2L;
  private static final long INACTIVE_ACCOUNT_ID = 3L;
  private static final String ACTIVE_CODE = "ACME";
  private static final String NO_RFQ_CODE = "NORFQ";
  private static final String INACTIVE_CODE = "SUSPENDED";

  // ---- Pre-packed currencies ----
  private static final int USD_PACKED = CurrencyStore.packCode((byte) 'U', (byte) 'S', (byte) 'D');
  private static final int EUR_PACKED = CurrencyStore.packCode((byte) 'E', (byte) 'U', (byte) 'R');

  private AccountStore accountStore;
  private CurrencyStore currencyStore;
  private RfqMetrics metrics;
  private RfqStateMachine stateMachine;
  private QuoteRequestHandler handler;
  private FakeClientSession session;
  private EventSink eventSink;

  @BeforeEach
  void setUp() {
    accountStore = new AccountStore();
    currencyStore = new CurrencyStore();
    metrics = new RfqMetrics();

    // USD and EUR in CurrencyStore.
    currencyStore.put(USD_PACKED, makeCurrency('U', 'S', 'D'));
    currencyStore.put(EUR_PACKED, makeCurrency('E', 'U', 'R'));

    // ACME: active, CAN_RFQ | CAN_TRADE.
    accountStore.put(
        makeAccount(
            ACTIVE_ACCOUNT_ID,
            ACTIVE_CODE,
            AccountStatusEnum.Active,
            AccountState.Capabilities.CAN_TRADE | AccountState.Capabilities.CAN_RFQ));

    // NORFQ: active, CAN_TRADE only — CAN_RFQ not set.
    accountStore.put(
        makeAccount(
            NO_RFQ_ACCOUNT_ID,
            NO_RFQ_CODE,
            AccountStatusEnum.Active,
            AccountState.Capabilities.CAN_TRADE));

    // SUSPENDED: inactive account.
    accountStore.put(
        makeAccount(
            INACTIVE_ACCOUNT_ID,
            INACTIVE_CODE,
            AccountStatusEnum.Suspended,
            AccountState.Capabilities.CAN_TRADE | AccountState.Capabilities.CAN_RFQ));

    stateMachine = newStateMachine();

    final var sequencer = new EventSequencer();
    final var journal = new EventJournal(256);
    eventSink = new EventSink(sequencer, journal);
    // Wire a FakeCluster on the eventSink so emit() broadcast has something to iterate.
    final var fakeCluster = new FakeCluster(0L);
    eventSink.setCluster(fakeCluster);

    handler = new QuoteRequestHandler(stateMachine, accountStore, currencyStore, metrics);
    // Do NOT call handler.setCluster() — cluster == null triggers the test path.

    session = new FakeClientSession(42L);
    fakeCluster.addClientSession(session);
  }

  // -------------------------------------------------------------------------
  // Happy path — emits template 104 with all fields
  // -------------------------------------------------------------------------

  /**
   * A fully-valid QuoteRequest from an active, CAN_RFQ account with known currencies must emit a
   * QuoteRequestedEvent (template 104) with all fields round-tripped correctly.
   */
  @Test
  void onCommand_validQuoteRequest_emitsTemplate104WithAllFields() {
    final var buf = new ExpandableArrayBuffer(512);
    final int len =
        SbeTestEncoder.encodeQuoteRequest(
            buf, 0, "REQ-001", "EURUSD", SideEnum.Buy, 100_000_000L, ACTIVE_CODE);

    dispatch(buf, len);

    assertEquals(1, session.messages.size(), "exactly one message must be emitted");
    final byte[] msg = session.messages.get(0);
    final int templateId = templateId(msg);
    assertEquals(
        QuoteRequestedEventDecoder.TEMPLATE_ID,
        templateId,
        "template 104 (QuoteRequestedEvent) expected, got " + templateId);

    final var dec = decodeQuoteRequested(msg);
    assertEquals(SideEnum.Buy, dec.side());
    assertEquals(100_000_000L, dec.orderQty());
    assertEquals(ProductTypeEnum.Spot, dec.productType());

    // quoteReqId round-trip
    final byte[] qrIdBytes = new byte[QuoteRequestedEventDecoder.quoteReqIdLength()];
    dec.getQuoteReqId(qrIdBytes, 0);
    final var qrId = new String(qrIdBytes, StandardCharsets.US_ASCII).trim();
    assertEquals("REQ-001", qrId.replace("\0", ""));

    assertEquals(1L, metrics.emitRequested, "emitRequested counter must be 1");
    assertEquals(0L, metrics.emitRejected, "emitRejected counter must be 0");
  }

  // -------------------------------------------------------------------------
  // Template ID introspection (simplified as per task description)
  // -------------------------------------------------------------------------

  /**
   * Verifies that {@link QuoteRequestHandler#commandTemplateId()} returns {@link
   * QuoteRequestDecoder#TEMPLATE_ID}. This ensures the handler is correctly self- describing for
   * the dispatch table.
   */
  @Test
  void commandTemplateId_returnsQuoteRequestTemplateId() {
    assertEquals(
        QuoteRequestDecoder.TEMPLATE_ID,
        handler.commandTemplateId(),
        "handler must declare the QuoteRequest template ID");
  }

  // -------------------------------------------------------------------------
  // Malformed — length below block length → template 106
  // -------------------------------------------------------------------------

  /**
   * A message shorter than {@code HDR_LEN + QuoteRequestDecoder.BLOCK_LENGTH} must emit a
   * QuoteRejectedEvent (106) with quoteRejectReason=Other and text="malformed".
   */
  @Test
  void onCommand_lengthBelowBlockLength_emitsTemplate106Malformed() {
    // Supply a buffer with length = HDR_LEN - 1 (clearly below the minimum).
    final var buf = new UnsafeBuffer(new byte[HDR_LEN]);
    final var hdr = new MessageHeaderEncoder();
    hdr.wrap(buf, 0)
        .blockLength(QuoteRequestDecoder.BLOCK_LENGTH)
        .templateId(QuoteRequestDecoder.TEMPLATE_ID)
        .schemaId(1)
        .version(1);

    // Pass length = 1 (far below HDR_LEN + BLOCK_LENGTH).
    dispatchRaw(buf, 1);

    assertEquals(1, session.messages.size());
    final var dec = decodeQuoteRejected(session.messages.get(0));
    assertEquals(QuoteRejectReasonEnum.Other, dec.quoteRejectReason());

    // text must be "malformed"
    assertTextEquals("malformed", dec);

    assertEquals(1L, metrics.rejectMalformed);
    assertEquals(1L, metrics.emitRejected);
    assertEquals(0L, metrics.emitRequested);
  }

  // -------------------------------------------------------------------------
  // Stale-buffer guard: malformed emit AFTER a successful 104 must zero
  // quoteReqId / symbol / accountCode so the reject doesn't ship stale wire bytes
  // -------------------------------------------------------------------------

  /**
   * R4 fix: {@code emitMalformed} reuses the egress buffer across emits. Without explicit
   * zero-fills of the char-array fields, a malformed-path 106 emitted after a successful 104 would
   * carry the previous emit's quoteReqId / symbol / accountCode at the same body offsets. This test
   * drives the "happy 104, then malformed" sequence and asserts every char field on the second emit
   * is fully zero.
   */
  @Test
  void onCommand_malformedAfterValid_doesNotLeakPriorBufferContent() {
    // First: drive a happy-path QuoteRequest that emits 104 with populated fields.
    final var firstBuf = new ExpandableArrayBuffer(512);
    final int firstLen =
        SbeTestEncoder.encodeQuoteRequest(
            firstBuf, 0, "QREQ-LEAK", "EURUSD", SideEnum.Buy, 100_000_000L, ACTIVE_CODE);
    dispatch(firstBuf, firstLen);
    assertEquals(1, session.messages.size());

    // Second: dispatch a malformed message (length below block length).
    final var secondBuf = new UnsafeBuffer(new byte[HDR_LEN]);
    final var hdr2 = new MessageHeaderEncoder();
    hdr2.wrap(secondBuf, 0)
        .blockLength(QuoteRequestDecoder.BLOCK_LENGTH)
        .templateId(QuoteRequestDecoder.TEMPLATE_ID)
        .schemaId(1)
        .version(1);
    dispatchRaw(secondBuf, 1);

    // Decode the second emit and verify every byte of the four char fields is zero.
    assertEquals(2, session.messages.size());
    final var dec = decodeQuoteRejected(session.messages.get(1));
    final byte[] qrIdBytes = new byte[20];
    final byte[] symBytes = new byte[8];
    final byte[] acctBytes = new byte[16];
    dec.getQuoteReqId(qrIdBytes, 0);
    dec.getSymbol(symBytes, 0);
    dec.getAccountCode(acctBytes, 0);
    for (int i = 0; i < qrIdBytes.length; i++) {
      assertEquals((byte) 0, qrIdBytes[i], "quoteReqId[" + i + "] not zero — stale buffer leak");
    }
    for (int i = 0; i < symBytes.length; i++) {
      assertEquals((byte) 0, symBytes[i], "symbol[" + i + "] not zero — stale buffer leak");
    }
    for (int i = 0; i < acctBytes.length; i++) {
      assertEquals((byte) 0, acctBytes[i], "accountCode[" + i + "] not zero — stale buffer leak");
    }
    assertEquals(SideEnum.NULL_VAL, dec.side(), "side must be NULL_VAL on malformed");
  }

  // -------------------------------------------------------------------------
  // Empty symbol → template 106 UnknownSymbol
  // -------------------------------------------------------------------------

  /**
   * A QuoteRequest with an all-NUL symbol must be rejected with {@code
   * quoteRejectReason=UnknownSymbol} and text "symbol empty".
   */
  @Test
  void onCommand_emptySymbol_emitsTemplate106UnknownSymbol() {
    // Encode a QuoteRequest with empty symbol by setting symbol="".
    final var buf = new ExpandableArrayBuffer(512);
    final var hdrEnc = new MessageHeaderEncoder();
    final var enc = new com.trading.engine.messages.sbe.QuoteRequestEncoder();
    enc.wrapAndApplyHeader(buf, 0, hdrEnc);
    enc.quoteReqId("REQ-NOSYM");
    enc.symbol(""); // empty → all NUL
    enc.side(SideEnum.Buy);
    enc.orderQty(100_000_000L);
    enc.accountCode(ACTIVE_CODE);
    enc.productType(ProductTypeEnum.Spot);
    enc.settlDate("20260101");
    enc.settlType(SettlTypeEnum.Regular);
    enc.tenor(TenorEnum.SN);
    enc.currency("USD");
    enc.settlCurrency("EUR");
    enc.transactTime(0L);
    enc.noLegsCount(0);
    final int len = HDR_LEN + enc.encodedLength();

    dispatch(buf, len);

    assertEquals(1, session.messages.size());
    final var dec = decodeQuoteRejected(session.messages.get(0));
    assertEquals(QuoteRejectReasonEnum.UnknownSymbol, dec.quoteRejectReason());
    assertTextEquals("symbol empty", dec);
    assertEquals(1L, metrics.rejectSymbolEmpty);
    assertEquals(1L, metrics.emitRejected);
  }

  // -------------------------------------------------------------------------
  // Inactive account → template 106 Other "account inactive"
  // -------------------------------------------------------------------------

  /**
   * A QuoteRequest from an inactive account must emit template 106 with {@code
   * quoteRejectReason=Other} and text "account inactive".
   */
  @Test
  void onCommand_inactiveAccount_emitsTemplate106Other() {
    final var buf = new ExpandableArrayBuffer(512);
    final int len =
        SbeTestEncoder.encodeQuoteRequest(
            buf, 0, "REQ-INACT", "EURUSD", SideEnum.Sell, 50_000_000L, INACTIVE_CODE);

    dispatch(buf, len);

    assertEquals(1, session.messages.size());
    final var dec = decodeQuoteRejected(session.messages.get(0));
    assertEquals(QuoteRejectReasonEnum.Other, dec.quoteRejectReason());
    assertTextEquals("account inactive", dec);
    assertEquals(1L, metrics.rejectAccountInactive);
    assertEquals(1L, metrics.emitRejected);
    assertEquals(0L, metrics.emitRequested);
  }

  // -------------------------------------------------------------------------
  // Account without CAN_RFQ → template 106 Other "rfq not permitted"
  // -------------------------------------------------------------------------

  /**
   * An active account without the {@code CAN_RFQ} capability bit must be rejected with text "rfq
   * not permitted".
   */
  @Test
  void onCommand_accountWithoutCanRfq_emitsTemplate106RfqNotPermitted() {
    final var buf = new ExpandableArrayBuffer(512);
    final int len =
        SbeTestEncoder.encodeQuoteRequest(
            buf, 0, "REQ-NORFQ", "EURUSD", SideEnum.Buy, 100_000_000L, NO_RFQ_CODE);

    dispatch(buf, len);

    assertEquals(1, session.messages.size());
    final var dec = decodeQuoteRejected(session.messages.get(0));
    assertEquals(QuoteRejectReasonEnum.Other, dec.quoteRejectReason());
    assertTextEquals("rfq not permitted", dec);
    assertEquals(1L, metrics.rejectRfqNotPermitted);
    assertEquals(1L, metrics.emitRejected);
  }

  // -------------------------------------------------------------------------
  // Unknown currency → template 106 Other "currency unknown"
  // -------------------------------------------------------------------------

  /**
   * A QuoteRequest with a currency not in CurrencyStore must emit 106 with text "currency unknown".
   */
  @Test
  void onCommand_unknownCurrency_emitsTemplate106Other() {
    // Encode manually with currency=GBP which is not in the store.
    final var buf = new ExpandableArrayBuffer(512);
    final var hdrEnc = new MessageHeaderEncoder();
    final var enc = new com.trading.engine.messages.sbe.QuoteRequestEncoder();
    enc.wrapAndApplyHeader(buf, 0, hdrEnc);
    enc.quoteReqId("REQ-CCY");
    enc.symbol("GBPUSD");
    enc.side(SideEnum.Buy);
    enc.orderQty(100_000_000L);
    enc.accountCode(ACTIVE_CODE);
    enc.productType(ProductTypeEnum.Spot);
    enc.settlDate("20260101");
    enc.settlType(SettlTypeEnum.Regular);
    enc.tenor(TenorEnum.SN);
    enc.currency("GBP"); // unknown
    enc.settlCurrency("EUR");
    enc.transactTime(0L);
    enc.noLegsCount(0);
    final int len = HDR_LEN + enc.encodedLength();

    dispatch(buf, len);

    assertEquals(1, session.messages.size());
    final var dec = decodeQuoteRejected(session.messages.get(0));
    assertEquals(QuoteRejectReasonEnum.Other, dec.quoteRejectReason());
    assertTextEquals("currency unknown", dec);
    assertEquals(1L, metrics.rejectCurrencyUnknown);
    assertEquals(1L, metrics.emitRejected);
  }

  // -------------------------------------------------------------------------
  // Rate limit exceeded — 101st call with same session → template 106 "rate limit"
  // -------------------------------------------------------------------------

  /**
   * After consuming all {@code DEFAULT_RFQ_RATE_LIMIT_PER_SESSION} tokens (100 by default), the
   * 101st request from the same session must be rejected with text "rate limit".
   *
   * <p>Each call uses a unique quoteReqId to avoid the duplicate-detection path.
   */
  @Test
  void onCommand_rateLimitExceeded_emitsTemplate106RateLimit() {
    // The default rate limit is 100 per 1s window; all requests sent at the same timestamp.
    final int limit = (int) TradingClusteredServiceFactory.DEFAULT_RFQ_RATE_LIMIT_PER_SESSION;

    final var buf = new ExpandableArrayBuffer(512);

    // First `limit` requests must succeed.
    for (int i = 1; i <= limit; i++) {
      session.messages.clear();
      final int len =
          SbeTestEncoder.encodeQuoteRequest(
              buf, 0, "RATE-" + i, "EURUSD", SideEnum.Buy, 100_000_000L, ACTIVE_CODE);
      dispatch(buf, len);
      // Each should produce a 104 (not 106).
      assertEquals(1, session.messages.size(), "request " + i + " should succeed");
      assertEquals(
          QuoteRequestedEventDecoder.TEMPLATE_ID,
          templateId(session.messages.get(0)),
          "request " + i + " should produce 104");
    }

    // 101st request — same session, same timestamp → rate limit exhausted.
    session.messages.clear();
    final int lenOver =
        SbeTestEncoder.encodeQuoteRequest(
            buf, 0, "RATE-OVER", "EURUSD", SideEnum.Buy, 100_000_000L, ACTIVE_CODE);
    dispatch(buf, lenOver);

    assertEquals(1, session.messages.size());
    final var dec = decodeQuoteRejected(session.messages.get(0));
    assertEquals(QuoteRejectReasonEnum.TooLateToEnter, dec.quoteRejectReason());
    assertTextEquals("rate limit", dec);
    assertEquals(1L, metrics.rejectRateLimit);
  }

  // -------------------------------------------------------------------------
  // Duplicate quoteReqId → template 106 "duplicate"
  // -------------------------------------------------------------------------

  /**
   * Sending the same quoteReqId twice (while the first is still REQUESTED) must reject the second
   * with text "duplicate".
   */
  @Test
  void onCommand_duplicateQuoteReqId_emitsTemplate106Duplicate() {
    final var buf = new ExpandableArrayBuffer(512);
    final int len =
        SbeTestEncoder.encodeQuoteRequest(
            buf, 0, "REQ-DUP", "EURUSD", SideEnum.Buy, 100_000_000L, ACTIVE_CODE);

    // First submission must succeed.
    dispatch(buf, len);
    assertEquals(1, session.messages.size());
    assertEquals(QuoteRequestedEventDecoder.TEMPLATE_ID, templateId(session.messages.get(0)));

    // Second submission with same quoteReqId.
    session.messages.clear();
    dispatch(buf, len);

    assertEquals(1, session.messages.size());
    final var dec = decodeQuoteRejected(session.messages.get(0));
    assertEquals(QuoteRejectReasonEnum.Other, dec.quoteRejectReason());
    assertTextEquals("duplicate", dec);
    assertEquals(1L, metrics.rejectDuplicate);
    assertEquals(1L, metrics.emitRejected);
  }

  // -------------------------------------------------------------------------
  // Pool exhausted → template 106 "pool exhausted"
  // -------------------------------------------------------------------------

  /**
   * When the RFQ slot pool is fully occupied, a new QuoteRequest must be rejected with text "pool
   * exhausted".
   *
   * <p>The pool is filled by sending {@code capacity} valid requests with distinct quoteReqIds. The
   * next request must then hit the pool-exhausted guard.
   */
  @Test
  void onCommand_poolExhausted_emitsTemplate106PoolExhausted() {
    // Use a small-capacity state machine so we don't need to send 8192 requests.
    final int smallCapacity = 256;
    stateMachine =
        new RfqStateMachine(
            smallCapacity,
            TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS,
            TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS,
            TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS,
            TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS,
            TradingClusteredServiceFactory.DEFAULT_RFQ_REQUEST_TIMEOUT_NANOS,
            /* rateLimitPerSession */ smallCapacity + 1L, // generous limit to not block fill
            TradingClusteredServiceFactory.DEFAULT_RFQ_RATE_LIMIT_WINDOW_NANOS,
            TradingClusteredServiceFactory.DEFAULT_RFQ_ACCEPT_PRICE_TOLERANCE_BPS,
            TradingClusteredServiceFactory.DEFAULT_RFQ_ACCEPT_QTY_TOLERANCE_BPS,
            accountStore,
            metrics);
    handler = new QuoteRequestHandler(stateMachine, accountStore, currencyStore, metrics);

    final var buf = new ExpandableArrayBuffer(512);

    // Fill the pool.
    for (int i = 0; i < smallCapacity; i++) {
      final int len =
          SbeTestEncoder.encodeQuoteRequest(
              buf, 0, "POOL-" + pad4(i), "EURUSD", SideEnum.Buy, 100_000_000L, ACTIVE_CODE);
      session.messages.clear();
      dispatch(buf, len);
      assertEquals(
          QuoteRequestedEventDecoder.TEMPLATE_ID,
          templateId(session.messages.get(0)),
          "slot " + i + " fill must succeed");
    }

    assertEquals(0L, stateMachine.freeCount(), "pool must be fully occupied");

    // Next request must fail with pool-exhausted.
    session.messages.clear();
    final int overLen =
        SbeTestEncoder.encodeQuoteRequest(
            buf, 0, "POOL-OVER", "EURUSD", SideEnum.Buy, 100_000_000L, ACTIVE_CODE);
    dispatch(buf, overLen);

    assertEquals(1, session.messages.size());
    final var dec = decodeQuoteRejected(session.messages.get(0));
    assertEquals(QuoteRejectReasonEnum.TooLateToEnter, dec.quoteRejectReason());
    assertTextEquals("pool exhausted", dec);
    assertEquals(1L, metrics.rejectPoolExhausted);
  }

  // -------------------------------------------------------------------------
  // Metrics are zeroed at setup and accurately track multiple paths
  // -------------------------------------------------------------------------

  /**
   * Verifies that after N successful QuoteRequests the metrics accurately report emitRequested=N
   * and emitRejected=0.
   */
  @Test
  void metrics_afterMultipleSuccessfulRequests_countedCorrectly() {
    final int count = 5;
    final var buf = new ExpandableArrayBuffer(512);
    for (int i = 0; i < count; i++) {
      final int len =
          SbeTestEncoder.encodeQuoteRequest(
              buf, 0, "MET-" + i, "EURUSD", SideEnum.Buy, 100_000_000L, ACTIVE_CODE);
      dispatch(buf, len);
    }
    assertEquals((long) count, metrics.emitRequested);
    assertEquals(0L, metrics.emitRejected);
  }

  /**
   * Verifies that a mix of valid and invalid requests correctly increments both emitRequested and
   * emitRejected.
   */
  @Test
  void metrics_mixedValidAndInvalidRequests_bothCountersIncrementCorrectly() {
    final var buf = new ExpandableArrayBuffer(512);

    // One valid.
    final int len =
        SbeTestEncoder.encodeQuoteRequest(
            buf, 0, "MIXED-OK", "EURUSD", SideEnum.Buy, 100_000_000L, ACTIVE_CODE);
    dispatch(buf, len);

    // One invalid (inactive account).
    final int lenBad =
        SbeTestEncoder.encodeQuoteRequest(
            buf, 0, "MIXED-BAD", "EURUSD", SideEnum.Buy, 100_000_000L, INACTIVE_CODE);
    dispatch(buf, lenBad);

    assertEquals(1L, metrics.emitRequested);
    assertEquals(1L, metrics.emitRejected);
    assertEquals(1L, metrics.rejectAccountInactive);
  }

  // =========================================================================
  // Helpers
  // =========================================================================

  /** Dispatches a command buffer to the handler via the test EventSink. */
  private void dispatch(final MutableDirectBuffer buf, final int len) {
    // Parse the SBE header to extract blockLength and version for the handler.
    final var hdrDec = new MessageHeaderDecoder();
    hdrDec.wrap(buf, 0);
    final int blockLength = hdrDec.blockLength();
    final int version = hdrDec.version();

    handler.onCommand(session, TIMESTAMP, buf, 0, len, blockLength, version, eventSink);
  }

  /** Dispatches with a raw UnsafeBuffer without re-parsing header (for malformed tests). */
  private void dispatchRaw(final UnsafeBuffer buf, final int len) {
    handler.onCommand(
        session,
        TIMESTAMP,
        buf,
        0,
        len,
        QuoteRequestDecoder.BLOCK_LENGTH,
        1, // sbeSchemaVersion — version 1 matches current schema
        eventSink);
  }

  /** Reads the templateId from the message header of a captured byte[]. */
  private static int templateId(final byte[] msg) {
    final var hdr = new MessageHeaderDecoder();
    hdr.wrap(new UnsafeBuffer(msg), 0);
    return hdr.templateId();
  }

  /** Decodes a {@link QuoteRequestedEventDecoder} from a captured byte[]. */
  private static QuoteRequestedEventDecoder decodeQuoteRequested(final byte[] msg) {
    final var buf = new UnsafeBuffer(msg);
    final var hdr = new MessageHeaderDecoder();
    hdr.wrap(buf, 0);
    final var dec = new QuoteRequestedEventDecoder();
    dec.wrap(buf, HDR_LEN, hdr.blockLength(), hdr.version());
    return dec;
  }

  /** Decodes a {@link QuoteRejectedEventDecoder} from a captured byte[]. */
  private static QuoteRejectedEventDecoder decodeQuoteRejected(final byte[] msg) {
    final var buf = new UnsafeBuffer(msg);
    final var hdr = new MessageHeaderDecoder();
    hdr.wrap(buf, 0);
    final var dec = new QuoteRejectedEventDecoder();
    dec.wrap(buf, HDR_LEN, hdr.blockLength(), hdr.version());
    return dec;
  }

  /**
   * Asserts that the {@code text} field of the given decoder starts with the expected ASCII prefix
   * (trimmed of trailing NUL padding).
   */
  private static void assertTextEquals(final String expected, final QuoteRejectedEventDecoder dec) {
    final byte[] textBytes = new byte[64];
    dec.getText(textBytes, 0);
    final var actual = new String(textBytes, StandardCharsets.US_ASCII).replace("\0", "").trim();
    assertEquals(expected, actual, "QuoteRejectedEvent text mismatch");
  }

  /** Creates a simple CurrencyState for a 3-letter uppercase code. */
  private static CurrencyState makeCurrency(final char b0, final char b1, final char b2) {
    final var s = new CurrencyState();
    s.setCcyCode((byte) b0, (byte) b1, (byte) b2);
    s.setIsoNumeric(0);
    s.setName(new byte[0], 0, 0);
    s.setDecimals(2);
    s.setCurrencyClass(com.trading.engine.messages.sbe.CurrencyClassEnum.Fiat);
    s.setStatus(AccountStatusEnum.Active);
    s.setTransactTime(0L);
    return s;
  }

  /** Creates an AccountState with the given id, code, status, and capabilities. */
  private static AccountState makeAccount(
      final long id, final String code, final AccountStatusEnum status, final long capabilities) {
    final var s = new AccountState();
    s.setAccountId(id);
    s.setParentAccountId(0L);
    final byte[] codeBytes = code.getBytes(StandardCharsets.US_ASCII);
    s.setAccountCode(codeBytes, 0, codeBytes.length);
    s.setStatus(status);
    s.setCapabilities(capabilities);
    s.setTransactTime(0L);
    return s;
  }

  /** Creates a default-configuration RfqStateMachine using the factory defaults. */
  private RfqStateMachine newStateMachine() {
    return new RfqStateMachine(
        TradingClusteredServiceFactory.DEFAULT_RFQ_POOL_CAPACITY,
        TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS,
        TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS,
        TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS,
        TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS,
        TradingClusteredServiceFactory.DEFAULT_RFQ_REQUEST_TIMEOUT_NANOS,
        TradingClusteredServiceFactory.DEFAULT_RFQ_RATE_LIMIT_PER_SESSION,
        TradingClusteredServiceFactory.DEFAULT_RFQ_RATE_LIMIT_WINDOW_NANOS,
        TradingClusteredServiceFactory.DEFAULT_RFQ_ACCEPT_PRICE_TOLERANCE_BPS,
        TradingClusteredServiceFactory.DEFAULT_RFQ_ACCEPT_QTY_TOLERANCE_BPS,
        accountStore,
        metrics);
  }

  /** Left-pads an integer to 4 digits for use in quoteReqId strings. */
  private static String pad4(final int n) {
    return String.format("%04d", n);
  }
}
