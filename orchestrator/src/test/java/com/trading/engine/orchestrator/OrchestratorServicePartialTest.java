package com.trading.engine.orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.ExecTypeEnum;
import com.trading.engine.messages.sbe.ExecutionReportDecoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.OrdStatusEnum;
import com.trading.engine.messages.sbe.OrdTypeEnum;
import com.trading.engine.messages.sbe.PriceResponseDecoder;
import com.trading.engine.messages.sbe.QuoteRejectReasonEnum;
import com.trading.engine.messages.sbe.QuoteRequestDecoder;
import com.trading.engine.messages.sbe.QuoteRequestRejectDecoder;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.orchestrator.codec.OrchestratorMessageEncoder;
import com.trading.engine.testsupport.buffer.SbeFieldUtil;
import com.trading.engine.testsupport.clock.ControllableNanoClock;
import com.trading.engine.testsupport.sbe.SbeTestEncoder;
import io.aeron.Subscription;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.UnsafeApi;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Partial unit tests for {@link OrchestratorService} — exercises the paths that are testable
 * without booting an Aeron media driver. Aeron's {@link io.aeron.ExclusivePublication} and {@link
 * Subscription} are {@code final} classes; the {@code Publisher} SAM refactor (Phase 2e) lets us
 * inject {@link RecordingPublisher} fakes for outbound traffic. Subscription is still typed but
 * never dereferenced in these test paths — see {@link #unusedSubscription()}.
 *
 * <p>End-to-end coverage of the full {@code doWork()} duty cycle (subscription polling, fragment
 * dispatch) is tracked separately by APP-33 (integration tests with a real media driver).
 */
class OrchestratorServicePartialTest {

  /**
   * Initial value for the deterministic test clock. Non-zero so any "uninitialised long" bug
   * surfaces immediately. The clock is advanced explicitly in tests that need to trip a timeout.
   */
  private static final long START_NANOS = 1_000_000_000L;

  private static final long PENDING_PRICE_TIMEOUT = 5_000_000_000L;
  private static final long QUOTED_TIMEOUT = 30_000_000_000L;
  private static final long PENDING_VALIDATION_TIMEOUT = 5_000_000_000L;
  private static final long SWEEP_INTERVAL = 1_000_000_000L;
  private static final int POOL_SIZE = 4;

  private static final String QUOTE_REQ_ID = "QR-000000000001";
  private static final String QUOTE_ID = "QTE-00000000001";
  private static final String SYMBOL = "EURUSD";
  private static final String ACCOUNT = "ACCT001";
  private static final String CL_ORD_ID = "ORD-00000000001";
  private static final long ORDER_QTY = 100_000_000L;
  private static final long BID_PX = 110_000_000L;
  private static final long OFFER_PX = 111_000_000L;

  private final MutableDirectBuffer scratch = new ExpandableArrayBuffer(512);
  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final QuoteRequestDecoder quoteReqDecoder = new QuoteRequestDecoder();
  private final PriceResponseDecoder priceRespDecoder = new PriceResponseDecoder();

  private RecordingPublisher gatewayPublisher;
  private RecordingPublisher pricingPublisher;
  private RfqStateMachine sm;
  private OrchestratorIdGenerator quoteIdGen;
  private OrchestratorMessageEncoder encoder;

  /** Fresh instance per test via {@link #setUp()} — clock state is NEVER shared across tests. */
  private ControllableNanoClock clock;

  private OrchestratorService service;

  @BeforeEach
  void setUp() {
    gatewayPublisher = new RecordingPublisher();
    pricingPublisher = new RecordingPublisher();
    sm =
        new RfqStateMachine(
            POOL_SIZE, PENDING_PRICE_TIMEOUT, QUOTED_TIMEOUT, PENDING_VALIDATION_TIMEOUT);
    quoteIdGen = new OrchestratorIdGenerator("QTE");
    encoder = new OrchestratorMessageEncoder();
    clock = new ControllableNanoClock(START_NANOS);
    service =
        new OrchestratorService(
            unusedSubscription(),
            gatewayPublisher,
            unusedSubscription(),
            pricingPublisher,
            sm,
            quoteIdGen,
            encoder,
            clock,
            clock,
            SWEEP_INTERVAL);
  }

  // ===========================================================================
  // validateQuoteRequest — pure-logic paths (no publisher invocation)
  // ===========================================================================

  @Test
  void validateQuoteRequest_emptySymbol_returnsRejectText() {
    final var decoder =
        encodeQuoteRequest(QUOTE_REQ_ID, /* symbol */ "", SideEnum.Buy, ORDER_QTY, ACCOUNT);
    final var rejectText = service.validateQuoteRequest(decoder);
    assertNotNull(rejectText);
    assertTrue(asciiOf(rejectText).startsWith("Empty symbol"));
    assertEquals(QuoteRejectReasonEnum.UnknownSymbol, service.lastValidationFailureReason());
    assertEquals(0, gatewayPublisher.callCount());
    assertEquals(0, pricingPublisher.callCount());
  }

  @Test
  void validateQuoteRequest_zeroOrderQty_returnsRejectText() {
    final var decoder =
        encodeQuoteRequest(QUOTE_REQ_ID, SYMBOL, SideEnum.Buy, /* orderQty */ 0L, ACCOUNT);
    final var rejectText = service.validateQuoteRequest(decoder);
    assertNotNull(rejectText);
    assertTrue(asciiOf(rejectText).startsWith("Non-positive orderQty"));
    assertEquals(QuoteRejectReasonEnum.Other, service.lastValidationFailureReason());
  }

  @Test
  void validateQuoteRequest_nullSide_returnsRejectText() {
    final var decoder =
        encodeQuoteRequest(QUOTE_REQ_ID, SYMBOL, SideEnum.NULL_VAL, ORDER_QTY, ACCOUNT);
    final var rejectText = service.validateQuoteRequest(decoder);
    assertNotNull(rejectText);
    assertTrue(asciiOf(rejectText).startsWith("Invalid side"));
    assertEquals(QuoteRejectReasonEnum.Other, service.lastValidationFailureReason());
  }

  @Test
  void validateQuoteRequest_emptyAccountCode_returnsRejectText() {
    final var decoder =
        encodeQuoteRequest(QUOTE_REQ_ID, SYMBOL, SideEnum.Buy, ORDER_QTY, /* account */ "");
    final var rejectText = service.validateQuoteRequest(decoder);
    assertNotNull(rejectText);
    assertTrue(asciiOf(rejectText).startsWith("Empty accountCode"));
    assertEquals(QuoteRejectReasonEnum.Other, service.lastValidationFailureReason());
  }

  @Test
  void validateQuoteRequest_allFieldsValid_returnsNull() {
    final var decoder = encodeQuoteRequest(QUOTE_REQ_ID, SYMBOL, SideEnum.Buy, ORDER_QTY, ACCOUNT);
    assertNull(service.validateQuoteRequest(decoder));
  }

  // ===========================================================================
  // onRfqExpired — captured via reapCallback (publisher invocation paths)
  // ===========================================================================

  @Test
  void onRfqExpired_pendingPriceState_sendsQuoteRequestReject() {
    // Arrange: acquire an RFQ in PENDING_PRICE
    final var quoteReqDec =
        encodeQuoteRequest(QUOTE_REQ_ID, SYMBOL, SideEnum.Buy, ORDER_QTY, ACCOUNT);
    assertNotNull(sm.onQuoteRequest(quoteReqDec, START_NANOS));

    expireOneRfq(START_NANOS + PENDING_PRICE_TIMEOUT + 1);

    // Assert: gateway received exactly one QuoteRequestReject (templateId 3) with reason
    // TooLateToEnter and "RFQ expired" text. Pricing publisher untouched.
    assertEquals(1, gatewayPublisher.callCount());
    assertEquals(0, pricingPublisher.callCount());
    final var captured = new UnsafeBuffer(gatewayPublisher.capturedBufferBytes(0));
    headerDecoder.wrap(captured, 0);
    assertEquals(QuoteRequestRejectDecoder.TEMPLATE_ID, headerDecoder.templateId());
    final var qrr = new QuoteRequestRejectDecoder();
    qrr.wrap(
        captured,
        MessageHeaderDecoder.ENCODED_LENGTH,
        headerDecoder.blockLength(),
        headerDecoder.version());
    assertTrue(qrr.quoteReqId().startsWith(QUOTE_REQ_ID));
    assertEquals(QuoteRejectReasonEnum.TooLateToEnter, qrr.quoteRejectReason());
    assertTrue(qrr.text().startsWith("RFQ expired"));
  }

  @Test
  void onRfqExpired_pendingValidationState_sendsRejectExecutionReport() {
    // Arrange: drive the RFQ from PENDING_PRICE → QUOTED → PENDING_VALIDATION with a stashed NOS
    final var quoteReqDec =
        encodeQuoteRequest(QUOTE_REQ_ID, SYMBOL, SideEnum.Buy, ORDER_QTY, ACCOUNT);
    assertNotNull(sm.onQuoteRequest(quoteReqDec, START_NANOS));

    final var qrid = SbeFieldUtil.zeroPad(QUOTE_REQ_ID, RfqState.QUOTE_REQ_ID_LENGTH);
    final var qid = SbeFieldUtil.zeroPad(QUOTE_ID, RfqState.QUOTE_ID_LENGTH);

    SbeTestEncoder.encodePriceResponse(
        scratch, 0, QUOTE_REQ_ID, SYMBOL, true, BID_PX, OFFER_PX, START_NANOS);
    headerDecoder.wrap(scratch, 0);
    priceRespDecoder.wrap(
        scratch,
        MessageHeaderDecoder.ENCODED_LENGTH,
        headerDecoder.blockLength(),
        headerDecoder.version());
    assertNotNull(
        sm.onPriceResponseAccepted(
            qrid, 0, qrid.length, priceRespDecoder, qid, 0, qid.length, START_NANOS));

    // Encode a real NOS into a buffer and stash it via the state machine
    final var nosBuf = new ExpandableArrayBuffer(256);
    final int nosLen = encodeNos(nosBuf);
    assertNotNull(sm.onNewOrderSingleWithQuote(qid, 0, qid.length, nosBuf, 0, nosLen, START_NANOS));

    expireOneRfq(START_NANOS + PENDING_VALIDATION_TIMEOUT + 1);

    // Assert: gateway received exactly one ExecutionReport (template 5) with execType=Rejected
    // and ClOrdID extracted from the stashed NOS
    assertEquals(1, gatewayPublisher.callCount());
    final var captured = new UnsafeBuffer(gatewayPublisher.capturedBufferBytes(0));
    headerDecoder.wrap(captured, 0);
    assertEquals(ExecutionReportDecoder.TEMPLATE_ID, headerDecoder.templateId());
    final var er = new ExecutionReportDecoder();
    er.wrap(
        captured,
        MessageHeaderDecoder.ENCODED_LENGTH,
        headerDecoder.blockLength(),
        headerDecoder.version());
    assertEquals(ExecTypeEnum.Rejected, er.execType());
    assertEquals(OrdStatusEnum.Rejected, er.ordStatus());
    assertTrue(er.clOrdId().startsWith(CL_ORD_ID));
    assertTrue(er.text().startsWith("RFQ expired"));
  }

  // ===========================================================================
  // Helpers
  // ===========================================================================

  /**
   * Aeron {@link Subscription} is {@code final}; tests that do not exercise polling use an
   * uninitialised shell allocated via {@link UnsafeApi#allocateInstance}. The shell is NEVER
   * dereferenced in the test paths covered here ({@code validateQuoteRequest}, {@code
   * onRfqExpired}, reap callback identity); if a future test calls a {@link Subscription} method on
   * this object, a runtime NPE/IllegalStateException will surface the bug. Aeron version is pinned
   * (libs.versions.toml: aeron=1.50.4); on Aeron upgrade, re-validate that no field initializer or
   * instance-method invariant is violated by the uninitialised shell.
   */
  private static Subscription unusedSubscription() {
    return (Subscription) UnsafeApi.allocateInstance(Subscription.class);
  }

  /**
   * Drives the captured reap callback past the supplied expiry timestamp until exactly one RFQ is
   * reaped. Asserts the count so callers can focus on the published-message assertions. Multiple
   * passes are required because {@code reapExpired} uses an incremental cursor that may need to
   * wrap to find the slot.
   */
  private void expireOneRfq(final long expiredAt) {
    final var callback = service.reapCallbackForTesting();
    int totalReaped = 0;
    for (int pass = 0; pass < sm.capacity() && totalReaped == 0; pass++) {
      totalReaped += sm.reapExpired(expiredAt, callback);
    }
    assertEquals(1, totalReaped);
  }

  /** Encodes a QuoteRequest into the scratch buffer and returns the wrapped decoder. */
  private QuoteRequestDecoder encodeQuoteRequest(
      final String quoteReqId,
      final String symbol,
      final SideEnum side,
      final long orderQty,
      final String accountCode) {
    SbeTestEncoder.encodeQuoteRequest(scratch, 0, quoteReqId, symbol, side, orderQty, accountCode);
    headerDecoder.wrap(scratch, 0);
    quoteReqDecoder.wrap(
        scratch,
        MessageHeaderDecoder.ENCODED_LENGTH,
        headerDecoder.blockLength(),
        headerDecoder.version());
    return quoteReqDecoder;
  }

  /**
   * Encodes a NewOrderSingle directly into the supplied buffer (separate from the test's main
   * scratch so it can be passed to {@code stashNos} without the scratch being overwritten).
   *
   * @return the total encoded length including SBE header
   */
  private int encodeNos(final MutableDirectBuffer dst) {
    return SbeTestEncoder.encodeNewOrderSingle(
        dst,
        0,
        CL_ORD_ID,
        SYMBOL,
        SideEnum.Buy,
        OrdTypeEnum.PreviouslyQuoted,
        BID_PX,
        ORDER_QTY,
        ACCOUNT,
        "USD");
  }

  private static String asciiOf(final byte[] bytes) {
    return new String(bytes, java.nio.charset.StandardCharsets.US_ASCII);
  }
}
