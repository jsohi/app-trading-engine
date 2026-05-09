package com.trading.engine.cluster.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.trading.engine.cluster.IdGenerator;
import com.trading.engine.cluster.TradingClusteredServiceFactory;
import com.trading.engine.cluster.journal.EventJournal;
import com.trading.engine.cluster.metrics.RfqMetrics;
import com.trading.engine.cluster.refdata.AccountState;
import com.trading.engine.cluster.refdata.AccountStore;
import com.trading.engine.cluster.sequencer.EventSequencer;
import com.trading.engine.cluster.state.RfqSlot;
import com.trading.engine.cluster.state.RfqSlotState;
import com.trading.engine.cluster.state.RfqStateMachine;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.PriceResponseDecoder;
import com.trading.engine.messages.sbe.ProductTypeEnum;
import com.trading.engine.messages.sbe.QuoteCreatedEventDecoder;
import com.trading.engine.messages.sbe.QuoteRejectReasonEnum;
import com.trading.engine.messages.sbe.QuoteRejectedEventDecoder;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.messages.sbe.TenorEnum;
import com.trading.engine.testsupport.aeron.FakeClientSession;
import com.trading.engine.testsupport.sbe.SbeTestEncoder;
import io.aeron.DirectBufferVector;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.logbuffer.BufferClaim;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link PriceResponseHandler}.
 *
 * <p>Each test seeds the {@link RfqStateMachine} with a REQUESTED slot (acquired via {@link
 * RfqStateMachine#acquire()}, bytes populated, then registered via {@link
 * RfqStateMachine#registerRequested(RfqSlot)}), dispatches a {@code PriceResponse} (template 51)
 * via {@link PriceResponseHandler#onCommand}, then asserts on:
 *
 * <ul>
 *   <li>The emitted template ID ({@code 105} = QuoteCreatedEvent or {@code 106} =
 *       QuoteRejectedEvent) via the {@link FakeClientSession#messages} capture.
 *   <li>The decoded field values on the emitted event.
 *   <li>The {@link RfqMetrics} counters.
 *   <li>The slot state transition.
 * </ul>
 *
 * <p>Tests that exercise {@link Cluster#scheduleTimer} use the test-local {@link
 * CapturingFakeCluster} to control and observe timer scheduling behavior. Tests that do not require
 * timer scheduling leave the cluster reference {@code null}.
 *
 * <p>TTL tests use a custom {@link RfqStateMachine} instance with distinct per-product TTL values
 * ({@link #TTL_SPOT_NANOS}, {@link #TTL_FORWARD_NANOS}, {@link #TTL_SWAP_NANOS}) to assert that
 * {@code slot.validUntil} is computed as {@code clusterTs + ttlForProduct(slot.productType)}.
 *
 * <p><b>Threading:</b> single-threaded — cluster duty cycle invariant.
 */
class PriceResponseHandlerTest {

  // -------------------------------------------------------------------------
  // Test-local cluster double that captures scheduleTimer calls
  // -------------------------------------------------------------------------

  /**
   * Aeron {@link Cluster} test double that records every {@code scheduleTimer(correlationId,
   * deadline)} call and returns {@code scheduleTimerResult} (default {@code true}).
   *
   * <p>All non-timer methods delegate to sensible no-op defaults. Implements the full {@link
   * Cluster} interface without subclassing {@link com.trading.engine.testsupport.aeron.FakeCluster}
   * (which is {@code final}) so we can capture and control timer results.
   */
  static final class CapturingFakeCluster implements Cluster {

    /**
     * All (correlationId → deadline) pairs from {@link #scheduleTimer}, in insertion order.
     * Primitive-keyed map (Agrona) — no autoboxing on test-fake capture.
     */
    final Long2LongHashMap scheduledTimers = new Long2LongHashMap(Long.MIN_VALUE);

    /** Controls the return value of {@link #scheduleTimer}. Default {@code true}. */
    boolean scheduleTimerResult = true;

    private final IdleStrategy idle =
        new IdleStrategy() {
          @Override
          public void idle(final int workCount) {}

          @Override
          public void idle() {}

          @Override
          public void reset() {}
        };

    @Override
    public boolean scheduleTimer(final long correlationId, final long deadline) {
      scheduledTimers.put(correlationId, deadline);
      return scheduleTimerResult;
    }

    @Override
    public boolean cancelTimer(final long correlationId) {
      return true;
    }

    @Override
    public int memberId() {
      return 0;
    }

    @Override
    public Role role() {
      return Role.LEADER;
    }

    @Override
    public long logPosition() {
      return 0L;
    }

    @Override
    public io.aeron.Aeron aeron() {
      return null;
    }

    @Override
    public ClusteredServiceContainer.Context context() {
      return null;
    }

    @Override
    public ClientSession getClientSession(final long clusterSessionId) {
      return null;
    }

    @Override
    public Collection<ClientSession> clientSessions() {
      return List.of();
    }

    @Override
    public void forEachClientSession(final Consumer<? super ClientSession> action) {}

    @Override
    public boolean closeClientSession(final long clusterSessionId) {
      return false;
    }

    @Override
    public long time() {
      return 0L;
    }

    @Override
    public TimeUnit timeUnit() {
      return TimeUnit.NANOSECONDS;
    }

    @Override
    public long offer(final DirectBuffer buffer, final int offset, final int length) {
      return 0L;
    }

    @Override
    public long offer(final DirectBufferVector[] vectors) {
      return 0L;
    }

    @Override
    public long tryClaim(final int length, final BufferClaim bufferClaim) {
      return 0L;
    }

    @Override
    public IdleStrategy idleStrategy() {
      return idle;
    }
  }

  // -------------------------------------------------------------------------
  // Constants
  // -------------------------------------------------------------------------

  private static final long TIMESTAMP = 1_700_000_000_000_000_000L;
  private static final int HDR_LEN = MessageHeaderEncoder.ENCODED_LENGTH;

  // Distinct per-product TTL values to make wrong-product assertions unambiguous.
  private static final long TTL_SPOT_NANOS = 10_000_000_000L; // 10 s
  private static final long TTL_FORWARD_NANOS = 20_000_000_000L; // 20 s
  private static final long TTL_SWAP_NANOS = 30_000_000_000L; // 30 s

  private static final long BID_PX = 107_500_000L; // 1.075 in 10^-8
  private static final long OFFER_PX = 107_600_000L; // 1.076 in 10^-8

  private static final String QUOTE_REQ_ID = "QREQ-0000000001";
  private static final String SYMBOL = "EURUSD";

  // -------------------------------------------------------------------------
  // Fixtures
  // -------------------------------------------------------------------------

  private AccountStore accountStore;
  private RfqMetrics metrics;
  private RfqStateMachine stateMachine;
  private IdGenerator quoteIdGen;
  private PriceResponseHandler handler;
  private FakeClientSession session;
  private EventSink eventSink;
  private CapturingFakeCluster capturingCluster;

  @BeforeEach
  void setUp() {
    accountStore = new AccountStore();
    metrics = new RfqMetrics();

    // Seed AccountStore with one active account so snapshot recovery paths have a valid account.
    seedAccount(accountStore, 1L, "ACME");

    stateMachine = buildStateMachine();
    quoteIdGen = new IdGenerator("QTE");

    final var sequencer = new EventSequencer();
    final var journal = new EventJournal(256);
    eventSink = new EventSink(sequencer, journal);

    capturingCluster = new CapturingFakeCluster();

    handler = new PriceResponseHandler(stateMachine, quoteIdGen, metrics);
    // cluster set per test — null means "no timer scheduling" (test-path).

    session = new FakeClientSession(42L);
  }

  // =========================================================================
  // §1 — Happy path: accepted=true, REQUESTED slot → emit 105 + schedule timer
  // =========================================================================

  /**
   * A PriceResponse with accepted=true for a REQUESTED slot must:
   *
   * <ul>
   *   <li>Emit template 105 (QuoteCreatedEvent).
   *   <li>Transition the slot to QUOTED state.
   *   <li>Schedule a TTL timer via {@link Cluster#scheduleTimer}.
   *   <li>Increment {@code metrics.emitCreated = 1}.
   *   <li>Leave {@code metrics.emitRejected = 0}.
   * </ul>
   */
  @Test
  void onCommand_acceptedTrueRequestedSlot_emits105AndSchedulesTimer() {
    handler.setCluster(capturingCluster);
    final var slot = acquireRequestedSlot(QUOTE_REQ_ID, ProductTypeEnum.Spot);

    final var buf = new ExpandableArrayBuffer(512);
    final int len = encodePriceResponse(buf, QUOTE_REQ_ID, true, BID_PX, OFFER_PX);

    dispatch(buf, len);

    assertEquals(1, session.messages.size(), "exactly one message must be emitted");
    assertEquals(
        QuoteCreatedEventDecoder.TEMPLATE_ID,
        templateId(session.messages.get(0)),
        "emitted event must be template 105 (QuoteCreatedEvent)");
    assertEquals(1L, metrics.emitCreated, "emitCreated counter must be 1");
    assertEquals(0L, metrics.emitRejected, "emitRejected counter must be 0");
    assertEquals(RfqSlotState.QUOTED, slot.state, "slot must transition to QUOTED");
    assertEquals(1, capturingCluster.scheduledTimers.size(), "one timer must be scheduled");
  }

  // =========================================================================
  // §2 — accepted=false, REQUESTED slot → emit 106 + release slot
  // =========================================================================

  /**
   * A PriceResponse with accepted=false for a REQUESTED slot must:
   *
   * <ul>
   *   <li>Emit template 106 (QuoteRejectedEvent) with reason=InvalidPrice.
   *   <li>Release the slot (return to FREE state).
   *   <li>Increment {@code metrics.rejectPricingDeclined = 1} and {@code metrics.emitRejected = 1}.
   *   <li>Leave {@code metrics.emitCreated = 0}.
   * </ul>
   */
  @Test
  void onCommand_acceptedFalseRequestedSlot_emits106AndReleasesSlot() {
    // No cluster needed — declined path does not schedule a timer.
    final var slot = acquireRequestedSlot(QUOTE_REQ_ID, ProductTypeEnum.Spot);

    final var buf = new ExpandableArrayBuffer(512);
    final int len = encodePriceResponse(buf, QUOTE_REQ_ID, false, 0L, 0L);

    dispatch(buf, len);

    assertEquals(1, session.messages.size(), "exactly one message must be emitted");
    final var dec = decodeQuoteRejected(session.messages.get(0));
    assertEquals(
        QuoteRejectReasonEnum.InvalidPrice,
        dec.quoteRejectReason(),
        "reject reason must be InvalidPrice");
    assertEquals(1L, metrics.rejectPricingDeclined, "rejectPricingDeclined counter must be 1");
    assertEquals(1L, metrics.emitRejected, "emitRejected counter must be 1");
    assertEquals(0L, metrics.emitCreated, "emitCreated counter must be 0");
    // Verify slot returned to FREE — release() sets slot.state = FREE.
    assertEquals(RfqSlotState.FREE, slot.state, "slot must be FREE after release");
  }

  // =========================================================================
  // §3 — Unknown quoteReqId → silently dropped
  // =========================================================================

  /**
   * A PriceResponse referencing a quoteReqId not in any active slot must be silently dropped
   * without emitting any event. {@code metrics.dropUnknownReqId} must increment by 1.
   */
  @Test
  void onCommand_unknownQuoteReqId_silentlyDropped() {
    // Do NOT acquire any slot — the state machine has no REQUESTED entries.
    final var buf = new ExpandableArrayBuffer(512);
    final int len = encodePriceResponse(buf, "UNKNOWN-REQ-ID-XX", true, BID_PX, OFFER_PX);

    dispatch(buf, len);

    assertEquals(0, session.messages.size(), "no event must be emitted for unknown quoteReqId");
    assertEquals(1L, metrics.dropUnknownReqId, "dropUnknownReqId counter must be 1");
    assertEquals(0L, metrics.emitCreated);
    assertEquals(0L, metrics.emitRejected);
  }

  // =========================================================================
  // §4 — QUOTED slot → silently dropped
  // =========================================================================

  /**
   * A PriceResponse for a slot already in QUOTED state must be silently dropped. This is the
   * idempotent path — template 105 was already emitted for this quoteReqId. {@code
   * metrics.dropAlreadyQuoted} must increment by 1.
   */
  @Test
  void onCommand_quotedSlot_silentlyDropped() {
    // Acquire a REQUESTED slot and transition it to QUOTED manually.
    final var slot = acquireRequestedSlot(QUOTE_REQ_ID, ProductTypeEnum.Spot);
    transitionToQuoted(slot, "QTE-00000000001", TIMESTAMP + TTL_SPOT_NANOS);

    // Now dispatch a second PriceResponse for the same quoteReqId.
    final var buf = new ExpandableArrayBuffer(512);
    final int len = encodePriceResponse(buf, QUOTE_REQ_ID, true, BID_PX, OFFER_PX);

    dispatch(buf, len);

    assertEquals(0, session.messages.size(), "no event must be emitted for a QUOTED slot");
    assertEquals(1L, metrics.dropAlreadyQuoted, "dropAlreadyQuoted counter must be 1");
    assertEquals(0L, metrics.emitCreated);
    assertEquals(0L, metrics.emitRejected);
  }

  // =========================================================================
  // §5 — ACCEPTED slot → silently dropped (terminal state)
  // =========================================================================

  /**
   * A PriceResponse for a slot in ACCEPTED state must be silently dropped. ACCEPTED is a terminal
   * state: the NOS was already processed. {@code metrics.dropTerminal} must increment by 1.
   */
  @Test
  void onCommand_acceptedSlot_silentlyDropped() {
    // Acquire REQUESTED → transition to QUOTED → then commit-accept to reach ACCEPTED/FREE.
    // Since commitAccept immediately releases the slot, we need to verify the slot is gone
    // (lookupByQuoteReqId returns null) and then a second dispatch sees dropUnknownReqId.
    //
    // The ACCEPTED state is transient — it is set and immediately freed in commitAccept. To test
    // the "slot in a non-REQUESTED, non-QUOTED state" guard (which catches EXPIRED, REJECTED, and
    // any stale transition), we instead manually force a REQUESTED slot to a state other than
    // REQUESTED/QUOTED and verify the dropTerminal counter path fires.
    //
    // We reuse a slot that we manually set to state=ACCEPTED before it is looked up by the handler.
    final var slot = acquireRequestedSlot(QUOTE_REQ_ID, ProductTypeEnum.Spot);
    // Force state to ACCEPTED (terminal — non-REQUESTED, non-QUOTED).
    slot.state = RfqSlotState.ACCEPTED;

    final var buf = new ExpandableArrayBuffer(512);
    final int len = encodePriceResponse(buf, QUOTE_REQ_ID, true, BID_PX, OFFER_PX);

    dispatch(buf, len);

    assertEquals(0, session.messages.size(), "no event must be emitted for terminal slot");
    assertEquals(1L, metrics.dropTerminal, "dropTerminal counter must be 1");
    assertEquals(0L, metrics.emitCreated);
    assertEquals(0L, metrics.emitRejected);
  }

  // =========================================================================
  // §6 — scheduleTimer returns false → emit 106, release slot (no QTE advance)
  // =========================================================================

  /**
   * When {@link Cluster#scheduleTimer} returns {@code false} (Aeron timer pool exhausted), the
   * handler must:
   *
   * <ul>
   *   <li>Emit template 106 (QuoteRejectedEvent) with reason=TooLateToEnter.
   *   <li>Release the slot (no QUOTED transition).
   *   <li>Increment {@code metrics.rejectTimerExhausted = 1} and {@code metrics.emitRejected = 1}.
   *   <li>NOT advance the QTE counter ({@link IdGenerator#currentCounter()} remains at 0 — the
   *       rollback contract: quoteIdGen.nextInto is only called AFTER scheduleTimer succeeds).
   * </ul>
   */
  @Test
  void onCommand_scheduleTimerReturnsFalse_emits106AndReleasesSlot() {
    capturingCluster.scheduleTimerResult = false;
    handler.setCluster(capturingCluster);

    final long counterBefore = quoteIdGen.currentCounter();
    final var slot = acquireRequestedSlot(QUOTE_REQ_ID, ProductTypeEnum.Spot);

    final var buf = new ExpandableArrayBuffer(512);
    final int len = encodePriceResponse(buf, QUOTE_REQ_ID, true, BID_PX, OFFER_PX);

    dispatch(buf, len);

    assertEquals(1, session.messages.size(), "exactly one 106 must be emitted on timer failure");
    final var dec = decodeQuoteRejected(session.messages.get(0));
    assertEquals(
        QuoteRejectReasonEnum.TooLateToEnter,
        dec.quoteRejectReason(),
        "reject reason must be TooLateToEnter (timer pool exhausted)");
    assertEquals(1L, metrics.rejectTimerExhausted, "rejectTimerExhausted counter must be 1");
    assertEquals(1L, metrics.emitRejected, "emitRejected counter must be 1");
    assertEquals(
        counterBefore,
        quoteIdGen.currentCounter(),
        "QTE counter must NOT advance on timer-exhausted rollback");
    // release() sets slot.state = FREE.
    assertEquals(RfqSlotState.FREE, slot.state, "slot must be released back to FREE");
  }

  // =========================================================================
  // §7 — QTE counter advances exactly once on successful 105 emission
  // =========================================================================

  /**
   * A successful PriceResponse acceptance must advance {@link IdGenerator#currentCounter()} by
   * exactly 1. This verifies the quoteId minting contract.
   */
  @Test
  void onCommand_idGeneratorAdvancesOnSuccess() {
    handler.setCluster(capturingCluster);

    final long counterBefore = quoteIdGen.currentCounter();
    acquireRequestedSlot(QUOTE_REQ_ID, ProductTypeEnum.Spot);

    final var buf = new ExpandableArrayBuffer(512);
    final int len = encodePriceResponse(buf, QUOTE_REQ_ID, true, BID_PX, OFFER_PX);

    dispatch(buf, len);

    assertEquals(
        counterBefore + 1,
        quoteIdGen.currentCounter(),
        "QTE counter must increment by exactly 1 on successful 105 emission");
  }

  // =========================================================================
  // §8 — validUntil = clusterTs + ttlSpotNanos (Spot product)
  // =========================================================================

  /**
   * After a successful PriceResponse for a Spot slot, {@code slot.validUntil} must equal {@code
   * TIMESTAMP + TTL_SPOT_NANOS}.
   */
  @Test
  void onCommand_validUntilEqualsTimestampPlusProductTtlSpot() {
    handler.setCluster(capturingCluster);
    final var slot = acquireRequestedSlot(QUOTE_REQ_ID, ProductTypeEnum.Spot);

    final var buf = new ExpandableArrayBuffer(512);
    final int len = encodePriceResponse(buf, QUOTE_REQ_ID, true, BID_PX, OFFER_PX);

    dispatch(buf, len);

    assertEquals(
        TIMESTAMP + TTL_SPOT_NANOS,
        slot.validUntil,
        "validUntil must equal clusterTs + ttlSpotNanos");
  }

  // =========================================================================
  // §9 — validUntil = clusterTs + ttlForwardNanos (Forward product)
  // =========================================================================

  /**
   * After a successful PriceResponse for a Forward slot, {@code slot.validUntil} must equal {@code
   * TIMESTAMP + TTL_FORWARD_NANOS}.
   */
  @Test
  void onCommand_validUntilEqualsTimestampPlusProductTtlForward() {
    handler.setCluster(capturingCluster);
    final var slot = acquireRequestedSlot(QUOTE_REQ_ID, ProductTypeEnum.Forward);

    final var buf = new ExpandableArrayBuffer(512);
    final int len = encodePriceResponse(buf, QUOTE_REQ_ID, true, BID_PX, OFFER_PX);

    dispatch(buf, len);

    assertEquals(
        TIMESTAMP + TTL_FORWARD_NANOS,
        slot.validUntil,
        "validUntil must equal clusterTs + ttlForwardNanos");
  }

  // =========================================================================
  // §10 — validUntil = clusterTs + ttlSwapNanos (Swap product)
  // =========================================================================

  /**
   * After a successful PriceResponse for a Swap slot, {@code slot.validUntil} must equal {@code
   * TIMESTAMP + TTL_SWAP_NANOS}.
   */
  @Test
  void onCommand_validUntilEqualsTimestampPlusProductTtlSwap() {
    handler.setCluster(capturingCluster);
    final var slot = acquireRequestedSlot(QUOTE_REQ_ID, ProductTypeEnum.Swap);

    final var buf = new ExpandableArrayBuffer(512);
    final int len = encodePriceResponse(buf, QUOTE_REQ_ID, true, BID_PX, OFFER_PX);

    dispatch(buf, len);

    assertEquals(
        TIMESTAMP + TTL_SWAP_NANOS,
        slot.validUntil,
        "validUntil must equal clusterTs + ttlSwapNanos");
  }

  // =========================================================================
  // §11 — Malformed message (length below block length) → silently dropped
  // =========================================================================

  /**
   * A PriceResponse whose total length is less than {@code HDR_LEN +
   * PriceResponseDecoder.BLOCK_LENGTH} must be silently dropped. {@code
   * metrics.dropMalformedPriceResponse} must increment by 1 and no event must be emitted.
   */
  @Test
  void onCommand_lengthBelowBlockLength_silentlyDroppedWithMetric() {
    // Encode a minimal header; supply length = 1 (far below HDR_LEN + BLOCK_LENGTH).
    final var buf = new UnsafeBuffer(new byte[HDR_LEN]);
    final var hdr = new MessageHeaderEncoder();
    hdr.wrap(buf, 0)
        .blockLength(PriceResponseDecoder.BLOCK_LENGTH)
        .templateId(PriceResponseDecoder.TEMPLATE_ID)
        .schemaId(1)
        .version(1);

    dispatchRaw(buf, 1);

    assertEquals(0, session.messages.size(), "no event must be emitted for malformed message");
    assertEquals(
        1L, metrics.dropMalformedPriceResponse, "dropMalformedPriceResponse counter must be 1");
    assertEquals(0L, metrics.emitCreated);
    assertEquals(0L, metrics.emitRejected);
  }

  // =========================================================================
  // §12 — commandTemplateId() sanity check
  // =========================================================================

  /**
   * Verifies that {@link PriceResponseHandler#commandTemplateId()} returns {@link
   * PriceResponseDecoder#TEMPLATE_ID}. This ensures the handler is correctly self-describing for
   * the dispatch table.
   */
  @Test
  void commandTemplateId_returnsPriceResponseTemplateId() {
    assertEquals(
        PriceResponseDecoder.TEMPLATE_ID,
        handler.commandTemplateId(),
        "handler must declare the PriceResponse template ID (51)");
  }

  // =========================================================================
  // Helpers
  // =========================================================================

  /**
   * Constructs an {@link RfqStateMachine} with distinct per-product TTL values so that validUntil
   * assertions are unambiguous per product type.
   */
  private RfqStateMachine buildStateMachine() {
    return new RfqStateMachine(
        TradingClusteredServiceFactory.DEFAULT_RFQ_POOL_CAPACITY,
        TTL_SPOT_NANOS, // defaultTtlNanos (same as Spot here)
        TTL_SPOT_NANOS,
        TTL_FORWARD_NANOS,
        TTL_SWAP_NANOS,
        TradingClusteredServiceFactory.DEFAULT_RFQ_REQUEST_TIMEOUT_NANOS,
        TradingClusteredServiceFactory.DEFAULT_RFQ_RATE_LIMIT_PER_SESSION,
        TradingClusteredServiceFactory.DEFAULT_RFQ_RATE_LIMIT_WINDOW_NANOS,
        TradingClusteredServiceFactory.DEFAULT_RFQ_ACCEPT_PRICE_TOLERANCE_BPS,
        TradingClusteredServiceFactory.DEFAULT_RFQ_ACCEPT_QTY_TOLERANCE_BPS,
        accountStore,
        metrics);
  }

  /**
   * Acquires a slot for {@code quoteReqId} with the given product type, populates mandatory enum
   * fields, calls {@link RfqSlot#syncQuoteReqIdKey()}, and registers it via {@link
   * RfqStateMachine#registerRequested(RfqSlot)}.
   *
   * <p>Enum fields (side, tenor) are set to valid non-zero defaults so that {@code SideEnum.get()}
   * and {@code TenorEnum.get()} do not throw in the encoder paths.
   *
   * @param quoteReqId the ASCII quoteReqId (up to 20 chars, NUL-padded)
   * @param productType the product type to set on the slot
   * @return the REQUESTED slot
   */
  private RfqSlot acquireRequestedSlot(final String quoteReqId, final ProductTypeEnum productType) {
    final var slot = stateMachine.acquire();
    assertNotNull(slot, "RFQ slot pool must not be exhausted during test setup");

    // Write quoteReqId bytes into the slot.
    writeFixedBytes(quoteReqId, slot.quoteReqIdBytes, RfqSlot.QUOTE_REQ_ID_LENGTH);

    // Set mandatory enum fields to valid non-zero values.
    slot.side = (byte) SideEnum.Buy.value();
    slot.productType = (byte) productType.value();
    slot.tenor = (byte) TenorEnum.ON.value();
    slot.transactTime = TIMESTAMP;

    slot.syncQuoteReqIdKey();
    stateMachine.registerRequested(slot);
    return slot;
  }

  /**
   * Manually transitions a slot from REQUESTED to QUOTED for idempotent-drop tests. Populates the
   * quoteId bytes, sets validUntil and timerCorrelationId, updates state, and calls {@link
   * RfqStateMachine#registerQuoted(RfqSlot)}.
   *
   * @param slot the slot to transition (must be in REQUESTED state)
   * @param quoteId the ASCII quoteId (up to 20 chars, NUL-padded)
   * @param validUntil the TTL deadline epoch nanos
   */
  private void transitionToQuoted(final RfqSlot slot, final String quoteId, final long validUntil) {
    writeFixedBytes(quoteId, slot.quoteIdBytes, RfqSlot.QUOTE_ID_LENGTH);
    slot.timerCorrelationId = 0xCAFEBABEL;
    slot.validUntil = validUntil;
    slot.state = RfqSlotState.QUOTED;
    slot.syncQuoteIdKey();
    stateMachine.registerQuoted(slot);
  }

  /**
   * Encodes a {@code PriceResponse} (template 51) into {@code dst} using {@link
   * SbeTestEncoder#encodePriceResponse}. Sets {@code productType=Spot} (the helper default).
   *
   * @param dst destination buffer
   * @param quoteReqId quoteReqId to embed (tag 131)
   * @param accepted pricing decision
   * @param bidPx bid price (fixed-point 10^-8; ignored when accepted=false)
   * @param offerPx offer price (fixed-point 10^-8; ignored when accepted=false)
   * @return total encoded length including SBE header
   */
  private static int encodePriceResponse(
      final ExpandableArrayBuffer dst,
      final String quoteReqId,
      final boolean accepted,
      final long bidPx,
      final long offerPx) {
    return SbeTestEncoder.encodePriceResponse(
        dst, 0, quoteReqId, SYMBOL, accepted, bidPx, offerPx, TIMESTAMP);
  }

  /**
   * Dispatches a command buffer through the handler, re-parsing the SBE header to extract
   * blockLength and version (mirrors the {@code QuoteRequestHandlerTest} pattern).
   *
   * @param buf the encoded message buffer
   * @param len total message length
   */
  private void dispatch(final ExpandableArrayBuffer buf, final int len) {
    final var hdrDec = new MessageHeaderDecoder();
    hdrDec.wrap(buf, 0);
    final int blockLength = hdrDec.blockLength();
    final int version = hdrDec.version();

    handler.onCommand(session, TIMESTAMP, buf, 0, len, blockLength, version, eventSink);
  }

  /**
   * Dispatches a raw {@link UnsafeBuffer} without re-parsing the header. Used for malformed-message
   * tests where the declared length is intentionally below the minimum.
   *
   * @param buf raw buffer (may be under-filled)
   * @param len the (intentionally short) declared length
   */
  private void dispatchRaw(final UnsafeBuffer buf, final int len) {
    handler.onCommand(
        session, TIMESTAMP, buf, 0, len, PriceResponseDecoder.BLOCK_LENGTH, 1, eventSink);
  }

  /**
   * Reads the templateId from the message header of a captured byte[].
   *
   * @param msg the captured message bytes
   * @return the SBE templateId
   */
  private static int templateId(final byte[] msg) {
    final var hdr = new MessageHeaderDecoder();
    hdr.wrap(new UnsafeBuffer(msg), 0);
    return hdr.templateId();
  }

  /**
   * Decodes a {@link QuoteCreatedEventDecoder} from a captured byte[].
   *
   * @param msg the captured message bytes
   * @return the decoder wrapped over the message body
   */
  private static QuoteCreatedEventDecoder decodeQuoteCreated(final byte[] msg) {
    final var buf = new UnsafeBuffer(msg);
    final var hdr = new MessageHeaderDecoder();
    hdr.wrap(buf, 0);
    final var dec = new QuoteCreatedEventDecoder();
    dec.wrap(buf, HDR_LEN, hdr.blockLength(), hdr.version());
    return dec;
  }

  /**
   * Decodes a {@link QuoteRejectedEventDecoder} from a captured byte[].
   *
   * @param msg the captured message bytes
   * @return the decoder wrapped over the message body
   */
  private static QuoteRejectedEventDecoder decodeQuoteRejected(final byte[] msg) {
    final var buf = new UnsafeBuffer(msg);
    final var hdr = new MessageHeaderDecoder();
    hdr.wrap(buf, 0);
    final var dec = new QuoteRejectedEventDecoder();
    dec.wrap(buf, HDR_LEN, hdr.blockLength(), hdr.version());
    return dec;
  }

  /**
   * Writes ASCII bytes from {@code text} into {@code dst}, NUL-padding to {@code fixedLen}. Does
   * not exceed the fixed length — longer strings are silently truncated.
   *
   * @param text ASCII source string
   * @param dst pre-allocated destination byte array (length >= fixedLen)
   * @param fixedLen number of bytes to fill
   */
  private static void writeFixedBytes(final String text, final byte[] dst, final int fixedLen) {
    final byte[] src = text.getBytes(StandardCharsets.US_ASCII);
    final int copyLen = Math.min(src.length, fixedLen);
    System.arraycopy(src, 0, dst, 0, copyLen);
    // NUL-pad remaining.
    for (int i = copyLen; i < fixedLen; i++) {
      dst[i] = 0;
    }
  }

  /**
   * Seeds an {@link AccountStore} with a minimal {@link AccountState} for the given numeric ID and
   * code string.
   *
   * @param store the account store to seed
   * @param accountId the numeric account ID
   * @param code the ASCII account code string
   */
  private static void seedAccount(
      final AccountStore store, final long accountId, final String code) {
    final var state = new AccountState();
    state.setAccountId(accountId);
    final byte[] codeBytes = code.getBytes(StandardCharsets.US_ASCII);
    state.setAccountCode(codeBytes, 0, codeBytes.length);
    state.setCapabilities(AccountState.Capabilities.CAN_TRADE | AccountState.Capabilities.CAN_RFQ);
    store.put(state);
  }
}
