package com.trading.engine.orchestrator;

import static com.trading.engine.orchestrator.OrchestratorConstants.ENCODING_BUFFER_SIZE;
import static com.trading.engine.orchestrator.OrchestratorConstants.GATEWAY_POLL_LIMIT;
import static com.trading.engine.orchestrator.OrchestratorConstants.MAX_PUBLICATION_RETRIES;
import static com.trading.engine.orchestrator.OrchestratorConstants.PRICING_POLL_LIMIT;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.trading.engine.messages.sbe.BooleanType;
import com.trading.engine.messages.sbe.NewOrderSingleDecoder;
import com.trading.engine.messages.sbe.PriceResponseDecoder;
import com.trading.engine.messages.sbe.PriceValidationResponseDecoder;
import com.trading.engine.messages.sbe.QuoteRejectReasonEnum;
import com.trading.engine.messages.sbe.QuoteRequestDecoder;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.orchestrator.codec.GatewayMessageDispatcher;
import com.trading.engine.orchestrator.codec.OrchestratorMessageEncoder;
import com.trading.engine.orchestrator.codec.PricingResponseDispatcher;
import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import java.util.Arrays;
import java.util.Objects;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Orchestrator agent bridging the gateway and pricing service for RFQ-to-execution workflow. Sits
 * between the gateway (Aeron IPC streams 100/101) and the pricing service (streams 200/201),
 * managing the RFQ state machine and coordinating quote pricing and price validation.
 *
 * <h3>Duty cycle</h3>
 *
 * <p>On each {@link #doWork()} invocation:
 *
 * <ol>
 *   <li>Polls pricing responses FIRST (latency-sensitive Quote → client path).
 *   <li>Polls gateway inbound (QuoteRequests, NewOrderSingle).
 *   <li>Incrementally sweeps the RFQ pool for expired entries (bounded, every 1 second).
 *   <li>Logs diagnostic counters (1 line per second, inside the sweep branch).
 * </ol>
 *
 * <h3>Back-pressure handling</h3>
 *
 * <p>Uses the <b>publish-before-mutate</b> pattern: outbound publications are attempted BEFORE
 * state machine mutations. If publication fails ({@link Action#ABORT}), the state machine is NOT
 * mutated, so Aeron re-delivery retries the entire operation idempotently. Re-delivery is detected
 * via {@link RfqStateMachine#findByQuoteReqId} and {@link RfqStateMachine#findByQuoteId}.
 *
 * <h3>Threading</h3>
 *
 * <p><b>Not thread-safe.</b> Runs on a single Aeron {@link org.agrona.concurrent.AgentRunner}
 * thread named "orchestrator".
 *
 * <h3>Allocation</h3>
 *
 * <p><b>Zero allocation after construction.</b> All flyweights, scratch buffers, dispatchers, and
 * the {@link RfqStateMachine.ReapCallback} are pre-allocated.
 *
 * @see RfqStateMachine
 * @see OrchestratorMessageEncoder
 * @see GatewayMessageDispatcher
 * @see PricingResponseDispatcher
 */
public final class OrchestratorService
    implements Agent,
        GatewayMessageDispatcher.GatewayMessageHandler,
        PricingResponseDispatcher.PricingResponseHandler {

  private static final Log LOG = LogFactory.getLog(OrchestratorService.class);

  // --- Pre-allocated reject text constants (ASCII byte arrays, explicit charset) ---
  private static final byte[] TEXT_POOL_EXHAUSTED =
      "RFQ pool exhausted".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
  private static final byte[] TEXT_DUPLICATE_QUOTE_REQ_ID =
      "Duplicate quoteReqId".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
  private static final byte[] TEXT_UNKNOWN_QUOTE_ID =
      "Unknown or expired quoteId".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
  private static final byte[] TEXT_NOS_TOO_LARGE =
      "Internal: NOS too large".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
  private static final byte[] TEXT_RFQ_EXPIRED =
      "RFQ expired".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
  private static final byte[] TEXT_PRICING_REJECTED =
      "Pricing service declined quote".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
  private static final byte[] TEXT_NULL_PRICES =
      "Internal: accepted PriceResponse with null prices"
          .getBytes(java.nio.charset.StandardCharsets.US_ASCII);
  private static final byte[] TEXT_INVALID_SYMBOL =
      "Empty symbol".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
  private static final byte[] TEXT_INVALID_QTY =
      "Non-positive orderQty".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
  private static final byte[] TEXT_INVALID_SIDE =
      "Invalid side".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
  private static final byte[] TEXT_INVALID_ACCOUNT =
      "Empty accountCode".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
  private static final byte[] TEXT_VALIDATION_FAILED =
      "Price validation failed".getBytes(java.nio.charset.StandardCharsets.US_ASCII);

  // --- Injected dependencies (all final, all non-null) ---
  private final Subscription gatewaySubscription;
  private final ExclusivePublication gatewayPublication;
  private final Subscription pricingSubscription;
  private final ExclusivePublication pricingPublication;
  private final RfqStateMachine stateMachine;
  private final OrchestratorIdGenerator quoteIdGenerator;
  private final OrchestratorMessageEncoder encoder;
  private final NanoClock nanoClock;
  private final EpochNanoClock epochClock;
  private final long sweepIntervalNanos;

  // --- Pre-allocated state ---
  private final UnsafeBuffer encodingBuffer = new UnsafeBuffer(new byte[ENCODING_BUFFER_SIZE]);
  private final GatewayMessageDispatcher gatewayDispatcher;
  private final PricingResponseDispatcher pricingDispatcher;
  private final byte[] quoteIdScratch = new byte[RfqState.QUOTE_ID_LENGTH];
  private final byte[] quoteReqIdScratch = new byte[RfqState.QUOTE_REQ_ID_LENGTH];

  /**
   * Pre-allocated scratch buffer for reading the stashed NOS bytes BEFORE the state machine
   * releases the pool slot (which zeros the flat buffer). Used in {@link
   * #onPriceValidationResponse} valid path to preserve the NOS for APP-31 cluster forwarding.
   */
  private final byte[] nosScratch = new byte[OrchestratorConstants.NOS_STASH_BUFFER_SIZE];

  /** Length of valid NOS bytes in {@link #nosScratch}, set before {@code onValidationValid()}. */
  private int nosScratchLength;

  // Pre-allocated scratch arrays for reject ExecutionReport encoding — avoids hot-path allocation
  private final byte[] clOrdIdScratch =
      new byte[RfqState.QUOTE_REQ_ID_LENGTH]; // ClOrdID (tag 11) = 20 bytes
  private final byte[] symbolScratch = new byte[RfqState.SYMBOL_LENGTH];
  private final byte[] settlDateScratch = new byte[RfqState.SETTL_DATE_LENGTH];
  private final byte[] currencyScratch = new byte[RfqState.CURRENCY_LENGTH];
  private final byte[] settlCurrencyScratch = new byte[RfqState.SETTL_CURRENCY_LENGTH];

  /**
   * Pre-allocated UnsafeBuffer view over {@link #nosScratch} for decoding the stashed NOS to
   * extract ClOrdID (tag 11) for reject ExecutionReports. Zero allocation — wraps the existing
   * scratch.
   */
  private final UnsafeBuffer stashedNosView = new UnsafeBuffer(nosScratch);

  /**
   * Pre-allocated SBE header decoder for decoding the stashed NOS header. Used to extract
   * blockLength/version before wrapping the NOS body decoder.
   */
  private final com.trading.engine.messages.sbe.MessageHeaderDecoder stashedHeaderDecoder =
      new com.trading.engine.messages.sbe.MessageHeaderDecoder();

  /**
   * Pre-allocated SBE NOS decoder for extracting ClOrdID (tag 11) from the stashed NOS fragment.
   * Zero allocation on the hot path — pre-allocated, re-wrapped on each use.
   */
  private final NewOrderSingleDecoder stashedNosDecoder = new NewOrderSingleDecoder();

  /**
   * Pre-allocated reap callback. Captured ONCE at construction to avoid per-doWork() lambda
   * allocation (this::onRfqExpired would allocate on every call).
   */
  private final RfqStateMachine.ReapCallback reapCallback;

  /**
   * Validation failure reason set by {@link #validateQuoteRequest}. Read by the caller after
   * validation returns a non-null text. Avoids allocating a pair/record on the hot path.
   */
  private QuoteRejectReasonEnum validationFailureReason;

  private long lastSweepNanos;

  // --- Diagnostic counters (logged every SWEEP_INTERVAL_NANOS) ---
  private long poolFullRejects;
  private long reapExpiredCount;
  private long publicationFailures;
  private long backPressureAborts;

  /**
   * Constructs the orchestrator service with all dependencies.
   *
   * @param gatewaySubscription inbound from gateway (stream 100)
   * @param gatewayPublication outbound to gateway (stream 101)
   * @param pricingSubscription inbound from pricing (stream 201)
   * @param pricingPublication outbound to pricing (stream 200)
   * @param stateMachine the RFQ state machine and pool
   * @param quoteIdGenerator the quote ID generator (prefix "QTE")
   * @param encoder the SBE message encoder
   * @param nanoClock monotonic clock for timeouts
   * @param epochClock wall clock for transactTime on outbound messages
   * @param sweepIntervalNanos interval between timeout sweeps
   */
  public OrchestratorService(
      final Subscription gatewaySubscription,
      final ExclusivePublication gatewayPublication,
      final Subscription pricingSubscription,
      final ExclusivePublication pricingPublication,
      final RfqStateMachine stateMachine,
      final OrchestratorIdGenerator quoteIdGenerator,
      final OrchestratorMessageEncoder encoder,
      final NanoClock nanoClock,
      final EpochNanoClock epochClock,
      final long sweepIntervalNanos) {

    this.gatewaySubscription = Objects.requireNonNull(gatewaySubscription, "gatewaySubscription");
    this.gatewayPublication = Objects.requireNonNull(gatewayPublication, "gatewayPublication");
    this.pricingSubscription = Objects.requireNonNull(pricingSubscription, "pricingSubscription");
    this.pricingPublication = Objects.requireNonNull(pricingPublication, "pricingPublication");
    this.stateMachine = Objects.requireNonNull(stateMachine, "stateMachine");
    this.quoteIdGenerator = Objects.requireNonNull(quoteIdGenerator, "quoteIdGenerator");
    this.encoder = Objects.requireNonNull(encoder, "encoder");
    this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
    this.epochClock = Objects.requireNonNull(epochClock, "epochClock");
    this.sweepIntervalNanos = sweepIntervalNanos;

    this.gatewayDispatcher = new GatewayMessageDispatcher(this);
    this.pricingDispatcher = new PricingResponseDispatcher(this);
    this.reapCallback = this::onRfqExpired;
  }

  // ===========================================================================
  // Agent lifecycle
  // ===========================================================================

  /** {@inheritDoc} Initialises the sweep timer and logs startup configuration. */
  @Override
  public void onStart() {
    lastSweepNanos = nanoClock.nanoTime();
    LOG.info()
        .append("Orchestrator started: pool=")
        .append(stateMachine.capacity())
        .append(" gwReqStream=")
        .append(OrchestratorConstants.GATEWAY_REQUEST_STREAM_ID)
        .append(" gwRespStream=")
        .append(OrchestratorConstants.GATEWAY_RESPONSE_STREAM_ID)
        .append(" pxReqStream=")
        .append(OrchestratorConstants.PRICING_REQUEST_STREAM_ID)
        .append(" pxRespStream=")
        .append(OrchestratorConstants.PRICING_RESPONSE_STREAM_ID)
        .commit();
  }

  /**
   * {@inheritDoc} Polls pricing responses, then gateway inbound, then runs an incremental timeout
   * sweep if the sweep interval has elapsed.
   *
   * @return the total number of fragments polled plus expired RFQs reaped
   */
  @Override
  public int doWork() {
    int workCount = 0;

    // 1. Poll pricing responses FIRST — lower latency for Quote → client path
    workCount += pricingSubscription.controlledPoll(pricingDispatcher, PRICING_POLL_LIMIT);

    // 2. Poll gateway inbound
    workCount += gatewaySubscription.controlledPoll(gatewayDispatcher, GATEWAY_POLL_LIMIT);

    // 3. Incremental timeout sweep + counter logging (bounded, every SWEEP_INTERVAL_NANOS)
    final long nowNanos = nanoClock.nanoTime();
    if (nowNanos - lastSweepNanos >= sweepIntervalNanos) {
      workCount += stateMachine.reapExpired(nowNanos, reapCallback);
      lastSweepNanos = nowNanos;

      // Diagnostic counter logging — 1 GFLog line per second
      LOG.info()
          .append("orch stats: active=")
          .append(stateMachine.activeCount())
          .append(" poolFull=")
          .append(poolFullRejects)
          .append(" expired=")
          .append(reapExpiredCount)
          .append(" pubFail=")
          .append(publicationFailures)
          .append(" bpAbort=")
          .append(backPressureAborts)
          .commit();
    }

    return workCount;
  }

  /**
   * {@inheritDoc} Drains all active RFQs via a full reap sweep, then closes Aeron subscriptions and
   * publications via {@link CloseHelper#closeAll}.
   */
  @Override
  public void onClose() {
    final int active = stateMachine.activeCount();
    LOG.info()
        .append("Orchestrator shutting down, draining ")
        .append(active)
        .append(" active RFQs")
        .commit();

    // Full reap: transition ALL active RFQs to EXPIRED, publish notifications
    stateMachine.reapAll(nanoClock.nanoTime(), reapCallback);

    // Close subscriptions first (stop ingest), then publications (stop outbound)
    CloseHelper.closeAll(
        gatewaySubscription, pricingSubscription, gatewayPublication, pricingPublication);

    LOG.info().append("Orchestrator shutdown complete").commit();
  }

  /**
   * {@inheritDoc}
   *
   * @return {@code "orchestrator"}
   */
  @Override
  public String roleName() {
    return "orchestrator";
  }

  // ===========================================================================
  // Gateway message handlers
  // ===========================================================================

  /**
   * Handles an inbound QuoteRequest (tag 35=R) from the gateway. Validates fields, checks for
   * duplicate/re-delivery, acquires a pool slot, and publishes a PriceRequest to the pricing
   * service.
   *
   * @param decoder pre-wrapped QuoteRequest SBE decoder
   * @param buffer the underlying fragment buffer
   * @param offset start offset of the fragment
   * @param length fragment length in bytes
   * @return {@link Action#CONTINUE} on success, {@link Action#ABORT} on transient back-pressure
   */
  @Override
  public Action onQuoteRequest(
      final QuoteRequestDecoder decoder,
      final DirectBuffer buffer,
      final int offset,
      final int length) {

    // 1. Validate inbound QuoteRequest — sets validationFailureText if invalid
    final var failText = validateQuoteRequest(decoder);
    if (failText != null) {
      final int len =
          encoder.encodeQuoteRequestReject(
              encodingBuffer,
              0,
              decoder,
              validationFailureReason,
              failText,
              failText.length,
              epochClock.nanoTime());
      final var gwResult = offerToGateway(len);
      if (gwResult != Action.CONTINUE) {
        return gwResult;
      }
      return Action.CONTINUE;
    }

    // 2. Extract quoteReqId + collision check
    decoder.getQuoteReqId(quoteReqIdScratch, 0);
    final var existing =
        stateMachine.findByQuoteReqId(quoteReqIdScratch, 0, RfqState.QUOTE_REQ_ID_LENGTH);

    if (existing != null && existing.isActive()) {
      if (existing.state() == RfqState.State.PENDING_PRICE) {
        // Re-delivery (ABORT retry): retry PriceRequest publication only
        final int prLen = encoder.encodePriceRequest(encodingBuffer, 0, existing);
        return offerToPricingOrAbort(prLen);
      }
      // Collision: different client, same quoteReqId
      LOG.warn()
          .append("QuoteReqId collision: existing state=")
          .append(existing.state().name())
          .append(" poolIndex=")
          .append(existing.poolIndex())
          .commit();
      final int len =
          encoder.encodeQuoteRequestReject(
              encodingBuffer,
              0,
              decoder,
              QuoteRejectReasonEnum.Other,
              TEXT_DUPLICATE_QUOTE_REQ_ID,
              TEXT_DUPLICATE_QUOTE_REQ_ID.length,
              epochClock.nanoTime());
      final var gwResult = offerToGateway(len);
      if (gwResult != Action.CONTINUE) {
        return gwResult;
      }
      return Action.CONTINUE;
    }

    // 3. Acquire pool slot FIRST — reject early if pool is exhausted (no orphan PriceRequest).
    // On ABORT re-delivery, the duplicate check at step 2 will find the existing RFQ in
    // PENDING_PRICE and retry the PriceRequest publication, so mutate-first is safe here.
    final long nowNanos = nanoClock.nanoTime();
    final var rfq = stateMachine.onQuoteRequest(decoder, nowNanos);
    if (rfq == null) {
      poolFullRejects++;
      final int len =
          encoder.encodeQuoteRequestReject(
              encodingBuffer,
              0,
              decoder,
              QuoteRejectReasonEnum.Other,
              TEXT_POOL_EXHAUSTED,
              TEXT_POOL_EXHAUSTED.length,
              epochClock.nanoTime());
      final var gwResult = offerToGateway(len);
      if (gwResult != Action.CONTINUE) {
        return gwResult;
      }
      return Action.CONTINUE;
    }

    // 4. Encode PriceRequest from the acquired RfqState and publish. On ABORT, the re-delivered
    // fragment hits the PENDING_PRICE re-delivery path at step 2, which retries publication.
    final int prLen = encoder.encodePriceRequest(encodingBuffer, 0, rfq);
    return offerToPricingOrAbort(prLen);
  }

  /**
   * Handles an inbound NewOrderSingle (tag 35=D) from the gateway. Routes to cluster bypass (no
   * quoteId), re-delivery retry (PENDING_VALIDATION), or validates the quote, stashes the NOS, and
   * publishes a PriceValidationRequest to the pricing service.
   *
   * @param decoder pre-wrapped NewOrderSingle SBE decoder
   * @param buffer the underlying fragment buffer (retained for NOS stash)
   * @param offset start offset of the fragment
   * @param length fragment length in bytes
   * @return {@link Action#CONTINUE} on success, {@link Action#ABORT} on transient back-pressure
   */
  @Override
  public Action onNewOrderSingle(
      final NewOrderSingleDecoder decoder,
      final DirectBuffer buffer,
      final int offset,
      final int length) {

    // 1. Extract quoteId; check if empty (all 0x00 = direct order bypass)
    decoder.getQuoteId(quoteIdScratch, 0);
    if (isAllZero(quoteIdScratch, RfqState.QUOTE_ID_LENGTH)) {
      // TODO(APP-31): forward NOS directly to cluster
      LOG.info().append("NOS bypass (no quoteId): forwarding to cluster stub").commit();
      return Action.CONTINUE;
    }

    // 2. Lookup by quoteId for re-delivery or new transition
    final var rfq = stateMachine.findByQuoteId(quoteIdScratch, 0, RfqState.QUOTE_ID_LENGTH);

    if (rfq != null && rfq.state() == RfqState.State.PENDING_VALIDATION) {
      // Re-delivery: retry PriceValidationRequest publication
      final int pvLen =
          encoder.encodePriceValidationRequest(
              encodingBuffer, 0, rfq, decoder, epochClock.nanoTime());
      return offerToPricingOrAbort(pvLen);
    }

    if (rfq == null || rfq.state() != RfqState.State.QUOTED) {
      // Unknown or wrong state → reject (zero-alloc: uses pre-allocated scratch arrays)
      decoder.getClOrdId(clOrdIdScratch, 0);
      Arrays.fill(symbolScratch, (byte) 0);
      Arrays.fill(settlDateScratch, (byte) 0);
      Arrays.fill(currencyScratch, (byte) 0);
      Arrays.fill(settlCurrencyScratch, (byte) 0);
      final int len =
          encoder.encodeRejectExecutionReport(
              encodingBuffer,
              0,
              clOrdIdScratch,
              0,
              quoteIdScratch,
              0,
              symbolScratch,
              0,
              (byte) SideEnum.NULL_VAL.value(),
              TEXT_UNKNOWN_QUOTE_ID,
              TEXT_UNKNOWN_QUOTE_ID.length,
              epochClock.nanoTime(),
              (byte) 0,
              settlDateScratch,
              0,
              (byte) 0,
              currencyScratch,
              0,
              settlCurrencyScratch,
              0,
              (byte) 0);
      final var gwResult = offerToGateway(len);
      if (gwResult != Action.CONTINUE) {
        return gwResult;
      }
      return Action.CONTINUE;
    }

    // 3. Stash NOS and transition state FIRST (QUOTED → PENDING_VALIDATION). On ABORT
    // re-delivery, the PENDING_VALIDATION re-delivery path at step 2 retries publication.
    final long nowNanos = nanoClock.nanoTime();
    final var transitioned =
        stateMachine.onNewOrderSingleWithQuote(
            quoteIdScratch, 0, RfqState.QUOTE_ID_LENGTH, buffer, offset, length, nowNanos);

    if (transitioned == null) {
      // NOS too large for stash buffer — reject immediately, no orphan validation request.
      decoder.getClOrdId(clOrdIdScratch, 0);
      rfq.putSymbolInto(symbolScratch, 0);
      rfq.putSettlDateInto(settlDateScratch, 0);
      rfq.putCurrencyInto(currencyScratch, 0);
      rfq.putSettlCurrencyInto(settlCurrencyScratch, 0);
      final int len =
          encoder.encodeRejectExecutionReport(
              encodingBuffer,
              0,
              clOrdIdScratch,
              0,
              quoteIdScratch,
              0,
              symbolScratch,
              0,
              rfq.sideRaw(),
              TEXT_NOS_TOO_LARGE,
              TEXT_NOS_TOO_LARGE.length,
              epochClock.nanoTime(),
              rfq.productTypeRaw(),
              settlDateScratch,
              0,
              rfq.settlTypeRaw(),
              currencyScratch,
              0,
              settlCurrencyScratch,
              0,
              rfq.tenorRaw());
      final var gwResult = offerToGateway(len);
      if (gwResult != Action.CONTINUE) {
        return gwResult;
      }
      // Release pool slot immediately — the RFQ can never complete (NOS always too large)
      stateMachine.rejectQuoted(quoteIdScratch, 0, RfqState.QUOTE_ID_LENGTH);
      return Action.CONTINUE;
    }

    // 4. Stash succeeded — encode and publish PriceValidationRequest. On ABORT, the re-delivered
    // NOS fragment hits the PENDING_VALIDATION re-delivery path at step 2.
    final int pvLen =
        encoder.encodePriceValidationRequest(
            encodingBuffer, 0, rfq, decoder, epochClock.nanoTime());
    return offerToPricingOrAbort(pvLen);
  }

  // ===========================================================================
  // Pricing response handlers
  // ===========================================================================

  /**
   * Handles an inbound PriceResponse from the pricing service. Routes accepted responses to the
   * Quote encoding path (PENDING_PRICE → QUOTED), or rejected responses to the QuoteRequestReject
   * path (PENDING_PRICE → REJECTED).
   *
   * @param decoder pre-wrapped PriceResponse SBE decoder
   * @param buffer the underlying fragment buffer
   * @param offset start offset of the fragment
   * @param length fragment length in bytes
   * @return {@link Action#CONTINUE} on success, {@link Action#ABORT} on transient back-pressure
   */
  @Override
  public Action onPriceResponse(
      final PriceResponseDecoder decoder,
      final DirectBuffer buffer,
      final int offset,
      final int length) {

    decoder.getQuoteReqId(quoteReqIdScratch, 0);

    if (decoder.accepted() == BooleanType.True) {
      // Validate pricing data: no null values on accepted response
      if (decoder.bidPx() == PriceResponseDecoder.bidPxNullValue()
          || decoder.offerPx() == PriceResponseDecoder.offerPxNullValue()
          || decoder.bidSize() == PriceResponseDecoder.bidSizeNullValue()
          || decoder.offerSize() == PriceResponseDecoder.offerSizeNullValue()
          || decoder.validUntil() == PriceResponseDecoder.validUntilNullValue()) {
        LOG.error()
            .append("Accepted PriceResponse with null prices for quoteReqId at scratch")
            .commit();
        // Fall through to rejected path
        return handlePriceResponseRejected(QuoteRejectReasonEnum.InvalidPrice, TEXT_NULL_PRICES);
      }

      // Generate quoteId. On ABORT re-delivery, a new quoteId is generated each time (the
      // previous one is discarded). This wastes counter values but is acceptable: the counter
      // has 100B headroom, IDs need not be contiguous, and the publish-before-mutate pattern
      // ensures the discarded quoteId is never stored in the state machine.
      final int quoteIdLen = quoteIdGenerator.nextInto(encodingBuffer, 0);
      encodingBuffer.getBytes(0, quoteIdScratch, 0, quoteIdLen);
      // Zero tail bytes so the full 20-byte scratch is clean for downstream lookups
      if (quoteIdLen < RfqState.QUOTE_ID_LENGTH) {
        Arrays.fill(quoteIdScratch, quoteIdLen, RfqState.QUOTE_ID_LENGTH, (byte) 0);
      }

      // Find RfqState for identity fields
      final var rfq =
          stateMachine.findByQuoteReqId(quoteReqIdScratch, 0, RfqState.QUOTE_REQ_ID_LENGTH);
      if (rfq == null || rfq.state() != RfqState.State.PENDING_PRICE) {
        LOG.warn().append("PriceResponse accepted: no matching PENDING_PRICE RFQ").commit();
        return Action.CONTINUE;
      }

      // Encode Quote (publish-before-mutate: quoteId from scratch, not RfqState)
      final int quoteLen =
          encoder.encodeQuote(
              encodingBuffer, 0, rfq, quoteIdScratch, 0, quoteIdLen, epochClock.nanoTime());

      // Attempt publication — if transient back-pressure, ABORT (no state mutation)
      final var gwResult = offerToGateway(quoteLen);
      if (gwResult != Action.CONTINUE) {
        return gwResult;
      }

      // Publication succeeded: now mutate state
      stateMachine.onPriceResponseAccepted(
          quoteReqIdScratch,
          0,
          RfqState.QUOTE_REQ_ID_LENGTH,
          decoder,
          quoteIdScratch,
          0,
          quoteIdLen,
          nanoClock.nanoTime());
      return Action.CONTINUE;

    } else {
      // Rejected by pricing
      return handlePriceResponseRejected(decoder.quoteRejectReason(), null);
    }
  }

  /**
   * Handles an inbound PriceValidationResponse from the pricing service. Valid responses forward
   * the stashed NOS to the cluster (PENDING_VALIDATION → COMPLETED). Invalid responses encode a
   * reject ExecutionReport with ClOrdID (tag 11) from the stashed NOS and transition to REJECTED.
   *
   * @param decoder pre-wrapped PriceValidationResponse SBE decoder
   * @param buffer the underlying fragment buffer
   * @param offset start offset of the fragment
   * @param length fragment length in bytes
   * @return {@link Action#CONTINUE} on success, {@link Action#ABORT} on transient back-pressure
   */
  @Override
  public Action onPriceValidationResponse(
      final PriceValidationResponseDecoder decoder,
      final DirectBuffer buffer,
      final int offset,
      final int length) {

    decoder.getQuoteId(quoteIdScratch, 0);

    if (decoder.valid() == BooleanType.True) {
      // Defensive: verify RFQ exists and is in the expected PENDING_VALIDATION state
      final var rfq = stateMachine.findByQuoteId(quoteIdScratch, 0, RfqState.QUOTE_ID_LENGTH);
      if (rfq == null || rfq.state() != RfqState.State.PENDING_VALIDATION) {
        LOG.warn()
            .append("PriceValidationResponse valid: no matching PENDING_VALIDATION RFQ")
            .commit();
        return Action.CONTINUE;
      }

      // Read the stashed NOS bytes BEFORE releasing the pool slot, which zeros the flat buffer.
      nosScratchLength = rfq.putNosInto(nosScratch, 0);

      // Now release the pool slot — flat buffer is zeroed after this call
      stateMachine.onValidationValid(quoteIdScratch, 0, RfqState.QUOTE_ID_LENGTH);

      // TODO(APP-31): forward NOS from nosScratch[0..nosScratchLength) to cluster ingress.
      // The NOS bytes have been pre-read into the scratch buffer above.
      LOG.info().append("Validation passed: forwarding NOS to cluster stub").commit();
      return Action.CONTINUE;

    } else {
      // Validation failed: encode reject ExecutionReport (publish-before-mutate)
      final var rfq = stateMachine.findByQuoteId(quoteIdScratch, 0, RfqState.QUOTE_ID_LENGTH);
      if (rfq == null || rfq.state() != RfqState.State.PENDING_VALIDATION) {
        LOG.warn()
            .append("PriceValidationResponse invalid: no matching PENDING_VALIDATION RFQ")
            .commit();
        return Action.CONTINUE;
      }

      // Extract ClOrdID (tag 11) from the stashed NOS (zero-alloc: pre-allocated decoders +
      // scratch)
      final int stashedNosLen = rfq.putNosInto(nosScratch, 0);
      if (stashedNosLen >= com.trading.engine.messages.sbe.MessageHeaderDecoder.ENCODED_LENGTH) {
        stashedNosView.wrap(nosScratch, 0, stashedNosLen);
        stashedHeaderDecoder.wrap(stashedNosView, 0);
        if (stashedNosLen
            >= com.trading.engine.messages.sbe.MessageHeaderDecoder.ENCODED_LENGTH
                + stashedHeaderDecoder.blockLength()) {
          stashedNosDecoder.wrap(
              stashedNosView,
              com.trading.engine.messages.sbe.MessageHeaderDecoder.ENCODED_LENGTH,
              stashedHeaderDecoder.blockLength(),
              stashedHeaderDecoder.version());
          stashedNosDecoder.getClOrdId(clOrdIdScratch, 0);
        } else {
          Arrays.fill(clOrdIdScratch, (byte) 0);
        }
      } else {
        Arrays.fill(clOrdIdScratch, (byte) 0);
      }
      rfq.putQuoteReqIdInto(quoteReqIdScratch, 0);
      rfq.putSymbolInto(symbolScratch, 0);
      rfq.putSettlDateInto(settlDateScratch, 0);
      rfq.putCurrencyInto(currencyScratch, 0);
      rfq.putSettlCurrencyInto(settlCurrencyScratch, 0);

      final int len =
          encoder.encodeRejectExecutionReport(
              encodingBuffer,
              0,
              clOrdIdScratch,
              0,
              quoteIdScratch,
              0,
              symbolScratch,
              0,
              rfq.sideRaw(),
              TEXT_VALIDATION_FAILED,
              TEXT_VALIDATION_FAILED.length,
              epochClock.nanoTime(),
              rfq.productTypeRaw(),
              settlDateScratch,
              0,
              rfq.settlTypeRaw(),
              currencyScratch,
              0,
              settlCurrencyScratch,
              0,
              rfq.tenorRaw());

      final var gwResult = offerToGateway(len);
      if (gwResult != Action.CONTINUE) {
        return gwResult;
      }

      stateMachine.onValidationInvalid(quoteIdScratch, 0, RfqState.QUOTE_ID_LENGTH);
      return Action.CONTINUE;
    }
  }

  // ===========================================================================
  // Reap callback
  // ===========================================================================

  private void onRfqExpired(final RfqState state) {
    reapExpiredCount++;

    final int len;
    if (state.nosLength() > 0) {
      // RFQ was in PENDING_VALIDATION — the client submitted a NOS, so the correct FIX response
      // is a reject ExecutionReport (35=8) with ClOrdID (tag 11) recovered from the stashed NOS.
      final int stashedNosLen = state.putNosInto(nosScratch, 0);
      if (stashedNosLen >= com.trading.engine.messages.sbe.MessageHeaderDecoder.ENCODED_LENGTH) {
        stashedNosView.wrap(nosScratch, 0, stashedNosLen);
        stashedHeaderDecoder.wrap(stashedNosView, 0);
        if (stashedNosLen
            >= com.trading.engine.messages.sbe.MessageHeaderDecoder.ENCODED_LENGTH
                + stashedHeaderDecoder.blockLength()) {
          stashedNosDecoder.wrap(
              stashedNosView,
              com.trading.engine.messages.sbe.MessageHeaderDecoder.ENCODED_LENGTH,
              stashedHeaderDecoder.blockLength(),
              stashedHeaderDecoder.version());
          stashedNosDecoder.getClOrdId(clOrdIdScratch, 0);
        } else {
          Arrays.fill(clOrdIdScratch, (byte) 0);
        }
      } else {
        Arrays.fill(clOrdIdScratch, (byte) 0);
      }
      state.putQuoteIdInto(quoteIdScratch, 0);
      state.putSymbolInto(symbolScratch, 0);
      state.putSettlDateInto(settlDateScratch, 0);
      state.putCurrencyInto(currencyScratch, 0);
      state.putSettlCurrencyInto(settlCurrencyScratch, 0);
      len =
          encoder.encodeRejectExecutionReport(
              encodingBuffer,
              0,
              clOrdIdScratch,
              0,
              quoteIdScratch,
              0,
              symbolScratch,
              0,
              state.sideRaw(),
              TEXT_RFQ_EXPIRED,
              TEXT_RFQ_EXPIRED.length,
              epochClock.nanoTime(),
              state.productTypeRaw(),
              settlDateScratch,
              0,
              state.settlTypeRaw(),
              currencyScratch,
              0,
              settlCurrencyScratch,
              0,
              state.tenorRaw());
    } else {
      // RFQ was in PENDING_PRICE or QUOTED — no NOS was submitted, send QuoteRequestReject
      len =
          encoder.encodeQuoteRequestReject(
              encodingBuffer,
              0,
              state,
              QuoteRejectReasonEnum.TooLateToEnter,
              TEXT_RFQ_EXPIRED,
              TEXT_RFQ_EXPIRED.length,
              epochClock.nanoTime());
    }

    final var reapGwResult = offerToGateway(len);
    if (reapGwResult != Action.CONTINUE) {
      LOG.warn()
          .append("Reap notification dropped: publication back-pressured for poolIndex=")
          .append(state.poolIndex())
          .commit();
    }

    // Pool slot release is handled by RfqStateMachine.reapExpired/reapAll in a finally block.
  }

  // ===========================================================================
  // Inbound validation
  // ===========================================================================

  /**
   * Validates basic QuoteRequest fields before pool acquisition. Returns the reject text if
   * invalid, or {@code null} if the request is acceptable. Sets {@link #validationFailureReason} as
   * a side-effect so the caller can read the reason without allocating a pair/record.
   *
   * <p>Checks: symbol not empty, orderQty > 0, side valid, accountCode not empty. Deeper validation
   * (credit, whitelist, permissions, market hours) is tracked in APP-215.
   *
   * @param decoder the pre-wrapped QuoteRequest decoder
   * @return pre-allocated reject text byte array, or {@code null} if validation passes
   */
  private byte[] validateQuoteRequest(final QuoteRequestDecoder decoder) {
    // Check symbol not all-zero
    decoder.getSymbol(quoteReqIdScratch, 0); // reuse scratch
    if (isAllZero(quoteReqIdScratch, RfqState.SYMBOL_LENGTH)) {
      validationFailureReason = QuoteRejectReasonEnum.UnknownSymbol;
      return TEXT_INVALID_SYMBOL;
    }
    // Check orderQty > 0
    if (decoder.orderQty() <= 0) {
      validationFailureReason = QuoteRejectReasonEnum.Other;
      return TEXT_INVALID_QTY;
    }
    // Check side is valid
    if (decoder.side() == SideEnum.NULL_VAL) {
      validationFailureReason = QuoteRejectReasonEnum.Other;
      return TEXT_INVALID_SIDE;
    }
    // Check accountCode not all-zero
    decoder.getAccountCode(quoteReqIdScratch, 0); // reuse scratch
    if (isAllZero(quoteReqIdScratch, RfqState.ACCOUNT_CODE_LENGTH)) {
      validationFailureReason = QuoteRejectReasonEnum.Other;
      return TEXT_INVALID_ACCOUNT;
    }
    return null;
  }

  // ===========================================================================
  // Publication helpers
  // ===========================================================================

  /**
   * Attempts to offer the encoded message to the gateway publication with bounded retries. Returns
   * {@link Action#CONTINUE} on success or terminal publication error (drop the message — the client
   * will time out), or {@link Action#ABORT} on transient back-pressure (so Aeron re-delivers the
   * inbound fragment).
   *
   * <p>Terminal failures (NOT_CONNECTED, CLOSED, MAX_POSITION_EXCEEDED) are mapped to CONTINUE to
   * avoid infinite ABORT loops on a permanently broken publication.
   *
   * @param encodedLength number of bytes in the encoding buffer to offer
   * @return {@link Action#CONTINUE} on success or terminal drop, {@link Action#ABORT} on transient
   *     back-pressure
   */
  private Action offerToGateway(final int encodedLength) {
    final int result = offerWithRetry(gatewayPublication, encodingBuffer, 0, encodedLength);
    if (result > 0) {
      return Action.CONTINUE;
    }
    if (result == 0) {
      // Transient: retries exhausted on BACK_PRESSURED/ADMIN_ACTION — ABORT for re-delivery
      backPressureAborts++;
      return Action.ABORT;
    }
    // Terminal: NOT_CONNECTED, CLOSED, MAX_POSITION_EXCEEDED — drop (CONTINUE) to avoid spin loop.
    // The client will time out via RFQ expiry and receive a QuoteRequestReject.
    LOG.warn()
        .append("Gateway publication terminal: dropping message to avoid ABORT loop")
        .commit();
    return Action.CONTINUE;
  }

  /**
   * Attempts to offer the encoded message to the pricing publication. Returns {@link
   * Action#CONTINUE} on success, {@link Action#ABORT} on transient back-pressure (so Aeron
   * re-delivers the inbound fragment), or {@link Action#CONTINUE} on terminal publication error
   * (drop the message — the client will time out).
   */
  private Action offerToPricingOrAbort(final int encodedLength) {
    final int result = offerWithRetry(pricingPublication, encodingBuffer, 0, encodedLength);
    if (result > 0) {
      return Action.CONTINUE;
    }
    if (result == 0) {
      // Transient: retries exhausted on BACK_PRESSURED/ADMIN_ACTION — ABORT for re-delivery
      backPressureAborts++;
      return Action.ABORT;
    }
    // Terminal: NOT_CONNECTED, CLOSED, MAX_POSITION_EXCEEDED — drop (CONTINUE) to avoid spin loop.
    // The client will time out via PENDING_PRICE timeout and receive a QuoteRequestReject.
    LOG.warn()
        .append("Pricing publication terminal: dropping message, client will time out")
        .commit();
    return Action.CONTINUE;
  }

  /**
   * Bounded retry loop for Aeron publication. Retries on BACK_PRESSURED and ADMIN_ACTION up to
   * {@link OrchestratorConstants#MAX_PUBLICATION_RETRIES} times. Returns a tri-state int:
   *
   * <ul>
   *   <li>Positive (1): publication succeeded
   *   <li>Zero (0): transient retries exhausted (BACK_PRESSURED/ADMIN_ACTION)
   *   <li>Negative (-1): terminal error (NOT_CONNECTED, CLOSED, MAX_POSITION_EXCEEDED)
   * </ul>
   */
  private int offerWithRetry(
      final ExclusivePublication publication,
      final DirectBuffer buffer,
      final int offset,
      final int length) {

    for (int attempt = 0; attempt < MAX_PUBLICATION_RETRIES; attempt++) {
      final long result = publication.offer(buffer, offset, length);
      if (result >= 0) {
        return 1;
      }
      if (result == Publication.BACK_PRESSURED || result == Publication.ADMIN_ACTION) {
        continue; // retry
      }
      // Terminal: NOT_CONNECTED, CLOSED, MAX_POSITION_EXCEEDED
      LOG.error().append("Publication terminal error: result=").append(result).commit();
      publicationFailures++;
      return -1;
    }
    // Retries exhausted (transient back-pressure) — tracked by callers via backPressureAborts
    return 0;
  }

  /**
   * Helper for the PriceResponse rejected path. Looks up the RfqState, encodes a
   * QuoteRequestReject, publishes to gateway (publish-before-mutate), then transitions state.
   */
  private Action handlePriceResponseRejected(
      final QuoteRejectReasonEnum reason, final byte[] overrideText) {

    final var rfq =
        stateMachine.findByQuoteReqId(quoteReqIdScratch, 0, RfqState.QUOTE_REQ_ID_LENGTH);
    if (rfq == null || rfq.state() != RfqState.State.PENDING_PRICE) {
      LOG.warn().append("PriceResponse rejected: no matching PENDING_PRICE RFQ").commit();
      return Action.CONTINUE;
    }

    final var text = overrideText != null ? overrideText : TEXT_PRICING_REJECTED;
    final int len =
        encoder.encodeQuoteRequestReject(
            encodingBuffer, 0, rfq, reason, text, text.length, epochClock.nanoTime());

    final var gwResult = offerToGateway(len);
    if (gwResult != Action.CONTINUE) {
      return gwResult;
    }

    stateMachine.onPriceResponseRejected(quoteReqIdScratch, 0, RfqState.QUOTE_REQ_ID_LENGTH);
    return Action.CONTINUE;
  }

  // ===========================================================================
  // Utility
  // ===========================================================================

  /** Returns {@code true} if the first {@code len} bytes of {@code data} are all zero. */
  private static boolean isAllZero(final byte[] data, final int len) {
    for (int i = 0; i < len; i++) {
      if (data[i] != 0) {
        return false;
      }
    }
    return true;
  }
}
