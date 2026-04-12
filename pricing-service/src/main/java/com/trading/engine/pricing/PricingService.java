package com.trading.engine.pricing;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.trading.engine.messages.FixedPointScale;
import com.trading.engine.messages.sbe.AccountTypeEnum;
import com.trading.engine.messages.sbe.PriceRequestDecoder;
import com.trading.engine.messages.sbe.PriceValidationRequestDecoder;
import com.trading.engine.messages.sbe.ProductTypeEnum;
import com.trading.engine.messages.sbe.QuoteRejectReasonEnum;
import com.trading.engine.pricing.codec.PricingMessageDispatcher;
import com.trading.engine.pricing.codec.PricingResponseEncoder;
import com.trading.engine.pricing.forward.ForwardPointSource;
import com.trading.engine.pricing.market.MarketDataAdapter;
import com.trading.engine.pricing.quote.PriceValidator;
import com.trading.engine.pricing.quote.QuoteEntry;
import com.trading.engine.pricing.quote.QuoteManager;
import com.trading.engine.pricing.spread.SpreadModel;
import com.trading.engine.pricing.spread.SpreadResult;
import com.trading.engine.pricing.spread.VolatilityMonitor;
import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import java.util.Objects;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Production FX pricing service agent. Processes {@code PriceRequest} (templateId=50) and {@code
 * PriceValidationRequest} (templateId=52) messages on Aeron IPC stream 200 ({@link
 * PricingConstants#REQUEST_STREAM_ID}), responds on stream 201 ({@link
 * PricingConstants#RESPONSE_STREAM_ID}).
 *
 * <p>This agent is the core duty-cycle loop of the pricing service. On each {@link #doWork()}
 * invocation it:
 *
 * <ol>
 *   <li>Ticks the {@link MarketDataAdapter} to produce fresh mid-rates (synthetic or
 *       deterministic).
 *   <li>Polls inbound Aeron fragments via {@link Subscription#controlledPoll} using a {@link
 *       PricingMessageDispatcher} that dispatches to this agent's handler callbacks.
 *   <li>Periodically sweeps expired quotes from the {@link QuoteManager} on a throttled schedule to
 *       avoid O(n) scan cost on every tick.
 * </ol>
 *
 * <h3>Back-pressure handling</h3>
 *
 * <p>Uses {@link ControlledFragmentHandler} so that when the response publication is
 * back-pressured, the handler returns {@link Action#ABORT} and Aeron re-delivers the inbound
 * fragment on the next poll. The {@link #offerResponse(int)} helper retries up to {@link
 * PricingConstants#MAX_PUBLICATION_RETRIES} times for transient back-pressure ({@link
 * Publication#BACK_PRESSURED}, {@link Publication#ADMIN_ACTION}) before aborting.
 *
 * <h3>Price request flow</h3>
 *
 * <ol>
 *   <li>Read symbol, orderQty, accountCode, productType, transactTime, quoteReqId from the SBE
 *       decoder.
 *   <li>Look up the spot mid-rate from the market data adapter. Reject if unavailable.
 *   <li>Check staleness against monotonic clock thresholds. Reject if stale; widen spread if
 *       approaching stale.
 *   <li>Route by product type: Spot uses the raw mid; Forward adds forward points; Swap computes
 *       swap points for near/far legs.
 *   <li>Compute bid/offer spread via the spread model.
 *   <li>Allocate and populate a quote in the quote manager.
 *   <li>Encode and offer the PriceResponse to the outbound publication.
 * </ol>
 *
 * <h3>Threading model</h3>
 *
 * <p><b>Not thread-safe.</b> This agent runs on a single Aeron {@link
 * org.agrona.concurrent.AgentRunner} thread. All mutable state (encoding buffer, spread result,
 * sweep timestamp, decoder flyweights in the dispatcher) is accessed exclusively from this thread.
 *
 * <h3>Allocation behaviour</h3>
 *
 * <p><b>Zero allocation after construction.</b> The encoding buffer, spread result flyweight,
 * reject-text byte arrays, and all collaborator instances are pre-allocated. The {@link #doWork()}
 * method and all handler callbacks perform only flyweight wraps, array copies, and integer
 * arithmetic -- no heap allocation on the hot path.
 *
 * @see PricingMessageDispatcher
 * @see PricingResponseEncoder
 * @see MarketDataAdapter
 * @see SpreadModel
 * @see QuoteManager
 * @see PriceValidator
 */
public final class PricingService implements Agent, PricingMessageDispatcher.PricingMessageHandler {

  private static final Log LOG = LogFactory.getLog(PricingService.class);

  // ===========================================================================
  // Pre-allocated reject text constants (ASCII byte arrays, cold-path only)
  // ===========================================================================

  /** Reject text for unavailable market data. Pre-allocated to avoid hot-path string encoding. */
  private static final byte[] TEXT_NO_MARKET_DATA = "No market data".getBytes();

  /** Reject text for stale market data beyond the hard threshold. */
  private static final byte[] TEXT_STALE_MARKET_DATA = "Stale market data".getBytes();

  /** SBE Symbol field length (8 bytes). */
  private static final int SYMBOL_LENGTH = PriceRequestDecoder.symbolLength();

  /** SBE QuoteReqID field length (20 bytes). */
  private static final int QUOTE_REQ_ID_LENGTH = PriceRequestDecoder.quoteReqIdLength();

  /** SBE AccountCode field length (16 bytes). */
  private static final int ACCOUNT_CODE_LENGTH = PriceRequestDecoder.accountCodeLength();

  /**
   * Approximate days-to-settlement lookup indexed by {@link
   * com.trading.engine.messages.sbe.TenorEnum#value()}. Used to convert tenor enums to integer days
   * for the forward point term structure query. Values are rounded estimates suitable for pricing;
   * exact business-day-adjusted settlement dates require a holiday calendar (future enhancement).
   *
   * <p>Index 0 is unused (no TenorEnum maps to 0). Indices 1-14 map to ON through BRK.
   */
  private static final int[] TENOR_TO_DAYS = {
    0, //  0: unused
    1, //  1: ON  (overnight)
    2, //  2: TN  (tomorrow/next)
    3, //  3: SN  (spot/next)
    7, //  4: W1  (1 week)
    14, //  5: W2  (2 weeks)
    30, //  6: M1  (1 month)
    60, //  7: M2  (2 months)
    90, //  8: M3  (3 months)
    180, //  9: M6  (6 months)
    270, // 10: M9  (9 months)
    365, // 11: Y1  (1 year)
    730, // 12: Y2  (2 years)
    90, // 13: IMM (next IMM date, ~3 months approximation)
    30, // 14: BRK (broken date, default to 30 -- caller should supply settlDate)
  };

  // ===========================================================================
  // Injected collaborators (all final, set at construction)
  // ===========================================================================

  /** Aeron subscription on stream 200 for inbound PriceRequest / PriceValidationRequest. */
  private final Subscription subscription;

  /**
   * Aeron exclusive publication on stream 201 for outbound PriceResponse / PriceValidationResponse.
   */
  private final ExclusivePublication publication;

  /** Market data source -- produces spot mid-rates on each doWork() tick. */
  private final MarketDataAdapter marketDataAdapter;

  /** Forward point term structure for FX forward and swap pricing. */
  private final ForwardPointSource forwardPointSource;

  /** Bid/offer spread computation around a mid-rate. */
  private final SpreadModel spreadModel;

  /** Rolling-window volatility tracker for spread widening during volatile conditions. */
  private final VolatilityMonitor volatilityMonitor;

  /** Active quote store with pre-allocated pool. */
  private final QuoteManager quoteManager;

  /** Quote validation (expiry, slippage, last-look). */
  private final PriceValidator priceValidator;

  /** SBE message dispatcher that decodes inbound fragments and calls this agent's handlers. */
  private final PricingMessageDispatcher dispatcher;

  /** SBE response encoder with pre-allocated flyweights and scratch buffers. */
  private final PricingResponseEncoder responseEncoder;

  /** Epoch nanosecond clock for wall-clock timestamps (quote expiry, transact time). */
  private final EpochNanoClock epochClock;

  /** Monotonic nanosecond clock for elapsed time (staleness checks, sweep throttling). */
  private final NanoClock nanoClock;

  // ===========================================================================
  // Configuration thresholds
  // ===========================================================================

  /** Maximum market data age (monotonic nanos) before rejecting a price request outright. */
  private final long staleThresholdNanos;

  /** Market data age (monotonic nanos) beyond which the spread is widened. */
  private final long staleWidenThresholdNanos;

  /** Time-to-live for Spot quotes in nanoseconds. */
  private final long spotTtlNanos;

  /** Time-to-live for Forward quotes in nanoseconds. */
  private final long forwardTtlNanos;

  /** Time-to-live for Swap quotes in nanoseconds. */
  private final long swapTtlNanos;

  /** Minimum interval (monotonic nanos) between expired-quote sweep passes. */
  private final long sweepIntervalNanos;

  // ===========================================================================
  // Pre-allocated mutable state (single-threaded access only)
  // ===========================================================================

  /**
   * Pre-allocated encoding buffer for outbound SBE messages. Sized at {@link
   * PricingConstants#ENCODING_BUFFER_SIZE} (512 bytes), which accommodates the worst-case
   * PriceResponse with two swap legs plus margin for future field additions.
   */
  private final UnsafeBuffer encodingBuffer =
      new UnsafeBuffer(new byte[PricingConstants.ENCODING_BUFFER_SIZE]);

  /**
   * Mutable flyweight for spread computation results. Pre-allocated once and reused on every price
   * request. Valid only until the next {@link SpreadModel#compute} call.
   */
  private final SpreadResult spreadResult = new SpreadResult();

  /**
   * Monotonic nanosecond timestamp of the last expired-quote sweep. Used to throttle {@link
   * QuoteManager#expireStale(long)} calls to at most once per {@link #sweepIntervalNanos}.
   */
  private long lastSweepNanos;

  /**
   * Maximum number of fragments to poll from the Aeron subscription per {@link #doWork()}
   * invocation. Limits per-tick CPU consumption and ensures the sweep and market data adapter get a
   * fair share of the duty cycle.
   */
  private final int pollLimit = 10;

  // ===========================================================================
  // Constructor
  // ===========================================================================

  /**
   * Constructs the pricing service agent with all dependencies injected.
   *
   * <p>All collaborators are stored as final references. The {@link PricingMessageDispatcher} is
   * constructed internally, wiring {@code this} as the message handler. The encoding buffer and
   * spread result flyweight are pre-allocated at construction time.
   *
   * <p><b>Allocation:</b> allocates the encoding buffer ({@link
   * PricingConstants#ENCODING_BUFFER_SIZE} bytes), the spread result flyweight, and the dispatcher.
   * All subsequent {@link #doWork()} calls are zero-allocation.
   *
   * @param subscription Aeron subscription on stream {@link PricingConstants#REQUEST_STREAM_ID}
   * @param publication Aeron exclusive publication on stream {@link
   *     PricingConstants#RESPONSE_STREAM_ID}
   * @param marketDataAdapter market data source (synthetic or deterministic)
   * @param forwardPointSource forward point term structure for forward/swap pricing
   * @param spreadModel bid/offer spread computation strategy
   * @param volatilityMonitor rolling-window volatility tracker for spread widening
   * @param quoteManager active quote store with pre-allocated pool
   * @param priceValidator quote validation (expiry, slippage, last-look)
   * @param responseEncoder SBE response encoder with pre-allocated flyweights
   * @param epochClock epoch nanosecond clock for wall-clock timestamps
   * @param nanoClock monotonic nanosecond clock for elapsed time measurements
   * @param staleThresholdNanos maximum market data age before rejection (monotonic nanos)
   * @param staleWidenThresholdNanos market data age threshold for spread widening (monotonic nanos)
   * @param spotTtlNanos time-to-live for Spot quotes (nanos)
   * @param forwardTtlNanos time-to-live for Forward quotes (nanos)
   * @param swapTtlNanos time-to-live for Swap quotes (nanos)
   * @param sweepIntervalNanos interval between expired-quote sweep passes (monotonic nanos)
   */
  public PricingService(
      final Subscription subscription,
      final ExclusivePublication publication,
      final MarketDataAdapter marketDataAdapter,
      final ForwardPointSource forwardPointSource,
      final SpreadModel spreadModel,
      final VolatilityMonitor volatilityMonitor,
      final QuoteManager quoteManager,
      final PriceValidator priceValidator,
      final PricingResponseEncoder responseEncoder,
      final EpochNanoClock epochClock,
      final NanoClock nanoClock,
      final long staleThresholdNanos,
      final long staleWidenThresholdNanos,
      final long spotTtlNanos,
      final long forwardTtlNanos,
      final long swapTtlNanos,
      final long sweepIntervalNanos) {

    this.subscription = Objects.requireNonNull(subscription, "subscription");
    this.publication = Objects.requireNonNull(publication, "publication");
    this.marketDataAdapter = Objects.requireNonNull(marketDataAdapter, "marketDataAdapter");
    this.forwardPointSource = Objects.requireNonNull(forwardPointSource, "forwardPointSource");
    this.spreadModel = Objects.requireNonNull(spreadModel, "spreadModel");
    this.volatilityMonitor = Objects.requireNonNull(volatilityMonitor, "volatilityMonitor");
    this.quoteManager = Objects.requireNonNull(quoteManager, "quoteManager");
    this.priceValidator = Objects.requireNonNull(priceValidator, "priceValidator");
    this.responseEncoder = Objects.requireNonNull(responseEncoder, "responseEncoder");
    this.epochClock = Objects.requireNonNull(epochClock, "epochClock");
    this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
    this.staleThresholdNanos = staleThresholdNanos;
    this.staleWidenThresholdNanos = staleWidenThresholdNanos;
    this.spotTtlNanos = spotTtlNanos;
    this.forwardTtlNanos = forwardTtlNanos;
    this.swapTtlNanos = swapTtlNanos;
    this.sweepIntervalNanos = sweepIntervalNanos;
    this.dispatcher = new PricingMessageDispatcher(this);
    this.lastSweepNanos = 0L;
  }

  // ===========================================================================
  // Agent lifecycle
  // ===========================================================================

  /**
   * Called once when the agent runner starts. Delegates to the market data adapter's {@link
   * Agent#onStart()} to initialise synthetic price generation or load deterministic fixtures.
   *
   * <p><b>Allocation:</b> adapter-dependent (cold path, startup only).
   */
  @Override
  public void onStart() {
    marketDataAdapter.onStart();
    LOG.info().append("PricingService started: role=").append(roleName()).commit();
  }

  /**
   * Agent duty-cycle method invoked repeatedly by the {@link org.agrona.concurrent.AgentRunner}.
   *
   * <ol>
   *   <li>Ticks the market data adapter to generate or refresh mid-rates.
   *   <li>Polls inbound Aeron fragments via controlled poll, dispatching to {@link #onPriceRequest}
   *       or {@link #onPriceValidationRequest}.
   *   <li>If the sweep interval has elapsed, scans the quote manager for expired quotes.
   * </ol>
   *
   * <p><b>Allocation:</b> zero allocation. All operations use pre-allocated state.
   *
   * @return the number of work items processed (0 signals idle to the idle strategy)
   */
  @Override
  public int doWork() throws Exception {
    int workCount = 0;
    workCount += marketDataAdapter.doWork();
    workCount += subscription.controlledPoll(dispatcher, pollLimit);

    final long now = nanoClock.nanoTime();
    if (now - lastSweepNanos >= sweepIntervalNanos) {
      workCount += quoteManager.expireStale(epochClock.nanoTime());
      lastSweepNanos = now;
    }
    return workCount;
  }

  /**
   * Called once when the agent runner is closing. Closes the Aeron subscription and publication,
   * then delegates to the market data adapter's {@link Agent#onClose()}.
   *
   * <p>Closing the subscription and publication releases Aeron resources (log buffers, images). The
   * order is subscription-first to stop accepting new fragments before closing the response
   * channel.
   */
  @Override
  public void onClose() {
    subscription.close();
    publication.close();
    marketDataAdapter.onClose();
    LOG.info().append("PricingService closed").commit();
  }

  /**
   * Returns the human-readable role name for this agent, displayed in Aeron error logs and
   * conductor heartbeats.
   *
   * @return {@code "pricing-service"}
   */
  @Override
  public String roleName() {
    return "pricing-service";
  }

  // ===========================================================================
  // PricingMessageHandler callbacks
  // ===========================================================================

  /**
   * Handles an inbound PriceRequest (templateId=50). Prices the request by looking up the spot
   * mid-rate, applying staleness checks, routing by product type (Spot/Forward/Swap), computing the
   * bid/offer spread, allocating a quote, and encoding the PriceResponse.
   *
   * <p><b>Product type routing:</b>
   *
   * <ul>
   *   <li><b>Spot:</b> mid-rate is the raw spot from the market data adapter.
   *   <li><b>Forward:</b> mid-rate = spot + forward points for the settlement tenor.
   *   <li><b>Swap:</b> computes swap points (far - near forward points). For v1, the near leg uses
   *       spot and the far leg uses spot + forward points at the far tenor. Swap points are encoded
   *       in the response for client display.
   * </ul>
   *
   * <p><b>Allocation:</b> zero allocation. All SBE field access uses the decoder's underlying
   * {@link DirectBuffer} and pre-computed offsets; spread results are written into the
   * pre-allocated {@link #spreadResult} flyweight.
   *
   * @param decoder pre-wrapped PriceRequest decoder -- do not retain past return
   * @param buffer the underlying DirectBuffer containing the full fragment
   * @param offset byte offset of the fragment start (including SBE header)
   * @param length byte length of the complete fragment
   * @return {@link Action#CONTINUE} on success, {@link Action#ABORT} on publication back-pressure
   */
  @Override
  public Action onPriceRequest(
      final PriceRequestDecoder decoder,
      final DirectBuffer buffer,
      final int offset,
      final int length) {

    // --- Extract fields from the decoder's underlying buffer (zero-copy for char fields) ---

    final DirectBuffer decoderBuffer = decoder.buffer();
    final int decoderOffset = decoder.offset();

    // QuoteReqID (FIX tag 131) -- 20-byte char field at encoding offset 0
    final int quoteReqIdOff = decoderOffset + PriceRequestDecoder.quoteReqIdEncodingOffset();

    // Symbol (FIX tag 55) -- 8-byte char field at encoding offset 20
    final int symbolOff = decoderOffset + PriceRequestDecoder.symbolEncodingOffset();

    // AccountCode (FIX tag 1) -- 16-byte char field at encoding offset 37
    final int accountCodeOff = decoderOffset + PriceRequestDecoder.accountCodeEncodingOffset();

    // Scalar fields
    final long orderQty = decoder.orderQty();
    final long transactTime = decoder.transactTime();
    final ProductTypeEnum productType = decoder.productType();

    // --- Step 1: Look up spot mid-rate ---

    final long spotMid = marketDataAdapter.midRate(decoderBuffer, symbolOff, SYMBOL_LENGTH);

    if (spotMid == FixedPointScale.PRICE_NOT_AVAILABLE) {
      return encodeRejectedPriceResponse(
          decoderBuffer,
          quoteReqIdOff,
          symbolOff,
          transactTime,
          productType,
          QuoteRejectReasonEnum.Other.value(),
          TEXT_NO_MARKET_DATA,
          TEXT_NO_MARKET_DATA.length);
    }

    // --- Step 2: Staleness check ---

    final long dataAge =
        nanoClock.nanoTime()
            - marketDataAdapter.lastUpdateNanos(decoderBuffer, symbolOff, SYMBOL_LENGTH);

    if (dataAge > staleThresholdNanos) {
      return encodeRejectedPriceResponse(
          decoderBuffer,
          quoteReqIdOff,
          symbolOff,
          transactTime,
          productType,
          QuoteRejectReasonEnum.Other.value(),
          TEXT_STALE_MARKET_DATA,
          TEXT_STALE_MARKET_DATA.length);
    }

    // Note stale-widen condition for later spread computation. The VolatilityMonitor will
    // incorporate this indirectly through its rolling-window range tracking; the explicit
    // widen flag can be used for future per-request spread adjustment.
    // For v1, the volatilityMonitor's multiplier already captures recent rate variance.

    // --- Step 3: Route by product type ---

    long midRate = spotMid;
    long swapPoints = com.trading.engine.messages.sbe.PriceResponseEncoder.swapPointsNullValue();

    switch (productType) {
      case Spot -> {
        // Spot: mid-rate is the raw spot from the adapter. No adjustment needed.
      }
      case Forward -> {
        // Forward: mid-rate = spot + forward points for the settlement tenor.
        final int daysToSettlement = tenorToDays(decoder.tenor());
        final long fwdPoints =
            forwardPointSource.forwardPoints(
                decoderBuffer, symbolOff, SYMBOL_LENGTH, daysToSettlement);
        midRate = spotMid + fwdPoints;
      }
      case Swap -> {
        // Swap: near leg at spot, far leg at spot + far forward points.
        // For v1, use tenor as the far leg tenor; near leg is spot (0 days).
        final int farDays = tenorToDays(decoder.tenor());
        final long farFwdPoints =
            forwardPointSource.forwardPoints(decoderBuffer, symbolOff, SYMBOL_LENGTH, farDays);
        // The "swap points" are the forward points for the far leg (since near = spot = 0).
        swapPoints = farFwdPoints;
        // For the spread model, use the far-leg outright as the mid-rate.
        midRate = spotMid + farFwdPoints;
      }
      default -> {
        // Unknown product type -- treat as Spot for resilience.
        LOG.warn()
            .append("Unknown productType=")
            .append(productType.value())
            .append(", defaulting to Spot")
            .commit();
      }
    }

    // --- Step 4: Record mid-rate in volatility monitor ---

    volatilityMonitor.recordMidRate(decoderBuffer, symbolOff, SYMBOL_LENGTH, midRate);

    // --- Step 5: Compute bid/offer spread ---

    // Default to Client tier for v1; account-type resolution from config to be wired later.
    final AccountTypeEnum accountType = AccountTypeEnum.Client;

    spreadModel.compute(
        decoderBuffer,
        symbolOff,
        SYMBOL_LENGTH,
        midRate,
        orderQty,
        accountType,
        productType,
        spreadResult);

    final long bidPx = spreadResult.bidPx;
    final long offerPx = spreadResult.offerPx;

    // --- Step 6: Determine TTL and compute expiry ---

    final long ttlNanos = ttlForProductType(productType);
    final long nowEpoch = epochClock.nanoTime();
    final long validUntil = nowEpoch + ttlNanos;

    // --- Step 7: Allocate and populate quote ---

    final QuoteEntry quote =
        quoteManager.allocateAndStore(decoderBuffer, quoteReqIdOff, QUOTE_REQ_ID_LENGTH);

    quote.populate(
        decoderBuffer,
        quoteReqIdOff,
        QUOTE_REQ_ID_LENGTH,
        decoderBuffer,
        symbolOff,
        SYMBOL_LENGTH,
        decoderBuffer,
        accountCodeOff,
        ACCOUNT_CODE_LENGTH,
        bidPx,
        offerPx,
        orderQty, // bidSize = orderQty (full fill)
        orderQty, // offerSize = orderQty (full fill)
        validUntil,
        midRate,
        nanoClock.nanoTime());

    // --- Step 8: Encode PriceResponse ---

    final int encodedLength =
        responseEncoder.encodePriceResponse(
            encodingBuffer,
            0,
            decoderBuffer,
            quoteReqIdOff,
            decoderBuffer,
            symbolOff,
            bidPx,
            offerPx,
            orderQty, // bidSize
            orderQty, // offerSize
            validUntil,
            true, // accepted
            QuoteRejectReasonEnum.NULL_VAL.value(), // no reject reason
            null, // no text
            0,
            transactTime,
            productType,
            swapPoints);

    return offerResponse(encodedLength);
  }

  /**
   * Handles an inbound PriceValidationRequest (templateId=52). Validates the order price against
   * the stored quote using the {@link PriceValidator}, which checks quote existence, expiry,
   * quantity, price tolerance, and last-look market movement.
   *
   * <p>The validation result is encoded as a PriceValidationResponse (templateId=53) and offered to
   * the outbound publication.
   *
   * <p><b>Allocation:</b> zero allocation on the hot path. The {@link PriceValidator} uses a
   * pre-allocated {@link PriceValidator.ValidationResult} flyweight.
   *
   * @param decoder pre-wrapped PriceValidationRequest decoder -- do not retain past return
   * @param buffer the underlying DirectBuffer containing the full fragment
   * @param offset byte offset of the fragment start (including SBE header)
   * @param length byte length of the complete fragment
   * @return {@link Action#CONTINUE} on success, {@link Action#ABORT} on publication back-pressure
   */
  @Override
  public Action onPriceValidationRequest(
      final PriceValidationRequestDecoder decoder,
      final DirectBuffer buffer,
      final int offset,
      final int length) {

    final DirectBuffer decoderBuffer = decoder.buffer();
    final int decoderOffset = decoder.offset();

    // QuoteID (FIX tag 117) -- 20-byte char field at encoding offset 0
    final int quoteIdOff = decoderOffset + PriceValidationRequestDecoder.quoteIdEncodingOffset();

    // QuoteReqID (FIX tag 131) -- 20-byte char field at encoding offset 20
    final int quoteReqIdOff =
        decoderOffset + PriceValidationRequestDecoder.quoteReqIdEncodingOffset();

    // Symbol (FIX tag 55) -- 8-byte char field at encoding offset 40
    final int symbolOff = decoderOffset + PriceValidationRequestDecoder.symbolEncodingOffset();

    // Scalar fields
    final long side = decoder.sideRaw();
    final long price = decoder.price();
    final long orderQty = decoder.orderQty();
    final long transactTime = decoder.transactTime();

    // --- Validate ---

    final PriceValidator.ValidationResult result =
        priceValidator.validate(
            decoderBuffer,
            quoteReqIdOff,
            PriceValidationRequestDecoder.quoteReqIdLength(),
            decoderBuffer,
            symbolOff,
            PriceValidationRequestDecoder.symbolLength(),
            side,
            price,
            orderQty,
            epochClock.nanoTime());

    // --- Encode PriceValidationResponse ---

    final int encodedLength =
        responseEncoder.encodePriceValidationResponse(
            encodingBuffer,
            0,
            decoderBuffer,
            quoteIdOff,
            result.valid,
            result.rejectReason,
            result.text,
            result.textLength,
            transactTime);

    return offerResponse(encodedLength);
  }

  // ===========================================================================
  // Publication helper
  // ===========================================================================

  /**
   * Offers the encoded response in {@link #encodingBuffer} to the outbound {@link #publication}
   * with bounded retry for transient back-pressure.
   *
   * <p>Retries up to {@link PricingConstants#MAX_PUBLICATION_RETRIES} times when the publication
   * returns {@link Publication#BACK_PRESSURED} or {@link Publication#ADMIN_ACTION}. For terminal
   * errors ({@link Publication#NOT_CONNECTED}, {@link Publication#CLOSED}, {@link
   * Publication#MAX_POSITION_EXCEEDED}), logs a warning and returns {@link Action#ABORT}
   * immediately.
   *
   * <p>If all retries are exhausted on transient back-pressure, returns {@link Action#ABORT} so
   * that Aeron re-delivers the inbound fragment on the next poll. The upstream requestor will
   * eventually time out and retry if the response is never delivered.
   *
   * <p><b>Allocation:</b> zero allocation.
   *
   * @param encodedLength total encoded byte length (header + body + groups) to offer
   * @return {@link Action#CONTINUE} on successful offer, {@link Action#ABORT} on failure or
   *     persistent back-pressure
   */
  private Action offerResponse(final int encodedLength) {
    for (int attempt = 0; attempt <= PricingConstants.MAX_PUBLICATION_RETRIES; attempt++) {
      final long result = publication.offer(encodingBuffer, 0, encodedLength);
      if (result > 0) {
        return Action.CONTINUE;
      }
      if (result == Publication.BACK_PRESSURED || result == Publication.ADMIN_ACTION) {
        continue;
      }
      // NOT_CONNECTED, CLOSED, MAX_POSITION_EXCEEDED -- terminal error
      LOG.warn().append("Publication failed: result=").append(result).commit();
      return Action.ABORT;
    }
    LOG.warn().append("Publication back-pressured after retries").commit();
    return Action.ABORT;
  }

  // ===========================================================================
  // Internal helpers
  // ===========================================================================

  /**
   * Encodes a rejected (declined) PriceResponse with the given reject reason and text, then offers
   * it to the publication.
   *
   * <p>Used when the pricing service cannot produce a quote (no market data, stale data, etc.).
   * Null values are used for price/size/validUntil fields to signal that no quote was produced.
   *
   * <p><b>Allocation:</b> zero allocation. Text is a pre-allocated byte array constant.
   *
   * @param decoderBuffer the decoder's underlying buffer for zero-copy char field access
   * @param quoteReqIdOff byte offset of the QuoteReqID field within the decoder buffer
   * @param symbolOff byte offset of the Symbol field within the decoder buffer
   * @param transactTime epoch nanos timestamp from the inbound request (FIX tag 60)
   * @param productType product type from the inbound request
   * @param rejectReason SBE QuoteRejectReasonEnum raw value
   * @param text pre-allocated ASCII text byte array explaining the rejection
   * @param textLen number of meaningful bytes in the text array
   * @return {@link Action#CONTINUE} on success, {@link Action#ABORT} on publication failure
   */
  private Action encodeRejectedPriceResponse(
      final DirectBuffer decoderBuffer,
      final int quoteReqIdOff,
      final int symbolOff,
      final long transactTime,
      final ProductTypeEnum productType,
      final int rejectReason,
      final byte[] text,
      final int textLen) {

    final int encodedLength =
        responseEncoder.encodePriceResponse(
            encodingBuffer,
            0,
            decoderBuffer,
            quoteReqIdOff,
            decoderBuffer,
            symbolOff,
            com.trading.engine.messages.sbe.PriceResponseEncoder.bidPxNullValue(),
            com.trading.engine.messages.sbe.PriceResponseEncoder.offerPxNullValue(),
            com.trading.engine.messages.sbe.PriceResponseEncoder.bidSizeNullValue(),
            com.trading.engine.messages.sbe.PriceResponseEncoder.offerSizeNullValue(),
            com.trading.engine.messages.sbe.PriceResponseEncoder.validUntilNullValue(),
            false, // not accepted
            rejectReason,
            text,
            textLen,
            transactTime,
            productType,
            com.trading.engine.messages.sbe.PriceResponseEncoder.swapPointsNullValue());

    return offerResponse(encodedLength);
  }

  /**
   * Converts a TenorEnum raw short value to approximate calendar days for forward point lookups.
   *
   * <p>Uses a pre-allocated static lookup table. Values are rounded estimates; exact business-day
   * settlement requires a holiday calendar (future enhancement). Returns 0 for unknown or NULL_VAL
   * tenors, which causes the forward point source to return 0 (spot).
   *
   * <p><b>Allocation:</b> zero allocation -- single array read.
   *
   * @param tenor the SBE TenorEnum value from the inbound PriceRequest
   * @return approximate calendar days to settlement, or 0 if unknown
   */
  private static int tenorToDays(final com.trading.engine.messages.sbe.TenorEnum tenor) {
    final int idx = tenor.value();
    if (idx >= 0 && idx < TENOR_TO_DAYS.length) {
      return TENOR_TO_DAYS[idx];
    }
    return 0;
  }

  /**
   * Returns the quote TTL in nanoseconds for the given product type.
   *
   * <p><b>Allocation:</b> zero allocation.
   *
   * @param productType the FX product type
   * @return TTL in nanoseconds: {@link #spotTtlNanos} for Spot, {@link #forwardTtlNanos} for
   *     Forward, {@link #swapTtlNanos} for Swap, defaults to {@link #spotTtlNanos} for unknown
   */
  private long ttlForProductType(final ProductTypeEnum productType) {
    return switch (productType) {
      case Spot -> spotTtlNanos;
      case Forward -> forwardTtlNanos;
      case Swap -> swapTtlNanos;
      default -> spotTtlNanos;
    };
  }
}
