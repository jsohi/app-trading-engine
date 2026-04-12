package com.trading.engine.pricing.quote;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.trading.engine.messages.FixedPointScale;
import com.trading.engine.pricing.ByteArrayKey;
import com.trading.engine.pricing.PricingMath;
import com.trading.engine.pricing.market.MarketDataAdapter;
import java.nio.charset.StandardCharsets;
import org.agrona.DirectBuffer;
import org.agrona.collections.Object2ObjectHashMap;

/**
 * Validates a price-validation request against a stored quote and the current market mid-rate,
 * applying per-symbol slippage tolerance and last-look protection.
 *
 * <h3>Validation pipeline</h3>
 *
 * <p>The {@link #validate} method executes a strict sequence of checks, short-circuiting on the
 * first failure. The order is chosen to fail fast on the cheapest checks:
 *
 * <ol>
 *   <li><b>Quote lookup</b> -- retrieve the stored {@link QuoteEntry} by {@code quoteReqId} (FIX
 *       tag 131). Reject with {@link RejectReason#QUOTE_NOT_FOUND} if absent.
 *   <li><b>Expiry</b> -- compare the quote's {@link QuoteEntry#validUntil} against the current
 *       epoch-nanos clock. Reject with {@link RejectReason#QUOTE_EXPIRED} if elapsed.
 *   <li><b>Quantity</b> -- ensure the order quantity does not exceed the quoted size for the given
 *       side (bid size for sells, offer size for buys). Reject with {@link
 *       RejectReason#QUANTITY_EXCEEDS_QUOTED_SIZE} if violated.
 *   <li><b>Price tolerance</b> -- check that the order price falls within the per-symbol slippage
 *       tolerance band around the quoted price. For buys, {@code price >= offerPx - tolerance}; for
 *       sells, {@code price <= bidPx + tolerance}. The tolerance is computed as {@code
 *       mulDiv(quotedPrice, toleranceBps, 10_000)}. Reject with {@link
 *       RejectReason#PRICE_OUTSIDE_TOLERANCE} if violated.
 *   <li><b>Last-look</b> -- compare the current market mid-rate (from {@link MarketDataAdapter})
 *       against the mid-rate snapshot recorded when the quote was created. If the absolute
 *       difference exceeds {@code mulDiv(quotedMid, lastLookBps, 10_000)}, reject with {@link
 *       RejectReason#MARKET_MOVED_BEYOND_LAST_LOOK}. This protects the market maker against latency
 *       arbitrage where a client holds a stale quote while the market moves.
 *   <li><b>Accept</b> -- all checks pass; set {@link ValidationResult#valid} to {@code true}.
 * </ol>
 *
 * <h3>Last-look mechanism</h3>
 *
 * <p>Last-look is a standard practice in FX ECN/SDP pricing (e.g., EBS Market, Currenex, FXall)
 * where the liquidity provider retains the right to reject an execution if the market has moved
 * adversely beyond a configured threshold during the hold period. The threshold is expressed in
 * basis points relative to the mid-rate at quote time and is configurable per symbol via {@link
 * #registerSlippage}.
 *
 * <h3>Side convention</h3>
 *
 * <p>Side values follow the SBE {@code SideEnum} definition (FIX tag 54):
 *
 * <ul>
 *   <li>{@code 1} = Buy -- client buys at the offer price
 *   <li>{@code 2} = Sell -- client sells at the bid price
 * </ul>
 *
 * <h3>Threading model</h3>
 *
 * <p><b>Not thread-safe.</b> All methods are invoked exclusively from the pricing-service agent's
 * single-threaded duty cycle. The mutable {@link #result} flyweight and the reusable {@link
 * #symbolProbeKey} are not safe for concurrent use.
 *
 * <h3>Allocation behaviour</h3>
 *
 * <p>Zero allocation after construction on the hot-path {@link #validate} method. The {@link
 * ValidationResult} is a pre-allocated mutable flyweight reused across calls. The {@link
 * #symbolProbeKey} is reused for slippage config lookups. {@link #registerSlippage} is a cold-path
 * method that allocates map keys and {@link SlippageConfig} instances at startup.
 *
 * @see QuoteManager
 * @see QuoteEntry
 * @see MarketDataAdapter
 */
public final class PriceValidator {

  private static final Log LOG = LogFactory.getLog(PriceValidator.class);

  /** SBE SideEnum value for Buy (FIX tag 54, value 1). */
  private static final long SIDE_BUY = 1;

  /** SBE SideEnum value for Sell (FIX tag 54, value 2). */
  private static final long SIDE_SELL = 2;

  // ---- Pre-allocated rejection text constants (zero-alloc on rejection path) ----

  private static final byte[] TEXT_INVALID_SIDE =
      "Invalid side value".getBytes(StandardCharsets.US_ASCII);
  private static final byte[] TEXT_QUOTE_NOT_FOUND =
      "Quote not found".getBytes(StandardCharsets.US_ASCII);
  private static final byte[] TEXT_QUOTE_EXPIRED =
      "Quote expired".getBytes(StandardCharsets.US_ASCII);
  private static final byte[] TEXT_QUANTITY_EXCEEDS =
      "Quantity exceeds quoted size".getBytes(StandardCharsets.US_ASCII);
  private static final byte[] TEXT_PRICE_OUTSIDE =
      "Price outside tolerance".getBytes(StandardCharsets.US_ASCII);
  private static final byte[] TEXT_MARKET_DATA_UNAVAILABLE =
      "Market data unavailable".getBytes(StandardCharsets.US_ASCII);
  private static final byte[] TEXT_MARKET_MOVED =
      "Market moved beyond last-look tolerance".getBytes(StandardCharsets.US_ASCII);

  /**
   * Basis-point divisor: 10,000. Used in {@link PricingMath#mulDiv} to convert a basis-point
   * tolerance into an absolute price amount: {@code tolerance = mulDiv(price, bps, 10_000)}.
   */
  private static final long BPS_DIVISOR = 10_000L;

  /** Maximum byte length of the SBE Symbol type, for probe key sizing. */
  private static final int SYMBOL_LENGTH = 8;

  /** Quote store used to look up active quotes by quoteReqId. */
  private final QuoteManager quoteManager;

  /** Market data source for current mid-rates, used in last-look validation. */
  private final MarketDataAdapter marketDataAdapter;

  /**
   * Per-symbol slippage configuration. Keys are owned {@link ByteArrayKey} instances allocated at
   * startup via {@link #registerSlippage}. Values are immutable {@link SlippageConfig} records.
   */
  private final Object2ObjectHashMap<ByteArrayKey, SlippageConfig> slippageConfigs;

  /** Default slippage config applied when a symbol has no explicit registration. */
  private final SlippageConfig defaultSlippage;

  /**
   * Reusable probe key for zero-allocation lookups into {@link #slippageConfigs}. Mutated in place
   * before each lookup. NEVER inserted into the map.
   */
  private final ByteArrayKey symbolProbeKey;

  /**
   * Pre-allocated, mutable validation result flyweight. Reused across all {@link #validate} calls
   * -- the caller must read the result before the next {@code validate} invocation.
   */
  private final ValidationResult result;

  /**
   * Constructs a price validator with the given quote store and market data source.
   *
   * <p><b>Allocation:</b> allocates the slippage config map, probe key, default slippage config,
   * and the validation result flyweight. This is a cold-path operation performed once at
   * pricing-service startup.
   *
   * @param quoteManager the quote store for active quote lookups; must not be {@code null}
   * @param marketDataAdapter the market data source for current mid-rates; must not be {@code null}
   */
  public PriceValidator(
      final QuoteManager quoteManager, final MarketDataAdapter marketDataAdapter) {
    this.quoteManager = quoteManager;
    this.marketDataAdapter = marketDataAdapter;
    this.slippageConfigs = new Object2ObjectHashMap<>(64, 0.55f);
    this.defaultSlippage = new SlippageConfig(5, 10); // 5 bps tolerance, 10 bps last-look
    this.symbolProbeKey = ByteArrayKey.emptyForLookup(SYMBOL_LENGTH);
    this.result = new ValidationResult();
  }

  /**
   * Registers per-symbol slippage tolerance and last-look threshold. This is a cold-path method
   * called at startup to configure the validator before the duty cycle begins.
   *
   * <p>If the symbol is already registered, the existing config is replaced.
   *
   * <p><b>Allocation:</b> allocates an owned {@link ByteArrayKey} and a new {@link SlippageConfig}
   * for map storage. Cold-path only.
   *
   * @param symbol the symbol bytes (must be {@code <= 8} bytes, matching SBE Symbol type)
   * @param toleranceBps maximum allowed price deviation from the quoted price, in basis points;
   *     must be {@code >= 0}
   * @param lastLookBps maximum allowed mid-rate movement since quote time, in basis points; must be
   *     {@code >= 0}
   */
  public void registerSlippage(final byte[] symbol, final int toleranceBps, final int lastLookBps) {
    final ByteArrayKey key = ByteArrayKey.copyOf(symbol, 0, symbol.length);
    slippageConfigs.put(key, new SlippageConfig(toleranceBps, lastLookBps));
  }

  /**
   * Validates a price-validation request against the stored quote and current market conditions.
   *
   * <p>Executes the five-step validation pipeline described in the class Javadoc. Returns a
   * reference to the pre-allocated {@link ValidationResult} flyweight, which is valid until the
   * next call to this method.
   *
   * <p><b>Allocation:</b> zero allocation. All intermediate computations use {@code long}
   * arithmetic via {@link PricingMath#mulDiv}.
   *
   * @param quoteReqId buffer containing the QuoteReqID bytes (FIX tag 131)
   * @param qrOff start offset of the QuoteReqID within the buffer
   * @param qrLen number of meaningful bytes in the QuoteReqID
   * @param symbol buffer containing the Symbol bytes (FIX tag 55)
   * @param sOff start offset of the Symbol within the buffer
   * @param sLen number of meaningful bytes in the Symbol
   * @param side order side: {@code 1} = Buy, {@code 2} = Sell (SBE SideEnum / FIX tag 54)
   * @param price order price in fixed-point {@code 10^-8}
   * @param orderQty order quantity in fixed-point {@code 10^-8}
   * @param nowEpochNanos current epoch nanosecond time for expiry checks
   * @return the pre-allocated {@link ValidationResult}; valid until the next {@code validate} call
   */
  public ValidationResult validate(
      final DirectBuffer quoteReqId,
      final int qrOff,
      final int qrLen,
      final DirectBuffer symbol,
      final int sOff,
      final int sLen,
      final long side,
      final long price,
      final long orderQty,
      final long nowEpochNanos) {

    // Step 0: Validate side.
    if (side != SIDE_BUY && side != SIDE_SELL) {
      result.setRejected(RejectReason.INVALID_REQUEST, TEXT_INVALID_SIDE);
      return result;
    }

    // Step 1: Lookup quote by quoteReqId.
    final QuoteEntry quote = quoteManager.lookup(quoteReqId, qrOff, qrLen);
    if (quote == null) {
      result.setRejected(RejectReason.QUOTE_NOT_FOUND, TEXT_QUOTE_NOT_FOUND);
      return result;
    }

    // Step 2: Check expiry.
    if (quote.isExpired(nowEpochNanos)) {
      result.setRejected(RejectReason.QUOTE_EXPIRED, TEXT_QUOTE_EXPIRED);
      return result;
    }

    // Step 3: Check quantity against quoted size for the given side.
    if (side == SIDE_BUY) {
      if (orderQty > quote.offerSize) {
        result.setRejected(RejectReason.QUANTITY_EXCEEDS_QUOTED_SIZE, TEXT_QUANTITY_EXCEEDS);
        return result;
      }
    } else if (side == SIDE_SELL) {
      if (orderQty > quote.bidSize) {
        result.setRejected(RejectReason.QUANTITY_EXCEEDS_QUOTED_SIZE, TEXT_QUANTITY_EXCEEDS);
        return result;
      }
    }

    // Resolve per-symbol slippage config.
    symbolProbeKey.set(symbol, sOff, sLen);
    final SlippageConfig slippage = slippageConfigs.getOrDefault(symbolProbeKey, defaultSlippage);

    // Step 4: Check price tolerance.
    if (side == SIDE_BUY) {
      // Buyer's price must be >= offerPx - tolerance.
      final long tolerance = PricingMath.mulDiv(quote.offerPx, slippage.toleranceBps, BPS_DIVISOR);
      if (price < quote.offerPx - tolerance) {
        result.setRejected(RejectReason.PRICE_OUTSIDE_TOLERANCE, TEXT_PRICE_OUTSIDE);
        return result;
      }
    } else if (side == SIDE_SELL) {
      // Seller's price must be <= bidPx + tolerance.
      final long tolerance = PricingMath.mulDiv(quote.bidPx, slippage.toleranceBps, BPS_DIVISOR);
      if (price > quote.bidPx + tolerance) {
        result.setRejected(RejectReason.PRICE_OUTSIDE_TOLERANCE, TEXT_PRICE_OUTSIDE);
        return result;
      }
    }

    // Step 5: Last-look — compare current mid against mid at quote time.
    final long currentMid = marketDataAdapter.midRate(symbol, sOff, sLen);
    if (currentMid == FixedPointScale.PRICE_NOT_AVAILABLE) {
      // If market data is unavailable, reject conservatively.
      result.setRejected(RejectReason.MARKET_MOVED_BEYOND_LAST_LOOK, TEXT_MARKET_DATA_UNAVAILABLE);
      return result;
    }

    final long quotedMid = quote.midRateAtQuoteTime;
    final long lastLookTolerance = PricingMath.mulDiv(quotedMid, slippage.lastLookBps, BPS_DIVISOR);
    final long midDelta = Math.abs(currentMid - quotedMid);
    if (midDelta > lastLookTolerance) {
      result.setRejected(RejectReason.MARKET_MOVED_BEYOND_LAST_LOOK, TEXT_MARKET_MOVED);
      return result;
    }

    // Step 6: All checks passed.
    result.setValid();
    return result;
  }

  /**
   * Structured rejection reason codes that map to SBE {@code RejectReasonEnum} values defined in
   * {@code trading-schema.xml}. These codes are written into the {@code PriceValidationResponse}
   * SBE message to give the cluster a machine-readable rejection reason.
   *
   * <p>Values are chosen to align with the schema enum where a direct mapping exists (e.g., {@link
   * #QUOTE_NOT_FOUND} = 6, {@link #QUOTE_EXPIRED} = 7 from RejectReasonEnum). Pricing- specific
   * codes that have no schema counterpart use values in the 100+ range to avoid collisions.
   */
  public static final class RejectReason {

    /** Invalid request parameter (e.g., unknown side value). Pricing-specific code (101). */
    public static final int INVALID_REQUEST = 101;

    /** Quote not found in the active quotes store. Maps to RejectReasonEnum.QuoteNotFound (6). */
    public static final int QUOTE_NOT_FOUND = 6;

    /** Quote has expired (validUntil elapsed). Maps to RejectReasonEnum.QuoteExpired (7). */
    public static final int QUOTE_EXPIRED = 7;

    /**
     * Order quantity exceeds the quoted size for the given side. Maps to
     * RejectReasonEnum.InsufficientQuantity (2).
     */
    public static final int QUANTITY_EXCEEDS_QUOTED_SIZE = 2;

    /**
     * Order price outside the allowed tolerance band. Maps to RejectReasonEnum.InvalidPrice (3).
     */
    public static final int PRICE_OUTSIDE_TOLERANCE = 3;

    /**
     * Market mid-rate has moved beyond the last-look tolerance since the quote was created.
     * Pricing-specific code with no direct schema mapping -- uses value 100 to avoid collisions
     * with the RejectReasonEnum range (1..19).
     */
    public static final int MARKET_MOVED_BEYOND_LAST_LOOK = 100;

    private RejectReason() {}
  }

  /**
   * Mutable flyweight holding the result of a single {@link #validate} invocation. A single
   * instance is pre-allocated in {@link PriceValidator} and reused across calls.
   *
   * <p>The result is valid from the moment {@link #setValid()} or {@link #setRejected(int, String)}
   * is called until the next {@code validate} invocation overwrites it.
   *
   * <h3>Threading model</h3>
   *
   * <p><b>Not thread-safe.</b> Single-threaded access from the pricing-service duty cycle.
   *
   * <h3>Allocation behaviour</h3>
   *
   * <p>Zero allocation after construction. {@link #setRejected(int, String)} encodes the text into
   * the pre-allocated byte array without allocating intermediate objects (the source {@link String}
   * is a compile-time constant, not a runtime-constructed string).
   */
  public static final class ValidationResult {

    /** Maximum byte length of the rejection text, matching SBE Text type (64 bytes). */
    static final int TEXT_LENGTH = 64;

    /**
     * {@code true} if the validation passed all checks; {@code false} if rejected. Read by the
     * response encoder to set the {@code valid} field in the PriceValidationResponse SBE message.
     */
    public boolean valid;

    /**
     * Structured rejection reason code. Only meaningful when {@link #valid} is {@code false}. Maps
     * to SBE {@code RejectReasonEnum} values or pricing-specific codes in the 100+ range.
     */
    public int rejectReason;

    /**
     * Pre-allocated byte array for the rejection text, matching the SBE {@code Text} type (64
     * bytes). Encoded as ASCII. Only meaningful when {@link #valid} is {@code false}.
     */
    public final byte[] text = new byte[TEXT_LENGTH];

    /**
     * Actual number of meaningful bytes in {@link #text}. The SBE field is fixed-width (64 bytes)
     * but the message may be shorter; trailing bytes are zero-padded.
     */
    public int textLength;

    /** Constructs a validation result flyweight. All fields are zeroed (invalid/empty state). */
    ValidationResult() {
      // Fields are zero-initialised by the JVM.
    }

    /**
     * Marks the result as valid (all checks passed). Clears the rejection reason and text.
     *
     * <p><b>Allocation:</b> zero allocation.
     */
    public void setValid() {
      this.valid = true;
      this.rejectReason = 0;
      this.textLength = 0;
    }

    /**
     * Marks the result as rejected with the given reason code and pre-allocated text bytes.
     *
     * <p>The {@code textBytes} parameter must be a {@code static final byte[]} constant
     * pre-computed at class load time — never a runtime-allocated array. This ensures zero
     * allocation on the rejection path, which can be frequent under sustained last-look rejections
     * in volatile markets.
     *
     * <p><b>Allocation:</b> zero allocation. Copies from the pre-allocated source array into the
     * pre-allocated destination array via {@link System#arraycopy}.
     *
     * @param reason the structured rejection reason code (from {@link RejectReason})
     * @param textBytes pre-allocated ASCII text bytes (from a {@code static final byte[]} constant)
     */
    public void setRejected(final int reason, final byte[] textBytes) {
      this.valid = false;
      this.rejectReason = reason;
      this.textLength = Math.min(textBytes.length, TEXT_LENGTH);
      System.arraycopy(textBytes, 0, this.text, 0, this.textLength);
      // Zero-fill the remainder to avoid leaking stale text from a prior rejection.
      for (int i = this.textLength; i < TEXT_LENGTH; i++) {
        this.text[i] = 0;
      }
    }
  }

  /**
   * Per-symbol slippage and last-look configuration. Instances are created at startup via {@link
   * PriceValidator#registerSlippage} and stored in the slippage config map. Fields are {@code
   * final} -- effectively immutable after construction.
   *
   * <p>Package-private visibility: accessed only by {@link PriceValidator}.
   */
  static final class SlippageConfig {

    /**
     * Maximum allowed deviation of the order price from the quoted price, in basis points. Applied
     * symmetrically: buyers may pay up to {@code toleranceBps} below the offer, sellers may receive
     * up to {@code toleranceBps} above the bid.
     */
    final int toleranceBps;

    /**
     * Maximum allowed movement of the market mid-rate since quote time, in basis points. If the
     * absolute change in the mid exceeds this threshold, the execution is rejected under last-look
     * protection.
     */
    final int lastLookBps;

    /**
     * Constructs a slippage config.
     *
     * @param toleranceBps price tolerance in basis points; must be {@code >= 0}
     * @param lastLookBps last-look threshold in basis points; must be {@code >= 0}
     */
    SlippageConfig(final int toleranceBps, final int lastLookBps) {
      this.toleranceBps = toleranceBps;
      this.lastLookBps = lastLookBps;
    }
  }
}
