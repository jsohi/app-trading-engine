package com.trading.engine.pricing.codec;

import com.trading.engine.messages.sbe.BooleanType;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.PriceResponseEncoder;
import com.trading.engine.messages.sbe.PriceValidationResponseEncoder;
import com.trading.engine.messages.sbe.ProductTypeEnum;
import com.trading.engine.messages.sbe.QuoteRejectReasonEnum;
import com.trading.engine.messages.sbe.RejectReasonEnum;
import com.trading.engine.messages.sbe.SettlTypeEnum;
import com.trading.engine.messages.sbe.SideEnum;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * Zero-allocation SBE response encoder for the pricing service. All flyweight encoders are
 * pre-allocated as instance fields.
 *
 * <p>This class encodes two outbound message types:
 *
 * <ul>
 *   <li><b>PriceResponse</b> (templateId=51) — the pricing service's answer to a {@code
 *       PriceRequest}. Contains bid/offer prices and sizes, expiry, accept/decline flag, and
 *       optional per-leg pricing for FX swaps.
 *   <li><b>PriceValidationResponse</b> (templateId=53) — the pricing service's answer to a {@code
 *       PriceValidationRequest}. Contains a valid/invalid flag, structured reject reason, and
 *       free-text explanation.
 * </ul>
 *
 * <p><b>Encoding pattern.</b> Each {@code encode*} method uses the SBE {@code wrapAndApplyHeader}
 * pattern: the header encoder writes the SBE message header (blockLength, templateId, schemaId,
 * version) at the given offset, then the body encoder writes fixed-length fields immediately after.
 * For messages with repeating groups (PriceResponse NoLegs), the group header and entries follow
 * the fixed block. The method returns the total encoded length (header + body + groups) so the
 * caller can offer exactly that many bytes to the Aeron publication.
 *
 * <p><b>DirectBuffer char-field copying.</b> SBE's generated {@code putXxx(byte[], int)} setters
 * require a {@code byte[]} source. Callers pass char-field data as {@link DirectBuffer} references
 * (pointing into a decoded request's underlying buffer) to avoid intermediate copies. This encoder
 * copies from the {@link DirectBuffer} into the pre-allocated {@link #charScratch} byte array, then
 * passes the scratch to the SBE setter. The scratch is sized to accommodate the largest SBE char
 * field in the encoded messages (Text = 64 bytes).
 *
 * <p><b>Text field convention.</b> The {@code text} parameter is a {@code byte[]} because reject
 * text is typically built from pre-allocated ASCII byte arrays (same pattern as {@code
 * FixSessionHandler.rejectTextScratch} in the gateway module). Callers supply the text content
 * length; the encoder null-pads the remainder of the 64-byte SBE field via the {@link #charScratch}
 * buffer.
 *
 * <p><b>Allocation.</b> Zero allocation after construction. The three SBE encoder flyweights, the
 * shared {@link MessageHeaderEncoder}, and the {@link #charScratch} / {@link #textScratch} buffers
 * are all pre-allocated at construction time and reused on every encode call.
 *
 * <p><b>Threading.</b> Not thread-safe — single-threaded pricing-service duty-cycle thread only.
 *
 * @see PricingMessageDispatcher
 */
public final class PricingResponseEncoder {

  /**
   * Scratch buffer for copying DirectBuffer char fields into byte[] for SBE putXxx setters. Sized
   * for the largest char field written by this encoder: QuoteReqID (20 bytes), Symbol (8), QuoteID
   * (20), SettlDate (8), Currency (3). The maximum is 20 bytes, but we use 24 for alignment
   * headroom.
   */
  private static final int CHAR_SCRATCH_LEN = 24;

  /**
   * Text scratch buffer sized to the SBE Text field length (64 bytes). Used to null-pad caller-
   * supplied text bytes before passing to the SBE {@code putText} setter, which requires exactly 64
   * source bytes.
   */
  private static final int TEXT_SCRATCH_LEN = PriceResponseEncoder.textLength();

  // Class-init sanity check: verify scratch buffers are large enough for all SBE char fields
  // this encoder writes. Belt-and-braces — the runtime copyField helper also checks.
  static {
    final int maxCharField =
        Math.max(
            PriceResponseEncoder.quoteReqIdLength(),
            Math.max(
                PriceResponseEncoder.symbolLength(),
                Math.max(
                    PriceResponseEncoder.NoLegsEncoder.legSettlDateLength(),
                    Math.max(
                        PriceResponseEncoder.NoLegsEncoder.legCurrencyLength(),
                        PriceValidationResponseEncoder.quoteIdLength()))));
    if (maxCharField > CHAR_SCRATCH_LEN) {
      throw new IllegalStateException(
          "PricingResponseEncoder CHAR_SCRATCH_LEN="
              + CHAR_SCRATCH_LEN
              + " too small for SBE char field "
              + maxCharField);
    }
    if (PriceResponseEncoder.textLength() != TEXT_SCRATCH_LEN) {
      throw new IllegalStateException(
          "TEXT_SCRATCH_LEN mismatch: expected="
              + PriceResponseEncoder.textLength()
              + " actual="
              + TEXT_SCRATCH_LEN);
    }
  }

  // --- Pre-allocated scratch buffers (zero-alloc char-field and text copying) ---

  /** Scratch for copying DirectBuffer char fields to byte[] for SBE putXxx setters. */
  private final byte[] charScratch = new byte[CHAR_SCRATCH_LEN];

  /**
   * Scratch for null-padding text fields. The SBE {@code putText(byte[], int)} setter requires
   * exactly {@link #TEXT_SCRATCH_LEN} bytes from the source offset; this buffer holds the caller's
   * text bytes followed by null padding.
   */
  private final byte[] textScratch = new byte[TEXT_SCRATCH_LEN];

  // --- Pre-allocated SBE flyweight encoders ---

  /** Shared SBE message header encoder; reused across all encode methods. */
  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();

  /** SBE encoder for PriceResponse (templateId=51). */
  private final PriceResponseEncoder priceResponseEncoder = new PriceResponseEncoder();

  /** SBE encoder for PriceValidationResponse (templateId=53). */
  private final PriceValidationResponseEncoder validationResponseEncoder =
      new PriceValidationResponseEncoder();

  /** Construct a response encoder with pre-allocated flyweights and scratch buffers. */
  public PricingResponseEncoder() {}

  // ===========================================================================
  // PriceResponse (templateId=51) — single-leg (no NoLegs group)
  // ===========================================================================

  /**
   * Encode a PriceResponse (templateId=51) without leg-level pricing. The NoLegs repeating group is
   * written with a count of zero.
   *
   * <p>When {@code accepted} is {@code false}, the caller should pass {@link
   * PriceResponseEncoder#bidPxNullValue()} (and equivalent) for the price/size/validUntil fields,
   * and supply a meaningful {@code quoteRejectReason} and {@code text}. When {@code accepted} is
   * {@code true}, {@code quoteRejectReason} is forced to {@link QuoteRejectReasonEnum#NULL_VAL}
   * regardless of the caller's value.
   *
   * @param buffer target buffer to encode into — must have capacity for at least {@link
   *     com.trading.engine.pricing.PricingConstants#ENCODING_BUFFER_SIZE} bytes from {@code offset}
   * @param offset byte offset in {@code buffer} at which to start encoding
   * @param quoteReqId DirectBuffer containing the QuoteReqID bytes (FIX tag 131, 20 bytes)
   * @param qrOff byte offset within {@code quoteReqId} of the first QuoteReqID byte
   * @param symbol DirectBuffer containing the Symbol bytes (FIX tag 55, 8 bytes)
   * @param sOff byte offset within {@code symbol} of the first Symbol byte
   * @param bidPx bid price, fixed-point 10^-8 (use {@link PriceResponseEncoder#bidPxNullValue()}
   *     when declined)
   * @param offerPx offer price, fixed-point 10^-8 (use null value when declined)
   * @param bidSize bid quantity, fixed-point 10^-8 (use null value when declined)
   * @param offerSize offer quantity, fixed-point 10^-8 (use null value when declined)
   * @param validUntil quote expiry in epoch nanos (use {@link
   *     PriceResponseEncoder#validUntilNullValue()} when declined)
   * @param accepted {@code true} if the pricing service produced a quote; {@code false} to decline
   * @param quoteRejectReason structured decline reason (FIX tag 658) when {@code accepted} is
   *     {@code false}; ignored when {@code accepted} is {@code true}
   * @param text free-text explanation bytes (ASCII); may be {@code null} or empty when {@code
   *     accepted} is {@code true}
   * @param textLen number of significant bytes in {@code text} to encode (0 if null/empty)
   * @param transactTime epoch nanos timestamp (FIX tag 60)
   * @param productType FX product classification (FIX custom tag 10013)
   * @param swapPoints far-near price differential for swaps, fixed-point 10^-8 (use {@link
   *     PriceResponseEncoder#swapPointsNullValue()} for non-swap products)
   * @return total encoded length in bytes (header + body + empty group header)
   */
  public int encodePriceResponse(
      final MutableDirectBuffer buffer,
      final int offset,
      final DirectBuffer quoteReqId,
      final int qrOff,
      final DirectBuffer symbol,
      final int sOff,
      final long bidPx,
      final long offerPx,
      final long bidSize,
      final long offerSize,
      final long validUntil,
      final boolean accepted,
      final int quoteRejectReason,
      final byte[] text,
      final int textLen,
      final long transactTime,
      final ProductTypeEnum productType,
      final long swapPoints) {

    priceResponseEncoder.wrapAndApplyHeader(buffer, offset, headerEncoder);

    // QuoteReqID (tag 131) — 20-byte fixed char field
    copyField(quoteReqId, qrOff, PriceResponseEncoder.quoteReqIdLength());
    priceResponseEncoder.putQuoteReqId(charScratch, 0);

    // Symbol (tag 55) — 8-byte fixed char field
    copyField(symbol, sOff, PriceResponseEncoder.symbolLength());
    priceResponseEncoder.putSymbol(charScratch, 0);

    // Prices and sizes — fixed-point 10^-8
    priceResponseEncoder.bidPx(bidPx);
    priceResponseEncoder.offerPx(offerPx);
    priceResponseEncoder.bidSize(bidSize);
    priceResponseEncoder.offerSize(offerSize);
    priceResponseEncoder.validUntil(validUntil);

    // Accepted flag — BooleanType enum (tag 10034)
    priceResponseEncoder.accepted(accepted ? BooleanType.True : BooleanType.False);

    // QuoteRejectReason (tag 658) — set only when declined; force NULL_VAL when accepted
    // to prevent stale reject reasons from leaking through a reused encoder.
    if (accepted) {
      priceResponseEncoder.quoteRejectReason(QuoteRejectReasonEnum.NULL_VAL);
    } else {
      priceResponseEncoder.quoteRejectReason(QuoteRejectReasonEnum.get((short) quoteRejectReason));
    }

    // TransactTime (tag 60)
    priceResponseEncoder.transactTime(transactTime);

    // Text (tag 58) — 64-byte fixed char field, null-padded
    padText(text, textLen);
    priceResponseEncoder.putText(textScratch, 0);

    // ProductType (custom tag 10013)
    priceResponseEncoder.productType(productType);

    // SwapPoints (custom tag 10003)
    priceResponseEncoder.swapPoints(swapPoints);

    // NoLegs group — empty for single-leg
    priceResponseEncoder.noLegsCount(0);

    return MessageHeaderEncoder.ENCODED_LENGTH + priceResponseEncoder.encodedLength();
  }

  // ===========================================================================
  // PriceResponse with legs (templateId=51) — swap pricing
  // ===========================================================================

  /**
   * Encode a PriceResponse (templateId=51) with per-leg pricing for FX swaps. Identical to {@link
   * #encodePriceResponse} for the fixed-block fields, but additionally encodes the NoLegs repeating
   * group with per-leg side, settlement, currency, bid/offer prices and sizes.
   *
   * <p>All array parameters must have at least {@code legCount} elements. The caller is responsible
   * for ensuring arrays are pre-allocated and reused across calls to maintain the zero-allocation
   * contract.
   *
   * @param buffer target buffer to encode into
   * @param offset byte offset in {@code buffer} at which to start encoding
   * @param quoteReqId DirectBuffer containing the QuoteReqID bytes (FIX tag 131, 20 bytes)
   * @param qrOff byte offset within {@code quoteReqId}
   * @param symbol DirectBuffer containing the Symbol bytes (FIX tag 55, 8 bytes)
   * @param sOff byte offset within {@code symbol}
   * @param bidPx bid price, fixed-point 10^-8
   * @param offerPx offer price, fixed-point 10^-8
   * @param bidSize bid quantity, fixed-point 10^-8
   * @param offerSize offer quantity, fixed-point 10^-8
   * @param validUntil quote expiry in epoch nanos
   * @param accepted {@code true} if the pricing service produced a quote
   * @param quoteRejectReason structured decline reason when {@code accepted} is {@code false}
   * @param text free-text explanation bytes (ASCII); may be {@code null}
   * @param textLen number of significant bytes in {@code text}
   * @param transactTime epoch nanos timestamp (FIX tag 60)
   * @param productType FX product classification
   * @param swapPoints far-near price differential, fixed-point 10^-8
   * @param legSide per-leg side enums (FIX tag 624)
   * @param legSettlDate per-leg settlement date DirectBuffers (FIX tag 588, 8 bytes each)
   * @param legSettlDateOff per-leg byte offsets within each {@code legSettlDate} buffer
   * @param legSettlType per-leg settlement type enums (FIX tag 587)
   * @param legCurrency per-leg currency DirectBuffers (FIX tag 556, 3 bytes each)
   * @param legCurrencyOff per-leg byte offsets within each {@code legCurrency} buffer
   * @param legBidPx per-leg bid prices, fixed-point 10^-8 (custom tag 10004)
   * @param legOfferPx per-leg offer prices, fixed-point 10^-8 (custom tag 10005)
   * @param legBidSize per-leg bid sizes, fixed-point 10^-8 (custom tag 10006)
   * @param legOfferSize per-leg offer sizes, fixed-point 10^-8 (custom tag 10007)
   * @param legCount number of legs to encode (typically 2 for an FX swap)
   * @return total encoded length in bytes (header + body + group header + leg entries)
   */
  public int encodePriceResponseWithLegs(
      final MutableDirectBuffer buffer,
      final int offset,
      final DirectBuffer quoteReqId,
      final int qrOff,
      final DirectBuffer symbol,
      final int sOff,
      final long bidPx,
      final long offerPx,
      final long bidSize,
      final long offerSize,
      final long validUntil,
      final boolean accepted,
      final int quoteRejectReason,
      final byte[] text,
      final int textLen,
      final long transactTime,
      final ProductTypeEnum productType,
      final long swapPoints,
      final SideEnum[] legSide,
      final DirectBuffer[] legSettlDate,
      final int[] legSettlDateOff,
      final SettlTypeEnum[] legSettlType,
      final DirectBuffer[] legCurrency,
      final int[] legCurrencyOff,
      final long[] legBidPx,
      final long[] legOfferPx,
      final long[] legBidSize,
      final long[] legOfferSize,
      final int legCount) {

    priceResponseEncoder.wrapAndApplyHeader(buffer, offset, headerEncoder);

    // --- Fixed-block fields (same as encodePriceResponse) ---

    copyField(quoteReqId, qrOff, PriceResponseEncoder.quoteReqIdLength());
    priceResponseEncoder.putQuoteReqId(charScratch, 0);

    copyField(symbol, sOff, PriceResponseEncoder.symbolLength());
    priceResponseEncoder.putSymbol(charScratch, 0);

    priceResponseEncoder.bidPx(bidPx);
    priceResponseEncoder.offerPx(offerPx);
    priceResponseEncoder.bidSize(bidSize);
    priceResponseEncoder.offerSize(offerSize);
    priceResponseEncoder.validUntil(validUntil);
    priceResponseEncoder.accepted(accepted ? BooleanType.True : BooleanType.False);

    if (accepted) {
      priceResponseEncoder.quoteRejectReason(QuoteRejectReasonEnum.NULL_VAL);
    } else {
      priceResponseEncoder.quoteRejectReason(QuoteRejectReasonEnum.get((short) quoteRejectReason));
    }

    priceResponseEncoder.transactTime(transactTime);

    padText(text, textLen);
    priceResponseEncoder.putText(textScratch, 0);

    priceResponseEncoder.productType(productType);
    priceResponseEncoder.swapPoints(swapPoints);

    // --- NoLegs repeating group ---
    final var legs = priceResponseEncoder.noLegsCount(legCount);
    for (int i = 0; i < legCount; i++) {
      legs.next();
      legs.legSide(legSide[i]);

      // LegSettlDate (tag 588) — 8-byte fixed char field
      copyField(
          legSettlDate[i],
          legSettlDateOff[i],
          PriceResponseEncoder.NoLegsEncoder.legSettlDateLength());
      legs.putLegSettlDate(charScratch, 0);

      // LegSettlType (tag 587) — optional enum
      legs.legSettlType(legSettlType[i]);

      // LegCurrency (tag 556) — 3-byte fixed char field
      copyField(
          legCurrency[i],
          legCurrencyOff[i],
          PriceResponseEncoder.NoLegsEncoder.legCurrencyLength());
      legs.putLegCurrency(charScratch, 0);

      // Per-leg prices and sizes (custom tags 10004-10007) — fixed-point 10^-8
      legs.legBidPx(legBidPx[i]);
      legs.legOfferPx(legOfferPx[i]);
      legs.legBidSize(legBidSize[i]);
      legs.legOfferSize(legOfferSize[i]);
    }

    return MessageHeaderEncoder.ENCODED_LENGTH + priceResponseEncoder.encodedLength();
  }

  // ===========================================================================
  // PriceValidationResponse (templateId=53)
  // ===========================================================================

  /**
   * Encode a PriceValidationResponse (templateId=53). Returns the total encoded length (header +
   * body).
   *
   * <p>When {@code valid} is {@code true}, the {@code rejectReason} field is forced to {@link
   * RejectReasonEnum#NULL_VAL} and the text field is zeroed regardless of the caller's values. When
   * {@code valid} is {@code false}, the caller should supply a meaningful {@code rejectReason} and
   * optional text explanation.
   *
   * @param buffer target buffer to encode into
   * @param offset byte offset in {@code buffer} at which to start encoding
   * @param quoteId DirectBuffer containing the QuoteID bytes (FIX tag 117, 20 bytes)
   * @param qIdOff byte offset within {@code quoteId} of the first QuoteID byte
   * @param valid {@code true} if the order price is acceptable to the pricing service
   * @param rejectReason structured rejection reason (custom tag 10022) when {@code valid} is {@code
   *     false}; ignored when {@code valid} is {@code true}
   * @param text free-text rejection reason bytes (ASCII); may be {@code null} when valid
   * @param textLen number of significant bytes in {@code text} (0 if null/empty)
   * @param transactTime epoch nanos timestamp (FIX tag 60)
   * @return total encoded length in bytes (header + body)
   */
  public int encodePriceValidationResponse(
      final MutableDirectBuffer buffer,
      final int offset,
      final DirectBuffer quoteId,
      final int qIdOff,
      final boolean valid,
      final int rejectReason,
      final byte[] text,
      final int textLen,
      final long transactTime) {

    validationResponseEncoder.wrapAndApplyHeader(buffer, offset, headerEncoder);

    // QuoteID (tag 117) — 20-byte fixed char field
    copyField(quoteId, qIdOff, PriceValidationResponseEncoder.quoteIdLength());
    validationResponseEncoder.putQuoteId(charScratch, 0);

    // Valid flag — BooleanType enum (custom tag 10035)
    validationResponseEncoder.valid(valid ? BooleanType.True : BooleanType.False);

    // RejectReason (custom tag 10022) — set only when invalid; force NULL_VAL when valid
    if (valid) {
      validationResponseEncoder.rejectReason(RejectReasonEnum.NULL_VAL);
    } else {
      validationResponseEncoder.rejectReason(RejectReasonEnum.get((short) rejectReason));
    }

    // Text (tag 58) — 64-byte fixed char field, null-padded
    if (valid) {
      // Clear text when valid — prevent stale reject text from leaking through reused scratch
      padText(null, 0);
    } else {
      padText(text, textLen);
    }
    validationResponseEncoder.putText(textScratch, 0);

    // TransactTime (tag 60)
    validationResponseEncoder.transactTime(transactTime);

    return MessageHeaderEncoder.ENCODED_LENGTH + validationResponseEncoder.encodedLength();
  }

  // ===========================================================================
  // Internal helpers — zero allocation
  // ===========================================================================

  /**
   * Copy {@code fieldLen} bytes from a {@link DirectBuffer} at {@code srcOff} into the
   * pre-allocated {@link #charScratch} buffer. The scratch is used as the source for the subsequent
   * SBE {@code putXxx(byte[], int)} call. This avoids allocating a temporary {@code byte[]} on
   * every field write.
   *
   * @param src source buffer containing the char-field bytes
   * @param srcOff byte offset within {@code src} of the first field byte
   * @param fieldLen SBE field length in bytes (must be <= {@link #CHAR_SCRATCH_LEN})
   * @throws IllegalStateException if {@code fieldLen} exceeds {@link #CHAR_SCRATCH_LEN}
   */
  private void copyField(final DirectBuffer src, final int srcOff, final int fieldLen) {
    if (fieldLen > CHAR_SCRATCH_LEN) {
      throw new IllegalStateException(
          "SBE char field length " + fieldLen + " exceeds CHAR_SCRATCH_LEN " + CHAR_SCRATCH_LEN);
    }
    src.getBytes(srcOff, charScratch, 0, fieldLen);
  }

  /**
   * Copy up to {@code textLen} bytes from the caller's text array into the pre-allocated {@link
   * #textScratch} buffer, null-padding the remainder to fill the full SBE Text field length (64
   * bytes). If {@code text} is null or {@code textLen} is zero, the entire scratch is zeroed.
   *
   * @param text source text bytes (ASCII); may be null
   * @param textLen number of significant bytes to copy from {@code text} (clamped to {@link
   *     #TEXT_SCRATCH_LEN})
   */
  private void padText(final byte[] text, final int textLen) {
    final int copyLen = (text != null) ? Math.min(textLen, TEXT_SCRATCH_LEN) : 0;
    if (copyLen > 0) {
      System.arraycopy(text, 0, textScratch, 0, copyLen);
    }
    // Null-pad the remainder so the SBE field contains clean bytes, not stale data from a
    // previous encode call.
    for (int i = copyLen; i < TEXT_SCRATCH_LEN; i++) {
      textScratch[i] = 0;
    }
  }
}
