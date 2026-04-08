package com.trading.engine.gateway;

import com.trading.engine.fix.decoder_flyweight.MultilegOrderCancelReplaceRequestDecoder;
import com.trading.engine.fix.decoder_flyweight.NewOrderMultilegDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.MultilegOrderCancelReplaceEncoder;
import com.trading.engine.messages.sbe.NewOrderMultilegEncoder;
import com.trading.engine.messages.sbe.OrdTypeEnum;
import com.trading.engine.messages.sbe.ProductTypeEnum;
import com.trading.engine.messages.sbe.SettlTypeEnum;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.messages.sbe.TenorEnum;
import com.trading.engine.messages.sbe.TimeInForceEnum;
import org.agrona.MutableDirectBuffer;
import uk.co.real_logic.artio.fields.UtcTimestampDecoder;

/**
 * Translator for the two multileg FIX 4.4 message types — {@code NewOrderMultileg} (35=AB) and
 * {@code MultilegOrderCancelReplaceRequest} (35=AC). Split out of {@link FixToSbeTranslator} so the
 * leg-loop complexity lives in one place and the top-level translator stays focused on flat
 * (single-instrument) messages.
 *
 * <p><b>Threading / allocation.</b> Same contract as {@link FixToSbeTranslator}: one instance per
 * ingress duty-cycle thread, zero allocation on every translate method. Pre-allocated encoders,
 * {@link UtcTimestampDecoder}, and a {@code chars} scratch buffer are instance fields reused across
 * calls. Enum mapping throws {@link IllegalStateException} on unmapped FIX chars; the gateway is
 * expected to convert that into a session-level FIX reject.
 */
public final class FixToSbeMultilegTranslator {

  private static final int SCRATCH_LEN = 64;

  // Class-init sanity check across every SBE char field this translator writes. The runtime
  // `dstLen > SCRATCH_LEN` check in the pad helpers is the actual safety net; this static block
  // is belt-and-braces to fail loudly at class-load if a future schema change widens any field
  // past SCRATCH_LEN.
  static {
    final var max =
        Math.max(
            NewOrderMultilegEncoder.clOrdIdLength(),
            Math.max(
                MultilegOrderCancelReplaceEncoder.clOrdIdLength(),
                Math.max(
                    NewOrderMultilegEncoder.NoLegsEncoder.legSymbolLength(),
                    MultilegOrderCancelReplaceEncoder.NoLegsEncoder.legSymbolLength())));
    if (max > SCRATCH_LEN) {
      // Thrown inside a static {} block — the JVM automatically wraps it in an
      // ExceptionInInitializerError with this IllegalStateException as the cause, giving the
      // caller a typed exception in the cause chain (class-init errors are unrecoverable in
      // practice, but the typed cause is easier to diagnose from logs).
      throw new IllegalStateException(
          "FixToSbeMultilegTranslator SCRATCH_LEN="
              + SCRATCH_LEN
              + " too small for SBE field "
              + max);
    }
  }

  private final byte[] chars = new byte[SCRATCH_LEN];

  private final MessageHeaderEncoder header = new MessageHeaderEncoder();
  private final NewOrderMultilegEncoder nom = new NewOrderMultilegEncoder();
  private final MultilegOrderCancelReplaceEncoder mocr = new MultilegOrderCancelReplaceEncoder();
  private final UtcTimestampDecoder utcTs = new UtcTimestampDecoder(false);

  public FixToSbeMultilegTranslator() {}

  // ---------------------------------------------------------------------------
  // NewOrderMultileg (35=AB)
  // ---------------------------------------------------------------------------

  /**
   * Translate a FIX 4.4 NewOrderMultileg (35=AB) into an SBE {@code NewOrderMultilegEncoder}
   * including its noLegs repeating group.
   */
  public int translateNewOrderMultileg(
      final NewOrderMultilegDecoder fix, final MutableDirectBuffer sbe, final int offset) {
    nom.wrapAndApplyHeader(sbe, offset, header);

    nom.putClOrdId(
        padFromChars(fix.clOrdID(), fix.clOrdIDLength(), NewOrderMultilegEncoder.clOrdIdLength()),
        0);
    nom.putQuoteId(
        fix.hasQuoteID()
            ? padFromChars(
                fix.quoteID(), fix.quoteIDLength(), NewOrderMultilegEncoder.quoteIdLength())
            : padNull(NewOrderMultilegEncoder.quoteIdLength()),
        0);
    nom.putSymbol(
        padFromChars(fix.symbol(), fix.symbolLength(), NewOrderMultilegEncoder.symbolLength()), 0);
    nom.side(mapSide(fix.side()));
    nom.ordType(mapOrdType(fix.ordType()));
    nom.price(
        fix.hasPrice()
            ? FixedPoint.toInt64(fix.price())
            : NewOrderMultilegEncoder.priceNullValue());
    nom.orderQty(FixedPoint.toInt64(fix.orderQty()));
    nom.timeInForce(fix.hasTimeInForce() ? mapTimeInForce(fix.timeInForce()) : TimeInForceEnum.Day);
    nom.transactTime(utcTs.decodeNanos(fix.transactTime(), fix.transactTimeLength()));
    nom.putAccountCode(
        fix.hasAccount()
            ? padFromChars(
                fix.account(), fix.accountLength(), NewOrderMultilegEncoder.accountCodeLength())
            : padNull(NewOrderMultilegEncoder.accountCodeLength()),
        0);
    nom.productType(ProductTypeEnum.NULL_VAL); // APP-45
    nom.putSettlDate(
        fix.hasSettlDate()
            ? padFromBytes(
                fix.settlDate(), fix.settlDateLength(), NewOrderMultilegEncoder.settlDateLength())
            : padNull(NewOrderMultilegEncoder.settlDateLength()),
        0);
    nom.settlType(fix.hasSettlType() ? mapSettlType(fix.settlType()) : SettlTypeEnum.NULL_VAL);
    nom.putCurrency(
        fix.hasCurrency()
            ? padFromChars(
                fix.currency(), fix.currencyLength(), NewOrderMultilegEncoder.currencyLength())
            : padNull(NewOrderMultilegEncoder.currencyLength()),
        0);
    nom.putSettlCurrency(
        fix.hasSettlCurrency()
            ? padFromChars(
                fix.settlCurrency(),
                fix.settlCurrencyLength(),
                NewOrderMultilegEncoder.settlCurrencyLength())
            : padNull(NewOrderMultilegEncoder.settlCurrencyLength()),
        0);
    nom.tenor(TenorEnum.NULL_VAL); // APP-45

    encodeNewOrderMultilegLegs(fix);

    return MessageHeaderEncoder.ENCODED_LENGTH + nom.encodedLength();
  }

  /**
   * Walk the FIX 35=AB {@code NoLegs} repeating group and copy each leg's fields into the SBE
   * {@link NewOrderMultilegEncoder.NoLegsEncoder} sub-group. Zero allocation — reuses the shared
   * {@link #chars} scratch buffer for every char field and Artio's cached leg-group iterator.
   * Unmapped FIX enum values propagate from the shared enum mappers as {@link
   * IllegalStateException}.
   */
  private void encodeNewOrderMultilegLegs(final NewOrderMultilegDecoder fix) {
    final var legCount = fix.noLegsGroupCounter();
    final var legs = nom.noLegsCount(legCount);
    final var iter = fix.legsGroupIterator();
    while (iter.hasNext()) {
      final var leg = iter.next();
      legs.next();
      legs.putLegSymbol(
          padFromChars(
              leg.legSymbol(),
              leg.legSymbolLength(),
              NewOrderMultilegEncoder.NoLegsEncoder.legSymbolLength()),
          0);
      legs.legSide(mapSide(leg.legSide()));
      legs.putLegSettlDate(
          leg.hasLegSettlDate()
              ? padFromBytes(
                  leg.legSettlDate(),
                  leg.legSettlDateLength(),
                  NewOrderMultilegEncoder.NoLegsEncoder.legSettlDateLength())
              : padNull(NewOrderMultilegEncoder.NoLegsEncoder.legSettlDateLength()),
          0);
      legs.legSettlType(
          leg.hasLegSettlType() ? mapSettlType(leg.legSettlType()) : SettlTypeEnum.NULL_VAL);
      legs.putLegCurrency(
          leg.hasLegCurrency()
              ? padFromChars(
                  leg.legCurrency(),
                  leg.legCurrencyLength(),
                  NewOrderMultilegEncoder.NoLegsEncoder.legCurrencyLength())
              : padNull(NewOrderMultilegEncoder.NoLegsEncoder.legCurrencyLength()),
          0);
      legs.legRatioQty(
          leg.hasLegRatioQty()
              ? FixedPoint.toInt64(leg.legRatioQty())
              : NewOrderMultilegEncoder.NoLegsEncoder.legRatioQtyNullValue());
      legs.legTenor(TenorEnum.NULL_VAL); // APP-45
      legs.legOrderQty(
          leg.hasLegQty()
              ? FixedPoint.toInt64(leg.legQty())
              : NewOrderMultilegEncoder.NoLegsEncoder.legOrderQtyNullValue());
      legs.legPrice(
          leg.hasLegPrice()
              ? FixedPoint.toInt64(leg.legPrice())
              : NewOrderMultilegEncoder.NoLegsEncoder.legPriceNullValue());
    }
  }

  // ---------------------------------------------------------------------------
  // MultilegOrderCancelReplaceRequest (35=AC)
  // ---------------------------------------------------------------------------

  /**
   * Translate a FIX 4.4 MultilegOrderCancelReplaceRequest (35=AC) into an SBE {@code
   * MultilegOrderCancelReplaceEncoder} including its noLegs repeating group.
   */
  public int translateMultilegOrderCancelReplace(
      final MultilegOrderCancelReplaceRequestDecoder fix,
      final MutableDirectBuffer sbe,
      final int offset) {
    mocr.wrapAndApplyHeader(sbe, offset, header);

    mocr.putOrigClOrdId(
        padFromChars(
            fix.origClOrdID(),
            fix.origClOrdIDLength(),
            MultilegOrderCancelReplaceEncoder.origClOrdIdLength()),
        0);
    mocr.putOrderId(
        fix.hasOrderID()
            ? padFromChars(
                fix.orderID(),
                fix.orderIDLength(),
                MultilegOrderCancelReplaceEncoder.orderIdLength())
            : padNull(MultilegOrderCancelReplaceEncoder.orderIdLength()),
        0);
    mocr.putClOrdId(
        padFromChars(
            fix.clOrdID(), fix.clOrdIDLength(), MultilegOrderCancelReplaceEncoder.clOrdIdLength()),
        0);
    mocr.putQuoteId(
        fix.hasQuoteID()
            ? padFromChars(
                fix.quoteID(),
                fix.quoteIDLength(),
                MultilegOrderCancelReplaceEncoder.quoteIdLength())
            : padNull(MultilegOrderCancelReplaceEncoder.quoteIdLength()),
        0);
    mocr.putSymbol(
        padFromChars(
            fix.symbol(), fix.symbolLength(), MultilegOrderCancelReplaceEncoder.symbolLength()),
        0);
    mocr.side(mapSide(fix.side()));
    mocr.ordType(mapOrdType(fix.ordType()));
    mocr.price(
        fix.hasPrice()
            ? FixedPoint.toInt64(fix.price())
            : MultilegOrderCancelReplaceEncoder.priceNullValue());
    mocr.orderQty(FixedPoint.toInt64(fix.orderQty()));
    mocr.timeInForce(
        fix.hasTimeInForce() ? mapTimeInForce(fix.timeInForce()) : TimeInForceEnum.Day);
    mocr.transactTime(utcTs.decodeNanos(fix.transactTime(), fix.transactTimeLength()));
    mocr.putAccountCode(
        fix.hasAccount()
            ? padFromChars(
                fix.account(),
                fix.accountLength(),
                MultilegOrderCancelReplaceEncoder.accountCodeLength())
            : padNull(MultilegOrderCancelReplaceEncoder.accountCodeLength()),
        0);
    mocr.productType(ProductTypeEnum.NULL_VAL); // APP-45
    mocr.putSettlDate(
        fix.hasSettlDate()
            ? padFromBytes(
                fix.settlDate(),
                fix.settlDateLength(),
                MultilegOrderCancelReplaceEncoder.settlDateLength())
            : padNull(MultilegOrderCancelReplaceEncoder.settlDateLength()),
        0);
    mocr.settlType(fix.hasSettlType() ? mapSettlType(fix.settlType()) : SettlTypeEnum.NULL_VAL);
    mocr.putCurrency(
        fix.hasCurrency()
            ? padFromChars(
                fix.currency(),
                fix.currencyLength(),
                MultilegOrderCancelReplaceEncoder.currencyLength())
            : padNull(MultilegOrderCancelReplaceEncoder.currencyLength()),
        0);
    mocr.putSettlCurrency(
        fix.hasSettlCurrency()
            ? padFromChars(
                fix.settlCurrency(),
                fix.settlCurrencyLength(),
                MultilegOrderCancelReplaceEncoder.settlCurrencyLength())
            : padNull(MultilegOrderCancelReplaceEncoder.settlCurrencyLength()),
        0);
    mocr.tenor(TenorEnum.NULL_VAL); // APP-45

    encodeMultilegCancelReplaceLegs(fix);

    return MessageHeaderEncoder.ENCODED_LENGTH + mocr.encodedLength();
  }

  /**
   * Walk the FIX 35=AC {@code NoLegs} repeating group and copy each leg's fields into the SBE
   * {@link MultilegOrderCancelReplaceEncoder.NoLegsEncoder} sub-group. Same contract as {@link
   * #encodeNewOrderMultilegLegs} — zero allocation, shared scratch buffer, enum mapping throws on
   * unmapped FIX values.
   */
  private void encodeMultilegCancelReplaceLegs(final MultilegOrderCancelReplaceRequestDecoder fix) {
    final var legCount = fix.noLegsGroupCounter();
    final var legs = mocr.noLegsCount(legCount);
    final var iter = fix.legsGroupIterator();
    while (iter.hasNext()) {
      final var leg = iter.next();
      legs.next();
      legs.putLegSymbol(
          padFromChars(
              leg.legSymbol(),
              leg.legSymbolLength(),
              MultilegOrderCancelReplaceEncoder.NoLegsEncoder.legSymbolLength()),
          0);
      legs.legSide(mapSide(leg.legSide()));
      legs.putLegSettlDate(
          leg.hasLegSettlDate()
              ? padFromBytes(
                  leg.legSettlDate(),
                  leg.legSettlDateLength(),
                  MultilegOrderCancelReplaceEncoder.NoLegsEncoder.legSettlDateLength())
              : padNull(MultilegOrderCancelReplaceEncoder.NoLegsEncoder.legSettlDateLength()),
          0);
      legs.legSettlType(
          leg.hasLegSettlType() ? mapSettlType(leg.legSettlType()) : SettlTypeEnum.NULL_VAL);
      legs.putLegCurrency(
          leg.hasLegCurrency()
              ? padFromChars(
                  leg.legCurrency(),
                  leg.legCurrencyLength(),
                  MultilegOrderCancelReplaceEncoder.NoLegsEncoder.legCurrencyLength())
              : padNull(MultilegOrderCancelReplaceEncoder.NoLegsEncoder.legCurrencyLength()),
          0);
      legs.legRatioQty(
          leg.hasLegRatioQty()
              ? FixedPoint.toInt64(leg.legRatioQty())
              : MultilegOrderCancelReplaceEncoder.NoLegsEncoder.legRatioQtyNullValue());
      legs.legTenor(TenorEnum.NULL_VAL); // APP-45
      legs.legOrderQty(
          leg.hasLegQty()
              ? FixedPoint.toInt64(leg.legQty())
              : MultilegOrderCancelReplaceEncoder.NoLegsEncoder.legOrderQtyNullValue());
      legs.legPrice(
          leg.hasLegPrice()
              ? FixedPoint.toInt64(leg.legPrice())
              : MultilegOrderCancelReplaceEncoder.NoLegsEncoder.legPriceNullValue());
    }
  }

  // ---------------------------------------------------------------------------
  // Char-array pad helpers (private copies — keeps this class self-contained)
  // ---------------------------------------------------------------------------

  private byte[] padFromChars(final char[] src, final int srcLen, final int dstLen) {
    if (dstLen > SCRATCH_LEN) {
      throw new IllegalStateException("SBE field exceeds scratch buffer: " + dstLen);
    }
    if (srcLen > dstLen) {
      throw new IllegalStateException(
          "FIX field length " + srcLen + " exceeds SBE field capacity " + dstLen);
    }
    for (int i = 0; i < srcLen; i++) {
      chars[i] = (byte) src[i];
    }
    for (int i = srcLen; i < dstLen; i++) {
      chars[i] = 0;
    }
    return chars;
  }

  private byte[] padFromBytes(final byte[] src, final int srcLen, final int dstLen) {
    if (dstLen > SCRATCH_LEN) {
      throw new IllegalStateException("SBE field exceeds scratch buffer: " + dstLen);
    }
    if (srcLen > dstLen) {
      throw new IllegalStateException(
          "FIX field length " + srcLen + " exceeds SBE field capacity " + dstLen);
    }
    System.arraycopy(src, 0, chars, 0, srcLen);
    for (int i = srcLen; i < dstLen; i++) {
      chars[i] = 0;
    }
    return chars;
  }

  private byte[] padNull(final int dstLen) {
    if (dstLen > SCRATCH_LEN) {
      throw new IllegalStateException("SBE field exceeds scratch buffer: " + dstLen);
    }
    for (int i = 0; i < dstLen; i++) {
      chars[i] = 0;
    }
    return chars;
  }

  // ---------------------------------------------------------------------------
  // Enum mappings (FIX char → SBE enum). Throw on unmapped values.
  // ---------------------------------------------------------------------------

  private static SideEnum mapSide(final char fix) {
    return switch (fix) {
      case '1' -> SideEnum.Buy;
      case '2' -> SideEnum.Sell;
      default -> throw new IllegalStateException("Unsupported FIX Side(54): " + fix);
    };
  }

  private static OrdTypeEnum mapOrdType(final char fix) {
    return switch (fix) {
      case '1' -> OrdTypeEnum.Market;
      case '2' -> OrdTypeEnum.Limit;
      case 'D' -> OrdTypeEnum.PreviouslyQuoted;
      default -> throw new IllegalStateException("Unsupported FIX OrdType(40): " + fix);
    };
  }

  private static TimeInForceEnum mapTimeInForce(final char fix) {
    return switch (fix) {
      case '0' -> TimeInForceEnum.Day;
      case '1' -> TimeInForceEnum.GTC;
      case '3' -> TimeInForceEnum.IOC;
      case '4' -> TimeInForceEnum.FOK;
      default -> throw new IllegalStateException("Unsupported FIX TimeInForce(59): " + fix);
    };
  }

  private static SettlTypeEnum mapSettlType(final char fix) {
    return switch (fix) {
      case '0' -> SettlTypeEnum.Regular;
      case '1' -> SettlTypeEnum.Cash;
      case '2' -> SettlTypeEnum.NextDay;
      case '3' -> SettlTypeEnum.TPlus2;
      case '4' -> SettlTypeEnum.TPlus3;
      case '5' -> SettlTypeEnum.TPlus4;
      case '6' -> SettlTypeEnum.Future;
      case '7' -> SettlTypeEnum.WhenAndIfIssued;
      case '8' -> SettlTypeEnum.SellersOption;
      case '9' -> SettlTypeEnum.TPlus5;
      case 'B' -> SettlTypeEnum.BrokenDate;
      case 'C' -> SettlTypeEnum.FXSpotNextDay;
      default -> throw new IllegalStateException("Unsupported FIX SettlType(63): " + fix);
    };
  }
}
