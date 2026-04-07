package com.trading.engine.gateway;

import com.trading.engine.fix.decoder_flyweight.MultilegOrderCancelReplaceRequestDecoder;
import com.trading.engine.fix.decoder_flyweight.NewOrderMultilegDecoder;
import com.trading.engine.fix.decoder_flyweight.NewOrderSingleDecoder;
import com.trading.engine.fix.decoder_flyweight.OrderCancelRequestDecoder;
import com.trading.engine.fix.decoder_flyweight.QuoteRequestDecoder;
import com.trading.engine.messages.sbe.CancelOrderRequestEncoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.MultilegOrderCancelReplaceEncoder;
import com.trading.engine.messages.sbe.NewOrderMultilegEncoder;
import com.trading.engine.messages.sbe.NewOrderSingleEncoder;
import com.trading.engine.messages.sbe.OrdTypeEnum;
import com.trading.engine.messages.sbe.ProductTypeEnum;
import com.trading.engine.messages.sbe.QuoteRequestEncoder;
import com.trading.engine.messages.sbe.SettlTypeEnum;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.messages.sbe.TenorEnum;
import com.trading.engine.messages.sbe.TimeInForceEnum;
import org.agrona.MutableDirectBuffer;
import uk.co.real_logic.artio.fields.UtcTimestampDecoder;

/**
 * Stateless translator from Artio FIX 4.4 decoders (flyweight) to SBE encoders. Each public method
 * writes a complete SBE message (header + body) into the caller-supplied buffer at the given offset
 * and returns the total encoded length in bytes.
 *
 * <p><b>Threading.</b> This class is single-threaded by contract. It owns {@code static final} SBE
 * encoder flyweights, an Artio {@link UtcTimestampDecoder}, and a single {@code byte[]} scratch
 * buffer that are reused across calls. The gateway invokes the translator from one duty-cycle
 * thread per ingress publication; do not call concurrently.
 *
 * <p><b>Allocation.</b> Zero allocation on every method. No {@code new}, no boxing, no {@code
 * String}, no streams, no captured lambdas. Char-array fields are copied from the FIX flyweight
 * {@code char[]} into the shared {@link #CHARS} scratch byte buffer, padded with {@code \0}, and
 * then handed to the SBE encoder's {@code putXxx(byte[], int)} setter. Decimal prices flow through
 * {@link FixedPoint}.
 *
 * <p><b>Errors.</b> Unmapped enum values throw {@link IllegalStateException} with a string-literal
 * message naming the field. The gateway is expected to catch and convert to a session-level FIX
 * reject. Lossy fixed-point conversion throws via {@link FixedPoint#toInt64}.
 *
 * <p><b>Custom FIX tags.</b> The trading engine's custom tags (Tenor=10001, ProductType=10013,
 * leg-level extras) are not present in the stock QuickFIX/J FIX44.xml dictionary. The corresponding
 * SBE fields are written as {@code NULL_VAL} sentinels until APP-45 (Wave 8) wires the dictionary
 * extension.
 */
public final class FixToSbeTranslator {

  /**
   * Scratch buffer for char-array conversions. Sized to comfortably exceed the largest SBE char
   * field in {@code trading-schema.xml} (largest is 20 bytes for clOrdId). The {@link
   * #padFromChars} / {@link #padFromBytes} helpers guard against any future field that exceeds the
   * buffer.
   */
  private static final int SCRATCH_LEN = 64;

  private static final byte[] CHARS = new byte[SCRATCH_LEN];

  // SBE encoders are stateful flyweights — wrap() resets them on every call.
  private static final MessageHeaderEncoder HEADER = new MessageHeaderEncoder();
  private static final NewOrderSingleEncoder NOS = new NewOrderSingleEncoder();
  private static final NewOrderMultilegEncoder NOM = new NewOrderMultilegEncoder();
  private static final CancelOrderRequestEncoder COR = new CancelOrderRequestEncoder();
  private static final MultilegOrderCancelReplaceEncoder MOCR =
      new MultilegOrderCancelReplaceEncoder();
  private static final QuoteRequestEncoder QR = new QuoteRequestEncoder();

  // Artio's UTC-timestamp decoder is stateless; one shared instance is safe.
  private static final UtcTimestampDecoder UTC_TS = new UtcTimestampDecoder(false);

  private FixToSbeTranslator() {}

  // ---------------------------------------------------------------------------
  // NewOrderSingle (35=D)
  // ---------------------------------------------------------------------------

  /**
   * Translate a FIX 4.4 NewOrderSingle (35=D) into an SBE {@code NewOrderSingleEncoder} message.
   *
   * @return total encoded length (header + body) in bytes
   */
  public static int translateNewOrderSingle(
      NewOrderSingleDecoder fix, MutableDirectBuffer sbe, int offset) {
    NOS.wrapAndApplyHeader(sbe, offset, HEADER);

    NOS.putClOrdId(
        padFromChars(fix.clOrdID(), fix.clOrdIDLength(), NewOrderSingleEncoder.clOrdIdLength()), 0);
    NOS.putQuoteId(padNull(NewOrderSingleEncoder.quoteIdLength()), 0);
    NOS.putSymbol(
        padFromChars(fix.symbol(), fix.symbolLength(), NewOrderSingleEncoder.symbolLength()), 0);
    NOS.side(mapSide(fix.side()));
    NOS.ordType(mapOrdType(fix.ordType()));
    NOS.price(
        fix.hasPrice() ? FixedPoint.toInt64(fix.price()) : NewOrderSingleEncoder.priceNullValue());
    NOS.orderQty(FixedPoint.toInt64(fix.orderQty()));
    NOS.timeInForce(fix.hasTimeInForce() ? mapTimeInForce(fix.timeInForce()) : TimeInForceEnum.Day);
    NOS.transactTime(UTC_TS.decodeNanos(fix.transactTime(), fix.transactTimeLength()));
    NOS.putAccountCode(
        fix.hasAccount()
            ? padFromChars(
                fix.account(), fix.accountLength(), NewOrderSingleEncoder.accountCodeLength())
            : padNull(NewOrderSingleEncoder.accountCodeLength()),
        0);
    NOS.productType(ProductTypeEnum.NULL_VAL); // APP-45
    NOS.putSettlDate(
        fix.hasSettlDate()
            ? padFromBytes(
                fix.settlDate(), fix.settlDateLength(), NewOrderSingleEncoder.settlDateLength())
            : padNull(NewOrderSingleEncoder.settlDateLength()),
        0);
    NOS.settlType(fix.hasSettlType() ? mapSettlType(fix.settlType()) : SettlTypeEnum.NULL_VAL);
    NOS.putCurrency(
        fix.hasCurrency()
            ? padFromChars(
                fix.currency(), fix.currencyLength(), NewOrderSingleEncoder.currencyLength())
            : padNull(NewOrderSingleEncoder.currencyLength()),
        0);
    NOS.putSettlCurrency(
        fix.hasSettlCurrency()
            ? padFromChars(
                fix.settlCurrency(),
                fix.settlCurrencyLength(),
                NewOrderSingleEncoder.settlCurrencyLength())
            : padNull(NewOrderSingleEncoder.settlCurrencyLength()),
        0);
    NOS.tenor(TenorEnum.NULL_VAL); // APP-45

    return MessageHeaderEncoder.ENCODED_LENGTH + NOS.encodedLength();
  }

  // ---------------------------------------------------------------------------
  // NewOrderMultileg (35=AB)
  // ---------------------------------------------------------------------------

  /**
   * Translate a FIX 4.4 NewOrderMultileg (35=AB) into an SBE {@code NewOrderMultilegEncoder}
   * including its noLegs repeating group.
   */
  public static int translateNewOrderMultileg(
      NewOrderMultilegDecoder fix, MutableDirectBuffer sbe, int offset) {
    NOM.wrapAndApplyHeader(sbe, offset, HEADER);

    NOM.putClOrdId(
        padFromChars(fix.clOrdID(), fix.clOrdIDLength(), NewOrderMultilegEncoder.clOrdIdLength()),
        0);
    NOM.putQuoteId(padNull(NewOrderMultilegEncoder.quoteIdLength()), 0);
    NOM.putSymbol(
        padFromChars(fix.symbol(), fix.symbolLength(), NewOrderMultilegEncoder.symbolLength()), 0);
    NOM.side(mapSide(fix.side()));
    NOM.ordType(mapOrdType(fix.ordType()));
    NOM.price(
        fix.hasPrice()
            ? FixedPoint.toInt64(fix.price())
            : NewOrderMultilegEncoder.priceNullValue());
    NOM.orderQty(FixedPoint.toInt64(fix.orderQty()));
    NOM.timeInForce(fix.hasTimeInForce() ? mapTimeInForce(fix.timeInForce()) : TimeInForceEnum.Day);
    NOM.transactTime(UTC_TS.decodeNanos(fix.transactTime(), fix.transactTimeLength()));
    NOM.putAccountCode(
        fix.hasAccount()
            ? padFromChars(
                fix.account(), fix.accountLength(), NewOrderMultilegEncoder.accountCodeLength())
            : padNull(NewOrderMultilegEncoder.accountCodeLength()),
        0);
    NOM.productType(ProductTypeEnum.NULL_VAL); // APP-45
    NOM.putSettlDate(
        fix.hasSettlDate()
            ? padFromBytes(
                fix.settlDate(), fix.settlDateLength(), NewOrderMultilegEncoder.settlDateLength())
            : padNull(NewOrderMultilegEncoder.settlDateLength()),
        0);
    NOM.settlType(fix.hasSettlType() ? mapSettlType(fix.settlType()) : SettlTypeEnum.NULL_VAL);
    NOM.putCurrency(
        fix.hasCurrency()
            ? padFromChars(
                fix.currency(), fix.currencyLength(), NewOrderMultilegEncoder.currencyLength())
            : padNull(NewOrderMultilegEncoder.currencyLength()),
        0);
    NOM.putSettlCurrency(padNull(NewOrderMultilegEncoder.settlCurrencyLength()), 0);
    NOM.tenor(TenorEnum.NULL_VAL); // APP-45

    final int legCount = fix.noLegsGroupCounter();
    final NewOrderMultilegEncoder.NoLegsEncoder legs = NOM.noLegsCount(legCount);
    final NewOrderMultilegDecoder.LegsGroupIterator iter = fix.legsGroupIterator();
    while (iter.hasNext()) {
      final NewOrderMultilegDecoder.LegsGroupDecoder leg = iter.next();
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
      legs.legOrderQty(NewOrderMultilegEncoder.NoLegsEncoder.legOrderQtyNullValue()); // APP-45
      legs.legPrice(
          leg.hasLegPrice()
              ? FixedPoint.toInt64(leg.legPrice())
              : NewOrderMultilegEncoder.NoLegsEncoder.legPriceNullValue());
    }

    return MessageHeaderEncoder.ENCODED_LENGTH + NOM.encodedLength();
  }

  // ---------------------------------------------------------------------------
  // OrderCancelRequest (35=F)
  // ---------------------------------------------------------------------------

  /**
   * Translate a FIX 4.4 OrderCancelRequest (35=F) into an SBE {@code CancelOrderRequestEncoder}.
   */
  public static int translateOrderCancelRequest(
      OrderCancelRequestDecoder fix, MutableDirectBuffer sbe, int offset) {
    COR.wrapAndApplyHeader(sbe, offset, HEADER);

    COR.putOrigClOrdId(
        padFromChars(
            fix.origClOrdID(),
            fix.origClOrdIDLength(),
            CancelOrderRequestEncoder.origClOrdIdLength()),
        0);
    COR.putClOrdId(
        padFromChars(fix.clOrdID(), fix.clOrdIDLength(), CancelOrderRequestEncoder.clOrdIdLength()),
        0);
    COR.putSymbol(
        padFromChars(fix.symbol(), fix.symbolLength(), CancelOrderRequestEncoder.symbolLength()),
        0);
    COR.side(mapSide(fix.side()));
    COR.transactTime(UTC_TS.decodeNanos(fix.transactTime(), fix.transactTimeLength()));
    COR.putAccountCode(
        fix.hasAccount()
            ? padFromChars(
                fix.account(), fix.accountLength(), CancelOrderRequestEncoder.accountCodeLength())
            : padNull(CancelOrderRequestEncoder.accountCodeLength()),
        0);
    COR.productType(ProductTypeEnum.NULL_VAL); // APP-45

    return MessageHeaderEncoder.ENCODED_LENGTH + COR.encodedLength();
  }

  // ---------------------------------------------------------------------------
  // MultilegOrderCancelReplaceRequest (35=AC)
  // ---------------------------------------------------------------------------

  /**
   * Translate a FIX 4.4 MultilegOrderCancelReplaceRequest (35=AC) into an SBE {@code
   * MultilegOrderCancelReplaceEncoder} including its noLegs repeating group.
   */
  public static int translateMultilegOrderCancelReplace(
      MultilegOrderCancelReplaceRequestDecoder fix, MutableDirectBuffer sbe, int offset) {
    MOCR.wrapAndApplyHeader(sbe, offset, HEADER);

    MOCR.putOrigClOrdId(
        padFromChars(
            fix.origClOrdID(),
            fix.origClOrdIDLength(),
            MultilegOrderCancelReplaceEncoder.origClOrdIdLength()),
        0);
    MOCR.putOrderId(padNull(MultilegOrderCancelReplaceEncoder.orderIdLength()), 0);
    MOCR.putClOrdId(
        padFromChars(
            fix.clOrdID(), fix.clOrdIDLength(), MultilegOrderCancelReplaceEncoder.clOrdIdLength()),
        0);
    MOCR.putQuoteId(padNull(MultilegOrderCancelReplaceEncoder.quoteIdLength()), 0);
    MOCR.putSymbol(
        padFromChars(
            fix.symbol(), fix.symbolLength(), MultilegOrderCancelReplaceEncoder.symbolLength()),
        0);
    MOCR.side(mapSide(fix.side()));
    MOCR.ordType(mapOrdType(fix.ordType()));
    MOCR.price(
        fix.hasPrice()
            ? FixedPoint.toInt64(fix.price())
            : MultilegOrderCancelReplaceEncoder.priceNullValue());
    MOCR.orderQty(FixedPoint.toInt64(fix.orderQty()));
    MOCR.timeInForce(
        fix.hasTimeInForce() ? mapTimeInForce(fix.timeInForce()) : TimeInForceEnum.Day);
    MOCR.transactTime(UTC_TS.decodeNanos(fix.transactTime(), fix.transactTimeLength()));
    MOCR.putAccountCode(
        fix.hasAccount()
            ? padFromChars(
                fix.account(),
                fix.accountLength(),
                MultilegOrderCancelReplaceEncoder.accountCodeLength())
            : padNull(MultilegOrderCancelReplaceEncoder.accountCodeLength()),
        0);
    MOCR.productType(ProductTypeEnum.NULL_VAL); // APP-45
    MOCR.putSettlDate(
        fix.hasSettlDate()
            ? padFromBytes(
                fix.settlDate(),
                fix.settlDateLength(),
                MultilegOrderCancelReplaceEncoder.settlDateLength())
            : padNull(MultilegOrderCancelReplaceEncoder.settlDateLength()),
        0);
    MOCR.settlType(fix.hasSettlType() ? mapSettlType(fix.settlType()) : SettlTypeEnum.NULL_VAL);
    MOCR.putCurrency(padNull(MultilegOrderCancelReplaceEncoder.currencyLength()), 0);
    MOCR.putSettlCurrency(padNull(MultilegOrderCancelReplaceEncoder.settlCurrencyLength()), 0);
    MOCR.tenor(TenorEnum.NULL_VAL); // APP-45

    final int legCount = fix.noLegsGroupCounter();
    final MultilegOrderCancelReplaceEncoder.NoLegsEncoder legs = MOCR.noLegsCount(legCount);
    final MultilegOrderCancelReplaceRequestDecoder.LegsGroupIterator iter = fix.legsGroupIterator();
    while (iter.hasNext()) {
      final MultilegOrderCancelReplaceRequestDecoder.LegsGroupDecoder leg = iter.next();
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
          MultilegOrderCancelReplaceEncoder.NoLegsEncoder.legOrderQtyNullValue()); // APP-45
      legs.legPrice(
          leg.hasLegPrice()
              ? FixedPoint.toInt64(leg.legPrice())
              : MultilegOrderCancelReplaceEncoder.NoLegsEncoder.legPriceNullValue());
    }

    return MessageHeaderEncoder.ENCODED_LENGTH + MOCR.encodedLength();
  }

  // ---------------------------------------------------------------------------
  // QuoteRequest (35=R)
  //
  // FIX 4.4 places symbol/side/qty inside the noRelatedSym(146) repeating group. The SBE schema
  // flattens that for single-instrument quotes onto the top-level message, so this translator
  // copies from the first related-sym entry only and writes a noLegs(0) sentinel for the leg
  // group. Multi-instrument QuoteRequests will need APP-47 (Wave 8) to fan out the legs.
  // ---------------------------------------------------------------------------

  /** Translate a FIX 4.4 QuoteRequest (35=R) into an SBE {@code QuoteRequestEncoder}. */
  public static int translateQuoteRequest(
      QuoteRequestDecoder fix, MutableDirectBuffer sbe, int offset) {
    QR.wrapAndApplyHeader(sbe, offset, HEADER);

    QR.putQuoteReqId(
        padFromChars(
            fix.quoteReqID(), fix.quoteReqIDLength(), QuoteRequestEncoder.quoteReqIdLength()),
        0);

    final QuoteRequestDecoder.RelatedSymGroupIterator iter = fix.relatedSymGroupIterator();
    if (iter.hasNext()) {
      final QuoteRequestDecoder.RelatedSymGroupDecoder firstRelatedSym = iter.next();
      QR.putSymbol(
          padFromChars(
              firstRelatedSym.symbol(),
              firstRelatedSym.symbolLength(),
              QuoteRequestEncoder.symbolLength()),
          0);
      QR.side(firstRelatedSym.hasSide() ? mapSide(firstRelatedSym.side()) : SideEnum.NULL_VAL);
      QR.orderQty(
          firstRelatedSym.hasOrderQty()
              ? FixedPoint.toInt64(firstRelatedSym.orderQty())
              : QuoteRequestEncoder.orderQtyNullValue());
      QR.putSettlDate(
          firstRelatedSym.hasSettlDate()
              ? padFromBytes(
                  firstRelatedSym.settlDate(),
                  firstRelatedSym.settlDateLength(),
                  QuoteRequestEncoder.settlDateLength())
              : padNull(QuoteRequestEncoder.settlDateLength()),
          0);
      QR.settlType(
          firstRelatedSym.hasSettlType()
              ? mapSettlType(firstRelatedSym.settlType())
              : SettlTypeEnum.NULL_VAL);
      QR.putCurrency(
          firstRelatedSym.hasCurrency()
              ? padFromChars(
                  firstRelatedSym.currency(),
                  firstRelatedSym.currencyLength(),
                  QuoteRequestEncoder.currencyLength())
              : padNull(QuoteRequestEncoder.currencyLength()),
          0);
      // Stock FIX 4.4 QuoteRequest's NoRelatedSym group has no SettlCurrency tag — APP-45 will
      // wire the trading-engine custom tag once the dictionary extension lands.
      QR.putSettlCurrency(padNull(QuoteRequestEncoder.settlCurrencyLength()), 0);
    } else {
      QR.putSymbol(padNull(QuoteRequestEncoder.symbolLength()), 0);
      QR.side(SideEnum.NULL_VAL);
      QR.orderQty(QuoteRequestEncoder.orderQtyNullValue());
      QR.putSettlDate(padNull(QuoteRequestEncoder.settlDateLength()), 0);
      QR.settlType(SettlTypeEnum.NULL_VAL);
      QR.putCurrency(padNull(QuoteRequestEncoder.currencyLength()), 0);
      QR.putSettlCurrency(padNull(QuoteRequestEncoder.settlCurrencyLength()), 0);
    }

    QR.putAccountCode(padNull(QuoteRequestEncoder.accountCodeLength()), 0);
    QR.transactTime(0L); // FIX QuoteRequest has no top-level transactTime
    QR.productType(ProductTypeEnum.NULL_VAL); // APP-45
    QR.tenor(TenorEnum.NULL_VAL); // APP-45
    QR.noLegsCount(0); // APP-47

    return MessageHeaderEncoder.ENCODED_LENGTH + QR.encodedLength();
  }

  // ---------------------------------------------------------------------------
  // Char-array helpers (zero-allocation, share the static CHARS scratch buffer)
  // ---------------------------------------------------------------------------

  private static byte[] padFromChars(char[] src, int srcLen, int dstLen) {
    if (dstLen > SCRATCH_LEN) {
      throw new IllegalStateException("SBE field exceeds scratch buffer: " + dstLen);
    }
    final int copy = Math.min(srcLen, dstLen);
    for (int i = 0; i < copy; i++) {
      CHARS[i] = (byte) src[i];
    }
    for (int i = copy; i < dstLen; i++) {
      CHARS[i] = 0;
    }
    return CHARS;
  }

  private static byte[] padFromBytes(byte[] src, int srcLen, int dstLen) {
    if (dstLen > SCRATCH_LEN) {
      throw new IllegalStateException("SBE field exceeds scratch buffer: " + dstLen);
    }
    final int copy = Math.min(srcLen, dstLen);
    System.arraycopy(src, 0, CHARS, 0, copy);
    for (int i = copy; i < dstLen; i++) {
      CHARS[i] = 0;
    }
    return CHARS;
  }

  private static byte[] padNull(int dstLen) {
    if (dstLen > SCRATCH_LEN) {
      throw new IllegalStateException("SBE field exceeds scratch buffer: " + dstLen);
    }
    for (int i = 0; i < dstLen; i++) {
      CHARS[i] = 0;
    }
    return CHARS;
  }

  // ---------------------------------------------------------------------------
  // Enum mappings (FIX char → SBE enum). Throw on unmapped values.
  // ---------------------------------------------------------------------------

  private static SideEnum mapSide(char fix) {
    return switch (fix) {
      case '1' -> SideEnum.Buy;
      case '2' -> SideEnum.Sell;
      default -> throw new IllegalStateException("Unsupported FIX Side(54): " + fix);
    };
  }

  private static OrdTypeEnum mapOrdType(char fix) {
    return switch (fix) {
      case '1' -> OrdTypeEnum.Market;
      case '2' -> OrdTypeEnum.Limit;
      case 'D' -> OrdTypeEnum.PreviouslyQuoted;
      default -> throw new IllegalStateException("Unsupported FIX OrdType(40): " + fix);
    };
  }

  private static TimeInForceEnum mapTimeInForce(char fix) {
    return switch (fix) {
      case '0' -> TimeInForceEnum.Day;
      case '1' -> TimeInForceEnum.GTC;
      case '3' -> TimeInForceEnum.IOC;
      case '4' -> TimeInForceEnum.FOK;
      default -> throw new IllegalStateException("Unsupported FIX TimeInForce(59): " + fix);
    };
  }

  private static SettlTypeEnum mapSettlType(char fix) {
    return switch (fix) {
      case '0' -> SettlTypeEnum.Regular;
      case '1' -> SettlTypeEnum.Cash;
      case '2' -> SettlTypeEnum.NextDay;
      case '3' -> SettlTypeEnum.TPlus2;
      case '4' -> SettlTypeEnum.TPlus3;
      case '5' -> SettlTypeEnum.TPlus4;
      case '6' -> SettlTypeEnum.Future;
      case '7' -> SettlTypeEnum.WhenAndIfIssued;
      default -> throw new IllegalStateException("Unsupported FIX SettlType(63): " + fix);
    };
  }
}
