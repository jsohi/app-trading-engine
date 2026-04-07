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
 * Translator from Artio FIX 4.4 decoders (flyweight) to SBE encoders. Each public method writes a
 * complete SBE message (header + body) into the caller-supplied buffer at the given offset and
 * returns the total encoded length in bytes.
 *
 * <p><b>Threading.</b> This class is <em>not</em> thread-safe. Each instance owns mutable flyweight
 * encoders, a {@link UtcTimestampDecoder}, and a {@code byte[]} scratch buffer that are reused
 * across calls; concurrent invocations on the same instance would corrupt them. The gateway is
 * expected to construct one {@code FixToSbeTranslator} per ingress duty-cycle thread (typically one
 * per FIX session worker) and never share an instance across threads.
 *
 * <p>The instance-based design is the standard Aeron/Artio pattern: per-thread state lives on a
 * per-thread instance, and the cost of constructing the instance is paid once at startup, after
 * which every {@code translateXxx} call is zero-allocation.
 *
 * <p><b>Allocation.</b> Zero allocation on every translator method. No {@code new}, no boxing, no
 * {@code String}, no streams, no captured lambdas. Char-array fields are copied from the FIX
 * flyweight {@code char[]} into the per-instance {@link #chars} scratch byte buffer, padded with
 * {@code \0}, and then handed to the SBE encoder's {@code putXxx(byte[], int)} setter. Decimal
 * prices flow through {@link FixedPoint}.
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

  private final byte[] chars = new byte[SCRATCH_LEN];

  // SBE encoders are stateful flyweights — wrap() resets them on every call.
  private final MessageHeaderEncoder header = new MessageHeaderEncoder();
  private final NewOrderSingleEncoder nos = new NewOrderSingleEncoder();
  private final NewOrderMultilegEncoder nom = new NewOrderMultilegEncoder();
  private final CancelOrderRequestEncoder cor = new CancelOrderRequestEncoder();
  private final MultilegOrderCancelReplaceEncoder mocr = new MultilegOrderCancelReplaceEncoder();
  private final QuoteRequestEncoder qr = new QuoteRequestEncoder();

  /**
   * Artio's UTC-timestamp decoder is stateless on the read path; one per instance is sufficient.
   */
  private final UtcTimestampDecoder utcTs = new UtcTimestampDecoder(false);

  public FixToSbeTranslator() {}

  // ---------------------------------------------------------------------------
  // NewOrderSingle (35=D)
  // ---------------------------------------------------------------------------

  /**
   * Translate a FIX 4.4 NewOrderSingle (35=D) into an SBE {@code NewOrderSingleEncoder} message.
   *
   * @return total encoded length (header + body) in bytes
   */
  public int translateNewOrderSingle(
      NewOrderSingleDecoder fix, MutableDirectBuffer sbe, int offset) {
    nos.wrapAndApplyHeader(sbe, offset, header);

    nos.putClOrdId(
        padFromChars(fix.clOrdID(), fix.clOrdIDLength(), NewOrderSingleEncoder.clOrdIdLength()), 0);
    nos.putQuoteId(padNull(NewOrderSingleEncoder.quoteIdLength()), 0);
    nos.putSymbol(
        padFromChars(fix.symbol(), fix.symbolLength(), NewOrderSingleEncoder.symbolLength()), 0);
    nos.side(mapSide(fix.side()));
    nos.ordType(mapOrdType(fix.ordType()));
    nos.price(
        fix.hasPrice() ? FixedPoint.toInt64(fix.price()) : NewOrderSingleEncoder.priceNullValue());
    nos.orderQty(FixedPoint.toInt64(fix.orderQty()));
    nos.timeInForce(fix.hasTimeInForce() ? mapTimeInForce(fix.timeInForce()) : TimeInForceEnum.Day);
    nos.transactTime(utcTs.decodeNanos(fix.transactTime(), fix.transactTimeLength()));
    nos.putAccountCode(
        fix.hasAccount()
            ? padFromChars(
                fix.account(), fix.accountLength(), NewOrderSingleEncoder.accountCodeLength())
            : padNull(NewOrderSingleEncoder.accountCodeLength()),
        0);
    nos.productType(ProductTypeEnum.NULL_VAL); // APP-45
    nos.putSettlDate(
        fix.hasSettlDate()
            ? padFromBytes(
                fix.settlDate(), fix.settlDateLength(), NewOrderSingleEncoder.settlDateLength())
            : padNull(NewOrderSingleEncoder.settlDateLength()),
        0);
    nos.settlType(fix.hasSettlType() ? mapSettlType(fix.settlType()) : SettlTypeEnum.NULL_VAL);
    nos.putCurrency(
        fix.hasCurrency()
            ? padFromChars(
                fix.currency(), fix.currencyLength(), NewOrderSingleEncoder.currencyLength())
            : padNull(NewOrderSingleEncoder.currencyLength()),
        0);
    nos.putSettlCurrency(
        fix.hasSettlCurrency()
            ? padFromChars(
                fix.settlCurrency(),
                fix.settlCurrencyLength(),
                NewOrderSingleEncoder.settlCurrencyLength())
            : padNull(NewOrderSingleEncoder.settlCurrencyLength()),
        0);
    nos.tenor(TenorEnum.NULL_VAL); // APP-45

    return MessageHeaderEncoder.ENCODED_LENGTH + nos.encodedLength();
  }

  // ---------------------------------------------------------------------------
  // NewOrderMultileg (35=AB)
  // ---------------------------------------------------------------------------

  /**
   * Translate a FIX 4.4 NewOrderMultileg (35=AB) into an SBE {@code NewOrderMultilegEncoder}
   * including its noLegs repeating group.
   */
  public int translateNewOrderMultileg(
      NewOrderMultilegDecoder fix, MutableDirectBuffer sbe, int offset) {
    nom.wrapAndApplyHeader(sbe, offset, header);

    nom.putClOrdId(
        padFromChars(fix.clOrdID(), fix.clOrdIDLength(), NewOrderMultilegEncoder.clOrdIdLength()),
        0);
    nom.putQuoteId(padNull(NewOrderMultilegEncoder.quoteIdLength()), 0);
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

    final int legCount = fix.noLegsGroupCounter();
    final NewOrderMultilegEncoder.NoLegsEncoder legs = nom.noLegsCount(legCount);
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

    return MessageHeaderEncoder.ENCODED_LENGTH + nom.encodedLength();
  }

  // ---------------------------------------------------------------------------
  // OrderCancelRequest (35=F)
  // ---------------------------------------------------------------------------

  /**
   * Translate a FIX 4.4 OrderCancelRequest (35=F) into an SBE {@code CancelOrderRequestEncoder}.
   */
  public int translateOrderCancelRequest(
      OrderCancelRequestDecoder fix, MutableDirectBuffer sbe, int offset) {
    cor.wrapAndApplyHeader(sbe, offset, header);

    cor.putOrigClOrdId(
        padFromChars(
            fix.origClOrdID(),
            fix.origClOrdIDLength(),
            CancelOrderRequestEncoder.origClOrdIdLength()),
        0);
    cor.putClOrdId(
        padFromChars(fix.clOrdID(), fix.clOrdIDLength(), CancelOrderRequestEncoder.clOrdIdLength()),
        0);
    cor.putSymbol(
        padFromChars(fix.symbol(), fix.symbolLength(), CancelOrderRequestEncoder.symbolLength()),
        0);
    cor.side(mapSide(fix.side()));
    cor.transactTime(utcTs.decodeNanos(fix.transactTime(), fix.transactTimeLength()));
    cor.putAccountCode(
        fix.hasAccount()
            ? padFromChars(
                fix.account(), fix.accountLength(), CancelOrderRequestEncoder.accountCodeLength())
            : padNull(CancelOrderRequestEncoder.accountCodeLength()),
        0);
    cor.productType(ProductTypeEnum.NULL_VAL); // APP-45

    return MessageHeaderEncoder.ENCODED_LENGTH + cor.encodedLength();
  }

  // ---------------------------------------------------------------------------
  // MultilegOrderCancelReplaceRequest (35=AC)
  // ---------------------------------------------------------------------------

  /**
   * Translate a FIX 4.4 MultilegOrderCancelReplaceRequest (35=AC) into an SBE {@code
   * MultilegOrderCancelReplaceEncoder} including its noLegs repeating group.
   */
  public int translateMultilegOrderCancelReplace(
      MultilegOrderCancelReplaceRequestDecoder fix, MutableDirectBuffer sbe, int offset) {
    mocr.wrapAndApplyHeader(sbe, offset, header);

    mocr.putOrigClOrdId(
        padFromChars(
            fix.origClOrdID(),
            fix.origClOrdIDLength(),
            MultilegOrderCancelReplaceEncoder.origClOrdIdLength()),
        0);
    mocr.putOrderId(padNull(MultilegOrderCancelReplaceEncoder.orderIdLength()), 0);
    mocr.putClOrdId(
        padFromChars(
            fix.clOrdID(), fix.clOrdIDLength(), MultilegOrderCancelReplaceEncoder.clOrdIdLength()),
        0);
    mocr.putQuoteId(padNull(MultilegOrderCancelReplaceEncoder.quoteIdLength()), 0);
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

    final int legCount = fix.noLegsGroupCounter();
    final MultilegOrderCancelReplaceEncoder.NoLegsEncoder legs = mocr.noLegsCount(legCount);
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

    return MessageHeaderEncoder.ENCODED_LENGTH + mocr.encodedLength();
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
  public int translateQuoteRequest(QuoteRequestDecoder fix, MutableDirectBuffer sbe, int offset) {
    qr.wrapAndApplyHeader(sbe, offset, header);

    qr.putQuoteReqId(
        padFromChars(
            fix.quoteReqID(), fix.quoteReqIDLength(), QuoteRequestEncoder.quoteReqIdLength()),
        0);

    final QuoteRequestDecoder.RelatedSymGroupIterator iter = fix.relatedSymGroupIterator();
    if (iter.hasNext()) {
      final QuoteRequestDecoder.RelatedSymGroupDecoder firstRelatedSym = iter.next();
      qr.putSymbol(
          padFromChars(
              firstRelatedSym.symbol(),
              firstRelatedSym.symbolLength(),
              QuoteRequestEncoder.symbolLength()),
          0);
      qr.side(firstRelatedSym.hasSide() ? mapSide(firstRelatedSym.side()) : SideEnum.NULL_VAL);
      qr.orderQty(
          firstRelatedSym.hasOrderQty()
              ? FixedPoint.toInt64(firstRelatedSym.orderQty())
              : QuoteRequestEncoder.orderQtyNullValue());
      qr.putSettlDate(
          firstRelatedSym.hasSettlDate()
              ? padFromBytes(
                  firstRelatedSym.settlDate(),
                  firstRelatedSym.settlDateLength(),
                  QuoteRequestEncoder.settlDateLength())
              : padNull(QuoteRequestEncoder.settlDateLength()),
          0);
      qr.settlType(
          firstRelatedSym.hasSettlType()
              ? mapSettlType(firstRelatedSym.settlType())
              : SettlTypeEnum.NULL_VAL);
      qr.putCurrency(
          firstRelatedSym.hasCurrency()
              ? padFromChars(
                  firstRelatedSym.currency(),
                  firstRelatedSym.currencyLength(),
                  QuoteRequestEncoder.currencyLength())
              : padNull(QuoteRequestEncoder.currencyLength()),
          0);
      // Stock FIX 4.4 QuoteRequest's NoRelatedSym group has no SettlCurrency tag — APP-45 will
      // wire the trading-engine custom tag once the dictionary extension lands.
      qr.putSettlCurrency(padNull(QuoteRequestEncoder.settlCurrencyLength()), 0);
    } else {
      qr.putSymbol(padNull(QuoteRequestEncoder.symbolLength()), 0);
      qr.side(SideEnum.NULL_VAL);
      qr.orderQty(QuoteRequestEncoder.orderQtyNullValue());
      qr.putSettlDate(padNull(QuoteRequestEncoder.settlDateLength()), 0);
      qr.settlType(SettlTypeEnum.NULL_VAL);
      qr.putCurrency(padNull(QuoteRequestEncoder.currencyLength()), 0);
      qr.putSettlCurrency(padNull(QuoteRequestEncoder.settlCurrencyLength()), 0);
    }

    qr.putAccountCode(padNull(QuoteRequestEncoder.accountCodeLength()), 0);
    qr.transactTime(0L); // FIX QuoteRequest has no top-level transactTime
    qr.productType(ProductTypeEnum.NULL_VAL); // APP-45
    qr.tenor(TenorEnum.NULL_VAL); // APP-45
    qr.noLegsCount(0); // APP-47

    return MessageHeaderEncoder.ENCODED_LENGTH + qr.encodedLength();
  }

  // ---------------------------------------------------------------------------
  // Char-array helpers (zero-allocation, share the static CHARS scratch buffer)
  // ---------------------------------------------------------------------------

  private byte[] padFromChars(char[] src, int srcLen, int dstLen) {
    if (dstLen > SCRATCH_LEN) {
      throw new IllegalStateException("SBE field exceeds scratch buffer: " + dstLen);
    }
    final int copy = Math.min(srcLen, dstLen);
    for (int i = 0; i < copy; i++) {
      chars[i] = (byte) src[i];
    }
    for (int i = copy; i < dstLen; i++) {
      chars[i] = 0;
    }
    return chars;
  }

  private byte[] padFromBytes(byte[] src, int srcLen, int dstLen) {
    if (dstLen > SCRATCH_LEN) {
      throw new IllegalStateException("SBE field exceeds scratch buffer: " + dstLen);
    }
    final int copy = Math.min(srcLen, dstLen);
    System.arraycopy(src, 0, chars, 0, copy);
    for (int i = copy; i < dstLen; i++) {
      chars[i] = 0;
    }
    return chars;
  }

  private byte[] padNull(int dstLen) {
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
