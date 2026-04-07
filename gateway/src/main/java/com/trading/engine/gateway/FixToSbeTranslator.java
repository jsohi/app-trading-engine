package com.trading.engine.gateway;

import com.trading.engine.fix.decoder_flyweight.MassQuoteDecoder;
import com.trading.engine.fix.decoder_flyweight.MultilegOrderCancelReplaceRequestDecoder;
import com.trading.engine.fix.decoder_flyweight.NewOrderMultilegDecoder;
import com.trading.engine.fix.decoder_flyweight.NewOrderSingleDecoder;
import com.trading.engine.fix.decoder_flyweight.OrderCancelRequestDecoder;
import com.trading.engine.fix.decoder_flyweight.QuoteRequestDecoder;
import com.trading.engine.messages.sbe.CancelOrderRequestEncoder;
import com.trading.engine.messages.sbe.MassQuoteEncoder;
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
   * field in {@code trading-schema.xml} (largest is 20 bytes for clOrdId/origClOrdId/etc.). Both
   * the class-init {@code static {}} block below AND the per-call guards in {@link #padFromChars} /
   * {@link #padFromBytes} / {@link #padNull} enforce {@code dstLen <= SCRATCH_LEN}. The static
   * block is belt-and-braces against common-case fields; the runtime check is the safety net for
   * fields the static block doesn't enumerate.
   */
  private static final int SCRATCH_LEN = 64;

  // Class-init sanity check on the most common SBE char fields. This is intentionally NOT
  // exhaustive (there are ~30 distinct char fields across all message types and groups) — the
  // pad helpers below also runtime-check `dstLen > SCRATCH_LEN` so a future schema change that
  // widens any field will throw a clean IllegalStateException at the call site, not AIOOBE.
  static {
    final int max =
        Math.max(
            NewOrderSingleEncoder.clOrdIdLength(),
            Math.max(
                NewOrderMultilegEncoder.clOrdIdLength(),
                Math.max(
                    MultilegOrderCancelReplaceEncoder.clOrdIdLength(),
                    Math.max(
                        CancelOrderRequestEncoder.origClOrdIdLength(),
                        Math.max(
                            QuoteRequestEncoder.quoteReqIdLength(),
                            MassQuoteEncoder.quoteIdLength())))));
    if (max > SCRATCH_LEN) {
      throw new ExceptionInInitializerError(
          "FixToSbeTranslator SCRATCH_LEN=" + SCRATCH_LEN + " too small for SBE field " + max);
    }
  }

  private final byte[] chars = new byte[SCRATCH_LEN];

  // SBE encoders are stateful flyweights — wrap() resets them on every call.
  private final MessageHeaderEncoder header = new MessageHeaderEncoder();
  private final NewOrderSingleEncoder nos = new NewOrderSingleEncoder();
  private final NewOrderMultilegEncoder nom = new NewOrderMultilegEncoder();
  private final CancelOrderRequestEncoder cor = new CancelOrderRequestEncoder();
  private final MultilegOrderCancelReplaceEncoder mocr = new MultilegOrderCancelReplaceEncoder();
  private final QuoteRequestEncoder qr = new QuoteRequestEncoder();
  private final MassQuoteEncoder mq = new MassQuoteEncoder();

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
    nos.putQuoteId(
        fix.hasQuoteID()
            ? padFromChars(
                fix.quoteID(), fix.quoteIDLength(), NewOrderSingleEncoder.quoteIdLength())
            : padNull(NewOrderSingleEncoder.quoteIdLength()),
        0);
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
      legs.legOrderQty(
          leg.hasLegQty()
              ? FixedPoint.toInt64(leg.legQty())
              : NewOrderMultilegEncoder.NoLegsEncoder.legOrderQtyNullValue());
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
          leg.hasLegQty()
              ? FixedPoint.toInt64(leg.legQty())
              : MultilegOrderCancelReplaceEncoder.NoLegsEncoder.legOrderQtyNullValue());
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

    // FIX QuoteRequest has no top-level symbol/side/qty/account/transactTime — they all live
    // inside the NoRelatedSym group. Walk the iterator once and copy from the first entry into
    // the SBE flat layout.
    final QuoteRequestDecoder.RelatedSymGroupIterator iter = fix.relatedSymGroupIterator();
    if (iter.hasNext()) {
      final QuoteRequestDecoder.RelatedSymGroupDecoder rs = iter.next();
      qr.putSymbol(
          padFromChars(rs.symbol(), rs.symbolLength(), QuoteRequestEncoder.symbolLength()), 0);
      qr.side(rs.hasSide() ? mapSide(rs.side()) : SideEnum.NULL_VAL);
      qr.orderQty(
          rs.hasOrderQty()
              ? FixedPoint.toInt64(rs.orderQty())
              : QuoteRequestEncoder.orderQtyNullValue());
      qr.putSettlDate(
          rs.hasSettlDate()
              ? padFromBytes(
                  rs.settlDate(), rs.settlDateLength(), QuoteRequestEncoder.settlDateLength())
              : padNull(QuoteRequestEncoder.settlDateLength()),
          0);
      qr.settlType(rs.hasSettlType() ? mapSettlType(rs.settlType()) : SettlTypeEnum.NULL_VAL);
      qr.putCurrency(
          rs.hasCurrency()
              ? padFromChars(
                  rs.currency(), rs.currencyLength(), QuoteRequestEncoder.currencyLength())
              : padNull(QuoteRequestEncoder.currencyLength()),
          0);
      // Stock FIX 4.4 QuoteRequest's NoRelatedSym group has no SettlCurrency tag — APP-45 will
      // wire the trading-engine custom tag once the dictionary extension lands.
      qr.putSettlCurrency(padNull(QuoteRequestEncoder.settlCurrencyLength()), 0);
      qr.putAccountCode(
          rs.hasAccount()
              ? padFromChars(
                  rs.account(), rs.accountLength(), QuoteRequestEncoder.accountCodeLength())
              : padNull(QuoteRequestEncoder.accountCodeLength()),
          0);
      // Use the SBE null sentinel (Long.MAX_VALUE for uint64 — see QuoteRequestEncoder
      // .transactTimeNullValue()) rather than 0L (epoch-zero) when transactTime is absent.
      // 0L on the wire would silently look like a valid 1970-01-01 timestamp downstream and
      // confuse audit/replay tooling. CONSUMER CONTRACT: cluster code consuming this field
      // MUST compare against transactTimeNullValue() before using — uint64's null sentinel
      // is a magic value, not Java null, and naïve comparisons / Instant.ofEpochNano calls
      // will produce nonsense. APP-13 (FixGateway) is the first consumer.
      qr.transactTime(
          rs.hasTransactTime()
              ? utcTs.decodeNanos(rs.transactTime(), rs.transactTimeLength())
              : QuoteRequestEncoder.transactTimeNullValue());
    } else {
      qr.putSymbol(padNull(QuoteRequestEncoder.symbolLength()), 0);
      qr.side(SideEnum.NULL_VAL);
      qr.orderQty(QuoteRequestEncoder.orderQtyNullValue());
      qr.putSettlDate(padNull(QuoteRequestEncoder.settlDateLength()), 0);
      qr.settlType(SettlTypeEnum.NULL_VAL);
      qr.putCurrency(padNull(QuoteRequestEncoder.currencyLength()), 0);
      qr.putSettlCurrency(padNull(QuoteRequestEncoder.settlCurrencyLength()), 0);
      qr.putAccountCode(padNull(QuoteRequestEncoder.accountCodeLength()), 0);
      qr.transactTime(QuoteRequestEncoder.transactTimeNullValue());
    }
    qr.productType(ProductTypeEnum.NULL_VAL); // APP-45
    qr.tenor(TenorEnum.NULL_VAL); // APP-45
    qr.noLegsCount(0); // APP-47

    return MessageHeaderEncoder.ENCODED_LENGTH + qr.encodedLength();
  }

  // ---------------------------------------------------------------------------
  // MassQuote (35=i)
  //
  // FIX 4.4 nests entries inside QuoteSets: noQuoteSets[N] → each set has noQuoteEntries[M].
  // The SBE schema flattens that into a single noQuoteEntries group on MassQuote. We do a
  // two-pass walk: first pass sums the total entry count across all sets, then we open the
  // SBE group with that count and walk again, copying every inner entry into the flat group.
  // Both walks reuse Artio's flyweight iterators (no allocation).
  // ---------------------------------------------------------------------------

  /** Translate a FIX 4.4 MassQuote (35=i) into an SBE {@code MassQuoteEncoder}. */
  public int translateMassQuote(MassQuoteDecoder fix, MutableDirectBuffer sbe, int offset) {
    mq.wrapAndApplyHeader(sbe, offset, header);

    mq.putQuoteId(
        padFromChars(fix.quoteID(), fix.quoteIDLength(), MassQuoteEncoder.quoteIdLength()), 0);
    mq.putAccountCode(
        fix.hasAccount()
            ? padFromChars(fix.account(), fix.accountLength(), MassQuoteEncoder.accountCodeLength())
            : padNull(MassQuoteEncoder.accountCodeLength()),
        0);
    // FIX MassQuote has no top-level transactTime; it lives on QuoteSet entries. Use the SBE
    // null sentinel rather than 0L (epoch-zero) so downstream tooling can distinguish "missing"
    // from a real 1970-01-01 timestamp. APP-45 may pull from the first set's value.
    // CONSUMER CONTRACT: same as translateQuoteRequest above — compare against
    // MassQuoteDecoder.transactTimeNullValue() before using; magic-value semantics, not
    // Java null.
    mq.transactTime(MassQuoteEncoder.transactTimeNullValue());

    // First pass: sum the inner-group counts. NOTE Artio's `quoteSetsGroupIterator()` returns
    // the SAME cached iterator instance both times (the accessor calls reset() and returns
    // `this`), so the second pass below re-uses it after a fresh reset — no allocation, but the
    // two locals are aliases.
    int totalEntries = 0;
    MassQuoteDecoder.QuoteSetsGroupIterator setsIter = fix.quoteSetsGroupIterator();
    while (setsIter.hasNext()) {
      final MassQuoteDecoder.QuoteSetsGroupDecoder set = setsIter.next();
      // Mirror Artio's own defensive pattern — a QuoteSet with zero entries is legal per FIX
      // and the unguarded counter accessor throws when validation is enabled.
      if (set.hasNoQuoteEntriesGroupCounter()) {
        totalEntries += set.noQuoteEntriesGroupCounter();
      }
    }

    // Second pass: open the flat SBE group and copy each FIX entry. fix.quoteSetsGroupIterator()
    // call resets the same cached iterator so we can walk again.
    final MassQuoteEncoder.NoQuoteEntriesEncoder sbeEntries = mq.noQuoteEntriesCount(totalEntries);
    setsIter = fix.quoteSetsGroupIterator();
    while (setsIter.hasNext()) {
      final MassQuoteDecoder.QuoteSetsGroupDecoder set = setsIter.next();
      final MassQuoteDecoder.QuoteSetsGroupDecoder.QuoteEntriesGroupIterator entriesIter =
          set.quoteEntriesGroupIterator();
      while (entriesIter.hasNext()) {
        final MassQuoteDecoder.QuoteSetsGroupDecoder.QuoteEntriesGroupDecoder entry =
            entriesIter.next();
        sbeEntries.next();
        sbeEntries.putQuoteEntryId(
            padFromChars(
                entry.quoteEntryID(),
                entry.quoteEntryIDLength(),
                MassQuoteEncoder.NoQuoteEntriesEncoder.quoteEntryIdLength()),
            0);
        sbeEntries.putSymbol(
            padFromChars(
                entry.symbol(),
                entry.symbolLength(),
                MassQuoteEncoder.NoQuoteEntriesEncoder.symbolLength()),
            0);
        sbeEntries.bidPx(
            entry.hasBidPx()
                ? FixedPoint.toInt64(entry.bidPx())
                : MassQuoteEncoder.NoQuoteEntriesEncoder.bidPxNullValue());
        sbeEntries.offerPx(
            entry.hasOfferPx()
                ? FixedPoint.toInt64(entry.offerPx())
                : MassQuoteEncoder.NoQuoteEntriesEncoder.offerPxNullValue());
        sbeEntries.bidSize(
            entry.hasBidSize()
                ? FixedPoint.toInt64(entry.bidSize())
                : MassQuoteEncoder.NoQuoteEntriesEncoder.bidSizeNullValue());
        sbeEntries.offerSize(
            entry.hasOfferSize()
                ? FixedPoint.toInt64(entry.offerSize())
                : MassQuoteEncoder.NoQuoteEntriesEncoder.offerSizeNullValue());
        sbeEntries.productType(ProductTypeEnum.NULL_VAL); // APP-45
        sbeEntries.putSettlDate(
            entry.hasSettlDate()
                ? padFromBytes(
                    entry.settlDate(),
                    entry.settlDateLength(),
                    MassQuoteEncoder.NoQuoteEntriesEncoder.settlDateLength())
                : padNull(MassQuoteEncoder.NoQuoteEntriesEncoder.settlDateLength()),
            0);
        // SettlType not in stock FIX 4.4 MassQuote QuoteEntry — APP-45
        sbeEntries.settlType(SettlTypeEnum.NULL_VAL);
        sbeEntries.putCurrency(
            entry.hasCurrency()
                ? padFromChars(
                    entry.currency(),
                    entry.currencyLength(),
                    MassQuoteEncoder.NoQuoteEntriesEncoder.currencyLength())
                : padNull(MassQuoteEncoder.NoQuoteEntriesEncoder.currencyLength()),
            0);
        // settlCurrency not in stock FIX 4.4 MassQuote QuoteEntry — APP-45
        sbeEntries.putSettlCurrency(
            padNull(MassQuoteEncoder.NoQuoteEntriesEncoder.settlCurrencyLength()), 0);
        sbeEntries.tenor(TenorEnum.NULL_VAL); // APP-45
      }
    }

    return MessageHeaderEncoder.ENCODED_LENGTH + mq.encodedLength();
  }

  // ---------------------------------------------------------------------------
  // Char-array helpers (zero-allocation, share the per-instance `chars` scratch buffer)
  // ---------------------------------------------------------------------------

  // The class-init `static {}` block above provides belt-and-braces validation against the
  // common-case clOrdId/quoteId fields, but there are ~30 distinct SBE char fields the
  // translator can write across all message types and the static block doesn't enumerate all
  // of them. The runtime `dstLen > SCRATCH_LEN` check below is a single int compare on the
  // cold path of helper invocation — JIT inlines it away from the cumulative cost of the
  // ASCII→byte copy loop — and prevents AIOOBE / silent corruption if any future schema
  // change widens an SBE char field beyond the scratch buffer without anyone updating the
  // static block.

  private byte[] padFromChars(char[] src, int srcLen, int dstLen) {
    if (dstLen > SCRATCH_LEN) {
      throw new IllegalStateException("SBE field exceeds scratch buffer: " + dstLen);
    }
    // Throw on overflow rather than silently truncating: in a trading system, truncating a
    // ClOrdID or Symbol leaks beyond the gateway as a corrupted identifier that prevents the
    // client from matching its execution back to the original order. Better to reject the
    // message at the boundary than to ship a broken one downstream.
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

  private byte[] padFromBytes(byte[] src, int srcLen, int dstLen) {
    if (dstLen > SCRATCH_LEN) {
      throw new IllegalStateException("SBE field exceeds scratch buffer: " + dstLen);
    }
    // Same overflow contract as padFromChars — never silently truncate identifier-like fields.
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
      case '8' -> SettlTypeEnum.SellersOption;
      case '9' -> SettlTypeEnum.TPlus5;
      case 'B' -> SettlTypeEnum.BrokenDate;
      case 'C' -> SettlTypeEnum.FXSpotNextDay;
      default -> throw new IllegalStateException("Unsupported FIX SettlType(63): " + fix);
    };
  }
}
