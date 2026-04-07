package com.trading.engine.gateway;

import com.trading.engine.fix.builder.ExecutionReportEncoder;
import com.trading.engine.messages.sbe.ExecTypeEnum;
import com.trading.engine.messages.sbe.ExecutionReportDecoder;
import com.trading.engine.messages.sbe.OrdStatusEnum;
import com.trading.engine.messages.sbe.SettlTypeEnum;
import com.trading.engine.messages.sbe.SideEnum;
import java.util.concurrent.TimeUnit;
import uk.co.real_logic.artio.fields.DecimalFloat;
import uk.co.real_logic.artio.fields.UtcTimestampEncoder;

/**
 * Translator from SBE decoders to Artio FIX 4.4 encoders. Each public method consumes an SBE
 * decoder positioned over a complete message and writes the field values into a caller-supplied
 * Artio encoder. The caller is responsible for the Artio session header (sender, target, seq num,
 * sending time) and for calling {@code encode()} on the resulting FIX message.
 *
 * <p><b>Threading.</b> This class is <em>not</em> thread-safe. Each instance owns mutable scratch
 * buffers, a {@link DecimalFloat}, and a {@link UtcTimestampEncoder} that are reused across calls;
 * concurrent invocations on the same instance would corrupt them. The gateway is expected to
 * construct one {@code SbeToFixTranslator} per egress duty-cycle thread (typically one per FIX
 * session worker) and never share an instance across threads.
 *
 * <p>The instance-based design is the standard Aeron/Artio pattern: per-thread state lives on a
 * per-thread instance, and the cost of constructing the instance is paid once at startup, after
 * which every {@code translateXxx} call is zero-allocation.
 *
 * <p><b>Allocation.</b> Zero allocation on every method. The SBE decoder's {@code getXxx(byte[],
 * int)} accessors copy char fields into <em>per-field</em> dedicated instance {@code byte[]}
 * scratch buffers (one per char field per message type), and the trailing null bytes are stripped
 * before handing the actual length to the Artio encoder's {@code xxx(byte[], int, int)} setter.
 * Per-field buffers are required because Artio's non-{@code AsCopy} setters store a
 * <em>reference</em> to the byte array and only read it when {@code encode()} is called — a single
 * shared scratch would be silently overwritten between fields. The {@code AsCopy} variants would
 * allocate per call, violating the zero-allocation rule. Numeric fields flow through {@link
 * FixedPoint#toDecimalFloat} into a single shared {@link DecimalFloat}. Timestamps flow through a
 * single shared {@link UtcTimestampEncoder}.
 *
 * <p><b>Errors.</b> Unmapped enum values throw {@link IllegalStateException} with a string-literal
 * message naming the field. The gateway is expected to catch and surface as a session-level FIX
 * reject. {@code NULL_VAL} on a required field is treated as a fatal cluster bug and throws.
 *
 * <p><b>Multileg out of scope for this commit.</b> The {@code noLegs} repeating group on
 * ExecutionReport is not yet handled — the cluster's leg-level fills are ignored at the FIX
 * boundary until a follow-up commit lands. APP-13 (FixGateway) only needs the single-leg path for
 * the basic FIX NOS → ExecutionReport flow.
 */
public final class SbeToFixTranslator {

  // Per-field dedicated scratch buffers. Artio's FIX encoder xxx(byte[], int, int) setters
  // store a *reference* to the byte array (no copy) and only read it during encode(); using a
  // single shared scratch would let later setters silently overwrite earlier ones. The
  // alternative — Artio's xxxAsCopy variants — allocates per call, which violates the
  // zero-allocation rule. So each char field gets its own byte[] sized to the SBE field length.
  //
  // ExecutionReport char fields:
  private final byte[] erOrderId =
      new byte[com.trading.engine.messages.sbe.ExecutionReportDecoder.orderIdLength()];
  private final byte[] erExecId =
      new byte[com.trading.engine.messages.sbe.ExecutionReportDecoder.execIdLength()];
  private final byte[] erClOrdId =
      new byte[com.trading.engine.messages.sbe.ExecutionReportDecoder.clOrdIdLength()];
  private final byte[] erSymbol =
      new byte[com.trading.engine.messages.sbe.ExecutionReportDecoder.symbolLength()];
  private final byte[] erText =
      new byte[com.trading.engine.messages.sbe.ExecutionReportDecoder.textLength()];
  private final byte[] erSettlDate =
      new byte[com.trading.engine.messages.sbe.ExecutionReportDecoder.settlDateLength()];
  private final byte[] erCurrency =
      new byte[com.trading.engine.messages.sbe.ExecutionReportDecoder.currencyLength()];
  private final byte[] erSettlCurrency =
      new byte[com.trading.engine.messages.sbe.ExecutionReportDecoder.settlCurrencyLength()];

  /** Reusable DecimalFloat for FIX decimal field setters. */
  private final DecimalFloat dec = new DecimalFloat();

  /** Reusable UTC timestamp encoder for transactTime fields. */
  private final UtcTimestampEncoder tsEnc = new UtcTimestampEncoder();

  public SbeToFixTranslator() {}

  // ---------------------------------------------------------------------------
  // ExecutionReport (35=8)
  // ---------------------------------------------------------------------------

  /**
   * Translate an SBE ExecutionReport into the supplied Artio FIX 4.4 encoder. The caller must have
   * wrapped {@code sbe} over a complete decoded ExecutionReport message and must populate the FIX
   * session header on {@code fix} before calling {@code encode}.
   */
  public void translateExecutionReport(ExecutionReportDecoder sbe, ExecutionReportEncoder fix) {
    // orderID — required. SBE field is null-padded; trim before passing to FIX.
    sbe.getOrderId(erOrderId, 0);
    fix.orderID(erOrderId, 0, trimNulls(erOrderId));

    // execID — required
    sbe.getExecId(erExecId, 0);
    fix.execID(erExecId, 0, trimNulls(erExecId));

    // clOrdID — required (per FIX 4.4 ER spec)
    sbe.getClOrdId(erClOrdId, 0);
    fix.clOrdID(erClOrdId, 0, trimNulls(erClOrdId));

    // execType — required, full enum exhaustion
    fix.execType(mapExecType(sbe.execType()));

    // ordStatus — required, full enum exhaustion
    fix.ordStatus(mapOrdStatus(sbe.ordStatus()));

    // symbol — required
    sbe.getSymbol(erSymbol, 0);
    fix.instrument().symbol(erSymbol, 0, trimNulls(erSymbol));

    // side — required
    fix.side(mapSide(sbe.side()));

    // leavesQty — required
    FixedPoint.toDecimalFloat(sbe.leavesQty(), dec);
    fix.leavesQty(dec);

    // cumQty — required
    FixedPoint.toDecimalFloat(sbe.cumQty(), dec);
    fix.cumQty(dec);

    // avgPx — required (FIX 4.4 ER), but SBE allows null on Rejected. If absent, write 0.
    long avg = sbe.avgPx();
    if (avg == ExecutionReportDecoder.avgPxNullValue()) {
      avg = 0L;
    }
    FixedPoint.toDecimalFloat(avg, dec);
    fix.avgPx(dec);

    // transactTime — required. SBE is uint64 epoch nanos; encode as Artio's UTC timestamp ASCII.
    int tsLen = tsEnc.encodeFrom(sbe.transactTime(), TimeUnit.NANOSECONDS);
    fix.transactTime(tsEnc.buffer(), 0, tsLen);

    // text — optional
    sbe.getText(erText, 0);
    int trimmed = trimNulls(erText);
    if (trimmed > 0) {
      fix.text(erText, 0, trimmed);
    }

    // settlType — optional
    if (sbe.settlType() != SettlTypeEnum.NULL_VAL) {
      fix.settlType(mapSettlTypeToFix(sbe.settlType()));
    }

    // settlDate — optional
    sbe.getSettlDate(erSettlDate, 0);
    trimmed = trimNulls(erSettlDate);
    if (trimmed > 0) {
      fix.settlDate(erSettlDate, 0, trimmed);
    }

    // currency — optional
    sbe.getCurrency(erCurrency, 0);
    trimmed = trimNulls(erCurrency);
    if (trimmed > 0) {
      fix.currency(erCurrency, 0, trimmed);
    }

    // settlCurrency — optional
    sbe.getSettlCurrency(erSettlCurrency, 0);
    trimmed = trimNulls(erSettlCurrency);
    if (trimmed > 0) {
      fix.settlCurrency(erSettlCurrency, 0, trimmed);
    }

    // productType / tenor — APP-45 (custom tags 10013/10001 not in stock FIX 4.4)
    // noLegs group — deferred, see class Javadoc
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /**
   * Find the length of the non-null prefix of {@code field}. SBE char fields are fixed length and
   * null-padded; FIX wire fields are variable length, so we drop the padding before passing the
   * actual length to the Artio encoder.
   */
  private static int trimNulls(byte[] field) {
    int len = field.length;
    while (len > 0 && field[len - 1] == 0) {
      len--;
    }
    return len;
  }

  // ---------------------------------------------------------------------------
  // Enum mappings (SBE enum → FIX char). Throw on NULL_VAL or unmapped values.
  // ---------------------------------------------------------------------------

  private static char mapSide(SideEnum sbe) {
    return switch (sbe) {
      case Buy -> '1';
      case Sell -> '2';
      default -> throw new IllegalStateException("Unsupported SBE Side for FIX wire: " + sbe);
    };
  }

  private static char mapExecType(ExecTypeEnum sbe) {
    return switch (sbe) {
      case New -> '0';
      case PartialFill -> '1';
      case Fill -> '2';
      case DoneForDay -> '3';
      case Canceled -> '4';
      case Replaced -> '5';
      case PendingCancel -> '6';
      case Stopped -> '7';
      case Rejected -> '8';
      case Suspended -> '9';
      case PendingNew -> 'A';
      case Calculated -> 'B';
      case Expired -> 'C';
      case Restated -> 'D';
      case PendingReplace -> 'E';
      case Trade -> 'F';
      case TradeCorrect -> 'G';
      case TradeCancel -> 'H';
      case OrderStatus -> 'I';
      default -> throw new IllegalStateException("Unsupported SBE ExecType for FIX wire: " + sbe);
    };
  }

  private static char mapOrdStatus(OrdStatusEnum sbe) {
    return switch (sbe) {
      case New -> '0';
      case PartiallyFilled -> '1';
      case Filled -> '2';
      case DoneForDay -> '3';
      case Canceled -> '4';
      case Replaced -> '5';
      case PendingCancel -> '6';
      case Stopped -> '7';
      case Rejected -> '8';
      case Suspended -> '9';
      case PendingNew -> 'A';
      case Calculated -> 'B';
      case Expired -> 'C';
      case AcceptedForBidding -> 'D';
      case PendingReplace -> 'E';
      default -> throw new IllegalStateException("Unsupported SBE OrdStatus for FIX wire: " + sbe);
    };
  }

  private static char mapSettlTypeToFix(SettlTypeEnum sbe) {
    return switch (sbe) {
      case Regular -> '0';
      case Cash -> '1';
      case NextDay -> '2';
      case TPlus2 -> '3';
      case TPlus3 -> '4';
      case TPlus4 -> '5';
      case Future -> '6';
      case WhenAndIfIssued -> '7';
      default -> throw new IllegalStateException("Unsupported SBE SettlType for FIX wire: " + sbe);
    };
  }
}
