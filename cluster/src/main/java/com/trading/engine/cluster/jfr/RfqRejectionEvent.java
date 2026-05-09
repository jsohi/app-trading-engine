package com.trading.engine.cluster.jfr;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

/**
 * JFR event recorded on every {@code QuoteRejectedEvent} (106) emission and on every NOS-with-
 * quoteId reject reason from the §9.2a peek phase. Carries the reject reason byte and a packed
 * 16-byte ASCII text identifier for postmortem analysis.
 *
 * <p><b>Always emitted at every reject call site.</b> All fields are primitive — no per-event heap
 * allocation when JFR is enabled or disabled. The {@link #textHigh} / {@link #textLow} pair encodes
 * the first 16 ASCII bytes of the {@code text} field (FIX tag 58, 64-byte fixed length) for
 * diagnostic identification; the JFR consumer or post-processing step can decode the lanes back
 * into ASCII.
 *
 * <p><b>Threading:</b> single-threaded cluster duty cycle.
 *
 * <p><b>Allocation:</b> zero allocation per event.
 */
@Name("com.trading.engine.cluster.RfqRejection")
@Label("RFQ Rejection")
@Category({"trading", "rfq"})
@Description("RFQ rejection (106 emission) or NOS-with-quoteId rejection.")
@StackTrace(false)
public final class RfqRejectionEvent extends Event {

  /**
   * SBE {@code QuoteRejectReasonEnum} value: 1=UnknownSymbol, 2=ExchangeClosed,
   * 3=QuoteExceedsLimit, 4=TooLateToEnter, 5=InvalidPrice, 99=Other.
   */
  @Label("Reject Reason Code")
  public byte reasonCode;

  /** First 8 ASCII bytes of the {@code text} field (FIX tag 58), little-endian packed. */
  @Label("Text High Lane")
  public long textHigh;

  /** Bytes 8–15 of the {@code text} field, little-endian packed. */
  @Label("Text Low Lane")
  public long textLow;
}
