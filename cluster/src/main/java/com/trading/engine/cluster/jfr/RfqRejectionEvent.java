package com.trading.engine.cluster.jfr;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

/**
 * JFR event recorded on every {@code QuoteRejectedEvent} (106) emission and on every NOS-with-
 * quoteId reject reason from the §9.2a peek phase. Carries the reject reason byte and the ASCII
 * text constant for postmortem analysis.
 *
 * <p>Always emitted at every reject call site. JFR's TLAB fast path makes the event allocation-free
 * when JFR is disabled. The default JFR profile enables this event.
 *
 * <p><b>Threading:</b> single-threaded cluster duty cycle.
 *
 * <p><b>Allocation:</b> JFR commits use a TLAB-cached event slot when enabled; zero allocation when
 * disabled.
 */
@Name("com.trading.engine.cluster.RfqRejection")
@Label("RFQ Rejection")
@Category({"trading", "rfq"})
@Description("RFQ rejection (106 emission) or NOS-with-quoteId rejection.")
@StackTrace(false)
public final class RfqRejectionEvent extends Event {

  /**
   * SBE {@code QuoteRejectReasonEnum} value: 1=UnknownSymbol, 2=ExchangeClosed, 3=QuoteExceedsLimit,
   * 4=TooLateToEnter, 5=InvalidPrice, 99=Other.
   */
  @Label("Reject Reason Code")
  public byte reasonCode;

  /** ASCII content of the {@code text} field (FIX tag 58) — up to 64 bytes, NUL-trimmed. */
  @Label("Reject Text")
  public String text;
}
