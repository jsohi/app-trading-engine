package com.trading.engine.cluster.jfr;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

/**
 * JFR event recorded on every emission of an RFQ-lifecycle SBE event (templates 104, 105, 106, 107)
 * and on every NOS-with-quoteId acceptance (templateId=0 sentinel for "accept-via-NOS").
 *
 * <p>Always emitted at every emit call site. JFR's TLAB fast path makes the event allocation-free
 * when JFR is disabled. The default JFR profile in {@code cluster/src/main/resources/jfr/trading-
 * engine.jfc} enables this event with stack traces disabled.
 *
 * <p><b>Threading:</b> single-threaded cluster duty cycle.
 *
 * <p><b>Allocation:</b> JFR commits use a TLAB-cached event slot when enabled; zero allocation when
 * disabled.
 */
@Name("com.trading.engine.cluster.RfqEmission")
@Label("RFQ Emission")
@Category({"trading", "rfq"})
@Description("Emission of an RFQ-lifecycle event (104/105/106/107) or NOS-with-quoteId accept.")
@StackTrace(false)
public final class RfqEmissionEvent extends Event {

  /**
   * Template ID of the emitted SBE event (104=Requested, 105=Created, 106=Rejected, 107=Expired).
   * Sentinel {@code 0} indicates a NOS-with-quoteId accept (no event emitted by the cluster RFQ
   * path; the NewOrderSingleHandler emits OrderCreatedEvent on its own).
   */
  @Label("Template ID")
  public int templateId;

  /** ASCII content of the QuoteReqID (FIX tag 131) — up to 20 bytes, NUL-trimmed. */
  @Label("QuoteReqID")
  public String quoteReqId;

  /** Wall-clock latency in nanoseconds from message ingest to emit. Diagnostic only. */
  @Label("Emit Latency (ns)")
  public long latencyNanos;
}
