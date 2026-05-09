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
 * <p><b>Always emitted at every emit call site.</b> When JFR is disabled, the event payload write
 * is short-circuited to a few primitive moves on the TLAB-cached event slot — no heap allocation.
 * When JFR is enabled, primitive fields are stored directly into the JFR ring buffer; no field
 * here forces a per-event {@link String} construction (which would defeat the zero-alloc goal).
 *
 * <p>The {@link #quoteReqIdHigh} / {@link #quoteReqIdLow} pair encodes the first 16 ASCII bytes
 * of the QuoteReqID (FIX tag 131) as two little-endian {@code long}s, which the JFR consumer or a
 * post-processing step can decode back into the originating ASCII string. This avoids any per-
 * emit {@link String} allocation while preserving full diagnostic provenance.
 *
 * <p><b>Threading:</b> single-threaded cluster duty cycle.
 *
 * <p><b>Allocation:</b> zero allocation per event. JFR's TLAB-cached event slot is reused.
 */
@Name("com.trading.engine.cluster.RfqEmission")
@Label("RFQ Emission")
@Category({"trading", "rfq"})
@Description("Emission of an RFQ-lifecycle event (104/105/106/107) or NOS-with-quoteId accept.")
@StackTrace(false)
public final class RfqEmissionEvent extends Event {

  /**
   * Template ID of the emitted SBE event (104=Requested, 105=Created, 106=Rejected, 107=Expired).
   * Sentinel {@code 0} indicates a NOS-with-quoteId accept.
   */
  @Label("Template ID")
  public int templateId;

  /** First 8 ASCII bytes of QuoteReqID (FIX tag 131), little-endian packed into a {@code long}. */
  @Label("QuoteReqID High Lane")
  public long quoteReqIdHigh;

  /** Bytes 8–15 of QuoteReqID (FIX tag 131), little-endian packed into a {@code long}. */
  @Label("QuoteReqID Low Lane")
  public long quoteReqIdLow;

  /**
   * Cluster-time emission latency in nanoseconds — derived from cluster timestamps
   * ({@code emitTs - ingestTs}), <b>not</b> wall-clock time. Diagnostic only; null/zero when the
   * emit path does not measure latency.
   */
  @Label("Emit Latency (cluster ns)")
  public long latencyNanos;
}
