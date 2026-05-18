package com.trading.engine.messages.telemetry;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.EventType;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;
import jdk.jfr.Threshold;

/**
 * JFR custom event emitted by {@link com.trading.engine.pricing.market.MarketDataPublisher} every
 * time a tick is dropped before or during the Aeron publication attempt.
 *
 * <p><b>Sampling model.</b> Annotated with {@code @Threshold(value = "0 ms")} so <em>every</em>
 * rejection emits a JFR event regardless of the JFR recording's configured threshold. Rejections
 * are pathological events (crossed market, non-positive price, back-pressure, no subscribers) —
 * missing even one reject under a sampling window would mask a real production anomaly. Volume is
 * self-limiting: a healthy publisher emits zero rejects per second in steady state; an unhealthy
 * publisher emitting thousands of rejects per second is itself the diagnostic signal to capture.
 *
 * <p><b>Allocation model.</b> The event object allocation itself is gated by a cheap
 * pre-construction {@link EventType#isEnabled()} check on the cached {@link #TYPE} field — NOT by
 * the post-construction {@code shouldCommit()} call. HotSpot escape analysis cannot prove {@code
 * Event.shouldCommit()} is pure (it dispatches through a native method) and therefore cannot
 * scalar-replace the {@code new Event()} allocation; under JFR-on the unguarded {@code new ...();
 * if (e.shouldCommit())} pattern leaks ~96 B/instance onto the heap on every emit. {@link
 * EventType#isEnabled()} is a cheap volatile read that returns {@code false} when no recording has
 * subscribed to this event type — the canonical fast-path "is anyone listening" check recommended
 * by the OpenJDK JFR team. When disabled (the steady-state production case AND the JFR-off case),
 * no {@code Event} subclass is allocated at all. On the recording path, the fields are one {@code
 * int} (reason ordinal) and one {@code String} (symbol). Callers MUST use this emit shape:
 *
 * <pre>{@code
 * if (MarketDataTickRejected.TYPE.isEnabled()) {
 *     final var e = new MarketDataTickRejected();
 *     e.reasonOrdinal = reason.ordinal();
 *     e.symbol = symbolStr;
 *     e.commit();
 * }
 * }</pre>
 *
 * <p><b>Threading model.</b> Instances are always created and committed on the pricing-service
 * agent thread (the single-writer thread of {@link
 * com.trading.engine.pricing.market.MarketDataPublisher}). Never shared across threads.
 *
 * <p><b>Reason ordinal mapping.</b> The {@link #reasonOrdinal} field stores the {@link
 * com.trading.engine.pricing.market.RejectReason#ordinal()} value. Mission Control can decode the
 * human-readable name by joining against the {@code RejectReason} enum declaration. The ordinal is
 * stored instead of the name string to avoid a {@code String} allocation on the hot path for every
 * rejection:
 *
 * <ul>
 *   <li>{@code 0} — {@code CROSSED} (bid &ge; ask)
 *   <li>{@code 1} — {@code NON_POSITIVE} (bid &le; 0 or ask &le; 0)
 *   <li>{@code 2} — {@code UNCONFIGURED} (symbol not in config registry)
 *   <li>{@code 3} — {@code BACK_PRESSURED} (Aeron -2)
 *   <li>{@code 4} — {@code NOT_CONNECTED} (Aeron -1)
 *   <li>{@code 5} — {@code ADMIN_ACTION} (Aeron -3)
 *   <li>{@code 6} — {@code MAX_POSITION_EXCEEDED} (Aeron -5)
 * </ul>
 *
 * <p><b>Design rationale.</b> Rejects are low-frequency in a healthy system, so the overhead of
 * {@code @Threshold(value = "0 ms")} (emit every event) is acceptable. The ordinal-instead-of-name
 * choice keeps the event allocation-friendly on paths that do reject frequently (e.g. Aeron {@code
 * NOT_CONNECTED} during cluster startup, where hundreds of rejects may occur before subscribers
 * attach).
 *
 * @see MarketDataTickPublished
 * @see MarketDataFeedStateTransition
 * @see com.trading.engine.pricing.market.RejectReason
 */
@Name("trading.MarketDataTickRejected")
@Label("Market Data Tick Rejected")
@Description(
    "Emitted on every market-data tick rejection (crossed market, non-positive, Aeron back-pressure, etc.).")
@Category({"Trading Engine", "Market Data"})
@Threshold("0 ms")
@StackTrace(false)
public final class MarketDataTickRejected extends Event {

  /**
   * Cached {@link EventType} for the cheap pre-allocation {@link EventType#isEnabled()} gate — see
   * emit sites for the pattern.
   */
  public static final EventType TYPE = EventType.getEventType(MarketDataTickRejected.class);

  /**
   * {@link com.trading.engine.pricing.market.RejectReason#ordinal()} of the drop cause. See the
   * Javadoc for the full ordinal-to-name mapping.
   */
  @Label("Reason Ordinal")
  public int reasonOrdinal;

  /**
   * Symbol string at the time of rejection. For input-validation rejects ({@code CROSSED}, {@code
   * NON_POSITIVE}) this is the symbol associated with the inbound tick. For Aeron return-code
   * rejects ({@code BACK_PRESSURED}, etc.) this is the symbol that was being published. An empty
   * string ({@code ""}) is set when the symbol cannot be determined (defensive fallback only).
   */
  @Label("Symbol")
  public String symbol;
}
