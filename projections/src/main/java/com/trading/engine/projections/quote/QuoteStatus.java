package com.trading.engine.projections.quote;

/**
 * Projection-local lifecycle state for a quote in the read model. Tracks the full RFQ lifecycle
 * across multiple domain events, from initial request through to terminal disposition.
 *
 * <p>This enum is <b>distinct from</b> the SBE {@code QuoteStatusEnum} (FIX tag 297), which is a
 * wire-level enum on the Quote command message (template 2) with values {@code Accepted}, {@code
 * Canceled}, {@code Rejected}, {@code Expired}. The projection enum adds {@code Requested} (derived
 * from QuoteRequestedEvent, template 104) and {@code Used} (derived from OrderCreatedEvent,
 * template 100, when {@code ordType=PreviouslyQuoted}).
 *
 * <p><b>State transitions:</b>
 *
 * <ul>
 *   <li>{@code Requested} &rarr; {@code Active} (105), {@code Rejected} (106), {@code Expired}
 *       (107)
 *   <li>{@code Active} &rarr; {@code Expired} (107), {@code Used} (100)
 *   <li>{@code Rejected}, {@code Expired}, {@code Used} &rarr; <b>terminal</b> (no further
 *       transitions)
 * </ul>
 *
 * <p><b>Terminal state semantics:</b> once a quote reaches {@code Rejected}, {@code Expired}, or
 * {@code Used}, subsequent events for the same quoteReqId/quoteId must not change its status. This
 * matches FIX 4.4 semantics — you cannot expire a quote that was already executed or rejected.
 *
 * @see QuoteProjection
 */
public enum QuoteStatus {

  /**
   * Quote request received from client (QuoteRequestedEvent, template 104). No quote has been
   * generated yet — the pricing service has not responded. Non-terminal.
   */
  Requested,

  /**
   * Quote created and sent to client (QuoteCreatedEvent, template 105). The quote is live and
   * awaiting client action (accept or let expire). Non-terminal.
   */
  Active,

  /**
   * Quote request rejected (QuoteRejectedEvent, template 106). The pricing service declined or
   * validation failed. Terminal — no further transitions.
   */
  Rejected,

  /**
   * Quote expired without being accepted (QuoteExpiredEvent, template 107). The {@code validUntil}
   * timestamp elapsed. Terminal — no further transitions.
   */
  Expired,

  /**
   * Quote accepted and converted to an order (OrderCreatedEvent, template 100, with {@code
   * ordType=PreviouslyQuoted} and a matching quoteId). Terminal — no further transitions.
   */
  Used
}
