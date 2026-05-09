package com.trading.engine.cluster.state;

/**
 * Internal lifecycle state of an {@link RfqSlot} inside the cluster's {@link RfqStateMachine} slot
 * pool. These states are cluster-internal only — they are never serialized to the wire; the SBE
 * schema uses {@link com.trading.engine.messages.sbe.RfqStateEnum} (Requested/Quoted/Accepted) for
 * snapshot template 203.
 *
 * <p>{@code FREE} is the only cluster-internal value with no wire equivalent. Terminal states
 * {@code REJECTED} and {@code EXPIRED} are not modelled here — when a slot transitions to a
 * terminal state the slot is immediately released back to {@code FREE} in the same processing step
 * that emits the corresponding event (106 or 107). This keeps the pool compact and avoids needing a
 * separate "recently-terminal" removal sweep.
 *
 * <p><b>Valid transitions:</b>
 *
 * <pre>
 * FREE → REQUESTED  (QuoteRequestHandler: slot acquired, 104 emitted)
 * REQUESTED → QUOTED (PriceResponseHandler: price accepted, 105 emitted)
 * REQUESTED → FREE   (PriceResponseHandler: price rejected → 106 emitted, slot released)
 * REQUESTED → FREE   (onTimerExpiry: request-timeout → 106 emitted, slot released)
 * QUOTED → ACCEPTED  (RfqStateMachine.commitAccept: NOS accepted, no event emitted here)
 * QUOTED → FREE      (onTimerExpiry: TTL expired → 107 emitted, slot released)
 * ACCEPTED → FREE    (Snapshot recovery only — slot in ACCEPTED state at restore is released
 *                     silently; the prior accept event was already journaled.)
 * </pre>
 *
 * <p><b>Threading:</b> single-threaded cluster duty cycle only. No synchronization required.
 *
 * @see RfqSlot
 * @see RfqStateMachine
 */
public enum RfqSlotState {

  /**
   * Slot is available in the free list. No RFQ occupies this slot. All mutable byte arrays contain
   * stale data from the previous lifecycle — callers must overwrite before reading.
   */
  FREE(0),

  /**
   * A {@code QuoteRequest} (template 1) has been accepted and a {@code QuoteRequestedEvent} (104)
   * emitted. The pricing service has been contacted but has not yet responded. Awaiting
   * {@code PriceResponse} (template 51) from the pricing service.
   */
  REQUESTED(1),

  /**
   * A {@code PriceResponse} (template 51) was accepted and a {@code QuoteCreatedEvent} (105) has
   * been emitted. The TTL timer is armed. The slot is live until the TTL fires (→ 107 + FREE) or a
   * {@code NewOrderSingle} with matching {@code quoteId} triggers commit-accept (→ ACCEPTED).
   */
  QUOTED(2),

  /**
   * A {@code NewOrderSingle} with {@code ordType=PreviouslyQuoted} and a matching {@code quoteId}
   * has been validated and accepted. {@link RfqStateMachine#commitAccept} transitions QUOTED →
   * ACCEPTED and immediately releases the slot back to FREE. This state is therefore transient —
   * it exists only in the brief window inside {@code commitAccept} before release. Snapshot encode
   * never observes it (single-threaded duty cycle; {@link RfqStateMachine#encodeInto} is never
   * called between peek and commit).
   */
  ACCEPTED(3);

  /** Numeric value for diagnostics / assertions. Not used in wire encoding. */
  private final int value;

  RfqSlotState(final int value) {
    this.value = value;
  }

  /**
   * Returns the numeric value of this state. For diagnostics and counter labelling only; never
   * serialized to the wire.
   *
   * @return the numeric value
   */
  public int value() {
    return value;
  }
}
