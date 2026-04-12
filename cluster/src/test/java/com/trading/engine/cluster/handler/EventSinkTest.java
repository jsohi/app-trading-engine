package com.trading.engine.cluster.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.trading.engine.messages.sbe.AccountLoadRejectedEventEncoder;
import com.trading.engine.messages.sbe.AccountLoadedEventEncoder;
import com.trading.engine.messages.sbe.CurrencyLoadRejectedEventEncoder;
import com.trading.engine.messages.sbe.CurrencyLoadedEventEncoder;
import com.trading.engine.messages.sbe.OrderCancelRejectedEventEncoder;
import com.trading.engine.messages.sbe.OrderCanceledEventEncoder;
import com.trading.engine.messages.sbe.OrderCreatedEventEncoder;
import com.trading.engine.messages.sbe.OrderFilledEventEncoder;
import com.trading.engine.messages.sbe.OrderRejectedEventEncoder;
import com.trading.engine.messages.sbe.PriceReceivedEventEncoder;
import com.trading.engine.messages.sbe.PriceRequestedEventEncoder;
import com.trading.engine.messages.sbe.QuoteCreatedEventEncoder;
import com.trading.engine.messages.sbe.QuoteExpiredEventEncoder;
import com.trading.engine.messages.sbe.QuoteRejectedEventEncoder;
import com.trading.engine.messages.sbe.QuoteRequestedEventEncoder;
import com.trading.engine.messages.sbe.RiskLimitLoadRejectedEventEncoder;
import com.trading.engine.messages.sbe.RiskLimitLoadedEventEncoder;
import org.junit.jupiter.api.Test;

/**
 * Compile-time safety assertions for the {@link EventSink} fixed-offset stamping convention. Every
 * domain event (template IDs 100-116) must have {@code sequenceNumber} at body offset 0 and {@code
 * timestamp} at body offset 8. EventSink stamps these fields using raw {@code putLong} at fixed
 * offsets — if any SBE schema change moves these fields, these tests fail immediately.
 */
class EventSinkTest {

  /**
   * Verifies that ALL domain event encoders place sequenceNumber at body offset 0 and timestamp at
   * body offset 8. This is the safety net for EventSink's fixed-offset stamping.
   */
  @Test
  void eventEncoderOffsetConventionHolds() {
    // OrderCreatedEvent (100)
    assertEquals(0, OrderCreatedEventEncoder.sequenceNumberEncodingOffset());
    assertEquals(8, OrderCreatedEventEncoder.timestampEncodingOffset());

    // OrderRejectedEvent (101)
    assertEquals(0, OrderRejectedEventEncoder.sequenceNumberEncodingOffset());
    assertEquals(8, OrderRejectedEventEncoder.timestampEncodingOffset());

    // OrderFilledEvent (102)
    assertEquals(0, OrderFilledEventEncoder.sequenceNumberEncodingOffset());
    assertEquals(8, OrderFilledEventEncoder.timestampEncodingOffset());

    // OrderCanceledEvent (103)
    assertEquals(0, OrderCanceledEventEncoder.sequenceNumberEncodingOffset());
    assertEquals(8, OrderCanceledEventEncoder.timestampEncodingOffset());

    // QuoteRequestedEvent (104)
    assertEquals(0, QuoteRequestedEventEncoder.sequenceNumberEncodingOffset());
    assertEquals(8, QuoteRequestedEventEncoder.timestampEncodingOffset());

    // QuoteCreatedEvent (105)
    assertEquals(0, QuoteCreatedEventEncoder.sequenceNumberEncodingOffset());
    assertEquals(8, QuoteCreatedEventEncoder.timestampEncodingOffset());

    // QuoteRejectedEvent (106)
    assertEquals(0, QuoteRejectedEventEncoder.sequenceNumberEncodingOffset());
    assertEquals(8, QuoteRejectedEventEncoder.timestampEncodingOffset());

    // QuoteExpiredEvent (107)
    assertEquals(0, QuoteExpiredEventEncoder.sequenceNumberEncodingOffset());
    assertEquals(8, QuoteExpiredEventEncoder.timestampEncodingOffset());

    // PriceRequestedEvent (108)
    assertEquals(0, PriceRequestedEventEncoder.sequenceNumberEncodingOffset());
    assertEquals(8, PriceRequestedEventEncoder.timestampEncodingOffset());

    // PriceReceivedEvent (109)
    assertEquals(0, PriceReceivedEventEncoder.sequenceNumberEncodingOffset());
    assertEquals(8, PriceReceivedEventEncoder.timestampEncodingOffset());

    // AccountLoadedEvent (110)
    assertEquals(0, AccountLoadedEventEncoder.sequenceNumberEncodingOffset());
    assertEquals(8, AccountLoadedEventEncoder.timestampEncodingOffset());

    // AccountLoadRejectedEvent (111)
    assertEquals(0, AccountLoadRejectedEventEncoder.sequenceNumberEncodingOffset());
    assertEquals(8, AccountLoadRejectedEventEncoder.timestampEncodingOffset());

    // OrderCancelRejectedEvent (112)
    assertEquals(0, OrderCancelRejectedEventEncoder.sequenceNumberEncodingOffset());
    assertEquals(8, OrderCancelRejectedEventEncoder.timestampEncodingOffset());

    // CurrencyLoadedEvent (113)
    assertEquals(0, CurrencyLoadedEventEncoder.sequenceNumberEncodingOffset());
    assertEquals(8, CurrencyLoadedEventEncoder.timestampEncodingOffset());

    // CurrencyLoadRejectedEvent (114)
    assertEquals(0, CurrencyLoadRejectedEventEncoder.sequenceNumberEncodingOffset());
    assertEquals(8, CurrencyLoadRejectedEventEncoder.timestampEncodingOffset());

    // RiskLimitLoadedEvent (115)
    assertEquals(0, RiskLimitLoadedEventEncoder.sequenceNumberEncodingOffset());
    assertEquals(8, RiskLimitLoadedEventEncoder.timestampEncodingOffset());

    // RiskLimitLoadRejectedEvent (116)
    assertEquals(0, RiskLimitLoadRejectedEventEncoder.sequenceNumberEncodingOffset());
    assertEquals(8, RiskLimitLoadRejectedEventEncoder.timestampEncodingOffset());
  }
}
