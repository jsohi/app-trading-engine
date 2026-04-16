package com.trading.engine.orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.trading.engine.messages.sbe.QuoteRequestRejectDecoder;
import org.junit.jupiter.api.Test;

/**
 * Compile-time guards that the SBE-generated {@link QuoteRequestRejectDecoder} preserves the
 * FIX-standard tag IDs declared in {@code messages/src/main/resources/trading-schema.xml}. SBE code
 * generation can drift if the schema is inadvertently modified (e.g., a stray edit to an {@code
 * id="..."} attribute); these one-line assertions catch the most likely accidental changes
 * affecting the QuoteRequestReject (35=AG) message.
 *
 * <p>Excludes the project's custom {@code productType} field (id 10013, non-FIX-standard) — only
 * the six FIX 4.4-mandated tags are asserted here.
 */
class QuoteRequestRejectSchemaTest {

  @Test
  void quoteReqId_fieldId_is131() {
    assertEquals(131, QuoteRequestRejectDecoder.quoteReqIdId());
  }

  @Test
  void quoteRejectReason_fieldId_is658() {
    assertEquals(658, QuoteRequestRejectDecoder.quoteRejectReasonId());
  }

  @Test
  void symbol_fieldId_is55() {
    assertEquals(55, QuoteRequestRejectDecoder.symbolId());
  }

  @Test
  void side_fieldId_is54() {
    assertEquals(54, QuoteRequestRejectDecoder.sideId());
  }

  @Test
  void transactTime_fieldId_is60() {
    assertEquals(60, QuoteRequestRejectDecoder.transactTimeId());
  }

  @Test
  void text_fieldId_is58() {
    assertEquals(58, QuoteRequestRejectDecoder.textId());
  }
}
