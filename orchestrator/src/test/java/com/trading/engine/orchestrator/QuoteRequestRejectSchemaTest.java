package com.trading.engine.orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.trading.engine.messages.sbe.QuoteRequestRejectDecoder;
import java.util.function.IntSupplier;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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

  @ParameterizedTest(name = "{0} fieldId == {1}")
  @MethodSource("fixStandardTags")
  void fieldId_matchesFixTag(
      final String fieldName, final int expectedId, final IntSupplier idAccessor) {
    assertEquals(expectedId, idAccessor.getAsInt());
  }

  static java.util.stream.Stream<Arguments> fixStandardTags() {
    return java.util.stream.Stream.of(
        Arguments.of("quoteReqId", 131, (IntSupplier) QuoteRequestRejectDecoder::quoteReqIdId),
        Arguments.of(
            "quoteRejectReason", 658, (IntSupplier) QuoteRequestRejectDecoder::quoteRejectReasonId),
        Arguments.of("symbol", 55, (IntSupplier) QuoteRequestRejectDecoder::symbolId),
        Arguments.of("side", 54, (IntSupplier) QuoteRequestRejectDecoder::sideId),
        Arguments.of("transactTime", 60, (IntSupplier) QuoteRequestRejectDecoder::transactTimeId),
        Arguments.of("text", 58, (IntSupplier) QuoteRequestRejectDecoder::textId));
  }
}
