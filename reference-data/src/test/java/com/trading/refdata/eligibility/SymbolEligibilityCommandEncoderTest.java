package com.trading.refdata.eligibility;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.LoadSymbolEligibilityBatchDecoder;
import com.trading.engine.messages.sbe.LoadSymbolEligibilityBatchDecoder.NoEligibilitiesDecoder;
import com.trading.engine.messages.sbe.LoadSymbolEligibilityBatchEncoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.Test;

/**
 * Tests SBE encode/decode round-trip and structural invariants for {@link
 * SymbolEligibilityCommandEncoder}.
 */
final class SymbolEligibilityCommandEncoderTest {

  private final SymbolEligibilityCommandEncoder encoder = new SymbolEligibilityCommandEncoder();
  private final MutableDirectBuffer buffer = new ExpandableArrayBuffer(4096);
  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final LoadSymbolEligibilityBatchDecoder batchDecoder =
      new LoadSymbolEligibilityBatchDecoder();

  @Test
  void encodeBatchRoundTrip() throws Exception {
    final var records =
        List.of(
            new SymbolEligibilityRecord("EURUSD", true, true, 250L, 1_000L),
            new SymbolEligibilityRecord("GBPUSD", false, false, 0L, 1_000L));

    final int length = encoder.encodeBatch(records, 0, 2, buffer, 0);
    assertTrue(length > 0);

    headerDecoder.wrap(buffer, 0);
    assertEquals(LoadSymbolEligibilityBatchEncoder.TEMPLATE_ID, headerDecoder.templateId());

    batchDecoder.wrapAndApplyHeader(buffer, 0, headerDecoder);
    assertEquals(0L, batchDecoder.transactTime());

    final NoEligibilitiesDecoder group = batchDecoder.noEligibilities();
    assertEquals(2, group.count());

    final byte[] symbolBytes = new byte[8];

    group.next();
    group.getSymbol(symbolBytes, 0);
    assertEquals("EURUSD", trimAscii(symbolBytes));
    assertEquals((short) 1, group.tradingAllowed());
    assertEquals((short) 1, group.shortSaleAllowed());
    assertEquals(250L, group.priceDeviationBpsOverride());

    group.next();
    group.getSymbol(symbolBytes, 0);
    assertEquals("GBPUSD", trimAscii(symbolBytes));
    assertEquals((short) 0, group.tradingAllowed());
    assertEquals((short) 0, group.shortSaleAllowed());
    assertEquals(0L, group.priceDeviationBpsOverride());
  }

  @Test
  void encodeBatchSubRange() throws Exception {
    final var records =
        List.of(
            new SymbolEligibilityRecord("EURUSD", true, true, 0L, 0L),
            new SymbolEligibilityRecord("GBPUSD", true, true, 0L, 0L),
            new SymbolEligibilityRecord("USDJPY", false, true, 100L, 0L));

    final int length = encoder.encodeBatch(records, 1, 3, buffer, 0);
    assertTrue(length > 0);

    headerDecoder.wrap(buffer, 0);
    batchDecoder.wrapAndApplyHeader(buffer, 0, headerDecoder);

    final var group = batchDecoder.noEligibilities();
    assertEquals(2, group.count());

    final byte[] symbolBytes = new byte[8];
    group.next();
    group.getSymbol(symbolBytes, 0);
    assertEquals("GBPUSD", trimAscii(symbolBytes));

    group.next();
    group.getSymbol(symbolBytes, 0);
    assertEquals("USDJPY", trimAscii(symbolBytes));
    assertEquals(100L, group.priceDeviationBpsOverride());
    assertEquals((short) 0, group.tradingAllowed());
  }

  /**
   * Verifies the encoder's symbol-scratch buffer is cleared between records — a residual byte from
   * a longer prior symbol must not bleed into the next record's tail. Pairs a 6-char symbol after
   * an 8-char symbol so any residual byte at position 6 / 7 would corrupt the round-trip.
   */
  @Test
  void encodeShortSymbolAfterLongClearsResidualBytes() throws Exception {
    final var records =
        List.of(
            new SymbolEligibilityRecord("XAUUSDXX", true, true, 0L, 0L),
            new SymbolEligibilityRecord("EURUSD", true, true, 0L, 0L));

    encoder.encodeBatch(records, 0, 2, buffer, 0);
    headerDecoder.wrap(buffer, 0);
    batchDecoder.wrapAndApplyHeader(buffer, 0, headerDecoder);

    final var group = batchDecoder.noEligibilities();
    group.next(); // XAUUSDXX
    group.next(); // EURUSD
    final byte[] symbolBytes = new byte[8];
    group.getSymbol(symbolBytes, 0);
    // The byte at position 6 must be zero (padding), not the 'X' from the prior symbol.
    assertEquals(0, symbolBytes[6]);
    assertEquals(0, symbolBytes[7]);
    assertEquals("EURUSD", trimAscii(symbolBytes));
  }

  @Test
  void templateIdMatchesSbeConstant() {
    assertEquals(LoadSymbolEligibilityBatchEncoder.TEMPLATE_ID, encoder.templateId());
  }

  @Test
  void maxBatchSizeIsPositive() {
    assertTrue(encoder.maxBatchSize() > 0);
  }

  @Test
  void entityTypeNonBlank() {
    assertEquals("SymbolEligibility", encoder.entityType());
  }

  @Test
  void encodedLengthMatchesReturnedValue() throws Exception {
    final var records = List.of(new SymbolEligibilityRecord("EURUSD", true, true, 0L, 0L));

    final int length = encoder.encodeBatch(records, 0, 1, buffer, 0);

    // Verify: header + block + group header + (1 * group block).
    final int expected =
        MessageHeaderEncoder.ENCODED_LENGTH
            + LoadSymbolEligibilityBatchEncoder.BLOCK_LENGTH
            + 4 // group header (blockLength u16 + numInGroup u16)
            + LoadSymbolEligibilityBatchEncoder.NoEligibilitiesEncoder.sbeBlockLength();
    assertEquals(expected, length);
  }

  private static String trimAscii(final byte[] bytes) {
    int end = bytes.length;
    while (end > 0 && bytes[end - 1] == 0) {
      end--;
    }
    return new String(bytes, 0, end, StandardCharsets.US_ASCII);
  }
}
