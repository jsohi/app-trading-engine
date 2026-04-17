package com.trading.refdata.currency;

import static org.junit.jupiter.api.Assertions.*;

import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.CurrencyClassEnum;
import com.trading.engine.messages.sbe.LoadCurrencyBatchDecoder;
import com.trading.engine.messages.sbe.LoadCurrencyBatchDecoder.NoCurrenciesDecoder;
import com.trading.engine.messages.sbe.LoadCurrencyBatchEncoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.refdata.ReferenceDataLoadException;
import java.util.List;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.Test;

/** Tests SBE encode/decode round-trip and error handling for {@link CurrencyCommandEncoder}. */
final class CurrencyCommandEncoderTest {

  private final CurrencyCommandEncoder encoder = new CurrencyCommandEncoder();
  private final MutableDirectBuffer buffer = new ExpandableArrayBuffer(4096);
  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final LoadCurrencyBatchDecoder batchDecoder = new LoadCurrencyBatchDecoder();

  @Test
  void encodeBatchRoundTrip() throws Exception {
    final var records =
        List.of(
            new CurrencyRecord("USD", 840, "US Dollar", 2, "Fiat", "Active"),
            new CurrencyRecord("XAU", 959, "Gold", 6, "Metal", "Active"));

    final int length = encoder.encodeBatch(records, 0, 2, buffer, 0);
    assertTrue(length > 0);

    headerDecoder.wrap(buffer, 0);
    assertEquals(LoadCurrencyBatchEncoder.TEMPLATE_ID, headerDecoder.templateId());

    batchDecoder.wrapAndApplyHeader(buffer, 0, headerDecoder);
    assertEquals(0L, batchDecoder.transactTime());

    final NoCurrenciesDecoder group = batchDecoder.noCurrencies();
    assertEquals(2, group.count());

    // First record — USD
    group.next();
    assertEquals("USD", group.ccyCode().trim());
    assertEquals(840, group.isoNumeric());
    assertEquals("US Dollar", group.name().trim());
    assertEquals(2, group.decimals());
    assertEquals(CurrencyClassEnum.Fiat, group.currencyClass());
    assertEquals(AccountStatusEnum.Active, group.status());

    // Second record — XAU
    group.next();
    assertEquals("XAU", group.ccyCode().trim());
    assertEquals(959, group.isoNumeric());
    assertEquals("Gold", group.name().trim());
    assertEquals(6, group.decimals());
    assertEquals(CurrencyClassEnum.Metal, group.currencyClass());
    assertEquals(AccountStatusEnum.Active, group.status());
  }

  @Test
  void encodeBatchSubRange() throws Exception {
    final var records =
        List.of(
            new CurrencyRecord("USD", 840, "US Dollar", 2, "Fiat", "Active"),
            new CurrencyRecord("EUR", 978, "Euro", 2, "Fiat", "Suspended"),
            new CurrencyRecord("BTC", 999, "Bitcoin", 8, "Crypto", "Active"));
    final int length = encoder.encodeBatch(records, 1, 3, buffer, 0);
    assertTrue(length > 0);

    headerDecoder.wrap(buffer, 0);
    batchDecoder.wrapAndApplyHeader(buffer, 0, headerDecoder);

    final var group = batchDecoder.noCurrencies();
    assertEquals(2, group.count());

    group.next();
    assertEquals("EUR", group.ccyCode().trim());
    assertEquals(978, group.isoNumeric());
    assertEquals(AccountStatusEnum.Suspended, group.status());

    group.next();
    assertEquals("BTC", group.ccyCode().trim());
    assertEquals(CurrencyClassEnum.Crypto, group.currencyClass());
  }

  @Test
  void encodeInvalidCurrencyClassThrows() {
    final var records =
        List.of(new CurrencyRecord("BAD", 100, "Bad Class", 2, "InvalidClass", "Active"));

    final var ex =
        assertThrows(
            ReferenceDataLoadException.class, () -> encoder.encodeBatch(records, 0, 1, buffer, 0));
    assertTrue(ex.getMessage().contains("currencyClass"));
  }

  @Test
  void encodeInvalidStatusThrows() {
    final var records =
        List.of(new CurrencyRecord("BAD", 100, "Bad Status", 2, "Fiat", "BadStatus"));

    final var ex =
        assertThrows(
            ReferenceDataLoadException.class, () -> encoder.encodeBatch(records, 0, 1, buffer, 0));
    assertTrue(ex.getMessage().contains("status"));
  }

  @Test
  void templateIdMatchesSbeConstant() {
    assertEquals(LoadCurrencyBatchEncoder.TEMPLATE_ID, encoder.templateId());
  }

  @Test
  void maxBatchSizeIsPositive() {
    assertTrue(encoder.maxBatchSize() > 0);
  }

  @Test
  void encodedLengthMatchesReturnedValue() throws Exception {
    final var records =
        List.of(new CurrencyRecord("GBP", 826, "British Pound", 2, "Fiat", "Active"));

    final int length = encoder.encodeBatch(records, 0, 1, buffer, 0);

    // Verify: header + block + group header + (1 * group block)
    final int expected =
        MessageHeaderEncoder.ENCODED_LENGTH
            + LoadCurrencyBatchEncoder.BLOCK_LENGTH
            + 4 // group header (blockLength u16 + numInGroup u16)
            + LoadCurrencyBatchEncoder.NoCurrenciesEncoder.sbeBlockLength();
    assertEquals(expected, length);
  }
}
