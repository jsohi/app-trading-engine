package com.trading.engine.cluster.refdata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.CurrencyClassEnum;
import com.trading.engine.messages.sbe.CurrencyLoadRejectedEventDecoder;
import com.trading.engine.messages.sbe.CurrencyLoadedEventDecoder;
import com.trading.engine.messages.sbe.LoadCurrencyEncoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.RejectReasonEnum;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.Test;

class LoadCurrencyHandlerTest {

  private static final long SEQ_NO = 42L;
  private static final long TIMESTAMP_NS = 1_700_000_000_000_000_000L;

  /** Build a complete LoadCurrency SBE message into a fresh buffer. Returns total bytes written. */
  private static int encodeLoadCurrency(
      final MutableDirectBuffer dst,
      final String code,
      final int isoNumeric,
      final String name,
      final int decimals,
      final CurrencyClassEnum cls,
      final AccountStatusEnum status) {
    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    final LoadCurrencyEncoder encoder = new LoadCurrencyEncoder();
    encoder.wrapAndApplyHeader(dst, 0, header);
    encoder.ccyCode(code);
    encoder.isoNumeric(isoNumeric);
    encoder.name(name);
    encoder.decimals((short) decimals);
    encoder.currencyClass(cls);
    encoder.status(status);
    encoder.transactTime(0L);
    return MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();
  }

  private static int dispatch(
      final LoadCurrencyHandler handler,
      final MutableDirectBuffer src,
      final int srcLength,
      final MutableDirectBuffer eventDst) {
    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    header.wrap(src, 0);
    return handler.onCommand(header, src, 0, srcLength, eventDst, 0, SEQ_NO, TIMESTAMP_NS);
  }

  @Test
  void validLoadEmitsLoadedEvent() {
    final CurrencyStore store = new CurrencyStore();
    final LoadCurrencyHandler handler = new LoadCurrencyHandler(store);

    final MutableDirectBuffer src = new ExpandableArrayBuffer(256);
    final int srcLength =
        encodeLoadCurrency(
            src,
            "USD",
            840,
            "United States Dollar",
            2,
            CurrencyClassEnum.Fiat,
            AccountStatusEnum.Active);

    final MutableDirectBuffer eventDst = new ExpandableArrayBuffer(256);
    final int eventLength = dispatch(handler, src, srcLength, eventDst);
    assertTrue(eventLength > 0);

    // Decode the emitted event.
    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    header.wrap(eventDst, 0);
    assertEquals(CurrencyLoadedEventDecoder.TEMPLATE_ID, header.templateId());

    final CurrencyLoadedEventDecoder decoder = new CurrencyLoadedEventDecoder();
    decoder.wrap(
        eventDst, MessageHeaderDecoder.ENCODED_LENGTH, header.blockLength(), header.version());
    assertEquals(SEQ_NO, decoder.sequenceNumber());
    assertEquals(TIMESTAMP_NS, decoder.timestamp());
    assertEquals('U', decoder.ccyCode(0));
    assertEquals('S', decoder.ccyCode(1));
    assertEquals('D', decoder.ccyCode(2));
    assertEquals(840, decoder.isoNumeric());
    assertEquals(2, decoder.decimals());
    assertEquals(CurrencyClassEnum.Fiat, decoder.currencyClass());

    // Store now contains the currency.
    assertEquals(1, store.size());
    assertNotNull(store.get(CurrencyStore.packCode((byte) 'U', (byte) 'S', (byte) 'D')));
  }

  @Test
  void invalidCcyCodeRejected() {
    final CurrencyStore store = new CurrencyStore();
    final LoadCurrencyHandler handler = new LoadCurrencyHandler(store);

    final MutableDirectBuffer src = new ExpandableArrayBuffer(256);
    final int srcLength =
        encodeLoadCurrency(
            src, "us1", 840, "Test", 2, CurrencyClassEnum.Fiat, AccountStatusEnum.Active);

    final MutableDirectBuffer eventDst = new ExpandableArrayBuffer(256);
    final int eventLength = dispatch(handler, src, srcLength, eventDst);
    assertTrue(eventLength > 0);

    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    header.wrap(eventDst, 0);
    assertEquals(CurrencyLoadRejectedEventDecoder.TEMPLATE_ID, header.templateId());

    final CurrencyLoadRejectedEventDecoder decoder = new CurrencyLoadRejectedEventDecoder();
    decoder.wrap(
        eventDst, MessageHeaderDecoder.ENCODED_LENGTH, header.blockLength(), header.version());
    assertEquals(RejectReasonEnum.InvalidCurrencyCode, decoder.rejectReason());

    // Store unchanged.
    assertEquals(0, store.size());
  }

  @Test
  void invalidIsoNumericRejected() {
    final CurrencyStore store = new CurrencyStore();
    final LoadCurrencyHandler handler = new LoadCurrencyHandler(store);

    final MutableDirectBuffer src = new ExpandableArrayBuffer(256);
    final int srcLength =
        encodeLoadCurrency(
            src, "USD", 0, "Test", 2, CurrencyClassEnum.Fiat, AccountStatusEnum.Active);

    final MutableDirectBuffer eventDst = new ExpandableArrayBuffer(256);
    dispatch(handler, src, srcLength, eventDst);

    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    header.wrap(eventDst, 0);
    assertEquals(CurrencyLoadRejectedEventDecoder.TEMPLATE_ID, header.templateId());

    final CurrencyLoadRejectedEventDecoder decoder = new CurrencyLoadRejectedEventDecoder();
    decoder.wrap(
        eventDst, MessageHeaderDecoder.ENCODED_LENGTH, header.blockLength(), header.version());
    assertEquals(RejectReasonEnum.InvalidLimitValue, decoder.rejectReason());
    assertEquals(0, store.size());
  }

  @Test
  void invalidDecimalsRejected() {
    final CurrencyStore store = new CurrencyStore();
    final LoadCurrencyHandler handler = new LoadCurrencyHandler(store);

    final MutableDirectBuffer src = new ExpandableArrayBuffer(256);
    final int srcLength =
        encodeLoadCurrency(
            src, "USD", 840, "Test", 19, CurrencyClassEnum.Fiat, AccountStatusEnum.Active);

    final MutableDirectBuffer eventDst = new ExpandableArrayBuffer(256);
    dispatch(handler, src, srcLength, eventDst);

    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    header.wrap(eventDst, 0);
    assertEquals(CurrencyLoadRejectedEventDecoder.TEMPLATE_ID, header.templateId());
    assertEquals(0, store.size());
  }

  @Test
  void upsertIdempotent() {
    final CurrencyStore store = new CurrencyStore();
    final LoadCurrencyHandler handler = new LoadCurrencyHandler(store);

    final MutableDirectBuffer src = new ExpandableArrayBuffer(256);
    final MutableDirectBuffer eventDst = new ExpandableArrayBuffer(256);

    int srcLength =
        encodeLoadCurrency(
            src, "USD", 840, "First", 2, CurrencyClassEnum.Fiat, AccountStatusEnum.Active);
    dispatch(handler, src, srcLength, eventDst);

    srcLength =
        encodeLoadCurrency(
            src, "USD", 840, "Second", 4, CurrencyClassEnum.Fiat, AccountStatusEnum.Active);
    dispatch(handler, src, srcLength, eventDst);

    assertEquals(1, store.size());
    assertEquals(
        4, store.get(CurrencyStore.packCode((byte) 'U', (byte) 'S', (byte) 'D')).decimals());
  }

  @Test
  void commandTemplateIdMatches() {
    final LoadCurrencyHandler handler = new LoadCurrencyHandler(new CurrencyStore());
    assertEquals(LoadCurrencyEncoder.TEMPLATE_ID, handler.commandTemplateId());
  }
}
