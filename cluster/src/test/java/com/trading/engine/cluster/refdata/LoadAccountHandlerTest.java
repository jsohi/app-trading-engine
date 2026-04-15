package com.trading.engine.cluster.refdata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.AccountLoadRejectedEventDecoder;
import com.trading.engine.messages.sbe.AccountLoadedEventDecoder;
import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.AccountTypeEnum;
import com.trading.engine.messages.sbe.CurrencyClassEnum;
import com.trading.engine.messages.sbe.LoadAccountEncoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.RejectReasonEnum;
import com.trading.engine.testsupport.sbe.SbeTestEncoder;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.Test;

class LoadAccountHandlerTest {

  private static final long SEQ_NO = 7L;
  private static final long TS = 1_700_000_000_000_000_000L;

  /** Encode a LoadAccount SBE message into {@code dst}. */
  private static int encodeLoadAccount(
      final MutableDirectBuffer dst,
      final long accountId,
      final String code,
      final String name,
      final String baseCcy) {
    return SbeTestEncoder.encodeLoadAccount(dst, 0, accountId, code, name, baseCcy);
  }

  /** Pre-load USD into a CurrencyStore so the FK check passes. */
  private static void seedCurrencyStore(final CurrencyStore store, final String code) {
    final MutableDirectBuffer src = new ExpandableArrayBuffer(256);
    final MutableDirectBuffer eventDst = new ExpandableArrayBuffer(256);
    final int len =
        SbeTestEncoder.encodeLoadCurrency(
            src, 0, code, 840, "test", 2, CurrencyClassEnum.Fiat, AccountStatusEnum.Active);

    final LoadCurrencyHandler h = new LoadCurrencyHandler(store);
    final MessageHeaderDecoder dec = new MessageHeaderDecoder();
    dec.wrap(src, 0);
    h.onCommand(dec, src, 0, len, eventDst, 0, 1L, 0L);
  }

  private static int dispatch(
      final LoadAccountHandler handler,
      final MutableDirectBuffer src,
      final int srcLength,
      final MutableDirectBuffer eventDst) {
    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    header.wrap(src, 0);
    return handler.onCommand(header, src, 0, srcLength, eventDst, 0, SEQ_NO, TS);
  }

  // ---------------------------------------------------------------------------
  // Happy path
  // ---------------------------------------------------------------------------

  @Test
  void validLoadEmitsLoadedEventAndUpserts() {
    final AccountStore accountStore = new AccountStore();
    final CurrencyStore currencyStore = new CurrencyStore();
    seedCurrencyStore(currencyStore, "USD");
    final LoadAccountHandler handler = new LoadAccountHandler(accountStore, currencyStore);

    final MutableDirectBuffer src = new ExpandableArrayBuffer(256);
    final int srcLength = encodeLoadAccount(src, 1L, "ACME", "Acme Inc", "USD");
    final MutableDirectBuffer eventDst = new ExpandableArrayBuffer(256);
    final int eventLength = dispatch(handler, src, srcLength, eventDst);
    assertTrue(eventLength > 0);

    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    header.wrap(eventDst, 0);
    assertEquals(AccountLoadedEventDecoder.TEMPLATE_ID, header.templateId());

    final AccountLoadedEventDecoder dec = new AccountLoadedEventDecoder();
    dec.wrap(eventDst, MessageHeaderDecoder.ENCODED_LENGTH, header.blockLength(), header.version());
    assertEquals(SEQ_NO, dec.sequenceNumber());
    assertEquals(1L, dec.accountId());
    assertEquals(AccountTypeEnum.Client, dec.accountType());

    assertEquals(1, accountStore.size());
    assertNotNull(accountStore.get(1L));
  }

  // ---------------------------------------------------------------------------
  // Rejections
  // ---------------------------------------------------------------------------

  @Test
  void zeroAccountIdRejected() {
    final AccountStore accountStore = new AccountStore();
    final LoadAccountHandler handler = new LoadAccountHandler(accountStore);

    final MutableDirectBuffer src = new ExpandableArrayBuffer(256);
    final int srcLength = encodeLoadAccount(src, 0L, "ACME", "Acme", "USD");
    final MutableDirectBuffer eventDst = new ExpandableArrayBuffer(256);
    dispatch(handler, src, srcLength, eventDst);

    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    header.wrap(eventDst, 0);
    assertEquals(AccountLoadRejectedEventDecoder.TEMPLATE_ID, header.templateId());

    final AccountLoadRejectedEventDecoder dec = new AccountLoadRejectedEventDecoder();
    dec.wrap(eventDst, MessageHeaderDecoder.ENCODED_LENGTH, header.blockLength(), header.version());
    assertEquals(RejectReasonEnum.InvalidAccountId, dec.rejectReason());
    assertEquals(0, accountStore.size());
  }

  @Test
  void unknownBaseCurrencyRejectedWhenCurrencyStoreWired() {
    final AccountStore accountStore = new AccountStore();
    final CurrencyStore currencyStore = new CurrencyStore(); // empty
    final LoadAccountHandler handler = new LoadAccountHandler(accountStore, currencyStore);

    final MutableDirectBuffer src = new ExpandableArrayBuffer(256);
    final int srcLength = encodeLoadAccount(src, 1L, "ACME", "Acme", "USD");
    final MutableDirectBuffer eventDst = new ExpandableArrayBuffer(256);
    dispatch(handler, src, srcLength, eventDst);

    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    header.wrap(eventDst, 0);
    assertEquals(AccountLoadRejectedEventDecoder.TEMPLATE_ID, header.templateId());

    final AccountLoadRejectedEventDecoder dec = new AccountLoadRejectedEventDecoder();
    dec.wrap(eventDst, MessageHeaderDecoder.ENCODED_LENGTH, header.blockLength(), header.version());
    assertEquals(RejectReasonEnum.UnknownCurrency, dec.rejectReason());
    assertEquals(0, accountStore.size());
  }

  @Test
  void unknownBaseCurrencyAcceptedWhenNoCurrencyStore() {
    final AccountStore accountStore = new AccountStore();
    final LoadAccountHandler handler = new LoadAccountHandler(accountStore); // no currency store

    final MutableDirectBuffer src = new ExpandableArrayBuffer(256);
    final int srcLength = encodeLoadAccount(src, 1L, "ACME", "Acme", "USD");
    final MutableDirectBuffer eventDst = new ExpandableArrayBuffer(256);
    dispatch(handler, src, srcLength, eventDst);

    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    header.wrap(eventDst, 0);
    assertEquals(AccountLoadedEventDecoder.TEMPLATE_ID, header.templateId());
    assertEquals(1, accountStore.size());
  }

  @Test
  void duplicateAccountCodeRejected() {
    final AccountStore accountStore = new AccountStore();
    final LoadAccountHandler handler = new LoadAccountHandler(accountStore);

    final MutableDirectBuffer src = new ExpandableArrayBuffer(256);
    final MutableDirectBuffer eventDst = new ExpandableArrayBuffer(256);

    // First load: id=1, code=ACME — succeeds.
    int srcLength = encodeLoadAccount(src, 1L, "ACME", "Acme v1", "USD");
    dispatch(handler, src, srcLength, eventDst);

    // Second load: DIFFERENT id=2 with SAME code=ACME — should be rejected.
    srcLength = encodeLoadAccount(src, 2L, "ACME", "Acme v2", "USD");
    dispatch(handler, src, srcLength, eventDst);

    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    header.wrap(eventDst, 0);
    assertEquals(AccountLoadRejectedEventDecoder.TEMPLATE_ID, header.templateId());

    final AccountLoadRejectedEventDecoder dec = new AccountLoadRejectedEventDecoder();
    dec.wrap(eventDst, MessageHeaderDecoder.ENCODED_LENGTH, header.blockLength(), header.version());
    assertEquals(RejectReasonEnum.DuplicateAccountCode, dec.rejectReason());

    // Store still has only the first account.
    assertEquals(1, accountStore.size());
    assertEquals(1L, accountStore.get(1L).accountId());
  }

  @Test
  void upsertSameAccountIdSameCodeIdempotent() {
    final AccountStore accountStore = new AccountStore();
    final LoadAccountHandler handler = new LoadAccountHandler(accountStore);

    final MutableDirectBuffer src = new ExpandableArrayBuffer(256);
    final MutableDirectBuffer eventDst = new ExpandableArrayBuffer(256);

    int srcLength = encodeLoadAccount(src, 1L, "ACME", "Acme v1", "USD");
    dispatch(handler, src, srcLength, eventDst);
    srcLength = encodeLoadAccount(src, 1L, "ACME", "Acme v2", "USD");
    dispatch(handler, src, srcLength, eventDst);

    assertEquals(1, accountStore.size());
    final byte[] expected = "Acme v2".getBytes();
    final byte[] actual = new byte[expected.length];
    accountStore.get(1L).copyAccountNameTo(actual, 0);
    assertEquals(new String(expected), new String(actual));
  }

  @Test
  void commandTemplateIdMatches() {
    final LoadAccountHandler h = new LoadAccountHandler(new AccountStore());
    assertEquals(LoadAccountEncoder.TEMPLATE_ID, h.commandTemplateId());
  }
}
