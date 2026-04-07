package com.trading.engine.cluster.refdata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.CurrencyClassEnum;
import com.trading.engine.messages.sbe.LoadCurrencyEncoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.Test;

class ReferenceDataRegistryTest {

  @Test
  void registerStoreAndLoader() {
    final ReferenceDataRegistry registry = new ReferenceDataRegistry();
    final CurrencyStore currencyStore = new CurrencyStore();
    registry.registerStore(currencyStore);
    registry.registerLoader(new LoadCurrencyHandler(currencyStore));

    assertEquals(1, registry.storeCount());
    assertEquals(1, registry.loaderCount());
  }

  @Test
  void registerStoreRejectsNull() {
    final ReferenceDataRegistry registry = new ReferenceDataRegistry();
    assertThrows(NullPointerException.class, () -> registry.registerStore(null));
  }

  @Test
  void registerStoreRejectsDuplicateTemplateId() {
    final ReferenceDataRegistry registry = new ReferenceDataRegistry();
    registry.registerStore(new CurrencyStore());
    assertThrows(IllegalArgumentException.class, () -> registry.registerStore(new CurrencyStore()));
  }

  @Test
  void registerLoaderRejectsDuplicateTemplateId() {
    final ReferenceDataRegistry registry = new ReferenceDataRegistry();
    final CurrencyStore store = new CurrencyStore();
    registry.registerLoader(new LoadCurrencyHandler(store));
    assertThrows(
        IllegalArgumentException.class,
        () -> registry.registerLoader(new LoadCurrencyHandler(store)));
  }

  @Test
  void dispatchCommandRoutesByTemplateId() {
    final CurrencyStore store = new CurrencyStore();
    final ReferenceDataRegistry registry = new ReferenceDataRegistry();
    registry.registerStore(store);
    registry.registerLoader(new LoadCurrencyHandler(store));

    final MutableDirectBuffer src = new ExpandableArrayBuffer(256);
    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    final LoadCurrencyEncoder enc = new LoadCurrencyEncoder();
    enc.wrapAndApplyHeader(src, 0, header);
    enc.ccyCode("USD");
    enc.isoNumeric(840);
    enc.name("United States Dollar");
    enc.decimals((short) 2);
    enc.currencyClass(CurrencyClassEnum.Fiat);
    enc.status(AccountStatusEnum.Active);
    enc.transactTime(0L);
    final int srcLength = MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();

    final MutableDirectBuffer eventDst = new ExpandableArrayBuffer(256);
    final MessageHeaderDecoder decoder = new MessageHeaderDecoder();
    decoder.wrap(src, 0);
    final int eventLength =
        registry.dispatchCommand(decoder, src, 0, srcLength, eventDst, 0, 1L, 0L);

    assertTrue(eventLength > 0);
    assertEquals(1, store.size());
  }

  @Test
  void dispatchCommandReturnsNotHandledForUnknownTemplateId() {
    final ReferenceDataRegistry registry = new ReferenceDataRegistry();

    // Build any well-formed SBE message; routing fails because no loader is registered.
    final MutableDirectBuffer src = new ExpandableArrayBuffer(256);
    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    header.wrap(src, 0).blockLength(0).templateId(9999).schemaId(1).version(1);

    final MessageHeaderDecoder decoder = new MessageHeaderDecoder();
    decoder.wrap(src, 0);
    final MutableDirectBuffer eventDst = new ExpandableArrayBuffer(64);
    final int result = registry.dispatchCommand(decoder, src, 0, 8, eventDst, 0, 1L, 0L);
    assertEquals(ReferenceDataRegistry.NOT_HANDLED, result);
  }

  @Test
  void snapshotAllAndRestoreFragmentRoundTrip() {
    // Build a registry with two stores, snapshot all, restore into a fresh registry, verify state.
    final CurrencyStore srcCurrency = new CurrencyStore();
    srcCurrency.put(
        CurrencyStore.packCode((byte) 'U', (byte) 'S', (byte) 'D'), currencyState("USD", 840, 2));

    final AccountStore srcAccount = new AccountStore();
    srcAccount.put(AccountStoreTest.makeState(1L, "ACME", "Acme", "USD"));

    final ReferenceDataRegistry srcReg = new ReferenceDataRegistry();
    srcReg.registerStore(srcCurrency);
    srcReg.registerStore(srcAccount);

    final MutableDirectBuffer buf = new ExpandableArrayBuffer(8192);
    final int written = srcReg.snapshotAll(buf, 0);
    assertTrue(written > 0);

    // Replay into a fresh registry.
    final CurrencyStore dstCurrency = new CurrencyStore();
    final AccountStore dstAccount = new AccountStore();
    final ReferenceDataRegistry dstReg = new ReferenceDataRegistry();
    dstReg.registerStore(dstCurrency);
    dstReg.registerStore(dstAccount);

    // Walk the buffer fragment by fragment, dispatching to restoreFragment.
    int offset = 0;
    while (offset < written) {
      final MessageHeaderDecoder header = new MessageHeaderDecoder();
      header.wrap(buf, offset);
      final int consumed = dstReg.restoreFragment(header, buf, offset);
      assertNotEquals(ReferenceDataRegistry.NOT_HANDLED, consumed);
      offset += consumed;
    }
    assertEquals(written, offset);

    assertEquals(1, dstCurrency.size());
    assertEquals(1, dstAccount.size());
  }

  @Test
  void resetAllClearsEveryStore() {
    final CurrencyStore currency = new CurrencyStore();
    currency.put(
        CurrencyStore.packCode((byte) 'U', (byte) 'S', (byte) 'D'), currencyState("USD", 840, 2));
    final AccountStore account = new AccountStore();
    account.put(AccountStoreTest.makeState(1L, "ACME", "Acme", "USD"));

    final ReferenceDataRegistry registry = new ReferenceDataRegistry();
    registry.registerStore(currency);
    registry.registerStore(account);

    registry.resetAll();
    assertEquals(0, currency.size());
    assertEquals(0, account.size());
  }

  private static CurrencyState currencyState(
      final String code, final int isoNum, final int decimals) {
    final CurrencyState s = new CurrencyState();
    s.setCcyCode(code.getBytes(), 0);
    s.setIsoNumeric(isoNum);
    final byte[] name = ("Name " + code).getBytes();
    s.setName(name, 0, name.length);
    s.setDecimals(decimals);
    s.setCurrencyClass(CurrencyClassEnum.Fiat);
    s.setStatus(AccountStatusEnum.Active);
    return s;
  }
}
