package com.trading.engine.cluster.refdata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.CurrencyClassEnum;
import com.trading.engine.messages.sbe.CurrencyLoadRejectedEventDecoder;
import com.trading.engine.messages.sbe.CurrencyLoadedEventDecoder;
import com.trading.engine.messages.sbe.LoadCurrencyBatchEncoder;
import com.trading.engine.messages.sbe.LoadCurrencyBatchEncoder.NoCurrenciesEncoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.RejectReasonEnum;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.Test;

class LoadCurrencyBatchHandlerTest {

  private static final long FIRST_SEQ = 100L;
  private static final long TS = 1_700_000_000_000_000_000L;

  /**
   * Encode a LoadCurrencyBatch with the given (code, isoNumeric) pairs. All records use decimals=2,
   * FIAT class, Active status.
   */
  private static int encodeBatch(final MutableDirectBuffer dst, final String[][] records) {
    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    final LoadCurrencyBatchEncoder enc = new LoadCurrencyBatchEncoder();
    enc.wrapAndApplyHeader(dst, 0, header);
    enc.transactTime(0L);
    final NoCurrenciesEncoder group = enc.noCurrenciesCount(records.length);
    for (final String[] r : records) {
      group.next();
      group.ccyCode(r[0]);
      group.isoNumeric(Integer.parseInt(r[1]));
      group.name(r[2]);
      group.decimals((short) 2);
      group.currencyClass(CurrencyClassEnum.Fiat);
      group.status(AccountStatusEnum.Active);
    }
    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  private static int dispatch(
      final LoadCurrencyBatchHandler handler,
      final MutableDirectBuffer src,
      final int srcLength,
      final MutableDirectBuffer eventDst) {
    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    header.wrap(src, 0);
    return handler.onBatchCommand(header, src, 0, srcLength, eventDst, 0, FIRST_SEQ, TS);
  }

  @Test
  void allValidEmitsLoadedEventPerRecord() {
    final CurrencyStore store = new CurrencyStore();
    final LoadCurrencyBatchHandler handler = new LoadCurrencyBatchHandler(store);

    final MutableDirectBuffer src = new ExpandableArrayBuffer(1024);
    final int srcLength =
        encodeBatch(
            src,
            new String[][] {
              {"USD", "840", "United States Dollar"},
              {"EUR", "978", "Euro"},
              {"JPY", "392", "Japanese Yen"},
            });

    final MutableDirectBuffer eventDst = new ExpandableArrayBuffer(2048);
    final int totalEventBytes = dispatch(handler, src, srcLength, eventDst);
    assertTrue(totalEventBytes > 0);
    assertEquals(3, store.size());

    // Walk the emitted events; each should be a CurrencyLoadedEvent with sequenceNumber
    // FIRST_SEQ, FIRST_SEQ+1, FIRST_SEQ+2 in order.
    int offset = 0;
    long expectedSeq = FIRST_SEQ;
    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    final CurrencyLoadedEventDecoder loaded = new CurrencyLoadedEventDecoder();
    while (offset < totalEventBytes) {
      header.wrap(eventDst, offset);
      assertEquals(CurrencyLoadedEventDecoder.TEMPLATE_ID, header.templateId());
      loaded.wrap(
          eventDst,
          offset + MessageHeaderDecoder.ENCODED_LENGTH,
          header.blockLength(),
          header.version());
      assertEquals(expectedSeq++, loaded.sequenceNumber());
      offset += MessageHeaderDecoder.ENCODED_LENGTH + loaded.encodedLength();
    }
    assertEquals(totalEventBytes, offset);
    assertEquals(FIRST_SEQ + 3, expectedSeq);
  }

  @Test
  void mixedValidAndInvalidProducesPerRecordEvents() {
    final CurrencyStore store = new CurrencyStore();
    final LoadCurrencyBatchHandler handler = new LoadCurrencyBatchHandler(store);

    final MutableDirectBuffer src = new ExpandableArrayBuffer(1024);
    final int srcLength =
        encodeBatch(
            src,
            new String[][] {
              {"USD", "840", "Dollar"}, // valid
              {"us1", "0", "Bad"}, // invalid code
              {"EUR", "978", "Euro"}, // valid
              {"XYZ", "9999", "BadIso"}, // invalid isoNumeric
            });

    final MutableDirectBuffer eventDst = new ExpandableArrayBuffer(2048);
    final int totalEventBytes = dispatch(handler, src, srcLength, eventDst);
    assertEquals(2, store.size()); // USD, EUR

    // Walk events: expect Loaded, Rejected, Loaded, Rejected with sequenceNumber 100..103.
    final int[] templates = new int[4];
    final long[] seqs = new long[4];
    final RejectReasonEnum[] reasons = new RejectReasonEnum[4];

    int offset = 0;
    int idx = 0;
    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    final CurrencyLoadedEventDecoder loaded = new CurrencyLoadedEventDecoder();
    final CurrencyLoadRejectedEventDecoder rejected = new CurrencyLoadRejectedEventDecoder();
    while (offset < totalEventBytes) {
      header.wrap(eventDst, offset);
      templates[idx] = header.templateId();
      if (header.templateId() == CurrencyLoadedEventDecoder.TEMPLATE_ID) {
        loaded.wrap(
            eventDst,
            offset + MessageHeaderDecoder.ENCODED_LENGTH,
            header.blockLength(),
            header.version());
        seqs[idx] = loaded.sequenceNumber();
        offset += MessageHeaderDecoder.ENCODED_LENGTH + loaded.encodedLength();
      } else {
        rejected.wrap(
            eventDst,
            offset + MessageHeaderDecoder.ENCODED_LENGTH,
            header.blockLength(),
            header.version());
        seqs[idx] = rejected.sequenceNumber();
        reasons[idx] = rejected.rejectReason();
        offset += MessageHeaderDecoder.ENCODED_LENGTH + rejected.encodedLength();
      }
      idx++;
    }
    assertEquals(4, idx);
    assertEquals(CurrencyLoadedEventDecoder.TEMPLATE_ID, templates[0]);
    assertEquals(CurrencyLoadRejectedEventDecoder.TEMPLATE_ID, templates[1]);
    assertEquals(CurrencyLoadedEventDecoder.TEMPLATE_ID, templates[2]);
    assertEquals(CurrencyLoadRejectedEventDecoder.TEMPLATE_ID, templates[3]);

    // Sequence numbers are contiguous starting at FIRST_SEQ.
    assertEquals(FIRST_SEQ, seqs[0]);
    assertEquals(FIRST_SEQ + 1, seqs[1]);
    assertEquals(FIRST_SEQ + 2, seqs[2]);
    assertEquals(FIRST_SEQ + 3, seqs[3]);

    assertEquals(RejectReasonEnum.InvalidCurrencyCode, reasons[1]);
    assertEquals(RejectReasonEnum.InvalidLimitValue, reasons[3]);
  }

  @Test
  void emptyBatchEmitsNoEvents() {
    final CurrencyStore store = new CurrencyStore();
    final LoadCurrencyBatchHandler handler = new LoadCurrencyBatchHandler(store);

    final MutableDirectBuffer src = new ExpandableArrayBuffer(64);
    final int srcLength = encodeBatch(src, new String[0][]);
    final MutableDirectBuffer eventDst = new ExpandableArrayBuffer(64);
    final int totalEventBytes = dispatch(handler, src, srcLength, eventDst);

    assertEquals(0, totalEventBytes);
    assertEquals(0, store.size());
  }

  @Test
  void batchTemplateIdMatches() {
    final LoadCurrencyBatchHandler h = new LoadCurrencyBatchHandler(new CurrencyStore());
    assertEquals(LoadCurrencyBatchEncoder.TEMPLATE_ID, h.batchCommandTemplateId());
  }

  @Test
  void largeBatchSnapshotSurvivesScale() {
    // Load 200 currencies (the realistic upper bound for ISO 4217 + crypto).
    final CurrencyStore store = new CurrencyStore();
    final LoadCurrencyBatchHandler handler = new LoadCurrencyBatchHandler(store);

    final String[][] records = new String[200][];
    for (int i = 0; i < 200; i++) {
      // Generate a synthetic 3-letter code from i: AAA, AAB, ..., AZZ etc.
      final char c0 = (char) ('A' + (i / 676));
      final char c1 = (char) ('A' + ((i / 26) % 26));
      final char c2 = (char) ('A' + (i % 26));
      records[i] = new String[] {"" + c0 + c1 + c2, String.valueOf(i + 1), "Currency " + i};
    }
    final MutableDirectBuffer src = new ExpandableArrayBuffer(64 * 1024);
    final int srcLength = encodeBatch(src, records);
    final MutableDirectBuffer eventDst = new ExpandableArrayBuffer(64 * 1024);
    dispatch(handler, src, srcLength, eventDst);
    assertEquals(200, store.size());

    // Snapshot round-trip.
    final MutableDirectBuffer snapshot = new ExpandableArrayBuffer(64 * 1024);
    final int snapBytes = store.snapshotTo(snapshot, 0);
    final CurrencyStore restored = new CurrencyStore();
    final int read = restored.restoreFrom(snapshot, 0);
    assertEquals(snapBytes, read);
    assertEquals(200, restored.size());

    // Spot-check a record.
    final CurrencyState found =
        restored.get(CurrencyStore.packCode((byte) 'A', (byte) 'A', (byte) 'A'));
    assertNotNull(found);
    assertEquals(1, found.isoNumeric());
  }
}
