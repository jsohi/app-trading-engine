package com.trading.engine.cluster.refdata;

import com.trading.engine.messages.sbe.LoadSymbolEligibilityBatchDecoder;
import com.trading.engine.messages.sbe.LoadSymbolEligibilityBatchEncoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.SymbolEligibilityLoadedEventEncoder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * {@link ReferenceDataBatchLoader} for {@link LoadSymbolEligibilityBatchDecoder
 * LoadSymbolEligibilityBatch} (templateId 20, APP-62 §G). Iterates the {@code noEligibilities}
 * repeating group, upserts each record into {@link SymbolEligibilityStore}, and emits one {@code
 * SymbolEligibilityLoadedEvent} (template 120) per record. Mirrors {@link
 * LoadSymbolEligibilityHandler} per-record semantics but reads the {@code transactTime} once from
 * the batch envelope (the schema places transactTime on the parent message, not the group entries —
 * a single batch is one operational load with one effective time).
 *
 * <p><b>Per-record events vs. summary event.</b> Per the {@link ReferenceDataBatchLoader} contract,
 * we emit one event per record. Projection consumers therefore see exactly one event per upserted
 * symbol regardless of whether the source command was single or batched — replay symmetry with
 * {@link LoadSymbolEligibilityHandler}.
 *
 * <p><b>Atomicity.</b> Not all-or-nothing — each record is upserted independently. Phase-1 §G does
 * not validate group records (no negative-value or empty-symbol gates) because the upstream YAML
 * loader has already done the structural validation; if a future PR introduces validation, it can
 * be added here with a {@code SymbolEligibilityLoadRejectedEvent} (not yet defined in the schema).
 *
 * <p><b>Threading.</b> Not thread-safe — single-threaded cluster duty cycle only. The {@code
 * symbolScratch} byte array is a per-instance mutable buffer reused on every iteration.
 *
 * <p><b>Allocation.</b> Zero-allocation on the upsert-and-emit hot path. The single exception is
 * the first-load {@code new SymbolEligibilityState()} for symbols with no prior record — this is
 * the reference-data ingress path and is acceptable per the CLAUDE.md carve-out.
 */
public final class LoadSymbolEligibilityBatchHandler implements ReferenceDataBatchLoader {

  private final SymbolEligibilityStore store;

  // Pre-allocated SBE flyweights — reused across every dispatch.
  private final LoadSymbolEligibilityBatchDecoder decoder = new LoadSymbolEligibilityBatchDecoder();
  private final SymbolEligibilityLoadedEventEncoder loadedEncoder =
      new SymbolEligibilityLoadedEventEncoder();
  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();

  // Scratch buffer for copying the 8-byte SBE Symbol field out of each group entry.
  private final byte[] symbolScratch = new byte[SymbolEligibilityState.SYMBOL_LENGTH];

  /**
   * Creates a batch handler wired to the given store.
   *
   * @param store the symbol-eligibility store to upsert into (must not be null)
   * @throws NullPointerException if {@code store} is null
   */
  public LoadSymbolEligibilityBatchHandler(final SymbolEligibilityStore store) {
    if (store == null) {
      throw new NullPointerException("store must not be null");
    }
    this.store = store;
  }

  @Override
  public int batchCommandTemplateId() {
    return LoadSymbolEligibilityBatchEncoder.TEMPLATE_ID;
  }

  /**
   * Iterates the {@code noEligibilities} group and processes each entry. One {@code
   * SymbolEligibilityLoadedEvent} per entry is written contiguously into {@code eventDst}.
   *
   * @return total bytes of all emitted events
   */
  @Override
  public int onBatchCommand(
      final MessageHeaderDecoder header,
      final DirectBuffer src,
      int srcOffset,
      int srcLength,
      final MutableDirectBuffer eventDst,
      int eventDstOffset,
      long firstSequenceNumber,
      long clusterTimestampNanos) {
    decoder.wrap(
        src,
        srcOffset + MessageHeaderDecoder.ENCODED_LENGTH,
        header.blockLength(),
        header.version());
    long batchTransactTime = decoder.transactTime();

    int written = 0;
    long seqNo = firstSequenceNumber;

    final var group = decoder.noEligibilities();
    while (group.hasNext()) {
      group.next();

      // Read group fields via primitive accessors (no String / byte[] allocation per record).
      group.getSymbol(symbolScratch, 0);
      long symbolHash = SymbolEligibilityState.packSymbolKey(symbolScratch, 0);

      boolean tradingAllowed = group.tradingAllowed() != 0;
      boolean shortSaleAllowed = group.shortSaleAllowed() != 0;
      long priceDeviationBpsOverride = group.priceDeviationBpsOverride();

      var state = store.get(symbolHash);
      if (state == null) {
        state = new SymbolEligibilityState();
      }
      state.setSymbolHash(symbolHash);
      state.setSymbolBytes(symbolScratch, 0, SymbolEligibilityState.SYMBOL_LENGTH);
      state.setTradingAllowed(tradingAllowed);
      state.setShortSaleAllowed(shortSaleAllowed);
      state.setPriceDeviationBpsOverride(priceDeviationBpsOverride);
      state.setAsOfTimestamp(clusterTimestampNanos);
      store.put(state);

      // Emit one SymbolEligibilityLoadedEvent for this record.
      loadedEncoder.wrapAndApplyHeader(eventDst, eventDstOffset + written, headerEncoder);
      loadedEncoder.sequenceNumber(seqNo++);
      loadedEncoder.timestamp(clusterTimestampNanos);
      loadedEncoder.putSymbol(symbolScratch, 0);
      loadedEncoder.tradingAllowed((short) (tradingAllowed ? 1 : 0));
      loadedEncoder.shortSaleAllowed((short) (shortSaleAllowed ? 1 : 0));
      loadedEncoder.priceDeviationBpsOverride(priceDeviationBpsOverride);
      loadedEncoder.transactTime(batchTransactTime);
      written += MessageHeaderEncoder.ENCODED_LENGTH + loadedEncoder.encodedLength();
    }
    return written;
  }
}
