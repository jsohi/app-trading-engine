package com.trading.engine.cluster.refdata;

import com.trading.engine.messages.sbe.LoadSymbolEligibilityDecoder;
import com.trading.engine.messages.sbe.LoadSymbolEligibilityEncoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.SymbolEligibilityLoadedEventEncoder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * {@link ReferenceDataLoader} for {@link LoadSymbolEligibilityDecoder LoadSymbolEligibility}
 * (templateId 19, APP-62 §G). Decodes one symbol-eligibility record, derives the {@code symbolHash}
 * (packed-{@code long} form of the 8-byte FIX tag 55 Symbol field), upserts the {@link
 * SymbolEligibilityStore}, and emits a {@code SymbolEligibilityLoadedEvent} (template 120) for the
 * read-side projection / debug consumers.
 *
 * <p><b>No 4-eyes.</b> Unlike {@link LoadRiskLimitHandler}, the §G symbol-eligibility command does
 * not carry proposerId / approverId — the policy attaches at the firm-level loader (the ops
 * playbook signs off on the start-of-day eligibility YAML); symbol-level edits flow through the
 * same approval gate at the loader, not at the SBE wire boundary. This handler therefore mirrors
 * the simpler {@link LoadCurrencyHandler} structure.
 *
 * <p><b>Threading.</b> Not thread-safe — single-threaded cluster duty cycle only. The {@code
 * symbolScratch} byte array is a per-instance mutable buffer reused on every {@link #onCommand}
 * invocation.
 *
 * <p><b>Allocation.</b> Zero-allocation on the decode-and-emit hot path. The single exception is
 * the first-load {@code new SymbolEligibilityState()} when the symbol has no prior record; this is
 * the reference-data ingress path (not the order-matching hot path) and is acceptable per the
 * CLAUDE.md reference-data carve-out. Re-loads of an existing symbol mutate the existing state in
 * place and allocate nothing.
 */
public final class LoadSymbolEligibilityHandler implements ReferenceDataLoader {

  private final SymbolEligibilityStore store;

  // Pre-allocated SBE flyweights — reused across every dispatch.
  private final LoadSymbolEligibilityDecoder decoder = new LoadSymbolEligibilityDecoder();
  private final SymbolEligibilityLoadedEventEncoder loadedEncoder =
      new SymbolEligibilityLoadedEventEncoder();
  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();

  // Scratch buffer for copying the 8-byte SBE Symbol field out of the decoder without allocation.
  private final byte[] symbolScratch = new byte[SymbolEligibilityState.SYMBOL_LENGTH];

  /**
   * Creates a handler wired to the given store.
   *
   * @param store the symbol-eligibility store to upsert into (must not be null)
   * @throws NullPointerException if {@code store} is null
   */
  public LoadSymbolEligibilityHandler(final SymbolEligibilityStore store) {
    if (store == null) {
      throw new NullPointerException("store must not be null");
    }
    this.store = store;
  }

  @Override
  public int commandTemplateId() {
    return LoadSymbolEligibilityEncoder.TEMPLATE_ID;
  }

  /**
   * Decodes one {@code LoadSymbolEligibility} command, upserts the store, and emits a {@code
   * SymbolEligibilityLoadedEvent}.
   *
   * @param header pre-wrapped SBE message header positioned over the command
   * @param src buffer holding the command (header + body) at {@code srcOffset}
   * @param srcOffset start offset of the SBE message header
   * @param srcLength total bytes of the command (header + body); unused here, retained for
   *     interface symmetry
   * @param eventDst destination buffer for the emitted {@code SymbolEligibilityLoadedEvent}
   * @param eventDstOffset start offset where the event message will be written
   * @param sequenceNumber monotonic event sequence number from the cluster's {@code EventSequencer}
   * @param clusterTimestampNanos deterministic cluster timestamp (epoch nanos) — used both for the
   *     event {@code timestamp} field and for stamping the upserted state's {@code asOfTimestamp}
   * @return total encoded length of the loaded event (SBE header + body)
   */
  @Override
  public int onCommand(
      final MessageHeaderDecoder header,
      final DirectBuffer src,
      int srcOffset,
      int srcLength,
      final MutableDirectBuffer eventDst,
      int eventDstOffset,
      long sequenceNumber,
      long clusterTimestampNanos) {
    decoder.wrap(
        src,
        srcOffset + MessageHeaderDecoder.ENCODED_LENGTH,
        header.blockLength(),
        header.version());

    // Copy the 8-byte FIX tag 55 (Symbol) field into the scratch buffer, then pack it. The pack
    // must match NewOrderSingleHandler#packSymbolKey byte-for-byte so order admission and this
    // store agree on the key.
    decoder.getSymbol(symbolScratch, 0);
    long symbolHash = SymbolEligibilityState.packSymbolKey(symbolScratch, 0);

    boolean tradingAllowed = decoder.tradingAllowed() != 0;
    boolean shortSaleAllowed = decoder.shortSaleAllowed() != 0;
    // SBE uint32 surfaces as long (sign-extended via the schema codegen); widening is safe.
    long priceDeviationBpsOverride = decoder.priceDeviationBpsOverride();
    long transactTime = decoder.transactTime();

    // Upsert. Re-load of an existing symbol mutates in place (no allocation); first-load
    // allocates one state on the reference-data ingress path (acceptable per CLAUDE.md carve-out).
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

    // Emit SymbolEligibilityLoadedEvent (templateId 120). symbolScratch is still the pristine
    // 8-byte symbol read above; passing it straight to the encoder avoids a second copy.
    loadedEncoder.wrapAndApplyHeader(eventDst, eventDstOffset, headerEncoder);
    loadedEncoder.sequenceNumber(sequenceNumber);
    loadedEncoder.timestamp(clusterTimestampNanos);
    loadedEncoder.putSymbol(symbolScratch, 0);
    loadedEncoder.tradingAllowed((short) (tradingAllowed ? 1 : 0));
    loadedEncoder.shortSaleAllowed((short) (shortSaleAllowed ? 1 : 0));
    loadedEncoder.priceDeviationBpsOverride(priceDeviationBpsOverride);
    loadedEncoder.transactTime(transactTime);

    return MessageHeaderEncoder.ENCODED_LENGTH + loadedEncoder.encodedLength();
  }
}
