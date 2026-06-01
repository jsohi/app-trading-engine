package com.trading.refdata.eligibility;

import com.trading.engine.messages.sbe.LoadSymbolEligibilityBatchEncoder;
import com.trading.engine.messages.sbe.LoadSymbolEligibilityBatchEncoder.NoEligibilitiesEncoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.refdata.ReferenceDataLoadException;
import com.trading.refdata.spi.ReferenceDataEncoder;
import java.util.List;
import org.agrona.MutableDirectBuffer;

/**
 * Encodes {@link SymbolEligibilityRecord} instances into a {@code LoadSymbolEligibilityBatch} SBE
 * message (templateId&nbsp;20, APP-62&nbsp;§G).
 *
 * <p>The batch form is preferred over the single-record form ({@code LoadSymbolEligibility}
 * template&nbsp;19) for the same reason {@link
 * com.trading.refdata.risklimit.RiskLimitCommandEncoder RiskLimitCommandEncoder} uses the batch
 * form for risk limits: at start-of-day the gateway uploads the full restricted-symbol cohort in
 * one shot, and the batch form amortises the SBE message header + cluster ingress hop across N
 * records.
 *
 * <p>The cluster's {@link com.trading.engine.cluster.refdata.LoadSymbolEligibilityBatchHandler}
 * emits one {@code SymbolEligibilityLoadedEvent} per record on egress; the gateway's {@code
 * RefDataEgressBridge} therefore counts each event individually against the {@code
 * ResponseCollector} expected-count, which equals the per-batch record count.
 *
 * <p><b>Threading.</b> Not thread-safe — reuses mutable SBE flyweight fields. Single-threaded use
 * only.
 *
 * <p><b>Allocation.</b> Pre-allocated SBE encoders + per-record {@code symbolScratch} buffer. The
 * only per-call allocation is the {@code getBytes(US_ASCII)} on each record's symbol string, which
 * is acceptable on the start-up reference-data ingress path.
 */
public final class SymbolEligibilityCommandEncoder
    implements ReferenceDataEncoder<SymbolEligibilityRecord> {

  /** SBE {@code Symbol} fixed-length is 8 bytes (matches FIX tag 55). */
  private static final int SBE_SYMBOL_LENGTH = 8;

  /**
   * Batch size cap. The cluster repeating-group dimension is encoded as a u16; far above any
   * realistic symbol cohort. The cap matches {@link
   * com.trading.refdata.risklimit.RiskLimitCommandEncoder}'s value for consistency, and bounds the
   * single-batch encode buffer to a safe ceiling so a future config of hundreds of thousands of
   * symbols stays in MTU-friendly chunks.
   */
  private static final int MAX_BATCH_SIZE = 200;

  private static final String ENTITY_TYPE = "SymbolEligibility";

  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  private final LoadSymbolEligibilityBatchEncoder batchEncoder =
      new LoadSymbolEligibilityBatchEncoder();
  // Per-record symbol scratch — reused across every group.next() to avoid per-record allocation
  // inside the encode loop (kept consistent with the in-cluster batch handler's reuse pattern).
  private final byte[] symbolScratch = new byte[SBE_SYMBOL_LENGTH];

  /** {@inheritDoc} */
  @Override
  public int encodeBatch(
      final List<SymbolEligibilityRecord> records,
      final int fromIndex,
      final int toIndex,
      final MutableDirectBuffer buffer,
      final int offset)
      throws ReferenceDataLoadException {

    int count = toIndex - fromIndex;
    batchEncoder.wrapAndApplyHeader(buffer, offset, headerEncoder);
    // Cluster overwrites the wire transactTime with its own deterministic timestamp on emit.
    batchEncoder.transactTime(0L);

    final NoEligibilitiesEncoder group = batchEncoder.noEligibilitiesCount(count);

    for (int i = fromIndex; i < toIndex; i++) {
      final var record = records.get(i);

      packSymbolAscii(record.symbol(), symbolScratch);

      group
          .next()
          .putSymbol(symbolScratch, 0)
          .tradingAllowed((short) (record.tradingAllowed() ? 1 : 0))
          .shortSaleAllowed((short) (record.shortSaleAllowed() ? 1 : 0))
          .priceDeviationBpsOverride(record.priceDeviationBpsOverride());
    }

    return MessageHeaderEncoder.ENCODED_LENGTH + batchEncoder.encodedLength();
  }

  /** {@inheritDoc} */
  @Override
  public int templateId() {
    return LoadSymbolEligibilityBatchEncoder.TEMPLATE_ID;
  }

  /** {@inheritDoc} */
  @Override
  public int maxBatchSize() {
    return MAX_BATCH_SIZE;
  }

  /** {@inheritDoc} */
  @Override
  public String entityType() {
    return ENTITY_TYPE;
  }

  /**
   * Packs the symbol string into an 8-byte ASCII buffer, zero-padded on the right (matches the SBE
   * {@code Symbol} fixed-length type and the cluster handler's expectations). The record's compact
   * constructor has already validated that the ASCII byte length fits.
   */
  private static void packSymbolAscii(final String symbol, final byte[] dst) {
    // Gemini R3: was `symbol.getBytes(StandardCharsets.US_ASCII)` which allocates a fresh byte
    // array per call. The record's SymbolEligibilityRecord compact constructor already validates
    // pure-ASCII so direct char→byte casting is safe (every codepoint fits in one byte). Copy
    // chars directly into the destination scratch with explicit narrowing — zero allocation per
    // record. Clear-then-copy semantics preserved by the trailing zero-fill loop.
    final int n = Math.min(symbol.length(), dst.length);
    for (int i = 0; i < n; i++) {
      dst[i] = (byte) symbol.charAt(i);
    }
    // Defensive trailing zero-fill: covers reuse of a scratch buffer that carried a longer
    // previous symbol's tail bytes. SBE Symbol is fixed-length so trailing zero bytes are the
    // canonical padding for a shorter symbol.
    for (int i = n; i < dst.length; i++) {
      dst[i] = 0;
    }
  }
}
