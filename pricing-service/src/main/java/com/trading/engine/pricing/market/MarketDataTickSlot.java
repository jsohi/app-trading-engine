package com.trading.engine.pricing.market;

/**
 * Mutable per-symbol top-of-book slot owned by {@link MarketDataPublisher}'s conflation map.
 *
 * <p>The publisher's adapter pushes a tick via {@code publisher.onTick(...)}; the publisher finds
 * (or creates, on first sight) the slot for that symbol and overwrites its fields in place. The
 * drain cycle (5 ms cadence) iterates every slot and publishes whichever was most recently written
 * for each symbol — natural top-of-book conflation. A 1 000 Hz mid-rate source therefore publishes
 * at most ~200 ticks/sec/symbol on the wire (5× compression).
 *
 * <p><b>Threading model.</b> Not thread-safe. Single-writer (the pricing-service agent thread)
 * mutates the fields and reads them on the same thread on the drain cycle. No fences or
 * synchronisation are needed: writes happen-before subsequent reads on the same thread per the JMM.
 * The {@code MarketDataPublisherSingleWriterJCStress} test asserts the runtime guard fires if a
 * future refactor attempts to call {@code onTick} from a different thread.
 *
 * <p><b>Allocation.</b> Zero after first sight per symbol. The slot is constructed exactly once —
 * on the first {@code onTick(packedSymbol)} for that symbol via {@link
 * java.util.function.LongFunction} factory installed as a {@code final} field on the publisher (NOT
 * a per-call lambda). Subsequent ticks mutate this instance in place.
 *
 * <p><b>Design rationale.</b> Mirrors the LMAX exchange-core "slot table" pattern used for
 * order-book inside markets: pre-allocate one instance per row at first sight; mutate in place
 * thereafter. Avoids per-tick allocation while preserving cache locality (the slot fields are read
 * out into the SBE encoder on every drain cycle, so a tightly-packed `long`-only layout minimises
 * L1 misses).
 *
 * <p><b>Dependencies.</b> None.
 *
 * @see MarketDataPublisher
 */
public final class MarketDataTickSlot {

  /** Bid price in fixed-point 10^-8. Mutated in place by every {@code onTick} for this symbol. */
  long bidPrice;

  /** Ask price in fixed-point 10^-8. */
  long askPrice;

  /** Bid size in fixed-point 10^-8. */
  long bidSize;

  /** Ask size in fixed-point 10^-8. */
  long askSize;

  /**
   * Epoch-nanos when the adapter sampled this mid-rate. Carried on the wire as the FIX tag-60
   * {@code TransactTime} (engine sample time); the browser computes publisher-stack latency as
   * {@code serverNanos - ingressNanos} where {@code serverNanos} is the publish wall-clock.
   */
  long ingressNanos;

  /**
   * Per-symbol monotonic publish sequence (FIX tag-83 {@code RptSeq}). Incremented on every
   * successful publish; {@code 0} on the snapshot path. Wrap is handled via {@code
   * Long.compareUnsigned} so an overflow rolls cleanly from {@code Long.MAX_VALUE} to {@code
   * Long.MIN_VALUE} without breaking ordering.
   */
  long symbolSeq;

  /** Default-construct an empty slot. The publisher overwrites every field on the first tick. */
  MarketDataTickSlot() {}

  /**
   * Overwrite every wire field except {@code symbolSeq} (the publisher owns the sequence counter
   * and increments it once per successful drain). Called from inside {@code
   * MarketDataPublisher.onTick} on the agent thread.
   *
   * @param bidPrice fixed-point 10^-8.
   * @param askPrice fixed-point 10^-8.
   * @param bidSize fixed-point 10^-8.
   * @param askSize fixed-point 10^-8.
   * @param ingressNanos adapter-sample timestamp in epoch nanos.
   */
  void set(
      final long bidPrice,
      final long askPrice,
      final long bidSize,
      final long askSize,
      final long ingressNanos) {
    this.bidPrice = bidPrice;
    this.askPrice = askPrice;
    this.bidSize = bidSize;
    this.askSize = askSize;
    this.ingressNanos = ingressNanos;
  }
}
