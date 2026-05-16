package com.trading.engine.pricing.market;

import com.trading.engine.messages.FixedPointScale;
import java.util.Arrays;
import java.util.Objects;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Pricing-service-side bridge that polls the shared {@link MidRateCache} on every agent tick and
 * pushes a synthetic top-of-book tick (bid / ask / sizes) into the {@link MarketDataPublisher}.
 *
 * <p>Lives on the same agent thread as the {@link MarketDataPublisher}. Composed into the
 * pricing-service {@code CompositeAgent} alongside {@link
 * com.trading.engine.pricing.PricingService} so the single-writer invariant holds without any
 * cross-thread channel.
 *
 * <p><b>Threading model.</b> Not thread-safe. The agent thread is the sole reader of the cache (for
 * THIS bridge instance; the cache itself is also read by RFQ pricing on the same thread) and the
 * sole caller into the publisher.
 *
 * <p><b>Allocation.</b> Zero per tick: the symbol-key array is wrapped once in {@link
 * UnsafeBuffer#wrap(byte[])} at construction; the cache lookup is allocation-free; the publisher's
 * {@code onTick} mutates a pre-existing slot in place.
 *
 * <p><b>Design rationale.</b> Separating the bridge from the synthetic adapter keeps the adapter's
 * existing RFQ-pricing contract intact (it writes mid-rates to a cache that the RFQ pricer reads).
 * The broadcast feed is purely additive: the bridge READS the same cache and DERIVES bid/ask via a
 * fixed spread + emits to the publisher. Production deployments substitute a real adapter that
 * pushes bid/ask directly; in dev/staging the synthetic mid → synthetic bid/ask path keeps the wire
 * shape identical to production.
 *
 * <p>The 1 bp spread + 1M unit-size constants below are chosen to match the rest of the synthetic
 * dev cohort (the RFQ pricer applies its own spread from {@code SpreadConfig}; the broadcast feed
 * uses a static spread for predictable browser rendering). They are not production-tunable — for
 * real-feed wiring, replace the bridge with an adapter that pushes actual bid/ask directly.
 *
 * <p><b>Dependencies.</b> {@link MidRateCache} (read), {@link MarketDataPublisher} (write), {@link
 * EpochNanoClock} (ingressNanos timestamp).
 */
public final class MidRateToTickBridge implements Agent {

  /** 1 basis point = 1/10_000 of the rate. Half-spread is 0.5 bp on each side. */
  private static final long ONE_BP_DIVISOR = 10_000L;

  /** Fixed bid/ask size — 1 million units in fixed-point 10^-8 = 10^8 × 10^6 = 10^14. */
  private static final long DEFAULT_SIZE_FP = 1_000_000L * FixedPointScale.PRICE_SCALE;

  /** Symbol length on the wire — 8-byte fixed-width SBE Symbol type. */
  private static final int SYMBOL_BYTES = 8;

  private final MidRateCache cache;
  private final MarketDataPublisher publisher;
  private final EpochNanoClock epochNanoClock;
  private final byte[][] symbolKeyBytes;
  private final long[] packedSymbols;
  private final UnsafeBuffer probeBuffer = new UnsafeBuffer(new byte[SYMBOL_BYTES]);

  /**
   * Constructs the bridge with the symbol cohort known at startup. The cohort is provided once
   * (cold path); steady-state {@code doWork} only iterates the pre-registered keys.
   *
   * @param cache shared mid-rate cache; the synthetic / deterministic adapter writes here.
   * @param publisher the market-data publisher; the bridge pushes ticks via {@code onTick}.
   * @param epochNanoClock wall-clock for the FIX tag-60 {@code ingressNanos} timestamp.
   * @param symbols the cohort of 8-byte symbol keys to bridge. Each entry must be exactly 8 bytes
   *     (SBE Symbol type, right-padded). One {@code onTick} fires per symbol per {@code doWork}
   *     iteration when a fresh mid-rate is present.
   */
  public MidRateToTickBridge(
      final MidRateCache cache,
      final MarketDataPublisher publisher,
      final EpochNanoClock epochNanoClock,
      final byte[][] symbols) {
    this.cache = Objects.requireNonNull(cache, "cache");
    this.publisher = Objects.requireNonNull(publisher, "publisher");
    this.epochNanoClock = Objects.requireNonNull(epochNanoClock, "epochNanoClock");
    this.symbolKeyBytes = new byte[symbols.length][];
    this.packedSymbols = new long[symbols.length];
    for (int i = 0; i < symbols.length; i++) {
      final byte[] sym = symbols[i];
      if (sym.length != SYMBOL_BYTES) {
        throw new IllegalArgumentException(
            "symbol bytes must be exactly " + SYMBOL_BYTES + " — got " + sym.length);
      }
      // Defensive copy so a caller mutating the input array post-construction cannot drift the
      // bridge's per-symbol keys.
      this.symbolKeyBytes[i] = Arrays.copyOf(sym, SYMBOL_BYTES);
      this.packedSymbols[i] = pack(sym);
    }
  }

  /** Cold-path role identifier for the agent runner / diagnostics. */
  @Override
  public String roleName() {
    return "midrate-to-tick-bridge";
  }

  /** No-op: the bridge has no resources to initialise beyond what the constructor sets up. */
  @Override
  public void onStart() {
    // Intentionally empty.
  }

  /** No-op: the bridge has no resources to release. */
  @Override
  public void onClose() {
    // Intentionally empty.
  }

  /**
   * Polls the mid-rate cache for each registered symbol; on a non-{@link
   * FixedPointScale#PRICE_NOT_AVAILABLE} value, derives bid/ask via a static spread and pushes to
   * the publisher.
   *
   * @return the number of {@code onTick} invocations completed this cycle.
   */
  @Override
  public int doWork() {
    final long ingressNanos = epochNanoClock.nanoTime();
    int count = 0;
    for (int i = 0; i < symbolKeyBytes.length; i++) {
      probeBuffer.wrap(symbolKeyBytes[i]);
      final long mid = cache.midRate(probeBuffer, 0, SYMBOL_BYTES);
      if (mid == FixedPointScale.PRICE_NOT_AVAILABLE) {
        continue;
      }
      final long halfSpread = (mid / ONE_BP_DIVISOR) / 2L; // 0.5 bp on each side
      final long bid = mid - halfSpread;
      final long ask = mid + halfSpread;
      publisher.onTick(packedSymbols[i], bid, ask, DEFAULT_SIZE_FP, DEFAULT_SIZE_FP, ingressNanos);
      count++;
    }
    return count;
  }

  /**
   * Pack an 8-byte SBE Symbol field into a {@code long} via little-endian byte interpretation. The
   * conflation map in {@link MarketDataPublisher} uses this packed value as the {@code
   * Long2ObjectHashMap} key — primitive-keyed map avoids the {@code String} hash + allocation that
   * would otherwise hit on every tick.
   *
   * @param sym the 8-byte symbol key.
   * @return the packed long.
   */
  static long pack(final byte[] sym) {
    long packed = 0L;
    for (int i = 0; i < SYMBOL_BYTES; i++) {
      packed |= ((long) (sym[i] & 0xFF)) << (i * 8);
    }
    return packed;
  }
}
