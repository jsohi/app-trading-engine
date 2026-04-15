package com.trading.engine.pricing.market;

import static com.trading.engine.testsupport.buffer.SbeFieldUtil.spacePad;
import static com.trading.engine.testsupport.buffer.SbeFieldUtil.wrapSymbol;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.trading.engine.messages.FixedPointScale;
import com.trading.engine.testsupport.buffer.SbeFieldUtil;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link DeterministicMarketDataAdapter}.
 *
 * <p>Verifies that the adapter correctly delegates to its underlying {@link MidRateCache},
 * returning pre-loaded mid-rates for known symbols and {@link FixedPointScale#PRICE_NOT_AVAILABLE}
 * for unknown symbols. The adapter's duty-cycle method ({@link
 * DeterministicMarketDataAdapter#doWork()}) must always return 0 because rates are static.
 */
class DeterministicMarketDataAdapterTest {

  /** EUR/USD mid-rate: 1.085 in fixed-point 10^-8. */
  private static final long EURUSD_MID = 108_500_000L;

  /** Monotonic timestamp for the preloaded entry. */
  private static final long PRELOAD_NANOS = 1_000_000_000L;

  /** Fixed NanoClock returning a constant value for deterministic tests. */
  private static final NanoClock FIXED_CLOCK = () -> PRELOAD_NANOS;

  /** SBE Symbol field width — 8 bytes, right-padded with spaces per convention. */
  private static final int SYMBOL_LENGTH = SbeFieldUtil.SYMBOL_LENGTH;

  private MidRateCache cache;
  private DeterministicMarketDataAdapter adapter;

  @BeforeEach
  void setUp() {
    cache = new MidRateCache();

    // Preload EURUSD: right-pad to 8 bytes with spaces (SBE Symbol convention).
    final byte[] eurusd = spacePad("EURUSD", SbeFieldUtil.SYMBOL_LENGTH);
    cache.preload(eurusd, EURUSD_MID, PRELOAD_NANOS);

    adapter = new DeterministicMarketDataAdapter(cache, FIXED_CLOCK);
  }

  /**
   * A known symbol that was preloaded must return the configured mid-rate when queried through the
   * adapter.
   */
  @Test
  void midRate_knownSymbol_returnsConfiguredRate() {
    final UnsafeBuffer symbolBuf = wrapSymbol("EURUSD");
    assertEquals(EURUSD_MID, adapter.midRate(symbolBuf, 0, SYMBOL_LENGTH));
  }

  /** An unknown symbol (not preloaded) must return {@link FixedPointScale#PRICE_NOT_AVAILABLE}. */
  @Test
  void midRate_unknownSymbol_returnsPriceNotAvailable() {
    final UnsafeBuffer symbolBuf = wrapSymbol("GBPUSD");
    assertEquals(FixedPointScale.PRICE_NOT_AVAILABLE, adapter.midRate(symbolBuf, 0, SYMBOL_LENGTH));
  }

  /**
   * The deterministic adapter performs no work — rates are static and never change. The duty-cycle
   * method must always return 0.
   */
  @Test
  void doWork_alwaysReturnsZero() {
    assertEquals(0, adapter.doWork());
    assertEquals(0, adapter.doWork());
  }

  /** A known symbol that was preloaded must return the timestamp provided at preload time. */
  @Test
  void lastUpdateNanos_knownSymbol_returnsTimestamp() {
    final UnsafeBuffer symbolBuf = wrapSymbol("EURUSD");
    assertEquals(PRELOAD_NANOS, adapter.lastUpdateNanos(symbolBuf, 0, SYMBOL_LENGTH));
  }

  /**
   * An unknown symbol must return 0 for the last-update timestamp (the cache's default for missing
   * entries).
   */
  @Test
  void lastUpdateNanos_unknownSymbol_returnsZero() {
    final UnsafeBuffer symbolBuf = wrapSymbol("GBPUSD");
    assertEquals(0L, adapter.lastUpdateNanos(symbolBuf, 0, SYMBOL_LENGTH));
  }
}
