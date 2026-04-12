package com.trading.engine.pricing.market;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.NanoClock;

/**
 * Market data adapter that serves fixed, pre-loaded mid-rates. Rates are provided by the caller at
 * construction time (loaded from YAML config externally) and written into the shared {@link
 * MidRateCache} via {@link MidRateCache#preload}. Once loaded, the rates never change.
 *
 * <p><b>Purpose:</b> for integration tests — produces known, fixed prices for deterministic spread
 * assertions. Because the rates are constant, test assertions can hard-code expected bid/ask values
 * without being sensitive to timing or random jitter.
 *
 * <h3>Threading model</h3>
 *
 * <p><b>Not thread-safe.</b> Single-threaded agent duty cycle. {@link #doWork()} is a no-op that
 * always returns 0 (no work performed) because rates are static.
 *
 * <h3>Allocation behaviour</h3>
 *
 * <p>Zero allocation after construction. {@link #doWork()}, {@link #midRate}, and {@link
 * #lastUpdateNanos} perform no heap allocation.
 *
 * @see MarketDataAdapter
 * @see MidRateCache
 */
public final class DeterministicMarketDataAdapter implements MarketDataAdapter {

  private static final Log LOG = LogFactory.getLog(DeterministicMarketDataAdapter.class);

  /** Shared cache that the pricing service reads from on the same thread. */
  private final MidRateCache cache;

  /** Monotonic clock for stamping the initial load time in {@link #onStart()}. */
  private final NanoClock nanoClock;

  /**
   * Constructs a deterministic adapter. The caller must have already loaded base rates into the
   * cache via {@link MidRateCache#preload} before the agent is started. {@link #onStart()} will
   * stamp all entries with the current {@link NanoClock} time.
   *
   * @param cache the mid-rate cache shared with the pricing service (must already contain
   *     pre-loaded symbols)
   * @param nanoClock monotonic clock for timestamp stamping; must not be {@code null}
   */
  public DeterministicMarketDataAdapter(final MidRateCache cache, final NanoClock nanoClock) {
    this.cache = cache;
    this.nanoClock = nanoClock;
  }

  /**
   * Logs the adapter start. Pre-loaded entries retain the timestamps provided at {@link
   * MidRateCache#preload} time.
   *
   * <p>This is a cold-path method invoked once by the {@link org.agrona.concurrent.AgentRunner}
   * before the duty cycle begins.
   */
  @Override
  public void onStart() {
    LOG.info()
        .append("DeterministicMarketDataAdapter started: symbols=")
        .append(cache.size())
        .commit();
    // No per-entry re-stamp needed here — preload already set the timestamps.
    // If the caller passed 0 as the initial timestamp, we could iterate and fix up,
    // but the contract is that the caller provides meaningful timestamps at preload time.
  }

  /** No-op — deterministic adapter has no resources to release. */
  @Override
  public void onClose() {
    // Intentionally empty.
  }

  /**
   * No-op duty cycle. Rates are fixed and never change, so there is no work to perform.
   *
   * @return always {@code 0} (no work done)
   */
  @Override
  public int doWork() {
    return 0;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Delegates to the shared {@link MidRateCache}. Zero allocation.
   */
  @Override
  public long midRate(final DirectBuffer symbol, final int offset, final int length) {
    return cache.midRate(symbol, offset, length);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Delegates to the shared {@link MidRateCache}. Zero allocation.
   */
  @Override
  public long lastUpdateNanos(final DirectBuffer symbol, final int offset, final int length) {
    return cache.lastUpdateNanos(symbol, offset, length);
  }

  /**
   * Returns the agent role name for diagnostics and error reporting.
   *
   * @return {@code "deterministic-market-data"}
   */
  @Override
  public String roleName() {
    return "deterministic-market-data";
  }
}
