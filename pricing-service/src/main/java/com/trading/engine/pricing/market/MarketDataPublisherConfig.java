package com.trading.engine.pricing.market;

import static com.trading.engine.messages.MarketDataConstants.MARKET_DATA_HEARTBEAT_BASE_MS;
import static com.trading.engine.messages.MarketDataConstants.MARKET_DATA_PUBLISH_CADENCE_MICROS;

/**
 * Runtime configuration for the {@link MarketDataPublisher}.
 *
 * <p>Loaded from {@code pricing-service.yaml} at startup (cold path) and passed to the publisher
 * constructor. Overrides the default cross-module constants in {@link
 * com.trading.engine.messages.MarketDataConstants} for soak / stress / dev tuning without requiring
 * a source-code change — addresses the Phase 3 non-negotiable "no kill switches, no fallbacks" rule
 * in spirit by keeping the choice in configuration, not source.
 *
 * <p><b>Threading model.</b> Immutable record — safe for unrestricted concurrent access.
 *
 * <p><b>Allocation.</b> One instance per process (constructed at startup); zero allocation on any
 * hot path.
 *
 * <p><b>Design rationale.</b> The Phase 3 plan explicitly calls out (Rec-16) that letting the soak
 * test select the {@link AdapterKind#DETERMINISTIC} adapter via configuration (vs a compile-time
 * branch in source) preserves the "single code path in production" invariant — the adapter
 * selection is the same mechanism prod uses, just with a different YAML value.
 *
 * <p><b>Dependencies.</b> {@link com.trading.engine.messages.MarketDataConstants} for default
 * cadence / heartbeat values.
 *
 * @param adapter which {@link MarketDataAdapter} the publisher's host agent constructs at startup.
 *     {@link AdapterKind#SYNTHETIC} is the default for dev / staging; {@link
 *     AdapterKind#DETERMINISTIC} is selected by the soak test for reproducible tick sequences.
 * @param cadenceMicros override for the publisher drain cadence in microseconds. Pass the same
 *     value as {@link
 *     com.trading.engine.messages.MarketDataConstants#MARKET_DATA_PUBLISH_CADENCE_MICROS} (=5 000)
 *     for the production default; soak/stress can lower to 1 000 or raise to 10 000.
 * @param heartbeatBaseMs override for the heartbeat emit base period in milliseconds. Pass the same
 *     value as {@link
 *     com.trading.engine.messages.MarketDataConstants#MARKET_DATA_HEARTBEAT_BASE_MS} (=1 000) for
 *     the production default; the ±10 % jitter is applied at runtime.
 */
public record MarketDataPublisherConfig(
    AdapterKind adapter, long cadenceMicros, long heartbeatBaseMs) {

  /** Adapter selection. */
  public enum AdapterKind {
    /** Geometric-Brownian-motion synthetic mid-rates. Default for dev/staging. */
    SYNTHETIC,
    /** Deterministic per-tick sequence — selected by soak tests for reproducibility. */
    DETERMINISTIC,
  }

  /**
   * Returns the production-default configuration (synthetic adapter, 5 ms drain cadence, 1 s
   * heartbeat base). Used when no YAML override is supplied.
   *
   * @return a config snapshot identical to the {@code MarketDataConstants} defaults.
   */
  public static MarketDataPublisherConfig defaults() {
    return new MarketDataPublisherConfig(
        AdapterKind.SYNTHETIC, MARKET_DATA_PUBLISH_CADENCE_MICROS, MARKET_DATA_HEARTBEAT_BASE_MS);
  }
}
