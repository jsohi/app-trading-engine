package com.trading.engine.websocket;

import io.aeron.logbuffer.FragmentHandler;

/**
 * Single-abstract-method seam for polling the market-data ingest source. Mirrors the project's
 * canonical {@code Publisher} / {@code BroadcastPublisher} SAM pattern (see {@code
 * docs/publishers.md}) — keeps the {@link AeronEgressThread.DWRRPollingAgent} testable without
 * depending on the concrete {@code io.aeron.Subscription} (which is {@code final} and cannot be
 * subclassed or easily mocked).
 *
 * <p><b>Binding idiom.</b> The launcher binds a method reference to a {@code final} field at
 * construction:
 *
 * <pre>{@code
 * final Subscription marketDataSub = aeron.addSubscription(...);
 * final MarketDataPoller poller = marketDataSub::poll;   // SAM allocated once
 * }</pre>
 *
 * Tests bind a lambda or a {@code FakeMarketDataPoller} implementation that returns scripted
 * fragment counts to drive DWRR fairness / starvation / idle-reset scenarios deterministically.
 *
 * <p><b>Threading.</b> Implementations MUST be safe to call from the single {@code aeron-egress}
 * thread; the {@link AeronEgressThread} contract is single-threaded by design.
 *
 * <p><b>Allocation.</b> Implementations MUST be zero-allocation per call; the {@code DWRRPolling
 * Agent} polls every {@code AgentRunner} cycle.
 */
@FunctionalInterface
public interface MarketDataPoller {

  /**
   * Polls the underlying market-data {@code Subscription} for up to {@code fragmentLimit}
   * fragments, dispatching each one to {@code handler}.
   *
   * @param handler the fragment handler — typically a {@code FragmentAssembler} wrapping {@link
   *     MarketDataIngressHandler}
   * @param fragmentLimit the maximum number of fragments to consume in this single poll call
   * @return the number of fragments actually consumed
   */
  int poll(FragmentHandler handler, int fragmentLimit);
}
