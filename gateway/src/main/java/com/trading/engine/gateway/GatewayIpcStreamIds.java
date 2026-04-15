package com.trading.engine.gateway;

/**
 * Aeron IPC stream ID constants for gateway ↔ orchestrator communication. These must match the
 * corresponding constants in {@code OrchestratorConstants} (orchestrator module).
 *
 * <p>TODO(APP-214): extract to the shared {@code messages} module to eliminate duplication and
 * ensure compile-time consistency between gateway and orchestrator.
 *
 * @see com.trading.engine.gateway.OrchestratorResponseListener
 * @see com.trading.engine.gateway.FixSessionHandler
 */
public final class GatewayIpcStreamIds {

  /** Stream ID for gateway → orchestrator requests (QuoteRequest, NewOrderSingle with quoteId). */
  public static final int ORCHESTRATOR_REQUEST_STREAM_ID = 100;

  /**
   * Stream ID for orchestrator → gateway responses (Quote, QuoteRequestReject, ExecutionReport,
   * forwarded NOS).
   */
  public static final int ORCHESTRATOR_RESPONSE_STREAM_ID = 101;

  private GatewayIpcStreamIds() {} // constants only
}
