package com.trading.engine.orchestrator;

import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.NewOrderSingleEncoder;

/**
 * Central constants for the orchestrator module's Aeron IPC transport layer, RFQ pool sizing, and
 * timeout configuration.
 *
 * <p><b>Stream IDs.</b> The orchestrator bridges two IPC channel pairs:
 *
 * <ul>
 *   <li><b>Gateway ↔ Orchestrator</b>: streams 100 (inbound: QuoteRequest, NewOrderSingle) and 101
 *       (outbound: Quote, QuoteRequestReject, ExecutionReport).
 *   <li><b>Orchestrator ↔ Pricing</b>: streams 200 (outbound: PriceRequest, PriceValidationRequest)
 *       and 201 (inbound: PriceResponse, PriceValidationResponse).
 * </ul>
 *
 * <p>Pricing stream IDs intentionally duplicate {@code PricingConstants.REQUEST_STREAM_ID} (200)
 * and {@code PricingConstants.RESPONSE_STREAM_ID} (201) because the orchestrator module does not
 * depend on the pricing-service module. TODO(APP-214): extract stream IDs to a shared {@code
 * StreamIds} class in the {@code messages} module.
 *
 * <p><b>Threading:</b> all fields are compile-time constants — safe for unrestricted concurrent
 * access from any thread.
 *
 * <p><b>Allocation:</b> zero allocation — no methods, no instances.
 */
public final class OrchestratorConstants {

  // ===========================================================================
  // Aeron IPC stream IDs
  // ===========================================================================

  /**
   * Aeron IPC stream ID for inbound messages from the gateway.
   *
   * <p>Carries {@code QuoteRequest} (templateId=1) and {@code NewOrderSingle} (templateId=4)
   * messages from the gateway to the orchestrator.
   */
  public static final int GATEWAY_REQUEST_STREAM_ID = 100;

  /**
   * Aeron IPC stream ID for outbound messages to the gateway.
   *
   * <p>Carries {@code Quote} (templateId=2), {@code QuoteRequestReject} (templateId=3), and {@code
   * ExecutionReport} (templateId=5) messages from the orchestrator to the gateway.
   */
  public static final int GATEWAY_RESPONSE_STREAM_ID = 101;

  /**
   * Aeron IPC stream ID for outbound price requests to the pricing service.
   *
   * <p>Carries {@code PriceRequest} (templateId=50) and {@code PriceValidationRequest}
   * (templateId=52) messages from the orchestrator to the pricing service.
   *
   * <p>Duplicates {@code PricingConstants.REQUEST_STREAM_ID}. TODO(APP-214): extract to shared
   * module.
   */
  public static final int PRICING_REQUEST_STREAM_ID = 200;

  /**
   * Aeron IPC stream ID for inbound price responses from the pricing service.
   *
   * <p>Carries {@code PriceResponse} (templateId=51) and {@code PriceValidationResponse}
   * (templateId=53) messages from the pricing service to the orchestrator.
   *
   * <p>Duplicates {@code PricingConstants.RESPONSE_STREAM_ID}. TODO(APP-214): extract to shared
   * module.
   */
  public static final int PRICING_RESPONSE_STREAM_ID = 201;

  /**
   * Aeron IPC channel URI.
   *
   * <p>All orchestrator communication uses shared-memory IPC via the external media driver. No UDP
   * transport is needed because the orchestrator is co-located with the gateway and pricing service
   * processes.
   */
  public static final String IPC_CHANNEL = "aeron:ipc";

  // ===========================================================================
  // Encoding buffer sizing
  // ===========================================================================

  /**
   * Pre-allocated encoding buffer size in bytes.
   *
   * <p>Sized for worst-case outbound message (Quote with two swap legs):
   *
   * <ul>
   *   <li>SBE message header: 8 bytes
   *   <li>Quote block fields: ~200 bytes
   *   <li>Repeating group header (noLegs): 4 bytes
   *   <li>2 leg entries at ~48 bytes each: ~96 bytes
   * </ul>
   *
   * <p>Total worst case: ~308 bytes. The 512-byte buffer provides ample margin for future field
   * additions and matches {@code PricingConstants.ENCODING_BUFFER_SIZE}.
   */
  public static final int ENCODING_BUFFER_SIZE = 512;

  /**
   * Pre-allocated per-RFQ NOS stash buffer size in bytes.
   *
   * <p>When a NewOrderSingle arrives with a quoteId (RFQ accept), the orchestrator stashes the raw
   * fragment bytes for later cluster forwarding after price validation succeeds (APP-31). Sized for
   * worst-case NOS with multileg group:
   *
   * <ul>
   *   <li>SBE message header: {@value MessageHeaderEncoder#ENCODED_LENGTH} bytes
   *   <li>NewOrderSingle block: {@value NewOrderSingleEncoder#BLOCK_LENGTH} bytes
   *   <li>Margin for multileg NOS with noLegs group: ~288 bytes
   * </ul>
   *
   * <p>Total: 116 bytes for single-leg NOS. 512 matches {@link #ENCODING_BUFFER_SIZE} and provides
   * generous margin for multileg orders and future schema additions.
   */
  public static final int NOS_STASH_BUFFER_SIZE = 512;

  // ===========================================================================
  // Publication retry
  // ===========================================================================

  /**
   * Maximum number of retries when an Aeron publication returns a back-pressure signal ({@link
   * io.aeron.Publication#BACK_PRESSURED} or {@link io.aeron.Publication#ADMIN_ACTION}).
   *
   * <p>Bounded to avoid spinning indefinitely on a saturated IPC channel. If all retries are
   * exhausted, the caller logs the failure and either returns {@code Action.ABORT} (for
   * ControlledFragmentHandler re-delivery) or drops the message (for reap notifications, where the
   * client-side timeout is the fallback).
   */
  public static final int MAX_PUBLICATION_RETRIES = 3;

  // ===========================================================================
  // RFQ pool sizing
  // ===========================================================================

  /**
   * Default maximum number of concurrently active RFQs in the pre-allocated pool.
   *
   * <p>Each pool slot is a pre-allocated {@link RfqState} with a {@link
   * #NOS_STASH_BUFFER_SIZE}-byte flat buffer. At 10,000 entries the total pool memory is
   * approximately 10,000 × 590 bytes ≈ 5.6 MB — well within a typical JVM heap for a trading
   * process.
   */
  public static final int DEFAULT_MAX_ACTIVE_RFQS = 10_000;

  // ===========================================================================
  // Per-state timeouts (aligned with docs/state-machines.md)
  // ===========================================================================

  /**
   * Default timeout for the PENDING_PRICE state (maps to "Requested" in state-machines.md).
   *
   * <p>If the pricing service does not respond within 5 seconds, the RFQ is expired and the client
   * receives a QuoteRequestReject with reason TooLateToEnter.
   */
  public static final long DEFAULT_PENDING_PRICE_TIMEOUT_NANOS = 5_000_000_000L;

  /**
   * Default timeout for the QUOTED state (maps to "Quoted" in state-machines.md).
   *
   * <p>If the client does not accept the quote (via NewOrderSingle with quoteId) within 30 seconds,
   * the RFQ is expired and the client receives a QuoteRequestReject with reason TooLateToEnter.
   */
  public static final long DEFAULT_QUOTED_TIMEOUT_NANOS = 30_000_000_000L;

  /**
   * Default timeout for the PENDING_VALIDATION state.
   *
   * <p>If the pricing service does not return a PriceValidationResponse within 5 seconds, the RFQ
   * is expired and the client receives a reject ExecutionReport.
   */
  public static final long DEFAULT_PENDING_VALIDATION_TIMEOUT_NANOS = 5_000_000_000L;

  // ===========================================================================
  // Sweep configuration
  // ===========================================================================

  /**
   * Interval between incremental timeout sweeps in the orchestrator duty cycle.
   *
   * <p>Every 1 second, the orchestrator scans a bounded portion of the pool for RFQs whose {@code
   * expiryNanos} has elapsed and transitions them to EXPIRED. Also triggers diagnostic counter
   * logging.
   */
  public static final long SWEEP_INTERVAL_NANOS = 1_000_000_000L;

  // ===========================================================================
  // Poll limits
  // ===========================================================================

  /**
   * Fragment limit for the gateway subscription per {@code doWork()} cycle.
   *
   * <p>Lower than {@link #PRICING_POLL_LIMIT} because gateway inbound (new QuoteRequests, NOS) is
   * less latency-critical than pricing responses. The client is already waiting for a quote; the
   * pricing response → Quote path is the latency-sensitive path.
   */
  public static final int GATEWAY_POLL_LIMIT = 32;

  /**
   * Fragment limit for the pricing subscription per {@code doWork()} cycle.
   *
   * <p>Higher than {@link #GATEWAY_POLL_LIMIT} because PriceResponse → Quote delivery is the
   * latency-sensitive path. The orchestrator polls pricing responses FIRST in the duty cycle.
   */
  public static final int PRICING_POLL_LIMIT = 128;

  private OrchestratorConstants() {}
}
