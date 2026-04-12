package com.trading.engine.pricing;

import com.trading.engine.messages.FixedPointScale;

/**
 * Central constants for the pricing-service module's Aeron IPC transport layer.
 *
 * <p><b>Stream IDs vs. SBE template IDs.</b> The stream IDs defined here ({@link
 * #REQUEST_STREAM_ID} = 200, {@link #RESPONSE_STREAM_ID} = 201) are <em>Aeron stream
 * identifiers</em> that multiplex logical channels over a single IPC media driver. They are
 * <b>not</b> SBE template IDs. The snapshot template ID range 200-206 in {@code trading-schema.xml}
 * is an independent numbering space; no collision occurs because Aeron streams and SBE templates
 * are resolved by completely different mechanisms (Aeron subscription vs. SBE header decoding).
 *
 * <p>For fixed-point price scale and sentinel values, use {@link FixedPointScale#PRICE_SCALE} and
 * {@link FixedPointScale#PRICE_NOT_AVAILABLE} from the shared {@code messages} module. This class
 * intentionally does not duplicate those constants.
 *
 * <p><b>Threading:</b> all fields are compile-time constants — safe for unrestricted concurrent
 * access from any thread.
 *
 * <p><b>Allocation:</b> zero allocation — no methods, no instances.
 */
public final class PricingConstants {

  /**
   * Aeron IPC stream ID for inbound price requests.
   *
   * <p>Carries {@code PriceRequest} and {@code PriceValidationRequest} SBE messages from the
   * gateway (or cluster egress) to the pricing-service agent.
   */
  public static final int REQUEST_STREAM_ID = 200;

  /**
   * Aeron IPC stream ID for outbound price responses.
   *
   * <p>Carries {@code PriceResponse} and {@code PriceValidationResponse} SBE messages from the
   * pricing-service agent back to the gateway (or cluster ingress).
   */
  public static final int RESPONSE_STREAM_ID = 201;

  /**
   * Aeron IPC channel URI.
   *
   * <p>All pricing-service communication uses shared-memory IPC via the external media driver. No
   * UDP transport is needed because the pricing service is co-located with the gateway process.
   */
  public static final String IPC_CHANNEL = "aeron:ipc";

  /**
   * Pre-allocated encoding buffer size in bytes.
   *
   * <p>Sized for worst-case PriceResponse with two swap legs:
   *
   * <ul>
   *   <li>SBE message header: 8 bytes
   *   <li>PriceResponse block fields: 151 bytes
   *   <li>Repeating group header (legs): 4 bytes
   *   <li>2 leg entries at 45 bytes each: 90 bytes
   * </ul>
   *
   * <p>Total worst case: 253 bytes. The 512-byte buffer provides ample margin for future field
   * additions without requiring a resize.
   */
  public static final int ENCODING_BUFFER_SIZE = 512;

  /**
   * Maximum number of retries when an Aeron publication returns a back-pressure signal ({@link
   * io.aeron.Publication#BACK_PRESSURED} or {@link io.aeron.Publication#ADMIN_ACTION}).
   *
   * <p>Bounded to avoid spinning indefinitely on a saturated IPC channel. If all retries are
   * exhausted, the caller should log the failure and drop the message — the upstream requestor will
   * time out and retry.
   */
  public static final int MAX_PUBLICATION_RETRIES = 3;

  private PricingConstants() {}
}
