package com.trading.engine.websocket;

import org.agrona.DirectBuffer;

/**
 * Single-abstract-method seam over the Aeron {@code Publication.offer(...)} used by the
 * dispatcher's {@link MarketDataAdmissionPipeline} to publish {@code MarketDataSnapshotRequest}
 * (template 56) frames to the pricing-service on stream {@link
 * com.trading.engine.messages.MarketDataConstants#MARKET_DATA_SNAPSHOT_REQUEST_STREAM_ID 205}.
 * Mirrors the project's canonical {@code Publisher} SAM pattern (see {@code docs/publishers.md}) —
 * keeps the admission pipeline testable without a real Aeron publication (the concrete {@code
 * io.aeron.ExclusivePublication} is {@code final}).
 *
 * <p><b>Binding idiom.</b> The launcher binds a method reference to a {@code final} field at
 * construction:
 *
 * <pre>{@code
 * final ExclusivePublication pub = aeron.addExclusivePublication(channel, streamId);
 * final SnapshotRequestPublisher publisher = pub::offer;   // SAM allocated once
 * }</pre>
 *
 * Tests bind a lambda or {@code FakeSnapshotRequestPublisher} that returns scripted Aeron {@code
 * offer()} return codes ({@code BACK_PRESSURED}, {@code NOT_CONNECTED}, {@code ADMIN_ACTION},
 * {@code MAX_POSITION_EXCEEDED}, {@code CLOSED}, success ≥ 0).
 *
 * <p><b>Threading.</b> Implementations are invoked from the channel's own Netty event loop —
 * single-threaded per session. Implementations MUST NOT block (Aeron's {@code offer()} is
 * non-blocking by contract).
 *
 * <p><b>Allocation.</b> Implementations MUST be zero-allocation per call; the dispatcher publishes
 * on the request hot path.
 */
@FunctionalInterface
public interface SnapshotRequestPublisher {

  /**
   * Offer the encoded {@code MarketDataSnapshotRequest} bytes to the underlying publication.
   * Semantics match Aeron's {@code Publication.offer(...)}.
   *
   * @param buffer source buffer containing the encoded frame
   * @param offset start offset in {@code buffer}
   * @param length total length in bytes
   * @return the stream position on success (≥ 0), or a negative Aeron error code ({@code
   *     BACK_PRESSURED}, {@code NOT_CONNECTED}, {@code ADMIN_ACTION}, {@code
   *     MAX_POSITION_EXCEEDED}, {@code CLOSED})
   */
  long offer(DirectBuffer buffer, int offset, int length);
}
