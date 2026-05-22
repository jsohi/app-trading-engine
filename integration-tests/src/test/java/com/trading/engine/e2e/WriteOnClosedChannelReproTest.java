package com.trading.engine.e2e;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * APP-225 §D7 reproducer — write-on-closed-channel desync.
 *
 * <p><b>Invariant under test:</b> when the WebSocket is closed while a write is in flight, the
 * worker MUST surface a {@code ConnectionLostError} (not a silent drop) and the in-flight command's
 * promise MUST reject within the slot timeout.
 *
 * <p><b>Defect class:</b> Netty channel state desync.
 *
 * <p><b>Status:</b> SKELETON. Disabled pending the implementation strategy. This reproducer needs a
 * real MediaDriver topology plus a {@code Channel.close()} interleaving primitive that races a NOS
 * write against channel teardown. The assertion must confirm that the client-side promise rejects
 * with {@code ConnectionLostError} within the configured slot timeout — not a silent drop detected
 * only via timeout expiry.
 *
 * <p><b>Threading:</b> single-threaded JUnit test method; the harness it eventually drives is
 * multi-process (real MediaDriver + cluster + gateway).
 *
 * <p><b>Allocation:</b> test path; allocation acceptable.
 */
@Tag("repro-d7")
@Disabled("APP-225 §D7 skeleton — pending harness + Channel.close() interleaving primitive")
final class WriteOnClosedChannelReproTest {

  @Test
  void writeOnClosedChannel_reproducesDefect_surfacesConnectionLostError() {
    // TODO(APP-225 §D7): implement the failure-injection harness for write-on-closed-channel.
    // Steps:
    //   1. Spin up a real MediaDriver + 3-node cluster + gateway + WebSocket server.
    //   2. Establish a WebSocket session and authenticate.
    //   3. Arm a Channel.close() interleave: close the Netty channel immediately after the
    //      write is issued but before the flush completes (use a ChannelOutboundHandler shim).
    //   4. Submit a NewOrderSingle via the WebSocket client.
    //   5. Assert: the client-side command promise rejects with ConnectionLostError within
    //      the slot timeout (not a silent drop / plain timeout).
    //   6. Assert: no OrderCreatedEvent is published to the cluster log for the dropped NOS.
    // Acceptance: in-flight NOS promise rejects with ConnectionLostError within slot timeout.
    throw new UnsupportedOperationException(
        "APP-225 §D7 WriteOnClosedChannelReproTest — see class Javadoc.");
  }
}
