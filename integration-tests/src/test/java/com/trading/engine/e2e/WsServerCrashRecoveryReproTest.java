package com.trading.engine.e2e;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * APP-225 §D7 reproducer — archive/replay correctness across WebSocket server restart.
 *
 * <p><b>Invariant under test:</b> when the WebSocket server JVM crashes and restarts, browser
 * clients that reconnect within the {@code RECONNECT_GATE_MS} window MUST receive a complete event
 * replay from the cluster archive — no events lost.
 *
 * <p><b>Defect class:</b> archive/replay correctness across server restart.
 *
 * <p><b>Status:</b> SKELETON. Disabled pending the implementation strategy. This reproducer needs a
 * real MediaDriver topology plus the ability to hard-kill and restart the WebSocket server process,
 * preserving the Aeron Archive across the restart. The assertion must confirm that a reconnecting
 * client receives every event that was published to the archive before the crash — verified by
 * comparing sequence numbers against the pre-crash archive position. The {@code RECONNECT_GATE_MS}
 * configuration must be wired for the gate to be testable.
 *
 * <p><b>Threading:</b> single-threaded JUnit test method; the harness it eventually drives is
 * multi-process (real MediaDriver + cluster + gateway).
 *
 * <p><b>Allocation:</b> test path; allocation acceptable.
 */
@Tag("repro-d7")
@Disabled(
    "APP-225 §D7 skeleton — pending harness + WS server process kill/restart + archive replay")
final class WsServerCrashRecoveryReproTest {

  @Test
  void wsServerCrashAndRestart_reproducesDefect_reconnectingClientReceivesCompleteReplay() {
    // TODO(APP-225 §D7): implement the failure-injection harness for archive/replay across restart.
    // Steps:
    //   1. Spin up a real MediaDriver + 3-node cluster + gateway + WebSocket server.
    //   2. Establish an authenticated WebSocket session and record the last received event seq.
    //   3. Publish N additional events (NOS round-trips) to the cluster log after the snapshot.
    //   4. Hard-kill the WebSocket server JVM (SIGKILL — not graceful shutdown).
    //   5. Restart the WebSocket server; wait for it to reconnect to the Aeron Archive.
    //   6. Reconnect the browser client within RECONNECT_GATE_MS.
    //   7. Assert: the client receives all events from its last-known seq to the current archive
    //      head — no gaps, no duplicates, sequence monotonically increasing.
    // Acceptance: zero events lost across crash/restart within RECONNECT_GATE_MS window.
    throw new UnsupportedOperationException(
        "APP-225 §D7 WsServerCrashRecoveryReproTest — see class Javadoc.");
  }
}
