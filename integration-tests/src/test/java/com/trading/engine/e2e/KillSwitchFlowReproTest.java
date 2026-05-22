package com.trading.engine.e2e;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * APP-225 §D7 reproducer — kill-switch propagation correctness under trading halt.
 *
 * <p><b>Invariant under test:</b> when the operator triggers the trading-halt circuit breaker
 * (APP-152), every in-flight NOS at the time of the halt MUST be rejected with {@code
 * OrderRejected(reason=TradingHalted)} — none silently dropped, none filled.
 *
 * <p><b>Defect class:</b> kill-switch propagation correctness.
 *
 * <p><b>Status:</b> SKELETON. Disabled pending the implementation strategy. This reproducer needs a
 * real MediaDriver topology plus the APP-152 kill-switch command to be implemented in the cluster
 * service. The failure-injection primitive is: fire N NOS commands and then race the kill-switch
 * command so it arrives at the cluster leader while at least one NOS is still being processed. The
 * assertion must confirm that every NOS whose offer() preceded the halt command's position in the
 * cluster log produces an {@code OrderRejectedEvent(reason=TradingHalted)} — no silent drop, no
 * fill, no {@code OrderCreatedEvent} for any post-halt command.
 *
 * <p><b>Threading:</b> single-threaded JUnit test method; the harness it eventually drives is
 * multi-process (real MediaDriver + cluster + gateway).
 *
 * <p><b>Allocation:</b> test path; allocation acceptable.
 */
@Tag("repro-d7")
@Disabled("APP-225 §D7 skeleton — pending harness + APP-152 kill-switch command in tree")
final class KillSwitchFlowReproTest {

  @Test
  void killSwitchTriggeredMidFlight_reproducesDefect_allInFlightNosRejectedWithTradingHalted() {
    // TODO(APP-225 §D7): implement the failure-injection harness for kill-switch propagation.
    // Steps:
    //   1. Spin up a real MediaDriver + 3-node cluster + gateway + WebSocket server.
    //   2. Establish authenticated sessions and confirm the cluster is in ACTIVE trading state.
    //   3. Submit a burst of N NOS commands (e.g., 100) via the gateway; record their ClOrdIDs.
    //   4. Immediately after the last NOS offer(), submit the TradingHalt kill-switch command
    //      (APP-152) so it races the tail of the NOS burst in the cluster log.
    //   5. Collect all outcome events (OrderCreatedEvent, OrderRejectedEvent) for all ClOrdIDs.
    //   6. Assert: every NOS whose log position is AFTER the halt command position produces
    //      exactly one OrderRejectedEvent with reason=TradingHalted.
    //   7. Assert: no NOS produces a fill (ExecutionReport with ExecType=Trade) after the halt.
    //   8. Assert: no ClOrdID is silently dropped (all N outcome events received).
    // Acceptance: zero fills and zero silent drops post-halt; all rejections carry TradingHalted.
    throw new UnsupportedOperationException(
        "APP-225 §D7 KillSwitchFlowReproTest — see class Javadoc.");
  }
}
