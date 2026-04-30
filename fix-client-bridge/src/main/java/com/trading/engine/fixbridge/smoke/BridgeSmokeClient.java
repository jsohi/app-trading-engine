package com.trading.engine.fixbridge.smoke;

/**
 * Stub placeholder for the bridge smoke client referenced as the {@code installDist} {@code
 * mainClass} in {@code fix-client-bridge/build.gradle.kts}.
 *
 * <p><b>Purpose.</b> The real smoke client (a Netty WebSocket client that opens a single connection
 * to the bridge, sends a dev-token Auth + a {@code QuoteRequest}, and asserts the round-trip Quote
 * arrives) is wired in APP-39 Phase 11 alongside the {@code scripts/e2e.sh} extension. Until then,
 * this stub exists solely to make {@code installDist} / {@code distZip} / {@code run} resolve a
 * concrete main class without producing a {@code ClassNotFoundException} at distribution time.
 *
 * <p><b>Threading.</b> N/A — single-method static entry point.
 *
 * <p><b>Allocation.</b> N/A — never reached on a green path.
 *
 * <p><b>Lifecycle.</b> Replaced by the real implementation in Phase 11. Operators who attempt to
 * run the distribution before Phase 11 lands receive a clear {@link UnsupportedOperationException}
 * naming the responsible ticket.
 *
 * <p><b>Dependencies.</b> None.
 */
public final class BridgeSmokeClient {

  private BridgeSmokeClient() {
    throw new AssertionError("BridgeSmokeClient is not instantiable");
  }

  /**
   * Stub entry point. Throws to surface the "smoke client not yet implemented" condition explicitly
   * rather than silently exiting on a no-op.
   *
   * @param args ignored
   * @throws UnsupportedOperationException always — the smoke client lands in APP-39 Phase 11
   */
  public static void main(final String[] args) {
    // TODO(APP-39): replace stub with smoke-client implementation in Phase 11 (Netty WS client +
    //   dev-token Auth + QuoteRequest round-trip; consumed by scripts/e2e.sh).
    throw new UnsupportedOperationException(
        "BridgeSmokeClient is a Phase 11 placeholder — see TODO(APP-39) for tracking");
  }
}
