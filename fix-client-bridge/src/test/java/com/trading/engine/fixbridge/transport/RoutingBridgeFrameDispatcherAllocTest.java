package com.trading.engine.fixbridge.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.trading.engine.fixbridge.audit.AuditLogger;
import com.trading.engine.fixbridge.json.MutableParsedMessage;
import com.trading.engine.fixbridge.quote.SessionId;
import com.trading.engine.fixbridge.quote.SessionQuoteIndex;
import com.trading.engine.fixbridge.ratelimit.PerTypeRateLimiter;
import com.trading.engine.websocket.JwtValidator.ValidatedClaims;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.agrona.concurrent.EpochNanoClock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Allocation regression tripwire for {@link RoutingBridgeFrameDispatcher#dispatch}.
 *
 * <p>Dispatches 100_000 frames across all 6 message types in round-robin after JIT warm-up and
 * asserts the GC count does not advance. The audit logger is {@link AuditLogger.Noop#INSTANCE} to
 * avoid introducing allocations in the audit path.
 *
 * <p>Gated by {@code -DrunAllocTests=true} so the regular {@code test} task skips it.
 *
 * <p><b>Threading.</b> Single-threaded — {@link RoutingBridgeFrameDispatcher} is not thread-safe
 * per its contract.
 *
 * <p><b>Allocation.</b> The dispatcher is documented as zero-alloc on the hot dispatch path when
 * the audit logger is {@link AuditLogger.Noop#INSTANCE} (no String conversions needed). The {@code
 * SessionQuoteIndex.onQuoteRequest} path allocates one {@code ReqIdEntry} per unique reqId, but we
 * reuse a fixed reqId to avoid growing the index unboundedly in a tight loop.
 */
@EnabledIfSystemProperty(named = "runAllocTests", matches = "true")
final class RoutingBridgeFrameDispatcherAllocTest {

  private static final int WARMUP_ITERATIONS = 5_000;
  private static final int MEASURED_ITERATIONS = 100_000;

  /** Six message types exercised in rotation. */
  private static final int[] TYPES = {
    MutableParsedMessage.TYPE_QUOTE_REQUEST,
    MutableParsedMessage.TYPE_ACCEPT_QUOTE,
    MutableParsedMessage.TYPE_REJECT_QUOTE,
    MutableParsedMessage.TYPE_NEW_ORDER_SINGLE,
    MutableParsedMessage.TYPE_CANCEL_ORDER,
    MutableParsedMessage.TYPE_ORDER_STATUS_REQUEST
  };

  @Test
  void dispatch_allSixMessageTypes_roundRobin_zeroAlloc() {
    final var sink = FixCommandSink.NOOP;
    final var quoteIndex = new SessionQuoteIndex();
    final EpochNanoClock fixedEpochClock = () -> 1_700_000_000_000_000_000L;
    final var dispatcher =
        new RoutingBridgeFrameDispatcher(
            sink, quoteIndex, AuditLogger.Noop.INSTANCE, fixedEpochClock, "127.0.0.1");

    final var claims =
        new ValidatedClaims("user-001", "jti-001", List.of(), Long.MAX_VALUE, true, List.of());
    final var session =
        new BridgeSession(
            new SessionId("sess-alloc"),
            claims,
            InetAddress.getLoopbackAddress(),
            new OutboundQueue(16),
            new PerTypeRateLimiter(0L));

    // Pre-populate a fixed reqId in the parsed flyweight so every QuoteRequest re-uses the same
    // reqId (accepted as a duplicate after the first → no new index allocation).
    final var parsed = new MutableParsedMessage();
    final byte[] reqIdBytes = "R-ALLOC".getBytes(StandardCharsets.US_ASCII);
    System.arraycopy(reqIdBytes, 0, parsed.scratch, 0, reqIdBytes.length);
    parsed.reqIdOff = 0;
    parsed.reqIdLen = reqIdBytes.length;

    // Warmup — register the reqId once so subsequent calls hit the duplicate path.
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      final int type = TYPES[i % TYPES.length];
      dispatcher.dispatch(session, parsed, type, (long) i);
    }

    final long beforeGc = totalGcCount();
    for (int i = 0; i < MEASURED_ITERATIONS; i++) {
      final int type = TYPES[i % TYPES.length];
      dispatcher.dispatch(session, parsed, type, (long) i);
    }
    final long afterGc = totalGcCount();

    assertEquals(
        beforeGc,
        afterGc,
        "RoutingBridgeFrameDispatcher.dispatch advanced GC count " + beforeGc + "→" + afterGc);
  }

  // ---------------------------------------------------------------------------
  // GC count helper.
  // ---------------------------------------------------------------------------

  private static long totalGcCount() {
    long total = 0L;
    final var beans = ManagementFactory.getGarbageCollectorMXBeans();
    for (final var bean : beans) {
      final long c = bean.getCollectionCount();
      if (c >= 0L) {
        total += c;
      }
    }
    return total;
  }
}
