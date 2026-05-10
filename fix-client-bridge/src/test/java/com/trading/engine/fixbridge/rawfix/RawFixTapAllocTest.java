package com.trading.engine.fixbridge.rawfix;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.management.ThreadMXBean;
import com.trading.engine.fixbridge.audit.AuditLogger;
import com.trading.engine.fixbridge.json.BrowserEvent;
import com.trading.engine.fixbridge.quote.SessionId;
import com.trading.engine.fixbridge.ratelimit.PerTypeRateLimiter;
import com.trading.engine.fixbridge.transport.BridgeSession;
import com.trading.engine.fixbridge.transport.OutboundQueue;
import com.trading.engine.websocket.JwtValidator.ValidatedClaims;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.agrona.concurrent.EpochNanoClock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Allocation regression tripwire for {@link RawFixTap#tap}.
 *
 * <p>Three regimes covered:
 *
 * <ul>
 *   <li><b>Steady-state hot path (bridgeDebug=true + audit_view role + bucket has tokens).</b> Each
 *       tap allocates EXACTLY two objects: one {@code byte[maskedLen]} (per-event copy of the
 *       masked bytes — required by the queue's per-event ownership invariant; see {@link RawFixTap}
 *       class Javadoc) plus one {@link BrowserEvent.RawFixSlice} record. The test asserts the
 *       per-tap byte count is <b>bounded</b> at the expected upper limit (record header + array
 *       header + payload bytes) — anything materially larger means the hot path has regressed (e.g.
 *       an accidental {@code String} allocation, a defensive copy, or a capturing lambda).
 *   <li><b>Disabled path (bridgeDebug=false).</b> The {@link RawFixTap.DropCounter#NOOP} is bound;
 *       the call returns at gate 1 without touching the mask scratch or the queue. Asserts zero
 *       GC-count delta over 100_000 iterations.
 *   <li><b>Rate-limit-rejected path.</b> Bucket exhausted; the call returns at gate 4. Asserts zero
 *       GC-count delta over 100_000 iterations (the {@link RawFixRateLimiter} hot path is itself
 *       zero-alloc per its own tripwire test).
 * </ul>
 *
 * <p>Gated by {@code -DrunAllocTests=true} so the regular {@code test} task skips it.
 *
 * <p><b>Threading.</b> Single-threaded — {@link RawFixTap} is not thread-safe per its contract; the
 * test owns the instance exclusively.
 *
 * <p><b>Platform note.</b> The steady-state test uses {@link ThreadMXBean#getThreadAllocatedBytes}
 * (HotSpot-specific) to measure per-tap bytes precisely. The disabled and rate-limited tests use
 * the GC-count delta pattern (matches the rest of the {@code *AllocTest} family in this module).
 */
@EnabledIfSystemProperty(named = "runAllocTests", matches = "true")
final class RawFixTapAllocTest {

  private static final int WARMUP_ITERATIONS = 5_000;
  private static final int MEASURED_ITERATIONS = 100_000;

  private static final String AUDIT_ROLE = "audit_view";

  /** Stable FIX message body — Account tag 1 is masked; tags 11/55 are preserved. */
  private static final byte[] SAMPLE_FIX =
      "8=FIX.4.4|35=D|49=BRIDGE|56=EXCH|1=SECRET|11=ORD-001|55=EUR/USD|"
          .getBytes(StandardCharsets.US_ASCII);

  /**
   * Per-tap allocation upper bound for the hot path. Two objects per tap:
   *
   * <ul>
   *   <li>{@code byte[maskedLen]} — array header (~16 bytes) + {@code SAMPLE_FIX.length} payload
   *       bytes (= 64 here) → ~80 bytes.
   *   <li>{@link BrowserEvent.RawFixSlice} — record with {@code (boolean, byte[], int, int)} fields
   *       → ~32 bytes including header.
   * </ul>
   *
   * <p>Total expected ~112 bytes/tap. Budget set to 256 bytes/tap to absorb JVM-version variance in
   * object layout while still catching gross regressions (any unintended {@link String} allocation
   * alone would push this above 200 bytes; a defensive {@code Arrays.copyOf} of the full 4 KiB
   * scratch would push it above 4 KB).
   */
  private static final long PER_TAP_BUDGET_BYTES = 256L;

  /** Test-only fixed-epoch clock. */
  private static final EpochNanoClock FIXED_EPOCH_CLOCK = () -> 1_700_000_000_000_000_000L;

  // ─── Steady-state hot path: bytes-per-tap upper bound ───────────────────────

  @Test
  void tap_steadyStateHappyPath_perTapAllocationUnderBudget() throws Exception {
    final var rawBean = ManagementFactory.getThreadMXBean();
    assertTrue(
        rawBean instanceof ThreadMXBean,
        "HotSpot ThreadMXBean.getThreadAllocatedBytes is required (running on non-HotSpot JVM?)");
    final ThreadMXBean threadBean = (ThreadMXBean) rawBean;
    assertTrue(threadBean.isThreadAllocatedMemorySupported(), "Thread alloc tracking unsupported");
    if (!threadBean.isThreadAllocatedMemoryEnabled()) {
      threadBean.setThreadAllocatedMemoryEnabled(true);
    }

    // A capacity that comfortably absorbs (warmup+measured) taps without overflow — we drain
    // the queue inline after each tap to keep its size bounded so OfferResult never returns
    // anything other than ACCEPTED on the hot path.
    final var queue = new OutboundQueue(64);
    final var session = buildSession(List.of(AUDIT_ROLE), queue);
    final var tap =
        new RawFixTap(
            session,
            PiiMask.withDefaultMask(),
            new RawFixRateLimiter(0L), // bucket starts full
            AuditLogger.Noop.INSTANCE,
            RawFixTap.DropCounter.NOOP,
            AUDIT_ROLE,
            true,
            FIXED_EPOCH_CLOCK);

    long nowNs = 0L;
    // Warm-up — JIT-compile mask + offer + RawFixSlice ctor.
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      nowNs += 1_000_000L; // +1ms — credits one token at the limiter's refill rate
      tap.tap(RawFixTap.DIRECTION_IN, SAMPLE_FIX, 0, SAMPLE_FIX.length, nowNs);
      // Drain so the queue stays at size 1 — OutboundQueue.poll returns the event; we discard.
      final var polled = queue.poll();
      if (polled == null) {
        throw new AssertionError("Warmup iteration " + i + " produced no event");
      }
    }

    final long threadId = Thread.currentThread().threadId();
    final long beforeBytes = threadBean.getThreadAllocatedBytes(threadId);
    for (int i = 0; i < MEASURED_ITERATIONS; i++) {
      nowNs += 1_000_000L;
      tap.tap(RawFixTap.DIRECTION_IN, SAMPLE_FIX, 0, SAMPLE_FIX.length, nowNs);
      queue.poll();
    }
    final long afterBytes = threadBean.getThreadAllocatedBytes(threadId);

    final long deltaBytes = afterBytes - beforeBytes;
    final long perTapBytes = deltaBytes / MEASURED_ITERATIONS;

    assertTrue(
        perTapBytes <= PER_TAP_BUDGET_BYTES,
        "RawFixTap.tap per-tap allocation regression: "
            + perTapBytes
            + " bytes/tap exceeds budget "
            + PER_TAP_BUDGET_BYTES
            + " (total delta "
            + deltaBytes
            + " over "
            + MEASURED_ITERATIONS
            + " iterations)");
    // Sanity: at least one byte/tap must be allocated — if perTapBytes is zero something is
    // suspicious (the SUT is likely no-opping, perhaps because the bucket depleted silently).
    assertTrue(
        perTapBytes >= 1L,
        "Steady-state happy-path tap must allocate at least 1 byte/tap (got "
            + perTapBytes
            + " — bucket depleted?)");
  }

  // ─── Disabled path: bridgeDebug=false → DropCounter.NOOP, zero alloc ────────

  @Test
  void tap_bridgeDebugFalse_zeroAlloc() throws Exception {
    final var queue = new OutboundQueue(64);
    final var session = buildSession(List.of(AUDIT_ROLE), queue);
    final var tap =
        new RawFixTap(
            session,
            PiiMask.withDefaultMask(),
            new RawFixRateLimiter(0L),
            AuditLogger.Noop.INSTANCE,
            RawFixTap.DropCounter.NOOP,
            AUDIT_ROLE,
            false /* bridgeDebug=false → gate 1 closes */,
            FIXED_EPOCH_CLOCK);

    // Warm-up — JIT-compile the gate-1 short-circuit branch.
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      tap.tap(RawFixTap.DIRECTION_IN, SAMPLE_FIX, 0, SAMPLE_FIX.length, 0L);
    }

    final long beforeGc = totalGcCount();
    for (int i = 0; i < MEASURED_ITERATIONS; i++) {
      tap.tap(RawFixTap.DIRECTION_IN, SAMPLE_FIX, 0, SAMPLE_FIX.length, 0L);
    }
    final long afterGc = totalGcCount();

    assertEquals(0, queue.size(), "No event must reach the queue when bridgeDebug=false");
    assertEquals(
        beforeGc,
        afterGc,
        "RawFixTap.tap (bridgeDebug=false) advanced GC count " + beforeGc + "→" + afterGc);
  }

  // ─── Rate-limit-rejected path: bucket exhausted → DropCounter.NOOP, zero alloc

  @Test
  void tap_rateLimitExhausted_zeroAlloc() throws Exception {
    final var queue = new OutboundQueue(64);
    final var session = buildSession(List.of(AUDIT_ROLE), queue);
    // Burst of 1, refill negligible — consume the single token then every subsequent call fails
    // gate 4 (rate limiter).
    final var limiter = new RawFixRateLimiter(1L, 0.000001, 0L);
    limiter.tryConsume(0L); // consume the single token
    final var tap =
        new RawFixTap(
            session,
            PiiMask.withDefaultMask(),
            limiter,
            AuditLogger.Noop.INSTANCE,
            RawFixTap.DropCounter.NOOP,
            AUDIT_ROLE,
            true /* bridgeDebug=true so we reach the limiter */,
            FIXED_EPOCH_CLOCK);

    // Warm-up — JIT-compile the gate-4 rejection branch (mask is NOT applied; the limiter
    // rejects before the per-event byte[] copy).
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      tap.tap(RawFixTap.DIRECTION_IN, SAMPLE_FIX, 0, SAMPLE_FIX.length, 1L);
    }

    final long beforeGc = totalGcCount();
    for (int i = 0; i < MEASURED_ITERATIONS; i++) {
      tap.tap(RawFixTap.DIRECTION_IN, SAMPLE_FIX, 0, SAMPLE_FIX.length, 1L);
    }
    final long afterGc = totalGcCount();

    assertEquals(0, queue.size(), "No event must reach the queue when limiter rejects");
    assertEquals(
        beforeGc,
        afterGc,
        "RawFixTap.tap (rate-limited) advanced GC count " + beforeGc + "→" + afterGc);
  }

  // ---------------------------------------------------------------------------
  // Helpers.
  // ---------------------------------------------------------------------------

  private static BridgeSession buildSession(final List<String> roles, final OutboundQueue queue) {
    final var claims =
        new ValidatedClaims("user-1", "jti-1", List.of("ACME-001"), Long.MAX_VALUE, true, roles);
    return new BridgeSession(
        new SessionId("sess-1"),
        claims,
        InetAddress.getLoopbackAddress(),
        queue,
        new PerTypeRateLimiter(0L));
  }

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
