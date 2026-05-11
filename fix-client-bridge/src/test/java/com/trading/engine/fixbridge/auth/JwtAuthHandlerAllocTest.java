package com.trading.engine.fixbridge.auth;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.management.ThreadMXBean;
import com.trading.engine.fixbridge.FixClientBridgeConfig;
import com.trading.engine.fixbridge.audit.AuditLogger;
import com.trading.engine.fixbridge.json.BrowserEventWriter;
import com.trading.engine.fixbridge.translator.DecimalStringEmitter;
import com.trading.engine.fixbridge.transport.AccountLimitsSource;
import com.trading.engine.fixbridge.transport.BridgeFrameDispatcher;
import com.trading.engine.fixbridge.transport.BridgeSession;
import com.trading.engine.websocket.AuthFailureTracker;
import com.trading.engine.websocket.JwtValidator;
import com.trading.engine.websocket.TestJwtFixture;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SystemNanoClock;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Allocation regression tripwire for {@link JwtAuthHandler#channelRead0}.
 *
 * <p><b>Cold path with bounded budget — NOT a strict zero-alloc tripwire.</b> Authentication is a
 * one-shot per-channel handshake, so a strict zero-alloc assertion would be wrong: each auth
 * necessarily allocates a {@link UUID}-derived {@code SessionId}, an {@code OutboundQueue} ring
 * buffer, a {@link com.trading.engine.fixbridge.ratelimit.PerTypeRateLimiter}, a {@link
 * BridgeSession}, an {@link com.trading.engine.fixbridge.transport.InboundReadGate}, a {@link
 * com.trading.engine.fixbridge.transport.WsListener}, an {@link
 * com.trading.engine.fixbridge.transport.OutboundDrainer}, plus {@link AuditLogger#record}
 * parameter strings.
 *
 * <p>Instead this test pins an UPPER BOUND on per-auth allocation using {@link ThreadMXBean
 * #getThreadAllocatedBytes(long)} so a regression that quietly bloats the auth path (e.g. a new
 * per-auth {@link java.util.HashMap} or a defensive copy of a JWT claims set) trips the bound and
 * fails the build.
 *
 * <p><b>Budget rationale.</b> The current per-auth allocation footprint, measured locally on JDK 25
 * with the in-memory JWKS validator and {@code Runnable::run} executor, is ~660 KB per iteration.
 * The dominant contributors are:
 *
 * <ul>
 *   <li>Nimbus {@code SignedJWT.parse} + RS256 signature verification — by far the largest line
 *       item; allocates BigInteger temporaries, ASN.1 decoding state, JWS header copies, and
 *       claim-set defensive copies on every parse. Confirmed via JFR allocation profiling on
 *       previous runs.
 *   <li>The TEST DRIVER's {@code fixture.mintValidJwt()} also signs a fresh RS256 JWT each
 *       iteration — second-largest contributor.
 *   <li>{@code MutableParsedMessage.scratch} — 64 KiB per-channel scratch buffer.
 *   <li>{@code OutboundQueue} ring buffer — {@code Object[capacity]}, capacity 64 = ~528 bytes.
 *   <li>{@code SessionId} (UUID + String), {@code BridgeSession}, {@code WsListener}, {@code
 *       OutboundDrainer}, {@code InboundReadGate} — ~1 KB combined.
 * </ul>
 *
 * <p>The 1 MB (1_048_576 byte) ceiling leaves ~50% headroom over the local baseline so JIT inlining
 * variance and minor Nimbus version drift do not trigger false failures, while still catching gross
 * regressions (e.g. a 2x per-auth blow-up from a new defensive copy of the full claims set or a
 * logging round-trip that materialises every claim).
 *
 * <p>Gated by {@code -DrunAllocTests=true} so the regular {@code test} task skips it.
 *
 * <p><b>Threading.</b> Single-threaded — {@link EmbeddedChannel} is synchronous; the validation
 * executor is {@code Runnable::run} so the JWT validation completes inline.
 *
 * <p><b>Platform note.</b> {@link com.sun.management.ThreadMXBean#getThreadAllocatedBytes} is a
 * HotSpot-specific API. The test fails fast with a clear message if the running JVM does not
 * support it (non-HotSpot) — production deployments and CI run on HotSpot/OpenJDK.
 */
@EnabledIfSystemProperty(named = "runAllocTests", matches = "true")
final class JwtAuthHandlerAllocTest {

  private static final int WARMUP_ITERATIONS = 50;
  private static final int MEASURED_ITERATIONS = 200;

  /**
   * Per-auth allocation upper bound in bytes. Set to 1 MiB — ~50% headroom over the locally
   * measured ~660 KB baseline (Nimbus RS256 parse + verify + the test-driver's per-iteration
   * mintValidJwt sign dominate). The assertion is on the AVERAGE across measured iterations so an
   * outlier auth (e.g. one Nimbus parse that triggers a fresh codec init on first use) does not
   * falsely fail the test.
   */
  private static final long PER_AUTH_BUDGET_BYTES = 1024L * 1024L;

  private static TestJwtFixture fixture;
  private static JwtValidator validator;

  @BeforeAll
  static void setUpAll() throws Exception {
    fixture = new TestJwtFixture();
    validator = fixture.buildValidator();
  }

  @Test
  void channelRead0_validJwt_perAuthAllocationUnderBudget() throws Exception {
    final var rawBean = ManagementFactory.getThreadMXBean();
    assertTrue(
        rawBean instanceof ThreadMXBean,
        "HotSpot ThreadMXBean.getThreadAllocatedBytes is required (running on non-HotSpot JVM?)");
    final ThreadMXBean threadBean = (ThreadMXBean) rawBean;
    assertTrue(
        threadBean.isThreadAllocatedMemorySupported(),
        "JVM does not support thread allocated memory tracking");
    if (!threadBean.isThreadAllocatedMemoryEnabled()) {
      threadBean.setThreadAllocatedMemoryEnabled(true);
    }

    // Warm-up — let the JIT compile parse + validate + completeAuthOnEventLoop, and let Nimbus
    // initialise its lazy codecs (BouncyCastle providers, etc.) before measuring.
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      runOneAuth();
    }

    final long threadId = Thread.currentThread().threadId();
    final long beforeBytes = threadBean.getThreadAllocatedBytes(threadId);
    for (int i = 0; i < MEASURED_ITERATIONS; i++) {
      runOneAuth();
    }
    final long afterBytes = threadBean.getThreadAllocatedBytes(threadId);

    final long deltaBytes = afterBytes - beforeBytes;
    final long perAuthBytes = deltaBytes / MEASURED_ITERATIONS;

    assertTrue(
        perAuthBytes <= PER_AUTH_BUDGET_BYTES,
        "Per-auth allocation regression: "
            + perAuthBytes
            + " bytes/auth exceeds budget "
            + PER_AUTH_BUDGET_BYTES
            + " (total delta "
            + deltaBytes
            + " over "
            + MEASURED_ITERATIONS
            + " iterations)");
  }

  /**
   * Run a single auth handshake end-to-end: build the handler, build the channel, mint a fresh JWT,
   * write the auth frame, verify the session was installed, then release the channel.
   *
   * <p>A fresh JWT is minted per iteration because UUID minting + RS256 signing dominate the
   * allocation footprint anyway and re-using a stale token would only test a degenerate
   * already-validated cache hit (not the production path).
   */
  private static void runOneAuth() throws Exception {
    final var jtiCache = new JtiRevocationCache();
    final var handler =
        new JwtAuthHandler(
            buildConfig(),
            validator,
            jtiCache,
            noLockoutTracker(),
            nowEpochClock(),
            systemNanoClock(),
            Runnable::run, // synchronous executor — validation completes inline
            BridgeFrameDispatcher.Factory.NOOP_FACTORY,
            AuditLogger.Noop.INSTANCE,
            new BrowserEventWriter(new DecimalStringEmitter()),
            AccountLimitsSource.NOOP,
            DpopValidator.NOOP);
    final var channel = new EmbeddedChannel(handler);

    final var token = fixture.mintValidJwt();
    final var frame =
        new TextWebSocketFrame(
            Unpooled.copiedBuffer(
                "{\"type\":\"Auth\",\"token\":\"" + token + "\"}", StandardCharsets.UTF_8));
    channel.writeInbound(frame);

    final var session = channel.attr(BridgeSession.ATTRIBUTE_KEY).get();
    assertNotNull(session, "Auth must succeed each iteration");

    // Drain any outbound (AccountLimits frames may have been emitted by the drainer schedule).
    Object out;
    while ((out = channel.readOutbound()) != null) {
      if (out instanceof io.netty.util.ReferenceCounted rc) {
        rc.release();
      }
    }

    channel.finishAndReleaseAll();
  }

  // ─── Fixture helpers (mirrors JwtAuthHandlerTest) ────────────────────────────

  private static FixClientBridgeConfig buildConfig() {
    return new FixClientBridgeConfig(
        8444,
        "127.0.0.1",
        "localhost",
        19880,
        "EXCH",
        "BRIDGE",
        "logs/sess",
        false,
        256,
        65536,
        64,
        64,
        30,
        15,
        5000L,
        5,
        Map.of(),
        TestJwtFixture.AUDIENCE,
        false,
        32,
        10,
        600,
        10,
        List.of(),
        "audit_view",
        5,
        60,
        null,
        null,
        true);
  }

  /**
   * Fixed epoch clock so per-iteration allocation stays deterministic — wall-time drift across
   * iterations would add measurement noise to the budget assertion (CodeRabbit PR #71 R2 fix for
   * the prior {@code System.currentTimeMillis()} call).
   */
  private static EpochNanoClock nowEpochClock() {
    return () -> 1_700_000_000_000_000_000L;
  }

  /**
   * Project-mandated monotonic clock per CLAUDE.md §Clock Usage. Replaces the prior bare {@code
   * System::nanoTime} method-reference (CodeRabbit PR #71 R2 fix).
   */
  private static NanoClock systemNanoClock() {
    return SystemNanoClock.INSTANCE;
  }

  private static AuthFailureTracker noLockoutTracker() {
    return new AuthFailureTracker(1000, 60, SystemNanoClock.INSTANCE);
  }
}
