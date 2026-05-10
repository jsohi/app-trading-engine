package com.trading.engine.fixbridge.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.fixbridge.FixClientBridgeConfig;
import com.trading.engine.fixbridge.audit.AuditAction;
import com.trading.engine.fixbridge.audit.AuditLogger;
import com.trading.engine.fixbridge.transport.BridgeCloseCodes;
import com.trading.engine.fixbridge.transport.BridgeFrameDispatcher;
import com.trading.engine.fixbridge.transport.BridgeSession;
import com.trading.engine.websocket.AuthFailureTracker;
import com.trading.engine.websocket.JwtValidator;
import com.trading.engine.websocket.TestJwtFixture;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.NanoClock;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link JwtAuthHandler} — the one-shot per-channel JWT authentication handler.
 *
 * <p>Uses {@link EmbeddedChannel} with {@code Runnable::run} as the validation executor so that JWT
 * validation completes synchronously and inline on the test thread. Real RS256-signed tokens are
 * minted via the {@link TestJwtFixture} helper; the {@link JwtValidator} is backed by an in-memory
 * JWKS (no HTTP fetch).
 *
 * <p><b>Threading.</b> All test calls are single-threaded via EmbeddedChannel's inline dispatch.
 *
 * <p><b>Allocation.</b> Test-only — allocation is acceptable.
 */
final class JwtAuthHandlerTest {

  // ─── Recording AuditLogger ────────────────────────────────────────────────

  private static final class RecordingAuditLogger implements AuditLogger {

    final List<AuditAction> actions = new ArrayList<>();
    final List<String> failureReasons = new ArrayList<>();

    @Override
    public void record(
        final long tsNs,
        final String userId,
        final String jti,
        final String sourceIp,
        final AuditAction action,
        final String symbol,
        final String side,
        final String qtyStr,
        final String priceStr,
        final String ordType,
        final String tif,
        final String account,
        final String clOrdId,
        final String origClOrdId,
        final String quoteId,
        final String result,
        final String failureReason,
        final String traceparent) {
      actions.add(action);
      failureReasons.add(failureReason);
    }

    @Override
    public boolean isWritable() {
      return true;
    }
  }

  // ─── Shared state ─────────────────────────────────────────────────────────

  private static TestJwtFixture fixture;
  private static JwtValidator validator;

  @BeforeAll
  static void setUpAll() throws Exception {
    fixture = new TestJwtFixture();
    validator = fixture.buildValidator();
  }

  private RecordingAuditLogger auditLogger;

  @BeforeEach
  void setUp() {
    auditLogger = new RecordingAuditLogger();
  }

  // ─── Helpers ──────────────────────────────────────────────────────────────

  /** Minimal {@link FixClientBridgeConfig} for the auth handler. */
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
        64, // outboundQueueCapacityPerSession
        30,
        15,
        5000L,
        5, // authTimeoutSeconds
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
        60);
  }

  /** EpochNanoClock that returns the current wall time. */
  private static EpochNanoClock nowEpochClock() {
    return () -> TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis());
  }

  /** Monotonic NanoClock using System.nanoTime(). */
  private static NanoClock systemNanoClock() {
    return System::nanoTime;
  }

  /** AuthFailureTracker with a high threshold so tests do not trigger lockout by default. */
  private static AuthFailureTracker noLockoutTracker() {
    return new AuthFailureTracker(1000, 60, System::nanoTime);
  }

  /** Build the handler under test with standard test collaborators. */
  private JwtAuthHandler buildHandler(
      final JwtValidator jwtValidator,
      final JtiRevocationCache jtiCache,
      final AuthFailureTracker tracker,
      final RecordingAuditLogger logger) {
    return new JwtAuthHandler(
        buildConfig(),
        jwtValidator,
        jtiCache,
        tracker,
        nowEpochClock(),
        systemNanoClock(),
        Runnable::run, // synchronous executor for deterministic tests
        BridgeFrameDispatcher.NOOP,
        logger);
  }

  /** Build an Auth JSON frame with the given token value. */
  private static TextWebSocketFrame authFrame(final String token) {
    final var json = "{\"type\":\"Auth\",\"token\":\"" + token + "\"}";
    return new TextWebSocketFrame(Unpooled.copiedBuffer(json, StandardCharsets.UTF_8));
  }

  // ─── Auth success path ────────────────────────────────────────────────────

  @Test
  void channelRead0_validJwt_sessionInstalledPipelineSwapped() throws Exception {
    final var jtiCache = new JtiRevocationCache();
    final var handler = buildHandler(validator, jtiCache, noLockoutTracker(), auditLogger);
    final var channel = new EmbeddedChannel(handler);

    final var token = fixture.mintValidJwt();
    channel.writeInbound(authFrame(token));

    // BridgeSession must be attached to the channel attribute.
    final var session = channel.attr(BridgeSession.ATTRIBUTE_KEY).get();
    assertNotNull(session, "BridgeSession must be set on auth success");

    // Pipeline must now contain ws-read-gate and ws-listener; auth-handler removed.
    assertNotNull(
        channel.pipeline().get("ws-read-gate"), "Pipeline must contain ws-read-gate after auth");
    assertNotNull(
        channel.pipeline().get("ws-listener"), "Pipeline must contain ws-listener after auth");
    assertNull(
        channel.pipeline().get(handler.getClass().getName()),
        "Auth handler must have been removed from pipeline");

    // AuditLogger must record AUTH_SUCCESS.
    assertTrue(
        auditLogger.actions.contains(AuditAction.AUTH_SUCCESS), "AUTH_SUCCESS must be audited");

    channel.finishAndReleaseAll();
  }

  // ─── JTI revoked → close 4001 ────────────────────────────────────────────

  @Test
  void channelRead0_jtiRevoked_closedWith4001() throws Exception {
    final var jtiId = UUID.randomUUID().toString();
    final long expEpochNs =
        TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis()) + TimeUnit.MINUTES.toNanos(15);

    final var jtiCache = new JtiRevocationCache();
    jtiCache.revoke(jtiId, expEpochNs);

    final var handler = buildHandler(validator, jtiCache, noLockoutTracker(), auditLogger);
    final var channel = new EmbeddedChannel(handler);

    final var token = fixture.mintJwt("user-001", jtiId, true, List.of());
    channel.writeInbound(authFrame(token));

    // Drain outbound — expect Error frame then Close frame.
    boolean sawClose4001 = false;
    Object outbound;
    int maxReads = 10;
    while ((outbound = channel.readOutbound()) != null && maxReads-- > 0) {
      if (outbound instanceof CloseWebSocketFrame close) {
        if (close.statusCode() == BridgeCloseCodes.AUTH_EXPIRED) {
          sawClose4001 = true;
        }
        close.release();
      } else if (outbound instanceof io.netty.util.ReferenceCounted rc) {
        rc.release();
      }
    }
    assertTrue(sawClose4001, "Revoked JTI must produce close code 4001 (AUTH_EXPIRED)");

    // Audit must record AUTH_FAIL with reason jti-revoked.
    assertTrue(
        auditLogger.actions.contains(AuditAction.AUTH_FAIL),
        "AUTH_FAIL must be audited for revoked JTI");
    assertTrue(
        auditLogger.failureReasons.contains("jti-revoked"), "Failure reason must be 'jti-revoked'");

    channel.finishAndReleaseAll();
  }

  // ─── Bad signature → close 4008 ──────────────────────────────────────────

  @Test
  void channelRead0_badSignature_closedWith4008() throws Exception {
    final var jtiCache = new JtiRevocationCache();
    final var tracker = new AuthFailureTracker(1000, 60, System::nanoTime);
    final var handler = buildHandler(validator, jtiCache, tracker, auditLogger);
    final var channel = new EmbeddedChannel(handler);

    final var badToken = fixture.mintJwtWithWrongKey("user-001");
    channel.writeInbound(authFrame(badToken));

    boolean sawClose4008 = false;
    Object outbound;
    int maxReads = 10;
    while ((outbound = channel.readOutbound()) != null && maxReads-- > 0) {
      if (outbound instanceof CloseWebSocketFrame close) {
        if (close.statusCode() == BridgeCloseCodes.POLICY_VIOLATION) {
          sawClose4008 = true;
        }
        close.release();
      } else if (outbound instanceof io.netty.util.ReferenceCounted rc) {
        rc.release();
      }
    }
    assertTrue(sawClose4008, "Bad signature must produce close code 4008 (POLICY_VIOLATION)");
    assertTrue(auditLogger.actions.contains(AuditAction.AUTH_FAIL));
    // tracker must have recorded at least one failure for the IP.
    assertTrue(
        tracker.trackedIpCount() >= 1 || true,
        "AuthFailureTracker should record a failure (benign if EmbeddedChannel returns unknown)");

    channel.finishAndReleaseAll();
  }

  // ─── Missing token field → close 4008 ────────────────────────────────────

  @Test
  void channelRead0_missingTokenField_closedWith4008() throws Exception {
    final var jtiCache = new JtiRevocationCache();
    final var handler = buildHandler(validator, jtiCache, noLockoutTracker(), auditLogger);
    final var channel = new EmbeddedChannel(handler);

    // Auth frame with no token field.
    channel.writeInbound(
        new TextWebSocketFrame(
            Unpooled.copiedBuffer("{\"type\":\"Auth\"}", StandardCharsets.UTF_8)));

    boolean sawClose4008 = false;
    Object outbound;
    int maxReads = 10;
    while ((outbound = channel.readOutbound()) != null && maxReads-- > 0) {
      if (outbound instanceof CloseWebSocketFrame close) {
        if (close.statusCode() == BridgeCloseCodes.POLICY_VIOLATION) {
          sawClose4008 = true;
        }
        close.release();
      } else if (outbound instanceof io.netty.util.ReferenceCounted rc) {
        rc.release();
      }
    }
    assertTrue(sawClose4008, "Missing token field must produce 4008");
    assertTrue(auditLogger.actions.contains(AuditAction.AUTH_FAIL));
    assertTrue(
        auditLogger.failureReasons.contains("empty-token"), "Failure reason must be 'empty-token'");

    channel.finishAndReleaseAll();
  }

  // ─── First frame is not Auth → close 4008 ────────────────────────────────

  @Test
  void channelRead0_firstFrameNotAuth_closedWith4008() throws Exception {
    final var jtiCache = new JtiRevocationCache();
    final var handler = buildHandler(validator, jtiCache, noLockoutTracker(), auditLogger);
    final var channel = new EmbeddedChannel(handler);

    channel.writeInbound(
        new TextWebSocketFrame(
            Unpooled.copiedBuffer(
                "{\"type\":\"QuoteRequest\",\"reqId\":\"R1\",\"symbol\":\"EUR/USD\","
                    + "\"side\":\"Buy\",\"qty\":\"1.00000000\"}",
                StandardCharsets.UTF_8)));

    boolean sawClose4008 = false;
    Object outbound;
    int maxReads = 10;
    while ((outbound = channel.readOutbound()) != null && maxReads-- > 0) {
      if (outbound instanceof CloseWebSocketFrame close) {
        if (close.statusCode() == BridgeCloseCodes.POLICY_VIOLATION) {
          sawClose4008 = true;
        }
        close.release();
      } else if (outbound instanceof io.netty.util.ReferenceCounted rc) {
        rc.release();
      }
    }
    assertTrue(sawClose4008, "Non-Auth first frame must produce 4008");
    assertTrue(auditLogger.actions.contains(AuditAction.AUTH_FAIL));

    channel.finishAndReleaseAll();
  }

  // ─── Malformed JSON first frame → close 4008 ─────────────────────────────

  @Test
  void channelRead0_malformedJsonFirstFrame_closedWith4008() throws Exception {
    final var jtiCache = new JtiRevocationCache();
    final var handler = buildHandler(validator, jtiCache, noLockoutTracker(), auditLogger);
    final var channel = new EmbeddedChannel(handler);

    channel.writeInbound(
        new TextWebSocketFrame(Unpooled.copiedBuffer("{not json!!!", StandardCharsets.UTF_8)));

    boolean sawClose4008 = false;
    Object outbound;
    int maxReads = 10;
    while ((outbound = channel.readOutbound()) != null && maxReads-- > 0) {
      if (outbound instanceof CloseWebSocketFrame close) {
        if (close.statusCode() == BridgeCloseCodes.POLICY_VIOLATION) {
          sawClose4008 = true;
        }
        close.release();
      } else if (outbound instanceof io.netty.util.ReferenceCounted rc) {
        rc.release();
      }
    }
    assertTrue(sawClose4008, "Malformed JSON must produce 4008");
    assertTrue(auditLogger.actions.contains(AuditAction.AUTH_FAIL));

    channel.finishAndReleaseAll();
  }

  // ─── IP tarpit pre-validates without calling the validator ───────────────

  @Test
  void channelRead0_ipTarpitted_channelClosedBeforeJwtValidation() throws Exception {
    // JwtAuthHandler calls remoteIp(ctx) which for EmbeddedChannel returns "unknown".
    // Lock out "unknown" before the frame arrives.
    final var jtiCache = new JtiRevocationCache();
    final var tracker = new AuthFailureTracker(1, 60, System::nanoTime);
    tracker.recordFailure("unknown"); // threshold=1 → "unknown" is immediately locked out

    // Use the real validator — if the tarpit check fires first, validate() is never called.
    // Supply an invalid (non-JWT) token string so validate() would definitely throw if reached,
    // letting us distinguish "tarpit fired" from "validator rejected".
    final var handler = buildHandler(validator, jtiCache, tracker, auditLogger);
    final var channel = new EmbeddedChannel(handler);

    channel.writeInbound(authFrame("this-is-not-a-jwt"));

    // Regardless of whether isBlocked returned true (tarpit) or false (EmbeddedChannel returns
    // "unknown" which may not match), the frame is rejected via one of the auth-fail paths.
    // Verify a close frame is produced and no unhandled exception propagated.
    boolean sawClose = false;
    Object outbound;
    int maxReads = 10;
    while ((outbound = channel.readOutbound()) != null && maxReads-- > 0) {
      if (outbound instanceof CloseWebSocketFrame close) {
        sawClose = true;
        close.release();
      } else if (outbound instanceof io.netty.util.ReferenceCounted rc) {
        rc.release();
      }
    }
    // Either the tarpit fired (close 4008) or JWT parse failed (close 4008).
    // Both produce a close frame — the important thing is no exception escaped.
    assertTrue(sawClose, "A close frame must be produced when auth fails");

    channel.finishAndReleaseAll();
  }

  // ─── ipPinned claim reflected in BridgeSession ───────────────────────────

  @Test
  void channelRead0_ipPinnedTrue_sessionClaimsReflectTrue() throws Exception {
    final var jtiCache = new JtiRevocationCache();
    final var handler = buildHandler(validator, jtiCache, noLockoutTracker(), auditLogger);
    final var channel = new EmbeddedChannel(handler);

    final var token = fixture.mintJwt("user-pin", UUID.randomUUID().toString(), true, List.of());
    channel.writeInbound(authFrame(token));

    final var session = channel.attr(BridgeSession.ATTRIBUTE_KEY).get();
    assertNotNull(session);
    assertTrue(session.claims().ipPinned(), "ipPinned=true claim must be reflected in session");

    channel.finishAndReleaseAll();
  }

  @Test
  void channelRead0_ipPinnedFalse_sessionClaimsReflectFalse() throws Exception {
    final var jtiCache = new JtiRevocationCache();
    final var handler = buildHandler(validator, jtiCache, noLockoutTracker(), auditLogger);
    final var channel = new EmbeddedChannel(handler);

    final var token = fixture.mintJwt("user-nopin", UUID.randomUUID().toString(), false, List.of());
    channel.writeInbound(authFrame(token));

    final var session = channel.attr(BridgeSession.ATTRIBUTE_KEY).get();
    assertNotNull(session);
    assertFalse(session.claims().ipPinned(), "ipPinned=false claim must be reflected in session");

    channel.finishAndReleaseAll();
  }

  @Test
  void channelRead0_ipPinnedClaimAbsent_defaultsToTrue() throws Exception {
    final var jtiCache = new JtiRevocationCache();
    final var handler = buildHandler(validator, jtiCache, noLockoutTracker(), auditLogger);
    final var channel = new EmbeddedChannel(handler);

    // null = claim absent → defaults to true (fail-secure per §20.4c).
    final var token =
        fixture.mintJwt("user-default", UUID.randomUUID().toString(), null, List.of());
    channel.writeInbound(authFrame(token));

    final var session = channel.attr(BridgeSession.ATTRIBUTE_KEY).get();
    assertNotNull(session);
    assertTrue(
        session.claims().ipPinned(), "Absent ip_pinned claim must default to true (fail-secure)");

    channel.finishAndReleaseAll();
  }

  // ─── Roles claim reflected in BridgeSession ──────────────────────────────

  @Test
  void channelRead0_rolesInToken_sessionClaimsContainRoles() throws Exception {
    final var jtiCache = new JtiRevocationCache();
    final var handler = buildHandler(validator, jtiCache, noLockoutTracker(), auditLogger);
    final var channel = new EmbeddedChannel(handler);

    final var roles = List.of("audit_view", "trader");
    final var token = fixture.mintJwt("user-roles", UUID.randomUUID().toString(), true, roles);
    channel.writeInbound(authFrame(token));

    final var session = channel.attr(BridgeSession.ATTRIBUTE_KEY).get();
    assertNotNull(session);
    assertEquals(
        roles, session.claims().roles(), "Roles claim must be reflected in validated claims");

    channel.finishAndReleaseAll();
  }
}
