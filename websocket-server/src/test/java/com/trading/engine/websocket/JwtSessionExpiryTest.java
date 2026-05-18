package com.trading.engine.websocket;

import static com.trading.engine.websocket.JwtExpirySweeper.SCAN_INTERVAL_NANOS;
import static com.trading.engine.websocket.JwtExpirySweeper.WARN_LEAD_NANOS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.testsupport.clock.ControllableNanoClock;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link JwtExpirySweeper} — verifies pre-auth skip, soft-expiry warning latch,
 * hard-expiry channel close, inactive-channel skip, cadence guard, multi-session independence, and
 * the {@link WebSocketSession#expEpochNanos(long)} setter contract (latch reset + validation).
 *
 * <p><b>Threading.</b> All tests run single-threaded on the JUnit runner, matching the
 * single-threaded drain-worker contract of the sweeper.
 *
 * <p><b>Clock strategy.</b> {@link ControllableNanoClock} from {@code test-support} implements both
 * {@link org.agrona.concurrent.NanoClock} and {@link org.agrona.concurrent.EpochNanoClock} so a
 * single instance serves the {@link WebSocketSessionManager} (monotonic) and the {@link
 * JwtExpirySweeper} (epoch-nanos).
 *
 * <p><b>Metrics assertion.</b> {@link SimpleMeterRegistry} is used so counter values are readable
 * via {@code registry.get("name").counter().count()}.
 */
final class JwtSessionExpiryTest {

  /**
   * A fixed epoch-nanos anchor well above zero — chosen as 5 minutes in nanos to represent a
   * session authenticated 5 minutes after the server started. Keeps relative math readable: {@code
   * NOW_ANCHOR + WARN_LEAD_NANOS + X} places the expiry inside the warn window.
   */
  private static final long NOW_ANCHOR = TimeUnit.MINUTES.toNanos(5L);

  private SimpleMeterRegistry registry;
  private WebSocketMetrics metrics;
  private WebSocketServerConfig config;
  private ControllableNanoClock clock;
  private WebSocketSessionManager sessionManager;
  private JwtExpirySweeper sweeper;

  /** Primary channel + session — used by most tests. */
  private EmbeddedChannel channel;

  private WebSocketSession session;

  @BeforeEach
  void setUp() {
    registry = new SimpleMeterRegistry();
    metrics = new WebSocketMetrics(registry);
    // Use builder defaults — they pass all validation constraints.
    config = WebSocketServerConfig.builder().build();
    // Start the clock at a fixed epoch anchor so expiry math stays readable.
    clock = new ControllableNanoClock(NOW_ANCHOR);
    sessionManager = new WebSocketSessionManager(config, metrics, clock);
    sweeper = new JwtExpirySweeper(sessionManager, metrics, clock);
    channel = new EmbeddedChannel();
    session = sessionManager.tryRegister(channel);
  }

  @AfterEach
  void tearDown() {
    if (channel.isOpen()) {
      channel.close();
    }
    channel.finishAndReleaseAll();
  }

  // ---------------------------------------------------------------------------
  // TC 1 — pre-auth session (expEpochNanos == 0L) is skipped silently
  // ---------------------------------------------------------------------------

  /**
   * Verifies that a session whose {@code expEpochNanos} has never been set (value == 0L, the
   * pre-auth sentinel) is ignored entirely: no warning frame written, no metric incremented,
   * channel stays open.
   */
  @Test
  void scan_preAuthSession_noEffect() {
    // Session was registered but expEpochNanos was never set — it is 0L by default.
    assertEquals(0L, session.expEpochNanos(), "pre-condition: expEpochNanos must be 0L");

    sweeper.scan();

    // No warning frame emitted.
    assertFalse(
        session.expiringWarningSent(), "warning latch must not be set for pre-auth session");
    // No counter increments.
    assertEquals(
        0.0,
        registry.get("websocket.auth.expiring_soon.emitted").counter().count(),
        "authExpiringSoonEmitted must stay 0 for pre-auth session");
    assertEquals(
        0.0,
        registry.get("websocket.auth.session.expired").counter().count(),
        "authSessionExpired must stay 0 for pre-auth session");
    // Channel unaffected.
    assertTrue(channel.isOpen(), "channel must remain open for pre-auth session");
    // No outbound writes.
    assertNull(channel.readOutbound(), "no frame must be written for pre-auth session");
  }

  // ---------------------------------------------------------------------------
  // TC 2 — session well within expiry (now + 5 min) — no action
  // ---------------------------------------------------------------------------

  /**
   * Verifies that a session whose expiry is 5 minutes in the future (outside the 60-second warn
   * window) produces no warning frame and no metric increment.
   */
  @Test
  void scan_wellWithinExpiry_noEffect() {
    // Expiry is 5 minutes ahead — outside the 60-second warn lead.
    final long expEpochNanos = clock.nanoTime() + TimeUnit.MINUTES.toNanos(5L);
    session.expEpochNanos(expEpochNanos);

    sweeper.scan();

    assertFalse(session.expiringWarningSent(), "warning latch must not be set outside warn window");
    assertEquals(
        0.0,
        registry.get("websocket.auth.expiring_soon.emitted").counter().count(),
        "authExpiringSoonEmitted must stay 0 outside warn window");
    assertTrue(channel.isOpen(), "channel must remain open outside warn window");
    assertNull(channel.readOutbound(), "no frame must be written outside warn window");
  }

  // ---------------------------------------------------------------------------
  // TC 3 — first crossing of warn boundary emits one warning frame + sets latch
  // ---------------------------------------------------------------------------

  /**
   * Verifies that when the clock is exactly inside the warn window ({@code expEpochNanos = now +
   * WARN_LEAD_NANOS - 1ns} places expiry 1 ns inside the warn-lead boundary, so the condition
   * {@code now >= exp - WARN_LEAD} is satisfied) the sweeper emits exactly one {@code
   * AuthExpiringSoon} frame, sets the latch, and increments the {@code authExpiringSoonEmitted}
   * counter. A subsequent call while still inside the window must NOT emit another frame (latch
   * held).
   */
  @Test
  void scan_atWarnBoundary_emitsAuthExpiringSoonOnce() {
    // Expiry is WARN_LEAD_NANOS - 1 ns from now — 1 ns inside the warn window boundary.
    // Condition: now >= expEpochNanos - WARN_LEAD_NANOS
    //         => now >= (now + WARN_LEAD_NANOS - 1) - WARN_LEAD_NANOS = now - 1 ✓
    final long expEpochNanos = clock.nanoTime() + WARN_LEAD_NANOS - 1L;
    session.expEpochNanos(expEpochNanos);

    // First scan — must emit warning.
    sweeper.scan();

    assertTrue(session.expiringWarningSent(), "latch must be set after first warn emission");
    assertEquals(
        1.0,
        registry.get("websocket.auth.expiring_soon.emitted").counter().count(),
        "authExpiringSoonEmitted must be 1 after first warn scan");
    assertNotNull(channel.readOutbound(), "one warning frame must be written on first warn scan");
    assertTrue(channel.isOpen(), "channel must remain open during warn window");

    // Advance clock within the warn window but still before hard expiry (add 30 s).
    clock.advanceSeconds(30L);

    // Second scan — SCAN_INTERVAL_NANOS guard must be satisfied; advance just past it.
    clock.advanceNanos(SCAN_INTERVAL_NANOS);
    sweeper.scan();

    // No additional frame.
    assertEquals(
        1.0,
        registry.get("websocket.auth.expiring_soon.emitted").counter().count(),
        "authExpiringSoonEmitted must remain 1 — latch prevents repeat emission");
    assertNull(channel.readOutbound(), "no second warning frame while latch is held");
  }

  // ---------------------------------------------------------------------------
  // TC 4 — warn latch survives 100 consecutive scan() calls
  // ---------------------------------------------------------------------------

  /**
   * Verifies that calling {@link JwtExpirySweeper#scan()} 50 times, each one second apart (to
   * defeat the cadence guard) while staying inside the 60-second warn window, produces exactly ONE
   * warning frame in total — the latch prevents all subsequent emissions. 50 iterations are chosen
   * to stay safely within the 60-second warn lead without crossing the hard expiry boundary.
   *
   * <p><b>Boundary math.</b> Session expires at {@code now + WARN_LEAD_NANOS} (exactly 60 s). The
   * cadence guard is reset on iteration 1; 50 subsequent 1-second advances bring the clock to
   * {@code now + 50s}, still inside the warn window. The 51st advance would reach {@code now +
   * 51s}, still below 60s — the loop is bounded at 50 to leave a comfortable margin.
   */
  @Test
  void scan_warnLatchSurvivesAcrossManyTicks() {
    // Session expires exactly at now + WARN_LEAD_NANOS (= now + 60s).
    // The warn window starts immediately (now >= exp - WARN_LEAD ⟺ now >= now), so the first
    // scan fires, emits the warn, and sets the latch.  The loop then advances clock by 1s each
    // iteration, keeping the clock below the hard-expiry boundary for all 50 iterations
    // (50s < 60s).
    final long expEpochNanos = clock.nanoTime() + WARN_LEAD_NANOS;
    session.expEpochNanos(expEpochNanos);

    int warnFrames = 0;
    for (int i = 0; i < 50; i++) {
      // Advance by SCAN_INTERVAL_NANOS each iteration to defeat the cadence guard, then scan.
      clock.advanceNanos(SCAN_INTERVAL_NANOS);
      sweeper.scan();
      if (channel.readOutbound() != null) {
        warnFrames++;
      }
    }

    assertEquals(1, warnFrames, "exactly one warn frame must be emitted across 50 scans");
    assertEquals(
        1.0,
        registry.get("websocket.auth.expiring_soon.emitted").counter().count(),
        "authExpiringSoonEmitted counter must be exactly 1");
  }

  // ---------------------------------------------------------------------------
  // TC 5 — hard expiry closes the channel
  // ---------------------------------------------------------------------------

  /**
   * Verifies that when {@code nowNanos >= expEpochNanos} the sweeper closes the channel, increments
   * {@code authSessionExpired}, and does NOT emit a warning frame (immediate close, no
   * warn-then-close dance).
   */
  @Test
  void scan_atHardExpiry_closesChannel() {
    // Expiry in the past by 1 ns.
    final long expEpochNanos = clock.nanoTime() - 1L;
    session.expEpochNanos(expEpochNanos);

    sweeper.scan();

    assertFalse(channel.isOpen(), "channel must be closed at hard expiry");
    assertEquals(
        1.0,
        registry.get("websocket.auth.session.expired").counter().count(),
        "authSessionExpired must be 1 after hard expiry");
    // No warning frame — we go straight to close.
    assertEquals(
        0.0,
        registry.get("websocket.auth.expiring_soon.emitted").counter().count(),
        "authExpiringSoonEmitted must be 0 when hard expiry fires directly");
    assertFalse(session.expiringWarningSent(), "warning latch must remain false after hard close");
    // channel.readOutbound() would return a Netty close future frame; no SBE warning frame written.
    channel.finishAndReleaseAll();
  }

  // ---------------------------------------------------------------------------
  // TC 6 — inactive channel is skipped
  // ---------------------------------------------------------------------------

  /**
   * Verifies that a session whose channel has been closed (isActive() == false) is skipped entirely
   * — no warning frame emitted, no metric incremented.
   */
  @Test
  void scan_inactiveChannel_skipped() {
    // Put the session inside the warn window.
    final long expEpochNanos = clock.nanoTime() + WARN_LEAD_NANOS - 1L;
    session.expEpochNanos(expEpochNanos);

    // Close the channel before scanning.
    channel.close();

    sweeper.scan();

    assertFalse(session.expiringWarningSent(), "latch must not be set for inactive channel");
    assertEquals(
        0.0,
        registry.get("websocket.auth.expiring_soon.emitted").counter().count(),
        "authExpiringSoonEmitted must stay 0 for inactive channel");
    assertEquals(
        0.0,
        registry.get("websocket.auth.session.expired").counter().count(),
        "authSessionExpired must stay 0 for inactive channel");
  }

  // ---------------------------------------------------------------------------
  // TC 7 — cadence guard: 100 rapid-fire scan() calls iterate sessions only once
  // ---------------------------------------------------------------------------

  /**
   * Verifies that the 1-second cadence guard causes back-to-back {@link JwtExpirySweeper#scan()}
   * calls (clock not advanced) to be no-ops after the first full iteration. Demonstrated via a
   * session that would emit a warn frame on the first scan and then NOT emit on subsequent calls
   * that occur within the same cadence window.
   *
   * <p>The cadence guard is confirmed by: (a) the warn frame appears exactly once, and (b) the
   * {@code authExpiringSoonEmitted} counter stays at 1 across all 100 calls.
   */
  @Test
  void scan_cadenceGuard_onlyFiresOncePerSecond() {
    // Session inside the warn window.
    final long expEpochNanos = clock.nanoTime() + WARN_LEAD_NANOS - 1L;
    session.expEpochNanos(expEpochNanos);

    // First call — cadence guard passes (lastScanEpochNanos starts at 0L; now = NOW_ANCHOR > 1s).
    sweeper.scan();
    // Consume the warning frame emitted on the first scan.
    assertNotNull(channel.readOutbound(), "first scan must emit warn frame");
    assertEquals(
        1.0,
        registry.get("websocket.auth.expiring_soon.emitted").counter().count(),
        "first scan must increment authExpiringSoonEmitted to 1");

    // 99 subsequent calls with clock NOT advanced — all must be cadence-guarded no-ops.
    for (int i = 0; i < 99; i++) {
      sweeper.scan();
    }

    assertEquals(
        1.0,
        registry.get("websocket.auth.expiring_soon.emitted").counter().count(),
        "authExpiringSoonEmitted must remain 1 while cadence guard suppresses scans");
    assertNull(
        channel.readOutbound(), "no additional frames must be written while cadence guard holds");

    // Advance past SCAN_INTERVAL_NANOS; next scan must iterate again.
    clock.advanceNanos(SCAN_INTERVAL_NANOS + 1L);
    sweeper.scan();

    // Latch is already set; no new frame but the iteration ran (counter still 1).
    assertEquals(
        1.0,
        registry.get("websocket.auth.expiring_soon.emitted").counter().count(),
        "authExpiringSoonEmitted must still be 1 — latch prevents re-emission after cadence reset");
    assertNull(channel.readOutbound(), "no second warning frame — latch is still held");
  }

  // ---------------------------------------------------------------------------
  // TC 8 — two sessions in warn window, each gets exactly one warn frame
  // ---------------------------------------------------------------------------

  /**
   * Verifies that the expiry warning latch is per-session: two {@link WebSocketSession} objects
   * that share the same expiry epoch-nanos each carry an independent latch. Clearing one session's
   * latch via {@code expEpochNanos()} does not affect the other, and {@code
   * markExpiringWarningSent()} on one does not spill into the other.
   *
   * <p><b>EmbeddedChannel collision note.</b> Netty's {@link EmbeddedChannel} uses a singleton
   * {@link io.netty.channel.embedded.EmbeddedChannelId} whose {@code equals()} returns {@code true}
   * for any other {@code EmbeddedChannelId} and whose {@code hashCode()} is always {@code 0}.
   * Registering two {@code EmbeddedChannel} instances via {@link
   * WebSocketSessionManager#tryRegister} would overwrite the first entry in the map because both
   * share the same key. To isolate the per-session latch contract from this Netty test-channel
   * limitation, two {@link WebSocketSession} objects are constructed directly (the constructor is
   * public) and driven through the sweeper-relevant API ({@code expEpochNanos}, {@code
   * markExpiringWarningSent}, {@code expiringWarningSent}) without routing through the session
   * manager. The sweeper's iteration over the manager is already covered by TC 3, TC 4, and TC 7.
   */
  @Test
  void scan_multipleSessions_independentLatches() {
    // Two sessions backed by the same EmbeddedChannel is fine here because we are testing
    // the per-session field independence, not the sweeper's map iteration.
    final long expEpochNanos = clock.nanoTime() + TimeUnit.SECONDS.toNanos(30L);

    // sessionAlpha — built on the primary channel from setUp.
    final var sessionAlpha = new WebSocketSession(channel, clock.nanoTime(), "10.0.0.1");
    sessionAlpha.expEpochNanos(expEpochNanos);

    // sessionBeta — separate WebSocketSession object on a second embedded channel.
    final var channelBeta = new EmbeddedChannel();
    try {
      final var sessionBeta = new WebSocketSession(channelBeta, clock.nanoTime(), "10.0.0.2");
      sessionBeta.expEpochNanos(expEpochNanos);

      // Pre-condition: both latches are clear.
      assertFalse(sessionAlpha.expiringWarningSent(), "sessionAlpha latch must start clear");
      assertFalse(sessionBeta.expiringWarningSent(), "sessionBeta latch must start clear");

      // Mark only sessionAlpha's latch.
      sessionAlpha.markExpiringWarningSent();

      assertTrue(sessionAlpha.expiringWarningSent(), "sessionAlpha latch must be set after mark");
      assertFalse(
          sessionBeta.expiringWarningSent(),
          "sessionBeta latch must remain clear — latches are independent");

      // Mark only sessionBeta's latch.
      sessionBeta.markExpiringWarningSent();

      assertTrue(sessionBeta.expiringWarningSent(), "sessionBeta latch must be set after mark");
      assertTrue(
          sessionAlpha.expiringWarningSent(),
          "sessionAlpha latch must still be set — unaffected by sessionBeta mark");

      // Renewing sessionAlpha's token via expEpochNanos() resets its latch without touching B.
      final long newExpiry = clock.nanoTime() + TimeUnit.HOURS.toNanos(1L);
      sessionAlpha.expEpochNanos(newExpiry);

      assertFalse(
          sessionAlpha.expiringWarningSent(), "sessionAlpha latch must reset on expEpochNanos()");
      assertTrue(
          sessionBeta.expiringWarningSent(),
          "sessionBeta latch must remain set — unaffected by sessionAlpha reset");

      // Verify sweeper emits exactly one warn frame for the single session registered in the
      // manager (from setUp) when it is inside the warn window.  Two-session iteration via the
      // manager is constrained by the EmbeddedChannelId singleton; the latch independence above
      // is the authoritative per-session contract assertion.
      session.expEpochNanos(expEpochNanos); // put setUp's session in the warn window
      sweeper.scan();

      assertEquals(
          1.0,
          registry.get("websocket.auth.expiring_soon.emitted").counter().count(),
          "sweeper must emit exactly one warn frame for the single registered session");
      assertNotNull(channel.readOutbound(), "setUp channel must have received one warn frame");
      assertNull(channel.readOutbound(), "setUp channel must have no second warn frame");

    } finally {
      if (channelBeta.isOpen()) {
        channelBeta.close();
      }
      channelBeta.finishAndReleaseAll();
    }
  }

  // ---------------------------------------------------------------------------
  // TC 9 — expEpochNanos setter resets the warning latch (re-auth re-arms window)
  // ---------------------------------------------------------------------------

  /**
   * Verifies that calling {@link WebSocketSession#expEpochNanos(long)} after the warning has been
   * latched resets {@link WebSocketSession#expiringWarningSent()} to {@code false}, so a renewed
   * token gets a fresh warn window.
   */
  @Test
  void expEpochNanosSetter_resetsWarningLatch() {
    // Set expiry and mark the latch as if a prior warn was sent.
    final long firstExpiry = clock.nanoTime() + TimeUnit.MINUTES.toNanos(1L);
    session.expEpochNanos(firstExpiry);
    session.markExpiringWarningSent();
    assertTrue(session.expiringWarningSent(), "pre-condition: latch must be set");

    // Re-auth: new token with fresh expiry.
    final long newExpiry = clock.nanoTime() + TimeUnit.HOURS.toNanos(1L);
    session.expEpochNanos(newExpiry);

    assertFalse(
        session.expiringWarningSent(),
        "warning latch must be reset when expEpochNanos is updated (re-auth re-arms warn window)");
    assertEquals(newExpiry, session.expEpochNanos(), "expEpochNanos must be updated to new value");
  }

  // ---------------------------------------------------------------------------
  // TC 10 — expEpochNanos setter rejects zero and negative values
  // ---------------------------------------------------------------------------

  /**
   * Verifies that {@link WebSocketSession#expEpochNanos(long)} throws {@link
   * IllegalArgumentException} for both zero and negative values, enforcing the contract that only
   * positive epoch-nanos are valid (RFC 7519 {@code exp} is always in the future relative to
   * issuance; zero is the pre-auth sentinel and must not be written by auth code).
   */
  @Test
  void expEpochNanosSetter_rejectsZeroOrNegative() {
    assertThrows(
        IllegalArgumentException.class,
        () -> session.expEpochNanos(0L),
        "expEpochNanos(0L) must throw IllegalArgumentException");

    assertThrows(
        IllegalArgumentException.class,
        () -> session.expEpochNanos(-1L),
        "expEpochNanos(-1L) must throw IllegalArgumentException");

    assertThrows(
        IllegalArgumentException.class,
        () -> session.expEpochNanos(Long.MIN_VALUE),
        "expEpochNanos(Long.MIN_VALUE) must throw IllegalArgumentException");

    // Sentinel value must remain 0L (no partial write from rejected calls).
    assertEquals(0L, session.expEpochNanos(), "expEpochNanos must remain 0L after rejected writes");
  }

  // ---------------------------------------------------------------------------
  // Helper — readable null assertion (avoids raw assertEquals(null, ...))
  // ---------------------------------------------------------------------------

  private static void assertNull(final Object obj, final String message) {
    assertEquals(null, obj, message);
  }

  private static void assertNotNull(final Object obj, final String message) {
    assertTrue(obj != null, message);
  }
}
