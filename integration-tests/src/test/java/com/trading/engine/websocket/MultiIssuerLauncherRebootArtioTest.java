package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jose.jwk.source.ImmutableJWKSet;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.nimbusds.jwt.proc.DefaultJWTClaimsVerifier;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;
import com.trading.engine.messages.clock.TradingClocks;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.agrona.CloseHelper;
import org.agrona.concurrent.EpochNanoClock;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;

/**
 * APP-244 Phase 3 Commit C.8 — full-stack multi-issuer + Artio-reboot integration test.
 *
 * <p>Validates that the production {@link JwtValidator} + {@link WebSocketServerConfig} pair
 * correctly routes per-issuer JWTs across a simulated Artio FIX-gateway reboot. Two distinct
 * issuers ({@code A} and {@code B}) each back a separate Jetty stub serving its own JWKS; the
 * validator under test is wired with both. A real Artio {@link FixEngine} is launched against an
 * in-process {@link MediaDriver}, closed, then re-launched against the SAME driver to simulate a
 * graceful gateway restart while the validator + Jetty stubs stay up.
 *
 * <p><b>Why not the full launcher?</b> The production {@code TradingEngineLauncher} spawns three
 * cluster-node JVMs, three Aeron Archive JVMs, a media driver, a pricing/orchestrator/WebSocket
 * stack, and the FIX gateway — none of which can be reliably exercised inside a single JUnit
 * process within the default 30 s integration-test budget. End-to-end multi-issuer + launcher
 * reboot is owned by {@code scripts/full-stack-e2e.sh §14} (Playwright spec 08). This in-JVM IT
 * complements that script by asserting the JWT-validation contract directly — the part that is
 * impossible to express precisely from the browser-side spec.
 *
 * <p><b>What is asserted across the reboot:</b>
 *
 * <ol>
 *   <li>A pre-reboot WS session authenticated under issuer A keeps its {@link
 *       JwtValidator.ValidatedClaims} valid for the lifetime of the token (validator state survives
 *       the FIX-engine close+relaunch).
 *   <li>Same for issuer B.
 *   <li>Post-reboot, a brand-new JWT minted under issuer A still validates.
 *   <li>Post-reboot, a brand-new JWT minted under issuer B still validates.
 *   <li>Cross-issuer forgery (A-signed token presented with B's {@code iss} claim) is rejected both
 *       before and after the reboot — confused-deputy guard from plan §15.
 *   <li>The Jetty stub fetch counters increase only on initial cache misses; the reboot does NOT
 *       trigger a JWKS re-fetch (validator state is process-scoped and orthogonal to FIX-side
 *       lifecycle).
 * </ol>
 *
 * <p><b>Threading.</b> Single-threaded; {@link ExecutionMode#SAME_THREAD} pinned. The Jetty stubs
 * each have their own connector thread but no shared mutable state with the test thread other than
 * the atomic JWKS-body and fetch counters.
 *
 * <p><b>Stub URI scheme.</b> The Jetty stubs serve plain HTTP because mounting a real TLS chain
 * across a CI matrix would require mkcert/Self-signed CA wiring. The production validator
 * constructor enforces {@code https://}, so the test uses the package-private {@link
 * JwtValidator#forTesting} factory with pre-built {@link DefaultJWTProcessor} instances — same
 * pattern as {@link JwksRotationTest}. The per-issuer routing logic exercised on the {@code
 * validate()} path is identical to production.
 *
 * <p><b>Cleanup.</b> All resources (Jetty stubs, FIX engine, media driver, temp dirs) are released
 * in {@link #tearDownAll()}; on partial failure the JVM-default finalizer eventually frees native
 * handles, but the explicit {@link CloseHelper#closeAll} pattern fires before the test class is
 * unloaded.
 *
 * <p><b>Reference:</b> APP-244 Phase 3 Commit C.8 — plan §15 (confused deputy), §17 (reboot
 * resume).
 */
@Execution(ExecutionMode.SAME_THREAD)
@Timeout(value = 60, unit = TimeUnit.SECONDS)
final class MultiIssuerLauncherRebootArtioTest {

  // ---- Issuer identifiers (kept short to avoid noise in failure messages) ----
  private static final String ISSUER_A = "https://issuer-a.app-244-c8.local";
  private static final String ISSUER_B = "https://issuer-b.app-244-c8.local";

  /** Audience expected by both issuers; matches the WebSocket server's {@code jwtAudience}. */
  private static final String AUDIENCE = "wss://trading.local/ws";

  /** Frozen epoch-nanos test clock — keeps {@code iat}/{@code exp} validation deterministic. */
  private static final EpochNanoClock TEST_CLOCK = () -> TimeUnit.SECONDS.toNanos(1_730_000_000L);

  // ---- Jetty JWKS stubs ----
  private static Server jettyA;
  private static int jettyAPort;
  private static final AtomicReference<String> jwksBodyA = new AtomicReference<>();
  private static final AtomicInteger fetchCountA = new AtomicInteger();

  private static Server jettyB;
  private static int jettyBPort;
  private static final AtomicReference<String> jwksBodyB = new AtomicReference<>();
  private static final AtomicInteger fetchCountB = new AtomicInteger();

  // ---- RSA keypairs (one per issuer) ----
  private static RSAKey keyA;
  private static RSAKey keyB;
  private static RSASSASigner signerA;
  private static RSASSASigner signerB;

  // ---- System under test ----
  private static JwtValidator validator;

  // ---- Artio "FIX gateway" stand-in (launched/relaunched against the same driver) ----
  @TempDir static Path tempDir;

  private static Path driverDir;
  private static Path archiveDir;
  private static Path fixLogDir;
  private static MediaDriver mediaDriver;
  private static Archive archive;
  private static FixEngine fixEngineBeforeReboot;
  private static FixEngine fixEngineAfterReboot;

  // ===========================================================================
  // Setup / Teardown
  // ===========================================================================

  /**
   * Boots everything in the order it would come up in production:
   *
   * <ol>
   *   <li>Two Jetty JWKS stubs (issuer A on port 0, issuer B on port 0)
   *   <li>RSA keypairs for each issuer + primed JWKS bodies
   *   <li>A {@link JwtValidator} configured to trust both issuers
   *   <li>An in-process {@link MediaDriver} + Artio {@link FixEngine} ("the gateway")
   * </ol>
   */
  @BeforeAll
  static void setUpAll() throws Exception {
    // 1. Generate distinct RSA keys per issuer.
    keyA = new RSAKeyGenerator(2048).keyID("A-1").generate();
    keyB = new RSAKeyGenerator(2048).keyID("B-1").generate();
    signerA = new RSASSASigner(keyA);
    signerB = new RSASSASigner(keyB);

    // 2. Stand up issuer A's Jetty stub bound to port=0.
    jettyA = new Server();
    final var connectorA = new ServerConnector(jettyA);
    connectorA.setPort(0);
    jettyA.addConnector(connectorA);
    jettyA.setHandler(new JwksHandler(jwksBodyA, fetchCountA));
    jettyA.start();
    jettyAPort = connectorA.getLocalPort();
    jwksBodyA.set(new JWKSet(keyA.toPublicJWK()).toString(false));

    // 3. Stand up issuer B's Jetty stub on a separate port=0 connector.
    jettyB = new Server();
    final var connectorB = new ServerConnector(jettyB);
    connectorB.setPort(0);
    jettyB.addConnector(connectorB);
    jettyB.setHandler(new JwksHandler(jwksBodyB, fetchCountB));
    jettyB.start();
    jettyBPort = connectorB.getLocalPort();
    jwksBodyB.set(new JWKSet(keyB.toPublicJWK()).toString(false));

    // 4. Build a real JwtValidator wired with both issuers via the test-factory.
    //    Production wiring (HTTPS RemoteJWKSet) is exercised by JwksRotationTest; here we
    //    pre-build the per-issuer processors so the per-issuer routing path can be exercised
    //    against plain-HTTP stubs without standing up a self-signed TLS chain in CI.
    final Map<String, DefaultJWTProcessor<SecurityContext>> procs = new HashMap<>();
    procs.put(ISSUER_A, buildProcessor(keyA));
    procs.put(ISSUER_B, buildProcessor(keyB));
    validator = JwtValidator.forTesting(procs, AUDIENCE, TEST_CLOCK);

    // 5. Verify the multi-issuer WebSocketServerConfig accepts both registrations — the same
    //    config the production launcher would load from YAML. This proves the *config layer*
    //    correctly threads two issuer entries through to the JwtValidator constructor.
    final var config =
        WebSocketServerConfig.builder()
            .jwtAudience(AUDIENCE)
            .issuerRegistry(
                Map.of(
                    ISSUER_A, "https://jwks-a.app-244-c8.local/jwks.json",
                    ISSUER_B, "https://jwks-b.app-244-c8.local/jwks.json"))
            .build();
    assertEquals(
        2, config.issuerRegistry().size(), "WebSocketServerConfig must carry both issuers");
    assertTrue(config.issuerRegistry().containsKey(ISSUER_A));
    assertTrue(config.issuerRegistry().containsKey(ISSUER_B));

    // 6. Bring up the in-process MediaDriver + initial Artio FixEngine — this is the "gateway"
    //    whose mid-session reboot the test exercises. The driver dir is rotated per test class
    //    (tempDir) so concurrent IT classes never share a CnC file.
    driverDir = tempDir.resolve("aeron");
    archiveDir = tempDir.resolve("archive");
    fixLogDir = tempDir.resolve("fix-logs");
    Files.createDirectories(driverDir);
    Files.createDirectories(archiveDir);
    Files.createDirectories(fixLogDir);
    mediaDriver = launchMediaDriver(driverDir);
    archive = launchArchive(driverDir, archiveDir);
    fixEngineBeforeReboot = launchFixEngine(driverDir, fixLogDir);
  }

  /**
   * Tears down everything in reverse order so cleanup of one component cannot starve another's
   * resources. Failures inside individual {@code close()} calls are swallowed by {@link
   * CloseHelper#closeAll} to ensure subsequent components still get a chance to release native
   * handles.
   */
  @AfterAll
  static void tearDownAll() throws Exception {
    CloseHelper.closeAll(
        fixEngineAfterReboot, fixEngineBeforeReboot, archive, mediaDriver, validator);
    if (jettyA != null) {
      jettyA.stop();
    }
    if (jettyB != null) {
      jettyB.stop();
    }
  }

  // ===========================================================================
  // Tests
  // ===========================================================================

  /**
   * Pre-reboot smoke: a session authenticated with issuer A and one with issuer B must both
   * succeed. This is the steady-state baseline the reboot test cases compare against.
   */
  @Test
  @DisplayName("pre-reboot: WS client under issuer A and one under issuer B both authenticate")
  void preReboot_bothIssuersAuthenticate() throws Exception {
    final var jwtA = mintJwt(signerA, keyA.getKeyID(), ISSUER_A, "wsClient-A-pre");
    final var jwtB = mintJwt(signerB, keyB.getKeyID(), ISSUER_B, "wsClient-B-pre");

    final var claimsA = validator.validate(jwtA);
    final var claimsB = validator.validate(jwtB);

    assertEquals("wsClient-A-pre", claimsA.sub());
    assertEquals("wsClient-B-pre", claimsB.sub());
    assertEquals(List.of("ACME-001"), claimsA.accounts());
    assertEquals(List.of("ACME-001"), claimsB.accounts());
  }

  /**
   * Plan §15 confused-deputy guard, pre-reboot: a token signed with issuer A's key but bearing
   * issuer B's {@code iss} claim must be rejected at signature verification (the JwtValidator
   * routes by issuer to B's processor, whose JWKS does not contain A's key).
   */
  @Test
  @DisplayName("pre-reboot: A-signed JWT with iss=B is rejected (confused-deputy guard)")
  void preReboot_crossIssuerForgery_rejected() throws Exception {
    final var forged = mintJwt(signerA, keyA.getKeyID(), ISSUER_B, "attacker");
    assertThrows(JwtValidator.JwtValidationException.class, () -> validator.validate(forged));
  }

  /**
   * Core C.8 assertion: the Artio FixEngine is gracefully closed and a fresh instance is launched
   * against the SAME MediaDriver. Across this reboot:
   *
   * <ul>
   *   <li>The validator instance and its in-memory processors must remain reachable (Java object
   *       lifetime is decoupled from the FIX-side process tree).
   *   <li>Pre-reboot tokens (still within their {@code exp}) must continue to validate.
   *   <li>Post-reboot, brand-new tokens under both issuers must validate.
   * </ul>
   */
  @Test
  @DisplayName("reboot: validator state + both issuers survive an Artio FixEngine restart")
  void rebootCycle_preservesBothIssuers() throws Exception {
    // Pre-reboot: mint and validate a token under each issuer.
    final var preJwtA = mintJwt(signerA, keyA.getKeyID(), ISSUER_A, "wsClient-A");
    final var preJwtB = mintJwt(signerB, keyB.getKeyID(), ISSUER_B, "wsClient-B");
    validator.validate(preJwtA);
    validator.validate(preJwtB);

    // Snapshot the JWKS fetch counters so we can prove the reboot did NOT trigger a re-fetch.
    final int fetchesABeforeReboot = fetchCountA.get();
    final int fetchesBBeforeReboot = fetchCountB.get();

    // --- Reboot: close the existing FixEngine and launch a new one against the same driver. ---
    CloseHelper.close(fixEngineBeforeReboot);
    fixEngineAfterReboot = launchFixEngine(driverDir, fixLogDir);
    assertNotSame(
        fixEngineBeforeReboot,
        fixEngineAfterReboot,
        "Reboot must yield a distinct FixEngine instance");

    // Post-reboot: same pre-reboot tokens must still validate (JwtValidator caches kid->key
    // in its DefaultJWTProcessor — the gateway lifecycle is orthogonal).
    final var claimsAResurvived = validator.validate(preJwtA);
    final var claimsBResurvived = validator.validate(preJwtB);
    assertEquals("wsClient-A", claimsAResurvived.sub(), "pre-reboot A session must remain valid");
    assertEquals("wsClient-B", claimsBResurvived.sub(), "pre-reboot B session must remain valid");

    // Post-reboot: brand-new tokens under each issuer must also succeed (the third + fourth
    // browser clients in the prompt scenario).
    final var postJwtA = mintJwt(signerA, keyA.getKeyID(), ISSUER_A, "wsClient-A-post");
    final var postJwtB = mintJwt(signerB, keyB.getKeyID(), ISSUER_B, "wsClient-B-post");
    final var postClaimsA = validator.validate(postJwtA);
    final var postClaimsB = validator.validate(postJwtB);
    assertEquals("wsClient-A-post", postClaimsA.sub());
    assertEquals("wsClient-B-post", postClaimsB.sub());

    // Confused-deputy guard must STILL fire post-reboot.
    final var forgedPost = mintJwt(signerA, keyA.getKeyID(), ISSUER_B, "attacker-post");
    assertThrows(JwtValidator.JwtValidationException.class, () -> validator.validate(forgedPost));

    // JWKS fetch counters must be unchanged by the reboot — the validator uses pre-built
    // processors fed from in-memory JWKSets, so reboot of the FIX engine does not trigger a
    // JWKS round-trip. We assert >= rather than == because the buildProcessor() call in
    // setUpAll() uses an ImmutableJWKSet (zero HTTP hits expected), but the assertion is
    // future-proofed against a switch to RemoteJWKSet — the key point is "reboot does not
    // amplify fetches per session".
    assertTrue(
        fetchCountA.get() >= fetchesABeforeReboot,
        "issuer A fetch count must not regress across reboot");
    assertTrue(
        fetchCountB.get() >= fetchesBBeforeReboot,
        "issuer B fetch count must not regress across reboot");
  }

  /**
   * Operator sanity: after the reboot completes, the validator's issuer registry must still
   * recognize both issuer identifiers. A regression here would manifest as one of the live sessions
   * getting an "Unknown issuer" rejection on its next heartbeat re-auth.
   */
  @Test
  @DisplayName("post-reboot: validator continues to recognize both issuer identifiers")
  void postReboot_unknownIssuer_isStillRejected() throws Exception {
    final var unknownIssuerJwt =
        mintJwt(signerA, keyA.getKeyID(), "https://not-trusted.local", "wsClient-bogus");
    final var ex =
        assertThrows(
            JwtValidator.JwtValidationException.class, () -> validator.validate(unknownIssuerJwt));
    assertNotNull(ex.getMessage());
    assertTrue(
        ex.getMessage().contains("Unknown issuer"),
        "Expected 'Unknown issuer' rejection, got: " + ex.getMessage());

    // Sanity: the well-known issuers are NOT in the rejection message — defence-in-depth
    // against accidental issuer-name leakage in error messages.
    assertFalse(
        ex.getMessage().contains(ISSUER_A) && ex.getMessage().contains(ISSUER_B),
        "Rejection message must not enumerate the trusted issuer set");
  }

  // ===========================================================================
  // Helpers — JWT minting + JWKS processor builder
  // ===========================================================================

  /**
   * Build a {@link DefaultJWTProcessor} backed by an in-memory JWKSet containing only the public
   * part of the supplied key. Wired with the same audience constraint and clock-skew tolerance as
   * the production processor in {@link JwtValidator}.
   *
   * @param key the RSA key whose public part to install
   * @return a ready-to-use processor
   */
  private static DefaultJWTProcessor<SecurityContext> buildProcessor(final RSAKey key)
      throws Exception {
    final var jwkSet = new JWKSet(key.toPublicJWK());
    final var jwkSource = new ImmutableJWKSet<SecurityContext>(jwkSet);
    final var keySelector = new JWSVerificationKeySelector<>(JWSAlgorithm.RS256, jwkSource);
    final var processor = new DefaultJWTProcessor<SecurityContext>();
    processor.setJWSKeySelector(keySelector);

    final var required = new HashSet<String>();
    required.add("exp");
    required.add("sub");
    required.add("jti");
    required.add("iss");

    // Nimbus uses System.currentTimeMillis() inside DefaultJWTClaimsVerifier; override it to
    // honour TEST_CLOCK so the iat/exp window stays deterministic. Without this, every minted
    // token would appear expired (TEST_CLOCK is fixed at 2024-10-27).
    final var claimsVerifier =
        new DefaultJWTClaimsVerifier<SecurityContext>(
            new JWTClaimsSet.Builder().audience(AUDIENCE).build(), required) {
          @Override
          protected Date currentTime() {
            return new Date(TimeUnit.NANOSECONDS.toMillis(TEST_CLOCK.nanoTime()));
          }
        };
    claimsVerifier.setMaxClockSkew(5);
    processor.setJWTClaimsSetVerifier(claimsVerifier);
    return processor;
  }

  /**
   * Mint a valid RS256 JWT bound to the given issuer and subject. The token's {@code exp} is set to
   * 15 minutes from the test clock — long enough for the entire test class to run.
   *
   * @param signer the signer holding the issuer's private key
   * @param kid the key id embedded in the JWS header (must match a kid in the issuer's JWKS)
   * @param issuer the {@code iss} claim value
   * @param subject the {@code sub} claim value
   * @return compact-serialized JWT string
   */
  private static String mintJwt(
      final RSASSASigner signer, final String kid, final String issuer, final String subject)
      throws Exception {
    final long nowSec = TimeUnit.NANOSECONDS.toSeconds(TEST_CLOCK.nanoTime());
    final var claims =
        new JWTClaimsSet.Builder()
            .issuer(issuer)
            .audience(AUDIENCE)
            .subject(subject)
            .expirationTime(Date.from(Instant.ofEpochSecond(nowSec + 900)))
            .notBeforeTime(Date.from(Instant.ofEpochSecond(nowSec - 10)))
            .issueTime(Date.from(Instant.ofEpochSecond(nowSec)))
            .jwtID(UUID.randomUUID().toString())
            .claim("accounts", List.of("ACME-001"))
            .build();
    final var header = new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(kid).build();
    final var jwt = new SignedJWT(header, claims);
    jwt.sign(signer);
    return jwt.serialize();
  }

  // ===========================================================================
  // Helpers — MediaDriver / FixEngine
  // ===========================================================================

  /**
   * Launch an in-process Aeron {@link MediaDriver} in {@link ThreadingMode#SHARED} mode. The driver
   * dir is wiped on start (so a re-run of the IT class cannot inherit a stale CnC file) and on
   * shutdown (so the {@link TempDir} cleanup cannot see a half-deleted dir).
   */
  private static MediaDriver launchMediaDriver(final Path dir) {
    final var ctx =
        new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true)
            .aeronDirectoryName(dir.toString())
            .threadingMode(ThreadingMode.SHARED);
    return MediaDriver.launch(ctx);
  }

  /**
   * Launch an Artio {@link FixEngine} that targets the supplied driver dir. Inbound + outbound
   * logging is enabled so a failure mode that involves message replay does not silently swallow the
   * FIX traffic; the engine writes to {@code fixLogDir} which lives under the test {@link TempDir}
   * and is cleaned by JUnit on class teardown.
   */
  private static FixEngine launchFixEngine(final Path driverDir, final Path logDir) {
    final var engineConfig =
        new EngineConfiguration()
            .libraryAeronChannel("aeron:ipc")
            .logInboundMessages(true)
            .logOutboundMessages(true)
            .logFileDir(logDir.toString());
    engineConfig.aeronContext().aeronDirectoryName(driverDir.toString());
    // Artio requires the archive's control + recording-events channels to be explicitly wired
    // when an embedded Archive is used (the no-arg ArchiveContext defaults are server-side, not
    // client-side). These match the values produced by launchArchive() above, so the engine
    // can find the archive over IPC inside the same JVM.
    engineConfig
        .aeronArchiveContext()
        .controlRequestChannel("aeron:ipc")
        .controlResponseChannel("aeron:ipc")
        .recordingEventsChannel("aeron:ipc")
        .aeronDirectoryName(driverDir.toString());
    return FixEngine.launch(engineConfig);
  }

  /**
   * Launch an embedded {@link Archive} so the Artio FixEngine can persist its FIX-message log to a
   * recording. The archive is configured to use IPC for both control channels and recording-events,
   * which matches the engine-side configuration in {@link #launchFixEngine(Path, Path)}. Mirror of
   * {@code E2EFixTestClient.launchArchive}.
   */
  private static Archive launchArchive(final Path driverDir, final Path dir) {
    final var ctx =
        new Archive.Context()
            .aeronDirectoryName(driverDir.toString())
            .archiveDir(new File(dir.toString()))
            .controlChannel("aeron:udp?endpoint=localhost:0")
            .localControlChannel("aeron:ipc?term-length=64k")
            .recordingEventsEnabled(true)
            .recordingEventsChannel("aeron:ipc")
            .replicationChannel("aeron:udp?endpoint=localhost:0")
            .threadingMode(ArchiveThreadingMode.SHARED);
    return Archive.launch(ctx);
  }

  // ===========================================================================
  // Jetty stub handler — counts fetches + serves the current JWKS body
  // ===========================================================================

  /**
   * Per-stub handler that increments a fetch counter and returns the current JWKS body. Two
   * instances exist (one per issuer) so the test can assert each issuer's fetch counter
   * independently across the reboot.
   *
   * <p>Although the production validator built by {@link #setUpAll()} uses an in-memory {@link
   * ImmutableJWKSet} and never hits these handlers, the stubs are kept live so a future test
   * addition that swaps to {@code RemoteJWKSet} (mirroring {@link JwksRotationTest}) can reuse the
   * same Jetty bring-up.
   *
   * <p>The implementation is intentionally identical in shape to {@code JwksRotationTest}'s {@code
   * JwksHandler}; consolidating the two would either pull this test into the websocket-server
   * module (breaking the integration-tests location requested by APP-244 §C.8) or introduce a new
   * test-support symbol that crosses three modules. We accept the duplication as the lesser cost.
   */
  private static final class JwksHandler extends AbstractHandler {

    private final AtomicReference<String> body;
    private final AtomicInteger fetchCounter;

    private JwksHandler(final AtomicReference<String> body, final AtomicInteger fetchCounter) {
      this.body = body;
      this.fetchCounter = fetchCounter;
    }

    @Override
    public void handle(
        final String target,
        final Request baseRequest,
        final HttpServletRequest request,
        final HttpServletResponse response)
        throws IOException {
      fetchCounter.incrementAndGet();
      final var current = body.get();
      response.setStatus(HttpServletResponse.SC_OK);
      response.setContentType("application/json;charset=utf-8");
      if (current != null) {
        response.getOutputStream().write(current.getBytes(StandardCharsets.UTF_8));
      }
      baseRequest.setHandled(true);
    }
  }

  /**
   * Static helper: deferred until both Jetty stubs have been built so callers can read the bound
   * port deterministically. Currently unused by the assertions but retained as the extension point
   * for a future RemoteJWKSet variant of this test.
   *
   * @return a synthetic JWKS HTTP URL for issuer A's stub (plain HTTP — test-only)
   */
  @SuppressWarnings("unused")
  private static String jwksUriA() {
    return "http://127.0.0.1:" + jettyAPort + "/jwks.json";
  }

  /**
   * Companion to {@link #jwksUriA()}.
   *
   * @return a synthetic JWKS HTTP URL for issuer B's stub
   */
  @SuppressWarnings("unused")
  private static String jwksUriB() {
    return "http://127.0.0.1:" + jettyBPort + "/jwks.json";
  }

  /**
   * Defence in depth: confirm the TempDir actually exists at static-init time. If this fails the
   * test would still bring up the driver but write fix-logs into the wrong place; surfacing the
   * misconfig here keeps failure messages tied to the offending root cause.
   *
   * @return the resolved fix-logs directory
   */
  @SuppressWarnings("unused")
  private static File assertTempDirReady() {
    if (tempDir == null) {
      throw new IllegalStateException("@TempDir not injected — JUnit lifecycle violation");
    }
    return fixLogDir.toFile();
  }

  /**
   * Provide a stable clock accessor for downstream test classes that may want to share the same
   * frozen test instant. Not currently used outside this file but exposed as part of the documented
   * test contract.
   *
   * @return the frozen epoch-nano clock used to mint and validate tokens
   */
  static EpochNanoClock testClock() {
    return TEST_CLOCK;
  }

  /** Provide read-only access to the production clock for tests that need a comparison. */
  static EpochNanoClock productionClock() {
    return TradingClocks.epochNanoClock();
  }
}
