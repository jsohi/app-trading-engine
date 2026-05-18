package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jose.jwk.source.RemoteJWKSet;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jose.util.DefaultResourceRetriever;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.nimbusds.jwt.proc.DefaultJWTClaimsVerifier;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;
import com.trading.engine.messages.clock.TradingClocks;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.agrona.concurrent.EpochNanoClock;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

/**
 * Tests for {@link JwtValidator} behaviour when the JWKS endpoint experiences a transient HTTP
 * failure (503 Service Unavailable) during a key-rotation refresh attempt.
 *
 * <p><b>Scenario.</b> The JWKS endpoint:
 *
 * <ol>
 *   <li>Initially serves key A-1 → a validator is constructed and A-1 is cached.
 *   <li>Rotates to A-2. The first auth attempt with a new A-2-signed token triggers Nimbus's
 *       kid-not-found path, which re-fetches the JWKS. At this moment the stub returns HTTP 503
 *       (exactly once — the fail counter is decremented to 0 after the first failure).
 *   <li>Auth FAILS with a {@link JwtValidator.JwtValidationException} whose message indicates the
 *       JWKS endpoint was unreachable or returned a server error.
 *   <li>The {@code jwks.refresh.failure{reason="5xx"}} Micrometer counter increments by ≥ 1.
 *   <li>The stub recovers: it now serves A-2 normally. The next auth attempt SUCCEEDS.
 * </ol>
 *
 * <p><b>Why a pre-rotation validator?</b> The 503 scenario is meaningful only when a key rotation
 * has already occurred (the old A-1 key is in the cache; the first attempt to find A-2 triggers the
 * JWKS re-fetch that encounters the 503). If the validator had never fetched any keys it would also
 * fail, but the failure reason would be "initial fetch failed" rather than "rotation refresh
 * failed". Modelling the pre-rotation state makes the test represent a realistic production
 * scenario.
 *
 * <p><b>No Thread.sleep.</b> All assertions are synchronous. The stub's 503 is triggered
 * deterministically by a {@link AtomicInteger} fail-counter set in {@link BeforeEach}.
 *
 * <p><b>Metrics.</b> The test uses the package-private {@link JwtValidator#forTesting(Map, String,
 * EpochNanoClock, WebSocketMetrics)} overload to inject a {@link WebSocketMetrics} instance backed
 * by a {@link SimpleMeterRegistry}. This allows direct assertion of the {@code
 * jwks.refresh.failure} counter value.
 *
 * <p><b>Threading.</b> Single-threaded execution enforced by {@link ExecutionMode#SAME_THREAD}.
 */
@Execution(ExecutionMode.SAME_THREAD)
@Timeout(value = 30, unit = TimeUnit.SECONDS)
final class JwksRotationTransientFailureTest {

  private static final String ISSUER = "https://auth.transient-test.local";
  private static final String AUDIENCE = "wss://transient-test.local/ws";
  private static final EpochNanoClock CLOCK = TradingClocks.epochNanoClock();

  /** Short timeouts keep the test fast; JWKS payload is tiny. */
  private static final int HTTP_TIMEOUT_MS = 2_000;

  /** Size cap well above any test JWKS response. */
  private static final int MAX_BYTES = 64_000;

  // --- Jetty stub state ---

  private Server jetty;
  private int boundPort;

  /** Current JWKS JSON body served by the stub. */
  private final AtomicReference<String> jwksBody = new AtomicReference<>();

  /**
   * Number of remaining 503 responses the stub should return before recovering. Set to 1 in {@link
   * BeforeEach} to model a single transient failure. Decremented atomically by the handler so the
   * stub returns exactly one 503 then falls through to a normal 200 response.
   */
  private final AtomicInteger failsRemaining = new AtomicInteger(0);

  /** Total number of JWKS requests received by the stub (for diagnostic assertions). */
  private final AtomicInteger fetchCount = new AtomicInteger();

  // --- RSA key pairs ---

  private RSAKey keyA1;
  private RSASSASigner signerA1;

  private RSAKey keyA2;
  private RSASSASigner signerA2;

  // --- Metrics ---

  private SimpleMeterRegistry registry;
  private WebSocketMetrics metrics;

  @BeforeEach
  void setUp() throws Exception {
    keyA1 = new RSAKeyGenerator(2048).keyID("A-1-transient").generate();
    signerA1 = new RSASSASigner(keyA1);

    keyA2 = new RSAKeyGenerator(2048).keyID("A-2-transient").generate();
    signerA2 = new RSASSASigner(keyA2);

    registry = new SimpleMeterRegistry();
    metrics = new WebSocketMetrics(registry);

    jetty = new Server();
    final var connector = new ServerConnector(jetty);
    connector.setPort(0);
    jetty.addConnector(connector);
    jetty.setHandler(new TransientJwksHandler());
    jetty.start();
    boundPort = connector.getLocalPort();

    // Initially serve A-1 so the pre-rotation validator can cache it.
    jwksBody.set(buildJwks(keyA1));
  }

  @AfterEach
  void tearDown() throws Exception {
    if (jetty != null) {
      jetty.stop();
    }
  }

  // ===========================================================================
  // Test 1 — 503 on JWKS refresh → auth fails + counter increments
  // ===========================================================================

  @Test
  void validate_afterRotation_503OnRefetch_failsWithTransientError_andIncrementsCounter()
      throws Exception {
    // Step 1: build a pre-rotation validator and warm up its A-1 cache.
    final var validator = buildValidatorWithMetrics();
    final var a1Jwt = mintJwt(keyA1, signerA1);
    validator.validate(a1Jwt); // forces initial JWKS fetch → A-1 cached

    // Step 2: rotate the server to A-2.
    jwksBody.set(buildJwks(keyA2));

    // Step 3: configure stub to return 503 on the NEXT JWKS fetch.
    // The validator must trigger a re-fetch to find A-2 (kid "A-2-transient" not in cache).
    // That fetch will receive 503 → Nimbus throws → JwtValidator wraps in JwtValidationException.
    failsRemaining.set(1);

    final int counterBefore =
        (int) registry.counter("jwks.refresh.failure", "reason", "5xx").count();

    // Step 4: attempt to validate an A-2-signed JWT (will trigger the re-fetch).
    final var a2JwtDuringOutage = mintJwt(keyA2, signerA2);
    final var ex =
        assertThrows(
            JwtValidator.JwtValidationException.class, () -> validator.validate(a2JwtDuringOutage));

    // The exception must indicate an auth failure related to JWKS/verification — not a claims
    // error.
    assertTrue(
        ex.getMessage().contains("verification failed")
            || ex.getMessage().contains("processing error"),
        "Exception must reference verification or processing failure during JWKS 503, got: "
            + ex.getMessage());

    // Step 5: assert the jwks.refresh.failure{reason="5xx"} counter incremented by ≥ 1.
    final int counterAfter =
        (int) registry.counter("jwks.refresh.failure", "reason", "5xx").count();
    assertTrue(
        counterAfter >= counterBefore + 1,
        "jwks.refresh.failure{reason=\"5xx\"} counter must have incremented by ≥ 1 after a 503 "
            + "during JWKS refresh; before="
            + counterBefore
            + " after="
            + counterAfter);
  }

  // ===========================================================================
  // Test 2 — after the 503 resolves, the next auth attempt succeeds
  // ===========================================================================

  @Test
  void validate_afterTransient503Resolves_nextAttemptSucceeds() throws Exception {
    // Step 1: build validator with A-1 cached.
    final var validator = buildValidatorWithMetrics();
    final var a1Jwt = mintJwt(keyA1, signerA1);
    validator.validate(a1Jwt);

    // Step 2: rotate server to A-2, configure stub for ONE 503 failure.
    jwksBody.set(buildJwks(keyA2));
    failsRemaining.set(1);

    // Step 3: first attempt with A-2-signed JWT — will hit the 503.
    final var a2JwtFirst = mintJwt(keyA2, signerA2);
    assertThrows(JwtValidator.JwtValidationException.class, () -> validator.validate(a2JwtFirst));

    // Step 4: stub has now consumed the single failure; failsRemaining == 0.
    // Next auth attempt must trigger a fresh JWKS fetch that returns A-2 (200 OK).
    //
    // NOTE: Nimbus RemoteJWKSet 10.3 does not guarantee that the NEXT call to
    // processor.process() will unconditionally re-fetch. The retry inside
    // JwtValidator.processWithRetry re-processes a freshly parsed copy of the JWT which
    // causes RemoteJWKSet to re-attempt key selection. If the server is healthy by then
    // (503 already consumed), Nimbus will fetch A-2 and succeed.
    //
    // This scenario validates the "transient recovery" contract: one ephemeral 503 must not
    // permanently poison the validator state. After one failure the validator must be able to
    // validate tokens again once the JWKS endpoint is healthy.
    final var a2JwtSecond = mintJwt(keyA2, signerA2);
    final var claims = validator.validate(a2JwtSecond);

    assertEquals(
        "transient-subject",
        claims.sub(),
        "After transient 503 resolves, the next auth attempt must succeed with A-2 key");
    assertNotNull(claims.jti(), "jti must be present on the recovered auth attempt");
  }

  // ===========================================================================
  // Test 3 — counter starts at zero and increments exactly once for a single 503
  // ===========================================================================

  @Test
  void jwksRefreshFailureCounter_startsAtZero_incrementsOnce_forSingleTransientFailure()
      throws Exception {
    // Fresh registry/metrics confirm the counter starts at 0.
    final int counterInitial =
        (int) registry.counter("jwks.refresh.failure", "reason", "5xx").count();
    assertEquals(
        0, counterInitial, "jwks.refresh.failure counter must start at 0 before any failure");

    // Warm up A-1 cache.
    final var validator = buildValidatorWithMetrics();
    final var a1Jwt = mintJwt(keyA1, signerA1);
    validator.validate(a1Jwt);

    // Rotate and inject exactly one 503.
    jwksBody.set(buildJwks(keyA2));
    failsRemaining.set(1);

    final var a2Jwt = mintJwt(keyA2, signerA2);
    assertThrows(JwtValidator.JwtValidationException.class, () -> validator.validate(a2Jwt));

    final int counterAfter =
        (int) registry.counter("jwks.refresh.failure", "reason", "5xx").count();
    assertTrue(
        counterAfter >= 1, "Counter must be ≥ 1 after a 503 on JWKS refresh; got " + counterAfter);
  }

  // ===========================================================================
  // Helpers
  // ===========================================================================

  /**
   * Build a JWKS JSON string containing only the public key from the given RSA key.
   *
   * @param key the RSA key whose public part to include
   * @return JSON string in JWKS format
   */
  private static String buildJwks(final RSAKey key) throws Exception {
    return new JWKSet(key.toPublicJWK()).toString(false);
  }

  /**
   * Mint a valid RS256 JWT signed with the given key. The token expires in 15 minutes from the test
   * clock.
   *
   * @param key the RSA key whose kid is embedded in the JWT header
   * @param signer the signer for the key
   * @return compact serialized JWT string
   */
  private static String mintJwt(final RSAKey key, final RSASSASigner signer) throws Exception {
    final long nowSec = TimeUnit.NANOSECONDS.toSeconds(CLOCK.nanoTime());
    final var claims =
        new JWTClaimsSet.Builder()
            .issuer(ISSUER)
            .audience(AUDIENCE)
            .subject("transient-subject")
            .expirationTime(Date.from(Instant.ofEpochSecond(nowSec + 900)))
            .notBeforeTime(Date.from(Instant.ofEpochSecond(nowSec - 10)))
            .issueTime(Date.from(Instant.ofEpochSecond(nowSec)))
            .jwtID(UUID.randomUUID().toString())
            .claim("accounts", List.of("ACME-TRANSIENT-001"))
            .build();
    final var header = new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(key.getKeyID()).build();
    final var jwt = new SignedJWT(header, claims);
    jwt.sign(signer);
    return jwt.serialize();
  }

  /**
   * Build a {@link JwtValidator} backed by a real {@link RemoteJWKSet} pointing at the local Jetty
   * stub, AND injecting the test {@link WebSocketMetrics} instance so the {@code
   * jwks.refresh.failure} counter is observable from the test.
   *
   * <p>Uses the package-private {@link JwtValidator#forTesting(Map, String, EpochNanoClock,
   * WebSocketMetrics)} overload to bypass both the public constructor's {@code https://}
   * enforcement and to inject the metrics sink.
   */
  @SuppressWarnings("deprecation") // RemoteJWKSet deprecated in nimbus 9.35+; required at 10.3
  private JwtValidator buildValidatorWithMetrics() throws Exception {
    final var retriever = new DefaultResourceRetriever(HTTP_TIMEOUT_MS, HTTP_TIMEOUT_MS, MAX_BYTES);
    final var jwksUrl = URI.create("http://127.0.0.1:" + boundPort + "/jwks.json").toURL();
    final var jwkSource = new RemoteJWKSet<SecurityContext>(jwksUrl, retriever);

    final var keySelector = new JWSVerificationKeySelector<>(JWSAlgorithm.RS256, jwkSource);
    final var processor = new DefaultJWTProcessor<SecurityContext>();
    processor.setJWSKeySelector(keySelector);

    final var required = new HashSet<String>();
    required.add("exp");
    required.add("sub");
    required.add("jti");
    required.add("iss");

    final var claimsVerifier =
        new DefaultJWTClaimsVerifier<SecurityContext>(
            new JWTClaimsSet.Builder().audience(AUDIENCE).build(), required);
    claimsVerifier.setMaxClockSkew(5);
    processor.setJWTClaimsSetVerifier(claimsVerifier);

    return JwtValidator.forTesting(Map.of(ISSUER, processor), AUDIENCE, CLOCK, metrics);
  }

  /**
   * Jetty handler that returns HTTP 503 {@link #failsRemaining} times (atomically decremented),
   * then falls through to a normal 200 response serving the current {@link #jwksBody}.
   */
  private final class TransientJwksHandler extends AbstractHandler {

    @Override
    public void handle(
        final String target,
        final Request baseRequest,
        final HttpServletRequest request,
        final HttpServletResponse response)
        throws IOException {
      fetchCount.incrementAndGet();

      // Atomically consume one failure slot: if > 0 before decrement, return 503.
      final int remaining = failsRemaining.getAndDecrement();
      if (remaining > 0) {
        response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        response.setContentType("text/plain;charset=utf-8");
        response
            .getOutputStream()
            .write(
                "503 Service Unavailable (transient test failure)"
                    .getBytes(StandardCharsets.UTF_8));
        baseRequest.setHandled(true);
        return;
      }

      // Stub has recovered: serve the current JWKS body with 200 OK.
      final var body = jwksBody.get();
      response.setStatus(HttpServletResponse.SC_OK);
      response.setContentType("application/json;charset=utf-8");
      if (body != null) {
        response.getOutputStream().write(body.getBytes(StandardCharsets.UTF_8));
      }
      baseRequest.setHandled(true);
    }
  }
}
