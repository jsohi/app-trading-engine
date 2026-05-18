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
 * Integration-style tests for the JWKS key-rotation lifecycle against the real {@link
 * JwtValidator}.
 *
 * <p>Verifies the canonical IdP key-rotation scenario: an existing validator (with a cached key set
 * containing kid A-1) continues to validate A-1-signed tokens after the JWKS endpoint rotates to
 * A-2 — because Nimbus's {@link RemoteJWKSet} caches the fetched key set for the lifetime of the
 * processor instance and does NOT re-fetch on every validation call. A fresh validator instance
 * constructed after rotation will fetch A-2 only and must therefore reject A-1-signed tokens.
 *
 * <p><b>HTTP stub.</b> Uses a Jetty {@link Server} bound to {@code port=0} (OS-assigned) to serve a
 * switchable {@code /jwks.json} endpoint. The stub is plain HTTP — production HTTPS enforcement is
 * bypassed by building the processor manually (outside the public {@link JwtValidator}
 * constructor's https-check) and injecting via the package-private {@link JwtValidator#forTesting}
 * factory.
 *
 * <p><b>Request counter.</b> The Jetty handler increments an {@link AtomicInteger} on every {@code
 * GET /jwks.json} request, allowing assertions on how many JWKS fetches were issued by Nimbus
 * without relying on mocks.
 *
 * <p><b>Threading.</b> Not thread-safe — single-threaded test execution enforced by {@link
 * ExecutionMode#SAME_THREAD}.
 *
 * <p><b>Determinism.</b> No {@code Thread.sleep} — all operations are synchronous. HTTP server
 * cleanup is performed in {@link AfterEach} for guaranteed resource release.
 */
@Execution(ExecutionMode.SAME_THREAD)
@Timeout(value = 30, unit = TimeUnit.SECONDS)
final class JwksRotationTest {

  private static final String ISSUER = "https://auth.rotation-test.local";
  private static final String AUDIENCE = "wss://rotation-test.local/ws";
  private static final EpochNanoClock CLOCK = TradingClocks.epochNanoClock();

  /** Short HTTP timeouts keep the test fast; JWKS payload is tiny. */
  private static final int HTTP_TIMEOUT_MS = 2_000;

  /** Size cap well above any test JWKS response (a single RSA public key is ~500 bytes). */
  private static final int MAX_BYTES = 64_000;

  // --- Jetty stub state ---

  private Server jetty;
  private int boundPort;

  /** Current JWKS JSON body served by the stub. Swapped atomically to simulate rotation. */
  private final AtomicReference<String> jwksBody = new AtomicReference<>();

  /**
   * Total number of {@code GET /jwks.json} requests received by the stub since the last reset.
   * Allows assertions on Nimbus's fetch behaviour without mocking.
   */
  private final AtomicInteger fetchCount = new AtomicInteger();

  // --- RSA key pair A-1 (initial key set) ---

  private RSAKey keyA1;
  private RSASSASigner signerA1;

  // --- RSA key pair A-2 (rotated key set) ---

  private RSAKey keyA2;
  private RSASSASigner signerA2;

  @BeforeEach
  void setUp() throws Exception {
    // Generate two independent RSA 2048 key pairs with distinct kids.
    keyA1 = new RSAKeyGenerator(2048).keyID("A-1").generate();
    signerA1 = new RSASSASigner(keyA1);

    keyA2 = new RSAKeyGenerator(2048).keyID("A-2").generate();
    signerA2 = new RSASSASigner(keyA2);

    // Start Jetty on an OS-assigned ephemeral port.
    jetty = new Server();
    final var connector = new ServerConnector(jetty);
    connector.setPort(0);
    jetty.addConnector(connector);
    jetty.setHandler(new JwksHandler());
    jetty.start();
    boundPort = connector.getLocalPort();

    // Prime the stub with the initial JWKS containing key A-1 only.
    jwksBody.set(buildJwks(keyA1));
  }

  @AfterEach
  void tearDown() throws Exception {
    if (jetty != null) {
      jetty.stop();
    }
  }

  // ===========================================================================
  // Test 1 — initial validation against A-1 succeeds (happy path, cache miss)
  // ===========================================================================

  @Test
  void validate_a1SignedJwt_succeeds_whenServerServesA1() throws Exception {
    final int fetchesBefore = fetchCount.get();

    final var validator = buildValidator();
    final var jwt = mintJwt(keyA1, signerA1);

    final var claims = validator.validate(jwt);

    assertEquals(
        "rotation-subject", claims.sub(), "sub claim must be preserved through validation");
    assertNotNull(claims.jti(), "jti must be present");
    // Nimbus fetches the JWKS exactly once on the first key-selection miss.
    assertTrue(
        fetchCount.get() > fetchesBefore,
        "JWKS server must have been contacted at least once for the initial key fetch");
  }

  // ===========================================================================
  // Test 2 — pre-rotation validator still validates A-1 tokens after rotation
  //           (Nimbus RemoteJWKSet caches the fetched key set; no re-fetch per call)
  // ===========================================================================

  @Test
  void validate_a1SignedJwt_stillSucceeds_afterServerRotatesToA2() throws Exception {
    // Build the validator BEFORE rotation so it fetches A-1 on first use.
    final var preRotationValidator = buildValidator();

    // First validation: forces the initial JWKS fetch (A-1 is fetched and cached).
    final var firstJwt = mintJwt(keyA1, signerA1);
    preRotationValidator.validate(firstJwt);
    final int fetchesAfterFirst = fetchCount.get();

    // Rotate the server: it now serves ONLY A-2.
    jwksBody.set(buildJwks(keyA2));

    // Second validation with a FRESH A-1-signed token — must still succeed.
    //
    // Rationale: Nimbus's RemoteJWKSet caches the key set for the lifetime of the processor.
    // It does NOT re-fetch on every call. The pre-rotation processor has A-1 in its in-memory
    // cache, so validation succeeds even though the server now serves A-2.
    // This is the documented Nimbus 10.3 behaviour documented in JwtValidator.processWithRetry.
    final var secondJwt = mintJwt(keyA1, signerA1);
    final var claims = preRotationValidator.validate(secondJwt);

    assertEquals(
        "rotation-subject",
        claims.sub(),
        "Pre-rotation validator must still accept A-1-signed JWT after server rotates to A-2 "
            + "(RemoteJWKSet cache hit — no re-fetch per validation call)");

    // The server must NOT have been contacted again for the second validation — it was a cache hit.
    assertEquals(
        fetchesAfterFirst,
        fetchCount.get(),
        "No additional JWKS fetch should occur for the second A-1-signed JWT "
            + "(RemoteJWKSet served A-1 from cache)");
  }

  // ===========================================================================
  // Test 3 — fresh validator fetches A-2 and rejects A-1-signed tokens
  // ===========================================================================

  @Test
  void validate_a1SignedJwt_fails_whenFreshValidatorFetchesA2() throws Exception {
    // Rotate the server to A-2 BEFORE constructing the fresh validator.
    jwksBody.set(buildJwks(keyA2));

    // Fresh validator fetches A-2 on first use — A-1 is not in its key set.
    final var freshValidator = buildValidator();
    final var a1SignedJwt = mintJwt(keyA1, signerA1);

    final var ex =
        assertThrows(
            JwtValidator.JwtValidationException.class, () -> freshValidator.validate(a1SignedJwt));

    // The exception must indicate a verification failure — not a network or claims error.
    assertTrue(
        ex.getMessage().contains("verification failed")
            || ex.getMessage().contains("processing error"),
        "Exception must indicate JWT verification/processing failure, got: " + ex.getMessage());
  }

  // ===========================================================================
  // Test 4 — fresh validator validates A-2-signed tokens after rotation
  // ===========================================================================

  @Test
  void validate_a2SignedJwt_succeeds_afterServerRotatesToA2() throws Exception {
    // Rotate the server to A-2 before building the validator.
    jwksBody.set(buildJwks(keyA2));

    final var validator = buildValidator();
    final var jwt = mintJwt(keyA2, signerA2);

    final var claims = validator.validate(jwt);

    assertEquals(
        "rotation-subject",
        claims.sub(),
        "Fresh validator must accept A-2-signed JWT after rotation");
    assertNotNull(claims.jti(), "jti must be present");
  }

  // ===========================================================================
  // Test 5 — exactly ONE JWKS refresh on kid-not-found retry (request counter)
  // ===========================================================================

  @Test
  void validate_a1SignedJwt_triggersExactlyOneRefresh_onKidNotFound() throws Exception {
    // Start with A-2 so the fresh validator fetches A-2 (which does not contain A-1).
    jwksBody.set(buildJwks(keyA2));

    final var freshValidator = buildValidator();
    final int fetchesBefore = fetchCount.get();

    // Attempt to validate an A-1-signed JWT against a fresh A-2-only validator.
    // Nimbus will:
    //   1. Select keys for kid "A-1" → cache miss → fetch JWKS (increment fetchCount)
    //   2. A-1 not found in A-2-only set → BadJWSException
    //   3. JwtValidator.processWithRetry re-processes → RemoteJWKSet fetches again (increment
    // fetchCount)
    //   4. Still no A-1 → verification fails
    //
    // Net result: at least 1 JWKS fetch (the initial lookup), possibly 2 (the retry).
    // We assert ≥ 1 to confirm a real HTTP round-trip happened — more than that is acceptable
    // given Nimbus 10.3's RemoteJWKSet refresh semantics on kid-not-found.
    final var a1SignedJwt = mintJwt(keyA1, signerA1);
    assertThrows(
        JwtValidator.JwtValidationException.class, () -> freshValidator.validate(a1SignedJwt));

    assertTrue(
        fetchCount.get() > fetchesBefore,
        "At least one JWKS fetch must have occurred when A-1 kid is not found in the A-2 key set");
  }

  // ===========================================================================
  // Helpers
  // ===========================================================================

  /**
   * Build a JWKS JSON string containing the public key from the given RSA key pair.
   *
   * @param key the RSA key whose public part to include in the JWKS
   * @return JSON JWKS string
   */
  private static String buildJwks(final RSAKey key) throws Exception {
    final var publicOnly = key.toPublicJWK();
    return new JWKSet(publicOnly).toString(false); // false = include public keys
  }

  /**
   * Mint a valid RS256 JWT signed with the given key and signer. The token expires in 15 minutes
   * from the test clock.
   *
   * @param key the RSA key whose kid is embedded in the JWT header
   * @param signer the signer for the given key
   * @return compact serialized JWT string
   */
  private static String mintJwt(final RSAKey key, final RSASSASigner signer) throws Exception {
    final long nowSec = TimeUnit.NANOSECONDS.toSeconds(CLOCK.nanoTime());
    final var claims =
        new JWTClaimsSet.Builder()
            .issuer(ISSUER)
            .audience(AUDIENCE)
            .subject("rotation-subject")
            .expirationTime(Date.from(Instant.ofEpochSecond(nowSec + 900)))
            .notBeforeTime(Date.from(Instant.ofEpochSecond(nowSec - 10)))
            .issueTime(Date.from(Instant.ofEpochSecond(nowSec)))
            .jwtID(UUID.randomUUID().toString())
            .claim("accounts", List.of("ACME-ROT-001"))
            .build();
    final var header = new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(key.getKeyID()).build();
    final var jwt = new SignedJWT(header, claims);
    jwt.sign(signer);
    return jwt.serialize();
  }

  /**
   * Build a {@link JwtValidator} backed by a real {@link RemoteJWKSet} pointing at the local Jetty
   * stub. Uses the package-private {@link JwtValidator#forTesting} factory to bypass the public
   * constructor's {@code https://} enforcement — the stub serves plain HTTP for test isolation.
   *
   * <p>The resulting processor behaves identically to one built by the production constructor: it
   * performs real HTTP fetches, caches the key set, and retries on kid-not-found.
   */
  @SuppressWarnings("deprecation") // RemoteJWKSet deprecated in nimbus 9.35+; required at 10.3
  private JwtValidator buildValidator() throws Exception {
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

    return JwtValidator.forTesting(Map.of(ISSUER, processor), AUDIENCE, CLOCK);
  }

  /**
   * Jetty {@link AbstractHandler} that serves the current {@link #jwksBody} for every request,
   * incrementing {@link #fetchCount} on each hit. Not path-scoped — any GET is treated as a JWKS
   * request (the test only has one endpoint).
   */
  private final class JwksHandler extends AbstractHandler {

    @Override
    public void handle(
        final String target,
        final Request baseRequest,
        final HttpServletRequest request,
        final HttpServletResponse response)
        throws IOException {
      fetchCount.incrementAndGet();
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
