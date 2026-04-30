package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jose.jwk.source.ImmutableJWKSet;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.proc.DefaultJWTClaimsVerifier;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;
import com.trading.engine.messages.clock.TradingClocks;
import java.util.HashSet;
import java.util.Map;
import org.agrona.concurrent.EpochNanoClock;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link JwtValidator#preflightOrThrow()}: verifies the fail-fast variant of the
 * constructor's preflight loop.
 *
 * <p>The constructor's existing preflight only logs failures so partial JWKS misconfiguration does
 * not block startup. {@code preflightOrThrow()} is the strict variant — invoked at fix-client
 * bridge startup so the bridge refuses to listen if any IdP is unreachable.
 */
final class JwtValidatorPreflightOrThrowTest {

  private static final String AUDIENCE = "wss://trading.test/ws";
  private static final EpochNanoClock CLOCK = TradingClocks.epochNanoClock();

  // --- Success path ---

  @Test
  void preflightOrThrow_reachableImmutableJwkSet_returnsNormally() throws Exception {
    // ImmutableJWKSet does NOT make any HTTP request — selectJWSKeys returns the keys directly.
    // This proves the success contract: when no exception is raised by the underlying key
    // selector, preflightOrThrow returns normally.
    final var validator = buildValidatorWithImmutableJwkSet();

    assertDoesNotThrow(validator::preflightOrThrow);
  }

  // --- Failure path ---

  @Test
  void preflightOrThrow_unreachableHttpsEndpoint_throwsJwtValidationException() {
    // 192.0.2.1 is RFC 5737 TEST-NET-1 — guaranteed not to route in any real network.
    // The connect attempt will fail within the JwtValidator's 5s connect timeout.
    final var unreachable = "https://192.0.2.1/jwks.json";
    final var validator =
        new JwtValidator(Map.of("https://issuer.test", unreachable), AUDIENCE, CLOCK);

    final var ex =
        assertThrows(JwtValidator.JwtValidationException.class, validator::preflightOrThrow);
    assertTrue(
        ex.getMessage().contains("https://issuer.test"),
        "exception should reference issuer name, got: " + ex.getMessage());
    assertTrue(
        ex.getMessage().contains("preflight"),
        "exception should mention preflight, got: " + ex.getMessage());
  }

  // --- Empty registry path ---

  @Test
  void preflightOrThrow_emptyIssuerRegistry_returnsNormally() {
    final var validator = new JwtValidator(Map.of(), AUDIENCE, CLOCK);

    assertDoesNotThrow(validator::preflightOrThrow);
  }

  // --- Helpers ---

  /**
   * Build a JwtValidator backed by an in-memory {@link ImmutableJWKSet}, which never performs HTTP
   * I/O during preflight. Used to exercise the success path without spinning up a real JWKS server.
   */
  private static JwtValidator buildValidatorWithImmutableJwkSet() throws Exception {
    final RSAKey rsaKey = new RSAKeyGenerator(2048).keyID("preflight-key-1").generate();
    final var jwkSet = new JWKSet(rsaKey.toPublicJWK());
    final var jwkSource = new ImmutableJWKSet<SecurityContext>(jwkSet);
    final var keySelector = new JWSVerificationKeySelector<>(JWSAlgorithm.RS256, jwkSource);

    final var processor = new DefaultJWTProcessor<SecurityContext>();
    processor.setJWSKeySelector(keySelector);

    final var requiredClaims = new HashSet<String>();
    requiredClaims.add("exp");
    requiredClaims.add("sub");
    requiredClaims.add("jti");
    requiredClaims.add("iss");
    final var claimsVerifier =
        new DefaultJWTClaimsVerifier<SecurityContext>(
            new JWTClaimsSet.Builder().audience(AUDIENCE).build(), requiredClaims);
    claimsVerifier.setMaxClockSkew(5);
    processor.setJWTClaimsSetVerifier(claimsVerifier);

    return JwtValidator.forTesting(Map.of("https://issuer.test", processor), AUDIENCE, CLOCK);
  }
}
