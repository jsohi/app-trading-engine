package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
import java.time.Instant;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.agrona.concurrent.EpochNanoClock;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Multi-issuer JWT validation: confused-deputy guard.
 *
 * <p>Two issuers ({@code A} and {@code B}) each have their own RS256 keypair. The kids are
 * deliberately namespaced ({@code A-1} vs {@code B-1}) per the plan §15 disjoint-namespace
 * convention. The tests assert:
 *
 * <ul>
 *   <li>Tokens correctly issued by A (signed by A's key, {@code iss=A}, {@code kid=A-1}) validate.
 *   <li>Tokens correctly issued by B validate against B's processor.
 *   <li>A token forged by signing with A's key but presenting {@code iss=B} fails — {@link
 *       JwtValidator} routes by {@code iss} to B's processor, which has only B's key, so the
 *       signature does not verify.
 *   <li>The colliding-kid scenario (where A and B share the same kid string) cannot let an A-key
 *       token slip through B's processor — same routing-by-issuer guarantee applies.
 * </ul>
 *
 * <p>This is the contract that prevents a confused-deputy where a compromised IdP with the same kid
 * namespace as a legitimate one could mint tokens accepted by the other's processor.
 */
@Timeout(value = 10, unit = TimeUnit.SECONDS)
final class JwtValidatorMultiIssuerTest {

  private static final String ISSUER_A = "https://issuer-a.example";
  private static final String ISSUER_B = "https://issuer-b.example";
  private static final String AUDIENCE = "trading-ui";

  private static final EpochNanoClock TEST_CLOCK = () -> TimeUnit.SECONDS.toNanos(1_700_000_000L);

  private static RSAKey keyA;
  private static RSAKey keyB;
  private static RSASSASigner signerA;
  private static RSASSASigner signerB;

  @BeforeAll
  static void generateKeys() throws Exception {
    keyA = new RSAKeyGenerator(2048).keyID("A-1").generate();
    keyB = new RSAKeyGenerator(2048).keyID("B-1").generate();
    signerA = new RSASSASigner(keyA);
    signerB = new RSASSASigner(keyB);
  }

  @Test
  void issuerA_acceptsItsOwnTokens() throws Exception {
    final var validator = build(buildProcessor(keyA), buildProcessor(keyB));
    final var jwt = sign(signerA, keyA.getKeyID(), ISSUER_A);
    final var claims = validator.validate(jwt);
    // ValidatedClaims doesn't carry issuer (validator already routed by it). Assert the subject
    // and sanity check accounts came through to confirm the validation pipeline ran end-to-end.
    assertEquals("user-001", claims.sub());
    assertEquals(List.of("ACME-001"), claims.accounts());
  }

  @Test
  void issuerB_acceptsItsOwnTokens() throws Exception {
    final var validator = build(buildProcessor(keyA), buildProcessor(keyB));
    final var jwt = sign(signerB, keyB.getKeyID(), ISSUER_B);
    final var claims = validator.validate(jwt);
    assertEquals("user-001", claims.sub());
  }

  @Test
  void aSignedTokenWithBIssuerClaim_isRejectedAtSignatureCheck() throws Exception {
    final var validator = build(buildProcessor(keyA), buildProcessor(keyB));
    // Forged token: signed with A's key but the iss claim says B. JwtValidator routes by iss so it
    // looks up B's processor — B's JWKS has only B's public key, so signature verification fails.
    final var forged = sign(signerA, keyA.getKeyID(), ISSUER_B);
    assertThrows(JwtValidator.JwtValidationException.class, () -> validator.validate(forged));
  }

  @Test
  void collidingKidNamespaces_doNotAllowCrossIssuerForgery() throws Exception {
    // Adversarial: rebuild the keys with the SAME kid string ("X-1") and assert that even with
    // identical kids, JwtValidator's issuer-then-kid routing rejects a cross-issuer forgery. This
    // is the explicit confused-deputy scenario from plan §15.
    final var aColl = new RSAKeyGenerator(2048).keyID("X-1").generate();
    final var bColl = new RSAKeyGenerator(2048).keyID("X-1").generate();
    final var validator = build(buildProcessor(aColl), buildProcessor(bColl));
    final var forged = sign(new RSASSASigner(aColl), aColl.getKeyID(), ISSUER_B);
    assertThrows(JwtValidator.JwtValidationException.class, () -> validator.validate(forged));
  }

  // ===========================================================================
  // Helpers — mirror JwtValidatorTest's pattern
  // ===========================================================================

  private static JwtValidator build(
      final DefaultJWTProcessor<SecurityContext> procA,
      final DefaultJWTProcessor<SecurityContext> procB) {
    return JwtValidator.forTesting(Map.of(ISSUER_A, procA, ISSUER_B, procB), AUDIENCE, TEST_CLOCK);
  }

  private static DefaultJWTProcessor<SecurityContext> buildProcessor(final RSAKey key) {
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
    // Override currentTime() to honor TEST_CLOCK (matches JwtValidator's production processor
    // wiring). Without this, nimbus uses System.currentTimeMillis() and rejects every token as
    // expired (TEST_CLOCK is Nov 2023; today is later).
    final var claimsVerifier =
        new DefaultJWTClaimsVerifier<SecurityContext>(
            new JWTClaimsSet.Builder().audience(AUDIENCE).build(), required) {
          @Override
          protected Date currentTime() {
            final long ms = TimeUnit.NANOSECONDS.toMillis(TEST_CLOCK.nanoTime());
            return new Date(ms);
          }
        };
    claimsVerifier.setMaxClockSkew(5);
    processor.setJWTClaimsSetVerifier(claimsVerifier);
    return processor;
  }

  private static String sign(final RSASSASigner signer, final String kid, final String issuer)
      throws Exception {
    final var now = Instant.ofEpochSecond(TimeUnit.NANOSECONDS.toSeconds(TEST_CLOCK.nanoTime()));
    final var header = new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(kid).build();
    final var claims =
        new JWTClaimsSet.Builder()
            .issuer(issuer)
            .audience(AUDIENCE)
            .subject("user-001")
            .expirationTime(Date.from(now.plusSeconds(900)))
            .notBeforeTime(Date.from(now.minusSeconds(10)))
            .issueTime(Date.from(now))
            .jwtID(UUID.randomUUID().toString())
            .claim("accounts", List.of("ACME-001"))
            .build();
    final var jwt = new SignedJWT(header, claims);
    jwt.sign(signer);
    assertNotNull(jwt.getSignature());
    return jwt.serialize();
  }
}
