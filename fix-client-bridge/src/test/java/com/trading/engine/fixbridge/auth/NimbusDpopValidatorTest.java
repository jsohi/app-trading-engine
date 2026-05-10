package com.trading.engine.fixbridge.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.trading.engine.fixbridge.auth.DpopValidator.Result;
import com.trading.engine.websocket.JwtValidator.ValidatedClaims;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.agrona.concurrent.EpochNanoClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Coverage matrix for {@link NimbusDpopValidator}.
 *
 * <p>Each test mints a fresh RSA key + DPoP proof JWT (no HTTP, no IdP) and exercises one branch of
 * the validator's decision tree. The validator is configured with a 30s skew window, default {@code
 * htm=POST}, and an {@link InMemoryJtiReplayCache}.
 */
final class NimbusDpopValidatorTest {

  private static final long SKEW_SECONDS = 30L;
  private static final EpochNanoClock CLOCK =
      com.trading.engine.messages.clock.TradingClocks.epochNanoClock();

  private InMemoryJtiReplayCache jtiCache;
  private NimbusDpopValidator validator;
  private RSAKey workerKey;
  private RSASSASigner workerSigner;

  @BeforeEach
  void setUp() throws Exception {
    jtiCache = new InMemoryJtiReplayCache();
    validator = new NimbusDpopValidator(CLOCK, SKEW_SECONDS, jtiCache);
    workerKey = new RSAKeyGenerator(2048).keyID("worker-1").generate();
    workerSigner = new RSASSASigner(workerKey);
  }

  @Test
  void nullHeader_noCnfJkt_returnsValid() {
    final var claims = claimsWith(null);
    assertEquals(Result.VALID, validator.validate(claims, null));
  }

  @Test
  void nullHeader_cnfJktPresent_returnsInvalid() {
    final var claims = claimsWith("any-thumbprint");
    assertEquals(Result.INVALID, validator.validate(claims, null));
  }

  @Test
  void emptyHeader_cnfJktPresent_returnsInvalid() {
    final var claims = claimsWith("any-thumbprint");
    assertEquals(Result.INVALID, validator.validate(claims, ""));
  }

  @Test
  void validProof_matchingCnfJkt_returnsValid() throws Exception {
    final var thumbprint = workerKey.toPublicJWK().computeThumbprint().toString();
    final var proof = mintProof(workerKey, workerSigner, "POST", "wss://bridge.test/ws", 0);
    final var claims = claimsWith(thumbprint);
    assertEquals(Result.VALID, validator.validate(claims, proof));
  }

  @Test
  void validProof_mismatchedCnfJkt_returnsStaleDpop() throws Exception {
    // Worker rotated to workerKey but bearer was issued for staleKey
    final var staleKey = new RSAKeyGenerator(2048).keyID("stale-key").generate();
    final var staleThumbprint = staleKey.toPublicJWK().computeThumbprint().toString();
    final var proof = mintProof(workerKey, workerSigner, "POST", "wss://bridge.test/ws", 0);
    final var claims = claimsWith(staleThumbprint);
    assertEquals(Result.STALE_DPOP, validator.validate(claims, proof));
  }

  @Test
  void validProof_noCnfJkt_returnsValid() throws Exception {
    final var proof = mintProof(workerKey, workerSigner, "POST", "wss://bridge.test/ws", 0);
    final var claims = claimsWith(null);
    assertEquals(Result.VALID, validator.validate(claims, proof));
  }

  @Test
  void iatOutsideSkew_returnsInvalid() throws Exception {
    // iat is 5 minutes in the past — well outside the 30s window
    final var proof = mintProof(workerKey, workerSigner, "POST", "wss://bridge.test/ws", -300);
    final var thumbprint = workerKey.toPublicJWK().computeThumbprint().toString();
    final var claims = claimsWith(thumbprint);
    assertEquals(Result.INVALID, validator.validate(claims, proof));
  }

  @Test
  void iatFutureOutsideSkew_returnsInvalid() throws Exception {
    final var proof = mintProof(workerKey, workerSigner, "POST", "wss://bridge.test/ws", 300);
    final var thumbprint = workerKey.toPublicJWK().computeThumbprint().toString();
    final var claims = claimsWith(thumbprint);
    assertEquals(Result.INVALID, validator.validate(claims, proof));
  }

  @Test
  void htmMismatch_returnsInvalid() throws Exception {
    final var proof = mintProof(workerKey, workerSigner, "GET", "wss://bridge.test/ws", 0);
    final var thumbprint = workerKey.toPublicJWK().computeThumbprint().toString();
    final var claims = claimsWith(thumbprint);
    assertEquals(Result.INVALID, validator.validate(claims, proof));
  }

  @Test
  void htuMismatch_returnsInvalid() throws Exception {
    final var withHtu =
        new NimbusDpopValidator(CLOCK, SKEW_SECONDS, jtiCache, "POST", "wss://expected.example/ws");
    final var proof = mintProof(workerKey, workerSigner, "POST", "wss://attacker/ws", 0);
    final var thumbprint = workerKey.toPublicJWK().computeThumbprint().toString();
    final var claims = claimsWith(thumbprint);
    assertEquals(Result.INVALID, withHtu.validate(claims, proof));
  }

  @Test
  void jtiReplay_returnsInvalid() throws Exception {
    // Re-use the SAME proof twice — second validate must see the jti replay.
    final var proof = mintProof(workerKey, workerSigner, "POST", "wss://bridge.test/ws", 0);
    final var thumbprint = workerKey.toPublicJWK().computeThumbprint().toString();
    final var claims = claimsWith(thumbprint);
    assertEquals(Result.VALID, validator.validate(claims, proof));
    assertEquals(Result.INVALID, validator.validate(claims, proof));
  }

  @Test
  void malformedProof_returnsInvalid() {
    final var claims = claimsWith("any-thumbprint");
    assertEquals(Result.INVALID, validator.validate(claims, "not.a.jwt"));
  }

  @Test
  void nonJwtGarbage_returnsInvalid() {
    final var claims = claimsWith("any-thumbprint");
    assertEquals(Result.INVALID, validator.validate(claims, "completely-garbled"));
  }

  @Test
  void badSignature_returnsInvalid() throws Exception {
    // Sign the proof with a key that does NOT match the embedded jwk. Strategy: build the proof
    // header with workerKey's public jwk embedded, but sign with a different (rogue) signer.
    final var rogueKey = new RSAKeyGenerator(2048).keyID("rogue").generate();
    final var rogueSigner = new RSASSASigner(rogueKey);
    final long nowSec = TimeUnit.NANOSECONDS.toSeconds(CLOCK.nanoTime());
    final var claims =
        new JWTClaimsSet.Builder()
            .jwtID(UUID.randomUUID().toString())
            .issueTime(Date.from(Instant.ofEpochSecond(nowSec)))
            .claim("htm", "POST")
            .claim("htu", "wss://bridge.test/ws")
            .build();
    final var header =
        new JWSHeader.Builder(JWSAlgorithm.RS256)
            .type(new com.nimbusds.jose.JOSEObjectType("dpop+jwt"))
            .jwk(workerKey.toPublicJWK())
            .build();
    final var proof = new SignedJWT(header, claims);
    proof.sign(rogueSigner);
    final var thumbprint = workerKey.toPublicJWK().computeThumbprint().toString();
    final var bearerClaims = claimsWith(thumbprint);
    assertEquals(Result.INVALID, validator.validate(bearerClaims, proof.serialize()));
  }

  @Test
  void missingJwkHeader_returnsInvalid() throws Exception {
    // Proof JWT with no embedded jwk in the header.
    final long nowSec = TimeUnit.NANOSECONDS.toSeconds(CLOCK.nanoTime());
    final var claims =
        new JWTClaimsSet.Builder()
            .jwtID(UUID.randomUUID().toString())
            .issueTime(Date.from(Instant.ofEpochSecond(nowSec)))
            .claim("htm", "POST")
            .claim("htu", "wss://bridge.test/ws")
            .build();
    final var header = new JWSHeader.Builder(JWSAlgorithm.RS256).build();
    final var proof = new SignedJWT(header, claims);
    proof.sign(workerSigner);
    final var bearerClaims = claimsWith("any-thumbprint");
    assertEquals(Result.INVALID, validator.validate(bearerClaims, proof.serialize()));
  }

  @Test
  void missingHtuClaim_returnsInvalid() throws Exception {
    final long nowSec = TimeUnit.NANOSECONDS.toSeconds(CLOCK.nanoTime());
    final var claims =
        new JWTClaimsSet.Builder()
            .jwtID(UUID.randomUUID().toString())
            .issueTime(Date.from(Instant.ofEpochSecond(nowSec)))
            .claim("htm", "POST")
            .build();
    final var header =
        new JWSHeader.Builder(JWSAlgorithm.RS256).jwk(workerKey.toPublicJWK()).build();
    final var proof = new SignedJWT(header, claims);
    proof.sign(workerSigner);
    final var thumbprint = workerKey.toPublicJWK().computeThumbprint().toString();
    final var bearerClaims = claimsWith(thumbprint);
    assertEquals(Result.INVALID, validator.validate(bearerClaims, proof.serialize()));
  }

  @Test
  void missingJtiClaim_returnsInvalid() throws Exception {
    final long nowSec = TimeUnit.NANOSECONDS.toSeconds(CLOCK.nanoTime());
    final var claims =
        new JWTClaimsSet.Builder()
            .issueTime(Date.from(Instant.ofEpochSecond(nowSec)))
            .claim("htm", "POST")
            .claim("htu", "wss://bridge.test/ws")
            .build();
    final var header =
        new JWSHeader.Builder(JWSAlgorithm.RS256).jwk(workerKey.toPublicJWK()).build();
    final var proof = new SignedJWT(header, claims);
    proof.sign(workerSigner);
    final var thumbprint = workerKey.toPublicJWK().computeThumbprint().toString();
    final var bearerClaims = claimsWith(thumbprint);
    assertEquals(Result.INVALID, validator.validate(bearerClaims, proof.serialize()));
  }

  // ---- helpers ----

  /** Build a ValidatedClaims with the given cnf.jkt (or null for no DPoP binding). */
  private static ValidatedClaims claimsWith(final String cnfJkt) {
    return new ValidatedClaims(
        "user-001",
        "bearer-jti-" + UUID.randomUUID(),
        List.of("ACME-001"),
        Long.MAX_VALUE,
        true,
        List.of(),
        cnfJkt);
  }

  /**
   * Mint a DPoP proof JWT.
   *
   * @param key RSA key whose public component is embedded as the proof's {@code jwk} header
   * @param signer matching RSA signer
   * @param htm HTTP method to put in the {@code htm} claim
   * @param htu target URI to put in the {@code htu} claim
   * @param iatOffsetSec offset relative to now for the {@code iat} claim (positive = future,
   *     negative = past)
   * @return serialized compact JWT
   */
  private static String mintProof(
      final RSAKey key,
      final RSASSASigner signer,
      final String htm,
      final String htu,
      final long iatOffsetSec)
      throws JOSEException {
    final long iatSec = TimeUnit.NANOSECONDS.toSeconds(CLOCK.nanoTime()) + iatOffsetSec;
    final var claims =
        new JWTClaimsSet.Builder()
            .jwtID(UUID.randomUUID().toString())
            .issueTime(Date.from(Instant.ofEpochSecond(iatSec)))
            .claim("htm", htm)
            .claim("htu", htu)
            .build();
    final var header =
        new JWSHeader.Builder(JWSAlgorithm.RS256)
            .type(new com.nimbusds.jose.JOSEObjectType("dpop+jwt"))
            .jwk(key.toPublicJWK())
            .build();
    final var proof = new SignedJWT(header, claims);
    proof.sign(signer);
    return proof.serialize();
  }
}
