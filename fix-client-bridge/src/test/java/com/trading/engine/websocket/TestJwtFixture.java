package com.trading.engine.websocket;

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
import java.time.Instant;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.agrona.concurrent.EpochNanoClock;

/**
 * Shared test fixture for building real RS256-signed JWTs and a {@link JwtValidator} backed by an
 * in-memory JWKS (no HTTP) — used by the fix-client-bridge test suite.
 *
 * <p>Lives in the {@code com.trading.engine.websocket} package so it can access the package-private
 * {@link JwtValidator#forTesting} factory without JPMS module violations (no module-info.java in
 * this project).
 *
 * <p>Mirrors the helper pattern in {@code
 * websocket-server/src/test/java/.../JwtValidatorTest.java}.
 *
 * <p><b>Threading.</b> Not thread-safe — intended for single-threaded test setup only.
 *
 * <p><b>Allocation.</b> Allocates on every call — cold path (test only).
 */
public final class TestJwtFixture {

  public static final String ISSUER = "https://auth.bridge.test";
  public static final String AUDIENCE = "wss://bridge.test/ws";
  public static final EpochNanoClock TEST_CLOCK = TradingClocks.epochNanoClock();

  public final RSAKey rsaKey;
  public final RSASSASigner signer;

  public TestJwtFixture() throws Exception {
    rsaKey = new RSAKeyGenerator(2048).keyID("bridge-test-key-1").generate();
    signer = new RSASSASigner(rsaKey);
  }

  /** Build a JwtValidator backed by our in-memory RSA public key (no HTTPS). */
  public JwtValidator buildValidator() {
    final var jwkSet = new JWKSet(rsaKey.toPublicJWK());
    final var jwkSource = new ImmutableJWKSet<SecurityContext>(jwkSet);
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

    // forTesting is package-private in JwtValidator; this class lives in the same package.
    return JwtValidator.forTesting(Map.of(ISSUER, processor), AUDIENCE, TEST_CLOCK);
  }

  /**
   * Mint a valid RS256 JWT with configurable optional claims. Token expires in 15 minutes from the
   * test clock.
   *
   * @param sub subject claim
   * @param jtiId JWT id
   * @param ipPinned value for the {@code ip_pinned} claim; null means claim absent
   * @param roles value for the {@code roles} claim; null means claim absent
   * @return serialized compact JWT string
   */
  public String mintJwt(
      final String sub, final String jtiId, final Boolean ipPinned, final List<String> roles)
      throws Exception {
    final long nowSec = TimeUnit.NANOSECONDS.toSeconds(TEST_CLOCK.nanoTime());
    final var builder =
        new JWTClaimsSet.Builder()
            .issuer(ISSUER)
            .audience(AUDIENCE)
            .subject(sub)
            .expirationTime(Date.from(Instant.ofEpochSecond(nowSec + 900)))
            .notBeforeTime(Date.from(Instant.ofEpochSecond(nowSec - 10)))
            .issueTime(Date.from(Instant.ofEpochSecond(nowSec)))
            .jwtID(jtiId)
            .claim("accounts", List.of("ACME-001"));
    if (ipPinned != null) {
      builder.claim("ip_pinned", ipPinned);
    }
    if (roles != null) {
      builder.claim("roles", roles);
    }
    final var claims = builder.build();
    final var header = new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(rsaKey.getKeyID()).build();
    final var jwt = new SignedJWT(header, claims);
    jwt.sign(signer);
    return jwt.serialize();
  }

  /** Convenience: mint with default sub + fresh jti, ip_pinned=true, empty roles. */
  public String mintValidJwt() throws Exception {
    return mintJwt("user-001", UUID.randomUUID().toString(), true, List.of());
  }

  /**
   * Mint a JWT signed with a DIFFERENT key — produces a signature that the validator will reject.
   */
  public String mintJwtWithWrongKey(final String sub) throws Exception {
    final var wrongKey = new RSAKeyGenerator(2048).keyID("wrong-key").generate();
    final var wrongSigner = new RSASSASigner(wrongKey);
    final long nowSec = TimeUnit.NANOSECONDS.toSeconds(TEST_CLOCK.nanoTime());
    final var claims =
        new JWTClaimsSet.Builder()
            .issuer(ISSUER)
            .audience(AUDIENCE)
            .subject(sub)
            .expirationTime(Date.from(Instant.ofEpochSecond(nowSec + 900)))
            .notBeforeTime(Date.from(Instant.ofEpochSecond(nowSec - 10)))
            .issueTime(Date.from(Instant.ofEpochSecond(nowSec)))
            .jwtID(UUID.randomUUID().toString())
            .claim("accounts", List.of("ACME-001"))
            .build();
    final var header = new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(wrongKey.getKeyID()).build();
    final var jwt = new SignedJWT(header, claims);
    jwt.sign(wrongSigner);
    return jwt.serialize();
  }
}
