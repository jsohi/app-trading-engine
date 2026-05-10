package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.crypto.MACSigner;
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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link JwtValidator} -- verifies RS256 validation, algorithm rejection, claim
 * validation, kid enforcement, audience matching, iat age check, and accounts claim extraction.
 *
 * <p>Uses a locally generated RSA key pair via nimbus {@link RSAKeyGenerator}. No external JWKS
 * endpoint is needed -- the tests use the package-private constructor with {@link ImmutableJWKSet}.
 */
final class JwtValidatorTest {

  private static RSAKey rsaKey;
  private static JWSSigner rsaSigner;

  private static final String ISSUER = "https://auth.trading.test";
  private static final String AUDIENCE = "wss://trading.test/ws";
  private static final String JWKS_URL = "https://auth.trading.test/.well-known/jwks.json";

  @BeforeAll
  static void generateKeys() throws Exception {
    rsaKey = new RSAKeyGenerator(2048).keyID("test-key-1").generate();
    rsaSigner = new RSASSASigner(rsaKey);
  }

  // --- Valid token ---

  @Test
  void validate_validRs256Token_returnsValidatedClaims() throws Exception {
    final var validator = buildTestValidator();
    final String jwt = buildValidJwt().serialize();

    final var claims = validator.validate(jwt);

    assertEquals("user-001", claims.sub());
    assertNotNull(claims.jti());
    assertEquals(List.of("ACME-001", "HEDGE-002"), claims.accounts());
    assertTrue(claims.expiryEpochSec() > nowFromTestClock().getEpochSecond());
  }

  // --- Algorithm rejection ---

  @Test
  void validate_hs256Algorithm_throwsException() throws Exception {
    final var validator = buildTestValidator();

    final byte[] secret = new byte[32];
    java.util.Arrays.fill(secret, (byte) 0x42);
    final var signer = new MACSigner(secret);
    final var header = new JWSHeader.Builder(JWSAlgorithm.HS256).keyID("hs-key").build();
    final var jwt = new SignedJWT(header, buildValidClaimsSet());
    jwt.sign(signer);

    final var ex =
        assertThrows(
            JwtValidator.JwtValidationException.class, () -> validator.validate(jwt.serialize()));
    assertTrue(ex.getMessage().contains("Unsupported algorithm"));
  }

  // --- Missing kid ---

  @Test
  void validate_missingKid_throwsException() throws Exception {
    final var validator = buildTestValidator();

    final var header = new JWSHeader.Builder(JWSAlgorithm.RS256).build(); // no kid
    final var jwt = new SignedJWT(header, buildValidClaimsSet());
    jwt.sign(rsaSigner);

    final var ex =
        assertThrows(
            JwtValidator.JwtValidationException.class, () -> validator.validate(jwt.serialize()));
    assertTrue(ex.getMessage().contains("Missing kid"));
  }

  // --- Missing claims ---

  @Test
  void validate_missingSub_throwsException() throws Exception {
    final var validator = buildTestValidator();
    final var claims =
        new JWTClaimsSet.Builder()
            .issuer(ISSUER)
            .audience(AUDIENCE)
            .expirationTime(Date.from(nowFromTestClock().plusSeconds(900)))
            .notBeforeTime(Date.from(nowFromTestClock().minusSeconds(10)))
            .issueTime(Date.from(nowFromTestClock()))
            .jwtID(UUID.randomUUID().toString())
            .claim("accounts", List.of("ACME-001"))
            .build();

    final var ex =
        assertThrows(
            JwtValidator.JwtValidationException.class,
            () -> validator.validate(signWithRs256(claims)));
    assertTrue(ex.getMessage().toLowerCase().contains("sub"));
  }

  @Test
  void validate_missingJti_throwsException() throws Exception {
    final var validator = buildTestValidator();
    final var claims =
        new JWTClaimsSet.Builder()
            .issuer(ISSUER)
            .audience(AUDIENCE)
            .subject("user-001")
            .expirationTime(Date.from(nowFromTestClock().plusSeconds(900)))
            .notBeforeTime(Date.from(nowFromTestClock().minusSeconds(10)))
            .issueTime(Date.from(nowFromTestClock()))
            .claim("accounts", List.of("ACME-001"))
            .build();

    final var ex =
        assertThrows(
            JwtValidator.JwtValidationException.class,
            () -> validator.validate(signWithRs256(claims)));
    assertTrue(ex.getMessage().toLowerCase().contains("jti"));
  }

  @Test
  void validate_missingAccounts_throwsException() throws Exception {
    final var validator = buildTestValidator();
    final var claims =
        new JWTClaimsSet.Builder()
            .issuer(ISSUER)
            .audience(AUDIENCE)
            .subject("user-001")
            .expirationTime(Date.from(nowFromTestClock().plusSeconds(900)))
            .notBeforeTime(Date.from(nowFromTestClock().minusSeconds(10)))
            .issueTime(Date.from(nowFromTestClock()))
            .jwtID(UUID.randomUUID().toString())
            .build();

    final var ex =
        assertThrows(
            JwtValidator.JwtValidationException.class,
            () -> validator.validate(signWithRs256(claims)));
    assertTrue(ex.getMessage().contains("accounts"));
  }

  @Test
  void validate_emptyAccountsList_throwsException() throws Exception {
    final var validator = buildTestValidator();
    final var claims =
        new JWTClaimsSet.Builder()
            .issuer(ISSUER)
            .audience(AUDIENCE)
            .subject("user-001")
            .expirationTime(Date.from(nowFromTestClock().plusSeconds(900)))
            .notBeforeTime(Date.from(nowFromTestClock().minusSeconds(10)))
            .issueTime(Date.from(nowFromTestClock()))
            .jwtID(UUID.randomUUID().toString())
            .claim("accounts", List.of())
            .build();

    final var ex =
        assertThrows(
            JwtValidator.JwtValidationException.class,
            () -> validator.validate(signWithRs256(claims)));
    assertTrue(ex.getMessage().contains("accounts"));
  }

  // --- Expired token ---

  @Test
  void validate_expiredToken_throwsException() throws Exception {
    final var validator = buildTestValidator();
    final var claims =
        new JWTClaimsSet.Builder()
            .issuer(ISSUER)
            .audience(AUDIENCE)
            .subject("user-001")
            .expirationTime(Date.from(nowFromTestClock().minusSeconds(60)))
            .notBeforeTime(Date.from(nowFromTestClock().minusSeconds(900)))
            .issueTime(Date.from(nowFromTestClock().minusSeconds(900)))
            .jwtID(UUID.randomUUID().toString())
            .claim("accounts", List.of("ACME-001"))
            .build();

    assertThrows(
        JwtValidator.JwtValidationException.class, () -> validator.validate(signWithRs256(claims)));
  }

  // --- iat too old ---

  @Test
  void validate_iatTooOld_throwsException() throws Exception {
    final var validator = buildTestValidator();
    final var claims =
        new JWTClaimsSet.Builder()
            .issuer(ISSUER)
            .audience(AUDIENCE)
            .subject("user-001")
            .expirationTime(Date.from(nowFromTestClock().plusSeconds(900)))
            .notBeforeTime(Date.from(nowFromTestClock().minusSeconds(1800)))
            .issueTime(Date.from(nowFromTestClock().minusSeconds(1800)))
            .jwtID(UUID.randomUUID().toString())
            .claim("accounts", List.of("ACME-001"))
            .build();

    final var ex =
        assertThrows(
            JwtValidator.JwtValidationException.class,
            () -> validator.validate(signWithRs256(claims)));
    assertTrue(ex.getMessage().contains("issued too long ago"));
  }

  // --- Unknown issuer ---

  @Test
  void validate_unknownIssuer_throwsException() throws Exception {
    final var validator = buildTestValidator();
    final var claims =
        new JWTClaimsSet.Builder()
            .issuer("https://unknown-issuer.test")
            .audience(AUDIENCE)
            .subject("user-001")
            .expirationTime(Date.from(nowFromTestClock().plusSeconds(900)))
            .notBeforeTime(Date.from(nowFromTestClock().minusSeconds(10)))
            .issueTime(Date.from(nowFromTestClock()))
            .jwtID(UUID.randomUUID().toString())
            .claim("accounts", List.of("ACME-001"))
            .build();

    final var ex =
        assertThrows(
            JwtValidator.JwtValidationException.class,
            () -> validator.validate(signWithRs256(claims)));
    assertTrue(ex.getMessage().contains("Unknown issuer"));
  }

  // --- Constructor validation ---

  @Test
  void constructor_emptyAudience_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new JwtValidator(Map.of(ISSUER, JWKS_URL), "", TEST_CLOCK));
  }

  @Test
  void constructor_httpJwksUrl_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new JwtValidator(Map.of(ISSUER, "http://insecure.test/jwks"), AUDIENCE, TEST_CLOCK));
  }

  // --- Clock skew tolerance ---

  @Test
  void validate_expiredWithin5sSkew_succeeds() throws Exception {
    final var validator = buildTestValidator();
    final var claims =
        new JWTClaimsSet.Builder()
            .issuer(ISSUER)
            .audience(AUDIENCE)
            .subject("user-001")
            .expirationTime(Date.from(nowFromTestClock().minusSeconds(3)))
            .notBeforeTime(Date.from(nowFromTestClock().minusSeconds(600)))
            .issueTime(Date.from(nowFromTestClock().minusSeconds(600)))
            .jwtID(UUID.randomUUID().toString())
            .claim("accounts", List.of("ACME-001"))
            .build();

    final var result = validator.validate(signWithRs256(claims));
    assertEquals("user-001", result.sub());
  }

  // --- Accounts claim formats ---

  @Test
  void validate_singleAccountString_extractsAsList() throws Exception {
    final var validator = buildTestValidator();
    final var claims =
        new JWTClaimsSet.Builder()
            .issuer(ISSUER)
            .audience(AUDIENCE)
            .subject("user-001")
            .expirationTime(Date.from(nowFromTestClock().plusSeconds(900)))
            .notBeforeTime(Date.from(nowFromTestClock().minusSeconds(10)))
            .issueTime(Date.from(nowFromTestClock()))
            .jwtID(UUID.randomUUID().toString())
            .claim("accounts", "SINGLE-ACCT")
            .build();

    final var result = validator.validate(signWithRs256(claims));
    assertEquals(List.of("SINGLE-ACCT"), result.accounts());
  }

  // --- Missing iss claim ---

  @Test
  void validate_missingIssuer_throwsException() throws Exception {
    final var validator = buildTestValidator();
    final var claims =
        new JWTClaimsSet.Builder()
            .audience(AUDIENCE)
            .subject("user-001")
            .expirationTime(Date.from(nowFromTestClock().plusSeconds(900)))
            .notBeforeTime(Date.from(nowFromTestClock().minusSeconds(10)))
            .issueTime(Date.from(nowFromTestClock()))
            .jwtID(UUID.randomUUID().toString())
            .claim("accounts", List.of("ACME-001"))
            .build();

    final var ex =
        assertThrows(
            JwtValidator.JwtValidationException.class,
            () -> validator.validate(signWithRs256(claims)));
    assertTrue(ex.getMessage().contains("iss"));
  }

  // --- Wrong audience ---

  @Test
  void validate_wrongAudience_throwsException() throws Exception {
    final var validator = buildTestValidator();
    final var claims =
        new JWTClaimsSet.Builder()
            .issuer(ISSUER)
            .audience("https://wrong-service.test") // wrong audience
            .subject("user-001")
            .expirationTime(Date.from(nowFromTestClock().plusSeconds(900)))
            .notBeforeTime(Date.from(nowFromTestClock().minusSeconds(10)))
            .issueTime(Date.from(nowFromTestClock()))
            .jwtID(UUID.randomUUID().toString())
            .claim("accounts", List.of("ACME-001"))
            .build();

    assertThrows(
        JwtValidator.JwtValidationException.class, () -> validator.validate(signWithRs256(claims)));
  }

  // --- Wrong signing key ---

  @Test
  void validate_wrongSigningKey_throwsException() throws Exception {
    final var validator = buildTestValidator();
    final var wrongKey = new RSAKeyGenerator(2048).keyID("wrong-key").generate();
    final var wrongSigner = new RSASSASigner(wrongKey);

    final var header = new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(wrongKey.getKeyID()).build();
    final var jwt = new SignedJWT(header, buildValidClaimsSet());
    jwt.sign(wrongSigner);

    assertThrows(
        JwtValidator.JwtValidationException.class, () -> validator.validate(jwt.serialize()));
  }

  // --- Malformed JWT ---

  @Test
  void validate_malformedJwt_throwsException() {
    final var validator = buildTestValidator();

    assertThrows(JwtValidator.JwtValidationException.class, () -> validator.validate("not-a-jwt"));
  }

  // --- Null JWT ---

  @Test
  void validate_nullJwt_throwsNullPointerException() {
    final var validator = buildTestValidator();

    assertThrows(NullPointerException.class, () -> validator.validate(null));
  }

  // --- Helpers ---

  /** Project epoch clock — used by both JwtValidator and claim construction for consistency. */
  private static final EpochNanoClock TEST_CLOCK = TradingClocks.epochNanoClock();

  /**
   * Derive an Instant from the same clock source as TEST_CLOCK, ensuring claim construction and
   * validation use the same time reference. Eliminates timing sensitivity from clock drift between
   * nowFromTestClock() and TEST_CLOCK.
   */
  private static Instant nowFromTestClock() {
    return Instant.ofEpochSecond(TimeUnit.NANOSECONDS.toSeconds(TEST_CLOCK.nanoTime()));
  }

  /** Build a JwtValidator using the package-private factory with an ImmutableJWKSet (no HTTP). */
  private static JwtValidator buildTestValidator() {
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

    return JwtValidator.forTesting(Map.of(ISSUER, processor), AUDIENCE, TEST_CLOCK);
  }

  private static SignedJWT buildValidJwt() throws Exception {
    final var header = new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(rsaKey.getKeyID()).build();
    final var jwt = new SignedJWT(header, buildValidClaimsSet());
    jwt.sign(rsaSigner);
    return jwt;
  }

  private static JWTClaimsSet buildValidClaimsSet() {
    return new JWTClaimsSet.Builder()
        .issuer(ISSUER)
        .audience(AUDIENCE)
        .subject("user-001")
        .expirationTime(Date.from(nowFromTestClock().plusSeconds(900)))
        .notBeforeTime(Date.from(nowFromTestClock().minusSeconds(10)))
        .issueTime(Date.from(nowFromTestClock()))
        .jwtID(UUID.randomUUID().toString())
        .claim("accounts", List.of("ACME-001", "HEDGE-002"))
        .build();
  }

  private static String signWithRs256(final JWTClaimsSet claims) throws Exception {
    final var header = new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(rsaKey.getKeyID()).build();
    final var jwt = new SignedJWT(header, claims);
    jwt.sign(rsaSigner);
    return jwt.serialize();
  }

  // --- ip_pinned claim ---

  @Test
  void validate_ipPinnedClaimAbsent_defaultsToTrue() throws Exception {
    final var validator = buildTestValidator();
    // buildValidClaimsSet() does not add ip_pinned → claim absent → must default to true.
    final var claims = signWithRs256(buildValidClaimsSet());

    final var result = validator.validate(claims);

    assertTrue(result.ipPinned(), "Absent ip_pinned claim must default to true (fail-secure)");
  }

  @Test
  void validate_ipPinnedClaimFalse_returnsFalse() throws Exception {
    final var validator = buildTestValidator();
    final var claims =
        new JWTClaimsSet.Builder(buildValidClaimsSet()).claim("ip_pinned", false).build();

    final var result = validator.validate(signWithRs256(claims));

    assertFalse(result.ipPinned(), "Explicit ip_pinned=false must be honoured");
  }

  @Test
  void validate_ipPinnedClaimNonBoolean_defaultsToTrue() throws Exception {
    final var validator = buildTestValidator();
    // Non-boolean value "yes" → fail-secure → true.
    final var claims =
        new JWTClaimsSet.Builder(buildValidClaimsSet()).claim("ip_pinned", "yes").build();

    final var result = validator.validate(signWithRs256(claims));

    assertTrue(result.ipPinned(), "Non-boolean ip_pinned claim must default to true (fail-secure)");
  }

  // --- roles claim ---

  @Test
  void validate_rolesClaimList_returnsList() throws Exception {
    final var validator = buildTestValidator();
    final var claims =
        new JWTClaimsSet.Builder(buildValidClaimsSet())
            .claim("roles", List.of("audit_view", "trader"))
            .build();

    final var result = validator.validate(signWithRs256(claims));

    assertEquals(List.of("audit_view", "trader"), result.roles());
  }

  @Test
  void validate_rolesClaimSingleString_wrappedAsList() throws Exception {
    final var validator = buildTestValidator();
    final var claims =
        new JWTClaimsSet.Builder(buildValidClaimsSet()).claim("roles", "audit_view").build();

    final var result = validator.validate(signWithRs256(claims));

    assertEquals(
        List.of("audit_view"),
        result.roles(),
        "Single-string roles claim must be wrapped in a list");
  }

  @Test
  void validate_rolesClaimAbsent_emptyList() throws Exception {
    final var validator = buildTestValidator();
    // buildValidClaimsSet() has no roles claim.
    final var result = validator.validate(signWithRs256(buildValidClaimsSet()));

    assertTrue(result.roles().isEmpty(), "Absent roles claim must yield an empty list");
  }
}
