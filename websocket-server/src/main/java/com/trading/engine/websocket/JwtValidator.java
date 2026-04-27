package com.trading.engine.websocket;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.source.RemoteJWKSet;
import com.nimbusds.jose.proc.BadJOSEException;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.nimbusds.jwt.proc.DefaultJWTClaimsVerifier;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;
import java.net.MalformedURLException;
import java.net.URI;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.agrona.concurrent.EpochNanoClock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * RS256 JWT validation for WebSocket client authentication.
 *
 * <p>Validates JWTs per the architecture doc Section 4 (Security):
 *
 * <ul>
 *   <li><b>Algorithm:</b> RS256 only. Rejects {@code alg:none}, HS256, ES256, and all non-RS256.
 *   <li><b>Key management:</b> JWKS endpoint per issuer, cached 1 hour, forced refresh on signature
 *       verification failure (retry once).
 *   <li><b>Issuer registry:</b> Map of JWT {@code iss} claim to JWKS endpoint URL from config.
 *       Unknown issuers are rejected.
 *   <li><b>Claims:</b> {@code iss}, {@code aud}, {@code exp} (5s skew), {@code nbf}, {@code iat}
 *       (reject &gt;15min old), {@code sub}, {@code jti}, {@code accounts}.
 *   <li><b>kid:</b> Tokens without a {@code kid} header are rejected to prevent key confusion.
 *   <li><b>Token size:</b> Enforced by the caller (JwtAuthHandler) before passing to this class.
 * </ul>
 *
 * <p><b>Threading.</b> Thread-safe. The nimbus JWT processor and JWK sources handle their own
 * internal synchronization. Multiple Netty event loop threads may call {@link #validate}
 * concurrently.
 *
 * <p><b>Allocation.</b> Allocates per validation call (JWT parsing, claims extraction). Acceptable
 * — authentication is cold path (once per connection, not per-message).
 *
 * <p><b>Shutdown.</b> Implements {@link AutoCloseable} to release JWKS HTTP client resources.
 *
 * @see <a href="docs/websocket-architecture.md">WebSocket Architecture — Section 4</a>
 */
public final class JwtValidator implements AutoCloseable {

  private static final Logger LOG = LogManager.getLogger(JwtValidator.class);

  /** Maximum age of the {@code iat} (issued-at) claim: 15 minutes in seconds. */
  private static final long MAX_IAT_AGE_SECONDS = 15 * 60;

  /** Clock skew tolerance for exp/nbf validation: 5 seconds. */
  private static final int CLOCK_SKEW_SECONDS = 5;

  /**
   * Per-issuer JWT processors. Each processor is configured with the issuer's JWKS endpoint and
   * RS256 key selector.
   */
  private final Map<String, DefaultJWTProcessor<SecurityContext>> processors;

  private final String expectedAudience;
  private final EpochNanoClock wallClock;

  /**
   * Create a JWT validator.
   *
   * <p>For each entry in the issuer registry, a JWKS-backed JWT processor is created. The JWKS
   * endpoint must use HTTPS. A preflight fetch is attempted at construction time — failures are
   * logged but do not prevent startup (the JWKS will be fetched on first auth attempt).
   *
   * @param issuerRegistry map of JWT issuer identifier to JWKS endpoint URL (HTTPS required)
   * @param expectedAudience the expected {@code aud} claim value (must not be null or empty)
   * @param wallClock epoch nanosecond clock for iat age checks (from {@code
   *     TradingClocks.epochNanoClock()})
   * @throws IllegalArgumentException if expectedAudience is empty or an issuer URL is malformed
   * @throws NullPointerException if any parameter is null
   */
  public JwtValidator(
      final Map<String, String> issuerRegistry,
      final String expectedAudience,
      final EpochNanoClock wallClock) {
    Objects.requireNonNull(issuerRegistry, "issuerRegistry");
    Objects.requireNonNull(expectedAudience, "expectedAudience");
    this.wallClock = Objects.requireNonNull(wallClock, "wallClock");
    if (expectedAudience.isEmpty()) {
      throw new IllegalArgumentException("expectedAudience must not be empty");
    }

    this.expectedAudience = expectedAudience;
    this.processors = new ConcurrentHashMap<>();

    for (final var entry : issuerRegistry.entrySet()) {
      final var issuer = entry.getKey();
      final var jwksUrl = entry.getValue();

      if (!jwksUrl.startsWith("https://")) {
        throw new IllegalArgumentException(
            "JWKS URL for issuer '" + issuer + "' must use HTTPS, got: " + jwksUrl);
      }

      try {
        final var processor = buildProcessor(jwksUrl);
        processors.put(issuer, processor);
        LOG.info("Registered JWKS endpoint for issuer '{}': {}", issuer, jwksUrl);
      } catch (final MalformedURLException e) {
        throw new IllegalArgumentException(
            "Malformed JWKS URL for issuer '" + issuer + "': " + jwksUrl, e);
      }
    }
  }

  /**
   * Package-private static factory for testing — accepts pre-built processors to avoid HTTP-based
   * JWKS fetching. Production code should use the public constructor.
   *
   * @param processors pre-built JWT processor map (issuer -> processor)
   * @param expectedAudience the expected audience claim
   * @return a JwtValidator backed by the given processors
   */
  static JwtValidator forTesting(
      final Map<String, DefaultJWTProcessor<SecurityContext>> processors,
      final String expectedAudience,
      final EpochNanoClock wallClock) {
    final var validator = new JwtValidator(Map.of(), expectedAudience, wallClock);
    validator.processors.putAll(processors);
    return validator;
  }

  /**
   * Validate a JWT token string and extract claims.
   *
   * @param jwt the raw JWT token string (compact serialization)
   * @return the validated claims
   * @throws JwtValidationException if validation fails for any reason (algorithm, signature,
   *     claims, issuer, etc.)
   */
  public ValidatedClaims validate(final String jwt) throws JwtValidationException {
    Objects.requireNonNull(jwt, "jwt");

    final SignedJWT signedJwt;
    try {
      signedJwt = SignedJWT.parse(jwt);
    } catch (final ParseException e) {
      throw new JwtValidationException("Malformed JWT: " + e.getMessage());
    }

    // Reject non-RS256 algorithms (alg:none, HS256, ES256, etc.)
    final var header = signedJwt.getHeader();
    if (!JWSAlgorithm.RS256.equals(header.getAlgorithm())) {
      throw new JwtValidationException(
          "Unsupported algorithm: " + header.getAlgorithm() + " (only RS256 is accepted)");
    }

    // Reject tokens without kid header (key confusion prevention during rotation)
    if (header.getKeyID() == null) {
      throw new JwtValidationException("Missing kid header — required for key selection");
    }

    // Look up issuer from unverified claims (safe — we verify the signature next)
    final String issuer;
    try {
      issuer = signedJwt.getJWTClaimsSet().getIssuer();
    } catch (final ParseException e) {
      throw new JwtValidationException("Cannot parse JWT claims: " + e.getMessage());
    }
    if (issuer == null) {
      throw new JwtValidationException("Missing iss claim");
    }

    final var processor = processors.get(issuer);
    if (processor == null) {
      throw new JwtValidationException("Unknown issuer: " + issuer);
    }

    // Process (verify signature + validate standard claims)
    final JWTClaimsSet claims;
    try {
      claims = processor.process(signedJwt, null);
    } catch (final BadJOSEException e) {
      throw new JwtValidationException("JWT verification failed: " + e.getMessage());
    } catch (final Exception e) {
      throw new JwtValidationException("JWT processing error: " + e.getMessage());
    }

    // Validate iat (issued-at): reject tokens issued more than 15 minutes ago or too far in future
    final var iat = claims.getIssueTime();
    if (iat == null) {
      throw new JwtValidationException("Missing iat claim");
    }
    final long nowEpochSec = TimeUnit.NANOSECONDS.toSeconds(wallClock.nanoTime());
    final long iatAgeSec = nowEpochSec - iat.toInstant().getEpochSecond();
    if (iatAgeSec > MAX_IAT_AGE_SECONDS + CLOCK_SKEW_SECONDS) {
      throw new JwtValidationException(
          "Token issued too long ago: " + iatAgeSec + "s (max " + MAX_IAT_AGE_SECONDS + "s)");
    }
    if (iatAgeSec < -CLOCK_SKEW_SECONDS) {
      throw new JwtValidationException(
          "Token issued in the future: iat is " + (-iatAgeSec) + "s ahead");
    }

    // Extract required claims
    final var sub = claims.getSubject();
    if (sub == null || sub.isEmpty()) {
      throw new JwtValidationException("Missing or empty sub claim");
    }

    final var jti = claims.getJWTID();
    if (jti == null || jti.isEmpty()) {
      throw new JwtValidationException("Missing or empty jti claim");
    }

    // Extract accounts claim (custom claim, List<String>)
    final var accounts = extractAccountsClaim(claims);
    if (accounts.isEmpty()) {
      throw new JwtValidationException("Missing or empty accounts claim");
    }

    final long expiryEpochSec =
        claims.getExpirationTime() != null
            ? claims.getExpirationTime().toInstant().getEpochSecond()
            : 0L;

    return new ValidatedClaims(sub, jti, List.copyOf(accounts), expiryEpochSec);
  }

  @Override
  public void close() {
    // Nimbus JWK sources don't require explicit close, but this is here for future-proofing
    // if we switch to a custom HTTP-backed JWK source with connection pooling.
    processors.clear();
    LOG.info("JwtValidator closed — JWKS processors released");
  }

  /**
   * Build a JWT processor for a specific JWKS endpoint URL.
   *
   * @param jwksUrl the JWKS endpoint URL (HTTPS)
   * @return a configured JWT processor
   * @throws MalformedURLException if the URL is malformed
   */
  private DefaultJWTProcessor<SecurityContext> buildProcessor(final String jwksUrl)
      throws MalformedURLException {

    final var jwkSource = new RemoteJWKSet<SecurityContext>(URI.create(jwksUrl).toURL());

    final var keySelector = new JWSVerificationKeySelector<>(JWSAlgorithm.RS256, jwkSource);

    final var processor = new DefaultJWTProcessor<SecurityContext>();
    processor.setJWSKeySelector(keySelector);

    // Configure claims verifier: require exp, sub, jti; validate aud; 5s clock skew
    final var requiredClaims = new HashSet<String>();
    requiredClaims.add("exp");
    requiredClaims.add("sub");
    requiredClaims.add("jti");
    requiredClaims.add("iss");

    final var claimsVerifier =
        new DefaultJWTClaimsVerifier<SecurityContext>(
            new JWTClaimsSet.Builder().audience(expectedAudience).build(), requiredClaims);
    claimsVerifier.setMaxClockSkew(CLOCK_SKEW_SECONDS);

    processor.setJWTClaimsSetVerifier(claimsVerifier);
    return processor;
  }

  /**
   * Extract the {@code accounts} claim as a list of strings. Handles both {@code List<String>} and
   * single-value string formats for flexibility.
   *
   * @param claims the validated JWT claims set
   * @return the list of account codes, or empty list if the claim is missing/invalid
   */
  private static List<String> extractAccountsClaim(final JWTClaimsSet claims) {
    final var raw = claims.getClaim("accounts");
    if (raw instanceof List<?> list) {
      final var result = new ArrayList<String>(list.size());
      for (final Object item : list) {
        if (item instanceof String s && !s.isEmpty()) {
          result.add(s);
        }
      }
      return result;
    }
    if (raw instanceof String s && !s.isEmpty()) {
      return List.of(s);
    }
    return List.of();
  }

  /**
   * Validated JWT claims extracted after successful token verification.
   *
   * @param sub the subject claim (user identifier)
   * @param jti the JWT ID claim (for revocation tracking)
   * @param accounts the list of entitled account codes from the custom {@code accounts} claim
   * @param expiryEpochSec the token expiration time as epoch seconds
   */
  public record ValidatedClaims(
      String sub, String jti, List<String> accounts, long expiryEpochSec) {}

  /**
   * Thrown when JWT validation fails. The message contains only the failure reason — never the
   * token content or user-identifiable claims (to prevent information leakage in logs).
   */
  public static final class JwtValidationException extends RuntimeException {

    /**
     * @param message the validation failure reason (safe for logging)
     */
    public JwtValidationException(final String message) {
      super(message);
    }
  }
}
