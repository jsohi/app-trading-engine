package com.trading.engine.websocket;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.JWKMatcher;
import com.nimbusds.jose.jwk.JWKSelector;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.jwk.source.RemoteJWKSet;
import com.nimbusds.jose.proc.BadJOSEException;
import com.nimbusds.jose.proc.BadJWSException;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jose.util.DefaultResourceRetriever;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.nimbusds.jwt.proc.DefaultJWTClaimsVerifier;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;
import java.net.MalformedURLException;
import java.net.URI;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
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

  /**
   * Per-issuer {@link JWKSource} references — used by {@link #preflightOrThrow()} to perform a
   * direct reachability test against each configured issuer. Populated only for processors built by
   * the public constructor (i.e., real {@link RemoteJWKSet} sources). Test-only processors inserted
   * via {@link #forTesting} are absent here so {@link #preflightOrThrow()} treats them as trivially
   * reachable.
   */
  private final Map<String, JWKSource<SecurityContext>> jwkSources;

  /**
   * Empty-matcher selector reused across {@link #preflightOrThrow()} calls so the preflight loop
   * does not allocate per issuer.
   */
  private static final JWKSelector PREFLIGHT_SELECTOR =
      new JWKSelector(new JWKMatcher.Builder().build());

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
    this.jwkSources = new ConcurrentHashMap<>();

    for (final var entry : issuerRegistry.entrySet()) {
      final var issuer = entry.getKey();
      final var jwksUrl = entry.getValue();

      if (!jwksUrl.startsWith("https://")) {
        throw new IllegalArgumentException(
            "JWKS URL for issuer '" + issuer + "' must use HTTPS, got: " + jwksUrl);
      }

      try {
        final var built = buildProcessor(jwksUrl);
        processors.put(issuer, built.processor);
        jwkSources.put(issuer, built.jwkSource);
        LOG.info("Registered JWKS endpoint for issuer '{}': {}", issuer, jwksUrl);
      } catch (final MalformedURLException e) {
        throw new IllegalArgumentException(
            "Malformed JWKS URL for issuer '" + issuer + "': " + jwksUrl, e);
      }
    }

    // Best-effort preflight at startup: attempt to fetch the JWKS for each issuer so DNS errors,
    // unreachable hosts, and TLS misconfiguration surface as ERROR log lines rather than as
    // first-auth failures. Failures are logged but do not prevent construction — operators that
    // require fail-fast semantics call {@link #preflightOrThrow()} after the constructor.
    for (final var entry : jwkSources.entrySet()) {
      try {
        entry.getValue().get(PREFLIGHT_SELECTOR, null);
      } catch (final Exception e) {
        LOG.error(
            "Preflight JWKS fetch failed for issuer '{}': {}", entry.getKey(), e.getMessage());
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
      throw new JwtValidationException("Malformed JWT: " + e.getMessage(), e);
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
      throw new JwtValidationException("Cannot parse JWT claims: " + e.getMessage(), e);
    }
    if (issuer == null) {
      throw new JwtValidationException("Missing iss claim");
    }

    final var processor = processors.get(issuer);
    if (processor == null) {
      throw new JwtValidationException("Unknown issuer: " + issuer);
    }

    // Process (verify signature + validate standard claims).
    // On BadJWSException (signature verification failure), force-refresh JWKS and retry once.
    // This handles IdP key rotation where the local cache may have a stale key set.
    // RemoteJWKSet automatically refreshes its cache on the next get() call after a failed
    // verification, so re-processing the same JWT triggers a JWKS fetch.
    final var claims = processWithRetry(signedJwt, processor);

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
    jwkSources.clear();
    LOG.info("JwtValidator closed — JWKS processors released");
  }

  /**
   * Re-runs the JWKS preflight fetch performed by the constructor and throws on the first
   * unreachable issuer. Unlike the constructor's preflight (which only logs failures so partial
   * misconfiguration does not block startup), this method is fail-fast and intended for callers
   * that require all configured issuers to be reachable before serving traffic (for example, the
   * FIX client bridge starts its WebSocket listener only after this returns successfully).
   *
   * <p><b>Threading.</b> Safe to call from any thread; performs blocking JWKS HTTPS GETs. Should be
   * invoked from a startup thread, never from a Netty event loop.
   *
   * <p><b>Allocation.</b> Allocates per HTTP request — startup-only, not a hot path.
   *
   * <p>Test-only processors registered via the package-private {@code forTesting} factory are not
   * preflighted: in-memory {@link com.nimbusds.jose.jwk.source.ImmutableJWKSet} sources need no
   * reachability check.
   *
   * @throws JwtValidationException on the first JWKS endpoint that is unreachable, returns a
   *     malformed JWKS document, or fails TLS validation. The exception message names the issuer
   *     and the underlying cause.
   */
  public void preflightOrThrow() {
    for (final var entry : jwkSources.entrySet()) {
      try {
        entry.getValue().get(PREFLIGHT_SELECTOR, null);
      } catch (final Exception e) {
        throw new JwtValidationException(
            "JWKS preflight failed for issuer '" + entry.getKey() + "': " + e.getMessage(), e);
      }
    }
    LOG.info("JWKS preflight succeeded for {} issuer(s)", jwkSources.size());
  }

  /**
   * Process a signed JWT with one retry on signature verification failure. On {@link
   * BadJWSException}, the processor's {@link RemoteJWKSet} will automatically refresh its JWKS
   * cache on the next key selection attempt, so re-processing the JWT triggers a fresh fetch.
   *
   * @param signedJwt the parsed JWT
   * @param processor the issuer-specific JWT processor
   * @return the validated claims set
   * @throws JwtValidationException if validation fails after retry
   */
  private static JWTClaimsSet processWithRetry(
      final SignedJWT signedJwt, final DefaultJWTProcessor<SecurityContext> processor)
      throws JwtValidationException {
    try {
      return processor.process(signedJwt, null);
    } catch (final BadJWSException e) {
      // Signature failure — retry once. Note: RemoteJWKSet refreshes its cache on kid-not-found,
      // not on bad-signature for the same kid. The retry is effective when the IdP has rotated to
      // a new kid that isn't in the cached set. For same-kid rotation (key replacement), the
      // retry may not help — this is a known limitation of nimbus 10.3. Full forced-refresh
      // requires JWKSourceBuilder (nimbus 10.7+) or manual cache eviction.
      LOG.warn("JWT signature failed, retrying with refreshed JWKS: {}", e.getMessage());
      try {
        return processor.process(SignedJWT.parse(signedJwt.serialize()), null);
      } catch (final Exception retryEx) {
        throw new JwtValidationException(
            "JWT verification failed after JWKS refresh: " + retryEx.getMessage(), retryEx);
      }
    } catch (final BadJOSEException e) {
      throw new JwtValidationException("JWT verification failed: " + e.getMessage(), e);
    } catch (final Exception e) {
      throw new JwtValidationException("JWT processing error: " + e.getMessage(), e);
    }
  }

  /**
   * Build a JWT processor for a specific JWKS endpoint URL.
   *
   * @param jwksUrl the JWKS endpoint URL (HTTPS)
   * @return a configured JWT processor paired with the underlying remote JWK source (preserved for
   *     direct preflight reachability checks)
   * @throws MalformedURLException if the URL is malformed
   */
  private BuiltProcessor buildProcessor(final String jwksUrl) throws MalformedURLException {

    // Configure HTTP retriever with explicit timeouts and SSRF hardening per architecture doc
    // Section 4: 5s connect + 5s read prevents Netty event loop blocking if IdP is unresponsive.
    // 256KB size limit prevents a compromised IdP from serving oversized JWKS responses.
    // Redirects disabled to prevent SSRF via DNS rebinding or open-redirect attacks.
    final int timeoutMs = 5_000;
    final int maxSizeBytes = 256_000;
    final boolean disconnectOnRedirect = true;
    final var retriever =
        new DefaultResourceRetriever(timeoutMs, timeoutMs, maxSizeBytes, disconnectOnRedirect);

    @SuppressWarnings("deprecation") // RemoteJWKSet deprecated in nimbus 9.35+ but
    // JWKSourceBuilder requires nimbus 10.7+; our version (10.3) must use RemoteJWKSet.
    final var jwkSource = new RemoteJWKSet<SecurityContext>(URI.create(jwksUrl).toURL(), retriever);

    final var keySelector = new JWSVerificationKeySelector<>(JWSAlgorithm.RS256, jwkSource);

    final var processor = new DefaultJWTProcessor<SecurityContext>();
    processor.setJWSKeySelector(keySelector);

    // Configure claims verifier: require exp, sub, jti; validate aud; 5s clock skew
    final var requiredClaims = new HashSet<String>();
    requiredClaims.add("exp");
    requiredClaims.add("sub");
    requiredClaims.add("jti");
    requiredClaims.add("iss");

    // Override currentTime() to use the injected EpochNanoClock instead of System clock.
    // This ensures exp/nbf validation uses the same clock source as iat validation.
    final var clock = wallClock; // capture for anonymous class
    final var claimsVerifier =
        new DefaultJWTClaimsVerifier<SecurityContext>(
            new JWTClaimsSet.Builder().audience(expectedAudience).build(), requiredClaims) {
          @Override
          protected Date currentTime() {
            final long epochMs = TimeUnit.NANOSECONDS.toMillis(clock.nanoTime());
            return new Date(epochMs);
          }
        };
    claimsVerifier.setMaxClockSkew(CLOCK_SKEW_SECONDS);

    processor.setJWTClaimsSetVerifier(claimsVerifier);
    return new BuiltProcessor(processor, jwkSource);
  }

  /**
   * Internal pairing of a configured {@link DefaultJWTProcessor} with the {@link JWKSource} that
   * backs it. Used to preserve a direct reference to the JWK source so {@link #preflightOrThrow()}
   * can call {@link JWKSource#get} without reflection.
   */
  private record BuiltProcessor(
      DefaultJWTProcessor<SecurityContext> processor, JWKSource<SecurityContext> jwkSource) {}

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
   * token content or user-identifiable claims (to prevent information leakage in logs). The
   * underlying cause (e.g. {@link java.io.IOException} from JWKS fetch) is preserved via the
   * standard {@link Throwable#getCause()} chain so operators can diagnose root failures without the
   * cause appearing in user-facing log messages.
   */
  public static final class JwtValidationException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * @param message the validation failure reason (safe for logging)
     */
    public JwtValidationException(final String message) {
      super(message);
    }

    /**
     * @param message the validation failure reason (safe for logging)
     * @param cause the underlying exception (e.g. JWKS network error); preserved on the cause chain
     *     for diagnostic purposes but never included in {@link #getMessage()}
     */
    public JwtValidationException(final String message, final Throwable cause) {
      super(message, cause);
    }
  }
}
