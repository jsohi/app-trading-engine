package com.trading.engine.fixbridge.auth;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSVerifier;
import com.nimbusds.jose.crypto.factories.DefaultJWSVerifierFactory;
import com.nimbusds.jose.jwk.AsymmetricJWK;
import com.nimbusds.jose.jwk.JWK;
import com.trading.engine.websocket.JwtValidator.ValidatedClaims;
import java.security.Key;
import java.text.ParseException;
import java.util.Date;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.agrona.concurrent.EpochNanoClock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Production {@link DpopValidator} backed by nimbus-jose+JWT (RFC 9449 — Demonstration of Proof of
 * Possession).
 *
 * <p><b>Validation pipeline.</b> For each invocation:
 *
 * <ol>
 *   <li>If the {@code DPoP} header is absent and the bearer token has no {@code cnf.jkt} binding,
 *       returns {@link Result#VALID} (DPoP not required for this token).
 *   <li>If the header is absent but the bearer carries {@code cnf.jkt}, returns {@link
 *       Result#INVALID} (token requires DPoP, worker omitted the proof).
 *   <li>Parses the proof JWT, extracts the embedded {@code jwk} header parameter, and verifies the
 *       proof's signature against that public key.
 *   <li>Verifies the proof's {@code htm} claim equals the configured HTTP method (default {@code
 *       POST}).
 *   <li>Verifies the proof's {@code iat} claim is within {@code ±maxClockSkewSeconds} of the
 *       injected {@link EpochNanoClock}.
 *   <li>Calls {@link JtiReplayCache#checkAndAdd} on the proof's {@code jti}; replay returns {@link
 *       Result#INVALID}.
 *   <li>Computes the SHA-256 thumbprint (RFC 7638) of the embedded JWK and compares against the
 *       bearer's {@code cnf.jkt}:
 *       <ul>
 *         <li>match → {@link Result#VALID}
 *         <li>mismatch → {@link Result#STALE_DPOP} (worker rotated key after token issuance)
 *         <li>bearer has no {@code cnf.jkt} → {@link Result#VALID} (proof verified but token
 *             unbound; harmless to honour)
 *       </ul>
 *   <li>Any nimbus parse/verify exception → {@link Result#INVALID}.
 * </ol>
 *
 * <p><b>htu (HTTP target URI).</b> Real DPoP proofs include an {@code htu} claim identifying the
 * resource URL. We accept the configured {@code expectedHtu} as a guard against cross-resource
 * proof reuse. When {@code expectedHtu} is null at construction (single-resource bridge), the
 * {@code htu} claim is parsed for presence but its value is not compared — the server only services
 * one upgrade path so the comparison would be redundant.
 *
 * <p><b>Threading.</b> Thread-safe. The validator holds only immutable configuration plus a
 * thread-safe {@link JtiReplayCache} reference (the cache impl is responsible for its own
 * synchronization). Multiple Netty event-loop threads MAY invoke {@link #validate} concurrently.
 *
 * <p><b>Allocation.</b> Cold path — invoked once per WebSocket upgrade, never on the message hot
 * path. Allocates per call (JWT parsing, JWK extraction, thumbprint computation). This matches
 * {@link com.trading.engine.websocket.JwtValidator}'s allocation profile.
 *
 * <p><b>Why not use {@code com.nimbusds.oauth2.sdk.dpop}?</b> The Nimbus OAuth2 SDK is a separate
 * Maven artifact ({@code com.nimbusds:oauth2-oidc-sdk}) not on this project's classpath. The base
 * nimbus-jose-jwt 10.3 library provides every primitive we need ({@link JWK#computeThumbprint},
 * {@link DefaultJWSVerifierFactory}) without dragging in the OAuth2 SDK's transitive surface.
 *
 * @see DpopValidator
 * @see JtiReplayCache
 * @see <a href="https://datatracker.ietf.org/doc/html/rfc9449">RFC 9449 — DPoP</a>
 * @see <a href="https://datatracker.ietf.org/doc/html/rfc7638">RFC 7638 — JWK Thumbprint</a>
 * @see <a href="https://datatracker.ietf.org/doc/html/rfc7800">RFC 7800 — Proof-of-Possession Key
 *     Semantics for JWT</a>
 */
public final class NimbusDpopValidator implements DpopValidator {

  private static final Logger LOG = LogManager.getLogger(NimbusDpopValidator.class);

  /**
   * Default HTTP method for the WebSocket upgrade — RFC 6455 mandates GET; bridges that front the
   * upgrade behind a POST proxy override via constructor.
   */
  public static final String DEFAULT_HTM = "POST";

  /**
   * SHA-256 hash algorithm name passed to {@link JWK#computeThumbprint(String)} per RFC 7638 §3.4
   * (the only mandatory-to-implement digest in the spec).
   */
  private static final String JWK_THUMBPRINT_ALG = "SHA-256";

  /**
   * DPoP proof type header value ({@code typ}) per RFC 9449 §4.2. Reserved for future strict- typ
   * validation; not currently enforced because some libraries omit the {@code typ} header.
   */
  @SuppressWarnings("unused")
  private static final String DPOP_TYP = "dpop+jwt";

  private final EpochNanoClock clock;
  private final long maxClockSkewSeconds;
  private final JtiReplayCache jtiCache;
  private final String expectedHtm;
  private final String expectedHtu;
  private final DefaultJWSVerifierFactory verifierFactory;

  /**
   * Construct a DPoP validator with the default HTTP method ({@value #DEFAULT_HTM}) and no {@code
   * htu} comparison.
   *
   * @param clock epoch-nanosecond clock for {@code iat} freshness checks; injected via {@code
   *     TradingClocks.epochNanoClock()}
   * @param maxClockSkewSeconds tolerated clock skew for {@code iat} (typical value: 30s)
   * @param jtiCache short-window replay cache; the validator delegates atomic check-and-insert to
   *     the cache impl
   * @throws NullPointerException if {@code clock} or {@code jtiCache} is null
   * @throws IllegalArgumentException if {@code maxClockSkewSeconds} is negative
   */
  public NimbusDpopValidator(
      final EpochNanoClock clock, final long maxClockSkewSeconds, final JtiReplayCache jtiCache) {
    this(clock, maxClockSkewSeconds, jtiCache, DEFAULT_HTM, null);
  }

  /**
   * Construct a DPoP validator with explicit {@code htm} and {@code htu} expectations.
   *
   * @param clock epoch-nanosecond clock for {@code iat} freshness checks
   * @param maxClockSkewSeconds tolerated clock skew for {@code iat}
   * @param jtiCache short-window replay cache
   * @param expectedHtm expected HTTP method in the proof's {@code htm} claim (case-sensitive match
   *     per RFC 9449); typically {@code POST} or {@code GET}
   * @param expectedHtu expected HTTP target URI in the proof's {@code htu} claim. When non-null,
   *     the proof's {@code htu} MUST equal this value (case-sensitive). When null, presence is
   *     checked but value comparison is skipped — appropriate when the server services exactly one
   *     upgrade path.
   * @throws NullPointerException if {@code clock}, {@code jtiCache}, or {@code expectedHtm} is null
   * @throws IllegalArgumentException if {@code maxClockSkewSeconds} is negative or {@code
   *     expectedHtm} is empty
   */
  public NimbusDpopValidator(
      final EpochNanoClock clock,
      final long maxClockSkewSeconds,
      final JtiReplayCache jtiCache,
      final String expectedHtm,
      final String expectedHtu) {
    this.clock = Objects.requireNonNull(clock, "clock");
    this.jtiCache = Objects.requireNonNull(jtiCache, "jtiCache");
    this.expectedHtm = Objects.requireNonNull(expectedHtm, "expectedHtm");
    if (expectedHtm.isEmpty()) {
      throw new IllegalArgumentException("expectedHtm must not be empty");
    }
    if (maxClockSkewSeconds < 0) {
      throw new IllegalArgumentException(
          "maxClockSkewSeconds must be >= 0, was " + maxClockSkewSeconds);
    }
    this.maxClockSkewSeconds = maxClockSkewSeconds;
    this.expectedHtu = expectedHtu; // nullable
    this.verifierFactory = new DefaultJWSVerifierFactory();
  }

  @Override
  public Result validate(final ValidatedClaims claims, final String dpopProofHeader) {
    Objects.requireNonNull(claims, "claims");

    final var cnfJkt = claims.cnfJkt();

    if (dpopProofHeader == null || dpopProofHeader.isEmpty()) {
      // No proof presented. INVALID iff the bearer token requires one.
      if (cnfJkt != null) {
        LOG.debug("DPoP header missing but bearer token has cnf.jkt — rejecting");
        return Result.INVALID;
      }
      return Result.VALID;
    }

    // Parse + cryptographically verify the proof JWT.
    final com.nimbusds.jwt.SignedJWT proof;
    try {
      proof = com.nimbusds.jwt.SignedJWT.parse(dpopProofHeader);
    } catch (final ParseException e) {
      LOG.debug("Malformed DPoP proof JWT: {}", e.getMessage());
      return Result.INVALID;
    }

    final var header = proof.getHeader();
    final var embeddedJwk = header.getJWK();
    if (embeddedJwk == null) {
      LOG.debug("DPoP proof header missing 'jwk' parameter");
      return Result.INVALID;
    }
    if (embeddedJwk.isPrivate()) {
      // RFC 9449 §4.2: the embedded JWK MUST be the public key only. Treat private-material leak
      // as proof-malformation.
      LOG.debug("DPoP proof embedded jwk contained private parameters — rejecting");
      return Result.INVALID;
    }

    // Verify the proof signature against the embedded jwk.
    if (!verifyProofSignature(proof, embeddedJwk, header)) {
      return Result.INVALID;
    }

    // Verify standard DPoP claims: htm, iat, jti, htu (when configured).
    final com.nimbusds.jwt.JWTClaimsSet proofClaims;
    try {
      proofClaims = proof.getJWTClaimsSet();
    } catch (final ParseException e) {
      LOG.debug("Cannot parse DPoP proof claims: {}", e.getMessage());
      return Result.INVALID;
    }

    final var htm = stringClaim(proofClaims, "htm");
    if (htm == null || !expectedHtm.equals(htm)) {
      LOG.debug("DPoP htm mismatch: expected={} actual={}", expectedHtm, htm);
      return Result.INVALID;
    }

    final var htu = stringClaim(proofClaims, "htu");
    if (htu == null) {
      LOG.debug("DPoP proof missing required htu claim");
      return Result.INVALID;
    }
    if (expectedHtu != null && !expectedHtu.equals(htu)) {
      LOG.debug("DPoP htu mismatch: expected={} actual={}", expectedHtu, htu);
      return Result.INVALID;
    }

    final var iat = proofClaims.getIssueTime();
    if (iat == null) {
      LOG.debug("DPoP proof missing required iat claim");
      return Result.INVALID;
    }
    if (!iatWithinSkew(iat)) {
      LOG.debug("DPoP iat outside skew window: iat={}", iat);
      return Result.INVALID;
    }

    final var jti = proofClaims.getJWTID();
    if (jti == null || jti.isEmpty()) {
      LOG.debug("DPoP proof missing required jti claim");
      return Result.INVALID;
    }
    final long expireAtNs = clock.nanoTime() + TimeUnit.SECONDS.toNanos(maxClockSkewSeconds * 2L);
    if (!jtiCache.checkAndAdd(jti, expireAtNs)) {
      LOG.debug("DPoP jti replay detected: jti={}", jti);
      return Result.INVALID;
    }

    // Compute proof JWK SHA-256 thumbprint (RFC 7638) and compare against bearer cnf.jkt.
    final String proofThumbprint;
    try {
      proofThumbprint = embeddedJwk.computeThumbprint(JWK_THUMBPRINT_ALG).toString();
    } catch (final JOSEException e) {
      LOG.debug("Failed to compute DPoP JWK thumbprint: {}", e.getMessage());
      return Result.INVALID;
    }

    if (cnfJkt == null) {
      // Bearer is not bound to any specific DPoP key — proof verified for its own sake; admit.
      return Result.VALID;
    }
    if (cnfJkt.equals(proofThumbprint)) {
      return Result.VALID;
    }
    // Bearer token was issued against a different DPoP key than the worker now possesses.
    LOG.debug(
        "DPoP key rotation detected: bearer cnf.jkt={} proof thumbprint={}",
        cnfJkt,
        proofThumbprint);
    return Result.STALE_DPOP;
  }

  /**
   * Verify the proof's JWS signature using the public key carried in the embedded {@code jwk}
   * header parameter. RFC 9449 §4.3 step 2.
   *
   * @param proof the parsed DPoP proof JWT
   * @param embeddedJwk the public JWK extracted from the proof's {@code jwk} header
   * @param header the proof's JWS header
   * @return {@code true} iff the signature is cryptographically valid
   */
  private boolean verifyProofSignature(
      final com.nimbusds.jwt.SignedJWT proof,
      final JWK embeddedJwk,
      final com.nimbusds.jose.JWSHeader header) {
    final Key publicKey;
    try {
      // RSAKey/ECKey/OctetKeyPair all implement AsymmetricJWK which exposes toPublicKey().
      // OctetSequenceKey (HMAC) is not asymmetric and would fail this cast — DPoP proofs MUST
      // use an asymmetric algorithm (RFC 9449 §4.2).
      if (!(embeddedJwk instanceof AsymmetricJWK asym)) {
        LOG.debug("DPoP embedded jwk is not asymmetric: {}", embeddedJwk.getKeyType());
        return false;
      }
      publicKey = asym.toPublicKey();
    } catch (final JOSEException e) {
      LOG.debug("Cannot extract public key from DPoP jwk: {}", e.getMessage());
      return false;
    }

    final JWSVerifier verifier;
    try {
      verifier = verifierFactory.createJWSVerifier(header, publicKey);
    } catch (final JOSEException e) {
      LOG.debug("Cannot build verifier for DPoP alg {}: {}", header.getAlgorithm(), e.getMessage());
      return false;
    }

    try {
      if (!proof.verify(verifier)) {
        LOG.debug("DPoP proof signature verification failed");
        return false;
      }
    } catch (final JOSEException e) {
      LOG.debug("DPoP proof verification threw: {}", e.getMessage());
      return false;
    }
    return true;
  }

  /**
   * Test whether the proof's {@code iat} timestamp falls within {@code ±maxClockSkewSeconds} of the
   * configured clock.
   *
   * @param iat the proof's issue-time claim
   * @return {@code true} iff the timestamp is acceptable
   */
  private boolean iatWithinSkew(final Date iat) {
    final long nowEpochSec = TimeUnit.NANOSECONDS.toSeconds(clock.nanoTime());
    final long iatEpochSec = iat.toInstant().getEpochSecond();
    final long deltaSec = Math.abs(nowEpochSec - iatEpochSec);
    return deltaSec <= maxClockSkewSeconds;
  }

  /**
   * Read a string-typed claim from the proof. Returns {@code null} when the claim is absent or not
   * a string (defensive — DPoP proofs may carry non-string values for these fields when emitted by
   * non-conformant clients, which we treat the same as missing).
   *
   * @param claims the proof claims set
   * @param name the claim name
   * @return the string value, or {@code null}
   */
  private static String stringClaim(final com.nimbusds.jwt.JWTClaimsSet claims, final String name) {
    final var raw = claims.getClaim(name);
    return raw instanceof String s ? s : null;
  }
}
