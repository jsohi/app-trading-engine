package com.trading.engine.websocket;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Locale;
import java.util.Objects;

/**
 * Resolves an OIDC {@code /.well-known/openid-configuration} discovery document into a {@code
 * jwks_uri} at server startup, with strict transport bounds and a defence-in-depth Jackson posture.
 *
 * <p><b>RFC 8414 §3 best-practice host-match:</b> the resolved {@code jwks_uri} MUST share host
 * (case-insensitive; ports may differ) with the originating {@code oidcDiscoveryUri}. A compromised
 * discovery endpoint cannot redirect JWKS lookups to an attacker-controlled host.
 *
 * <p><b>Trust boundary:</b> {@code oidcDiscoveryUri} is operator-controlled config (YAML/env at
 * startup), NOT tenant-supplied. The SSRF surface is therefore the operator's responsibility — see
 * {@link WebSocketServerConfig.IssuerRegistryEntry} doc-comment which documents this constraint so
 * the field never silently becomes tenant-driven.
 *
 * <p><b>HTTPS only.</b> Both the discovery URI and the resolved {@code jwks_uri} must use {@code
 * https://}; either fails fast at startup.
 *
 * <p><b>Bounded transport:</b>
 *
 * <ul>
 *   <li>{@link #CONNECT_TIMEOUT} 5s — caps wait on a slowloris IdP.
 *   <li>{@link #REQUEST_TIMEOUT} 10s — caps total request time.
 *   <li>{@link #MAX_RESPONSE_BYTES} 64 KiB — caps memory exposure if the IdP returns a huge body.
 * </ul>
 *
 * <p><b>SECURITY:</b> uses the JDK {@link HttpClient} with default {@link javax.net.ssl.SSLContext}
 * (system trust store) and default hostname verification. NO custom {@code SSLContext}, NO {@code
 * HostnameVerifier} override, NO {@code trustAll}. Do not introduce any of these.
 *
 * <p><b>Jackson defence-in-depth:</b> dedicated {@link JsonMapper} instance with {@link
 * DeserializationFeature#FAIL_ON_UNKNOWN_PROPERTIES} disabled (forward-compat with IdP doc growth);
 * {@code enableDefaultTyping} explicitly NOT enabled (no polymorphic deserialization gadget chain);
 * no {@code JavaTimeModule} (the doc has no temporal fields). Deserialised into a small POJO with
 * {@code jwks_uri} only.
 *
 * <p><b>Threading:</b> Stateless once constructed — the {@link HttpClient} is thread-safe, the
 * mapper is thread-safe after configuration. Used at single-threaded startup; thread-safety is
 * defence-in-depth.
 *
 * <p><b>Allocation:</b> One allocation at construction; per-call allocates the request, response
 * body buffer, and one POJO. Not on the hot path — startup-only.
 */
public final class OidcDiscoveryClient {

  /** TCP connect timeout for the discovery fetch. */
  public static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(5);

  /** Total HTTP request timeout (connect + send + receive). */
  public static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(10);

  /** Hard cap on the discovery doc body size (bytes). */
  public static final int MAX_RESPONSE_BYTES = 64 * 1024;

  private final HttpClient httpClient;
  private final ObjectMapper mapper;
  private final boolean allowInsecureDiscoveryUri;

  /**
   * Constructs a production client with the supplied HTTPS HttpClient. Discovery URIs MUST use
   * https:// (enforced in {@link #resolveJwksUri(String)}); the resolved {@code jwks_uri} is
   * subject to the same enforcement.
   *
   * @param httpClient the HTTPS client (must use the system trust store; see SECURITY note above)
   */
  public OidcDiscoveryClient(final HttpClient httpClient) {
    this(httpClient, false);
  }

  /**
   * Package-private overload that relaxes the discovery-URI https check, used ONLY by {@code
   * WebSocketServerConfigOidcDiscoveryTest} to point at a plain-HTTP Jetty stub. The resolved
   * {@code jwks_uri} https check, RFC 8414 host-match, body-size cap, and JSON validation remain in
   * force. There is intentionally no public constructor with this flag — the production surface
   * CANNOT accidentally turn off the https check.
   */
  OidcDiscoveryClient(final HttpClient httpClient, final boolean allowInsecureDiscoveryUri) {
    this.httpClient = Objects.requireNonNull(httpClient, "httpClient");
    this.allowInsecureDiscoveryUri = allowInsecureDiscoveryUri;
    this.mapper =
        JsonMapper.builder()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            // enableDefaultTyping NOT enabled — disabling explicitly is the secure default but we
            // document it here so a future maintainer cannot silently flip it.
            .build();
  }

  /** Production factory — JDK HttpClient with the bounded connect/timeout configuration. */
  public static OidcDiscoveryClient createDefault() {
    final var client =
        HttpClient.newBuilder()
            .connectTimeout(CONNECT_TIMEOUT)
            .version(HttpClient.Version.HTTP_2)
            .build();
    return new OidcDiscoveryClient(client);
  }

  /**
   * Fetches the discovery document at {@code oidcDiscoveryUri} and returns the {@code jwks_uri}
   * value, validated.
   *
   * @param oidcDiscoveryUri the operator-provided discovery URL (must use https://)
   * @return the discovered JWKS URI (guaranteed https:// and same-host as {@code oidcDiscoveryUri})
   * @throws IllegalArgumentException if the discovery URI is not https://
   * @throws OidcDiscoveryException if the fetch fails, the doc is malformed, the size cap is
   *     breached, or the resolved jwks_uri fails the http-only / host-match invariants
   */
  public String resolveJwksUri(final String oidcDiscoveryUri) {
    Objects.requireNonNull(oidcDiscoveryUri, "oidcDiscoveryUri");
    if (!oidcDiscoveryUri.regionMatches(true, 0, "https://", 0, 8)) {
      // Production path: https-only. Test path: package-private constructor flag relaxes this so
      // a Jetty stub can serve plaintext.
      if (!(allowInsecureDiscoveryUri
          && oidcDiscoveryUri.regionMatches(true, 0, "http://", 0, 7))) {
        throw new IllegalArgumentException(
            "oidcDiscoveryUri must use https://, got: " + oidcDiscoveryUri);
      }
    }

    final URI discoveryUri;
    try {
      discoveryUri = new URI(oidcDiscoveryUri);
    } catch (final URISyntaxException e) {
      throw new OidcDiscoveryException("malformed oidcDiscoveryUri: " + oidcDiscoveryUri, e);
    }

    final var request =
        HttpRequest.newBuilder()
            .uri(discoveryUri)
            .timeout(REQUEST_TIMEOUT)
            .GET()
            .header("Accept", "application/json")
            .build();

    final HttpResponse<byte[]> response;
    try {
      response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
    } catch (final IOException e) {
      throw new OidcDiscoveryException("failed to fetch " + oidcDiscoveryUri + ": " + e, e);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new OidcDiscoveryException("interrupted fetching " + oidcDiscoveryUri, e);
    }

    if (response.statusCode() != 200) {
      throw new OidcDiscoveryException(
          "discovery URI returned HTTP " + response.statusCode() + ": " + oidcDiscoveryUri);
    }

    final var body = response.body();
    if (body.length > MAX_RESPONSE_BYTES) {
      throw new OidcDiscoveryException(
          "discovery doc exceeds "
              + MAX_RESPONSE_BYTES
              + " byte cap (got "
              + body.length
              + "): "
              + oidcDiscoveryUri);
    }

    final DiscoveryDoc doc;
    try {
      doc = mapper.readValue(body, DiscoveryDoc.class);
    } catch (final IOException e) {
      throw new OidcDiscoveryException("malformed discovery JSON at " + oidcDiscoveryUri, e);
    }
    if (doc == null || doc.getJwksUri() == null || doc.getJwksUri().isBlank()) {
      throw new OidcDiscoveryException(
          "discovery doc missing required 'jwks_uri' field: " + oidcDiscoveryUri);
    }

    if (!doc.getJwksUri().regionMatches(true, 0, "https://", 0, 8)) {
      throw new OidcDiscoveryException(
          "discovery doc jwks_uri must use https://, got: " + doc.getJwksUri());
    }

    final URI jwksUri;
    try {
      jwksUri = new URI(doc.getJwksUri());
    } catch (final URISyntaxException e) {
      throw new OidcDiscoveryException(
          "malformed jwks_uri in discovery doc: " + doc.getJwksUri(), e);
    }

    // RFC 8414 §3 best-practice host-match: discovered jwks_uri must share host with the
    // discovery URI. Ports may differ (some IdPs serve JWKS on a different port). Compares
    // case-insensitively to match RFC 3986 host normalization.
    final var discoveryHost = nullToEmpty(discoveryUri.getHost()).toLowerCase(Locale.ROOT);
    final var jwksHost = nullToEmpty(jwksUri.getHost()).toLowerCase(Locale.ROOT);
    if (discoveryHost.isEmpty() || !discoveryHost.equals(jwksHost)) {
      throw new OidcDiscoveryException(
          "discovery doc jwks_uri host '"
              + jwksHost
              + "' does not match oidcDiscoveryUri host '"
              + discoveryHost
              + "' (RFC 8414 §3 host-match)");
    }

    return doc.getJwksUri();
  }

  private static String nullToEmpty(final String s) {
    return s == null ? "" : s;
  }

  /**
   * Subset of the OIDC discovery doc — we only consume {@code jwks_uri}. {@link
   * DeserializationFeature#FAIL_ON_UNKNOWN_PROPERTIES} is disabled so unknown sibling fields (e.g.
   * {@code token_endpoint}, {@code response_types_supported}, {@code scopes_supported}) parse
   * without complaint. The field is {@code private final} with a getter and {@link
   * JsonCreator}-annotated constructor — matches production Jackson hygiene elsewhere in the repo
   * (no public mutable fields).
   */
  static final class DiscoveryDoc {
    private final String jwksUri;

    @JsonCreator
    DiscoveryDoc(@JsonProperty("jwks_uri") final String jwksUri) {
      this.jwksUri = jwksUri;
    }

    String getJwksUri() {
      return jwksUri;
    }
  }
}
