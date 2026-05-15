package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

/**
 * Validates {@link WebSocketServerConfig#fromYaml(Path, OidcDiscoveryClient)} OIDC discovery
 * resolution end-to-end against a Jetty stub server.
 *
 * <p>Single-threaded execution is mandatory — each test stands up its own Jetty {@link
 * ServerConnector} bound to {@code port=0} and reads the bound port via {@link
 * ServerConnector#getLocalPort()}, so concurrent CI jobs cannot collide.
 *
 * <p>The test stub serves plaintext HTTP — production https-only enforcement is bypassed via the
 * package-private {@link OidcDiscoveryClient#OidcDiscoveryClient(HttpClient, boolean)} overload.
 * The resolved {@code jwks_uri} https check, RFC 8414 §3 host-match, body-size cap, and JSON
 * validity remain in force, so this test exercises the same security invariants production traffic
 * does.
 *
 * <p>The unreachable-URI case uses {@code https://127.0.0.1:1} (port 1) as a deterministic
 * blackhole — faster and more reliable than relying on connect-refused timing of an unbound
 * ephemeral port.
 */
@Execution(ExecutionMode.SAME_THREAD)
@Timeout(value = 30, unit = TimeUnit.SECONDS)
final class WebSocketServerConfigOidcDiscoveryTest {

  private Server jetty;
  private int boundPort;
  private final AtomicReference<String> stubBody = new AtomicReference<>();
  private final AtomicReference<Integer> stubStatus = new AtomicReference<>(200);

  @BeforeEach
  void startStub() throws Exception {
    jetty = new Server();
    final var connector = new ServerConnector(jetty);
    connector.setPort(0);
    jetty.addConnector(connector);
    jetty.setHandler(
        new AbstractHandler() {
          @Override
          public void handle(
              final String target,
              final Request baseRequest,
              final HttpServletRequest request,
              final HttpServletResponse response)
              throws IOException {
            final var body = stubBody.get();
            response.setStatus(stubStatus.get());
            response.setContentType("application/json");
            if (body != null) {
              response.getOutputStream().write(body.getBytes(StandardCharsets.UTF_8));
            }
            baseRequest.setHandled(true);
          }
        });
    jetty.start();
    boundPort = connector.getLocalPort();
  }

  @AfterEach
  void stopStub() throws Exception {
    if (jetty != null) {
      jetty.stop();
    }
  }

  // ===========================================================================
  // Success path — discovery → jwks_uri substitution lands in registry
  // ===========================================================================

  @Test
  void successfulResolution_substitutesJwksUriInRegistry(@TempDir final Path tmp) throws Exception {
    // Resolved jwks_uri uses https://127.0.0.1:<port> so the production https-only check on the
    // resolved value passes. The test only validates the substitution into the registry — actual
    // JWKS reachability is exercised by the full-stack E2E.
    stubBody.set(
        "{\"issuer\":\"https://issuer-x\",\"jwks_uri\":\"https://127.0.0.1:"
            + boundPort
            + "/keys.json\"}");
    final var yaml = writeOidcConfig(tmp, boundPort);
    final var cfg = WebSocketServerConfig.fromYaml(yaml, testClient());
    assertEquals(
        "https://127.0.0.1:" + boundPort + "/keys.json",
        cfg.issuerRegistry().get("https://issuer-x"));
  }

  // ===========================================================================
  // Negative paths
  // ===========================================================================

  @Test
  void malformedJsonInDiscoveryDoc_throws(@TempDir final Path tmp) throws Exception {
    stubBody.set("{ not valid json");
    final var yaml = writeOidcConfig(tmp, boundPort);
    final var ex =
        assertThrows(
            OidcDiscoveryException.class, () -> WebSocketServerConfig.fromYaml(yaml, testClient()));
    assertTrue(ex.getMessage().contains("malformed discovery JSON"), ex.getMessage());
  }

  @Test
  void missingJwksUriInDiscoveryDoc_throws(@TempDir final Path tmp) throws Exception {
    stubBody.set("{\"issuer\":\"https://issuer-x\"}");
    final var yaml = writeOidcConfig(tmp, boundPort);
    final var ex =
        assertThrows(
            OidcDiscoveryException.class, () -> WebSocketServerConfig.fromYaml(yaml, testClient()));
    assertTrue(ex.getMessage().contains("jwks_uri"), ex.getMessage());
  }

  @Test
  void httpJwksUriInDiscoveryDoc_throwsAtDiscoveryTime(@TempDir final Path tmp) throws Exception {
    // jwks_uri uses http:// — discovery client must reject it BEFORE the registry's own https
    // check would catch the substituted value.
    stubBody.set("{\"jwks_uri\":\"http://elsewhere.example/keys.json\"}");
    final var yaml = writeOidcConfig(tmp, boundPort);
    final var ex =
        assertThrows(
            OidcDiscoveryException.class, () -> WebSocketServerConfig.fromYaml(yaml, testClient()));
    assertTrue(ex.getMessage().contains("https://"), ex.getMessage());
  }

  @Test
  void unreachableDiscoveryUri_throwsWithinConnectTimeout(@TempDir final Path tmp)
      throws Exception {
    final var yaml =
        writeYaml(
            tmp,
            """
            jwtAudience: trading-ui
            issuerRegistry:
              "https://issuer-x":
                oidcDiscoveryUri: "https://127.0.0.1:1/.well-known/openid-configuration"
            """);
    final long start = System.nanoTime();
    final var ex =
        assertThrows(
            OidcDiscoveryException.class,
            () -> WebSocketServerConfig.fromYaml(yaml, OidcDiscoveryClient.createDefault()));
    final long elapsedMs = (System.nanoTime() - start) / 1_000_000L;
    // 10s ceiling = 5s connect-timeout × 2 safety factor for cold CI runners.
    // Anything beyond that indicates a regression in the bounded-transport posture.
    assertTrue(
        elapsedMs < 10_000,
        "expected fast-fail within 5s connect timeout × 2 safety, got " + elapsedMs + "ms");
    assertTrue(ex.getMessage().contains("failed to fetch"), ex.getMessage());
  }

  @Test
  void crossHostJwksUri_throwsHostMismatch(@TempDir final Path tmp) throws Exception {
    stubBody.set("{\"jwks_uri\":\"https://attacker.example:" + boundPort + "/keys.json\"}");
    final var yaml = writeOidcConfig(tmp, boundPort);
    final var ex =
        assertThrows(
            OidcDiscoveryException.class, () -> WebSocketServerConfig.fromYaml(yaml, testClient()));
    assertTrue(ex.getMessage().contains("RFC 8414"), ex.getMessage());
  }

  @Test
  void responseBodyExceeds64KiB_throws(@TempDir final Path tmp) throws Exception {
    final var huge = new StringBuilder(70 * 1024);
    huge.append("{\"jwks_uri\":\"https://127.0.0.1:").append(boundPort).append("/keys.json\",");
    while (huge.length() < 65 * 1024) {
      huge.append("\"pad").append(huge.length()).append("\":\"x\",");
    }
    huge.append("\"end\":1}");
    stubBody.set(huge.toString());
    final var yaml = writeOidcConfig(tmp, boundPort);
    final var ex =
        assertThrows(
            OidcDiscoveryException.class, () -> WebSocketServerConfig.fromYaml(yaml, testClient()));
    assertTrue(ex.getMessage().contains("byte cap"), ex.getMessage());
  }

  // ===========================================================================
  // Helpers
  // ===========================================================================

  private Path writeOidcConfig(final Path tmp, final int port) throws IOException {
    return writeYaml(
        tmp,
        """
        jwtAudience: trading-ui
        issuerRegistry:
          "https://issuer-x":
            oidcDiscoveryUri: "http://127.0.0.1:%d/.well-known/openid-configuration"
        """
            .formatted(port));
  }

  private Path writeYaml(final Path tmp, final String contents) throws IOException {
    final var p = tmp.resolve("config.yaml");
    Files.writeString(p, contents, StandardCharsets.UTF_8);
    return p;
  }

  /**
   * Builds an OidcDiscoveryClient that talks to the local plaintext Jetty stub. Uses the
   * package-private {@link OidcDiscoveryClient#OidcDiscoveryClient(HttpClient, boolean)} overload
   * with {@code allowInsecureDiscoveryUri=true}.
   */
  private static OidcDiscoveryClient testClient() {
    return new OidcDiscoveryClient(
        HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build(), true);
  }
}
