package com.trading.engine.launcher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link E2eManagementServer} — the dev/e2e-only HTTP endpoint that lets the
 * full-stack Playwright harness pause/resume the pricing-service AgentRunner.
 *
 * <p>The tests bind on an ephemeral port (port=0 not supported by the constructor's range check, so
 * we use a high-numbered port that should be free in CI) and verify pause/resume callbacks fire,
 * health returns 200, and unsupported methods return 405.
 */
final class E2eManagementServerTest {

  /** Bounded request timeout — keeps a hung server from blocking CI. */
  private static final Duration HTTP_TIMEOUT = Duration.ofSeconds(2);

  @Test
  void constructor_invalidPort_throws() {
    assertThrows(
        IllegalArgumentException.class, () -> new E2eManagementServer(0, () -> {}, () -> {}));
    assertThrows(
        IllegalArgumentException.class, () -> new E2eManagementServer(70_000, () -> {}, () -> {}));
  }

  @Test
  void constructor_nullCallback_throws() {
    assertThrows(NullPointerException.class, () -> new E2eManagementServer(38_001, null, () -> {}));
    assertThrows(NullPointerException.class, () -> new E2eManagementServer(38_001, () -> {}, null));
  }

  @Test
  void pauseEndpoint_invokesCallback_returns200() throws Exception {
    final var pauseCount = new AtomicInteger();
    final var resumeCount = new AtomicInteger();
    try (var server =
        new E2eManagementServer(
            38_011, pauseCount::incrementAndGet, resumeCount::incrementAndGet)) {
      server.start();
      final var resp = post("http://127.0.0.1:" + server.boundPort() + "/e2e/pricing/pause");
      assertEquals(200, resp.statusCode());
      assertEquals("paused", resp.body());
      assertEquals(1, pauseCount.get());
      assertEquals(0, resumeCount.get());
    }
  }

  @Test
  void resumeEndpoint_invokesCallback_returns200() throws Exception {
    final var pauseCount = new AtomicInteger();
    final var resumeCount = new AtomicInteger();
    try (var server =
        new E2eManagementServer(
            38_012, pauseCount::incrementAndGet, resumeCount::incrementAndGet)) {
      server.start();
      final var resp = post("http://127.0.0.1:" + server.boundPort() + "/e2e/pricing/resume");
      assertEquals(200, resp.statusCode());
      assertEquals("resumed", resp.body());
      assertEquals(0, pauseCount.get());
      assertEquals(1, resumeCount.get());
    }
  }

  @Test
  void healthEndpoint_returnsOk() throws Exception {
    try (var server = new E2eManagementServer(38_013, () -> {}, () -> {})) {
      server.start();
      final var resp = get("http://127.0.0.1:" + server.boundPort() + "/e2e/health");
      assertEquals(200, resp.statusCode());
      assertEquals("ok", resp.body());
    }
  }

  @Test
  void getOnPauseEndpoint_returns405() throws Exception {
    try (var server = new E2eManagementServer(38_014, () -> {}, () -> {})) {
      server.start();
      final var resp = get("http://127.0.0.1:" + server.boundPort() + "/e2e/pricing/pause");
      assertEquals(405, resp.statusCode());
    }
  }

  @Test
  void callbackThrows_returns500() throws Exception {
    try (var server =
        new E2eManagementServer(
            38_015,
            () -> {
              throw new RuntimeException("boom");
            },
            () -> {})) {
      server.start();
      final var resp = post("http://127.0.0.1:" + server.boundPort() + "/e2e/pricing/pause");
      assertEquals(500, resp.statusCode());
      assertTrue(resp.body().contains("boom"), "body should include callback error message");
    }
  }

  @Test
  void boundPort_beforeStart_throws() {
    final var server = new E2eManagementServer(38_016, () -> {}, () -> {});
    assertThrows(IllegalStateException.class, server::boundPort);
  }

  @Test
  void close_isIdempotent() throws IOException {
    final var server = new E2eManagementServer(38_017, () -> {}, () -> {});
    server.start();
    server.close();
    server.close(); // must not throw
  }

  @Test
  void fromEnvironment_envUnset_returnsNull() {
    // ENV_ENABLED is not set in the test JVM; factory returns null.
    if (System.getenv(E2eManagementServer.ENV_ENABLED) == null) {
      assertEquals(null, E2eManagementServer.fromEnvironment(() -> {}, () -> {}));
    } else {
      // Skip: ambient env var present; we cannot assert null safely.
      assertNotNull(System.getenv(E2eManagementServer.ENV_ENABLED));
    }
  }

  // ---------------------------------------------------------------------------
  // HTTP helpers
  // ---------------------------------------------------------------------------

  private static HttpResponse<String> post(final String url) throws Exception {
    final var client = HttpClient.newBuilder().connectTimeout(HTTP_TIMEOUT).build();
    final var req =
        HttpRequest.newBuilder(URI.create(url))
            .timeout(HTTP_TIMEOUT)
            .POST(HttpRequest.BodyPublishers.noBody())
            .build();
    return client.send(req, BodyHandlers.ofString());
  }

  private static HttpResponse<String> get(final String url) throws Exception {
    final var client = HttpClient.newBuilder().connectTimeout(HTTP_TIMEOUT).build();
    final var req = HttpRequest.newBuilder(URI.create(url)).timeout(HTTP_TIMEOUT).GET().build();
    return client.send(req, BodyHandlers.ofString());
  }
}
