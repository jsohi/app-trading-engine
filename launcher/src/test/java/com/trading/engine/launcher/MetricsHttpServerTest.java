package com.trading.engine.launcher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micrometer.core.instrument.Counter;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link MetricsHttpServer} — the production Prometheus scrape endpoint exposed by
 * the launcher.
 *
 * <p>The tests boot the server on an ephemeral port (port=0) so they can run in parallel without
 * conflicting on a hard-coded port. Each test wires a fresh {@link PrometheusMeterRegistry} so the
 * scrape output is deterministic.
 */
final class MetricsHttpServerTest {

  /** Bounded request timeout — keeps a hung server from blocking CI. */
  private static final Duration HTTP_TIMEOUT = Duration.ofSeconds(2);

  // ---------------------------------------------------------------------------
  // Constructor validation
  // ---------------------------------------------------------------------------

  @Test
  void constructor_blankBindHost_throws() {
    final var registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    assertThrows(IllegalArgumentException.class, () -> new MetricsHttpServer("", 0, registry));
    assertThrows(IllegalArgumentException.class, () -> new MetricsHttpServer("   ", 0, registry));
  }

  @Test
  void constructor_invalidPort_throws() {
    final var registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    assertThrows(
        IllegalArgumentException.class, () -> new MetricsHttpServer("127.0.0.1", -1, registry));
    assertThrows(
        IllegalArgumentException.class, () -> new MetricsHttpServer("127.0.0.1", 70_000, registry));
  }

  @Test
  void constructor_nullRegistry_throws() {
    assertThrows(NullPointerException.class, () -> new MetricsHttpServer("127.0.0.1", 0, null));
  }

  // ---------------------------------------------------------------------------
  // Routing
  // ---------------------------------------------------------------------------

  @Test
  void metricsEndpoint_returnsCounterLine() throws Exception {
    final var registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    final var counter =
        Counter.builder("websocket.marketdata.feed.state.transitions")
            .description("test counter for MetricsHttpServerTest")
            .register(registry);
    counter.increment();
    counter.increment();
    counter.increment();

    try (var server = new MetricsHttpServer("127.0.0.1", 0, registry)) {
      server.start();
      final var resp = get("http://127.0.0.1:" + server.boundPort() + "/metrics");
      assertEquals(200, resp.statusCode());
      assertTrue(
          resp.headers().firstValue("Content-Type").orElse("").contains("version=0.0.4"),
          "scrape must declare Prometheus 0.0.4 text format; headers=" + resp.headers().map());
      // Prometheus converts dots to underscores and appends _total to Counter names.
      assertTrue(
          resp.body().contains("websocket_marketdata_feed_state_transitions_total"),
          "scrape body must include the registered counter line; body=" + resp.body());
      assertTrue(
          resp.body().contains("3.0"),
          "scrape body must reflect the counter value (3.0); body=" + resp.body());
    }
  }

  @Test
  void metricsEndpoint_emptyRegistry_returns200WithEmptyOrHeaderOnlyBody() throws Exception {
    final var registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    try (var server = new MetricsHttpServer("127.0.0.1", 0, registry)) {
      server.start();
      final var resp = get("http://127.0.0.1:" + server.boundPort() + "/metrics");
      assertEquals(200, resp.statusCode());
      // Body may be empty or just contain a trailing newline; the request must succeed regardless.
    }
  }

  @Test
  void healthzEndpoint_returnsOk() throws Exception {
    final var registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    try (var server = new MetricsHttpServer("127.0.0.1", 0, registry)) {
      server.start();
      final var resp = get("http://127.0.0.1:" + server.boundPort() + "/healthz");
      assertEquals(200, resp.statusCode());
      assertEquals("ok\n", resp.body());
    }
  }

  @Test
  void unknownPath_returns404() throws Exception {
    final var registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    try (var server = new MetricsHttpServer("127.0.0.1", 0, registry)) {
      server.start();
      final var resp = get("http://127.0.0.1:" + server.boundPort() + "/does/not/exist");
      assertEquals(404, resp.statusCode());
    }
  }

  @Test
  void postOnMetrics_returns405() throws Exception {
    final var registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    try (var server = new MetricsHttpServer("127.0.0.1", 0, registry)) {
      server.start();
      final var resp = post("http://127.0.0.1:" + server.boundPort() + "/metrics");
      assertEquals(405, resp.statusCode());
    }
  }

  // ---------------------------------------------------------------------------
  // Lifecycle
  // ---------------------------------------------------------------------------

  @Test
  void boundPort_beforeStart_throws() {
    final var registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    final var server = new MetricsHttpServer("127.0.0.1", 0, registry);
    assertThrows(IllegalStateException.class, server::boundPort);
  }

  @Test
  void boundPort_afterStart_returnsEphemeralPort() throws Exception {
    final var registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    try (var server = new MetricsHttpServer("127.0.0.1", 0, registry)) {
      server.start();
      assertNotEquals(0, server.boundPort(), "ephemeral port=0 must resolve to a real port");
      assertTrue(server.metricsUrl().contains("/metrics"));
    }
  }

  @Test
  void start_twice_throws() throws Exception {
    final var registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    try (var server = new MetricsHttpServer("127.0.0.1", 0, registry)) {
      server.start();
      assertThrows(IllegalStateException.class, server::start);
    }
  }

  @Test
  void close_isIdempotent() throws Exception {
    final var registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    final var server = new MetricsHttpServer("127.0.0.1", 0, registry);
    server.start();
    server.close();
    server.close(); // must not throw
  }

  // ---------------------------------------------------------------------------
  // HTTP helpers
  // ---------------------------------------------------------------------------

  private static HttpResponse<String> get(final String url) throws Exception {
    final var client = HttpClient.newBuilder().connectTimeout(HTTP_TIMEOUT).build();
    final var req = HttpRequest.newBuilder(URI.create(url)).timeout(HTTP_TIMEOUT).GET().build();
    return client.send(req, BodyHandlers.ofString());
  }

  private static HttpResponse<String> post(final String url) throws Exception {
    final var client = HttpClient.newBuilder().connectTimeout(HTTP_TIMEOUT).build();
    final var req =
        HttpRequest.newBuilder(URI.create(url))
            .timeout(HTTP_TIMEOUT)
            .POST(HttpRequest.BodyPublishers.noBody())
            .build();
    return client.send(req, BodyHandlers.ofString());
  }
}
