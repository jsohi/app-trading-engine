package com.trading.engine.launcher;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Production HTTP endpoint exposing the Micrometer {@link PrometheusMeterRegistry} scrape surface
 * so Prometheus / Grafana can pull WebSocket-server metrics from the launcher JVM.
 *
 * <p><b>Routes.</b>
 *
 * <ul>
 *   <li>{@code GET /metrics} — returns the Prometheus text-format scrape from {@link
 *       PrometheusMeterRegistry#scrape()}. Response content-type is {@code text/plain;
 *       version=0.0.4; charset=utf-8} per the Prometheus exposition spec.
 *   <li>{@code GET /healthz} — returns {@code 200 ok\n}. Probe endpoint for k8s/load-balancer
 *       readiness checks.
 *   <li>Everything else — {@code 404}.
 *   <li>Non-{@code GET} on a known route — {@code 405}.
 * </ul>
 *
 * <p><b>Bind defaults.</b> Reads {@code TRADING_METRICS_PORT} (default {@code 9100}) and {@code
 * TRADING_METRICS_BIND} (default {@code 127.0.0.1}). Binding to {@code 127.0.0.1} by default is a
 * deliberate security posture — the metrics surface exposes internal counters that an operator may
 * not want reachable from outside the host. Production deployments that want to expose the endpoint
 * externally must set {@code TRADING_METRICS_BIND=0.0.0.0} (or a specific NIC address) explicitly,
 * typically behind a reverse proxy that enforces auth and TLS. Co-locating Prometheus on the same
 * host and scraping over loopback is the recommended pattern.
 *
 * <p><b>Threading.</b> The JDK {@link HttpServer} is configured with a single-thread {@link
 * Executors#newSingleThreadExecutor() ExecutorService}. Scrape requests run on that thread, NOT on
 * any application thread (Aeron egress, Netty event loop, cluster duty cycle). {@link
 * PrometheusMeterRegistry#scrape()} is thread-safe and lock-free — no synchronisation with metric
 * publishers is required. The single-thread executor is sized for Prometheus's expected scrape
 * cadence (typically 15-60 s); a thread pool would just waste idle threads.
 *
 * <p><b>Allocation.</b> Cold path. Each scrape allocates the response body via {@code
 * PrometheusMeterRegistry.scrape()}, plus a handful of small framing buffers. Not on any hot path.
 *
 * <p><b>Lifecycle.</b> Unlike the dev-only {@link E2eManagementServer}, this server is ALWAYS
 * installed in production via {@link #fromEnvironment(PrometheusMeterRegistry)} — there is no env
 * gate. The launcher's shutdown hook calls {@link #close()} to stop the listener with a 1-second
 * grace period for in-flight scrapes and to shut down the executor.
 *
 * @see PrometheusMeterRegistry#scrape()
 * @see <a href="https://prometheus.io/docs/instrumenting/exposition_formats/">Prometheus exposition
 *     formats</a>
 */
public final class MetricsHttpServer implements AutoCloseable {

  private static final Logger LOG = LogManager.getLogger(MetricsHttpServer.class);

  /**
   * Environment variable for the listener TCP port. Optional — defaults to {@link #DEFAULT_PORT}
   * when unset or blank.
   */
  public static final String ENV_PORT = "TRADING_METRICS_PORT";

  /**
   * Environment variable for the listener bind address. Optional — defaults to {@link
   * #DEFAULT_BIND} when unset or blank. Set to {@code 0.0.0.0} (or a specific interface) to expose
   * the endpoint beyond the local host.
   */
  public static final String ENV_BIND = "TRADING_METRICS_BIND";

  /** Default TCP port. {@code 9100} is the conventional node-exporter port; intentional. */
  public static final int DEFAULT_PORT = 9100;

  /** Default bind address — loopback only. */
  public static final String DEFAULT_BIND = "127.0.0.1";

  /**
   * Prometheus exposition content-type. Pinned to {@code version=0.0.4} (the stable text format)
   * rather than OpenMetrics, matching what {@link PrometheusMeterRegistry#scrape()} emits by
   * default. UTF-8 is the only charset Prometheus servers accept here.
   */
  static final String PROMETHEUS_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8";

  /** Grace period for in-flight HTTP handlers during {@link #close()}. */
  private static final int STOP_GRACE_SECONDS = 1;

  /** Executor shutdown grace period during {@link #close()}. */
  private static final int EXECUTOR_GRACE_SECONDS = 2;

  private final String bindHost;
  private final int port;
  private final PrometheusMeterRegistry registry;
  private HttpServer server;
  private ExecutorService executor;

  /**
   * Constructs a metrics endpoint bound to {@code bindHost:port}. Does not start the listener —
   * call {@link #start()}.
   *
   * @param bindHost the bind address; must not be null or blank. Use {@code 127.0.0.1} for
   *     loopback, {@code 0.0.0.0} to expose externally, or a specific NIC address for multi-homed
   *     hosts.
   * @param port TCP port to bind on {@code bindHost}; must be in {@code [0, 65535]}. {@code 0}
   *     requests an ephemeral port — useful for tests; the actual bound port is then available via
   *     {@link #boundPort()} after {@link #start()}.
   * @param registry the Prometheus meter registry whose {@code scrape()} output backs the {@code
   *     /metrics} route; must not be null
   * @throws IllegalArgumentException if {@code bindHost} is blank or {@code port} is outside {@code
   *     [0, 65535]}
   * @throws NullPointerException if {@code bindHost} or {@code registry} is null
   */
  public MetricsHttpServer(
      final String bindHost, final int port, final PrometheusMeterRegistry registry) {
    Objects.requireNonNull(bindHost, "bindHost");
    if (bindHost.isBlank()) {
      throw new IllegalArgumentException("bindHost must not be blank");
    }
    if (port < 0 || port > 65_535) {
      throw new IllegalArgumentException("port must be in [0, 65535], got: " + port);
    }
    this.bindHost = bindHost;
    this.port = port;
    this.registry = Objects.requireNonNull(registry, "registry");
  }

  /**
   * Factory that constructs a {@link MetricsHttpServer} from environment variables, applying the
   * documented defaults when env vars are unset or blank. Unlike {@link
   * E2eManagementServer#fromEnvironment}, this factory ALWAYS returns a non-null server — the
   * metrics endpoint is a production feature and has no opt-in gate.
   *
   * @param registry the Prometheus meter registry; must not be null
   * @return a configured but un-started server
   * @throws NullPointerException if {@code registry} is null
   * @throws IllegalStateException if {@link #ENV_PORT} is set but cannot be parsed as a valid TCP
   *     port
   */
  public static MetricsHttpServer fromEnvironment(final PrometheusMeterRegistry registry) {
    Objects.requireNonNull(registry, "registry");
    final var bindHost = resolveBind();
    final int parsedPort = resolvePort();
    return new MetricsHttpServer(bindHost, parsedPort, registry);
  }

  private static String resolveBind() {
    final var raw = System.getenv(ENV_BIND);
    if (raw == null || raw.isBlank()) {
      return DEFAULT_BIND;
    }
    return raw.trim();
  }

  private static int resolvePort() {
    final var raw = System.getenv(ENV_PORT);
    if (raw == null || raw.isBlank()) {
      return DEFAULT_PORT;
    }
    try {
      return Integer.parseInt(raw.trim());
    } catch (final NumberFormatException e) {
      throw new IllegalStateException(
          ENV_PORT + " must be a valid TCP port number, got: '" + raw + "'", e);
    }
  }

  /**
   * Binds the HTTP listener on {@code bindHost:port} and registers the {@code /metrics} and {@code
   * /healthz} handlers. Installs a single-thread executor for scrape handling. The handlers do not
   * touch any application thread.
   *
   * @throws IOException if the listener cannot bind (e.g., port already in use, permission denied)
   * @throws IllegalStateException if {@link #start()} has already been called on this instance
   */
  public void start() throws IOException {
    if (server != null) {
      throw new IllegalStateException("MetricsHttpServer already started");
    }
    server = HttpServer.create(new InetSocketAddress(bindHost, port), 0);
    server.createContext("/metrics", this::handleMetrics);
    server.createContext("/healthz", this::handleHealthz);
    // Root context catches everything not matched by /metrics or /healthz and returns 404.
    // HttpServer's longest-prefix match guarantees /metrics and /healthz take precedence.
    server.createContext("/", this::handleUnknown);
    // Bounded single-thread executor — Prometheus scrapes are infrequent (15-60s cadence) and
    // serial; a pool would just waste idle threads. Daemon thread so it never blocks JVM exit if
    // the shutdown hook is bypassed (e.g. kill -9 on a child).
    executor =
        Executors.newSingleThreadExecutor(
            r -> {
              final var t = new Thread(r, "metrics-http");
              t.setDaemon(true);
              return t;
            });
    server.setExecutor(executor);
    server.start();
    LOG.info("Metrics endpoint listening on http://{}:{}/metrics", bindHost, boundPort());
  }

  /**
   * Returns the actual port the server is bound to. Useful when the caller asked for an ephemeral
   * port (via {@code port=0}) — for example in unit tests.
   *
   * @return the bound TCP port
   * @throws IllegalStateException if {@link #start()} has not been called
   */
  public int boundPort() {
    if (server == null) {
      throw new IllegalStateException("server not started");
    }
    return server.getAddress().getPort();
  }

  /**
   * Returns the URL of the metrics endpoint, e.g. {@code http://127.0.0.1:9100/metrics}. Logged at
   * startup so operators can see the resolved address without grepping the boot output.
   *
   * @return the absolute URL of the {@code /metrics} route
   * @throws IllegalStateException if {@link #start()} has not been called
   */
  public String metricsUrl() {
    return "http://" + bindHost + ":" + boundPort() + "/metrics";
  }

  private void handleMetrics(final HttpExchange exchange) throws IOException {
    if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
      respond(exchange, 405, "text/plain; charset=utf-8", "method not allowed");
      return;
    }
    final String scrape;
    try {
      scrape = registry.scrape();
    } catch (final RuntimeException e) {
      // PrometheusMeterRegistry.scrape() should not throw, but the contract is not enforced — guard
      // so a bad meter cannot poison every subsequent scrape with an HTTP 500 storm.
      LOG.error("PrometheusMeterRegistry.scrape() failed", e);
      respond(exchange, 500, "text/plain; charset=utf-8", "scrape failed: " + e.getMessage());
      return;
    }
    respond(exchange, 200, PROMETHEUS_CONTENT_TYPE, scrape);
  }

  private void handleHealthz(final HttpExchange exchange) throws IOException {
    if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
      respond(exchange, 405, "text/plain; charset=utf-8", "method not allowed");
      return;
    }
    respond(exchange, 200, "text/plain; charset=utf-8", "ok\n");
  }

  private void handleUnknown(final HttpExchange exchange) throws IOException {
    respond(exchange, 404, "text/plain; charset=utf-8", "not found");
  }

  private static void respond(
      final HttpExchange exchange, final int status, final String contentType, final String body)
      throws IOException {
    final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", contentType);
    exchange.sendResponseHeaders(status, bytes.length);
    try (var os = exchange.getResponseBody()) {
      os.write(bytes);
    }
  }

  /**
   * Stops the HTTP server with a {@link #STOP_GRACE_SECONDS}-second grace period for in-flight
   * scrapes, then shuts down the executor with a {@link #EXECUTOR_GRACE_SECONDS}-second grace
   * period. Idempotent — subsequent calls are no-ops.
   */
  @Override
  public void close() {
    if (server != null) {
      try {
        server.stop(STOP_GRACE_SECONDS);
        LOG.info("Metrics endpoint stopped");
      } catch (final RuntimeException e) {
        LOG.warn("Metrics endpoint stop raised", e);
      } finally {
        server = null;
      }
    }
    if (executor != null) {
      executor.shutdown();
      try {
        if (!executor.awaitTermination(EXECUTOR_GRACE_SECONDS, TimeUnit.SECONDS)) {
          executor.shutdownNow();
        }
      } catch (final InterruptedException e) {
        executor.shutdownNow();
        Thread.currentThread().interrupt();
      } finally {
        executor = null;
      }
    }
  }
}
