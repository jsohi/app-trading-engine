package com.trading.engine.launcher;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Dev/E2E-only HTTP management endpoint that lets the full-stack Playwright harness pause and
 * resume the pricing-service {@code AgentRunner} without killing the launcher JVM (which would also
 * pause the WebSocket-server egress thread and heartbeats — see spec 09 §Harness Gap).
 *
 * <p><b>Production safety.</b> The server is constructed only when the environment variable {@code
 * TRADING_E2E_MGMT_ENABLED=1} is present. {@link #fromEnvironment(Runnable, Runnable)} returns
 * {@code null} otherwise, and the launcher never wires it in. The endpoint binds to {@code
 * 127.0.0.1} so it is unreachable from any other host even if the env var is set in error.
 *
 * <p><b>Wire protocol.</b>
 *
 * <ul>
 *   <li>{@code POST /e2e/pricing/pause} — invokes the pause callback (closes the pricing
 *       AgentRunner). Returns 200 with body {@code "paused"} on success, 500 on callback failure.
 *   <li>{@code POST /e2e/pricing/resume} — invokes the resume callback (re-launches the pricing
 *       AgentRunner with the original config). Returns 200 with body {@code "resumed"} on success.
 *   <li>{@code GET /e2e/health} — returns 200 with body {@code "ok"}. Used by the harness to verify
 *       the management endpoint is reachable before invoking pause/resume.
 *   <li>Any other method/path returns 404 / 405.
 * </ul>
 *
 * <p><b>Threading.</b> The JDK {@link HttpServer} uses an internal executor (default
 * single-threaded sync executor in this configuration). Pause/resume callbacks run on the executor
 * thread, NOT the launcher main thread. Callbacks MUST be thread-safe with respect to the rest of
 * launcher state (they are — they only touch the {@code pricingRef AtomicReference}).
 *
 * <p><b>Allocation.</b> Cold path — only invoked during e2e tests. No zero-allocation requirement.
 *
 * <p><b>Lifecycle.</b> {@link #start()} binds the listener; {@link #close()} stops the server with
 * a 1-second grace period for in-flight handlers. The launcher's shutdown hook calls {@link
 * #close()}.
 */
public final class E2eManagementServer implements AutoCloseable {

  private static final Logger LOG = LogManager.getLogger(E2eManagementServer.class);

  /** Environment variable that gates the entire server. Must equal {@code "1"} to enable. */
  public static final String ENV_ENABLED = "TRADING_E2E_MGMT_ENABLED";

  /** Environment variable for the listener TCP port. Required when {@link #ENV_ENABLED} is set. */
  public static final String ENV_PORT = "TRADING_E2E_MGMT_PORT";

  /**
   * Loopback bind address. The endpoint MUST NEVER be reachable from another host — pausing the
   * pricing service in production would corrupt market data.
   */
  private static final String BIND_HOST = "127.0.0.1";

  /** Grace period for in-flight HTTP handlers during {@link #close()}. */
  private static final int STOP_GRACE_SECONDS = 1;

  private final int port;
  private final Runnable pauseCallback;
  private final Runnable resumeCallback;
  private HttpServer server;

  /**
   * Constructs a management server bound to {@code 127.0.0.1:port}. Does not start the listener —
   * call {@link #start()}.
   *
   * @param port TCP port to bind on the loopback interface; must be in [1, 65535]
   * @param pauseCallback invoked on {@code POST /e2e/pricing/pause}; must not be null
   * @param resumeCallback invoked on {@code POST /e2e/pricing/resume}; must not be null
   * @throws IllegalArgumentException if port is out of range
   * @throws NullPointerException if either callback is null
   */
  public E2eManagementServer(
      final int port, final Runnable pauseCallback, final Runnable resumeCallback) {
    if (port < 1 || port > 65_535) {
      throw new IllegalArgumentException("port must be in [1, 65535], got: " + port);
    }
    this.port = port;
    this.pauseCallback = Objects.requireNonNull(pauseCallback, "pauseCallback");
    this.resumeCallback = Objects.requireNonNull(resumeCallback, "resumeCallback");
  }

  /**
   * Factory that constructs an {@link E2eManagementServer} from environment variables if {@link
   * #ENV_ENABLED} is set to {@code "1"}, or returns {@code null} otherwise.
   *
   * <p>This is the gate that keeps the endpoint out of production: the launcher only calls {@link
   * #start()} on a non-null result, and the env var defaults to unset.
   *
   * @param pauseCallback invoked on pause; must not be null
   * @param resumeCallback invoked on resume; must not be null
   * @return a configured but un-started server, or {@code null} if the env var is not set to "1"
   * @throws IllegalStateException if {@link #ENV_ENABLED}=1 but {@link #ENV_PORT} is missing or
   *     invalid
   */
  public static E2eManagementServer fromEnvironment(
      final Runnable pauseCallback, final Runnable resumeCallback) {
    final var enabled = System.getenv(ENV_ENABLED);
    if (enabled == null || !enabled.equals("1")) {
      return null;
    }
    final var portStr = System.getenv(ENV_PORT);
    if (portStr == null || portStr.isBlank()) {
      throw new IllegalStateException(
          ENV_ENABLED + "=1 but " + ENV_PORT + " is unset — refusing to start management endpoint");
    }
    final int parsedPort;
    try {
      parsedPort = Integer.parseInt(portStr.trim());
    } catch (final NumberFormatException e) {
      throw new IllegalStateException(
          ENV_PORT + " must be a valid TCP port number, got: '" + portStr + "'", e);
    }
    return new E2eManagementServer(parsedPort, pauseCallback, resumeCallback);
  }

  /**
   * Binds the HTTP listener on {@code 127.0.0.1:port} and registers the pause / resume / health
   * handlers. Uses the JDK default executor (synchronous, single-threaded) — sufficient for the
   * handful of e2e management requests per test run.
   *
   * @throws IOException if the listener cannot bind (e.g., port already in use)
   */
  public void start() throws IOException {
    server = HttpServer.create(new InetSocketAddress(BIND_HOST, port), 0);
    server.createContext("/e2e/pricing/pause", this::handlePause);
    server.createContext("/e2e/pricing/resume", this::handleResume);
    server.createContext("/e2e/health", this::handleHealth);
    server.start();
    LOG.warn(
        "E2E management endpoint enabled on http://{}:{}/e2e/* — "
            + "MUST NOT be set in production; gated by env {}=1",
        BIND_HOST,
        port,
        ENV_ENABLED);
  }

  /**
   * Returns the actual port the server is bound to. Useful when the caller asked for an ephemeral
   * port (not used by full-stack-e2e.sh, but enables clean unit tests).
   *
   * @return the bound port
   * @throws IllegalStateException if {@link #start()} has not been called
   */
  public int boundPort() {
    if (server == null) {
      throw new IllegalStateException("server not started");
    }
    return server.getAddress().getPort();
  }

  private void handlePause(final HttpExchange exchange) throws IOException {
    if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
      respond(exchange, 405, "method not allowed");
      return;
    }
    try {
      LOG.info("E2E management: pausing pricing-service");
      pauseCallback.run();
      respond(exchange, 200, "paused");
    } catch (final RuntimeException e) {
      LOG.error("E2E management: pause callback failed", e);
      respond(exchange, 500, "pause failed: " + e.getMessage());
    }
  }

  private void handleResume(final HttpExchange exchange) throws IOException {
    if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
      respond(exchange, 405, "method not allowed");
      return;
    }
    try {
      LOG.info("E2E management: resuming pricing-service");
      resumeCallback.run();
      respond(exchange, 200, "resumed");
    } catch (final RuntimeException e) {
      LOG.error("E2E management: resume callback failed", e);
      respond(exchange, 500, "resume failed: " + e.getMessage());
    }
  }

  private void handleHealth(final HttpExchange exchange) throws IOException {
    if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
      respond(exchange, 405, "method not allowed");
      return;
    }
    respond(exchange, 200, "ok");
  }

  private static void respond(final HttpExchange exchange, final int status, final String body)
      throws IOException {
    final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
    exchange.sendResponseHeaders(status, bytes.length);
    try (var os = exchange.getResponseBody()) {
      os.write(bytes);
    }
  }

  /**
   * Stops the HTTP server with a {@link #STOP_GRACE_SECONDS}-second grace period for in-flight
   * handlers. Idempotent.
   */
  @Override
  public void close() {
    if (server != null) {
      try {
        server.stop(STOP_GRACE_SECONDS);
        LOG.info("E2E management endpoint stopped");
      } catch (final RuntimeException e) {
        LOG.warn("E2E management endpoint stop raised", e);
      } finally {
        server = null;
      }
    }
  }
}
