package com.trading.engine.monitoring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Locks in the validator's pass/fail contract:
 *
 * <ul>
 *   <li>The real shipped dashboards + alerts.yaml pass.
 *   <li>Empty dashboards directory fails with exit 1.
 *   <li>Duplicate dashboard uid fails with exit 1.
 *   <li>Wrong-shape alerts YAML fails with exit 1.
 * </ul>
 *
 * <p>The validator calls {@link System#exit(int)}; tests trap the call with a {@link
 * SecurityManager}-free approach by reading the exit code through a fork would be cleanest, but a
 * SecurityManager-style trap is fragile on JDK 25 (SecurityManager removed). Instead we install a
 * dedicated wrapper that intercepts the exit via a thread-local flag.
 */
final class GrafanaValidatorTest {

  @TempDir Path tmp;

  private PrintStream originalOut;
  private PrintStream originalErr;
  private ByteArrayOutputStream capturedOut;
  private ByteArrayOutputStream capturedErr;

  @BeforeEach
  void redirectStdio() {
    originalOut = System.out;
    originalErr = System.err;
    capturedOut = new ByteArrayOutputStream();
    capturedErr = new ByteArrayOutputStream();
    System.setOut(new PrintStream(capturedOut));
    System.setErr(new PrintStream(capturedErr));
  }

  @AfterEach
  void restoreStdio() {
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  @Test
  void shippedArtifacts_validate_pass() throws Exception {
    final Path projectRoot = repoRoot().resolve("monitoring");
    final Path dashboards = projectRoot.resolve("dashboards");
    final Path alerts = projectRoot.resolve("alerts.yaml");
    final Path dashboardSchema =
        projectRoot.resolve("src/main/resources/schema/grafana-dashboard-v11.schema.json");
    final Path alertsSchema =
        projectRoot.resolve("src/main/resources/schema/prometheus-alerts.schema.json");
    final Path marker = tmp.resolve("marker.txt");

    final int code = runValidator(dashboards, alerts, dashboardSchema, alertsSchema, marker);
    assertEquals(0, code, "validator must accept the shipped dashboards. stderr=" + capturedErr);
    assertTrue(Files.exists(marker), "marker file should be written on success");
  }

  @Test
  void emptyDashboardsDir_fails() throws Exception {
    final Path emptyDir = Files.createDirectory(tmp.resolve("empty"));
    final Path projectRoot = repoRoot().resolve("monitoring");
    final int code =
        runValidator(
            emptyDir,
            projectRoot.resolve("alerts.yaml"),
            projectRoot.resolve("src/main/resources/schema/grafana-dashboard-v11.schema.json"),
            projectRoot.resolve("src/main/resources/schema/prometheus-alerts.schema.json"),
            tmp.resolve("marker.txt"));
    assertEquals(1, code);
    assertTrue(capturedErr.toString().contains("no *.json dashboards"));
  }

  @Test
  void duplicateUid_fails() throws Exception {
    final Path projectRoot = repoRoot().resolve("monitoring");
    final Path duplicates = Files.createDirectory(tmp.resolve("dupes"));
    final Path original = projectRoot.resolve("dashboards/websocket-server.json");
    Files.copy(original, duplicates.resolve("a.json"));
    Files.copy(original, duplicates.resolve("b.json"));
    final int code =
        runValidator(
            duplicates,
            projectRoot.resolve("alerts.yaml"),
            projectRoot.resolve("src/main/resources/schema/grafana-dashboard-v11.schema.json"),
            projectRoot.resolve("src/main/resources/schema/prometheus-alerts.schema.json"),
            tmp.resolve("marker.txt"));
    assertEquals(1, code);
    assertTrue(capturedErr.toString().contains("duplicate uid"));
  }

  @Test
  void malformedAlerts_fails() throws Exception {
    final Path projectRoot = repoRoot().resolve("monitoring");
    final Path badAlerts = tmp.resolve("bad-alerts.yaml");
    Files.writeString(badAlerts, "groups:\n  - name: bad\n    rules:\n      - foo: bar\n");
    final int code =
        runValidator(
            projectRoot.resolve("dashboards"),
            badAlerts,
            projectRoot.resolve("src/main/resources/schema/grafana-dashboard-v11.schema.json"),
            projectRoot.resolve("src/main/resources/schema/prometheus-alerts.schema.json"),
            tmp.resolve("marker.txt"));
    assertEquals(1, code);
    assertTrue(
        capturedErr.toString().contains("bad-alerts.yaml"),
        "stderr should reference the bad alerts file. stderr=" + capturedErr);
  }

  /**
   * Invokes {@link GrafanaValidator#main(String[])} in-process while trapping {@link
   * System#exit(int)} via an {@code ExitTrappedException} (rethrown from a no-op security check
   * replacement on JDK 25 — we use a small wrapper around {@code Runtime.exit} via reflection-free
   * indirection: launch the main on a thread that catches a custom exit signal). Because JDK 25
   * removed SecurityManager, the simplest portable approach is to run main on a dedicated thread
   * and have it short-circuit on a static holder when test mode is enabled.
   *
   * <p>For reliability we invoke {@code main} directly and capture the exit-code by reading the
   * marker file presence + the captured stderr. We CANNOT trap System.exit; instead we run main in
   * a separate JVM via a {@link Runtime#exec} would be heavyweight. As a pragmatic alternative, the
   * validator's contract is that on success the marker file is written and stderr is empty; on
   * failure stderr contains a diagnostic. We assert both.
   */
  private int runValidator(
      final Path dashboardsDir,
      final Path alertsFile,
      final Path dashboardSchema,
      final Path alertsSchema,
      final Path markerFile)
      throws Exception {
    // Run in a subprocess so we get a real exit code without depending on SecurityManager
    // (removed in JDK 25).
    final String classpath = System.getProperty("java.class.path");
    final String javaHome = System.getProperty("java.home");
    final String javaBin = javaHome + "/bin/java";
    final ProcessBuilder pb =
        new ProcessBuilder(
            javaBin,
            "-cp",
            classpath,
            GrafanaValidator.class.getName(),
            dashboardsDir.toAbsolutePath().toString(),
            alertsFile.toAbsolutePath().toString(),
            dashboardSchema.toAbsolutePath().toString(),
            alertsSchema.toAbsolutePath().toString(),
            markerFile.toAbsolutePath().toString());
    pb.redirectErrorStream(false);
    final Process proc = pb.start();
    final byte[] stdoutBytes = proc.getInputStream().readAllBytes();
    final byte[] stderrBytes = proc.getErrorStream().readAllBytes();
    final int code = proc.waitFor();
    capturedOut.write(stdoutBytes);
    capturedErr.write(stderrBytes);
    return code;
  }

  private static Path repoRoot() {
    // Tests run from the :monitoring project dir; walk up to the repo root.
    Path p = Path.of("").toAbsolutePath();
    while (p != null && !Files.exists(p.resolve("settings.gradle.kts"))) {
      p = p.getParent();
    }
    if (p == null) {
      throw new IllegalStateException(
          "could not locate repo root from " + Path.of("").toAbsolutePath());
    }
    return p;
  }
}
