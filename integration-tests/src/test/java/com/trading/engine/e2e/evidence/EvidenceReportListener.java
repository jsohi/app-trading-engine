package com.trading.engine.e2e.evidence;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestPlan;

/**
 * JUnit Platform {@link TestExecutionListener} that renders the APP-62 boundary-fuzz evidence pack
 * as a regulator-friendly markdown table after the {@code @Tag("risk-evidence")} test plan
 * completes.
 *
 * <p><b>Wiring.</b> Discovered through Java's {@link java.util.ServiceLoader ServiceLoader} via the
 * SPI descriptor at {@code
 * integration-tests/src/test/resources/META-INF/services/org.junit.platform.launcher.TestExecutionListener}.
 * JUnit Platform 1.x auto-loads any {@code TestExecutionListener} on the test classpath that
 * declares itself through this SPI, so the listener fires for every Gradle test JVM (including
 * {@code :test} and {@code :riskControlEvidence}).
 *
 * <p><b>Output paths.</b> Two artifacts:
 *
 * <ul>
 *   <li>{@code build/reports/risk-control-evidence/APP-62-evidence-&lt;sha&gt;.md} — the
 *       SHA-stamped run-time artifact under {@code $buildDir}, NOT committed
 *   <li>{@code docs/evidence/APP-62-evidence-latest.md} — the canonical regulator-facing artifact
 *       that IS committed alongside the implementation. Reviewers look at this file in the PR.
 * </ul>
 *
 * <p><b>Schema-SHA computation.</b> The header records the git SHA of the cluster commit and the
 * git-tracked content hash of {@code messages/src/main/resources/trading-schema.xml} via {@code git
 * rev-parse HEAD:messages/src/main/resources/trading-schema.xml}. On a dirty worktree the commit
 * SHA refers to the last commit (NOT the dirty work) and the schema SHA likewise reflects the
 * committed schema — a warning suffix {@code (dirty)} is appended to the commit SHA so the
 * regulator artifact never silently lies about working-tree contamination.
 *
 * <p><b>Inactivity.</b> When no {@code @Tag("risk-evidence")} test methods run (e.g. the default
 * {@code :test} task, which {@code excludeTags("risk-evidence")}), {@link RiskEvidenceRecorder}
 * accumulates zero rows and {@link #testPlanExecutionFinished} is a no-op (it only writes when at
 * least one row is present). This keeps the listener inert for non-evidence test JVMs even though
 * it is auto-registered globally via SPI.
 *
 * <p><b>Threading.</b> JUnit Platform invokes lifecycle callbacks from a single test-engine thread.
 * The listener instance holds no mutable state and never escapes that thread.
 *
 * <p><b>Allocation.</b> Off the hot path — runs once per test-plan completion. Allocates a {@link
 * StringBuilder} for the markdown body and the two output {@link Path Paths}; acceptable for an
 * audit artifact.
 */
public final class EvidenceReportListener implements TestExecutionListener {

  /**
   * System-property key consumed from the {@code riskControlEvidence} Gradle task. Value is the
   * absolute path to the build-local report directory ({@code
   * build/reports/risk-control-evidence}). When unset (e.g. running under the default {@code :test}
   * task), the listener stays inert.
   */
  private static final String OUTPUT_DIR_PROP = "evidence.outputDir";

  /**
   * System-property key for the committed canonical artifact path ({@code
   * docs/evidence/APP-62-evidence-latest.md}). When unset, the listener still writes the
   * SHA-stamped build-local copy but skips the committed copy — useful for ad-hoc local runs.
   */
  private static final String COMMITTED_LATEST_PROP = "evidence.committedLatestPath";

  private static final DateTimeFormatter UTC_TS =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneOffset.UTC);

  /** {@inheritDoc} */
  @Override
  public void testPlanExecutionStarted(final TestPlan testPlan) {
    // Defensive: clear any stale rows from a prior in-JVM run (Gradle forks a fresh JVM per task,
    // but in-IDE re-runs share a JVM and would otherwise duplicate every row).
    RiskEvidenceRecorder.clear();
  }

  /**
   * Snapshots the recorded rows and writes the markdown evidence pack. No-op when no rows were
   * recorded (i.e. the test plan did not include any {@code @Tag("risk-evidence")} methods).
   *
   * @param testPlan the JUnit Platform test plan that just finished (unused — rows come from the
   *     static recorder)
   */
  @Override
  public void testPlanExecutionFinished(final TestPlan testPlan) {
    final var rows = RiskEvidenceRecorder.snapshot();
    if (rows.isEmpty()) {
      return;
    }
    final var outputDir = System.getProperty(OUTPUT_DIR_PROP);
    if (outputDir == null) {
      // Listener loaded under the default :test JVM — should never trip because the test methods
      // are excluded by tag, but defensive in case a developer @Tags a non-evidence test by
      // mistake.
      return;
    }
    final var commitSha = readCommitSha();
    final var schemaSha = readSchemaSha();
    final var runTs = UTC_TS.format(Instant.now());
    final var body = renderMarkdown(rows, commitSha, schemaSha, runTs);
    try {
      final var dir = Paths.get(outputDir);
      Files.createDirectories(dir);
      // SHA-suffixed build-local copy.
      final var shaTag = sanitizeShaForFilename(commitSha);
      final var buildLocal = dir.resolve("APP-62-evidence-" + shaTag + ".md");
      Files.writeString(buildLocal, body, StandardCharsets.UTF_8);
      // Convenience un-suffixed pointer for "latest run on this checkout".
      final var buildLocalLatest = dir.resolve("APP-62-evidence.md");
      Files.writeString(buildLocalLatest, body, StandardCharsets.UTF_8);
      // Committed canonical artifact.
      final var committedPath = System.getProperty(COMMITTED_LATEST_PROP);
      if (committedPath != null) {
        final var committed = Paths.get(committedPath);
        // Defensive: a bare filename system-property value yields a null parent path; only
        // create the parent directory when it exists, otherwise let writeString handle the
        // working-directory-relative file directly.
        final var committedParent = committed.getParent();
        if (committedParent != null) {
          Files.createDirectories(committedParent);
        }
        Files.writeString(committed, body, StandardCharsets.UTF_8);
      }
    } catch (final IOException ioe) {
      throw new EvidenceWriteException("failed to write APP-62 evidence pack", ioe);
    }
  }

  /**
   * Renders the markdown body — header (with SHAs + UTC timestamp), one-row-per-boundary table,
   * summary footer, and the FINRA 3110 / RTS 6 §9 / SEC 15c3-5 citation block.
   *
   * @param rows the recorded boundary-fuzz rows
   * @param commitSha resolved git commit SHA (may carry a {@code (dirty)} suffix)
   * @param schemaSha resolved git content hash for {@code trading-schema.xml}
   * @param runTs UTC timestamp string for the run
   * @return the full markdown document body
   */
  private static String renderMarkdown(
      final List<RiskEvidenceRecorder.Row> rows,
      final String commitSha,
      final String schemaSha,
      final String runTs) {
    final var sb = new StringBuilder(8192);
    sb.append("# APP-62 — Pre-Trade Risk Control Boundary-Fuzz Evidence Pack\n\n");
    sb.append("**Regulator-facing artifact.** This file is the canonical evidence pack ")
        .append("referenced from PR review for the APP-62 pre-trade risk-control set. ")
        .append("SHA-stamped per-run copies live under ")
        .append("`build/reports/risk-control-evidence/APP-62-evidence-<sha>.md` ")
        .append("(emitted by the `:integration-tests:riskControlEvidence` Gradle task and ")
        .append("NOT committed to the repository).\n\n");
    sb.append("| field | value |\n");
    sb.append("|---|---|\n");
    sb.append("| git commit SHA | `").append(commitSha).append("` |\n");
    sb.append("| schema SHA (`messages/src/main/resources/trading-schema.xml`) | `")
        .append(schemaSha)
        .append("` |\n");
    sb.append("| run timestamp (UTC) | ").append(runTs).append(" |\n");
    sb.append("| plan reference | APP-62 plan §5.3 (boundary-fuzz evidence pack) |\n");
    sb.append("\n## Boundary-fuzz table\n\n");
    sb.append(
        "| checkName | boundaryCase | inputValue | expectedReason | observedReason | result |\n");
    sb.append("|---|---|---|---|---|---|\n");
    int passes = 0;
    for (final var r : rows) {
      sb.append("| ")
          .append(r.checkName())
          .append(" | ")
          .append(r.boundaryCase())
          .append(" | `")
          .append(r.inputValue())
          .append("` | ")
          .append(r.expectedReason())
          .append(" | ")
          .append(r.observedReason())
          .append(" | ")
          .append(r.pass() ? "PASS" : "**FAIL**")
          .append(" |\n");
      if (r.pass()) {
        passes++;
      }
    }
    final int total = rows.size();
    final int fails = total - passes;
    sb.append("\n## Summary\n\n");
    sb.append("- total rows: ").append(total).append("\n");
    sb.append("- pass: ").append(passes).append("\n");
    sb.append("- fail: ").append(fails).append("\n");
    sb.append("\n## Regulatory cross-reference\n\n");
    sb.append(
            "- **FINRA Rule 3110(a) — Supervision.** Pre-trade controls must be tested and the test ")
        .append("evidence retained as part of the firm's supervisory record. This pack is the ")
        .append("artifact referenced from the APP-62 PR for that test evidence.\n");
    sb.append(
            "- **SEC Rule 15c3-5(b) — Market Access Rule.** Brokers providing market access must have ")
        .append("risk-management controls reasonably designed to systematically prevent the entry ")
        .append("of erroneous orders, by rejecting orders that exceed appropriate price or size ")
        .append("parameters or that exceed pre-set credit / capital thresholds. Each row below ")
        .append("documents the boundary at which the corresponding 15c3-5(b) control engages.\n");
    sb.append("- **MiFID II RTS 6 Art. 9 (Pre-trade controls) and Art. 17 (Periodic review).** ")
        .append("Investment firms engaged in algorithmic trading must apply pre-trade controls on ")
        .append("order entry (price collar, max order value, max order volume) and must annually ")
        .append("self-assess the calibration. The boundary cases below provide the calibration ")
        .append("evidence for the periodic review.\n");
    sb.append(
            "- **MiFID II RTS 6 §1(2) — Four-eyes principle.** Risk-limit changes must be subject to ")
        .append("dual control. The `FourEyesViolation` rows below evidence that the cluster ")
        .append("rejects single-eye and self-approved risk-limit loads.\n");
    sb.append("\n_Generated by `:integration-tests:riskControlEvidence` ")
        .append("(class `RiskControlEvidenceIT`, listener `EvidenceReportListener`)._");
    sb.append("\n");
    return sb.toString();
  }

  /**
   * Reads the git commit SHA via {@code git rev-parse HEAD} and appends {@code (dirty)} when the
   * worktree has uncommitted changes. Returns the literal string {@code "unknown"} when git is
   * unavailable.
   *
   * @return commit SHA with optional dirty suffix, or {@code "unknown"} on failure
   */
  private static String readCommitSha() {
    final var sha = runGit("rev-parse", "HEAD");
    if (sha == null) {
      return "unknown";
    }
    final var dirty = runGit("status", "--porcelain");
    if (dirty != null && !dirty.isBlank()) {
      return sha + " (dirty)";
    }
    return sha;
  }

  /**
   * Reads the git-tracked content hash of {@code trading-schema.xml} via {@code git rev-parse
   * HEAD:&lt;path&gt;}. Returns {@code "unknown"} when git is unavailable.
   *
   * @return schema blob SHA, or {@code "unknown"} on failure
   */
  private static String readSchemaSha() {
    final var sha = runGit("rev-parse", "HEAD:messages/src/main/resources/trading-schema.xml");
    return sha == null ? "unknown" : sha;
  }

  /**
   * Runs a {@code git} command and returns its trimmed stdout, or {@code null} on failure.
   *
   * @param args git subcommand and arguments (e.g. {@code "rev-parse"}, {@code "HEAD"})
   * @return trimmed stdout, or {@code null} on non-zero exit / IO failure / interruption
   */
  private static String runGit(final String... args) {
    Process proc = null;
    try {
      final var cmd = new String[args.length + 1];
      cmd[0] = "git";
      System.arraycopy(args, 0, cmd, 1, args.length);
      // Keep stderr separate (do NOT redirectErrorStream) so a git warning
      // ("warning: refname 'HEAD' is ambiguous", etc.) never corrupts the SHA we
      // emit into the regulator artifact header. Drain both streams to prevent
      // the child process blocking on a full pipe buffer.
      proc = new ProcessBuilder(cmd).redirectErrorStream(false).start();
      final var stdout = new String(proc.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
      proc.getErrorStream().readAllBytes();
      final int rc = proc.waitFor();
      if (rc != 0) {
        return null;
      }
      return stdout.trim();
    } catch (final IOException ioe) {
      return null;
    } catch (final InterruptedException ie) {
      // Forcibly terminate the git sub-process before re-asserting the interrupt
      // so we never leave a zombie on shutdown.
      if (proc != null && proc.isAlive()) {
        proc.destroyForcibly();
      }
      Thread.currentThread().interrupt();
      return null;
    }
  }

  /**
   * Maps a commit SHA (possibly carrying the {@code (dirty)} suffix) to a filesystem-safe filename
   * fragment.
   *
   * <p>Edge case: when {@code readCommitSha} returned {@code "unknown"} (git missing or rev-parse
   * failed), the {@code (dirty)} suffix is never appended upstream — see {@code readCommitSha}. In
   * that case the filename fragment is the literal {@code "unknown"} with NO {@code "-dirty"}
   * marker, even if the worktree is in fact dirty. The dirty-signal is therefore only meaningful
   * when paired with a real SHA; the {@code "unknown"} pathway is itself the audit failure signal.
   *
   * @param sha commit SHA, optionally with the {@code " (dirty)"} suffix or {@code "unknown"}
   * @return a filename-safe fragment: first 12 hex chars (or "unknown") plus an optional {@code
   *     "-dirty"} suffix
   */
  private static String sanitizeShaForFilename(final String sha) {
    final var dirty = sha.endsWith(" (dirty)");
    final var bare = dirty ? sha.substring(0, sha.length() - " (dirty)".length()) : sha;
    final var head = bare.length() >= 12 ? bare.substring(0, 12) : bare;
    return dirty ? head + "-dirty" : head;
  }

  /**
   * Wraps an {@link IOException} thrown while writing the evidence pack as an unchecked failure so
   * the {@code riskControlEvidence} Gradle task fails loudly — a regulator artifact missing from
   * the build report dir is a build-breaking condition, not a soft warning.
   */
  private static final class EvidenceWriteException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    EvidenceWriteException(final String message, final Throwable cause) {
      super(message, cause);
    }
  }
}
