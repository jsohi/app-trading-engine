package com.trading.engine.e2e.evidence;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * In-memory recorder for APP-62 boundary-fuzz evidence rows. Each pre-trade check exercised by
 * {@link RiskControlEvidenceIT} calls {@link #record} with a single boundary case (input value,
 * expected and observed reject reasons, and pass/fail flag) — the {@link EvidenceReportListener}
 * snapshots and formats these rows into the regulator-facing markdown table when the JUnit Platform
 * test plan finishes.
 *
 * <p><b>Design rationale.</b> A static singleton list is used (rather than a JUnit extension /
 * test-instance field) so the {@link EvidenceReportListener} — which lives outside the test
 * instance lifecycle and is loaded once per JVM via the {@code TestExecutionListener} SPI — can
 * read the accumulated rows without needing a back-channel to every test class. The {@code
 * riskControlEvidence} Gradle task forks a fresh JVM per run and {@link #clear} is invoked up-front
 * by {@link EvidenceReportListener#testPlanExecutionStarted} so cross-run leakage is impossible.
 *
 * <p><b>Threading.</b> Not thread-safe — the {@code :integration-tests:riskControlEvidence} task
 * runs tests sequentially on a single forked JVM (no parallel execution configured), and the JUnit
 * Platform test engine guarantees ordered test-method invocations. Adding {@code synchronized} here
 * would be defensive theatre with no callers that benefit.
 *
 * <p><b>Allocation.</b> Off the hot path — allocates one {@link Row} per boundary case (≈40 total
 * for APP-62) and one {@link ArrayList} for the backing collection. Acceptable for a regulator-
 * facing audit pack; not intended for production code.
 */
public final class RiskEvidenceRecorder {

  private static final List<Row> ROWS = new ArrayList<>();

  private RiskEvidenceRecorder() {}

  /**
   * Records one boundary-fuzz evidence row.
   *
   * @param checkName the APP-62 check identifier (e.g. {@code "PositionLimit"}); appears in the
   *     {@code checkName} column of the rendered markdown table
   * @param boundaryCase short human-readable label for the boundary scenario (e.g. {@code "limit-1
   *     (admit)"}); appears in the {@code boundaryCase} column
   * @param inputValue string-formatted representation of the driving input (e.g. {@code
   *     "projectedLong=9, maxLongPosition=10"}); appears in the {@code inputValue} column
   * @param expectedReason expected reject reason (e.g. {@code "PositionLimitExceeded"}) or {@code
   *     "(admit)"} for non-reject expectations
   * @param observedReason observed reject reason (or {@code "(admit)"})
   * @param pass {@code true} when observed matched expected
   */
  public static void record(
      final String checkName,
      final String boundaryCase,
      final String inputValue,
      final String expectedReason,
      final String observedReason,
      final boolean pass) {
    ROWS.add(new Row(checkName, boundaryCase, inputValue, expectedReason, observedReason, pass));
  }

  /**
   * Returns an unmodifiable snapshot of every row recorded so far. Called by {@link
   * EvidenceReportListener} when rendering the markdown evidence pack.
   *
   * @return an unmodifiable list view of the recorded boundary-fuzz rows in insertion order
   */
  public static List<Row> snapshot() {
    return Collections.unmodifiableList(new ArrayList<>(ROWS));
  }

  /**
   * Clears the recorder. Invoked by {@link EvidenceReportListener#testPlanExecutionStarted} at the
   * start of every {@code riskControlEvidence} run so the listener never accumulates rows across
   * multiple test-plan executions in the same JVM (defensive — Gradle forks a fresh JVM per task,
   * but in-IDE re-runs share a JVM).
   */
  public static void clear() {
    ROWS.clear();
  }

  /**
   * One boundary-fuzz evidence row. Public so {@link EvidenceReportListener} can read fields
   * directly without going through accessors — this class is package-test-only.
   */
  public record Row(
      String checkName,
      String boundaryCase,
      String inputValue,
      String expectedReason,
      String observedReason,
      boolean pass) {}
}
