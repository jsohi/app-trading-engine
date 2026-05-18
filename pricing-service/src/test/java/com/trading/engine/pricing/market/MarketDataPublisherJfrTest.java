package com.trading.engine.pricing.market;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.testsupport.clock.ControllableNanoClock;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * JFR instrumentation integration test for {@link MarketDataPublisher}.
 *
 * <p><b>Purpose.</b> Opens a JFR recording, drives the publisher through publish and reject
 * scenarios, dumps the recording, and asserts that {@code trading.MarketDataTickPublished} and
 * {@code trading.MarketDataTickRejected} events appear in the recording file. The test validates
 * that the JFR wiring in the production code path is live-end-to-end: event classes annotated,
 * registered, field-set, and committed correctly.
 *
 * <p><b>Publish sampling.</b> {@code MarketDataTickPublished} uses {@code @Period("100 ms")}
 * sampling, so it will NOT record one event per publish call. Instead, the JFR runtime samples at
 * 10 Hz. With 1000 ticks driven over ~10 drains (100 ms clock advances) the test asserts {@code
 * count >= 1} — at least one event must have been captured in the sampled window. Using an
 * exact-count assertion here would make the test brittle and environment-dependent.
 *
 * <p><b>Reject counting.</b> {@code MarketDataTickRejected} uses {@code @Threshold("0 ms")} (emit
 * every reject). The test drives exactly {@link #REJECT_COUNT} crossed-market ticks and asserts the
 * recorded event count equals {@link #REJECT_COUNT}. This verifies the every-reject contract.
 *
 * <p><b>Threading model.</b> Single-threaded JUnit test thread. The publisher's agent thread
 * invariant is met because all calls originate on the test thread (which is also the thread that
 * called {@code onStart()}).
 *
 * <p><b>Allocation.</b> No allocation constraints apply in test code. The JFR recording dump
 * allocates freely; the assertion loop reads event objects from the recording file.
 *
 * <p><b>Dependencies.</b> {@link MarketDataPublisher}, {@link FakeBroadcastPublisher}, {@link
 * ControllableNanoClock}, {@link MarketDataPublisherConfig}; JDK JFR API ({@code
 * jdk.jfr.Recording}, {@code jdk.jfr.consumer.RecordingFile}); JUnit Jupiter {@code @TempDir}.
 */
final class MarketDataPublisherJfrTest {

  // ── Constants ─────────────────────────────────────────────────────────────

  private static final long BID = 118_500_000_000L;
  private static final long ASK = 118_510_000_000L;
  private static final long SIZE = 1_000_000L * 100_000_000L;
  private static final long INGRESS = 1_700_000_000_000_000_000L;
  private static final long CADENCE_MICROS = 5_000L;
  private static final long HEARTBEAT_BASE_MS = 1_000L;

  /** Number of reject ticks driven; asserted exactly against recording count. */
  private static final int REJECT_COUNT = 5;

  /** Number of publish ticks driven (across multiple drain cycles). */
  private static final int PUBLISH_TICK_COUNT = 1_000;

  private static final long EURUSD = pack("EURUSD  ");

  private static long pack(final String s) {
    long packed = 0L;
    for (int i = 0; i < 8; i++) {
      final long b = i < s.length() ? (byte) s.charAt(i) : (byte) ' ';
      packed |= (b & 0xFFL) << (i * 8);
    }
    return packed;
  }

  // ── Helpers ───────────────────────────────────────────────────────────────

  private static MarketDataPublisher buildPublisher(
      final FakeBroadcastPublisher fake, final ControllableNanoClock clock) {
    final var config =
        new MarketDataPublisherConfig(
            MarketDataPublisherConfig.AdapterKind.DETERMINISTIC, CADENCE_MICROS, HEARTBEAT_BASE_MS);
    final var publisher = new MarketDataPublisher(fake, null, clock, clock, config);
    publisher.onStart();
    return publisher;
  }

  private static long countEvents(final Path jfrFile, final String eventName) throws IOException {
    try (final var rf = new RecordingFile(jfrFile)) {
      long count = 0L;
      while (rf.hasMoreEvents()) {
        final RecordedEvent event = rf.readEvent();
        if (eventName.equals(event.getEventType().getName())) {
          count++;
        }
      }
      return count;
    }
  }

  // ── Tests ─────────────────────────────────────────────────────────────────

  /**
   * Opens a JFR recording, drives 1000 publish ticks across multiple drain cycles, stops the
   * recording, and asserts at least one {@code trading.MarketDataTickPublished} event was captured.
   * The {@code @Period("100 ms")} sampling means count will be less than 1000 but must be >= 1 for
   * the wiring to be considered live.
   *
   * @param tempDir JUnit-managed temporary directory for the JFR dump file.
   */
  @Test
  void publishPath_withJfrRecording_emitsMarketDataTickPublishedEvents(@TempDir final Path tempDir)
      throws IOException {
    final var fake = new FakeBroadcastPublisher();
    final var clock = new ControllableNanoClock(1_000_000_000L);
    final var publisher = buildPublisher(fake, clock);

    final Path jfrFile = tempDir.resolve("publish-jfr-test.jfr");

    try (final var recording = new Recording()) {
      recording.enable("trading.MarketDataTickPublished");
      recording.start();

      // Drive PUBLISH_TICK_COUNT ticks across multiple drain cycles.
      // Each iteration advances 10 ms (2× the 5 ms cadence) so every tick triggers a drain.
      for (int i = 0; i < PUBLISH_TICK_COUNT; i++) {
        publisher.onTick(EURUSD, BID, ASK, SIZE, SIZE, INGRESS + i);
        clock.advanceMillis(10L);
        publisher.doWork();
      }

      recording.stop();
      recording.dump(jfrFile);
    }

    final long publishedCount = countEvents(jfrFile, "trading.MarketDataTickPublished");
    // Period-sampled at 100 ms; with 1000 ticks over ~10 s clock time expect multiple samples,
    // but the contract is only >= 1 to tolerate JFR period-window alignment variance.
    assertTrue(
        publishedCount >= 1,
        "Expected at least 1 trading.MarketDataTickPublished event in JFR recording; "
            + "got 0 — check that MarketDataTickPublished.shouldCommit() guard and commit() are "
            + "wired in MarketDataPublisher.publishOneSlot()");
  }

  /**
   * Opens a JFR recording, drives exactly {@link #REJECT_COUNT} crossed-market ticks (bid == ask),
   * stops the recording, and asserts that exactly {@link #REJECT_COUNT} {@code
   * trading.MarketDataTickRejected} events appear. The {@code @Threshold("0 ms")} contract requires
   * every reject to emit, so the count must match exactly.
   *
   * @param tempDir JUnit-managed temporary directory for the JFR dump file.
   */
  @Test
  void rejectPath_crossedMarket_emitsExactlyOneRejectedEventPerReject(@TempDir final Path tempDir)
      throws IOException {
    final var fake = new FakeBroadcastPublisher();
    final var clock = new ControllableNanoClock(1_000_000_000L);
    final var publisher = buildPublisher(fake, clock);

    final Path jfrFile = tempDir.resolve("reject-jfr-test.jfr");

    try (final var recording = new Recording()) {
      recording.enable("trading.MarketDataTickRejected");
      recording.start();

      // Drive REJECT_COUNT crossed-market ticks. Each tick has bid == ask which triggers
      // RejectReason.CROSSED (bid >= ask). Advance clock slightly between calls to ensure the
      // publisher's rate-limit log does not suppress subsequent rejects.
      for (int i = 0; i < REJECT_COUNT; i++) {
        // bid == ask => CROSSED reject
        publisher.onTick(EURUSD, ASK, ASK, SIZE, SIZE, INGRESS + i);
        clock.advanceMillis(2_000L); // 2 s exceeds the 1 s rate-limit window
      }

      recording.stop();
      recording.dump(jfrFile);
    }

    final long rejectedCount = countEvents(jfrFile, "trading.MarketDataTickRejected");
    assertEquals(
        REJECT_COUNT,
        rejectedCount,
        "Expected exactly "
            + REJECT_COUNT
            + " trading.MarketDataTickRejected events (one per reject, @Threshold 0 ms); "
            + "got "
            + rejectedCount
            + " — check that MarketDataTickRejected.shouldCommit() guard and commit() are "
            + "wired in MarketDataPublisher.dropWithSymbol()");
  }

  /**
   * Verifies that the recorded {@code trading.MarketDataTickRejected} events carry the correct
   * {@code reasonOrdinal} for a {@link RejectReason#CROSSED} rejection and a non-null {@code
   * symbol} field corresponding to the rejected tick's symbol.
   *
   * @param tempDir JUnit-managed temporary directory for the JFR dump file.
   */
  @Test
  void rejectPath_crossedMarket_recordedEventCarriesCorrectReasonOrdinalAndSymbol(
      @TempDir final Path tempDir) throws IOException {
    final var fake = new FakeBroadcastPublisher();
    final var clock = new ControllableNanoClock(1_000_000_000L);
    final var publisher = buildPublisher(fake, clock);

    final Path jfrFile = tempDir.resolve("reject-fields-jfr-test.jfr");

    try (final var recording = new Recording()) {
      recording.enable("trading.MarketDataTickRejected");
      recording.start();

      // One crossed-market reject for EURUSD
      publisher.onTick(EURUSD, ASK, ASK, SIZE, SIZE, INGRESS);

      recording.stop();
      recording.dump(jfrFile);
    }

    final List<RecordedEvent> events = RecordingFile.readAllEvents(jfrFile);

    final var rejectEvents =
        events.stream()
            .filter(e -> "trading.MarketDataTickRejected".equals(e.getEventType().getName()))
            .toList();

    assertEquals(1, rejectEvents.size(), "Expected exactly 1 reject event");

    final RecordedEvent event = rejectEvents.get(0);
    assertEquals(
        RejectReason.CROSSED.ordinal(),
        event.getInt("reasonOrdinal"),
        "reasonOrdinal must match RejectReason.CROSSED.ordinal()");

    final String symbol = event.getString("symbol");
    assertTrue(
        symbol != null && !symbol.isEmpty(),
        "symbol field must be non-null and non-empty; got: " + symbol);
    assertTrue(
        symbol.startsWith("EURUSD"), "symbol field must start with 'EURUSD'; got: " + symbol);
  }
}
