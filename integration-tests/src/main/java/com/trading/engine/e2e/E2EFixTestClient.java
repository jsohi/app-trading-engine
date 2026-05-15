package com.trading.engine.e2e;

import static java.util.Collections.singletonList;

import com.trading.engine.e2e.E2EFixTestClientArgs.ArgsParseException;
import com.trading.engine.e2e.E2EFixTestClientArgs.CliScenarioSpec;
import com.trading.engine.e2e.E2EFixTestClientArgs.RunMode;
import com.trading.engine.fix.Side;
import com.trading.engine.fix.builder.NewOrderSingleEncoder;
import com.trading.engine.fix.decoder.BusinessMessageRejectDecoder;
import com.trading.engine.fix.decoder.ExecutionReportDecoder;
import com.trading.engine.fix.decoder.RejectDecoder;
import com.trading.engine.messages.FixedPointScale;
import com.trading.engine.messages.clock.TradingClocks;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.fields.UtcTimestampEncoder;
import uk.co.real_logic.artio.library.AcquiringSessionExistsHandler;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import uk.co.real_logic.artio.library.OnMessageInfo;
import uk.co.real_logic.artio.library.SessionConfiguration;
import uk.co.real_logic.artio.library.SessionHandler;
import uk.co.real_logic.artio.messages.DisconnectReason;
import uk.co.real_logic.artio.messages.SessionState;
import uk.co.real_logic.artio.session.Session;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

/**
 * Standalone data-driven E2E FIX test client — Artio initiator that connects to the trading
 * engine's FIX gateway, loads test scenarios from YAML, sends NewOrderSingle messages, and
 * validates ExecutionReport responses.
 *
 * <p><b>Usage:</b> {@code java ... E2EFixTestClient --host localhost --port 19880 --data-dir
 * path/to/data}
 *
 * <p><b>Exit codes:</b>
 *
 * <ul>
 *   <li>0 — PASS (all scenarios passed)
 *   <li>1 — assertion failure (response didn't match expectations)
 *   <li>2 — connection failure (couldn't reach gateway or session disconnected)
 *   <li>3 — timeout (no response within deadline)
 *   <li>4 — unexpected exception
 * </ul>
 *
 * <p>When multiple scenarios fail with different exit codes, the highest-severity (numerically
 * largest) code is returned. The per-scenario summary table logged to stdout provides full
 * individual PASS/FAIL detail for CI triage.
 *
 * <p><b>Threading.</b> Single-threaded — runs on the main thread. Not designed for concurrent use.
 *
 * <p><b>Allocation.</b> Not zero-allocation — this is a test harness, not a hot-path component.
 * Uses Agrona {@link OneToOneConcurrentArrayQueue} for message capture (lock-free, bounded).
 */
public final class E2EFixTestClient {

  private static final Logger LOG = LogManager.getLogger(E2EFixTestClient.class);

  /** Exit code: all scenarios passed. */
  private static final int EXIT_PASS = 0;

  /** Exit code: assertion failure — response didn't match expectations. */
  private static final int EXIT_ASSERTION = 1;

  /** Exit code: connection failure — session didn't reach ACTIVE. */
  private static final int EXIT_CONNECTION = 2;

  /** Exit code: timeout — no response within deadline. */
  private static final int EXIT_TIMEOUT = 3;

  /** Exit code: unexpected exception. */
  private static final int EXIT_EXCEPTION = 4;

  private static final long SESSION_TIMEOUT_MS = 30_000;
  private static final long RESPONSE_TIMEOUT_MS = 30_000;
  private static final int LIBRARY_POLL_LIMIT = 10;
  private static final int MESSAGE_QUEUE_CAPACITY = 64;

  /** Monotonic clock for elapsed-time measurement and deadlines. */
  private static final NanoClock NANO_CLOCK = TradingClocks.nanoClock();

  /** Epoch-nanosecond clock for FIX TransactTime timestamps. */
  private static final EpochNanoClock EPOCH_CLOCK = TradingClocks.epochNanoClock();

  private E2EFixTestClient() {}

  /**
   * Main entry point. Parses CLI args via {@link E2EFixTestClientArgs}, connects to gateway, then
   * runs either the YAML-driven scenario suite or a single CLI-specified order (per {@link
   * RunMode}).
   */
  public static void main(final String[] args) {
    final E2EFixTestClientArgs parsed;
    try {
      parsed = E2EFixTestClientArgs.parse(args);
    } catch (final ArgsParseException e) {
      LOG.error("E2E FAIL: {}", e.getMessage());
      LogManager.shutdown();
      System.exit(EXIT_EXCEPTION);
      return; // unreachable, satisfies the compiler's definite-assignment analysis
    }

    LOG.info(
        "E2E FIX Test Client: host={} port={} senderCompId={} targetCompId={} mode={}",
        parsed.host(),
        parsed.port(),
        parsed.senderCompId(),
        parsed.targetCompId(),
        parsed.runMode());

    int exitCode = EXIT_EXCEPTION;
    try {
      exitCode = run(parsed);
    } catch (final Exception e) {
      LOG.error("E2E FAIL: unexpected exception", e);
    }

    LogManager.shutdown();
    System.exit(exitCode);
  }

  private static int run(final E2EFixTestClientArgs parsed) throws Exception {
    final var host = parsed.host();
    final var port = parsed.port();
    final var senderCompId = parsed.senderCompId();
    final var targetCompId = parsed.targetCompId();

    final var driverDir = Files.createTempDirectory("e2e-fix-client");
    final var archiveDir = Files.createTempDirectory("e2e-archive");
    final var fixLogDir = Files.createTempDirectory("e2e-fix-logs");

    // Resource stack — closed in reverse order via CloseHelper
    MediaDriver driver = null;
    Archive archive = null;
    FixEngine engine = null;
    FixLibrary library = null;

    try {
      // 1. Launch embedded MediaDriver (SHARED, temp dir, auto-cleaned on exit)
      driver = launchMediaDriver(driverDir);
      LOG.info("MediaDriver launched: {}", driverDir);

      // 2. Launch embedded archive — Artio requires AeronArchive for FIX message log persistence
      archive = launchArchive(driverDir, archiveDir);
      LOG.info("Archive launched: {}", archiveDir);

      // 3. Launch Artio FixEngine (initiator mode — no bindTo, no server socket)
      engine = launchFixEngine(driverDir, fixLogDir);
      LOG.info("FixEngine launched (initiator mode)");

      // 4. Connect FixLibrary
      final var messageQueue =
          new OneToOneConcurrentArrayQueue<CapturedMessage>(MESSAGE_QUEUE_CAPACITY);
      library = connectLibrary(driverDir, messageQueue);
      LOG.info("FixLibrary connected");

      // 5. Wait for library ↔ engine handshake
      long libDeadlineNs = NANO_CLOCK.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (!library.isConnected()) {
        library.poll(LIBRARY_POLL_LIMIT);
        if (NANO_CLOCK.nanoTime() > libDeadlineNs) {
          LOG.error("E2E FAIL: FixLibrary did not connect to engine within 10s");
          return EXIT_CONNECTION;
        }
        Thread.onSpinWait();
      }
      LOG.info("FixLibrary connected to engine");

      // 6. Initiate FIX session to gateway
      long logonStartNs = NANO_CLOCK.nanoTime();
      final var sessionConfig =
          SessionConfiguration.builder()
              .address(host, port)
              .senderCompId(senderCompId)
              .targetCompId(targetCompId)
              .resetSeqNum(true)
              .build();

      final var reply = library.initiate(sessionConfig);

      long sessionDeadlineNs =
          NANO_CLOCK.nanoTime() + TimeUnit.MILLISECONDS.toNanos(SESSION_TIMEOUT_MS);
      while (reply.isExecuting()) {
        library.poll(LIBRARY_POLL_LIMIT);
        if (NANO_CLOCK.nanoTime() > sessionDeadlineNs) {
          LOG.error("E2E FAIL: session initiation timed out after {}ms", SESSION_TIMEOUT_MS);
          return EXIT_CONNECTION;
        }
        Thread.onSpinWait();
      }

      if (reply.hasErrored()) {
        LOG.error("E2E FAIL: session initiation error: {}", reply.error());
        return EXIT_CONNECTION;
      }
      if (reply.hasTimedOut()) {
        LOG.error("E2E FAIL: session initiation timed out (Artio reply timeout)");
        return EXIT_CONNECTION;
      }

      final var session = reply.resultIfPresent();
      if (session == null || session.state() != SessionState.ACTIVE) {
        LOG.error(
            "E2E FAIL: session not ACTIVE, state={}", session != null ? session.state() : "null");
        return EXIT_CONNECTION;
      }

      long logonMs = TimeUnit.NANOSECONDS.toMillis(NANO_CLOCK.nanoTime() - logonStartNs);
      LOG.info("FIX session ACTIVE (logon: {}ms)", logonMs);

      // 7. Run mode-specific scenario(s)
      final int result;
      if (parsed.runMode() == RunMode.CLI) {
        result = runCliScenario(session, library, messageQueue, parsed.cliScenario().orElseThrow());
      } else {
        final var dataDir = Path.of(parsed.dataDir().orElseThrow()).toAbsolutePath();
        result = runAllScenarios(session, library, messageQueue, dataDir);
      }

      // 8. Clean shutdown: logout then close
      session.logoutAndDisconnect();
      long logoutDeadlineNs = NANO_CLOCK.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (session.state() != SessionState.DISCONNECTED
          && NANO_CLOCK.nanoTime() < logoutDeadlineNs) {
        library.poll(LIBRARY_POLL_LIMIT);
        Thread.onSpinWait();
      }

      return result;
    } finally {
      // Reverse-order cleanup — library → engine → archive → driver
      CloseHelper.quietCloseAll(library, engine, archive, driver);
      // Clean up temp directories (driver dir is auto-cleaned via dirDeleteOnShutdown)
      deleteRecursively(fixLogDir);
      deleteRecursively(archiveDir);
    }
  }

  // ===========================================================================
  // Data-driven scenario runner
  // ===========================================================================

  /**
   * Loads scenarios from YAML and runs them sequentially over the same FIX session.
   *
   * <p>Exit code semantics: {@code Math.max(worstExitCode, result)} propagates the highest-severity
   * exit code. If mixed failure types occur (e.g., one timeout + one assertion), only the most
   * severe code reaches the caller. The per-scenario summary table logged to stdout compensates —
   * it shows every scenario's individual PASS/FAIL for full triage context.
   *
   * @param session the active FIX session
   * @param library the Artio library (polled for message delivery)
   * @param messageQueue queue of captured inbound FIX messages
   * @param dataDir path to the E2E data directory containing e2e-scenarios.yaml
   * @return worst (highest) exit code across all scenarios
   */
  private static int runAllScenarios(
      final Session session,
      final FixLibrary library,
      final OneToOneConcurrentArrayQueue<CapturedMessage> messageQueue,
      final Path dataDir) {

    final List<NosScenario> scenarios =
        E2EScenarioLoader.load(dataDir.resolve("e2e-scenarios.yaml"));

    int passed = 0;
    int failed = 0;
    int worstExitCode = EXIT_PASS;
    final var results = new String[scenarios.size()];

    for (int i = 0; i < scenarios.size(); i++) {
      final var scenario = scenarios.get(i);
      final int total = scenarios.size();
      LOG.info("[{}/{}] Running scenario: {}", i + 1, total, scenario.name());

      // Best-effort drain of stale messages from previous scenario.
      // This is an optimization to reduce log noise — ClOrdID matching in the poll loop
      // is the actual correctness mechanism for scenario isolation.
      while (messageQueue.poll() != null) {
        // discard
      }

      final int result = runNosScenario(session, library, messageQueue, scenario, i, total);

      if (result == EXIT_PASS) {
        passed++;
        results[i] = "[PASS] " + scenario.name();
      } else {
        failed++;
        results[i] = "[FAIL] " + scenario.name();
        worstExitCode = Math.max(worstExitCode, result);
      }

      // Connection failure (session disconnect) — remaining scenarios cannot run.
      // Session-level Reject (35=3) does NOT trigger this — per FIX 4.4, the session
      // remains active after a Reject and subsequent scenarios can still execute.
      if (result == EXIT_CONNECTION) {
        LOG.error("FIX session disconnected — aborting remaining scenarios");
        for (int j = i + 1; j < scenarios.size(); j++) {
          results[j] = "[SKIP] " + scenarios.get(j).name();
        }
        break;
      }
    }

    // Summary table for CI triage
    LOG.info("E2E RESULTS: {}/{} passed", passed, passed + failed);
    for (final var line : results) {
      if (line != null) {
        LOG.info("  {}", line);
      }
    }
    return worstExitCode;
  }

  // ===========================================================================
  // CLI mode — single or matched-pair scenario, ClOrdID supplied by caller
  // ===========================================================================

  /**
   * Executes one or two CLI-specified NOS messages, reusing the YAML-mode {@link
   * #runNosScenarioWithClOrdId} helper for the actual encode/send/validate loop.
   *
   * <p>For {@link CliScenarioSpec.ScenarioKind#SINGLE}: one NOS, ClOrdID exactly as supplied.
   *
   * <p>For {@link CliScenarioSpec.ScenarioKind#MATCH}: two NOS — buy then sell — with ClOrdIDs
   * suffixed {@code -buy} and {@code -sell}. Both legs share symbol/qty/price/account/currency
   * (only side flips). Used by the full-stack-e2e Playwright spec 4 to produce a fill that lands a
   * non-zero row in PositionsBlotter.
   *
   * @return worst (highest) exit code across the legs
   */
  private static int runCliScenario(
      final Session session,
      final FixLibrary library,
      final OneToOneConcurrentArrayQueue<CapturedMessage> messageQueue,
      final CliScenarioSpec spec) {

    return switch (spec.kind()) {
      case SINGLE -> runCliLeg(session, library, messageQueue, spec, spec.clOrdId(), spec.side());
      case MATCH -> {
        final var ids = spec.matchClOrdIds();
        final int buyResult = runCliLeg(session, library, messageQueue, spec, ids[0], Side.BUY);
        if (buyResult != EXIT_PASS) {
          yield buyResult;
        }
        final int sellResult = runCliLeg(session, library, messageQueue, spec, ids[1], Side.SELL);
        yield Math.max(buyResult, sellResult);
      }
    };
  }

  private static int runCliLeg(
      final Session session,
      final FixLibrary library,
      final OneToOneConcurrentArrayQueue<CapturedMessage> messageQueue,
      final CliScenarioSpec spec,
      final String clOrdId,
      final Side side) {

    // Best-effort drain of stale messages (matches YAML-mode discipline).
    while (messageQueue.poll() != null) {
      // discard
    }

    // Build a NosScenario at runtime from the CLI spec — reuses the YAML-mode validators.
    // Unscaled (value, scale) fields are derived from the pre-computed fixed-point longs by
    // dividing back to (long, SCALE_DIGITS) form. The encode path in runNosScenarioWithClOrdId
    // calls priceValue/priceScale/qtyValue/qtyScale, which is what NewOrderSingleEncoder needs.
    final long priceFp = spec.priceFixedPoint();
    final long qtyFp = spec.qtyFixedPoint();
    final var scenario =
        new NosScenario(
            "cli-" + spec.kind() + "-" + side + "-" + clOrdId,
            NosScenario.ScenarioType.NEW_ORDER_SINGLE,
            NosScenario.ExpectedOutcome.NEW,
            spec.account(),
            spec.symbol(),
            spec.currency(),
            side,
            spec.ordType(),
            spec.timeInForce(),
            priceFp, // priceValue (already scaled — see priceScale=SCALE_DIGITS below)
            FixedPointScale.SCALE_DIGITS,
            spec.hasPrice(),
            qtyFp,
            FixedPointScale.SCALE_DIGITS,
            priceFp,
            qtyFp,
            null);

    // Reuse the YAML-mode runner with the externally-supplied ClOrdID.
    return runNosScenarioWithClOrdId(session, library, messageQueue, scenario, clOrdId, 0, 1);
  }

  // ===========================================================================
  // NOS scenario execution
  // ===========================================================================

  /**
   * Executes a single NewOrderSingle scenario: encodes and sends the NOS, polls for the
   * ExecutionReport, and validates the response against the scenario's expected outcome.
   *
   * @param session the active FIX session
   * @param library the Artio library (polled for message delivery)
   * @param messageQueue queue of captured inbound FIX messages
   * @param scenario the NOS scenario to execute
   * @param index zero-based scenario index (for logging)
   * @param total total number of scenarios (for logging)
   * @return exit code (0=pass, 1=assertion, 2=connection, 3=timeout)
   */
  private static int runNosScenario(
      final Session session,
      final FixLibrary library,
      final OneToOneConcurrentArrayQueue<CapturedMessage> messageQueue,
      final NosScenario scenario,
      final int index,
      final int total) {

    // ClOrdID: truncate nanoTime to 9 digits — stays within FIX 4.4's 20-char limit.
    // Math.abs() handles negative nanoTime() values (allowed by contract).
    final var clOrdId = "E2E-" + index + "-" + Math.abs(NANO_CLOCK.nanoTime() % 1_000_000_000L);
    return runNosScenarioWithClOrdId(
        session, library, messageQueue, scenario, clOrdId, index, total);
  }

  /**
   * Variant of {@link #runNosScenario} that takes an externally-supplied ClOrdID. Used by CLI mode
   * where the ClOrdID is the deterministic grep key the Playwright spec expects to see in the
   * OrderBlotter row.
   */
  private static int runNosScenarioWithClOrdId(
      final Session session,
      final FixLibrary library,
      final OneToOneConcurrentArrayQueue<CapturedMessage> messageQueue,
      final NosScenario scenario,
      final String clOrdId,
      final int index,
      final int total) {

    // Encode NewOrderSingle
    final var nos = new NewOrderSingleEncoder();
    nos.clOrdID(clOrdId);
    nos.instrument().symbol(scenario.symbol());
    nos.side(scenario.side());
    nos.ordType(scenario.ordType());
    if (scenario.hasPrice()) {
      nos.price(scenario.priceValue(), scenario.priceScale());
    }
    nos.orderQtyData().orderQty(scenario.qtyValue(), scenario.qtyScale());
    nos.account(scenario.accountCode());
    nos.currency(scenario.currency());
    nos.timeInForce(scenario.timeInForce());

    // TransactTime — required FIX field. Use project clock infrastructure.
    final var tsEncoder = new UtcTimestampEncoder();
    long epochNanos = EPOCH_CLOCK.nanoTime();
    long epochMillis = TimeUnit.NANOSECONDS.toMillis(epochNanos);
    int tsLen = tsEncoder.encode(epochMillis);
    nos.transactTime(tsEncoder.buffer(), tsLen);

    // Send NOS — retry on transient backpressure under a bounded deadline
    long nosStartNs = NANO_CLOCK.nanoTime();
    long sendDeadlineNs = nosStartNs + TimeUnit.SECONDS.toNanos(5);
    long sendResult;
    do {
      sendResult = session.trySend(nos);
      if (sendResult >= 0) {
        break;
      }
      library.poll(LIBRARY_POLL_LIMIT);
      Thread.onSpinWait();
    } while (NANO_CLOCK.nanoTime() < sendDeadlineNs);

    if (sendResult < 0) {
      LOG.error(
          "[{}/{}] {} — FAIL: trySend returned {} after retries",
          index + 1,
          total,
          scenario.name(),
          sendResult);
      return EXIT_ASSERTION;
    }
    LOG.info("[{}/{}] Sent NOS: ClOrdID={}", index + 1, total, clOrdId);

    // Poll for ExecutionReport (35=8) response — filter by message type, match ClOrdID
    long responseDeadlineNs =
        NANO_CLOCK.nanoTime() + TimeUnit.MILLISECONDS.toNanos(RESPONSE_TIMEOUT_MS);

    while (NANO_CLOCK.nanoTime() < responseDeadlineNs) {
      library.poll(LIBRARY_POLL_LIMIT);

      final var msg = messageQueue.poll();
      if (msg != null) {
        // Fail-fast on session-level Reject (35=3).
        // Per FIX 4.4, a session Reject means the NOS was malformed but the session
        // remains active — this is an assertion failure, not a connection failure.
        if (msg.messageType() == RejectDecoder.MESSAGE_TYPE) {
          LOG.error(
              "[{}/{}] {} — FAIL: Received session Reject (35=3): {}",
              index + 1,
              total,
              scenario.name(),
              new String(msg.data(), StandardCharsets.US_ASCII));
          return EXIT_ASSERTION;
        }

        // Fail-fast on BusinessMessageReject (35=j)
        if (msg.messageType() == BusinessMessageRejectDecoder.MESSAGE_TYPE) {
          LOG.error(
              "[{}/{}] {} — FAIL: Received BusinessMessageReject (35=j): {}",
              index + 1,
              total,
              scenario.name(),
              new String(msg.data(), StandardCharsets.US_ASCII));
          return EXIT_ASSERTION;
        }

        // Filter: only process ExecutionReport (35=8)
        if (msg.messageType() != ExecutionReportDecoder.MESSAGE_TYPE) {
          LOG.info(
              "[{}/{}] Ignoring non-ER message: msgType={}", index + 1, total, msg.messageType());
          continue;
        }

        // Decode ER and match ClOrdID — discard stale responses from previous scenarios
        final var buffer = new MutableAsciiBuffer(msg.data());
        final var decoder = new ExecutionReportDecoder();
        decoder.decode(buffer, 0, msg.data().length);

        final var rxClOrdId = trimChars(decoder.clOrdID(), decoder.clOrdIDLength());
        if (!clOrdId.equals(rxClOrdId)) {
          LOG.warn(
              "[{}/{}] Discarding stale ER with ClOrdID={} (expected {})",
              index + 1,
              total,
              rxClOrdId,
              clOrdId);
          continue;
        }

        // Validate based on expected outcome
        long rttMs = TimeUnit.NANOSECONDS.toMillis(NANO_CLOCK.nanoTime() - nosStartNs);
        return switch (scenario.expectedOutcome()) {
          case NEW -> validateNewAck(decoder, scenario, index, total, rttMs);
          case REJECTED -> validateRejected(decoder, scenario, index, total, rttMs);
        };
      }

      // Session disconnect detection — fail fast instead of spinning to timeout
      if (session.state() != SessionState.ACTIVE) {
        LOG.error(
            "[{}/{}] {} — FAIL: FIX session disconnected (state={})",
            index + 1,
            total,
            scenario.name(),
            session.state());
        return EXIT_CONNECTION;
      }

      Thread.onSpinWait();
    }

    LOG.error(
        "[{}/{}] {} — FAIL: no response within {}ms",
        index + 1,
        total,
        scenario.name(),
        RESPONSE_TIMEOUT_MS);
    return EXIT_TIMEOUT;
  }

  // ===========================================================================
  // ER validation — Happy Path (ExpectedOutcome.NEW)
  // ===========================================================================

  /**
   * Validates an ExecutionReport for a successful New acknowledgement. Checks every field that
   * proves the full pipeline (FIX→SBE→Raft→Event→FIX) preserved data correctly.
   */
  private static int validateNewAck(
      final ExecutionReportDecoder decoder,
      final NosScenario scenario,
      final int index,
      final int total,
      final long rttMs) {

    int failures = 0;
    final String prefix = "[" + (index + 1) + "/" + total + "] " + scenario.name();

    // ExecType (tag 150) — '0' = New
    char execType = decoder.execType();
    if (execType != '0') {
      LOG.error("{} — FAIL: ExecType (tag 150): expected='0' (New), actual='{}'", prefix, execType);
      failures++;
    }

    // OrdStatus (tag 39) — '0' = New
    char ordStatus = decoder.ordStatus();
    if (ordStatus != '0') {
      LOG.error(
          "{} — FAIL: OrdStatus (tag 39): expected='0' (New), actual='{}'", prefix, ordStatus);
      failures++;
    }

    // OrderID (tag 37) — must be present and non-empty
    final var orderId = trimChars(decoder.orderID(), decoder.orderIDLength());
    if (orderId.isEmpty()) {
      LOG.error("{} — FAIL: OrderID (tag 37) is empty", prefix);
      failures++;
    }

    // ExecID (tag 17) — must be present and non-empty
    final var execId = trimChars(decoder.execID(), decoder.execIDLength());
    if (execId.isEmpty()) {
      LOG.error("{} — FAIL: ExecID (tag 17) is empty", prefix);
      failures++;
    }

    // Symbol (tag 55)
    final var symbol = trimChars(decoder.symbol(), decoder.symbolLength());
    if (!scenario.symbol().equals(symbol)) {
      LOG.error(
          "{} — FAIL: Symbol (tag 55): expected='{}', actual='{}'",
          prefix,
          scenario.symbol(),
          symbol);
      failures++;
    }

    // Side (tag 54)
    char side = decoder.side();
    if (side != scenario.side().representation()) {
      LOG.error(
          "{} — FAIL: Side (tag 54): expected='{}', actual='{}'",
          prefix,
          scenario.side().representation(),
          side);
      failures++;
    }

    // Account (tag 1)
    final var account = trimChars(decoder.account(), decoder.accountLength());
    if (!scenario.accountCode().equals(account)) {
      LOG.error(
          "{} — FAIL: Account (tag 1): expected='{}', actual='{}'",
          prefix,
          scenario.accountCode(),
          account);
      failures++;
    }

    // Currency (tag 15)
    final var currency = trimChars(decoder.currency(), decoder.currencyLength());
    if (!scenario.currency().equals(currency)) {
      LOG.error(
          "{} — FAIL: Currency (tag 15): expected='{}', actual='{}'",
          prefix,
          scenario.currency(),
          currency);
      failures++;
    }

    // OrdType (tag 40)
    if (decoder.hasOrdType()) {
      char ordType = decoder.ordType();
      if (ordType != scenario.ordType().representation()) {
        LOG.error(
            "{} — FAIL: OrdType (tag 40): expected='{}', actual='{}'",
            prefix,
            scenario.ordType().representation(),
            ordType);
        failures++;
      }
    }

    // TimeInForce (tag 59)
    if (decoder.hasTimeInForce()) {
      char tif = decoder.timeInForce();
      if (tif != scenario.timeInForce().representation()) {
        LOG.error(
            "{} — FAIL: TimeInForce (tag 59): expected='{}', actual='{}'",
            prefix,
            scenario.timeInForce().representation(),
            tif);
        failures++;
      }
    }

    // OrderQty (tag 38) — echo-back validation
    if (decoder.hasOrderQty()) {
      long rxQtyFp = E2EScenarioLoader.toFixedPoint(decoder.orderQty());
      if (rxQtyFp != scenario.qtyFixedPoint()) {
        LOG.error(
            "{} — FAIL: OrderQty (tag 38): expected={}, actual={}",
            prefix,
            scenario.qtyFixedPoint(),
            rxQtyFp);
        failures++;
      }
    }

    // Price (tag 44) — only for orders that have a price (Limit)
    if (scenario.hasPrice()) {
      if (!decoder.hasPrice()) {
        LOG.error("{} — FAIL: Price (tag 44) expected but absent", prefix);
        failures++;
      } else {
        long rxPriceFp = E2EScenarioLoader.toFixedPoint(decoder.price());
        if (rxPriceFp != scenario.priceFixedPoint()) {
          LOG.error(
              "{} — FAIL: Price (tag 44): expected={}, actual={}",
              prefix,
              scenario.priceFixedPoint(),
              rxPriceFp);
          failures++;
        }
      }
    }

    // LeavesQty (tag 151) — should equal OrderQty since no fills
    long rxLeavesQtyFp = E2EScenarioLoader.toFixedPoint(decoder.leavesQty());
    if (rxLeavesQtyFp != scenario.qtyFixedPoint()) {
      LOG.error(
          "{} — FAIL: LeavesQty (tag 151): expected={}, actual={}",
          prefix,
          scenario.qtyFixedPoint(),
          rxLeavesQtyFp);
      failures++;
    }

    // CumQty (tag 14) — should be 0 (no fills yet)
    if (decoder.cumQty().value() != 0) {
      LOG.error(
          "{} — FAIL: CumQty (tag 14): expected=0, actual={}", prefix, decoder.cumQty().value());
      failures++;
    }

    // AvgPx (tag 6) — should be 0 (no fills)
    if (decoder.avgPx().value() != 0) {
      LOG.error("{} — FAIL: AvgPx (tag 6): expected=0, actual={}", prefix, decoder.avgPx().value());
      failures++;
    }

    // TransactTime (tag 60) — presence check
    if (!decoder.hasTransactTime()) {
      LOG.error("{} — FAIL: TransactTime (tag 60) is absent", prefix);
      failures++;
    }

    if (failures > 0) {
      LOG.error("{} — FAIL: {} assertion failure(s)", prefix, failures);
      return EXIT_ASSERTION;
    }

    LOG.info("{} — PASS (round-trip: {}ms)", prefix, rttMs);
    return EXIT_PASS;
  }

  // ===========================================================================
  // ER validation — Reject Path (ExpectedOutcome.REJECTED)
  // ===========================================================================

  /**
   * Validates an ExecutionReport for an expected order rejection. Checks ExecType/OrdStatus for
   * Rejected, verifies zero qty fields, and matches the Text (tag 58) against the expected reject
   * text substring.
   */
  private static int validateRejected(
      final ExecutionReportDecoder decoder,
      final NosScenario scenario,
      final int index,
      final int total,
      final long rttMs) {

    int failures = 0;
    final String prefix = "[" + (index + 1) + "/" + total + "] " + scenario.name();

    // ExecType (tag 150) — '8' = Rejected
    char execType = decoder.execType();
    if (execType != '8') {
      LOG.error(
          "{} — FAIL: ExecType (tag 150): expected='8' (Rejected), actual='{}'", prefix, execType);
      failures++;
    }

    // OrdStatus (tag 39) — '8' = Rejected
    char ordStatus = decoder.ordStatus();
    if (ordStatus != '8') {
      LOG.error(
          "{} — FAIL: OrdStatus (tag 39): expected='8' (Rejected), actual='{}'", prefix, ordStatus);
      failures++;
    }

    // LeavesQty (tag 151) — should be 0
    if (decoder.leavesQty().value() != 0) {
      LOG.error(
          "{} — FAIL: LeavesQty (tag 151): expected=0, actual={}",
          prefix,
          decoder.leavesQty().value());
      failures++;
    }

    // CumQty (tag 14) — should be 0
    if (decoder.cumQty().value() != 0) {
      LOG.error(
          "{} — FAIL: CumQty (tag 14): expected=0, actual={}", prefix, decoder.cumQty().value());
      failures++;
    }

    // OrdRejReason (tag 103) — log for diagnostics if present
    if (decoder.hasOrdRejReason()) {
      LOG.info("{} — OrdRejReason (tag 103): {}", prefix, decoder.ordRejReason());
    }

    // Text (tag 58) — contains expectedRejectText
    if (scenario.expectedRejectText() != null) {
      if (!decoder.hasText()) {
        LOG.error(
            "{} — FAIL: Text (tag 58) is absent but expected to contain '{}'",
            prefix,
            scenario.expectedRejectText());
        failures++;
      } else {
        final var text = decoder.textAsString();
        if (!text.contains(scenario.expectedRejectText())) {
          LOG.error(
              "{} — FAIL: Text (tag 58): expected to contain '{}', actual='{}'",
              prefix,
              scenario.expectedRejectText(),
              text);
          failures++;
        }
      }
    }

    // TransactTime (tag 60) — presence check
    if (!decoder.hasTransactTime()) {
      LOG.error("{} — FAIL: TransactTime (tag 60) is absent", prefix);
      failures++;
    }

    if (failures > 0) {
      LOG.error("{} — FAIL: {} assertion failure(s)", prefix, failures);
      return EXIT_ASSERTION;
    }

    LOG.info("{} — PASS (round-trip: {}ms)", prefix, rttMs);
    return EXIT_PASS;
  }

  // ===========================================================================
  // Utilities
  // ===========================================================================

  /** Extracts a trimmed String from a FIX char[] field. */
  private static String trimChars(final char[] chars, final int length) {
    if (chars == null || length <= 0) {
      return "";
    }
    return new String(chars, 0, length).trim();
  }

  /** Best-effort recursive delete of a temp directory. Swallows exceptions. */
  @SuppressWarnings("ResultOfMethodCallIgnored")
  private static void deleteRecursively(final Path dir) {
    try {
      try (var stream = Files.walk(dir)) {
        stream.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
      }
    } catch (final Exception e) {
      LOG.warn("Failed to clean up temp dir {}: {}", dir, e.getMessage());
    }
  }

  // ===========================================================================
  // Resource factory methods
  // ===========================================================================

  private static MediaDriver launchMediaDriver(final Path driverDir) {
    final var ctx =
        new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true)
            .aeronDirectoryName(driverDir.toString())
            .threadingMode(ThreadingMode.SHARED);
    return MediaDriver.launch(ctx);
  }

  private static Archive launchArchive(final Path driverDir, final Path archiveDir) {
    final var ctx =
        new Archive.Context()
            .aeronDirectoryName(driverDir.toString())
            .archiveDir(new File(archiveDir.toString()))
            .controlChannel("aeron:udp?endpoint=localhost:0")
            .localControlChannel("aeron:ipc?term-length=64k")
            .recordingEventsEnabled(true)
            .recordingEventsChannel("aeron:ipc")
            .replicationChannel("aeron:udp?endpoint=localhost:0")
            .threadingMode(ArchiveThreadingMode.SHARED);
    return Archive.launch(ctx);
  }

  private static FixEngine launchFixEngine(final Path driverDir, final Path fixLogDir) {
    final var engineConfig =
        new EngineConfiguration()
            .libraryAeronChannel("aeron:ipc")
            .logInboundMessages(true)
            .logOutboundMessages(true)
            .logFileDir(fixLogDir.toString());
    engineConfig.aeronContext().aeronDirectoryName(driverDir.toString());
    engineConfig
        .aeronArchiveContext()
        .controlRequestChannel("aeron:ipc")
        .controlResponseChannel("aeron:ipc")
        .recordingEventsChannel("aeron:ipc")
        .aeronDirectoryName(driverDir.toString());
    return FixEngine.launch(engineConfig);
  }

  private static FixLibrary connectLibrary(
      final Path driverDir, final OneToOneConcurrentArrayQueue<CapturedMessage> messageQueue) {
    final var libConfig =
        new LibraryConfiguration()
            .sessionAcquireHandler(
                (session, acquiredInfo) -> createSessionHandler(session, messageQueue))
            .sessionExistsHandler(new AcquiringSessionExistsHandler())
            .libraryAeronChannels(singletonList("aeron:ipc"));
    libConfig.aeronContext().aeronDirectoryName(driverDir.toString());
    return FixLibrary.connect(libConfig);
  }

  // ===========================================================================
  // Session handler
  // ===========================================================================

  /**
   * Creates a {@link SessionHandler} that captures inbound messages into the queue. Defensively
   * copies the transient Artio buffer to a byte[] before queueing.
   */
  private static SessionHandler createSessionHandler(
      final Session session, final OneToOneConcurrentArrayQueue<CapturedMessage> messageQueue) {

    LOG.info("Session acquired: id={} state={}", session.id(), session.state());

    return new SessionHandler() {
      @Override
      public Action onMessage(
          final DirectBuffer buffer,
          final int offset,
          final int length,
          final int libraryId,
          final Session session,
          final int sequenceIndex,
          final long messageType,
          final long timestampInNs,
          final long position,
          final OnMessageInfo messageInfo) {

        // Defensively copy — Artio reuses the buffer after this callback returns
        final byte[] copy = new byte[length];
        buffer.getBytes(offset, copy);
        messageQueue.offer(new CapturedMessage(messageType, copy));
        return Action.CONTINUE;
      }

      @Override
      public void onTimeout(final int libraryId, final Session session) {
        LOG.warn("Session timeout: id={}", session.id());
      }

      @Override
      public void onSlowStatus(
          final int libraryId, final Session session, final boolean hasBecomeSlow) {
        LOG.warn("Session slow status: id={} slow={}", session.id(), hasBecomeSlow);
      }

      @Override
      public Action onDisconnect(
          final int libraryId, final Session session, final DisconnectReason reason) {
        LOG.warn("Session disconnected: id={} reason={}", session.id(), reason);
        return Action.CONTINUE;
      }

      @Override
      public void onSessionStart(final Session session) {
        LOG.info("Session started: id={}", session.id());
      }
    };
  }

  /**
   * Captured FIX message — defensive copy of the transient Artio buffer content plus the message
   * type for dispatch.
   */
  private record CapturedMessage(long messageType, byte[] data) {}
}
