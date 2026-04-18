package com.trading.engine.e2e;

import static java.util.Collections.singletonList;

import com.trading.engine.fix.OrdType;
import com.trading.engine.fix.Side;
import com.trading.engine.fix.TimeInForce;
import com.trading.engine.fix.builder.NewOrderSingleEncoder;
import com.trading.engine.fix.decoder.ExecutionReportDecoder;
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
 * Standalone E2E FIX test client — Artio initiator that connects to the trading engine's FIX
 * gateway, sends a NewOrderSingle, and validates the ExecutionReport response.
 *
 * <p><b>Usage:</b> {@code java ... E2EFixTestClient --host localhost --port 19880}
 *
 * <p><b>Exit codes:</b>
 *
 * <ul>
 *   <li>0 — PASS (all scenarios passed)
 *   <li>1 — assertion failure (response didn't match expectations)
 *   <li>2 — connection failure (couldn't reach gateway or session didn't reach ACTIVE)
 *   <li>3 — timeout (no response within deadline)
 *   <li>4 — unexpected exception
 * </ul>
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
   * Main entry point. Parses CLI args, connects to gateway, runs test scenarios.
   *
   * @param args CLI arguments: {@code --host <host> --port <port> --sender-comp-id <id>
   *     --target-comp-id <id>}
   */
  public static void main(final String[] args) {
    String host = "localhost";
    int port = 19880;
    String senderCompId = "CLIENT1";
    String targetCompId = "TRADING";

    for (int i = 0; i < args.length - 1; i += 2) {
      switch (args[i]) {
        case "--host" -> host = args[i + 1];
        case "--port" -> port = Integer.parseInt(args[i + 1]);
        case "--sender-comp-id" -> senderCompId = args[i + 1];
        case "--target-comp-id" -> targetCompId = args[i + 1];
        default -> LOG.warn("Unknown CLI arg: {}", args[i]);
      }
    }

    LOG.info(
        "E2E FIX Test Client: host={} port={} senderCompId={} targetCompId={}",
        host,
        port,
        senderCompId,
        targetCompId);

    int exitCode = EXIT_EXCEPTION;
    try {
      exitCode = run(host, port, senderCompId, targetCompId);
    } catch (final Exception e) {
      LOG.error("E2E FAIL: unexpected exception", e);
    }

    LogManager.shutdown();
    System.exit(exitCode);
  }

  private static int run(
      final String host, final int port, final String senderCompId, final String targetCompId)
      throws Exception {

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

      // 7. Run test scenario: NewOrderSingle happy path
      int result = runNewOrderSingleScenario(session, library, messageQueue);

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
  // Test scenario
  // ===========================================================================

  /**
   * Scenario: send a NewOrderSingle for EURUSD on account ACME, validate ExecutionReport response.
   *
   * @return exit code (0=pass, 1=assertion, 3=timeout)
   */
  private static int runNewOrderSingleScenario(
      final Session session,
      final FixLibrary library,
      final OneToOneConcurrentArrayQueue<CapturedMessage> messageQueue) {

    final var clOrdId = "E2E-" + NANO_CLOCK.nanoTime();

    // Encode NewOrderSingle
    final var nos = new NewOrderSingleEncoder();
    nos.clOrdID(clOrdId);
    nos.instrument().symbol("EURUSD");
    nos.side(Side.BUY);
    nos.ordType(OrdType.LIMIT);
    nos.price(105, 2); // 1.05
    nos.orderQtyData().orderQty(1, 0); // 1.0
    nos.account("ACME");
    nos.currency("USD");
    nos.timeInForce(TimeInForce.DAY);

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
      LOG.error("E2E FAIL: trySend returned {} after retries", sendResult);
      return EXIT_ASSERTION;
    }
    LOG.info("Sent NewOrderSingle: ClOrdID={}", clOrdId);

    // Poll for ExecutionReport (35=8) response — filter by message type
    long responseDeadlineNs =
        NANO_CLOCK.nanoTime() + TimeUnit.MILLISECONDS.toNanos(RESPONSE_TIMEOUT_MS);

    while (NANO_CLOCK.nanoTime() < responseDeadlineNs) {
      library.poll(LIBRARY_POLL_LIMIT);

      final var msg = messageQueue.poll();
      if (msg != null) {
        // Filter: only process ExecutionReport (35=8, MESSAGE_TYPE=56)
        if (msg.messageType() != ExecutionReportDecoder.MESSAGE_TYPE) {
          LOG.info("Ignoring non-ER message: msgType={}", msg.messageType());
          continue;
        }
        long rttMs = TimeUnit.NANOSECONDS.toMillis(NANO_CLOCK.nanoTime() - nosStartNs);
        return validateExecutionReport(msg, clOrdId, rttMs);
      }
      Thread.onSpinWait();
    }

    LOG.error("E2E FAIL: no response within {}ms", RESPONSE_TIMEOUT_MS);
    return EXIT_TIMEOUT;
  }

  /**
   * Validates the received FIX ExecutionReport against expected values for a NewOrderSingle
   * acknowledgement. Checks every field that proves the full pipeline (FIX→SBE→Raft→Event→FIX)
   * preserved data correctly.
   */
  private static int validateExecutionReport(
      final CapturedMessage msg, final String expectedClOrdId, final long rttMs) {

    final var buffer = new MutableAsciiBuffer(msg.data());
    final var decoder = new ExecutionReportDecoder();
    decoder.decode(buffer, 0, msg.data().length);

    int failures = 0;

    // --- Identity fields ---

    // ExecType (tag 150) — '0' = New
    char execType = decoder.execType();
    if (execType != '0') {
      LOG.error(
          "ExecType='{}' (expected '0' New). Full message: {}",
          execType,
          new String(msg.data(), StandardCharsets.US_ASCII));
      failures++;
    }

    // OrdStatus (tag 39) — '0' = New
    char ordStatus = decoder.ordStatus();
    if (ordStatus != '0') {
      LOG.error("OrdStatus='{}' (expected '0' New)", ordStatus);
      failures++;
    }

    // ClOrdID (tag 11) — must echo back what we sent
    final var clOrdId = trimChars(decoder.clOrdID(), decoder.clOrdIDLength());
    if (!expectedClOrdId.equals(clOrdId)) {
      LOG.error("ClOrdID='{}' (expected '{}')", clOrdId, expectedClOrdId);
      failures++;
    }

    // OrderID (tag 37) — must be present and non-empty (cluster IdGenerator assigned)
    final var orderId = trimChars(decoder.orderID(), decoder.orderIDLength());
    if (orderId.isEmpty()) {
      LOG.error("OrderID is empty");
      failures++;
    }

    // ExecID (tag 17) — must be present and non-empty (cluster ExecIdGenerator assigned)
    final var execId = trimChars(decoder.execID(), decoder.execIDLength());
    if (execId.isEmpty()) {
      LOG.error("ExecID is empty");
      failures++;
    }

    // --- Instrument fields (prove SBE→FIX translation preserved data) ---

    // Symbol (tag 55) — must echo "EURUSD"
    final var symbol = trimChars(decoder.symbol(), decoder.symbolLength());
    if (!"EURUSD".equals(symbol)) {
      LOG.error("Symbol='{}' (expected 'EURUSD')", symbol);
      failures++;
    }

    // Side (tag 54) — '1' = Buy
    char side = decoder.side();
    if (side != '1') {
      LOG.error("Side='{}' (expected '1' Buy)", side);
      failures++;
    }

    // --- Account / Currency ---

    // Account (tag 1) — must echo "ACME"
    final var account = trimChars(decoder.account(), decoder.accountLength());
    if (!"ACME".equals(account)) {
      LOG.error("Account='{}' (expected 'ACME')", account);
      failures++;
    }

    // Currency (tag 15) — must echo "USD"
    final var currency = trimChars(decoder.currency(), decoder.currencyLength());
    if (!"USD".equals(currency)) {
      LOG.error("Currency='{}' (expected 'USD')", currency);
      failures++;
    }

    // --- Quantity fields (prove fixed-point conversion round-tripped) ---

    // LeavesQty (tag 151) — should equal OrderQty (1.0) since no fills
    final var leavesQty = decoder.leavesQty();
    if (leavesQty.value() != 1 || leavesQty.scale() != 0) {
      LOG.error("LeavesQty={} scale={} (expected 1.0)", leavesQty.value(), leavesQty.scale());
      failures++;
    }

    // CumQty (tag 14) — should be 0 (no fills yet)
    final var cumQty = decoder.cumQty();
    if (cumQty.value() != 0) {
      LOG.error("CumQty={} (expected 0)", cumQty.value());
      failures++;
    }

    // --- Result ---

    if (failures > 0) {
      LOG.error("E2E FAIL: {} assertion failure(s)", failures);
      return EXIT_ASSERTION;
    }

    LOG.info(
        "E2E PASS: ExecType=New OrdStatus=New ClOrdID={} OrderID={} ExecID={}"
            + " Symbol={} Side=Buy Account={} Currency={} LeavesQty=1.0 CumQty=0"
            + " (round-trip: {}ms)",
        clOrdId,
        orderId,
        execId,
        symbol,
        account,
        currency,
        rttMs);
    return EXIT_PASS;
  }

  /** Extracts a trimmed String from a FIX char[] field. */
  private static String trimChars(final char[] chars, final int length) {
    if (chars == null || length <= 0) {
      return "";
    }
    return new String(chars, 0, length).trim();
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
