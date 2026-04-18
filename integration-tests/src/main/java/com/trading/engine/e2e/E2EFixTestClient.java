package com.trading.engine.e2e;

import static java.util.Collections.singletonList;

import com.trading.engine.fix.OrdType;
import com.trading.engine.fix.Side;
import com.trading.engine.fix.TimeInForce;
import com.trading.engine.fix.builder.NewOrderSingleEncoder;
import com.trading.engine.fix.decoder.ExecutionReportDecoder;
import com.trading.engine.messages.clock.TradingClocks;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.NanoClock;
import uk.co.real_logic.artio.Reply;
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
 */
public final class E2EFixTestClient {

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

  /** Monotonic clock for elapsed-time measurement and deadlines. */
  private static final NanoClock NANO_CLOCK = TradingClocks.nanoClock();

  private E2EFixTestClient() {}

  /**
   * Main entry point. Parses CLI args, connects to gateway, runs test scenarios.
   *
   * @param args CLI arguments: {@code --host <host> --port <port> --sender-comp-id <id>
   *     --target-comp-id <id>}
   */
  public static void main(final String[] args) {
    String host = "localhost";
    int port = 9880;
    String senderCompId = "CLIENT1";
    String targetCompId = "TRADING";

    for (int i = 0; i < args.length - 1; i += 2) {
      switch (args[i]) {
        case "--host" -> host = args[i + 1];
        case "--port" -> port = Integer.parseInt(args[i + 1]);
        case "--sender-comp-id" -> senderCompId = args[i + 1];
        case "--target-comp-id" -> targetCompId = args[i + 1];
        default -> {
          /* ignore unknown args */
        }
      }
    }

    System.out.println(
        "E2E FIX Test Client: host="
            + host
            + " port="
            + port
            + " senderCompId="
            + senderCompId
            + " targetCompId="
            + targetCompId);

    int exitCode = EXIT_EXCEPTION;
    try {
      exitCode = run(host, port, senderCompId, targetCompId);
    } catch (final Exception e) {
      System.err.println("E2E FAIL: unexpected exception: " + e.getMessage());
      e.printStackTrace(System.err);
    }

    System.exit(exitCode);
  }

  private static int run(
      final String host, final int port, final String senderCompId, final String targetCompId)
      throws Exception {

    // 1. Launch embedded MediaDriver (SHARED, temp dir, auto-cleaned on exit)
    final Path driverDir = Files.createTempDirectory("e2e-fix-client");
    final MediaDriver.Context driverCtx =
        new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true)
            .aeronDirectoryName(driverDir.toString())
            .threadingMode(ThreadingMode.SHARED);

    try (var driver = MediaDriver.launch(driverCtx)) {
      System.out.println("MediaDriver launched: " + driverDir);

      // 2. Launch Artio FixEngine (initiator mode — no bindTo, no server socket)
      final Path fixLogDir = Files.createTempDirectory("e2e-fix-logs");
      final EngineConfiguration engineConfig =
          new EngineConfiguration()
              .libraryAeronChannel("aeron:ipc")
              .logInboundMessages(true)
              .logOutboundMessages(true)
              .logFileDir(fixLogDir.toString());
      engineConfig.aeronContext().aeronDirectoryName(driverDir.toString());

      try (var engine = FixEngine.launch(engineConfig)) {
        System.out.println("FixEngine launched (initiator mode)");

        // 3. Message queue for capturing inbound FIX messages from SessionHandler callbacks
        final LinkedBlockingQueue<CapturedMessage> messageQueue = new LinkedBlockingQueue<>();

        // 4. Connect FixLibrary
        final LibraryConfiguration libConfig =
            new LibraryConfiguration()
                .sessionAcquireHandler(
                    (session, acquiredInfo) -> createSessionHandler(session, messageQueue))
                .sessionExistsHandler(new AcquiringSessionExistsHandler())
                .libraryAeronChannels(singletonList("aeron:ipc"));
        libConfig.aeronContext().aeronDirectoryName(driverDir.toString());

        try (var library = FixLibrary.connect(libConfig)) {
          System.out.println("FixLibrary connected");

          // 5. Initiate FIX session to gateway
          final long logonStartNs = NANO_CLOCK.nanoTime();
          final SessionConfiguration sessionConfig =
              SessionConfiguration.builder()
                  .address(host, port)
                  .senderCompId(senderCompId)
                  .targetCompId(targetCompId)
                  .resetSeqNum(true)
                  .build();

          final Reply<Session> reply = library.initiate(sessionConfig);

          // Poll library until session reply completes (ACTIVE or error)
          final long sessionDeadlineNs =
              NANO_CLOCK.nanoTime() + TimeUnit.MILLISECONDS.toNanos(SESSION_TIMEOUT_MS);
          while (reply.isExecuting()) {
            library.poll(LIBRARY_POLL_LIMIT);
            if (NANO_CLOCK.nanoTime() > sessionDeadlineNs) {
              System.err.println(
                  "E2E FAIL: session initiation timed out after " + SESSION_TIMEOUT_MS + "ms");
              return EXIT_CONNECTION;
            }
            Thread.onSpinWait();
          }

          if (reply.hasErrored()) {
            System.err.println("E2E FAIL: session initiation error: " + reply.error());
            return EXIT_CONNECTION;
          }
          if (reply.hasTimedOut()) {
            System.err.println("E2E FAIL: session initiation timed out (Artio reply timeout)");
            return EXIT_CONNECTION;
          }

          final Session session = reply.resultIfPresent();
          if (session == null || session.state() != SessionState.ACTIVE) {
            System.err.println(
                "E2E FAIL: session not ACTIVE, state="
                    + (session != null ? session.state() : "null"));
            return EXIT_CONNECTION;
          }

          final long logonMs = TimeUnit.NANOSECONDS.toMillis(NANO_CLOCK.nanoTime() - logonStartNs);
          System.out.println("FIX session ACTIVE (logon: " + logonMs + "ms)");

          // 6. Run test scenario: NewOrderSingle happy path
          final int result =
              runNewOrderSingleScenario(session, library, messageQueue, senderCompId);

          // 7. Clean shutdown: logout then close
          session.logoutAndDisconnect();
          final long logoutDeadlineNs = NANO_CLOCK.nanoTime() + TimeUnit.SECONDS.toNanos(5);
          while (session.state() != SessionState.DISCONNECTED
              && NANO_CLOCK.nanoTime() < logoutDeadlineNs) {
            library.poll(LIBRARY_POLL_LIMIT);
            Thread.onSpinWait();
          }

          return result;
        }
      }
    }
  }

  /**
   * Scenario: send a NewOrderSingle for EURUSD on account ACME, validate ExecutionReport response.
   *
   * @return exit code (0=pass, 1=assertion, 3=timeout)
   */
  private static int runNewOrderSingleScenario(
      final Session session,
      final FixLibrary library,
      final LinkedBlockingQueue<CapturedMessage> messageQueue,
      final String senderCompId) {

    final String clOrdId = "E2E-" + NANO_CLOCK.nanoTime();

    // Encode NewOrderSingle
    final NewOrderSingleEncoder nos = new NewOrderSingleEncoder();
    nos.clOrdID(clOrdId);
    nos.instrument().symbol("EURUSD");
    nos.side(Side.BUY);
    nos.ordType(OrdType.LIMIT);
    nos.price(105, 2); // 1.05
    nos.orderQtyData().orderQty(1, 0); // 1.0
    nos.account("ACME");
    nos.currency("USD");
    nos.timeInForce(TimeInForce.DAY);

    // TransactTime — required FIX field
    final UtcTimestampEncoder tsEncoder = new UtcTimestampEncoder();
    final int tsLen = tsEncoder.encode(System.currentTimeMillis());
    nos.transactTime(tsEncoder.buffer(), tsLen);

    // Send NOS — retry on transient backpressure under a bounded deadline
    final long nosStartNs = NANO_CLOCK.nanoTime();
    final long sendDeadlineNs = nosStartNs + TimeUnit.SECONDS.toNanos(5);
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
      System.err.println("E2E FAIL: trySend returned " + sendResult + " after retries");
      return EXIT_ASSERTION;
    }
    System.out.println("Sent NewOrderSingle: ClOrdID=" + clOrdId);

    // Poll for ExecutionReport (35=8) response — filter by message type
    final long responseDeadlineNs =
        NANO_CLOCK.nanoTime() + TimeUnit.MILLISECONDS.toNanos(RESPONSE_TIMEOUT_MS);

    while (NANO_CLOCK.nanoTime() < responseDeadlineNs) {
      library.poll(LIBRARY_POLL_LIMIT);

      final CapturedMessage msg = messageQueue.poll();
      if (msg != null) {
        // Filter: only process ExecutionReport (35=8, MESSAGE_TYPE=56)
        if (msg.messageType() != ExecutionReportDecoder.MESSAGE_TYPE) {
          System.out.println("Ignoring non-ER message: msgType=" + msg.messageType());
          continue;
        }
        final long rttMs = TimeUnit.NANOSECONDS.toMillis(NANO_CLOCK.nanoTime() - nosStartNs);
        return validateExecutionReport(msg, clOrdId, rttMs);
      }
      Thread.onSpinWait();
    }

    System.err.println("E2E FAIL: no response within " + RESPONSE_TIMEOUT_MS + "ms");
    return EXIT_TIMEOUT;
  }

  /**
   * Validates the received FIX message is an ExecutionReport with ExecType=New, OrdStatus=New, and
   * the ClOrdID matches.
   */
  private static int validateExecutionReport(
      final CapturedMessage msg, final String expectedClOrdId, final long rttMs) {

    final MutableAsciiBuffer buffer = new MutableAsciiBuffer(msg.data());
    final ExecutionReportDecoder decoder = new ExecutionReportDecoder();
    decoder.decode(buffer, 0, msg.data().length);

    // ExecType (tag 150) — '0' = New
    final char execType = decoder.execType();
    if (execType != '0') {
      System.err.println(
          "E2E FAIL: ExecType='"
              + execType
              + "' (expected '0' New). "
              + "Full message: "
              + new String(msg.data()));
      return EXIT_ASSERTION;
    }

    // OrdStatus (tag 39) — '0' = New
    final char ordStatus = decoder.ordStatus();
    if (ordStatus != '0') {
      System.err.println("E2E FAIL: OrdStatus='" + ordStatus + "' (expected '0' New)");
      return EXIT_ASSERTION;
    }

    // ClOrdID (tag 11) — must echo back what we sent
    final String clOrdId = new String(decoder.clOrdID(), 0, decoder.clOrdIDLength()).trim();
    if (!expectedClOrdId.equals(clOrdId)) {
      System.err.println(
          "E2E FAIL: ClOrdID='" + clOrdId + "' (expected '" + expectedClOrdId + "')");
      return EXIT_ASSERTION;
    }

    // OrderID (tag 37) — must be present and non-empty
    final int orderIdLen = decoder.orderIDLength();
    if (orderIdLen <= 0) {
      System.err.println("E2E FAIL: OrderID is empty");
      return EXIT_ASSERTION;
    }
    final String orderId = new String(decoder.orderID(), 0, orderIdLen).trim();

    System.out.println(
        "E2E PASS: ExecType=New OrdStatus=New ClOrdID="
            + clOrdId
            + " OrderID="
            + orderId
            + " (round-trip: "
            + rttMs
            + "ms)");
    return EXIT_PASS;
  }

  /**
   * Creates a {@link SessionHandler} that captures inbound messages into the queue. Defensively
   * copies the transient Artio buffer to a byte[] before queueing.
   */
  private static SessionHandler createSessionHandler(
      final Session session, final LinkedBlockingQueue<CapturedMessage> messageQueue) {

    System.out.println("Session acquired: id=" + session.id() + " state=" + session.state());

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
        System.err.println("Session timeout: id=" + session.id());
      }

      @Override
      public void onSlowStatus(
          final int libraryId, final Session session, final boolean hasBecomeSlow) {
        System.err.println("Session slow status: id=" + session.id() + " slow=" + hasBecomeSlow);
      }

      @Override
      public Action onDisconnect(
          final int libraryId, final Session session, final DisconnectReason reason) {
        System.err.println("Session disconnected: id=" + session.id() + " reason=" + reason);
        return Action.CONTINUE;
      }

      @Override
      public void onSessionStart(final Session session) {
        System.out.println("Session started: id=" + session.id());
      }
    };
  }

  /**
   * Captured FIX message — defensive copy of the transient Artio buffer content plus the message
   * type for dispatch.
   */
  private record CapturedMessage(long messageType, byte[] data) {}
}
