package com.trading.engine.gateway;

import static java.util.Collections.singletonList;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.trading.engine.fix.builder.ExecutionReportEncoder;
import com.trading.engine.fix.builder.OrderCancelRejectEncoder;
import com.trading.engine.fix.builder.QuoteEncoder;
import com.trading.engine.fix.builder.QuoteRequestRejectEncoder;
import com.trading.engine.messages.sbe.ExecutionReportDecoder;
import com.trading.engine.messages.sbe.OrderCancelRejectDecoder;
import com.trading.engine.messages.sbe.OrderCanceledEventDecoder;
import com.trading.engine.messages.sbe.OrderCreatedEventDecoder;
import com.trading.engine.messages.sbe.OrderExpiredEventDecoder;
import com.trading.engine.messages.sbe.OrderRejectedEventDecoder;
import com.trading.engine.messages.sbe.QuoteDecoder;
import com.trading.engine.messages.sbe.QuoteRequestRejectDecoder;
import io.aeron.ControlledFragmentAssembler;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.logbuffer.ControlledFragmentHandler;
import java.io.File;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.NanoClock;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.library.AcquiringSessionExistsHandler;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import uk.co.real_logic.artio.library.OnMessageInfo;
import uk.co.real_logic.artio.library.SessionAcquireHandler;
import uk.co.real_logic.artio.library.SessionHandler;
import uk.co.real_logic.artio.messages.DisconnectReason;
import uk.co.real_logic.artio.session.Session;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;
import uk.co.real_logic.artio.validation.AuthenticationStrategy;
import uk.co.real_logic.artio.validation.MessageValidationStrategy;

/**
 * Artio FIX 4.4 acceptor gateway. Implements {@link Agent} for single-threaded duty-cycle
 * composition. Bridges inbound FIX sessions to the Aeron Cluster via the {@link ClusterClient}.
 *
 * <p><b>Lifecycle.</b> {@link #onStart()} launches the Artio {@link FixEngine} and connects a
 * {@link FixLibrary}. {@link #doWork()} polls both the Artio library (for inbound FIX messages) and
 * the cluster client (for egress responses). {@link #onClose()} performs a two-phase drain: waits
 * for in-flight commands, sends Logout to all sessions, then shuts down.
 *
 * <p><b>Session management.</b> On each new FIX session, {@link #onSessionAcquired} creates a
 * {@link FixSessionHandler} and registers the session in the {@link SessionRegistry}. Session
 * capacity is enforced (global max + per-CompID max).
 *
 * <p><b>Egress callback.</b> Cluster responses are routed via the {@link
 * ClusterEgressListener.EgressCallback} to the correct FIX session by looking up the session key in
 * the {@link SessionRegistry} and calling {@link GatewaySession#trySend}.
 *
 * <p><b>Allocation.</b> Zero allocation on hot path after startup. Shared translators, encoders,
 * and buffers are pre-allocated and reused across all sessions.
 *
 * <p><b>Threading.</b> Not thread-safe — single-threaded gateway duty-cycle thread only.
 */
public final class FixGateway implements Agent {

  private static final Log LOG = LogFactory.getLog(FixGateway.class);

  /** Stateless handler returned when a session is rejected at capacity. Pre-allocated once. */
  private static final SessionHandler NO_OP_HANDLER = new NoOpSessionHandler();

  private static final long SWEEP_INTERVAL_NS = TimeUnit.SECONDS.toNanos(60);
  private static final long DRAIN_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(10);
  private static final long LOGOUT_POLL_NS = TimeUnit.SECONDS.toNanos(1);
  private static final int LIBRARY_POLL_LIMIT = 10;

  /**
   * Default correlation TTL: sum of orchestrator state timeouts (5s + 30s + 5s) + 5s margin.
   * Correlations older than this are expired regardless of session state, protecting against
   * orchestrator crash/stall scenarios.
   */
  private static final long DEFAULT_CORRELATION_TTL_NS = TimeUnit.SECONDS.toNanos(45);

  // --- Configuration ---
  private final String bindAddress;
  private final int port;
  private final String aeronChannel;
  private final String aeronDirectoryName;
  private final String archiveDir;
  private final String logFileDir;
  private final String targetCompId;
  private final Collection<String> allowedSenderCompIds;

  // --- Collaborators ---
  private ClusterClient clusterClient;
  private ClusterEgressListener egressListener;
  private ExclusivePublication orchestratorRequestPub;
  private final SessionRegistry registry;
  private final FixToSbeTranslator fixToSbeTranslator;
  private final RejectEmitter rejectEmitter;
  private final InFlightTracker inFlightTracker;
  private final NanoClock nanoClock;

  // --- Shared buffers (single-threaded, safe to share across session handlers) ---
  private final MutableAsciiBuffer sharedAsciiBuffer = new MutableAsciiBuffer(new byte[4096]);
  private final MutableDirectBuffer sharedSbeBuffer = new ExpandableDirectByteBuffer(1024);

  // --- Pre-allocated FIX response encoders (for egress + orchestrator response path) ---
  private final ExecutionReportEncoder erEncoder = new ExecutionReportEncoder();
  private final OrderCancelRejectEncoder cxlRejEncoder = new OrderCancelRejectEncoder();
  private final QuoteEncoder quoteEncoder = new QuoteEncoder();
  private final QuoteRequestRejectEncoder qrrEncoder = new QuoteRequestRejectEncoder();

  // --- Orchestrator IPC (stream 101, optional — set via initOrchestrator) ---
  private Subscription orchestratorResponseSub;
  private OrchestratorResponseListener orchestratorResponseListener;
  private ControlledFragmentAssembler orchestratorAssembler;
  private SbeToFixTranslator orchestratorTranslator;

  // --- Embedded archive for Artio FIX message log persistence (created in onStart) ---
  private Archive archive;

  // --- Artio components (created in onStart) ---
  private FixEngine engine;
  private FixLibrary library;

  // --- Cached lambda to avoid allocation on each session acquire ---
  private final FixSessionHandler.DrainingSupplier drainingSupplier = this::isDraining;

  // --- State ---
  // Not volatile: onClose() is called by the Agent framework on the same duty-cycle thread as
  // doWork(). If external shutdown coordination is needed, the caller must signal the AgentRunner
  // to stop, which then calls onClose() on the correct thread.
  private boolean draining;

  /** Guards against double-start of clusterClient; set once in onStart(). */
  private boolean clusterClientStarted;

  private long lastSweepNs;
  private final long correlationTtlNs = DEFAULT_CORRELATION_TTL_NS;

  /**
   * @param bindAddress TCP bind address for FIX connections
   * @param port TCP port for FIX connections
   * @param aeronChannel Aeron IPC channel for engine↔library communication
   * @param aeronDirectoryName Aeron media driver directory (e.g. {@code /tmp/aeron-e2e-gateway})
   * @param archiveDir directory for the gateway's embedded Aeron Archive recordings
   * @param logFileDir directory for Artio FIX message logs (persistence)
   * @param targetCompId this gateway's CompID (TargetCompID from client perspective)
   * @param allowedSenderCompIds allowed SenderCompIDs for authentication
   * @param registry session + correlation registry
   * @param fixToSbeTranslator FIX→SBE translator
   * @param rejectEmitter pre-allocated reject encoder
   * @param inFlightTracker in-flight command tracker
   * @param nanoClock monotonic clock for sweep timing
   */
  public FixGateway(
      final String bindAddress,
      final int port,
      final String aeronChannel,
      final String aeronDirectoryName,
      final String archiveDir,
      final String logFileDir,
      final String targetCompId,
      final Collection<String> allowedSenderCompIds,
      final SessionRegistry registry,
      final FixToSbeTranslator fixToSbeTranslator,
      final RejectEmitter rejectEmitter,
      final InFlightTracker inFlightTracker,
      final NanoClock nanoClock) {
    this.bindAddress = bindAddress;
    this.port = port;
    this.aeronChannel = aeronChannel;
    this.aeronDirectoryName = aeronDirectoryName;
    this.archiveDir = archiveDir;
    this.logFileDir = logFileDir;
    this.targetCompId = targetCompId;
    this.allowedSenderCompIds = allowedSenderCompIds;
    this.registry = registry;
    this.fixToSbeTranslator = fixToSbeTranslator;
    this.rejectEmitter = rejectEmitter;
    this.inFlightTracker = inFlightTracker;
    this.nanoClock = nanoClock;
  }

  /**
   * Deferred init for the cluster client (breaks circular construction dependency). Must be called
   * before {@link #onStart()}.
   */
  public void init(final ClusterClient clusterClient, final ClusterEgressListener egressListener) {
    if (clusterClient == null) {
      throw new NullPointerException("clusterClient");
    }
    if (egressListener == null) {
      throw new NullPointerException("egressListener");
    }
    this.clusterClient = clusterClient;
    this.egressListener = egressListener;
  }

  /**
   * Deferred init for orchestrator IPC. Must be called before {@link #onStart()} if orchestrator
   * routing is enabled. When not called, orchestrator features are disabled and all messages route
   * directly to the cluster.
   *
   * @param orchRequestPub Aeron IPC publication for stream 100 (gateway → orchestrator)
   * @param orchResponseSub Aeron IPC subscription for stream 101 (orchestrator → gateway)
   */
  public void initOrchestrator(
      final ExclusivePublication orchRequestPub, final Subscription orchResponseSub) {
    if (orchRequestPub == null) {
      throw new NullPointerException("orchRequestPub");
    }
    if (orchResponseSub == null) {
      throw new NullPointerException("orchResponseSub");
    }
    if (clusterClient == null) {
      throw new IllegalStateException("init() must be called before initOrchestrator()");
    }
    this.orchestratorRequestPub = orchRequestPub;

    // Reuse the egress listener's translator — safe because the single-threaded duty cycle
    // processes orchestrator responses and cluster egress sequentially, never concurrently.
    this.orchestratorTranslator = egressListener.translator();

    this.orchestratorResponseListener =
        new OrchestratorResponseListener(
            registry, registry, clusterClient, nanoClock, this::onOrchestratorResponse);
    this.orchestratorResponseSub = orchResponseSub;
    this.orchestratorAssembler = new ControlledFragmentAssembler(orchestratorResponseListener);
  }

  // ===========================================================================
  // Agent lifecycle
  // ===========================================================================

  @Override
  public String roleName() {
    return "fix-gateway";
  }

  @Override
  public void onStart() {
    if (clusterClient == null) {
      throw new IllegalStateException("init() must be called before onStart()");
    }

    // Configure message validation (CompID allowlist)
    final MessageValidationStrategy validation =
        MessageValidationStrategy.targetCompId(targetCompId)
            .and(MessageValidationStrategy.senderCompId(allowedSenderCompIds));
    final AuthenticationStrategy auth = AuthenticationStrategy.of(validation);

    // Launch embedded archive on the gateway media driver — Artio requires AeronArchive
    // for FIX message log persistence. SHARED threading is sufficient for the gateway's
    // archive usage (recording FIX messages only, no cluster replication).
    // Archive controlChannel MUST be UDP (Aeron 1.50+ rejects IPC for archive control).
    // Use ephemeral port 0 — the gateway archive is only accessed in-process via IPC local
    // control; the UDP control channel is for external tooling (ClusterTool, backup).
    final var archiveCtx =
        new Archive.Context()
            .aeronDirectoryName(aeronDirectoryName)
            .archiveDir(new File(archiveDir))
            .controlChannel("aeron:udp?endpoint=localhost:0")
            .localControlChannel("aeron:ipc?term-length=64k")
            .recordingEventsEnabled(true)
            .recordingEventsChannel("aeron:ipc?term-length=64k")
            .replicationChannel("aeron:udp?endpoint=localhost:0")
            .threadingMode(ArchiveThreadingMode.SHARED)
            .errorHandler(t -> LOG.error().append("Gateway archive error: ").append(t).commit());
    archive = Archive.launch(archiveCtx);
    LOG.info().append("Gateway archive launched: ").append(archiveDir).commit();

    // Configure and launch FIX engine
    final EngineConfiguration engineConfig =
        new EngineConfiguration()
            .bindTo(bindAddress, port)
            .libraryAeronChannel(aeronChannel)
            .authenticationStrategy(auth)
            .logInboundMessages(true)
            .logOutboundMessages(true)
            .logFileDir(logFileDir)
            .slowConsumerTimeoutInMs(5000);
    // Point Artio's internal Aeron client at the gateway's media driver directory
    engineConfig.aeronContext().aeronDirectoryName(aeronDirectoryName);
    // Artio requires an AeronArchive for FIX message log persistence. Configure the archive
    // client context to point at the embedded archive launched in onStart() below.
    engineConfig
        .aeronArchiveContext()
        .controlRequestChannel("aeron:ipc")
        .controlResponseChannel("aeron:ipc")
        .recordingEventsChannel("aeron:ipc")
        .aeronDirectoryName(aeronDirectoryName);

    engine = FixEngine.launch(engineConfig);
    LOG.info()
        .append("FIX Engine launched: ")
        .append(bindAddress)
        .append(":")
        .append(port)
        .commit();

    // Configure and connect library
    final LibraryConfiguration libConfig =
        new LibraryConfiguration()
            .sessionAcquireHandler(
                (SessionAcquireHandler) (session, acquiredInfo) -> onSessionAcquired(session))
            .sessionExistsHandler(new AcquiringSessionExistsHandler())
            .libraryAeronChannels(singletonList(aeronChannel));
    libConfig.aeronContext().aeronDirectoryName(aeronDirectoryName);

    library = FixLibrary.connect(libConfig);
    LOG.info().append("FIX Library connected").commit();

    // Start cluster client within the Agent lifecycle — called exactly once by AgentRunner.
    // Delegating here (instead of a manual call in the launcher) ensures the lifecycle contract
    // is respected and prevents double-start if FixGateway is later composed into a
    // DynamicCompositeAgent.
    if (!clusterClientStarted) {
      clusterClient.onStart();
      clusterClientStarted = true;
    }

    lastSweepNs = nanoClock.nanoTime();
  }

  @Override
  public int doWork() {
    int workCount = 0;

    // 1. Poll orchestrator responses FIRST — Quote-to-client is the latency-sensitive last-mile.
    // Poll limit 32 matches the orchestrator's GATEWAY_POLL_LIMIT (source rate matching).
    if (orchestratorResponseSub != null) {
      workCount += orchestratorResponseSub.controlledPoll(orchestratorAssembler, 32);
    }

    // 2. Poll Artio library (inbound FIX messages)
    if (library != null) {
      workCount += library.poll(LIBRARY_POLL_LIMIT);
    }

    // 3. Cluster client duty cycle (egress responses, reconnection, keep-alive)
    if (clusterClient != null) {
      workCount += clusterClient.doWork();
    }

    // 4. Periodic sweeps: stale correlations (disconnected sessions) + TTL expiry (orchestrator)
    final long nowNs = nanoClock.nanoTime();
    if (nowNs - lastSweepNs >= Math.min(SWEEP_INTERVAL_NS, correlationTtlNs)) {
      final int swept = registry.sweepStaleCorrelations();
      final int ttlExpired = registry.sweepExpiredCorrelations(nowNs, correlationTtlNs);
      if (swept > 0 || ttlExpired > 0) {
        LOG.info()
            .append("Correlation sweep: stale=")
            .append(swept)
            .append(" ttlExpired=")
            .append(ttlExpired)
            .commit();
      }
      lastSweepNs = nowNs;
    }

    return workCount;
  }

  @Override
  public void onClose() {
    LOG.info().append("FixGateway shutting down — starting drain").commit();

    // Phase 1: Stop accepting new orders
    draining = true;

    // Phase 2: Wait for in-flight commands to drain (includes orchestrator responses)
    final long drainDeadline = nanoClock.nanoTime() + DRAIN_TIMEOUT_NS;
    while (inFlightTracker.size() > 0 && nanoClock.nanoTime() < drainDeadline) {
      int workDone = 0;
      if (orchestratorResponseSub != null) {
        workDone += orchestratorResponseSub.controlledPoll(orchestratorAssembler, 32);
      }
      if (library != null) {
        workDone += library.poll(LIBRARY_POLL_LIMIT);
      }
      if (clusterClient != null) {
        workDone += clusterClient.doWork();
      }
      if (workDone == 0) {
        Thread.yield();
      }
    }
    if (inFlightTracker.size() > 0) {
      LOG.warn()
          .append("Drain timeout: ")
          .append(inFlightTracker.size())
          .append(" commands still in-flight")
          .commit();
    }

    // Phase 3: Send Logout to all connected sessions. Collect keys first to avoid concurrent
    // modification of the Agrona map — logoutAndDisconnect() may trigger onDisconnect() callbacks
    // which call registry.removeSession(), invalidating the open-addressing iterator.
    final int sessionCount = registry.sessionCount();
    final long[] sessionKeys = new long[sessionCount];
    int keyIdx = 0;
    final var sessions = registry.allSessions();
    while (sessions.hasNext()) {
      final GatewaySession session = sessions.next();
      sessionKeys[keyIdx++] = session.id();
    }
    for (int i = 0; i < keyIdx; i++) {
      final GatewaySession session = registry.findSession(sessionKeys[i]);
      if (session != null && session.isConnected()) {
        final long logoutResult = session.logoutAndDisconnect();
        if (logoutResult < 0) {
          LOG.warn()
              .append("Logout failed for sessionId=")
              .append(sessionKeys[i])
              .append(" result=")
              .append(logoutResult)
              .commit();
        }
      }
    }

    // Phase 4: Poll briefly to deliver Logout ACKs
    final long logoutDeadline = nanoClock.nanoTime() + LOGOUT_POLL_NS;
    while (nanoClock.nanoTime() < logoutDeadline) {
      final int work = library != null ? library.poll(LIBRARY_POLL_LIMIT) : 0;
      if (work == 0) {
        Thread.yield();
      }
    }

    // Phase 5: Close Artio
    if (library != null) {
      library.close();
    }
    if (engine != null) {
      engine.close();
    }

    // Phase 6: Close embedded archive (after Artio — Artio may still be writing recordings)
    CloseHelper.quietClose(archive);

    // Phase 7: Close orchestrator IPC resources
    CloseHelper.closeAll(orchestratorResponseSub, orchestratorRequestPub);

    LOG.info().append("FixGateway shutdown complete").commit();
  }

  // ===========================================================================
  // Session acquisition
  // ===========================================================================

  private SessionHandler onSessionAcquired(final Session session) {
    final GatewaySession gatewaySession = new ArtioGatewaySession(session);
    final long sessionKey = gatewaySession.id();

    // Extract the counterparty's SenderCompID (tag 49) for per-CompID capacity enforcement.
    // GatewaySession.senderCompId() delegates to compositeKey().remoteCompId() which, for
    // acceptor sessions, returns the client's SenderCompID from the Logon message.
    final String senderCompId = gatewaySession.senderCompId();
    final long compIdHash = fnv1aHashString(senderCompId);

    if (!registry.tryRegisterSession(sessionKey, compIdHash, gatewaySession)) {
      LOG.warn()
          .append("Session capacity exceeded, disconnecting: sessionId=")
          .append(sessionKey)
          .commit();
      gatewaySession.logoutAndDisconnect();
      return NO_OP_HANDLER;
    }

    LOG.info().append("Session acquired: sessionId=").append(sessionKey).commit();

    return new FixSessionHandler(
        gatewaySession,
        clusterClient,
        fixToSbeTranslator,
        registry,
        rejectEmitter,
        sharedAsciiBuffer,
        sharedSbeBuffer,
        nanoClock,
        orchestratorRequestPub,
        drainingSupplier);
  }

  // ===========================================================================
  // Egress callback (cluster response → FIX client)
  // ===========================================================================

  /**
   * Called by {@link ClusterEgressListener} when a cluster response arrives. Translates SBE → FIX
   * and sends to the correct Artio session.
   *
   * <p><b>Visibility:</b> Public for cross-package method reference from {@link
   * com.trading.engine.launcher.GatewayLauncher}. Not intended for external callers.
   *
   * @param sessionKey Artio session key identifying the target FIX session
   * @param templateId SBE template ID of the cluster response message
   * @param timestamp cluster timestamp (epoch nanoseconds) of the response event
   * @return {@code true} if the message was delivered or the session is gone (ACK either way);
   *     {@code false} if the Artio session's send buffer is full (backpressure)
   */
  public boolean onEgressMessage(
      final long sessionKey, final int templateId, final long timestamp) {
    final GatewaySession session = registry.findSession(sessionKey);
    if (session == null || !session.isConnected()) {
      if (session != null) {
        registry.removeSession(sessionKey);
      }
      return true; // ACK to clear in-flight — session is gone
    }

    final long position;
    switch (templateId) {
      case ExecutionReportDecoder.TEMPLATE_ID -> {
        erEncoder.reset();
        egressListener
            .translator()
            .translateExecutionReport(egressListener.executionReportDecoder(), erEncoder);
        position = session.trySend(erEncoder);
      }
      case OrderCancelRejectDecoder.TEMPLATE_ID -> {
        cxlRejEncoder.reset();
        egressListener
            .translator()
            .translateOrderCancelReject(egressListener.orderCancelRejectDecoder(), cxlRejEncoder);
        position = session.trySend(cxlRejEncoder);
      }
      case QuoteDecoder.TEMPLATE_ID -> {
        quoteEncoder.reset();
        egressListener.translator().translateQuote(egressListener.quoteDecoder(), quoteEncoder);
        position = session.trySend(quoteEncoder);
      }
      case OrderCreatedEventDecoder.TEMPLATE_ID -> {
        erEncoder.reset();
        egressListener
            .translator()
            .translateOrderCreatedEvent(egressListener.orderCreatedDecoder(), erEncoder);
        position = session.trySend(erEncoder);
      }
      case OrderRejectedEventDecoder.TEMPLATE_ID -> {
        erEncoder.reset();
        egressListener
            .translator()
            .translateOrderRejectedEvent(egressListener.orderRejectedDecoder(), erEncoder);
        position = session.trySend(erEncoder);
      }
      case OrderCanceledEventDecoder.TEMPLATE_ID -> {
        // APP-151 phase 2 — cluster-emitted OrderCanceledEvent (template 103) → FIX ER
        // ExecType=Canceled('4'). Cluster phase-1 emitter is the session-disconnect orphan-cancel
        // path in NewOrderSingleHandler.onSessionClose.
        erEncoder.reset();
        egressListener
            .translator()
            .translateOrderCanceledEvent(egressListener.orderCanceledDecoder(), erEncoder);
        position = session.trySend(erEncoder);
      }
      case OrderExpiredEventDecoder.TEMPLATE_ID -> {
        // APP-62 §J — cluster-emitted OrderExpiredEvent (template 121) → FIX ER
        // ExecType=Expired('C', tag 150). Cluster emitters today: idle-session timeout
        // (NewOrderSingleHandler.onIdleScan). Reserved future emitter: TIF-driven expiries.
        erEncoder.reset();
        egressListener
            .translator()
            .translateOrderExpiredEvent(egressListener.orderExpiredDecoder(), erEncoder);
        position = session.trySend(erEncoder);
      }
      default -> {
        LOG.warn().append("Unknown egress templateId=").append(templateId).commit();
        return true;
      }
    }

    if (position >= 0) {
      // Clean up the correlation entry now that the response is delivered. Safe to read
      // lastCorrelationScratch here because this callback is invoked synchronously within
      // ClusterEgressListener.handleXxx — no other message can be processed until we return.
      registry.removeCorrelation(
          egressListener.lastCorrelationScratch(), 0, egressListener.lastCorrelationLen());
      return true;
    }
    return false;
  }

  // ===========================================================================
  // Orchestrator response callback (orchestrator → FIX client)
  // ===========================================================================

  /**
   * Called by {@link OrchestratorResponseListener} when an orchestrator response arrives on stream
   * 101. Translates SBE → FIX and sends to the correct Artio session. Handles Quote (35=S),
   * QuoteRequestReject (35=AG), and reject ExecutionReport (35=8) from the orchestrator.
   *
   * @param sessionKey Artio session key identifying the target FIX session
   * @param templateId SBE template ID of the orchestrator response message
   * @param timestamp unused (orchestrator responses do not carry a cluster timestamp)
   * @return {@code true} if the message was delivered or the session is gone; {@code false} if the
   *     Artio session's send buffer is full (backpressure → ABORT)
   */
  boolean onOrchestratorResponse(
      final long sessionKey, final int templateId, final long timestamp) {
    final GatewaySession session = registry.findSession(sessionKey);
    if (session == null || !session.isConnected()) {
      if (session != null) {
        registry.removeSession(sessionKey);
      }
      return true; // ACK — session is gone
    }

    final long position;
    switch (templateId) {
      case QuoteDecoder.TEMPLATE_ID -> {
        quoteEncoder.reset();
        orchestratorTranslator.translateQuote(
            orchestratorResponseListener.quoteDecoder(), quoteEncoder);
        position = session.trySend(quoteEncoder);
      }
      case QuoteRequestRejectDecoder.TEMPLATE_ID -> {
        qrrEncoder.reset();
        orchestratorTranslator.translateQuoteRequestReject(
            orchestratorResponseListener.quoteRequestRejectDecoder(), qrrEncoder);
        position = session.trySend(qrrEncoder);
      }
      case ExecutionReportDecoder.TEMPLATE_ID -> {
        erEncoder.reset();
        orchestratorTranslator.translateExecutionReport(
            orchestratorResponseListener.executionReportDecoder(), erEncoder);
        position = session.trySend(erEncoder);
      }
      default -> {
        LOG.warn().append("Unknown orchestrator response templateId=").append(templateId).commit();
        return true;
      }
    }

    if (position >= 0) {
      // Correlation cleanup is handled by OrchestratorResponseListener after this callback returns.
      return true;
    }
    return false;
  }

  // ===========================================================================
  // Accessors
  // ===========================================================================

  boolean isDraining() {
    return draining;
  }

  /** Returns the session registry (for testing). */
  SessionRegistry registry() {
    return registry;
  }

  /**
   * FNV-1a hash of a String, treating each char as a byte (ASCII). Used for CompID hashing. Zero
   * allocation — reads chars inline without getBytes(). Returns FNV offset basis for null/empty
   * strings (not 0, which could collide with a legitimate hash). The result is remapped via {@link
   * SessionRegistry#remapSentinel} to avoid collision with Agrona's {@code MISSING_VALUE} sentinel.
   */
  static long fnv1aHashString(final String s) {
    if (s == null || s.isEmpty()) {
      return SessionRegistry.remapSentinel(0xcbf29ce484222325L);
    }
    long hash = 0xcbf29ce484222325L;
    for (int i = 0, len = s.length(); i < len; i++) {
      hash ^= (s.charAt(i) & 0xFFL);
      hash *= 0x100000001b3L;
    }
    return SessionRegistry.remapSentinel(hash);
  }

  // ===========================================================================
  // No-op session handler for rejected sessions
  // ===========================================================================

  private static final class NoOpSessionHandler implements SessionHandler {

    @Override
    public ControlledFragmentHandler.Action onMessage(
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
      return ControlledFragmentHandler.Action.CONTINUE;
    }

    @Override
    public void onSessionStart(final Session session) {}

    @Override
    public void onTimeout(final int libraryId, final Session session) {}

    @Override
    public void onSlowStatus(
        final int libraryId, final Session session, final boolean hasBecomeSlow) {}

    @Override
    public ControlledFragmentHandler.Action onDisconnect(
        final int libraryId, final Session session, final DisconnectReason reason) {
      return ControlledFragmentHandler.Action.CONTINUE;
    }
  }
}
