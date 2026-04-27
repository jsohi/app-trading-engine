# PR 3 Steps 13-18: Auth Handler, Frame Dispatcher, Drain Filtering, Wiring

## Context

PR 3 (APP-35) adds JWT authentication, per-session subscription filtering, and account entitlement enforcement to the WebSocket server. Steps 0-12 are complete — all supporting infrastructure exists (JwtValidator, JtiRevocationCache, AuthFailureTracker, SubscriptionFilter, SymbolExtractor, AccountExtractor, UserEntitlementService, session/config/metrics updates, SbeTestEncoder additions). Steps 13-18 build the two Netty handlers, refactor the drain handler for filtering + per-session sequence numbers, and wire everything through the launcher.

---

## Pre-Requisite A: WebSocketServerConfig — 5 New Fields

**File:** `websocket-server/src/main/java/.../websocket/WebSocketServerConfig.java`

Config is missing 5 fields required by JwtAuthHandler (step 13). Must be added first.

| Field | Default | Validation |
|-------|---------|------------|
| `String jwtAudience` | `""` (builder) | Required non-empty when `issuerRegistry` is non-empty |
| `int maxTokenSizeBytes` | `8192` | `[256, 65536]` |
| `int maxPendingAuth` | `64` | `>= 1` |
| `int authFailureLockoutThreshold` | `5` | `> 0` |
| `int authFailureLockoutSeconds` | `60` | `> 0` |

Also add validation:
- All `issuerRegistry` JWKS URLs must use `https://` scheme (reject `http://`).
- `issuerRegistry` must be non-empty when `jwtAudience` is non-empty (and vice versa).

Changes touch: fields, constructor, `validate()`, `fromYaml()`, accessors, `Builder` class.

**Test updates:** `WebSocketServerConfigTest` — add ~7 tests:
- `build_jwtAudienceRequiredWhenIssuerRegistryPresent`
- `build_maxTokenSizeBytesTooSmall_throws`
- `build_maxTokenSizeBytesTooLarge_throws`
- `build_maxTokenSizeBytesAtBoundary_succeeds`
- `build_maxPendingAuthZero_throws`
- `build_issuerRegistryHttpUrl_throws`
- `build_authFailureLockoutFieldsValidation`

---

## Pre-Requisite B: SbeTestEncoder — 2 New Encode Methods

**File:** `test-support/src/main/java/.../testsupport/sbe/SbeTestEncoder.java`

Missing `encodeWebSocketSubscribe` and `encodeWebSocketUnsubscribe` (needed by WebSocketFrameDispatcherTest).

- `encodeWebSocketSubscribe(MutableDirectBuffer dst, int offset, String[] symbols, int[] eventTypes)` — repeating group: header + per-entry (8B symbol + 4B eventTypes)
- `encodeWebSocketUnsubscribe(MutableDirectBuffer dst, int offset, String[] symbols)` — repeating group: header + per-entry (8B symbol). Empty array = unsubscribe-all.

Uses `WebSocketSubscribeEncoder` / `WebSocketUnsubscribeEncoder` (both exist in generated codecs).

---

## Pre-Requisite C: AccountExtractor Zero-Alloc Refactor

**Files:**
- `websocket-server/src/main/java/.../websocket/AccountExtractor.java`
- `websocket-server/src/main/java/.../websocket/WebSocketSession.java`
- `websocket-server/src/test/java/.../websocket/AccountExtractorTest.java`

**Problem (Performance Review Item 2+7):** `AccountExtractor.extractAccountCode()` allocates a `String` per call on the drain hot path (~256K allocs/sec at peak → ~8-12MB/sec GC pressure). `session.entitledAccounts().contains(String)` compounds this with `hashCode()` computation. This violates the project's zero-alloc hot-path policy and is inconsistent with `SymbolExtractor` which uses packed `long` via `SymbolPacker`.

**Fix:** Create `AccountPacker` (mirrors `SymbolPacker`):
- `static long packHigh(byte[] src, int offset)` — packs bytes 0-7 of the 16-byte SBE `char[16]` account code into a `long`
- `static long packLow(byte[] src, int offset)` — packs bytes 8-15 into a `long`
- Refactor `AccountExtractor` to provide a **single-call API** that computes the account offset once:
  - `static boolean extractPackedAccount(int templateId, byte[] sbePayload, int offset, int length, long[] out)` — writes `out[0]=high`, `out[1]=low`, returns `true` if account field present and not truncated; `false` otherwise. The `long[2] out` array is a pre-allocated flyweight field on `WebSocketDrainHandler` (NOT per-call allocation). This avoids the double `absoluteAccountOffset()` switch evaluation that two separate `extractHigh/extractLow` calls would cause.
- Refactor `WebSocketSession.entitledAccounts` from `Set<String>` to `long[]` packed pairs (1-4 accounts → 2-8 longs, linear scan beats hash for N<8)
- Add `session.isEntitledAccount(long high, long low)` — linear scan over packed array, zero allocation
- Update `UserEntitlementService.validateAccounts()` return type to include packed representation. **Implementation note:** when packing account codes from `AccountReadModel.accountCode()` (String), NUL-pad to 16 bytes before packing to match SBE wire encoding. Add assertion in test: `AccountPacker.packHigh/Low` on String-derived bytes equals packed value from SBE `char[16]`.
- Update `AccountExtractorTest` to verify packed extraction matches original String extraction

This eliminates all `String` allocation and `hashCode` computation from the drain path. Estimated ~200ns savings per call.

---

## Pre-Requisite D: JwtValidator JWKS Hardening + Event Loop Protection

**File:** `websocket-server/src/main/java/.../websocket/JwtValidator.java`

**Problem 1 (Security Review Item 9):** JWKS `DefaultResourceRetriever` follows HTTP redirects by default (SSRF risk). No explicit response size limit.

**Fix (2 one-line changes):**
1. Use 4-parameter `DefaultResourceRetriever(connectTimeoutMs, readTimeoutMs, sizeLimit, disconnectOnRedirect)` constructor: `new DefaultResourceRetriever(5000, 5000, 256_000, true)` — 256KB size limit, redirects disabled.
2. Add inline comment: `// SSRF prevention: redirects disabled, response size capped at 256KB`

**Problem 2 (Performance Review R2 Item 4):** `jwtValidator.validate()` is synchronous — a JWKS cache miss blocks the Netty event loop for up to 5s (connect) + 5s (read) = 10s, stalling ALL channels on that event loop thread, not just the authenticating one.

**Fix:** Move JWT validation off the event loop in `JwtAuthHandler.channelRead()`:
```java
// Offload blocking JWT validation to a dedicated executor to avoid stalling the event loop.
// The ForkJoinPool.commonPool() is acceptable here because auth is a cold path (once per
// connection, max 64 concurrent via pendingAuthCount). A dedicated single-thread executor
// would be over-engineered for 64 max concurrent validations.
CompletableFuture.supplyAsync(() -> jwtValidator.validate(tokenString))
    .whenCompleteAsync((claims, ex) -> {
        if (ex != null) {
            rejectAuth(ctx, remoteIp, "JWT validation failed: " + ex.getMessage());
            return;
        }
        // Continue auth flow on the event loop thread (steps 8-20)
        continueAuthOnEventLoop(ctx, claims, remoteIp, channel);
    }, ctx.executor());
```
This ensures the event loop thread is never blocked by JWKS HTTP fetches. The `whenCompleteAsync(_, ctx.executor())` callback runs on the channel's event loop, preserving the single-threaded guarantee for pipeline operations. The `authResolved` volatile guard at the top of `channelRead` protects against races with the auth timeout.

**Implementation detail:** Split `channelRead` into two phases:
1. **Sync phase (on event loop):** frame size check, templateId check, protocol version check, IP lockout check, extract token String. These are all non-blocking.
2. **Async phase (off event loop):** `jwtValidator.validate()` runs on `ForkJoinPool.commonPool()`.
3. **Completion phase (back on event loop via `ctx.executor()`):** JTI check, entitlement validation, session registration, pipeline mutation, response encoding. All pipeline operations remain single-threaded.

**Test impact:** `JwtValidator.forTesting()` uses in-memory JWKS (no HTTP), so `validate()` returns in ~1ms. Tests can use `EmbeddedChannel.runPendingTasks()` to execute the completion callback synchronously. Add test: `channelRead_validAuth_asyncValidationCompletesOnEventLoop`.

---

## Step 13: JwtAuthHandler + JwtAuthHandlerTest

### New file: `websocket-server/src/main/java/.../websocket/JwtAuthHandler.java`

Per-channel `ChannelInboundHandlerAdapter`, NOT `@Sharable`. One-shot auth gate — removes self from pipeline after success, adds WebSocketFrameDispatcher dynamically.

**Constructor dependencies (all exist):**
- `AtomicInteger pendingAuthCount` (shared across channels, created by WebSocketServerMain)
- `JwtValidator`, `JtiRevocationCache`, `UserEntitlementService`, `AuthFailureTracker`
- `WebSocketSessionManager`, `WebSocketMetrics`, `WebSocketServerConfig`
- `NanoClock` (for dispatcher creation on auth success)

**Per-channel fields:**
- `volatile boolean authResolved` — double-decrement guard
- `ScheduledFuture<?> authTimeoutFuture` — 5s timeout, cancelled on resolution
- `ExpandableArrayBuffer responseBuf` (128B, pre-allocated for SBE response encoding)
- `MessageHeaderDecoder headerDecoder`, `WebSocketAuthDecoder authDecoder`
- `WebSocketSession registeredSession` — tracks if tryRegister succeeded (for cleanup on partial failure)

**channelActive flow:**
1. Increment `pendingAuthCount`; if `>= config.maxPendingAuth()`, close immediately, decrement
2. Schedule 5s auth timeout via `ctx.executor().schedule()`

**channelRead(BinaryWebSocketFrame) flow:**
0. **Guard:** `if (authResolved) { ReferenceCountUtil.release(msg); return; }` — defense-in-depth against post-timeout reads (Correctness Review Item 13)
1. Frame size check: `> config.maxTokenSizeBytes()` → reject
2. Decode SBE header → reject if `templateId != 60`
3. Decode `WebSocketAuth`: extract `protocolVersion`, token bytes
4. Protocol version check: if `!= EXPECTED_PROTOCOL_VERSION` → send `WebSocketError(VersionMismatch)`, close
5. Per-IP lockout: `authFailureTracker.isBlocked(remoteIp)` → reject; `metrics.authLockout()`
6. Convert token to UTF-8 String
7. **Auth latency timer start:** `long authStart = nanoClock.nanoTime()`
8. `jwtValidator.validate(tokenString)` → catch exceptions → reject
9. `jtiCache.isRevoked(claims.jti())` → reject if revoked
10. `entitlementService.validateAccounts(claims.accounts())` → reject if empty set
11. `sessionManager.tryRegister(channel)` → reject if null (global capacity); store as `registeredSession`
12. `sessionManager.setUserId(session, claims.sub())` → reject if false (per-user limit); **on failure, deregister session** (Correctness Review Item 19)
13. Store: `session.jti(claims.jti())`, `session.entitledAccounts(validatedAccounts)`, `session.initSubscriptionFilter(config.maxSubscriptionsPerClient())`
14. Cancel auth timeout
15. `metrics.recordAuthLatencyNanos(nanoClock.nanoTime() - authStart)`
16. Encode + send `WebSocketAuthAck(sessionId.getMostSignificantBits(), sessionId.getLeastSignificantBits(), protocolVersion, maxSubscriptions)`
17. `ctx.pipeline().addAfter(ctx.name(), "frame-dispatcher", new WebSocketFrameDispatcher(...))`
18. `ctx.pipeline().remove(this)`
19. Set `authResolved = true`, decrement `pendingAuthCount`; set `registeredSession = null`
20. `metrics.authSucceeded()`

**Error path:** Single `WebSocketError(AuthenticationFailed)` for ALL failures (no oracle). **If `registeredSession != null`, call `sessionManager.removeSession(channel)` before closing** (prevents session leak on partial auth failure — Correctness Review Item 19). Close channel. Record failure in tracker + metrics. Set `authResolved`, decrement counter.

**ByteBuf leak prevention:** Use `boolean written = false` / try/finally pattern for all response encoding:
```java
boolean written = false;
final var buf = ctx.alloc().buffer(frameSize);
try {
    // encode SBE into buf
    ctx.writeAndFlush(new BinaryWebSocketFrame(buf));
    written = true;
} finally {
    if (!written) buf.release();
}
```
(Security Review Item 20)

**channelInactive:** Cancel timeout; if `!authResolved`, decrement `pendingAuthCount`. If `registeredSession != null`, call `sessionManager.removeSession(channel)`.

**Logging:** INFO success (userId, sessionId); WARN failure (reason, remoteIp). **Never log JWT token, claims, or accounts.** Account codes in `UserEntitlementService` should be logged at DEBUG level only (Security Review Item 17).

### New file: `websocket-server/src/test/java/.../websocket/JwtAuthHandlerTest.java`

EmbeddedChannel tests with `ResourceLeakDetector.PARANOID`. Uses `JwtValidator.forTesting()` with in-memory RSA key pair (pattern from `JwtValidatorTest`).

**Key test cases (26):**
- `channelRead_validAuth_sendsAuthAckAndAddsDispatcher`
- `channelRead_validAuth_asyncValidationCompletesOnEventLoop` (Performance R2 — async JWT validation)
- `channelRead_validAuth_removesHandlerFromPipeline` (Correctness Item 25.3)
- `channelRead_validAuth_storesJtiOnSession` (Correctness Item 25.4)
- `channelRead_validAuth_storesEntitledAccountsOnSession` (Correctness Item 25.5)
- `channelRead_validAuth_initializesSubscriptionFilter` (Correctness Item 25.6)
- `channelRead_validAuth_recordsAuthLatency` (Correctness Item 25.5 - auth latency)
- `channelRead_invalidToken_sendsErrorAndCloses`
- `channelRead_revokedJti_sendsErrorAndCloses`
- `channelRead_allAccountsInactive_sendsErrorAndCloses`
- `channelRead_perUserLimitExceeded_sendsErrorAndCloses`
- `channelRead_setUserIdFails_deregistersSession` (Correctness Item 19 — session leak)
- `channelRead_globalCapacityExceeded_sendsErrorAndCloses`
- `channelRead_nonAuthTemplateId_sendsErrorAndCloses`
- `channelRead_protocolVersionMismatch_sendsErrorAndCloses`
- `channelRead_tokenSizeExceeded_sendsErrorAndCloses`
- `channelRead_tokenExactlyAtLimit_succeeds` (Correctness Item 9 — boundary)
- `channelRead_emptyBinaryFrame_sendsErrorAndCloses` (Correctness Item 9 — zero-length)
- `channelRead_pendingAuthLimitExceeded_closesChannel`
- `channelRead_perIpLockout_sendsErrorWithoutJwtValidation`
- `channelRead_afterTimeoutFired_ignoresFrame` (Correctness Item 13 — post-timeout guard)
- `channelInactive_beforeAuthResolved_decrementsPendingAuth`
- `channelInactive_afterAuthResolved_doesNotDoubleDeccrement`
- `authTimeout_noAuthWithin5Seconds_sendsErrorAndCloses`
- `channelRead_validAuth_metricsAuthSucceededCalled`
- `channelRead_invalidAuth_metricsAuthFailedCalled`

---

## Step 14: WebSocketFrameDispatcher + WebSocketFrameDispatcherTest

### New file: `websocket-server/src/main/java/.../websocket/WebSocketFrameDispatcher.java`

Per-channel `ChannelInboundHandlerAdapter`, NOT `@Sharable`. Post-auth frame router — added to pipeline by JwtAuthHandler on auth success.

**Constructor dependencies:**
- `WebSocketSessionManager`, `JwtValidator`, `JtiRevocationCache`, `UserEntitlementService`
- `WebSocketServerConfig`, `WebSocketMetrics`, `NanoClock`

**Per-channel fields:**
- Reusable SBE decoders: `MessageHeaderDecoder`, `WebSocketSubscribeDecoder`, `WebSocketUnsubscribeDecoder`, `WebSocketAuthDecoder`, `ClientHeartbeatDecoder`, `ClientAckDecoder`
- `UnsafeBuffer wrapBuffer` — reusable, re-wrapped per channelRead. **Assertion:** `assert !frame.content().isComposite()` to document zero-copy assumption (Performance Review Item 22)
- `int consecutiveUnknownCount` — close after 3 consecutive unknowns
- `ExpandableArrayBuffer responseBuf` (128B, for re-auth ack / error responses)

**channelRead routing:**

**Guard at top:** `var session = sessionManager.findSession(ctx.channel()); if (session == null) { ReferenceCountUtil.release(msg); return; }` (Correctness Review Item 14 — null session guard)

**Frame type handling:**
- **`BinaryWebSocketFrame`:** Process SBE message (see routing below)
- **`TextWebSocketFrame`:** Release frame, log WARN, increment unknown counter. **Do NOT pass through** (Correctness Review Item 6 — prevents ByteBuf leak)
- **Other frame types:** Release and ignore

**SBE routing (BinaryWebSocketFrame only):**
- Wrap `ByteBuf.nioBuffer()` in `UnsafeBuffer` (valid only within channelRead scope)
- Decode `MessageHeaderDecoder`
- Switch on templateId:
  - **60 → `handleReAuth`**: validate new JWT, **check `jtiCache.isRevoked(newClaims.jti())`** (Correctness Item 10), verify `sub` matches `session.userId()` (reject on mismatch — prevents hijack), revoke old JTI via `jtiCache.revoke(session.jti())`, refresh entitlements, update session. **Update `metrics.activeSubscriptions()`** if entitlement changes affect subscriptions. Send AuthAck on success, Error on failure. Do NOT close on re-auth failure.
  - **62 → `handleSubscribe`**: decode `WebSocketSubscribeDecoder`, iterate symbols group, pack each via `SymbolPacker.pack()`, mask `eventTypes & 0x1F`, add to `session.subscriptionFilter()`. Enforce `maxSubscriptionsPerClient`. **On capacity exceeded:** send `WebSocketError(SubscriptionLimitExceeded)`, accept symbols up to capacity (partial accept). Update `metrics.activeSubscriptions()` gauge.
  - **63 → `handleUnsubscribe`**: decode `WebSocketUnsubscribeDecoder`. Empty symbols group → `filter.clear()` (unsubscribe-all). Otherwise remove each symbol. Update `metrics.activeSubscriptions()` gauge.
  - **65 → `handleClientHeartbeat`**: `session.updateHeartbeat(nanoClock.nanoTime())`
  - **68 → `handleGapRequest`**: send `WebSocketError(NotImplemented)`, log WARN `// TODO(APP-35): implement with ReliableStreamTracker in PR 4` (Security Review Item 29.4 — reject rather than silently log)
  - **69 → `handleSessionResume`**: send `WebSocketError(NotImplemented)`, log WARN `// TODO(APP-35): implement session resume in PR 4` (Security Review Item 29.4)
  - **71 → `handleClientAck`**: `session.lastClientCmdSeqNo(ackDecoder.lastReceivedSeqNo())`
  - **default**: log WARN, increment `consecutiveUnknownCount`, close if `>= 3`. Reset counter on any valid templateId.

**exceptionCaught override:** Log ERROR with channel details, close channel. Prevents unhandled exceptions from corrupting pipeline state (Correctness Review Item 12).

**ByteBuf leak prevention:** Same `boolean written = false` / try/finally pattern for all response encoding.

**Logging:** DEBUG subscribe/unsubscribe (symbol count only); WARN unknown templateId, TextWebSocketFrame; INFO re-auth success. Never log token.

### New file: `websocket-server/src/test/java/.../websocket/WebSocketFrameDispatcherTest.java`

EmbeddedChannel tests with `ResourceLeakDetector.PARANOID`. Pre-register session with subscriptionFilter + entitlements before each test. Uses `SbeTestEncoder.encodeWebSocketSubscribe/Unsubscribe` (from pre-req).

**Key test cases (21):**
- `channelRead_subscribe_addsToFilter`
- `channelRead_subscribe_metricsActiveSubscriptionsIncremented` (Correctness Item 25.14)
- `channelRead_unsubscribe_removesFromFilter`
- `channelRead_unsubscribe_metricsActiveSubscriptionsDecremented` (Correctness Item 25.15)
- `channelRead_emptyUnsubscribe_clearsAllSubscriptions`
- `channelRead_reAuth_refreshesEntitlementsAndRevokesOldJti`
- `channelRead_reAuth_checksNewJtiNotRevoked` (Correctness Item 10)
- `channelRead_reAuth_oldJtiAlreadyExpired_succeeds` (Correctness Item 10)
- `channelRead_reAuthSubMismatch_sendsErrorDoesNotClose`
- `channelRead_reAuthInvalidToken_sendsErrorDoesNotClose` (Correctness Item 6)
- `channelRead_twoConsecutiveReAuth_jtiChainCorrect` (Correctness Item 16)
- `channelRead_clientHeartbeat_updatesSessionTimestamp`
- `channelRead_clientAck_updatesLastCmdSeqNo`
- `channelRead_unknownTemplateId_logsWarning`
- `channelRead_threeConsecutiveUnknown_closesChannel`
- `channelRead_subscriptionLimitExceeded_partialAcceptAndError`
- `channelRead_undefinedEventTypeBits_maskedTo0x1F`
- `channelRead_gapRequest_sendsNotImplementedError`
- `channelRead_sessionResume_sendsNotImplementedError`
- `channelRead_textFrame_releasedAndWarned` (Correctness Item 6 — TextWebSocketFrame)
- `channelRead_sessionDeregistered_ignoresFrame` (Correctness Item 14)

---

## Step 15: WebSocketDrainHandler Modifications

**File:** `websocket-server/src/main/java/.../websocket/WebSocketDrainHandler.java`

Refactor `writeToAllChannels(EgressEntry)` into two methods. Replace `forEachSession(lambda)` with `for` loop over `sessionManager.sessions()`.

### `writeReliableToAllChannels(EgressEntry entry)`

Per-session ByteBuf (different seqNo → different CRC → can't share). **Uses `boolean written` try/finally pattern** (Security Review Item 20):
```java
for (final var session : sessionManager.sessions()) {
    final var filter = session.subscriptionFilter();
    if (filter == null) continue;                              // pre-auth
    if (!filter.matches(templateId, bytes, 0, length)) {
        metrics.filterFiltered(); continue;
    }
    // Zero-alloc account entitlement check (single-call packed long extraction)
    if (AccountExtractor.extractPackedAccount(templateId, bytes, 0, length, packedAccountBuf)
            && !session.isEntitledAccount(packedAccountBuf[0], packedAccountBuf[1])) {
        metrics.filterFiltered(); continue;
    }
    metrics.filterMatched();
    final var buf = PooledByteBufAllocator.DEFAULT.buffer(
        FrameParser.RELIABLE_HEADER_SIZE + length,
        FrameParser.RELIABLE_HEADER_SIZE + length);
    boolean written = false;
    try {
        FrameParser.encodeReliable(buf, session.nextReliableSeqNo(), bytes, 0, length);
        session.channel().write(new BinaryWebSocketFrame(buf));
        written = true;
    } finally {
        if (!written) buf.release();
    }
}
```

### `writeBestEffortToAllChannels(EgressEntry entry)`

Shared ByteBuf with `retainedDuplicate()` (seqNo=0 for all — unchanged pattern):
- Add SubscriptionFilter + packed account entitlement checks before writing
- Skip unwritable channels (existing behavior)
- `metrics.filterMatched()` / `metrics.filterFiltered()`

### `drain()` changes

**Add drain cycle latency metric** (Performance Review Item 8):
```java
final long cycleStart = System.nanoTime();
// ... drain loop ...
if (drained > 0) {
    metrics.recordDrainCycleNanos(System.nanoTime() - cycleStart);
}
```

Replace lambda flush with for-loop:
```java
for (final var session : sessionManager.sessions()) {
    if (session.channel().isActive()) session.channel().flush();
}
```

### Test updates: `WebSocketDrainHandlerTest.java`

Existing tests must be updated — they currently expect all sessions to receive all messages. With filtering, sessions need matching subscriptions + entitlements.

**Setup changes:** After `tryRegister`, call `session.initSubscriptionFilter(100)`, add matching subscriptions, set entitled accounts (packed longs).

**New test cases (11):**
- `drain_reliableMessage_usesPerSessionSeqNo`
- `drain_twoSessions_independentSeqNos` (Correctness Item 25.16)
- `drain_multipleMessages_seqNoPersistsAcrossCycles` (Correctness Item 25.17)
- `drain_filterMiss_doesNotWriteToSession`
- `drain_filterMatch_writesToSession`
- `drain_accountNotEntitled_doesNotWriteToSession`
- `drain_noSubscriptionFilter_skipsSession`
- `drain_bestEffort_sharedByteBuf`
- `drain_filterMetrics_incrementedCorrectly`
- `drain_zeroLengthPayload_handledGracefully` (Correctness Item 25.18)
- `drain_reliableAndBestEffort_interleaved_correctHeaders` (Correctness Item 25.19)

---

## Step 16: WebSocketServerMain Modifications

**File:** `websocket-server/src/main/java/.../websocket/WebSocketServerMain.java`

### Constructor changes

Add parameters:
- `JwtValidator jwtValidator`
- `JtiRevocationCache jtiCache`
- `UserEntitlementService entitlementService`
- `AuthFailureTracker authFailureTracker`
- `NanoClock nanoClock` (for dispatcher heartbeat updates)

New field: `AtomicInteger pendingAuthCount = new AtomicInteger(0)` (shared across all channels).

Update class-level Javadoc: document new constructor params, updated pipeline diagram, JwtValidator lifecycle.

### `initChannel()` changes (line 142)

After `origin-validator`, add JwtAuthHandler:
```java
pipeline.addLast("auth-handler", new JwtAuthHandler(
    pendingAuthCount, jwtValidator, jtiCache, entitlementService,
    authFailureTracker, sessionManager, metrics, config, nanoClock));
```

WebSocketFrameDispatcher is NOT added here — JwtAuthHandler adds it dynamically on auth success.

### `close()` changes

Add `jwtValidator.close()` to release JWKS HTTP client resources (before event loop shutdown).

---

## Step 17: WebSocketLauncher + WebSocketComponents + TradingEngineLauncher

### `launcher/.../WebSocketLauncher.java`

Add auth dependency creation to `launch()`:

```java
public static WebSocketComponents launch(
    String aeronDir, String ingressEndpoints, Path configPath) throws Exception {
    // ... existing steps 1-7 ...

    // NEW: Auth dependencies
    final var jwtValidator = new JwtValidator(config.issuerRegistry(), config.jwtAudience(),
        TradingClocks.epochNanoClock());
    final var jtiCache = new JtiRevocationCache(config.maxRevokedJtis(),
        config.revocationTtlMinutes(), SystemNanoClock.INSTANCE);
    final var entitlementService = new UserEntitlementService(code -> null);
        // TODO(APP-237): wire AccountProjection for real account lookup
    final var authFailureTracker = new AuthFailureTracker(
        config.authFailureLockoutThreshold(), config.authFailureLockoutSeconds(),
        SystemNanoClock.INSTANCE);

    // MODIFIED: Pass to WebSocketServerMain
    final var server = new WebSocketServerMain(config, egressQueue, egressListener,
        sessionManager, metrics, jwtValidator, jtiCache, entitlementService,
        authFailureTracker, SystemNanoClock.INSTANCE);

    // ... existing start + error handling ...

    return new WebSocketComponents(server, egressThread, clusterClient, jwtValidator);
}
```

**Note on UserEntitlementService:** The `code -> null` stub means all account lookups return null, so `validateAccounts()` returns an empty set, and auth fails with "no valid accounts". This is a known limitation — the real `AccountProjection` wiring is tracked as **TODO(APP-237)**. A real Linear issue must be filed before implementation begins (Correctness Review Item 2/4).

**Note on E2E tests:** With the `code -> null` stub, the WebSocket server will reject all auth attempts. E2E tests (`./gradlew e2e`) test the cluster path (FIX → cluster → response), not the WebSocket path. The WebSocket auth pipeline is verified by unit tests with `JwtValidator.forTesting()`. A full WebSocket pipeline integration test is deferred to APP-237.

Update `launch()` Javadoc: add `@throws` for JwtValidator construction failure, document auth dependency lifecycle.

### `launcher/.../WebSocketComponents.java`

Add `JwtValidator` field + constructor param. Close `jwtValidator.close()` FIRST in `close()` method (before server close — release JWKS HTTP clients before closing channels, preventing JWKS refresh attempts during channel shutdown).

### `launcher/.../TradingEngineLauncher.java`

Line ~264: No signature change needed — launcher creates auth deps internally. The existing call `WebSocketLauncher.launch(aeronDir, ingressEndpoints, Path.of(wsConfigFile))` remains unchanged.

---

## Step 18: docs/websocket-architecture.md Update

Mark PR 3 components as implemented:
- JwtAuthHandler (5s auth timeout, pendingAuth limit, IP lockout, authResolved guard)
- WebSocketFrameDispatcher (subscribe/unsubscribe/re-auth/heartbeat/ack routing, TextWebSocketFrame rejection)
- SubscriptionFilter copy-on-write thread safety model
- Account-level drain filtering via zero-alloc packed AccountExtractor
- Per-session reliable sequence numbers
- Pipeline diagram: add JwtAuthHandler after origin-validator (dynamic: adds dispatcher on auth success)
- JWKS hardening: redirects disabled, 256KB response size limit

---

## Implementation Order

```
Phase A (parallel, no deps):
  A1: WebSocketServerConfig + 5 new fields + tests
  A2: SbeTestEncoder + 2 encode methods (subscribe/unsubscribe)
  A3: AccountExtractor zero-alloc refactor (AccountPacker + packed entitlements)
  A4: JwtValidator JWKS hardening (2-line fix)

Phase B (after A1, A3, A4):
  B1: JwtAuthHandler + JwtAuthHandlerTest        [depends on config fields + packed accounts]
  B2: WebSocketFrameDispatcher + DispatcherTest   [depends on SbeTestEncoder + config; no dep on B1]

Phase C (after A3, parallel with B):
  C1: WebSocketDrainHandler refactor + tests      [deps: packed AccountExtractor, SubscriptionFilter]

Phase D (after B + C):
  D1: WebSocketServerMain modifications           [depends on JwtAuthHandler class existing]
  D2: WebSocketLauncher + Components + TradingEngineLauncher  [depends on D1]

Phase E (after D):
  E1: docs/websocket-architecture.md update
  E2: WebSocketMetrics: add drainCycleLatency timer

Verification at each phase:
  A: ./gradlew :websocket-server:test :test-support:compileJava
  B: ./gradlew :websocket-server:test
  C: ./gradlew :websocket-server:test
  D: ./gradlew build
  E: ./gradlew e2e (3-node cluster)
```

---

## Verification

1. `./gradlew :websocket-server:test` — all new + updated unit tests pass
2. `./gradlew :test-support:compileJava` — SbeTestEncoder additions compile
3. `./gradlew :launcher:compileJava` — WebSocketLauncher + TradingEngineLauncher compile
4. `./gradlew build` — full build passes
5. `./gradlew spotlessApply && ./gradlew spotlessCheck` — formatting OK
6. `./gradlew e2e` — 3-node cluster integration tests pass
7. ByteBuf leak detection: all tests run with `ResourceLeakDetector.PARANOID`
8. Grep test output for JWT patterns — verify no accidental token logging
9. Verify `TextWebSocketFrame` is handled without leak in dispatcher tests
10. Verify session cleanup on partial auth failure (no orphaned sessions in SessionManager)
11. Verify packed account entitlement check works end-to-end in drain handler tests

---

## Critical Files

| File | Action |
|------|--------|
| `websocket-server/.../WebSocketServerConfig.java` | Add 5 fields + validation |
| `websocket-server/.../WebSocketServerConfigTest.java` | Add ~7 tests |
| `test-support/.../SbeTestEncoder.java` | Add 2 encode methods |
| `websocket-server/.../AccountExtractor.java` | Zero-alloc refactor (packed longs) |
| `websocket-server/.../AccountExtractorTest.java` | Update for packed extraction |
| `websocket-server/.../WebSocketSession.java` | Packed entitledAccounts + isEntitledAccount() |
| `websocket-server/.../JwtValidator.java` | JWKS redirect disable + size limit |
| `websocket-server/.../JwtAuthHandler.java` | **NEW** (~320 lines, async JWT validation) |
| `websocket-server/.../JwtAuthHandlerTest.java` | **NEW** (~26 tests) |
| `websocket-server/.../WebSocketFrameDispatcher.java` | **NEW** (~320 lines) |
| `websocket-server/.../WebSocketFrameDispatcherTest.java` | **NEW** (~21 tests) |
| `websocket-server/.../WebSocketDrainHandler.java` | Refactor write methods + filtering |
| `websocket-server/.../WebSocketDrainHandlerTest.java` | Update existing + ~11 new tests |
| `websocket-server/.../WebSocketServerMain.java` | Expand constructor + pipeline wiring |
| `websocket-server/.../WebSocketMetrics.java` | Add drainCycleLatency timer |
| `launcher/.../WebSocketLauncher.java` | Create auth deps, pass to server |
| `launcher/.../WebSocketComponents.java` | Add JwtValidator for shutdown |
| `docs/websocket-architecture.md` | Mark PR 3 complete |

---

## Review Findings Incorporated

### Security (was 71% → target 95%+)

| Finding | Severity | Fix Applied |
|---------|----------|-------------|
| JWKS follows HTTP redirects (SSRF) | HIGH | Pre-Req D: disable redirects + size limit |
| ByteBuf leak in try/catch (no finally) | HIGH | All response encoding uses `boolean written` try/finally |
| Missing security audit logger | HIGH | Tracked as go-live gate (out of PR 3 scope) |
| Distributed brute force gap | MEDIUM | Documented; global auth failure circuit breaker tracked for pre-go-live |
| Account code logging at INFO | MEDIUM | Changed to DEBUG level |
| Symbol-level entitlements deferred | MEDIUM | Tracked as APP-236 |
| SessionResume stub silently logs | MEDIUM | Changed to send `WebSocketError(NotImplemented)` |

### Performance (was 72% → target 90%+)

| Finding | Severity | Fix Applied |
|---------|----------|-------------|
| AccountExtractor allocates String on hot path | HIGH | Pre-Req C: zero-alloc packed long extraction |
| String.equals in entitlements check | HIGH | Pre-Req C: packed long comparison |
| Missing drain cycle latency metric | HIGH | Step 15: added `drainCycleLatency` timer |
| JWKS cache miss blocks event loop | HIGH | Fixed: async JWT validation via CompletableFuture off event loop |
| Double offset computation in drain path | MEDIUM | Fixed: single-call extractPackedAccount(out) API |
| Per-session ByteBuf for reliable | MEDIUM | Accepted (pooled alloc per CLAUDE.md WebSocket exception) |
| BinaryWebSocketFrame heap alloc | MEDIUM | Accepted (inherent to Netty; ZGC handles sub-1ms pauses) |
| Cross-event-loop Runnable alloc | MEDIUM | Documented; channel pinning deferred to future PR |
| Single drain thread bottleneck | MEDIUM | Documented; drain budget deferred to future PR |

### Correctness (was 72% → target 95%+)

| Finding | Severity | Fix Applied |
|---------|----------|-------------|
| TextWebSocketFrame unhandled (ByteBuf leak) | CRITICAL | Step 14: explicit release + WARN log |
| Post-timeout channelRead unguarded | CRITICAL | Step 13: `authResolved` guard at channelRead top |
| Session leak on partial auth failure | CRITICAL | Step 13: `registeredSession` tracking + deregister in error path |
| activeSubscriptions gauge not updated | HIGH | Step 14: subscribe/unsubscribe update gauge |
| 5 missing JwtAuthHandler test cases | HIGH | Step 13: expanded to 25 tests |
| SeqNo test depth insufficient | HIGH | Step 15: 3 dedicated seqNo tests |
| Re-auth JTI checks incomplete | HIGH | Step 14: `isRevoked(newJti)` check + 3 tests |
| 19 missing test cases total | HIGH | All incorporated across steps 13-15 |
| TODO(APP-XX) placeholder invalid | MEDIUM | Replaced with TODO(APP-237) — file real Linear issue |
| Dispatcher null session unguarded | MEDIUM | Step 14: null-guard at channelRead top |
| No full pipeline integration test | MEDIUM | Documented deferral to APP-237 |
| Javadoc for modified constructors | MEDIUM | Step 16-17: update Javadoc noted |

---

## Accepted Out-of-Scope (with justification)

| Item | Decision | Justification | Industry Standard? |
|------|----------|---------------|-------------------|
| Certificate pinning (JWKS) | Go-live gate 17 | Requires cert rotation drill infrastructure | CME/Eurex use mTLS+HSM; this is a gap |
| Security audit logger | Go-live gate 8/13 | Separate PR with Loki integration | Required for MiFID II/FCA compliance |
| Full command rate limiter (50 cmd/sec) | Separate component | Architecture doc spec; not WebSocket scope | Exchange-grade requires per-user rate limiting |
| SlowConsumerHandler (graduated backpressure) | PR 4 | Existing writeBufferHighWaterMark provides basic protection | LMAX uses graduated disconnect; PR 4 addresses |
| Symbol-level entitlements | APP-236 | Account-level sufficient for FX OTC; no symbol ACL model yet | Eurex has per-product; acceptable for OTC |
| AccountProjection wiring | APP-237 | Requires projection sharing between cluster and WS server | Must-have before real auth testing |
| Session resume security | PR 4 | Stub returns `NotImplemented` error (not silently logged) | Must reject rather than ignore |
| Global auth failure circuit breaker | Pre-go-live | Enhances distributed brute force mitigation beyond per-IP | LMAX/CME use global rate limiting |
| Channel pinning (single event loop) | Future PR | Eliminates cross-thread Runnable alloc; not required at 256 sessions | LMAX uses dedicated dissemination thread |
| Drain loop budget | Future PR | Caps worst-case cycle time; not blocking at current scale | Recommended for production scale-up |
| BinaryWebSocketFrame pooling | Future PR | ZGC handles current allocation rate | Netty 5 may eliminate this |
| JWKS fetch off-event-loop | **Fixed in PR 3** | Async via CompletableFuture; completion on event loop | Industry standard for blocking I/O in Netty |
